"""Background worker for processing IFCB jobs using pluggable processors."""

from __future__ import annotations

import asyncio
import io
import json
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any, TYPE_CHECKING
import logging

from ._optional import require_pandas, require_pillow_image

from .processor import BaseProcessor
from .jobs import job_store
from .storage import s3_client
from .output_writers import ResultsWriter
from .models import JobResult, FeaturesOutput, MasksOutput, MasksShard, JobCounts
from .config import settings

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class JobProcessor:
    """Processes IFCB jobs using a pluggable processor."""

    def __init__(self, processor: BaseProcessor):
        """
        Initialize job processor with algorithm-specific processor.

        Args:
            processor: Implementation of BaseProcessor for the algorithm
        """
        self.processor = processor
        logger.info(f"JobProcessor initialized with {processor.name} v{processor.version}")

    async def process_job(self, job_id: str):
        """
        Process a single job asynchronously.

        Args:
            job_id: Job ID to process
        """
        try:
            logger.info(f"Starting processing for job {job_id}")

            # Ensure required optional dependencies are present
            require_pandas()

            # Update job status
            job_store.update_job(
                job_id=job_id,
                status="processing",
                started_at=datetime.utcnow(),
            )

            # Get job metadata
            metadata = job_store.get_job_metadata(job_id)
            if not metadata:
                raise ValueError(f"Job metadata not found for {job_id}")

            # Get manifest
            manifest = await self._load_manifest(metadata)

            total_bins = len(manifest.get('bins', []))

            def make_progress_emitter(current_bin: int, bin_id: str):
                def emit(partial: Dict):
                    stage = partial.get("stage", "processing")
                    bin_percent = partial.get("percent")

                    job_percent = None
                    if total_bins:
                        if bin_percent is not None:
                            job_percent = ((current_bin - 1) + (bin_percent / 100.0)) / total_bins * 100.0
                        else:
                            job_percent = ((current_bin - 1) / total_bins) * 100.0

                    detail = {
                        "bin_id": bin_id,
                        "bin_index": current_bin,
                        "bin_total": total_bins,
                    }

                    if bin_percent is not None:
                        detail["bin_percent"] = bin_percent

                    for key, value in partial.items():
                        if key not in {"stage", "percent"}:
                            detail[key] = value

                    payload = {
                        "stage": stage,
                        "percent": job_percent,
                        "detail": detail,
                    }

                    job_store.update_job(
                        job_id=job_id,
                        progress=payload,
                    )
                return emit

            # Initialize results writer
            results_writer = ResultsWriter(job_id)

            # Process each bin in the manifest
            for index, bin_entry in enumerate(manifest['bins'], start=1):
                bin_id = bin_entry['bin_id']
                file_uris = bin_entry['files']

                logger.info(f"Processing bin {bin_id} with {self.processor.name}")

                emit_progress = make_progress_emitter(index, bin_id)
                emit_progress({"stage": "download", "message": "start"})

                # Download bin files from S3
                bin_files = await self._download_bin_files(bin_id, file_uris)
                emit_progress({"stage": "download", "message": "complete"})

                # Process using the algorithm-specific processor
                emit_progress({"stage": "processing", "message": "executor_start", "percent": 0.0})
                features_df, artifacts = await self._process_bin(
                    bin_id,
                    bin_files,
                    progress_callback=emit_progress,
                )
                emit_progress(
                    {
                        "stage": "processing",
                        "message": "executor_complete",
                        "percent": 100.0,
                        "rois_in_bin": len(features_df),
                    }
                )

                # Add to results writer (no additional progress; handled via emit_progress)
                results_writer.add_bin_results(bin_id, features_df, artifacts)

                # Clean up temporary files
                for path in bin_files.values():
                    Path(path).unlink(missing_ok=True)

            # Finalize outputs
            results = results_writer.finalize()

            # Create JobResult model
            job_result = JobResult(
                job_id=job_id,
                features=FeaturesOutput(**results['features']),
                masks=MasksOutput(
                    format="webdataset",
                    shards=[MasksShard(**shard) for shard in results['masks']['shards']],
                ),
                counts=JobCounts(**results['counts']),
            )

            # Update job as completed
            job_store.update_job(
                job_id=job_id,
                status="completed",
                completed_at=datetime.utcnow(),
                result=job_result,
                progress={
                    "stage": "completed",
                    "percent": 100.0,
                    "detail": {
                        "processed_bins": total_bins,
                        "total_bins": total_bins,
                        "processed_rois": results['counts']['rois'],
                    },
                },
            )

            logger.info(f"Job {job_id} completed successfully")

            # Send webhook if configured
            callback_url = metadata.get('callback_url')
            if callback_url:
                await self._send_webhook(callback_url, job_result)

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job_store.update_job(
                job_id=job_id,
                status="failed",
                completed_at=datetime.utcnow(),
                error=str(e),
            )

    async def _load_manifest(self, metadata: Dict) -> Dict:
        """
        Load manifest from S3 URI or inline data.

        Args:
            metadata: Job metadata

        Returns:
            Manifest dict
        """
        manifest_uri = metadata.get('manifest_uri')
        manifest_data = metadata.get('manifest_data')

        if manifest_data:
            # Use inline manifest
            return manifest_data

        if manifest_uri:
            # Download from S3
            # Extract key from URI (s3://bucket/key)
            if not manifest_uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {manifest_uri}")

            parts = manifest_uri[5:].split('/', 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid S3 URI format: {manifest_uri}")

            bucket, key = parts
            if bucket != s3_client.bucket:
                raise ValueError(f"Manifest bucket {bucket} does not match configured bucket {s3_client.bucket}")

            # Download manifest
            buffer = io.BytesIO()
            s3_client.download_fileobj(key, buffer)
            buffer.seek(0)

            # Parse manifest (JSONL or JSON)
            content = buffer.read().decode('utf-8')
            if manifest_uri.endswith('.jsonl'):
                # Parse JSONL
                bins = [json.loads(line) for line in content.strip().split('\n')]
                return {'bins': bins}
            else:
                # Parse JSON
                return json.loads(content)

        raise ValueError("No manifest provided")

    async def _download_bin_files(self, bin_id: str, file_uris: List[str]) -> Dict[str, str]:
        """
        Download bin files from S3 to temporary location.

        Args:
            bin_id: Bin identifier
            file_uris: List of S3 URIs

        Returns:
            Dict mapping extension to local file path
        """
        bin_files = {}

        for uri in file_uris:
            # Extract key from S3 URI
            if not uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {uri}")

            key = uri[5:].split('/', 1)[1]

            # Determine file extension
            extension = Path(key).suffix  # e.g., .adc, .roi, .hdr

            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=extension,
                prefix=f"{bin_id}_",
            )
            temp_file.close()

            # Download from S3
            with open(temp_file.name, 'wb') as f:
                s3_client.download_fileobj(key, f)

            bin_files[extension] = temp_file.name
            logger.debug(f"Downloaded {uri} to {temp_file.name}")

        return bin_files

    async def _process_bin(
        self,
        bin_id: str,
        bin_files: Dict[str, str],
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> tuple["pd.DataFrame", List]:
        """
        Process a bin using the algorithm-specific processor.

        Args:
            bin_id: Bin identifier
            bin_files: Dict mapping extension to file path
            progress_callback: Optional callback for progress updates

        Returns:
            Tuple of (features_df, artifacts_list)
        """
        # Convert paths to Path objects
        bin_paths = {ext: Path(path) for ext, path in bin_files.items()}

        # Run processor (CPU-bound, so run in executor)
        loop = asyncio.get_event_loop()
        if progress_callback:
            self.processor.set_progress_callback(progress_callback)
        else:
            self.processor.set_progress_callback(None)

        try:
            features_df, artifacts = await loop.run_in_executor(
                None,
                self.processor.process_bin,
                bin_id,
                bin_paths,
            )
        finally:
            self.processor.set_progress_callback(None)

        # Convert artifacts to bytes if needed
        artifacts_bytes = []
        image_module = None
        for artifact in artifacts:
            if isinstance(artifact, bytes):
                artifacts_bytes.append(artifact)
            else:
                # Assume it's a numpy array or PIL image
                import numpy as np
                if isinstance(artifact, np.ndarray):
                    # Convert to uint8 if needed
                    if artifact.dtype != np.uint8:
                        artifact = (artifact * 255).astype(np.uint8)
                    if image_module is None:
                        image_module = require_pillow_image()
                    img = image_module.fromarray(artifact)
                elif hasattr(artifact, 'save'):
                    # PIL Image
                    img = artifact
                else:
                    raise TypeError(f"Unsupported artifact type: {type(artifact)}")

                # Convert to PNG bytes
                buffer = io.BytesIO()
                img.save(buffer, format='PNG')
                artifacts_bytes.append(buffer.getvalue())

        logger.info(
            f"Processed bin {bin_id}: {len(features_df)} rows, "
            f"{len(artifacts_bytes)} artifacts"
        )

        return features_df, artifacts_bytes

    async def _send_webhook(self, callback_url: str, job_result: JobResult):
        """
        Send webhook notification for completed job.

        Args:
            callback_url: Webhook URL
            job_result: Job result
        """
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    callback_url,
                    json=job_result.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        logger.info(f"Webhook sent successfully to {callback_url}")
                    else:
                        logger.warning(f"Webhook returned status {response.status}")
        except Exception as e:
            logger.error(f"Failed to send webhook to {callback_url}: {e}")


class WorkerPool:
    """Pool of background workers for processing jobs."""

    def __init__(self, processor: BaseProcessor):
        """
        Initialize worker pool with algorithm-specific processor.

        Args:
            processor: Implementation of BaseProcessor
        """
        self.processor_instance = JobProcessor(processor)
        self.active_jobs: Dict[str, asyncio.Task] = {}
        self.max_concurrent = settings.max_concurrent_jobs
        self.running = False

    async def start(self):
        """Start the worker pool."""
        self.running = True
        logger.info(f"Worker pool started with max_concurrent={self.max_concurrent}")

        # Start background loop
        asyncio.create_task(self._process_loop())

    async def stop(self):
        """Stop the worker pool."""
        self.running = False
        logger.info("Worker pool stopping...")

        # Wait for active jobs to complete (with timeout)
        if self.active_jobs:
            tasks = list(self.active_jobs.values())
            await asyncio.wait(tasks, timeout=30)

    async def submit_job(self, job_id: str):
        """
        Submit a job for processing.

        Args:
            job_id: Job ID
        """
        if job_id in self.active_jobs:
            logger.warning(f"Job {job_id} already being processed")
            return

        # Wait if at capacity
        while len(self.active_jobs) >= self.max_concurrent:
            await asyncio.sleep(1)

        # Start processing task
        task = asyncio.create_task(self._process_job_wrapper(job_id))
        self.active_jobs[job_id] = task
        logger.info(f"Submitted job {job_id} for processing ({len(self.active_jobs)} active)")

    async def _process_job_wrapper(self, job_id: str):
        """Wrapper to clean up after job completes."""
        try:
            await self.processor_instance.process_job(job_id)
        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
                logger.debug(f"Removed job {job_id} from active jobs")

    async def _process_loop(self):
        """Background loop to check for queued jobs."""
        while self.running:
            try:
                # Check for queued jobs
                jobs = job_store.list_jobs(limit=100)
                for job in jobs:
                    if job.status == 'queued' and job.job_id not in self.active_jobs:
                        await self.submit_job(job.job_id)

                # Sleep before next check
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(5)


def create_worker_pool(processor: BaseProcessor) -> WorkerPool:
    """
    Factory function to create a worker pool for a processor.

    Args:
        processor: Implementation of BaseProcessor

    Returns:
        WorkerPool instance
    """
    return WorkerPool(processor)
