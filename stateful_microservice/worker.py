"""Background worker for processing queued jobs using pluggable processors."""

import asyncio
import io
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any, Tuple, Type
import logging

from pydantic import BaseModel

from .processor import BaseProcessor, JobInput, DefaultResult
from .jobs import job_store
from .storage import s3_client
from .models import JobResult
from .config import settings

logger = logging.getLogger(__name__)


class JobProcessor:
    """Processes queued jobs using a pluggable processor."""

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
            file_uris = manifest.get('files') or []
            if not file_uris:
                raise ValueError(f"No files provided in manifest for job {job_id}")

            # Progress callback that updates job store directly
            def progress_callback(progress_data: Dict):
                job_store.update_job(
                    job_id=job_id,
                    progress=progress_data,
                )

            logger.info(f"Processing job {job_id} with {self.processor.name} ({len(file_uris)} files)")

            # Download all files from S3
            progress_callback({"stage": "download", "message": "start"})
            job_input, temp_dir = await self._prepare_job_input(job_id, file_uris)
            progress_callback({"stage": "download", "message": "complete"})

            # Process using the algorithm-specific processor (single call)
            progress_callback({"stage": "processing", "percent": 0.0})
            processor_result = await self._process_input(job_input, progress_callback=progress_callback)
            progress_callback({"stage": "processing", "percent": 100.0})

            # Clean up temporary files
            shutil.rmtree(temp_dir, ignore_errors=True)

            if processor_result is None:
                processor_result = DefaultResult()

            job_result = self._convert_to_job_result(job_id, processor_result)

            # Update job as completed
            job_store.update_job(
                job_id=job_id,
                status="completed",
                completed_at=datetime.utcnow(),
                result=job_result,
                progress={
                    "stage": "completed",
                    "percent": 100.0,
                },
            )

            logger.info(f"Job {job_id} completed successfully")

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job_store.update_job(
                job_id=job_id,
                status="failed",
                completed_at=datetime.utcnow(),
                error=str(e),
            )

    def _convert_to_job_result(self, job_id: str, payload: Optional[BaseModel]) -> JobResult:
        """
        Normalize processor output into a JobResult.

        Processors can return:
            - An instance of `processor.result_model`
            - None (falls back to `DefaultResult`)
        """
        result_model: Type[BaseModel] = getattr(self.processor, "result_model", DefaultResult)

        if payload is None:
            model_instance = result_model()
        else:
            if not isinstance(payload, result_model):
                raise TypeError(f"Processor returned {type(payload)}, expected {result_model}.")
            model_instance = payload

        return JobResult(
            job_id=job_id,
            payload=model_instance.model_dump(),
        )

    async def _load_manifest(self, metadata: Dict) -> Dict:
        """
        Load manifest from S3 URI or inline data.

        Args:
            metadata: Job metadata

        Returns:
            Manifest dict with 'files' key
        """
        manifest_uri = metadata.get('manifest_uri')
        manifest_data = metadata.get('manifest_data')

        if manifest_data:
            # Use inline manifest
            return manifest_data

        if manifest_uri:
            # Download from S3
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

            # Parse manifest JSON
            content = buffer.read().decode('utf-8')
            manifest = json.loads(content)
            return manifest

        raise ValueError("No manifest provided")

    async def _prepare_job_input(
        self,
        job_id: str,
        file_uris: List[str],
    ) -> Tuple[JobInput, Path]:
        """Download input files and provide their local paths for processor use."""
        temp_dir = Path(tempfile.mkdtemp(prefix=f"{job_id}_"))
        local_paths: List[str] = []

        for uri in file_uris:
            if not uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {uri}")

            parts = uri[5:].split('/', 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid S3 URI format: {uri}")
            bucket, key = parts
            if bucket != s3_client.bucket:
                raise ValueError(
                    f"Input bucket {bucket} does not match configured bucket {s3_client.bucket}"
                )

            dest_path = temp_dir / Path(key).name
            with open(dest_path, 'wb') as f:
                s3_client.download_fileobj(key, f)

            local_paths.append(str(dest_path))
            logger.debug(f"Downloaded {uri} to {dest_path}")

        job_input = JobInput(
            job_id=job_id,
            local_paths=local_paths,
        )
        return job_input, temp_dir

    async def _process_input(
        self,
        job_input: JobInput,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Optional[BaseModel]:
        """
        Process an input payload using the algorithm-specific processor.

        Args:
            job_input: Runtime input context containing metadata and file references
            progress_callback: Optional callback for progress updates

        Returns:
            Optional instance of the processor’s `result_model`.
        """
        loop = asyncio.get_event_loop()
        if progress_callback:
            self.processor.set_progress_callback(progress_callback)
        else:
            self.processor.set_progress_callback(None)

        try:
            result = await loop.run_in_executor(
                None,
                self.processor.process_input,
                job_input,
            )
        finally:
            self.processor.set_progress_callback(None)

        if result is None:
            return None

        if not isinstance(result, BaseModel):
            raise TypeError(
                f"Processors must return instances of their result_model (or None). Got {type(result)}."
            )

        logger.info(f"Processor emitted job-level results for job {job_input.job_id}")
        return result


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
