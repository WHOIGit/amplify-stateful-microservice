"""Job state management and tracking."""

import uuid
from datetime import datetime
from typing import Dict, Optional, List
from threading import Lock
import logging

from .models import JobStatus, JobResult

logger = logging.getLogger(__name__)


class JobStore:
    """
    In-memory job state store.

    For production, replace with Redis or PostgreSQL for persistence
    and multi-instance support.
    """

    def __init__(self):
        self._jobs: Dict[str, JobStatus] = {}
        self._lock = Lock()
        self._upload_metadata: Dict[str, Dict] = {}  # Temporary storage for upload metadata

    def create_job(
        self,
        manifest_uri: Optional[str] = None,
        manifest_data: Optional[Dict] = None,
        parameters: Optional[Dict] = None,
        callback_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        job_id_override: Optional[str] = None,
    ) -> str:
        """
        Create a new job.

        Args:
            manifest_uri: S3 URI to manifest file
            manifest_data: Inline manifest data
            parameters: Processing parameters
            callback_url: Webhook URL for completion
            idempotency_key: Optional idempotency key
            job_id_override: Use an existing job_id (e.g., ingest-created)

        Returns:
            Job ID
        """
        # Check idempotency
        if idempotency_key:
            with self._lock:
                for existing_id, job in self._jobs.items():
                    metadata = self._upload_metadata.get(existing_id, {})
                    if metadata.get('idempotency_key') == idempotency_key:
                        logger.info(f"Returning existing job {existing_id} for idempotency key {idempotency_key}")
                        return existing_id

        if job_id_override:
            job_id = job_id_override
        else:
            job_id = str(uuid.uuid4())
        now = datetime.utcnow()

        job_status = JobStatus(
            job_id=job_id,
            status="queued",
            created_at=now,
            started_at=None,
            completed_at=None,
            error=None,
            result=None,
            progress=None,
        )

        with self._lock:
            if job_id in self._jobs:
                raise ValueError(f"Job {job_id} already exists")

            self._jobs[job_id] = job_status
            existing_metadata = self._upload_metadata.get(job_id, {}).copy()
            existing_metadata.update({
                'manifest_uri': manifest_uri,
                'manifest_data': manifest_data,
                'parameters': parameters or {},
                'callback_url': callback_url,
                'idempotency_key': idempotency_key,
            })
            self._upload_metadata[job_id] = existing_metadata

        logger.info(f"Created job {job_id}")
        return job_id

    def get_job(self, job_id: str) -> Optional[JobStatus]:
        """
        Get job status.

        Args:
            job_id: Job ID

        Returns:
            JobStatus or None if not found
        """
        with self._lock:
            return self._jobs.get(job_id)

    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error: Optional[str] = None,
        result: Optional[JobResult] = None,
        progress: Optional[Dict] = None,
    ):
        """
        Update job status.

        Args:
            job_id: Job ID
            status: New status
            started_at: Processing start time
            completed_at: Completion time
            error: Error message
            result: Job result
            progress: Progress information
        """
        with self._lock:
            if job_id not in self._jobs:
                raise ValueError(f"Job {job_id} not found")

            job = self._jobs[job_id]
            if status is not None:
                job.status = status
            if started_at is not None:
                job.started_at = started_at
            if completed_at is not None:
                job.completed_at = completed_at
            if error is not None:
                job.error = error
            if result is not None:
                job.result = result
            if progress is not None:
                job.progress = progress

        logger.info(f"Updated job {job_id}: status={status}")

    def get_job_metadata(self, job_id: str) -> Optional[Dict]:
        """Get job metadata (manifest URI, parameters, etc.)."""
        with self._lock:
            return self._upload_metadata.get(job_id)

    def list_jobs(self, limit: int = 100) -> List[JobStatus]:
        """
        List recent jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of JobStatus objects
        """
        with self._lock:
            jobs = list(self._jobs.values())
            # Sort by creation time, most recent first
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return jobs[:limit]

    # ==========================================================================
    # Ingest Metadata Helpers
    # ==========================================================================

    def init_ingest_job(self, job_id: str, bin_ids: List[str]):
        """Initialize ingest metadata for a job."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            metadata['bin_order'] = bin_ids
            metadata['bins'] = {
                bin_id: {
                    'uploads': {},
                    'file_order': [],
                    'manifest_entry': None,
                }
                for bin_id in bin_ids
            }
            metadata['file_to_bin'] = {}
            self._upload_metadata[job_id] = metadata

    def store_upload_info(self, job_id: str, bin_id: str, file_id: str, upload_info: Dict):
        """
        Store temporary upload information.

        Args:
            job_id: Job ID
            bin_id: Bin ID
            file_id: File ID
            upload_info: Upload metadata (upload_id, s3_key, etc.)
        """
        with self._lock:
            metadata = self._upload_metadata.setdefault(job_id, {})
            bins = metadata.setdefault('bins', {})
            bin_meta = bins.setdefault(bin_id, {
                'uploads': {},
                'file_order': [],
                'manifest_entry': None,
            })

            bin_meta['uploads'][file_id] = upload_info
            if file_id not in bin_meta['file_order']:
                bin_meta['file_order'].append(file_id)

            metadata.setdefault('file_to_bin', {})[file_id] = bin_id

    def get_upload_info(self, job_id: str, bin_id: str, file_id: str) -> Optional[Dict]:
        """Get stored upload information for a specific bin/file."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id, {})
            uploads = bin_meta.get('uploads', {})
            return uploads.get(file_id)

    def mark_upload_completed(self, job_id: str, bin_id: str, file_id: str, etag: str):
        """Mark an upload as complete and store its final ETag."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id)
            if not bin_meta or file_id not in bin_meta['uploads']:
                raise ValueError(f"Upload info not found for job {job_id}, bin {bin_id}, file {file_id}")

            upload_info = bin_meta['uploads'][file_id]
            upload_info['completed'] = True
            upload_info['etag'] = etag

    def bin_is_complete(self, job_id: str, bin_id: str) -> bool:
        """Return True if all uploads for the bin are complete."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id)
            if not bin_meta:
                return False
            uploads = bin_meta.get('uploads', {})
            return bool(uploads) and all(upload.get('completed') for upload in uploads.values())

    def bin_manifest_exists(self, job_id: str, bin_id: str) -> bool:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id, {})
            return bin_meta.get('manifest_entry') is not None

    def set_bin_manifest(self, job_id: str, bin_id: str, manifest_entry: Dict):
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            if bin_id not in bins:
                raise ValueError(f"Bin {bin_id} not found for job {job_id}")
            bins[bin_id]['manifest_entry'] = manifest_entry

    def all_bins_have_manifest(self, job_id: str) -> bool:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            if not bins:
                return False
            return all(bin_meta.get('manifest_entry') is not None for bin_meta in bins.values())

    def get_bin_order(self, job_id: str) -> List[str]:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            return metadata.get('bin_order', [])

    def get_manifest_entries(self, job_id: str) -> List[Dict]:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            order = metadata.get('bin_order', list(bins.keys()))
            entries = []
            for bin_id in order:
                bin_meta = bins.get(bin_id)
                if not bin_meta or not bin_meta.get('manifest_entry'):
                    continue
                entries.append(bin_meta['manifest_entry'])
            return entries

    def get_bin_uploads(self, job_id: str, bin_id: str) -> Dict[str, Dict]:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id, {})
            return bin_meta.get('uploads', {}).copy()

    def get_bin_file_order(self, job_id: str, bin_id: str) -> List[str]:
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            bins = metadata.get('bins', {})
            bin_meta = bins.get(bin_id, {})
            return list(bin_meta.get('file_order', []))

    def set_manifest_data(self, job_id: str, manifest_data: Dict):
        with self._lock:
            metadata = self._upload_metadata.setdefault(job_id, {})
            metadata['manifest_data'] = manifest_data


# Global job store instance
job_store = JobStore()
