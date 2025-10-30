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

    def store_upload_info(self, job_id: str, file_id: str, upload_info: Dict):
        """
        Store temporary upload information.

        Args:
            job_id: Job ID
            file_id: File ID
            upload_info: Upload metadata (upload_id, s3_key, etc.)
        """
        with self._lock:
            if job_id not in self._upload_metadata:
                self._upload_metadata[job_id] = {}
            if 'uploads' not in self._upload_metadata[job_id]:
                self._upload_metadata[job_id]['uploads'] = {}
            self._upload_metadata[job_id]['uploads'][file_id] = upload_info

    def get_upload_info(self, job_id: str, file_id: str) -> Optional[Dict]:
        """
        Get stored upload information.

        Args:
            job_id: Job ID
            file_id: File ID

        Returns:
            Upload metadata or None
        """
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            uploads = metadata.get('uploads', {})
            return uploads.get(file_id)


# Global job store instance
job_store = JobStore()
