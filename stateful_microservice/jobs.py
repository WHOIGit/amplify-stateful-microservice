"""Job state management and tracking."""

import uuid
from datetime import datetime
from typing import Dict, Optional, List, Any
from threading import Lock
import logging

import asyncio

from .models import JobStatus

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
        job_id_override: Optional[str] = None,
    ) -> str:
        """
        Create a new job.

        Args:
            manifest_uri: S3 URI to manifest file
            manifest_data: Inline manifest data
            job_id_override: Use an existing job_id (e.g., ingest-created)

        Returns:
            Job ID
        """
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
        result: Optional[Dict[str, Any]] = None,
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

        self._notify_websocket(job_id)

    def _notify_websocket(self, job_id: str):
        """ Send job update to websocket. """
        from .api import active_websockets

        if job_id in active_websockets:
            ws = active_websockets[job_id]
            job = self.get_job(job_id)

            try:
                asyncio.create_task(ws.send_json(job.dict()))
            except RuntimeError:
                pass

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

    def store_upload_info(self, job_id: str, file_id: str, upload_info: Dict):
        """
        Store temporary upload information.

        Args:
            job_id: Job ID
            file_id: File ID
            upload_info: Upload metadata (upload_id, s3_key, etc.)
        """
        with self._lock:
            metadata = self._upload_metadata.setdefault(job_id, {})
            uploads = metadata.setdefault('uploads', {})

            uploads[file_id] = upload_info

    def get_upload_info(self, job_id: str, file_id: str) -> Optional[Dict]:
        """Get stored upload information for a file."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            uploads = metadata.get('uploads', {})
            return uploads.get(file_id)

    def mark_upload_completed(self, job_id: str, file_id: str, etag: str):
        """Mark an upload as complete and store its final ETag."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            uploads = metadata.get('uploads', {})
            if file_id not in uploads:
                raise ValueError(f"Upload info not found for job {job_id}, file {file_id}")

            upload_info = uploads[file_id]
            upload_info['completed'] = True
            upload_info['etag'] = etag

    def all_files_complete(self, job_id: str) -> bool:
        """Return True if all uploads for the job are complete."""
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            uploads = metadata.get('uploads', {})
            if not uploads:
                return False
            return all(upload.get('completed') for upload in uploads.values())

    def get_completed_file_uris(self, job_id: str) -> List[str]:
        """Get S3 URIs for all completed files in upload order."""
        from .storage import s3_client
        with self._lock:
            metadata = self._upload_metadata.get(job_id, {})
            uploads = metadata.get('uploads', {})

            uris = []
            for file_id in uploads.keys():
                info = uploads[file_id]
                if info.get('completed'):
                    uris.append(s3_client.get_object_url(info['s3_key']))
            return uris

    def set_manifest_data(self, job_id: str, manifest_data: Dict):
        with self._lock:
            metadata = self._upload_metadata.setdefault(job_id, {})
            metadata['manifest_data'] = manifest_data


# Global job store instance
job_store = JobStore()
