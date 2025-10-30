"""Synchronous IFCB client."""

import json
import time
from pathlib import Path
from typing import Optional, List, Dict
import httpx

from .models import (
    HealthResponse,
    JobStatus,
    JobSubmitResponse,
    IngestStartResponse,
    IngestCompleteResponse,
    Manifest,
    BinManifestEntry,
)
from .exceptions import (
    JobNotFoundError,
    JobFailedError,
    JobTimeoutError,
    APIError,
    NetworkError,
    UploadError,
)
from .utils import calculate_part_size, validate_bin_files


class IFCBClient:
    """
    Synchronous client for IFCB microservices.

    Works with any IFCB algorithm service (features, classifier, etc.)
    since they all expose the same API.

    Example:
        >>> client = IFCBClient("http://localhost:8001")
        >>> job = client.submit_job(manifest_uri="s3://bucket/manifest.json")
        >>> result = client.wait_for_job(job.job_id)
        >>> print(result.result.counts)
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        debug: bool = False,
    ):
        """
        Initialize IFCB client.

        Args:
            base_url: Base URL of the IFCB service (e.g., "http://localhost:8001")
            timeout: Request timeout in seconds
            max_retries: Number of retries for failed requests
            debug: Print debug output for each step when True
        """
        self.base_url = base_url.rstrip('/')
        self.client = httpx.Client(
            timeout=timeout,
            transport=httpx.HTTPTransport(retries=max_retries),
        )
        self.debug = debug

    def _debug(self, message: str):
        """Print debug messages when debug mode is enabled."""
        if self.debug:
            print(f"[IFCBClient] {message}")

    def _format_payload(self, payload: Dict, max_length: int = 2000) -> str:
        """Return a compact JSON representation, truncated if necessary."""
        try:
            serialized = json.dumps(payload, default=str)
        except TypeError:
            serialized = str(payload)

        if len(serialized) > max_length:
            return serialized[:max_length] + "...(truncated)"
        return serialized

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        self.close()

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """
        Make an HTTP request with error handling.

        Args:
            method: HTTP method
            path: URL path
            **kwargs: Additional arguments to pass to httpx

        Returns:
            HTTP response

        Raises:
            APIError: If API returns an error
            NetworkError: If network request fails
        """
        url = f"{self.base_url}{path}"

        try:
            if self.debug and "json" in kwargs and kwargs["json"] is not None:
                formatted = self._format_payload(kwargs["json"])
                self._debug(f"{method} {path} payload={formatted}")

            response = self.client.request(method, url, **kwargs)

            # Handle error responses
            if response.status_code >= 400:
                try:
                    error_detail = response.json().get('detail', response.text)
                except Exception:
                    error_detail = response.text

                if response.status_code == 404:
                    raise JobNotFoundError(error_detail)
                else:
                    raise APIError(response.status_code, error_detail)

            return response

        except httpx.RequestError as e:
            raise NetworkError(f"Network error: {e}") from e

    # ============================================================================
    # Health Endpoints
    # ============================================================================

    def health(self) -> HealthResponse:
        """
        Check service health.

        Returns:
            Health response with status and version

        Example:
            >>> health = client.health()
            >>> print(health.status, health.version)
        """
        response = self._request("GET", "/health")
        return HealthResponse(**response.json())

    # ============================================================================
    # Job Endpoints
    # ============================================================================

    def submit_job(
        self,
        manifest_uri: Optional[str] = None,
        manifest_inline: Optional[Manifest] = None,
        callback_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        parameters: Optional[Dict] = None,
    ) -> JobSubmitResponse:
        """
        Submit a processing job.

        Provide either manifest_uri (S3 path) or manifest_inline (dict/object).

        Args:
            manifest_uri: S3 URI to manifest file
            manifest_inline: Inline manifest data
            callback_url: Webhook URL for completion notification
            idempotency_key: Key to prevent duplicate processing
            parameters: Algorithm-specific parameters

        Returns:
            Job submission response with job_id

        Raises:
            ValueError: If neither or both manifests provided
            APIError: If API returns an error

        Example:
            >>> manifest = Manifest(bins=[
            ...     BinManifestEntry(
            ...         bin_id="test",
            ...         files=["s3://bucket/test.adc", "s3://bucket/test.roi", "s3://bucket/test.hdr"],
            ...         bytes=1000000
            ...     )
            ... ])
            >>> job = client.submit_job(manifest_inline=manifest)
            >>> print(job.job_id)
        """
        if not manifest_uri and not manifest_inline:
            raise ValueError("Must provide either manifest_uri or manifest_inline")

        if manifest_uri and manifest_inline:
            raise ValueError("Cannot provide both manifest_uri and manifest_inline")

        payload = {}

        if manifest_uri:
            payload["manifest_uri"] = manifest_uri

        if manifest_inline:
            if isinstance(manifest_inline, Manifest):
                payload["manifest_inline"] = manifest_inline.model_dump()
            else:
                payload["manifest_inline"] = manifest_inline

        if callback_url:
            payload["callback_url"] = callback_url

        if idempotency_key:
            payload["idempotency_key"] = idempotency_key

        if parameters:
            payload["parameters"] = parameters

        response = self._request("POST", "/jobs", json=payload)
        return JobSubmitResponse(**response.json())

    def get_job(self, job_id: str) -> JobStatus:
        """
        Get job status.

        Args:
            job_id: Job ID

        Returns:
            Job status with details

        Raises:
            JobNotFoundError: If job doesn't exist

        Example:
            >>> status = client.get_job("job-123")
            >>> print(status.status)  # "completed", "processing", etc.
        """
        response = self._request("GET", f"/jobs/{job_id}")
        return JobStatus(**response.json())

    def list_jobs(self, limit: int = 50) -> List[JobStatus]:
        """
        List recent jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job statuses

        Example:
            >>> jobs = client.list_jobs(limit=10)
            >>> for job in jobs:
            ...     print(job.job_id, job.status)
        """
        response = self._request("GET", "/jobs", params={"limit": limit})
        return [JobStatus(**job) for job in response.json()]

    def wait_for_job(
        self,
        job_id: str,
        poll_interval: float = 5.0,
        timeout: Optional[float] = 3600.0,
    ) -> JobStatus:
        """
        Poll job status until completion or failure.

        Args:
            job_id: Job ID to wait for
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait (None = no timeout)

        Returns:
            Final job status

        Raises:
            JobFailedError: If job fails
            JobTimeoutError: If timeout exceeded
            JobNotFoundError: If job doesn't exist

        Example:
            >>> job = client.submit_job(manifest_uri="s3://bucket/manifest.json")
            >>> result = client.wait_for_job(job.job_id)
            >>> print(result.result.counts.rois)
        """
        start_time = time.time()

        while True:
            status = self.get_job(job_id)

            if status.status == "completed":
                return status

            if status.status == "failed":
                raise JobFailedError(job_id, status.error or "Unknown error")

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise JobTimeoutError(job_id, int(timeout))

            time.sleep(poll_interval)

    # ============================================================================
    # Ingest Endpoints (Multipart Upload)
    # ============================================================================

    def start_ingest(
        self,
        bin_id: str,
        files: List[Dict[str, int]],
    ) -> IngestStartResponse:
        """
        Start multipart upload for bin files.

        Args:
            bin_id: Bin identifier
            files: List of dicts with 'filename' and 'size_bytes'

        Returns:
            Ingest response with presigned URLs

        Example:
            >>> response = client.start_ingest(
            ...     bin_id="D20230101T120000_IFCB123",
            ...     files=[
            ...         {"filename": "test.adc", "size_bytes": 1000000},
            ...         {"filename": "test.roi", "size_bytes": 5000000},
            ...         {"filename": "test.hdr", "size_bytes": 5000},
            ...     ]
            ... )
            >>> print(response.job_id)
        """
        payload = {
            "bin_id": bin_id,
            "files": files,
        }

        response = self._request("POST", "/ingest/start", json=payload)
        ingest_response = IngestStartResponse(**response.json())

        self._debug(
            f"Start ingest succeeded: job_id={ingest_response.job_id}, bin_id={ingest_response.bin_id}"
        )
        for file_info in ingest_response.files:
            self._debug(
                "  file "
                f"{file_info.filename}: file_id={file_info.file_id}, "
                f"upload_id={file_info.upload_id}, parts={len(file_info.part_urls)}"
            )

        return ingest_response

    def complete_ingest(
        self,
        job_id: str,
        file_id: str,
        upload_id: str,
        parts: List[Dict[str, any]],
    ) -> IngestCompleteResponse:
        """
        Complete multipart upload for a file.

        Args:
            job_id: Job ID from start_ingest
            file_id: File ID from start_ingest
            upload_id: Upload ID from start_ingest
            parts: List of dicts with 'PartNumber' and 'ETag'

        Returns:
            Completion response

        Example:
            >>> response = client.complete_ingest(
            ...     job_id="job-123",
            ...     file_id="file-456",
            ...     upload_id="upload-789",
            ...     parts=[{"PartNumber": 1, "ETag": "etag1"}]
            ... )
        """
        payload = {
            "job_id": job_id,
            "file_id": file_id,
            "upload_id": upload_id,
            "parts": parts,
        }

        response = self._request("POST", "/ingest/complete", json=payload)
        complete_response = IngestCompleteResponse(**response.json())
        self._debug(
            f"Complete ingest succeeded: file_id={complete_response.file_id}, "
            f"s3_key={complete_response.s3_key}, etag={complete_response.etag}"
        )
        return complete_response

    def upload_bin(
        self,
        bin_id: str,
        file_paths: Dict[str, Path],
    ) -> str:
        """
        Upload bin files using multipart upload (convenience method).

        This handles the entire upload workflow:
        1. Validates files exist
        2. Starts multipart upload
        3. Uploads all parts
        4. Completes upload

        Args:
            bin_id: Bin identifier
            file_paths: Dict mapping extension to file path
                       Example: {'.adc': Path('test.adc'), ...}

        Returns:
            Job ID for the uploaded bin

        Raises:
            UploadError: If upload fails
            ValueError: If files are invalid

        Example:
            >>> from pathlib import Path
            >>> job_id = client.upload_bin(
            ...     bin_id="D20230101T120000_IFCB123",
            ...     file_paths={
            ...         '.adc': Path('/data/test.adc'),
            ...         '.roi': Path('/data/test.roi'),
            ...         '.hdr': Path('/data/test.hdr'),
            ...     }
            ... )
            >>> result = client.wait_for_job(job_id)
        """
        # Validate files
        validate_bin_files(file_paths)
        self._debug(f"Uploading bin {bin_id}")

        # Prepare file info
        files_info = []
        for ext, path in file_paths.items():
            self._debug(f"  file {path.name}: size_bytes={path.stat().st_size}")
            files_info.append({
                "filename": path.name,
                "size_bytes": path.stat().st_size,
            })

        # Start ingest
        ingest_response = self.start_ingest(bin_id, files_info)

        # Upload each file
        for file_info in ingest_response.files:
            self._debug(
                f"Uploading file {file_info.filename} "
                f"(file_id={file_info.file_id}, upload_id={file_info.upload_id})"
            )
            # Find corresponding local file
            local_path = None
            for ext, path in file_paths.items():
                if path.name == file_info.filename:
                    local_path = path
                    break

            if not local_path:
                raise UploadError(f"File {file_info.filename} not found in file_paths")

            # Upload parts
            part_size, num_parts = calculate_part_size(local_path.stat().st_size)
            completed_parts = []

            with open(local_path, 'rb') as f:
                for part in file_info.part_urls:
                    part_number = part.part_number

                    # Read part data
                    chunk = f.read(part_size)
                    chunk_len = len(chunk)

                    # Upload to presigned URL
                    try:
                        upload_response = httpx.put(part.url, content=chunk)
                        upload_response.raise_for_status()

                        # Get ETag from response
                        etag = upload_response.headers.get('ETag')
                        if not etag:
                            raise UploadError(
                                f"Upload part {part_number} for {file_info.filename} "
                                "succeeded but no ETag header was returned"
                            )

                        # AWS expects the ETag value (including quotes) that was returned
                        # from the upload_part request.
                        if not etag.startswith('"'):
                            etag = f'"{etag}"'

                        completed_parts.append({
                            "PartNumber": part_number,
                            "ETag": etag,
                        })

                        self._debug(
                            f"    uploaded part {part_number} ({chunk_len} bytes) "
                            f"status={upload_response.status_code} etag={etag}"
                        )

                    except Exception as e:
                        raise UploadError(f"Failed to upload part {part_number}: {e}") from e

            # Complete upload for this file
            self.complete_ingest(
                job_id=ingest_response.job_id,
                file_id=file_info.file_id,
                upload_id=file_info.upload_id,
                parts=completed_parts,
            )

        self._debug(f"Upload flow completed for job_id={ingest_response.job_id}")
        return ingest_response.job_id
