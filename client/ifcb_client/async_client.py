"""Asynchronous IFCB client."""

import asyncio
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


class AsyncIFCBClient:
    """
    Asynchronous client for IFCB microservices.

    Use this for concurrent operations or in async applications.

    Example:
        >>> async with AsyncIFCBClient("http://localhost:8001") as client:
        ...     job = await client.submit_job(manifest_uri="s3://bucket/manifest.json")
        ...     result = await client.wait_for_job(job.job_id)
        ...     print(result.result.counts)
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        """
        Initialize async IFCB client.

        Args:
            base_url: Base URL of the IFCB service
            timeout: Request timeout in seconds
            max_retries: Number of retries for failed requests
        """
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient(
            timeout=timeout,
            transport=httpx.AsyncHTTPTransport(retries=max_retries),
        )

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args):
        """Async context manager exit."""
        await self.close()

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with error handling."""
        url = f"{self.base_url}{path}"

        try:
            response = await self.client.request(method, url, **kwargs)

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

    async def health(self) -> HealthResponse:
        """Check service health."""
        response = await self._request("GET", "/health")
        return HealthResponse(**response.json())

    # ============================================================================
    # Job Endpoints
    # ============================================================================

    async def submit_job(
        self,
        manifest_uri: Optional[str] = None,
        manifest_inline: Optional[Manifest] = None,
        callback_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        parameters: Optional[Dict] = None,
    ) -> JobSubmitResponse:
        """Submit a processing job."""
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

        response = await self._request("POST", "/jobs", json=payload)
        return JobSubmitResponse(**response.json())

    async def get_job(self, job_id: str) -> JobStatus:
        """Get job status."""
        response = await self._request("GET", f"/jobs/{job_id}")
        return JobStatus(**response.json())

    async def list_jobs(self, limit: int = 50) -> List[JobStatus]:
        """List recent jobs."""
        response = await self._request("GET", "/jobs", params={"limit": limit})
        return [JobStatus(**job) for job in response.json()]

    async def wait_for_job(
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
            timeout: Maximum seconds to wait

        Returns:
            Final job status

        Raises:
            JobFailedError: If job fails
            JobTimeoutError: If timeout exceeded
        """
        start_time = asyncio.get_event_loop().time()

        while True:
            status = await self.get_job(job_id)

            if status.status == "completed":
                return status

            if status.status == "failed":
                raise JobFailedError(job_id, status.error or "Unknown error")

            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    raise JobTimeoutError(job_id, int(timeout))

            await asyncio.sleep(poll_interval)

    # ============================================================================
    # Ingest Endpoints
    # ============================================================================

    async def start_ingest(
        self,
        bin_id: str,
        files: List[Dict[str, int]],
    ) -> IngestStartResponse:
        """Start multipart upload for bin files."""
        payload = {
            "bin_id": bin_id,
            "files": files,
        }

        response = await self._request("POST", "/ingest/start", json=payload)
        return IngestStartResponse(**response.json())

    async def complete_ingest(
        self,
        job_id: str,
        file_id: str,
        upload_id: str,
        parts: List[Dict[str, any]],
    ) -> IngestCompleteResponse:
        """Complete multipart upload for a file."""
        payload = {
            "job_id": job_id,
            "file_id": file_id,
            "upload_id": upload_id,
            "parts": parts,
        }

        response = await self._request("POST", "/ingest/complete", json=payload)
        return IngestCompleteResponse(**response.json())

    async def upload_bin(
        self,
        bin_id: str,
        file_paths: Dict[str, Path],
    ) -> str:
        """
        Upload bin files using multipart upload.

        Args:
            bin_id: Bin identifier
            file_paths: Dict mapping extension to file path

        Returns:
            Job ID for the uploaded bin
        """
        # Validate files
        validate_bin_files(file_paths)

        # Prepare file info
        files_info = []
        for ext, path in file_paths.items():
            files_info.append({
                "filename": path.name,
                "size_bytes": path.stat().st_size,
            })

        # Start ingest
        ingest_response = await self.start_ingest(bin_id, files_info)

        # Upload each file
        for file_info in ingest_response.files:
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

                    # Upload to presigned URL
                    try:
                        async with httpx.AsyncClient() as upload_client:
                            upload_response = await upload_client.put(part.url, content=chunk)
                            upload_response.raise_for_status()

                            # Get ETag from response
                            etag = upload_response.headers.get('ETag', '').strip('"')

                            completed_parts.append({
                                "PartNumber": part_number,
                                "ETag": etag,
                            })

                    except Exception as e:
                        raise UploadError(f"Failed to upload part {part_number}: {e}") from e

            # Complete upload for this file
            await self.complete_ingest(
                job_id=ingest_response.job_id,
                file_id=file_info.file_id,
                upload_id=file_info.upload_id,
                parts=completed_parts,
            )

        return ingest_response.job_id
