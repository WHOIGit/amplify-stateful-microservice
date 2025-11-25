"""Asynchronous IFCB client."""

import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterable

import httpx
import websockets

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
from .utils import calculate_part_size, validate_bin_files, discover_bins


class AsyncIFCBClient:
    """
    Asynchronous client for IFCB microservices.

    Use this for concurrent operations or in async applications.

    Example:
        >>> async with AsyncIFCBClient("http://localhost:8001") as client:
        ...     job = await client.submit_job(manifest_uri="s3://bucket/manifest.json")
        ...     result = await client.wait_for_job(job.job_id)
        ...     print(result.result.payload)
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

    async def wait_for_job_ws(
        self,
        job_id: str,
        timeout: Optional[float] = 3600.0,
    ) -> JobStatus:
        """ Wait for job completion using websocket. """
        try:
            ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
            ws_url = f"{ws_url}/jobs/{job_id}/progress"

            async with websockets.connect(ws_url) as ws:
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    status = JobStatus(**json.loads(msg))

                    if status.status == "completed":
                        return status
                    elif status.status == "failed":
                        raise JobFailedError(job_id, status.error)

        except Exception as e:
            logger.warning(f"WebSocket failed: {e}. Falling back to polling.")
            return await self.wait_for_job(job_id, timeout=timeout)

    # ============================================================================
    # Ingest Endpoints
    # ============================================================================

    async def start_ingest(
        self,
        files: List[Dict[str, Any]],
    ) -> IngestStartResponse:
        """Start multipart upload for files."""
        if not files:
            raise ValueError("Must provide at least one file to start ingest")

        payload = {"files": files}

        response = await self._request("POST", "/ingest/start", json=payload)
        return IngestStartResponse(**response.json())

    async def complete_ingest(
        self,
        job_id: str,
        file_id: str,
        upload_id: str,
        parts: List[Dict[str, Any]],
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
        validate_bin_files(file_paths)
        return await self.upload_bins({bin_id: file_paths})

    async def upload_bins(self, bins: Dict[str, Dict[str, Path]]) -> str:
        """Upload multiple bins in a single ingest workflow."""

        if not bins:
            raise ValueError("No bins provided")

        # Flatten all files into a single list
        file_entries = []
        local_file_map: Dict[str, Path] = {}  # filename -> local path

        for bin_id, file_paths in bins.items():
            validate_bin_files(file_paths)

            for ext, path in file_paths.items():
                if path.name in local_file_map:
                    raise UploadError(f"Duplicate filename {path.name} found")
                file_entries.append({
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                })
                local_file_map[path.name] = path

        ingest_response = await self.start_ingest(file_entries)

        if not ingest_response.files:
            raise UploadError("Ingest response did not include any files")

        async with httpx.AsyncClient() as upload_client:
            for file_info in ingest_response.files:
                local_path = local_file_map.get(file_info.filename)
                if not local_path:
                    raise UploadError(f"Local file for {file_info.filename} not found")

                part_size, _ = calculate_part_size(local_path.stat().st_size)
                completed_parts = []

                with open(local_path, 'rb') as file_handle:
                    for part in file_info.part_urls:
                        part_number = part.part_number
                        chunk = file_handle.read(part_size)

                        try:
                            upload_response = await upload_client.put(part.url, content=chunk)
                            upload_response.raise_for_status()
                        except Exception as exc:
                            raise UploadError(
                                f"Failed to upload part {part_number} for {file_info.filename}: {exc}"
                            ) from exc

                        etag = upload_response.headers.get('ETag')
                        if not etag:
                            raise UploadError(
                                f"Upload part {part_number} for {file_info.filename} "
                                "succeeded but no ETag header was returned"
                            )
                        if not etag.startswith('"'):
                            etag = f'"{etag}"'

                        completed_parts.append({
                            "PartNumber": part_number,
                            "ETag": etag,
                        })

                await self.complete_ingest(
                    job_id=ingest_response.job_id,
                    file_id=file_info.file_id,
                    upload_id=file_info.upload_id,
                    parts=completed_parts,
                )

        return ingest_response.job_id

    async def upload_bins_from_directory(
        self,
        root: Path | str,
        *,
        recursive: bool = True,
        skip_incomplete: bool = False,
        required_extensions: Optional[Iterable[str]] = None,
    ) -> str:
        """Discover bins under a directory and upload them as a single job."""

        discovered = discover_bins(
            root,
            recursive=recursive,
            required_extensions=required_extensions,
            skip_incomplete=skip_incomplete,
        )

        if not discovered:
            raise ValueError("No complete bins found in the specified directory")

        return await self.upload_bins(discovered)
