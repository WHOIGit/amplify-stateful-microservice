"""Synchronous IFCB client."""

import json
import os
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
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
    DownloadError,
)
from .utils import calculate_part_size, validate_bin_files, discover_bins


class IFCBClient:
    """
    Synchronous client for IFCB microservices.

    Works with any IFCB algorithm service (features, classifier, etc.)
    since they all expose the same API.

    Example:
        >>> client = IFCBClient("http://localhost:8001")
        >>> job = client.submit_job(manifest_uri="s3://bucket/manifest.json")
        >>> result = client.wait_for_job(job.job_id)
        >>> print(result.result.payload)
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        debug: bool = False,
        s3_endpoint_url: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_use_ssl: Optional[bool] = None,
    ):
        """
        Initialize IFCB client.

        Args:
            base_url: Base URL of the IFCB service (e.g., "http://localhost:8001")
            timeout: Request timeout in seconds
            max_retries: Number of retries for failed requests
            debug: Print debug output for each step when True
            s3_endpoint_url: Optional override for S3 endpoint (defaults to env S3_ENDPOINT_URL)
            s3_access_key: Optional S3 access key (defaults to env S3_ACCESS_KEY)
            s3_secret_key: Optional S3 secret key (defaults to env S3_SECRET_KEY)
            s3_use_ssl: Force HTTPS when connecting to S3 (defaults to env S3_USE_SSL or False)
        """
        self.base_url = base_url.rstrip('/')
        self.client = httpx.Client(
            timeout=timeout,
            transport=httpx.HTTPTransport(retries=max_retries),
        )
        self.debug = debug
        self._s3_endpoint_url = s3_endpoint_url or os.getenv("S3_ENDPOINT_URL")
        self._s3_access_key = s3_access_key or os.getenv("S3_ACCESS_KEY")
        self._s3_secret_key = s3_secret_key or os.getenv("S3_SECRET_KEY")
        if s3_use_ssl is not None:
            self._s3_use_ssl = s3_use_ssl
        else:
            env_ssl = os.getenv("S3_USE_SSL")
            self._s3_use_ssl = env_ssl.lower() == "true" if env_ssl else False
        self._s3_client: Optional[Any] = None

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
    ) -> JobSubmitResponse:
        """
        Submit a processing job.

        Provide either manifest_uri (S3 path) or manifest_inline (dict/object).

        Args:
            manifest_uri: S3 URI to manifest file
            manifest_inline: Inline manifest data

        Returns:
            Job submission response with job_id

        Raises:
            ValueError: If neither or both manifests provided
            APIError: If API returns an error

        Example:
            >>> manifest = Manifest(files=[
            ...     "s3://bucket/test.adc",
            ...     "s3://bucket/test.roi",
            ...     "s3://bucket/test.hdr"
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
            >>> print(result.result.payload)
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
        files: List[Dict[str, Any]],
    ) -> IngestStartResponse:
        """
        Start multipart upload for files.

        Args:
            files: List of dicts with keys:
                  - 'filename': Original filename
                  - 'size_bytes': File size in bytes

        Returns:
            Ingest response with presigned URLs for each file

        Example:
            >>> response = client.start_ingest([
            ...     {"filename": "test.adc", "size_bytes": 1000000},
            ...     {"filename": "test.roi", "size_bytes": 5000000},
            ...     {"filename": "test.hdr", "size_bytes": 5000},
            ... ])
            >>> print(response.job_id)
        """
        if not files:
            raise ValueError("Must provide at least one file to start ingest")

        payload = {"files": files}

        response = self._request("POST", "/ingest/start", json=payload)
        ingest_response = IngestStartResponse(**response.json())

        self._debug(
            f"Start ingest succeeded: job_id={ingest_response.job_id}, files={len(ingest_response.files)}"
        )
        for file_info in ingest_response.files:
            self._debug(
                f"  file {file_info.filename}: file_id={file_info.file_id}, "
                f"upload_id={file_info.upload_id}, parts={len(file_info.part_urls)}"
            )

        return ingest_response

    def complete_ingest(
        self,
        job_id: str,
        file_id: str,
        upload_id: str,
        parts: List[Dict[str, Any]],
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
        return self.upload_bins({bin_id: file_paths})

    def upload_bins(self, bins: Dict[str, Dict[str, Path]]) -> str:
        """Upload multiple bins in a single ingest workflow."""

        if not bins:
            raise ValueError("No bins provided")

        # Flatten all files into a single list
        file_entries = []
        local_file_map: Dict[str, Path] = {}  # filename -> local path

        for bin_id, file_paths in bins.items():
            validate_bin_files(file_paths)
            self._debug(f"Preparing bin {bin_id}")

            for ext, path in file_paths.items():
                self._debug(f"  file {path.name}: size_bytes={path.stat().st_size}")
                if path.name in local_file_map:
                    raise UploadError(f"Duplicate filename {path.name} found")
                file_entries.append({
                    "filename": path.name,
                    "size_bytes": path.stat().st_size,
                })
                local_file_map[path.name] = path

        ingest_response = self.start_ingest(file_entries)

        if not ingest_response.files:
            raise UploadError("Ingest response did not include any files")

        with httpx.Client() as upload_client:
            for file_info in ingest_response.files:
                local_path = local_file_map.get(file_info.filename)
                if not local_path:
                    raise UploadError(f"Local file for {file_info.filename} not found")

                self._debug(
                    f"Uploading file {file_info.filename} "
                    f"(file_id={file_info.file_id}, upload_id={file_info.upload_id})"
                )

                part_size, _ = calculate_part_size(local_path.stat().st_size)
                completed_parts = []

                with open(local_path, 'rb') as file_handle:
                    for part in file_info.part_urls:
                        part_number = part.part_number
                        chunk = file_handle.read(part_size)
                        chunk_len = len(chunk)

                        try:
                            upload_response = upload_client.put(part.url, content=chunk)
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

                        self._debug(
                            f"    uploaded part {part_number} ({chunk_len} bytes) "
                            f"status={upload_response.status_code} etag={etag}"
                        )

                self.complete_ingest(
                    job_id=ingest_response.job_id,
                    file_id=file_info.file_id,
                    upload_id=file_info.upload_id,
                    parts=completed_parts,
                )

        self._debug(f"Upload flow completed for job_id={ingest_response.job_id}")
        return ingest_response.job_id

    def upload_bins_from_directory(
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

        return self.upload_bins(discovered)

    # ============================================================================
    # Results Download
    # ============================================================================

    def _ensure_s3_client(self):
        """Create and cache a boto3 S3 client using configured credentials."""
        if self._s3_client is not None:
            return self._s3_client

        client_kwargs: Dict[str, Any] = {
            "config": Config(signature_version="s3v4"),
        }
        credentials: Dict[str, Any] = {}

        if self._s3_endpoint_url:
            client_kwargs["endpoint_url"] = self._s3_endpoint_url
        if self._s3_use_ssl is not None:
            client_kwargs["use_ssl"] = self._s3_use_ssl
        if self._s3_access_key:
            credentials["aws_access_key_id"] = self._s3_access_key
        if self._s3_secret_key:
            credentials["aws_secret_access_key"] = self._s3_secret_key

        self._s3_client = boto3.client("s3", **client_kwargs, **credentials)
        return self._s3_client

    @staticmethod
    def _parse_s3_uri(uri: str) -> tuple[str, str]:
        """Split an s3:// URI into (bucket, key)."""
        if not uri.startswith("s3://"):
            raise ValueError(f"Unsupported URI (expected s3://): {uri}")
        parts = uri[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI: {uri}")
        return parts[0], parts[1]

    @staticmethod
    def _relative_key(key: str) -> Path:
        """
        Derive a local relative path from an S3 key.

        Drops the leading results/ or datasets/ prefix if present so downloads
        land under <output>/<job_id>/...
        """
        parts = key.split("/", 1)
        if len(parts) == 2 and parts[0] in {"results", "datasets"}:
            return Path(parts[1])
        return Path(key)

    def _download_from_s3(self, uri: str, output_dir: Path, overwrite: bool) -> Path:
        """Download a single S3 object to the output directory."""
        bucket, key = self._parse_s3_uri(uri)
        relative_path = self._relative_key(key)
        local_path = output_dir / relative_path

        if local_path.exists() and not overwrite:
            self._debug(f"Skipping existing file {local_path}")
            return local_path

        local_path.parent.mkdir(parents=True, exist_ok=True)

        s3_client = self._ensure_s3_client()
        self._debug(f"Downloading {uri} -> {local_path}")

        try:
            s3_client.download_file(bucket, key, str(local_path))
        except ClientError as exc:
            raise DownloadError(f"Failed to download {uri}: {exc}") from exc

        return local_path

    def download_file(
        self,
        uri: str,
        output_dir: Path | str,
        overwrite: bool = False,
    ) -> Path:
        """
        Download a single file from S3.

        Args:
            uri: S3 URI (s3://bucket/key) to download
            output_dir: Local directory where file will be stored
            overwrite: Overwrite existing file if it already exists

        Returns:
            Path to the downloaded file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return self._download_from_s3(uri, output_dir, overwrite)

    def download_files(
        self,
        uris: List[str],
        output_dir: Path | str,
        overwrite: bool = False,
    ) -> List[Path]:
        """
        Download multiple files from S3.

        Args:
            uris: List of S3 URIs (s3://bucket/key) to download
            output_dir: Local directory where files will be stored
            overwrite: Overwrite existing files if they already exist

        Returns:
            List of paths to the downloaded files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded = []
        for uri in uris:
            local_path = self._download_from_s3(uri, output_dir, overwrite)
            downloaded.append(local_path)
        return downloaded
