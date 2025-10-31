"""IFCB Client - Python client library for IFCB microservices."""

from .client import IFCBClient
from .async_client import AsyncIFCBClient
from .models import (
    HealthResponse,
    JobStatus,
    JobSubmitResponse,
    JobResult,
    Manifest,
    BinManifestEntry,
)
from .exceptions import (
    IFCBClientError,
    JobNotFoundError,
    JobFailedError,
    JobTimeoutError,
    UploadError,
    APIError,
    NetworkError,
    DownloadError,
)

__version__ = "1.0.0"

__all__ = [
    # Clients
    "IFCBClient",
    "AsyncIFCBClient",
    # Models
    "HealthResponse",
    "JobStatus",
    "JobSubmitResponse",
    "JobResult",
    "Manifest",
    "BinManifestEntry",
    # Exceptions
    "IFCBClientError",
    "JobNotFoundError",
    "JobFailedError",
    "JobTimeoutError",
    "UploadError",
    "APIError",
    "NetworkError",
    "DownloadError",
]
