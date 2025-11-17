"""IFCB Client - Python client library for IFCB microservices."""

from .client import IFCBClient
from .async_client import AsyncIFCBClient
from .models import (
    HealthResponse,
    JobStatus,
    JobSubmitResponse,
    JobResult,
    Manifest,
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
from .utils import discover_bins

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
    # Exceptions
    "IFCBClientError",
    "JobNotFoundError",
    "JobFailedError",
    "JobTimeoutError",
    "UploadError",
    "APIError",
    "NetworkError",
    "DownloadError",
    # Utilities
    "discover_bins",
]
