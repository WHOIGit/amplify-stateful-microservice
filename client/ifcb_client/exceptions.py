"""Custom exceptions for IFCB client."""


class IFCBClientError(Exception):
    """Base exception for all IFCB client errors."""
    pass


class JobNotFoundError(IFCBClientError):
    """Job was not found on the server."""
    pass


class JobFailedError(IFCBClientError):
    """Job processing failed on the server."""

    def __init__(self, job_id: str, error_message: str):
        self.job_id = job_id
        self.error_message = error_message
        super().__init__(f"Job {job_id} failed: {error_message}")


class JobTimeoutError(IFCBClientError):
    """Job did not complete within the timeout period."""

    def __init__(self, job_id: str, timeout: int):
        self.job_id = job_id
        self.timeout = timeout
        super().__init__(f"Job {job_id} did not complete within {timeout}s")


class UploadError(IFCBClientError):
    """Error during file upload."""
    pass


class APIError(IFCBClientError):
    """Generic API error."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API error {status_code}: {detail}")


class NetworkError(IFCBClientError):
    """Network/connection error."""
    pass
