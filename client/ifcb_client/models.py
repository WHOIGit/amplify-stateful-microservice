"""Pydantic models for IFCB API responses."""

from typing import List, Dict, Optional, Literal, Any
from pydantic import BaseModel, Field
from datetime import datetime


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str


class PartUrl(BaseModel):
    """Pre-signed URL for a part upload."""
    part_number: int
    url: str


class FileUploadInfo(BaseModel):
    """Upload information for a single file."""
    file_id: str
    filename: str
    s3_key: str
    upload_id: str
    part_urls: List[PartUrl]


class BinUploadInfo(BaseModel):
    """Upload information for a single bin."""
    bin_id: str
    files: List[FileUploadInfo]


class IngestStartResponse(BaseModel):
    """Response from /ingest/start."""
    job_id: str
    bins: List[BinUploadInfo]
    expires_at: datetime


class IngestCompleteResponse(BaseModel):
    """Response from /ingest/complete."""
    file_id: str
    s3_key: str
    etag: str
    status: str


class JobResult(BaseModel):
    """Job results."""
    job_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class JobStatus(BaseModel):
    """Job status information."""
    job_id: str
    status: Literal["queued", "processing", "completed", "failed"]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    result: Optional[JobResult] = None
    progress: Optional[Dict] = None


class JobSubmitResponse(BaseModel):
    """Response from job submission."""
    job_id: str
    status: Literal["queued", "processing"]
    created_at: datetime


class Manifest(BaseModel):
    """List of files to process in a job."""
    files: List[str]
