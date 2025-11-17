"""Pydantic models for S3-based API request/response validation."""

from typing import List, Dict, Optional, Literal, Any
from pydantic import BaseModel, Field
from datetime import datetime


# ============================================================================
# Health & Error Models
# ============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(default="healthy")
    version: str = Field(default="2.0.0")


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")


# ============================================================================
# Ingest Models (Multipart Upload)
# ============================================================================

class FileUploadSpec(BaseModel):
    """Specification for a single file to upload."""
    filename: str = Field(..., description="Original filename (e.g., input.bin)")
    size_bytes: int = Field(..., description="File size in bytes", gt=0)


class IngestStartRequest(BaseModel):
    """Request to start ingesting files."""
    files: List[FileUploadSpec] = Field(
        ...,
        description="List of files to upload",
        min_length=1,
    )


class PartUrl(BaseModel):
    """Pre-signed URL for a single part."""
    part_number: int = Field(..., description="Part number (1-indexed)")
    url: str = Field(..., description="Pre-signed URL for uploading this part")


class FileUploadInfo(BaseModel):
    """Upload information for a single file."""
    file_id: str = Field(..., description="Unique file identifier")
    filename: str = Field(..., description="Original filename")
    s3_key: str = Field(..., description="S3 object key")
    upload_id: str = Field(..., description="Multipart upload ID")
    part_urls: List[PartUrl] = Field(..., description="Pre-signed URLs for all parts")


class IngestStartResponse(BaseModel):
    """Response with upload information for all files."""
    job_id: str = Field(..., description="Job ID for this ingest request")
    files: List[FileUploadInfo] = Field(
        ...,
        description="Upload info for each file",
    )
    expires_at: datetime = Field(..., description="When the pre-signed URLs expire")


class CompletedPart(BaseModel):
    """Information about a completed part."""
    PartNumber: int = Field(..., description="Part number (must match S3 response)")
    ETag: str = Field(..., description="ETag from S3 upload response")


class IngestCompleteRequest(BaseModel):
    """Request to complete upload for a single file."""
    job_id: str = Field(..., description="Job ID")
    file_id: str = Field(..., description="File ID from start response")
    upload_id: str = Field(..., description="Upload ID from start response")
    parts: List[CompletedPart] = Field(..., description="List of completed parts with ETags")


class IngestCompleteResponse(BaseModel):
    """Response after completing a file upload."""
    file_id: str = Field(..., description="File ID")
    s3_key: str = Field(..., description="S3 object key")
    etag: str = Field(..., description="Final object ETag")
    status: str = Field(default="completed")


# ============================================================================
# Manifest Models
# ============================================================================

class JobManifest(BaseModel):
    """List of files to process in a job."""
    files: List[str] = Field(
        ...,
        description="List of S3 URIs (s3://bucket/key) to process",
        min_length=1,
    )


# ============================================================================
# Job Models
# ============================================================================

class JobSubmitRequest(BaseModel):
    """Request to submit a processing job."""
    manifest_uri: Optional[str] = Field(None, description="S3 URI pointing to a manifest JSON/JSONL file")
    manifest_inline: Optional[JobManifest] = Field(
        None,
        description="Inline manifest (useful for testing or small jobs)",
    )


class JobSubmitResponse(BaseModel):
    """Response after submitting a job."""
    job_id: str = Field(..., description="Unique job ID")
    status: Literal["queued", "processing"] = Field(..., description="Initial job status")
    created_at: datetime = Field(..., description="Job creation timestamp")


class JobResult(BaseModel):
    """Result summary for a completed job."""
    job_id: str = Field(..., description="Job ID")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Processor-defined result payload")


class JobStatus(BaseModel):
    """Job status information."""
    job_id: str = Field(..., description="Job ID")
    status: Literal["queued", "processing", "completed", "failed"] = Field(..., description="Current status")
    created_at: datetime = Field(..., description="Job creation time")
    started_at: Optional[datetime] = Field(None, description="Processing start time")
    completed_at: Optional[datetime] = Field(None, description="Completion time")
    error: Optional[str] = Field(None, description="Error message if failed")
    result: Optional[JobResult] = Field(None, description="Results if completed")
    progress: Optional[Dict[str, Any]] = Field(None, description="Progress information")
