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
    filename: str = Field(..., description="Name of the file (e.g., file1.adc)")
    size_bytes: int = Field(..., description="File size in bytes", gt=0)


class IngestStartRequest(BaseModel):
    """Request to start ingesting a bin (3 files)."""
    bin_id: str = Field(..., description="Unique bin identifier")
    files: List[FileUploadSpec] = Field(..., description="List of 3 files to upload", min_length=3, max_length=3)


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
    job_id: str = Field(..., description="Job ID for this bin")
    bin_id: str = Field(..., description="Bin identifier")
    files: List[FileUploadInfo] = Field(..., description="Upload info for each file")
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

class BinManifestEntry(BaseModel):
    """Single bin entry in a manifest."""
    bin_id: str = Field(..., description="Bin identifier")
    files: List[str] = Field(..., description="List of 3 S3 URIs (s3://bucket/key)", min_length=3, max_length=3)
    bytes: int = Field(..., description="Total size in bytes")
    sha256: Optional[str] = Field(None, description="Optional checksum")


class Manifest(BaseModel):
    """Collection of bins to process."""
    bins: List[BinManifestEntry] = Field(..., description="List of bins")


# ============================================================================
# Job Models
# ============================================================================

class JobParameters(BaseModel):
    """Optional processing parameters."""
    batch_size: Optional[int] = Field(32, description="Batch size for processing")


class JobSubmitRequest(BaseModel):
    """Request to submit a processing job."""
    manifest_uri: Optional[str] = Field(None, description="S3 URI to manifest file (s3://bucket/key)")
    manifest_inline: Optional[Manifest] = Field(None, description="Inline manifest (for small jobs)")
    parameters: Optional[JobParameters] = Field(default_factory=JobParameters)
    callback_url: Optional[str] = Field(None, description="Webhook URL for completion notification")
    idempotency_key: Optional[str] = Field(None, description="Idempotency key to prevent duplicate processing")


class JobSubmitResponse(BaseModel):
    """Response after submitting a job."""
    job_id: str = Field(..., description="Unique job ID")
    status: Literal["queued", "processing"] = Field(..., description="Initial job status")
    created_at: datetime = Field(..., description="Job creation timestamp")


class FeaturesOutput(BaseModel):
    """Information about features output."""
    format: Literal["parquet"] = Field(default="parquet")
    uris: List[str] = Field(..., description="List of S3 URIs for Parquet files")
    schema: Dict[str, str] = Field(..., description="Column name to type mapping")


class MasksShard(BaseModel):
    """Information about a single masks shard."""
    uri: str = Field(..., description="S3 URI to TAR shard")
    index_uri: str = Field(..., description="S3 URI to JSON index for this shard")


class MasksOutput(BaseModel):
    """Information about masks output."""
    format: Literal["webdataset"] = Field(default="webdataset")
    shards: List[MasksShard] = Field(..., description="List of TAR shards")


class JobCounts(BaseModel):
    """Job processing counts."""
    bins: int = Field(..., description="Number of bins processed")
    rois: int = Field(..., description="Total number of ROIs")
    masks: int = Field(..., description="Total number of masks")


class JobResult(BaseModel):
    """Results index for a completed job."""
    job_id: str = Field(..., description="Job ID")
    features: FeaturesOutput = Field(..., description="Features output information")
    masks: MasksOutput = Field(..., description="Masks output information")
    counts: JobCounts = Field(..., description="Processing counts")


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
