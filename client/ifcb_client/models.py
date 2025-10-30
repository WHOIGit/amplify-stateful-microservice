"""Pydantic models for IFCB API responses."""

from typing import List, Dict, Optional, Literal
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


class IngestStartResponse(BaseModel):
    """Response from /ingest/start."""
    job_id: str
    bin_id: str
    files: List[FileUploadInfo]
    expires_at: datetime


class IngestCompleteResponse(BaseModel):
    """Response from /ingest/complete."""
    file_id: str
    s3_key: str
    etag: str
    status: str


class FeaturesOutput(BaseModel):
    """Features output information."""
    format: Literal["parquet"]
    uris: List[str]
    schema: Dict[str, str]


class MasksShard(BaseModel):
    """Single masks shard."""
    uri: str
    index_uri: str


class MasksOutput(BaseModel):
    """Masks output information."""
    format: Literal["webdataset"]
    shards: List[MasksShard]


class JobCounts(BaseModel):
    """Job processing counts."""
    bins: int
    rois: int
    masks: int


class JobResult(BaseModel):
    """Job results."""
    job_id: str
    features: FeaturesOutput
    masks: MasksOutput
    counts: JobCounts


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


class BinManifestEntry(BaseModel):
    """Single bin entry in a manifest."""
    bin_id: str
    files: List[str]
    bytes: int
    sha256: Optional[str] = None


class Manifest(BaseModel):
    """Collection of bins to process."""
    bins: List[BinManifestEntry]
