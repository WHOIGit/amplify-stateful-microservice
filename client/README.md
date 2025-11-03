# IFCB Client

Python client library for IFCB microservices.

Works with **any** IFCB algorithm service (features, classifier, segmentation, etc.) since they all expose the same API.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from ifcb_client import IFCBClient, Manifest, BinManifestEntry

# Create a client
client = IFCBClient("http://localhost:8001")

# Submit a job
manifest = Manifest(bins=[
    BinManifestEntry(
        bin_id="D20230101T120000_IFCB123",
        files=[
            "s3://bucket/data/D20230101T120000_IFCB123.adc",
            "s3://bucket/data/D20230101T120000_IFCB123.roi",
            "s3://bucket/data/D20230101T120000_IFCB123.hdr",
        ],
        bytes=5000000,
    )
])

job = client.submit_job(manifest_inline=manifest)

# Wait for completion
result = client.wait_for_job(job.job_id)

print(f"Processed {result.result.counts.rois} ROIs")
print(f"Features: {result.result.features.uris}")

client.close()
```

## Features

### ✅ Synchronous Client
- Simple, blocking API
- Perfect for scripts and notebooks

### ✅ Async Client
- For concurrent operations
- Process multiple bins in parallel

### ✅ Complete API Coverage
- Job submission and status
- Multipart file uploads
- Health checks
- Job listing

### ✅ Convenience Methods
- `wait_for_job()` - Poll until complete
- `upload_bin()` - Handle entire upload workflow

### ✅ Type Hints & Validation
- Pydantic models for all responses
- Full type checking support

### ✅ Error Handling
- Custom exceptions for different error cases
- Automatic retries for network issues

## Usage

### Working with Multiple Services

The client works with **any** IFCB service:

```python
# Features extraction
features_client = IFCBClient("http://localhost:8001")
features_job = features_client.submit_job(manifest_uri="s3://bucket/manifest.json")

# Classification
classifier_client = IFCBClient("http://localhost:8002")
classifier_job = classifier_client.submit_job(manifest_uri="s3://bucket/manifest.json")

# Same API, different algorithms!
```

### Uploading Local Files

```python
from pathlib import Path

# Upload bin files from local disk
job_id = client.upload_bin(
    bin_id="D20230101T120000_IFCB123",
    file_paths={
        '.adc': Path('/data/test.adc'),
        '.roi': Path('/data/test.roi'),
        '.hdr': Path('/data/test.hdr'),
    }
)

result = client.wait_for_job(job_id)
```

### Uploading All Bins From A Directory

```python
from pathlib import Path

job_id = client.upload_bins_from_directory(
    Path("/data/ifcb"),
    recursive=True,
    skip_incomplete=False,
)

print(f"Queued job {job_id} for directory upload")
result = client.wait_for_job(job_id)
```

### Async Usage

```python
import asyncio
from ifcb_client import AsyncIFCBClient

async def process_bins():
    async with AsyncIFCBClient("http://localhost:8001") as client:
        # Submit multiple jobs concurrently
        jobs = await asyncio.gather(
            client.submit_job(manifest_uri="s3://bucket/bin1.json"),
            client.submit_job(manifest_uri="s3://bucket/bin2.json"),
            client.submit_job(manifest_uri="s3://bucket/bin3.json"),
        )

        # Wait for all to complete
        results = await asyncio.gather(
            *[client.wait_for_job(job.job_id) for job in jobs]
        )

        return results

asyncio.run(process_bins())
```

### Context Manager

```python
# Automatic cleanup
with IFCBClient("http://localhost:8001") as client:
    job = client.submit_job(manifest_uri="s3://bucket/manifest.json")
    result = client.wait_for_job(job.job_id)
    # Client automatically closed
```

### Error Handling

```python
from ifcb_client import (
    IFCBClient,
    JobFailedError,
    JobTimeoutError,
    JobNotFoundError
)

client = IFCBClient("http://localhost:8001")

try:
    job = client.submit_job(manifest_uri="s3://bucket/manifest.json")
    result = client.wait_for_job(job.job_id, timeout=600)

except JobFailedError as e:
    print(f"Job {e.job_id} failed: {e.error_message}")

except JobTimeoutError as e:
    print(f"Job {e.job_id} did not complete within {e.timeout}s")

except JobNotFoundError:
    print("Job not found")

client.close()
```

### Downloading Results

```python
from pathlib import Path

downloads = client.download_results(
    job_id,
    output_dir=Path("./ifcb-results") / job_id,
    overwrite=True,
)

for category, files in downloads.items():
    for file_path in files:
        print(category, file_path)
```

## API Reference

### IFCBClient

**Methods:**

- `health()` - Check service health
- `submit_job(manifest_uri=..., manifest_inline=..., callback_url=..., parameters=...)` - Submit job
- `get_job(job_id)` - Get job status
- `list_jobs(limit=50)` - List recent jobs
- `wait_for_job(job_id, poll_interval=5, timeout=3600)` - Poll until complete
- `start_ingest(bins)` - Start multipart upload for one or more bins
- `complete_ingest(job_id, bin_id, file_id, upload_id, parts)` - Complete upload for a file within a bin
- `upload_bin(bin_id, file_paths)` - Upload and process bin files
- `upload_bins(bins)` - Upload a mapping of bin IDs to local files in one job
- `upload_bins_from_directory(root, recursive=True, skip_incomplete=False)` - Discover and upload bins under a directory
- `download_results(job_id, output_dir, include_features=True, include_masks=True, include_index=True, overwrite=False)` - Fetch artifacts from S3

### AsyncIFCBClient

Same methods as `IFCBClient`, but async:

```python
await client.health()
await client.submit_job(...)
await client.wait_for_job(...)
```

### Models

- `HealthResponse` - Health check result
- `JobStatus` - Job status and results
- `JobSubmitResponse` - Job submission response
- `JobResult` - Processing results with URIs
- `Manifest` - Job manifest
- `BinManifestEntry` - Single bin in manifest

### Exceptions

- `IFCBClientError` - Base exception
- `JobNotFoundError` - Job doesn't exist
- `JobFailedError` - Job processing failed
- `JobTimeoutError` - Job didn't complete in time
- `UploadError` - File upload failed
- `APIError` - API returned an error
- `NetworkError` - Network/connection error

## Examples

See the `examples/` directory:

- `basic_usage.py` - Simple synchronous example
- `async_example.py` - Process multiple bins concurrently
- `upload_bin.py` - Upload local files

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```
