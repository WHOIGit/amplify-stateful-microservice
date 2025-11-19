# Amplify Stateful Microservices

A framework for building long-running, queue-backed processing microservices. Submit jobs via S3 manifests or multipart file uploads, process them asynchronously, and poll for completion.

## Quick Start: Creating a Service

### 1. Define Your Processor

```python
from pydantic import BaseModel
from stateful_microservice import BaseProcessor, JobInput

class MyResult(BaseModel):
    """Custom result schema."""
    processed_files: int
    output_uri: str

class MyProcessor(BaseProcessor):
    """Your algorithm implementation."""

    @property
    def name(self) -> str:
        return "my-processor"

    @property
    def version(self) -> str:
        return "1.0.0"

    def process_input(self, job_input: JobInput) -> MyResult:
        """
        Process downloaded files.

        Args:
            job_input: Contains job_id and local_paths (list of file paths)

        Returns:
            Result as a Pydantic model
        """
        # Access downloaded files
        for file_path in job_input.local_paths:
            # Process your files...
            pass

        # Upload results to S3 (you handle this)
        output_uri = "s3://bucket/results/output.json"

        return MyResult(
            processed_files=len(job_input.local_paths),
            output_uri=output_uri
        )
```

### 2. Create the FastAPI App

```python
from stateful_microservice import create_app

# Instantiate your processor
processor = MyProcessor()

# Create the FastAPI app
app = create_app(processor)

# Run with: uvicorn main:app --host 0.0.0.0 --port 8000
```

That's it! Your service now has:
- `POST /jobs` - Submit jobs via manifest
- `GET /jobs/{job_id}` - Check job status
- `POST /ingest/start` - Upload files directly
- `POST /ingest/complete` - Complete multipart upload
- `GET /health` - Health check

### 3. Progress Reporting (Optional)

```python
def process_input(self, job_input: JobInput) -> MyResult:
    total = len(job_input.local_paths)

    for i, file_path in enumerate(job_input.local_paths):
        # Process file...

        # Report progress
        self.report_progress(
            stage="processing",
            percent=(i + 1) / total * 100.0,
            message=f"Processed {i + 1}/{total} files"
        )

    return MyResult(...)
```

## Job Submission Methods

### Option 1: Manifest URI
Files already in S3, reference a manifest file:
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"manifest_uri": "s3://bucket/manifest.json"}'
```

### Option 2: Inline Manifest
Files already in S3, provide list directly:
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "manifest_inline": {
      "files": ["s3://bucket/file1.dat", "s3://bucket/file2.dat"]
    }
  }'
```

### Option 3: Multipart Upload
Upload local files, then process:
```python
from ifcb_client import IFCBClient

client = IFCBClient("http://localhost:8000")
job_id = client.upload_bin(
    bin_id="my-data",
    file_paths={
        '.dat': Path('/local/data.dat'),
        '.hdr': Path('/local/data.hdr'),
    }
)
result = client.wait_for_job(job_id)
```

## Client Usage

Install the client library:
```bash
pip install -e ./client
```

Submit jobs and poll for results:
```python
from ifcb_client import IFCBClient
from pathlib import Path

client = IFCBClient("http://localhost:8000")

# Submit a job
job = client.submit_job(
    manifest_inline={"files": ["s3://bucket/data.dat"]}
)

# Wait for completion
result = client.wait_for_job(job.job_id)

# Access processor-defined result
print(result.result)  # Dict with your processor's output

# Download files if URIs are in the result
uris = result.result.get("output_uris", [])
if uris:
    downloaded = client.download_files(uris, Path("./output"))
```

See `client/README.md` for full documentation.

## Optional Output Helpers

The framework includes helpers for common output patterns:

### JSONL Records
```python
from stateful_microservice.output_writers import JsonlResultUploader

uploader = JsonlResultUploader(job_input.job_id, response_model=MyRecord)
for record in records:
    uploader.add_record(record)
output = uploader.finalize()  # Returns dict with URIs
```

### WebDataset (TAR Shards)
```python
from stateful_microservice.output_writers import WebDatasetUploader

uploader = WebDatasetUploader(job_input.job_id)
for i, artifact in enumerate(artifacts):
    uploader.add_artifact("input-1", f"{i:05d}", artifact, extension=".png")
output = uploader.finalize()  # Returns dict with shard URIs
```

## Configuration

Set via environment variables (`.env` file):

```bash
# S3 Storage
S3_ENDPOINT_URL=http://localhost:9000
S3_BUCKET=my-bucket
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin

# Job Processing
MAX_CONCURRENT_JOBS=3
```

See `stateful_microservice/config.py` for all options.

## Example Service

See `examples/ifcb_features_service/` for an example service that:
- Extracts features from IFCB imagery
- Uses output helpers for JSONL and WebDataset
- Includes Docker setup
- Shows progress reporting
