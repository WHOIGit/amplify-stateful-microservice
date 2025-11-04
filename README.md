# IFCB Microservices Architecture

Scalable, flexible microservice architecture for IFCB data processing algorithms.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│     ifcb-microservice-base (shared package)     │
│  • S3 upload/download                           │
│  • Job management                               │
│  • Worker pool                                  │
│  • API routes (health, jobs, ingest)            │
│  • BaseProcessor interface                      │
└─────────────────────────────────────────────────┘
                      ▲
         ┌────────────┼─────────────┐
         │            │             │
    ┌────┴────┐  ┌────┴─────┐  ┌────┴────┐
    │Features │  │Classifier│  │Segment  │
    │:8001    │  │:8002     │  │:8003    │
    └─────────┘  └──────────┘  └─────────┘
```

Repository layout:

- `ifcb_microservice/` – reusable infrastructure (ingest API, S3 orchestration, job store, workers)
- `examples/ifcb_features_service/` – reference implementation of the features processor using the framework
- `client/` – Python SDK and CLI helpers (multipart upload, directory ingestion, job polling)

## Quick Start

### 1. Configuration

Copy the example config:

```bash
cp examples/ifcb_features_service/.env.example examples/ifcb_features_service/.env
```

If you plan to run the API directly with `uvicorn` from the project root, copy the file to `.env` at the root (or export the variables manually).

Edit `.env` with your S3 settings:

```bash
S3_ENDPOINT_URL=http://your-s3-server:9000
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=ifcb-features
```

### 2. Run with Docker Compose

```bash
cd examples/ifcb_features_service

# Build and start the features service
docker compose up -d

# View logs
docker compose logs -f features

# Stop services
docker compose down
```

### 3. Test the API

```bash
# Health check
curl http://localhost:8001/health

# Submit a job
curl -X POST http://localhost:8001/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "manifest_inline": {
      "bins": [{
        "bin_id": "D20230101T120000_IFCB123",
        "files": [
          "s3://ifcb-features/data/D20230101T120000_IFCB123.adc",
          "s3://ifcb-features/data/D20230101T120000_IFCB123.roi",
          "s3://ifcb-features/data/D20230101T120000_IFCB123.hdr"
        ],
        "bytes": 1234567
      }]
    }
  }'

# Check job status
curl http://localhost:8001/jobs/{job_id}

# Or upload bins via the client helper
python client/examples/upload_bin.py /path/to/bin-directory
```

## Available Services

### Features Service Example (Port 8001)

Extracts morphological features from IFCB ROI images using the [ifcb-features](https://github.com/WHOIGit/ifcb-features) library.

**Output:**
- ~241 feature columns per ROI
- Blob segmentation masks
- Parquet files + WebDataset TAR archives

**Location:** `examples/ifcb_features_service/`

**Documentation:** See `examples/ifcb_features_service/README.md`

### Future Services

- **Classifier** (Port 8002) - Species classification
- **Segmentation** (Port 8003) - Advanced segmentation

## Creating a New Algorithm Service

The shared `ifcb_microservice` package lets you wrap any IFCB algorithm in the same REST/S3 workflow. To create a new service:

1. **Create a package** (e.g., `my_algorithm_service/`).
2. **Subclass `BaseProcessor`** and implement `process_bin` to emit a `pandas.DataFrame` plus optional artifacts.
3. **Expose the API** with a minimal `main.py`:

   ```python
   from ifcb_microservice import create_app
   from .processor import MyProcessor

   app = create_app(MyProcessor())

   if __name__ == "__main__":
       import uvicorn
       uvicorn.run(app, host="0.0.0.0", port=8000)
   ```

4. **Package & deploy**: list dependencies in `pyproject.toml`, build a Docker image similar to the provided `Dockerfile`, and run under Uvicorn. The framework handles ingest endpoints, multipart uploads, background workers, and job state so you focus only on algorithm logic.

## API Documentation

All services expose the same REST API:

### Health Endpoints

- `GET /` - Root health check
- `GET /health` - Detailed health status

### Job Endpoints

- `POST /jobs` - Submit a processing job
- `GET /jobs/{job_id}` - Get job status
- `GET /jobs` - List recent jobs

### Ingest Endpoints (Multipart Upload)

- `POST /ingest/start` - Start multipart upload for one or more bins
- `POST /ingest/complete` - Complete file upload

Full API documentation available at `http://localhost:800X/docs` (FastAPI auto-generated).

## Development

### Running locally without Docker

```bash
# Install the microservice locally (base + features example)
uv pip install -e .
uv pip install git+https://github.com/WHOIGit/ifcb-features.git

# Run the example API
uvicorn examples.ifcb_features_service.main:app --reload
```

Ensure the required environment variables are set before running locally (for convenience you can copy `examples/ifcb_features_service/.env.example` to `.env` in the project root).

### Testing

```bash
# Test the example processor directly
python -c "
from examples.ifcb_features_service.processor import FeaturesProcessor
from pathlib import Path

processor = FeaturesProcessor()
results_df, artifacts = processor.process_bin(
    'test_bin',
    {
        '.adc': Path('test.adc'),
        '.roi': Path('test.roi'),
        '.hdr': Path('test.hdr'),
    }
)
print(results_df.head())
"
```
