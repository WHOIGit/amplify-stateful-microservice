# Amplify Microservices Architecture

Scalable architecture for IFCB processing microservices and general direct-response microservices.

## Architecture Overview

```
┌──────────────────────────────────────────────┐
│            amplify-microservice              │
│  • S3 upload/download                        │
│  • Job management (IFCB batch)               │
│  • Worker pool                               │
│  • Direct action helpers (generic)           │
│  • API routes (health, jobs, ingest, direct) │
│  • BaseProcessor interface                   │
└──────────────────────────────────────────────┘
                ▲                       ▲
         ┌──────┴──────┐                │
         │             │                | 
    ┌────┴────┐   ┌────┴────┐      ┌────┴────┐
    │Features │   │Segment  │      │Convert  │
    │:8001    │   │:8003    │      │Img :8010│
    └─────────┘   └─────────┘      └─────────┘
          
         IFCB batch jobs         Generic direct APIs
```

Repository layout:

- `ifcb_microservice/` – reusable infrastructure (ingest API, S3 orchestration, job store, workers, direct-action helpers)
- `examples/ifcb_features_service/` – reference implementation of the features processor using the framework
- `examples/image_format_conversion_service/` – direct-response microservice example (generic image conversion)
- `client/` – Python library for interacting with IFCB processing microservices (multipart upload, directory ingestion, job polling)

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
- 30 morphology/texture features per ROI
- Blob segmentation masks
- Parquet files + WebDataset TAR archives

**Location:** `examples/ifcb_features_service/`

**Documentation:** See `examples/ifcb_features_service/README.md`

### Image Format Conversion Direct Service (Port 8010)

Instant image format conversion built with the direct-action plumbing (no job queue). This example shows how to use the base library for non-IFCB utilities.

**Endpoint:**
- `POST /media/image/convert` – convert an image stored in S3 to a new format (returns bytes)

**Location:** `examples/image_format_conversion_service/`

**Documentation:** See `examples/image_format_conversion_service/README.md`

**Docker:** `cd examples/image_format_conversion_service && cp .env.example .env && docker compose up --build`

### Future Services

- **Classifier** (Port 8002) - Species classification
- **Segmentation** (Port 8003) - Advanced segmentation

## Creating a New Algorithm Service

The shared `ifcb_microservice` package lets you wrap any IFCB algorithm in the same REST/S3 workflow. To create a new service:

1. **Create a package** (e.g., `my_algorithm_service/`).
2. **Subclass `BaseProcessor`**:
   - Override `process_bin` for long-running batch jobs, **and/or**
   - Override `get_direct_actions` to return `DirectAction` descriptors for instant endpoints (can be IFCB-specific or totally generic).
3. **Expose the API** with a minimal `main.py`:

   ```python
   from ifcb_microservice import ServiceConfig, create_app
   from .processor import MyProcessor

   config = ServiceConfig(
       enable_async_jobs=True,          # queue-backed jobs
       enable_direct_actions=True      # immediate responses
   )

   app = create_app(MyProcessor(), config)

   if __name__ == "__main__":
       import uvicorn
       uvicorn.run(app, host="0.0.0.0", port=8000)
   ```

4. **Package & deploy**: list dependencies in `pyproject.toml`, build a Docker image similar to the provided `Dockerfile`, and run under Uvicorn. The framework handles ingest endpoints, multipart uploads, background workers, and job state so you focus only on algorithm logic.

## API Documentation

All services expose health endpoints. Job-enabled services include ingest + job APIs, while direct services expose whatever `DirectAction` routes the processor registers.

### Health Endpoints

- `GET /` - Root health check
- `GET /health` - Detailed health status

### Job Endpoints (when `enable_async_jobs=True`)

- `POST /jobs` - Submit a processing job
- `GET /jobs/{job_id}` - Get job status
- `GET /jobs` - List recent jobs

### Ingest Endpoints (Multipart Upload)

- `POST /ingest/start` - Start multipart upload for one or more bins
- `POST /ingest/complete` - Complete file upload

### Direct Action Endpoints

Direct services surface custom endpoints based on the `DirectAction` definitions returned by the processor. These routes can operate on IFCB or non-IFCB data and participate in OpenAPI docs automatically (see `/docs`).

Full API documentation available at `http://localhost:800X/docs` (FastAPI auto-generated).
