# Amplify Microservices Architecture

Scalable architecture for IFCB processing microservices and general direct-response microservices.

## Architecture Overview

```
┌──────────────────────────────────────────────┐
│            amplify-microservice              │
│  • S3 upload/download                        │
│  • Job management (IFCB batch)               │
│  • Worker pool                               │
│  • Direct-action microservices (generic)     │
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

- `amplify_microservice/` – reusable infrastructure (ingest API, S3 orchestration, job store, workers, direct-action tooling)
- `examples/ifcb_features_service/` – reference implementation of the features processor using the framework
- `examples/image_format_conversion_service/` – direct-response microservice example (generic image conversion)
- `client/` – Python library for interacting with IFCB processing microservices (multipart upload, directory ingestion, job polling)

## Creating a New Service

The shared `amplify_microservice` package lets you wrap any algorithm in the same REST/S3 workflow. To create a new service:

1. **Create a package** (e.g., `my_algorithm_service/`).
2. **Subclass `BaseProcessor`**:
   - Override `process_bin` for long-running batch jobs, **and/or**
   - Override `get_direct_actions` to return `DirectAction` descriptors for instant endpoints (can be IFCB-specific or totally generic).
3. **Expose the API** with a minimal `main.py`:

   ```python
   from amplify_microservice import ServiceConfig, create_app
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

## Reverse Proxy Config

Generate an Apache vhost snippet for any Amplify microservice:

```bash
python -m amplify_microservice.apache_conf \
  --service white-balance \
  --hostname white-balance.example.com \
  --backend http://127.0.0.1:8015 \
  --output apache/white-balance.conf
```

Add `--https` to emit an HTTPS VirtualHost (with placeholder cert paths). Supply `--path /api/white-balance/` if the service lives behind a prefix, or `--no-virtualhost` when you already manage the `<VirtualHost>` block and just want the proxy directives. The generated file can be dropped into `/etc/apache2/sites-available/` and enabled via `a2ensite`.

Full API documentation available at `http://localhost:800X/docs` (FastAPI auto-generated).

## Available Services

### Features Service Example

Extracts morphological features from IFCB ROI images using the [ifcb-features](https://github.com/WHOIGit/ifcb-features) library.

**Output:**
- 30 morphology/texture features per ROI
- Blob segmentation masks
- Parquet files + WebDataset TAR archives

**Location:** `examples/ifcb_features_service/`

**Documentation:** See `examples/ifcb_features_service/README.md`

### Image Format Conversion Direct Service

Instant image format conversion built with the direct-action plumbing (no job queue). This example shows how to use the base library for non-IFCB utilities.

**Endpoint:**
- `POST /media/image/convert` – convert an image stored in S3 to a new format (returns bytes)

**Location:** `examples/image_format_conversion_service/`

**Documentation:** See `examples/image_format_conversion_service/README.md`

**Docker:** `cd examples/image_format_conversion_service && cp .env.example .env && docker compose up --build`
