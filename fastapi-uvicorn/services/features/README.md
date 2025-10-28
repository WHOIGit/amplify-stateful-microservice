# IFCB Features Extraction Service

Microservice for extracting morphological features from IFCB data using the [ifcb-features](https://github.com/WHOIGit/ifcb-features) library.

## What it does

- Extracts ~241 morphological features per ROI
- Generates blob segmentation masks
- Outputs Parquet files and WebDataset TAR archives

## Running the service

### With Docker

```bash
# Build
docker build -t ifcb-features-service .

# Run
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  -e S3_BUCKET=your-bucket \
  ifcb-features-service
```

### Local development

```bash
# Install dependencies
uv pip install -e ../../base
uv pip install -e .
uv pip install git+https://github.com/WHOIGit/ifcb-features.git

# Run
uvicorn app.main:app --reload
```

## API

See the parent README for full API documentation. Key endpoints:

- `GET /health` - Health check
- `POST /jobs` - Submit a processing job
- `GET /jobs/{job_id}` - Get job status
- `POST /ingest/start` - Start multipart upload
- `POST /ingest/complete` - Complete file upload

## Implementation

This service demonstrates the minimal code needed for an IFCB algorithm microservice:

**app/processor.py** (~100 lines) - Algorithm-specific logic
```python
class FeaturesProcessor(BaseProcessor):
    def process_bin(self, bin_id, bin_files):
        # Extract features using ifcb-features
        return features_df, masks_list
```

**app/main.py** (~5 lines) - Service entry point
```python
from ifcb_microservice import create_app
from .processor import FeaturesProcessor

app = create_app(FeaturesProcessor())
```

Everything else (S3, jobs, workers, API routes) is provided by `ifcb-microservice-base`.
