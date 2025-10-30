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
         ┌────────────┼────────────┐
         │            │            │
    ┌────┴────┐  ┌────┴────┐  ┌────┴────┐
    │Features │  │Classifier│  │Segment  │
    │:8001    │  │:8002     │  │:8003    │
    └─────────┘  └──────────┘  └─────────┘
```

## Quick Start

### 1. Configuration

Copy the example config:

```bash
cd fastapi-uvicorn
cp services/features/env.example services/features/.env
```

Edit `.env` with your S3 settings:

```bash
S3_ENDPOINT_URL=http://your-s3-server:9000
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=ifcb-features
```

### 2. Run with Docker Compose

```bash
cd fastapi-uvicorn

# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f features

# Stop services
docker-compose down
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
```

## Available Services

### Features Service (Port 8001)

Extracts morphological features from IFCB ROI images using the [ifcb-features](https://github.com/WHOIGit/ifcb-features) library.

**Output:**
- ~241 feature columns per ROI
- Blob segmentation masks
- Parquet files + WebDataset TAR archives

**Documentation:** See `services/features/README.md`

### Future Services

- **Classifier** (Port 8002) - Species classification
- **Segmentation** (Port 8003) - Advanced segmentation

## Creating a New Algorithm Service

To add a new service, implement the `BaseProcessor` interface:

### 1. Create service directory

```bash
mkdir -p services/my-algorithm/app
```

### 2. Implement the processor

**`services/my-algorithm/app/processor.py`**

```python
from ifcb_microservice import BaseProcessor
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, List

class MyProcessor(BaseProcessor):
    @property
    def name(self) -> str:
        return "my-algorithm"

    @property
    def version(self) -> str:
        return "1.0.0"

    def process_bin(
        self,
        bin_id: str,
        bin_files: Dict[str, Path]
    ) -> Tuple[pd.DataFrame, List]:
        """
        Process IFCB bin files.

        Args:
            bin_id: Bin identifier
            bin_files: {'.adc': Path, '.roi': Path, '.hdr': Path}

        Returns:
            (results_dataframe, artifacts_list)
        """
        # Validate required files
        self.validate_bin_files(bin_files, {'.adc', '.roi', '.hdr'})

        # Your algorithm here
        results = []
        for roi in self._read_rois(bin_files):
            result = self._process_roi(roi)
            results.append(result)

        df = pd.DataFrame(results)
        self.validate_output(df)

        return df, []  # No artifacts for this example
```

### 3. Create the app

**`services/my-algorithm/app/main.py`**

```python
from ifcb_microservice import create_app
from .processor import MyProcessor

app = create_app(MyProcessor())
```

### 4. Define dependencies

**`services/my-algorithm/pyproject.toml`**

```toml
[project]
name = "ifcb-my-algorithm-service"
version = "1.0.0"
dependencies = [
    "ifcb-microservice-base",
    # Your algorithm's dependencies
    "torch>=2.0.0",
    "torchvision>=0.15.0",
]
```

### 5. Create Dockerfile

**`services/my-algorithm/Dockerfile`**

```dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

# Install base framework
COPY ../../base /tmp/base
RUN cd /tmp/base && uv pip install --system .

# Install service dependencies
COPY pyproject.toml ./
RUN uv pip install --system .

# Copy service code
COPY ./app /app/app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 6. Add to docker-compose.yml

```yaml
  my-algorithm:
    build:
      context: .
      dockerfile: services/my-algorithm/Dockerfile
    ports:
      - "8004:8000"
    environment:
      - S3_ENDPOINT_URL=${S3_ENDPOINT_URL}
      - S3_ACCESS_KEY=${S3_ACCESS_KEY}
      - S3_SECRET_KEY=${S3_SECRET_KEY}
      - S3_BUCKET=${S3_BUCKET}
```

Done! All infrastructure (S3, jobs, uploads, workers, API) is handled automatically.

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

- `POST /ingest/start` - Start multipart upload
- `POST /ingest/complete` - Complete file upload

Full API documentation available at `http://localhost:800X/docs` (FastAPI auto-generated).

## Development

### Running locally without Docker

```bash
# Install base package
cd base
uv pip install -e .

# Install service
cd ../services/features
uv pip install -e .
uv pip install git+https://github.com/WHOIGit/ifcb-features.git

# Run
uvicorn app.main:app --reload
```

### Testing

```bash
# Test the processor directly
python -c "
from services.features.app.processor import FeaturesProcessor
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
