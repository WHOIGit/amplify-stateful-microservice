# IFCB Microservice Base

Shared infrastructure package for building IFCB algorithm microservices.

## What's Included

- **S3 Storage**: Multipart upload/download with presigned URLs
- **Job Management**: Queue, status tracking, results storage
- **Worker Pool**: Async background processing
- **API Routes**: FastAPI endpoints for health, jobs, and ingest
- **Output Writers**: Parquet and WebDataset formatters
- **BaseProcessor**: Abstract interface for algorithm implementation

## Usage

### 1. Install the base package

```bash
pip install -e /path/to/base
```

### 2. Implement a processor

```python
from ifcb_microservice import BaseProcessor
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, List

class MyProcessor(BaseProcessor):
    @property
    def name(self) -> str:
        return "my-algorithm"

    def process_bin(
        self,
        bin_id: str,
        bin_files: Dict[str, Path]
    ) -> Tuple[pd.DataFrame, List]:
        # Your algorithm here
        results = []
        for roi in self._read_rois(bin_files):
            result = self._process_roi(roi)
            results.append(result)

        return pd.DataFrame(results), []
```

### 3. Create the FastAPI app

```python
from ifcb_microservice import create_app
from .processor import MyProcessor

app = create_app(MyProcessor())
```

### 4. Run it

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

That's it! All infrastructure (S3, jobs, uploads, workers) is handled automatically.

## Environment Variables

Required configuration:

```bash
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
S3_BUCKET=your-bucket
S3_PREFIX=ifcb-data/
MAX_CONCURRENT_JOBS=4
```

See `config.py` for full configuration options.
