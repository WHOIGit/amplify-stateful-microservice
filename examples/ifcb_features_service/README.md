# IFCB Features Service Quick Start

### 1. Configuration

Copy the example config:

```bash
cp .env.example .env
```

Edit `.env` with your S3 settings:

```bash
S3_ENDPOINT_URL=http://your-s3-server:9000
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=your-s3-bucket
```

### 2. Run with Docker Compose

```bash
# Build and start the features service
docker compose up -d

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


