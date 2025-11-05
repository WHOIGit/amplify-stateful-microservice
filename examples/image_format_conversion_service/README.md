# Image Format Conversion Direct Service

Direct-response FastAPI service (non-IFCB example) that exposes one helper endpoint:

- `POST /media/image/convert` — Fetch an image from S3, optionally convert mode, and re-encode to a new format.

The service demonstrates how to use `ServiceConfig(enable_async_jobs=False, enable_direct_actions=True)` together with `DirectAction` descriptors to expose immediate responses without the job queue.

## Request Examples

```bash
# Convert an image from TIFF to PNG
curl -X POST http://localhost:8010/media/image/convert \
  -H "Content-Type: application/json" \
  --output frame.png \
  -d '{
        "source_uri": "s3://media-tools/examples/sample.tiff",
        "target_format": "png"
      }'
```

## Run with Docker Compose

```bash
cd examples/image_format_conversion_service
cp .env.example .env    # edit with your S3 credentials
docker compose up --build
```

The container listens on port 8010. Shut it down with `docker compose down`.
