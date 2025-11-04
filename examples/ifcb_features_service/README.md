# IFCB Features Service

REST microservice that wraps the `ifcb-features` algorithm using the shared `ifcb_microservice` infrastructure.

## Running locally

```bash
uv pip install -e .
uv pip install git+https://github.com/WHOIGit/ifcb-features.git
uvicorn examples.ifcb_features_service.main:app --reload
```

## Environment variables

See `examples/ifcb_features_service/.env.example` for the full list. Copy it to `.env` in the same directory before using Docker Compose.

The most important values are:

- `S3_ENDPOINT_URL` – S3-compatible endpoint (e.g. MinIO)
- `S3_ACCESS_KEY` / `S3_SECRET_KEY`
- `S3_BUCKET`
- `MAX_CONCURRENT_JOBS`

## Output format

- Feature tables written as Parquet files under `s3://{bucket}/{results_prefix}/{job_id}/features/`
- ROI masks exported as WebDataset shards under `.../masks/`
- A `results.json` index summarising all artefacts and counts

## Extending

To build a different IFCB algorithm service, create your own package with a custom `BaseProcessor` implementation and call `create_app()` exactly like `examples/ifcb_features_service/main.py`.

## Docker Compose

```bash
docker compose up -d
docker compose logs -f features
```
