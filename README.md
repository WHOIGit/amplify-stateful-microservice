# Amplify Stateful Microservice Toolkit

This repo captures everything required to run long-running, queue-backed IFCB processing APIs. It contains the production library (`stateful_microservice/`), the reference IFCB features processor (`examples/ifcb_features_service/`), and the accompanying Python client used to submit ingest requests and poll for job completion.

## Developing a New Jobs Microservice

1. Implement `stateful_microservice.BaseProcessor` for your algorithm, ensuring `process_bin` returns a DataFrame (features) and optional artifacts.
2. Call `stateful_microservice.create_app(processor, ServiceConfig(...))` to get a FastAPI instance that exposes `/ingest/*`, `/jobs`, and `/health`.
3. Run workers via `stateful_microservice.create_worker_pool(processor)` when deploying outside of the FastAPI lifespan context.
4. Use the code in `examples/ifcb_features_service/` as a template for Dockerfiles, `.env` files, and configuration.

## Client Usage

The `client/` folder is ready to publish separately so automation can programmatically:

- Open multipart uploads via `/ingest/start`
- Upload IFCB bins directly to S3
- Submit manifests and poll `/jobs/{id}`
- Download result indexes once processing completes

Install from this repo by pointing `pip` at the packaged wheel or using editable installs during development.
