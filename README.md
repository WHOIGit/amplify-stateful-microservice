# Amplify Stateful Microservices

This repo captures everything required to run long-running, queue-backed processing APIs. It contains the generic production library (`stateful_microservice/`), a reference IFCB features processor (`examples/ifcb_features_service/`) that demonstrates how to plug in domain logic, and the accompanying Python client used by the IFCB tooling to submit ingest requests and poll for job completion.

## Developing a New Microservice

1. Implement `stateful_microservice.BaseProcessor` for your algorithm. `process_input` receives a `JobInput` containing the job ID plus the list of downloaded local file paths. Return an instance of your processor’s `result_model` after you’ve uploaded whatever outputs/artifacts your service produces (return `None` only if you’re deferring completion until a later invocation).
   - Inspect `job_input.local_paths` (string paths) and decide how to open/process each file for your domain. The worker downloads all files for an input into a temporary directory and cleans it up after `process_input` returns.
   - Use the optional helpers in `stateful_microservice.output_writers` (e.g., `JsonlResultUploader`, `WebDatasetUploader`, `write_results_index`) to serialize structured data or artifacts to S3 before returning your result model. Those helpers are entirely opt-in; processors can always manage their own uploads manually.
   - Set `result_model` on your processor (defaults to `DefaultResult`) so you have a typed schema for whatever completion payload you emit. Whatever you return is serialized and exposed under `job.result.payload` for clients to consume.
2. Call `stateful_microservice.create_app(processor, ServiceConfig(...))` to get a FastAPI instance that exposes `/ingest/*`, `/jobs`, and `/health`.
3. Run workers via `stateful_microservice.create_worker_pool(processor)` when deploying outside of the FastAPI lifespan context.
4. Use the code in `examples/ifcb_features_service/` as a template for Dockerfiles, `.env` files, and configuration.

Artifacts returned from `process_input` must already be serialized as bytes (PNG, npy, etc.). The framework uploads them directly without additional conversion.

## Client Usage

The `client/` folder is ready to publish separately so automation can programmatically:

- Open multipart uploads via `/ingest/start`
- Upload domain-specific payloads directly to S3
- Submit manifests and poll `/jobs/{id}`
- Download result indexes once processing completes

The framework provides optional helpers for emitting JSON Lines (`JsonlResultUploader`) and WebDataset artifact shards (`WebDatasetUploader`). Processors are free to use these, implement their own serialization (e.g., Parquet), or return only metadata in their result model.

Install from this repo by pointing `pip` at the packaged wheel or using editable installs during development.
