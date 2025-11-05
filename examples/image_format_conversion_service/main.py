"""FastAPI entrypoint for the image format conversion direct service."""

from amplify_microservice import ServiceConfig, create_app

from .processor import ImageConvertProcessor

config = ServiceConfig(
    enable_async_jobs=False,
    enable_direct_actions=True,
    description="Instant image format conversions over S3 assets.",
)

app = create_app(ImageConvertProcessor(), config)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8010)
