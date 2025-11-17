"""Configuration management using pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # API Settings
    api_title: str = "Stateful Microservice"
    api_version: str = "1.0.0"
    api_description: str = "S3-based async processing framework for long-running jobs"

    # S3 Settings (for local S3-compatible storage like MinIO)
    s3_endpoint_url: str = "http://localhost:9000"  # Your local S3 endpoint
    s3_bucket: str = "stateful-microservice"
    s3_access_key: str = "minioadmin"  # Change via environment variable
    s3_secret_key: str = "minioadmin"  # Change via environment variable
    s3_use_ssl: bool = False  # Set to True if using HTTPS

    # Storage paths (S3 prefixes)
    s3_datasets_prefix: str = "datasets"
    s3_results_prefix: str = "results"

    # Multipart upload settings
    multipart_part_size_mb: int = 5  # S3 minimum is 5MB
    multipart_url_ttl_seconds: int = 7200  # 2 hours

    # Job settings
    max_concurrent_jobs: int = 3
    job_timeout_seconds: int = 3600  # 1 hour

    # Artifact shard settings
    artifact_shard_size_mb: int = 256  # Target size for TAR shards
    artifacts_per_shard_estimate: int = 10000  # Rough estimate for pre-allocation
    responses_chunk_size_mb: int = 64  # Chunk size for JSONL uploads

    # Processing settings
    batch_size: int = 32  # For micro-batching (if needed later)


# Global settings instance
settings = Settings()
