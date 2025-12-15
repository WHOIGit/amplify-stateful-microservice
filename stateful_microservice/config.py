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

    # Storage configuration
    storage_config_path: Optional[str] = None  # Path to YAML storage config file

    # S3 Settings (for local S3-compatible storage like MinIO)
    # Used when storage_config_path is not set (backward compatibility)
    s3_endpoint_url: str = "http://localhost:9000"  # Your local S3 endpoint
    s3_bucket: str = "stateful-microservice"
    s3_access_key: str = "minioadmin"  # Change via environment variable
    s3_secret_key: str = "minioadmin"  # Change via environment variable
    s3_use_ssl: bool = False  # Set to True if using HTTPS

    # Storage paths (key prefixes - backend-agnostic)
    s3_datasets_prefix: str = "datasets"
    s3_results_prefix: str = "results"

    # Multipart upload settings (S3-specific)
    multipart_part_size_mb: int = 5  # S3 minimum is 5MB
    multipart_url_ttl_seconds: int = 7200  # 2 hours

    # Simple upload settings (for non-S3 backends)
    simple_upload_max_size_mb: int = 100  # Maximum file size for simple uploads

    # Job settings
    max_concurrent_jobs: int = 3

    # Artifact shard settings
    artifact_shard_size_mb: int = 256  # Target size for TAR shards
    responses_chunk_size_mb: int = 64  # Chunk size for JSONL uploads


# Global settings instance
settings = Settings()
