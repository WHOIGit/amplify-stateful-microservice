"""Factory for creating storage adapters from configuration."""

import logging
from typing import Optional

from storage import StoreFactory
from storage.s3 import BucketStore

from .storage_adapter import StorageAdapter
from .config import settings

logger = logging.getLogger(__name__)

# Global singleton storage adapter
_storage_adapter: Optional[StorageAdapter] = None


def create_storage_adapter(config_settings=None) -> StorageAdapter:
    """
    Create storage adapter from configuration.

    Priority:
    1. If STORAGE_CONFIG_PATH is set: Load YAML config and use StoreFactory
    2. Otherwise: Create BucketStore from environment variables (backward compatible)

    Args:
        config_settings: Settings object (defaults to global settings)

    Returns:
        Initialized StorageAdapter instance
    """
    if config_settings is None:
        config_settings = settings

    if config_settings.storage_config_path:
        logger.info(f"Loading storage config from: {config_settings.storage_config_path}")

        # Load YAML config using amplify-storage-utils StoreFactory
        factory = StoreFactory(config_settings.storage_config_path)
        store = factory.build(factory.main_store)

        # Initialize the store (context manager entry)
        store.__enter__()

        # Extract bucket name for S3, use default for others
        if isinstance(store, BucketStore):
            bucket_name = store.bucket_name
            logger.info(f"Initialized S3 backend with bucket: {bucket_name}")
        else:
            bucket_name = "default"
            backend_type = type(store).__name__
            logger.info(f"Initialized {backend_type} backend")

        return StorageAdapter(store, bucket_name)
    else:
        logger.info("Using S3 configuration from environment variables")

        # Backward compatibility: Create BucketStore from env vars
        store = BucketStore(
            s3_url=config_settings.s3_endpoint_url,
            s3_access_key=config_settings.s3_access_key,
            s3_secret_key=config_settings.s3_secret_key,
            bucket_name=config_settings.s3_bucket,
        )

        # Initialize the store
        store.__enter__()

        logger.info(f"Initialized S3 backend with bucket: {config_settings.s3_bucket}")
        return StorageAdapter(store, config_settings.s3_bucket)


def get_storage_adapter() -> StorageAdapter:
    """
    Get or create global storage adapter singleton.

    Returns:
        Global StorageAdapter instance
    """
    global _storage_adapter

    if _storage_adapter is None:
        _storage_adapter = create_storage_adapter()

    return _storage_adapter


def reset_storage_adapter() -> None:
    """
    Reset the global storage adapter (useful for testing).

    This allows tests to reconfigure storage between test cases.
    """
    global _storage_adapter
    _storage_adapter = None
