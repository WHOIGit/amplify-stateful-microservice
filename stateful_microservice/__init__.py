"""Stateful Microservice Toolkit - reusable infrastructure for long-running batch jobs."""

from .processor import BaseProcessor, JobInput, DefaultResult
from .api import create_app, ServiceConfig
from .config import settings
from .apache_conf import ApacheConfigParams, generate_apache_vhost_config
from .output_writers import JsonlResultUploader, WebDatasetUploader, write_results_index

__version__ = "1.0.0"


def create_worker_pool(*args, **kwargs):
    """Lazily import worker pool support so direct-only services avoid heavy deps."""
    from .worker import create_worker_pool as _create_worker_pool

    return _create_worker_pool(*args, **kwargs)


__all__ = [
    "BaseProcessor",
    "create_app",
    "create_worker_pool",
    "ServiceConfig",
    "settings",
    "ApacheConfigParams",
    "generate_apache_vhost_config",
    "JobInput",
    "DefaultResult",
    "JsonlResultUploader",
    "WebDatasetUploader",
    "write_results_index",
]
