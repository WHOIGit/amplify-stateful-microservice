"""Amplify Microservice Base - shared infrastructure for IFCB batch jobs and generic direct-response services."""

from .processor import BaseProcessor, DirectAction
from .api import create_app, ServiceConfig
from .config import settings
from .direct import fetch_s3_bytes, run_blocking, render_bytes
from .apache_conf import ApacheConfigParams, generate_apache_vhost_config

__version__ = "1.0.0"


def create_worker_pool(*args, **kwargs):
    """Lazily import worker pool support so direct-only services avoid heavy deps."""
    from .worker import create_worker_pool as _create_worker_pool

    return _create_worker_pool(*args, **kwargs)


__all__ = [
    "BaseProcessor",
    "DirectAction",
    "create_app",
    "create_worker_pool",
    "ServiceConfig",
    "settings",
    "fetch_s3_bytes",
    "run_blocking",
    "render_bytes",
    "ApacheConfigParams",
    "generate_apache_vhost_config",
]
