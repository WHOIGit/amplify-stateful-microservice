"""IFCB Microservice Base - shared infrastructure for IFCB batch jobs and generic direct-response services."""

from .processor import BaseProcessor, DirectAction
from .api import create_app, ServiceConfig
from .worker import create_worker_pool
from .config import settings
from .direct import fetch_s3_bytes, run_blocking, render_bytes

__version__ = "1.0.0"

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
]
