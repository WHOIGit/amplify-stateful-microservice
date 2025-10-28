"""IFCB Microservice Base - Shared infrastructure for IFCB algorithm services."""

from .processor import BaseProcessor
from .api import create_app
from .worker import create_worker_pool
from .config import settings

__version__ = "1.0.0"

__all__ = [
    "BaseProcessor",
    "create_app",
    "create_worker_pool",
    "settings",
]
