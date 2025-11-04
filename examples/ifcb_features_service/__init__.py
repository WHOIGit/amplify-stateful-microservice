"""IFCB features service package."""

from .main import app
from .processor import FeaturesProcessor

__all__ = ["app", "FeaturesProcessor"]
