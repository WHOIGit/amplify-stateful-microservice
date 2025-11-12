"""Helpers for loading optional dependencies when advanced features are used."""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=None)
def require_pandas():
    """Return the pandas module, raising a helpful error if missing."""
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pandas is required for IFCB job processing features. "
            "Install pandas in the environment to enable this functionality."
        ) from exc
    return pd


@lru_cache(maxsize=None)
def require_pyarrow():
    """Return the (pyarrow, pyarrow.parquet) modules, ensuring they are available."""
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pyarrow is required to write Parquet feature outputs. "
            "Install pyarrow in the environment to enable this functionality."
        ) from exc
    return pa, pq


@lru_cache(maxsize=None)
def require_pillow_image():
    """Return Pillow's Image module with a descriptive error if unavailable."""
    try:
        from PIL import Image  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Pillow is required to serialize artifacts as PNGs. "
            "Install Pillow in the environment to enable this functionality."
        ) from exc
    return Image
