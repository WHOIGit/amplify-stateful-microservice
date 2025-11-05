"""Utilities for building direct-response IFCB microservices."""

from __future__ import annotations

import asyncio
import io
from typing import Any, Awaitable, Callable

from fastapi import HTTPException, Response

from .storage import s3_client


def _parse_s3_uri(uri: str) -> str:
    if not uri.startswith("s3://"):
        raise HTTPException(status_code=400, detail=f"Invalid S3 URI: {uri}")
    parts = uri[5:].split("/", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=400, detail=f"Invalid S3 URI format: {uri}")
    bucket, key = parts
    if bucket != s3_client.bucket:
        raise HTTPException(
            status_code=400,
            detail=f"URI bucket {bucket} does not match configured bucket {s3_client.bucket}",
        )
    if not key:
        raise HTTPException(status_code=400, detail=f"Missing key in S3 URI: {uri}")
    return key


async def fetch_s3_bytes(uri: str) -> bytes:
    """
    Download object bytes for the given S3 URI.

    Raises HTTPException with status 404 if object missing.
    """

    key = _parse_s3_uri(uri)

    def _download() -> bytes:
        buffer = io.BytesIO()
        s3_client.download_fileobj(key, buffer)
        buffer.seek(0)
        return buffer.read()

    try:
        return await asyncio.to_thread(_download)
    except Exception as exc:  # pragma: no cover - boto3 exceptions not easily modeled
        raise HTTPException(status_code=404, detail=f"Failed to fetch {uri}: {exc}") from exc


async def run_blocking(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """
    Run a blocking function in a thread pool to avoid blocking the event loop.
    """

    return await asyncio.to_thread(func, *args, **kwargs)


def render_bytes(payload: bytes | bytearray | memoryview, media_type: str) -> Response:
    """
    Shortcut for returning binary payloads from direct actions.
    """

    return Response(content=bytes(payload), media_type=media_type)


__all__ = ["fetch_s3_bytes", "run_blocking", "render_bytes"]
