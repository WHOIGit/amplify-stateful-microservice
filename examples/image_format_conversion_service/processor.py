"""Direct-response image conversion processor."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from fastapi import HTTPException
from PIL import Image
from pydantic import BaseModel, Field

from ifcb_microservice import BaseProcessor, DirectAction
from ifcb_microservice.direct import fetch_s3_bytes, render_bytes, run_blocking

logger = logging.getLogger(__name__)


class ImageConvertRequest(BaseModel):
    """Request payload for image format conversion."""

    source_uri: str = Field(..., description="Input image S3 URI")
    target_format: str = Field(
        ...,
        pattern="^[A-Za-z0-9]+$",
        description="Target image format (PNG, JPEG, TIFF, ...)",
    )
    mode: str | None = Field(
        None,
        description="Optional Pillow mode conversion (RGB, L, ...)",
    )


class ImageConvertProcessor(BaseProcessor):
    """Processor exposing direct-response image conversion."""

    @property
    def name(self) -> str:
        return "image-convert"

    def process_bin(self, bin_id: str, bin_files: Dict[str, Path]) -> Tuple[Any, List[Any]]:
        raise NotImplementedError("Async job processing is not supported for this service.")

    def get_direct_actions(self) -> List[DirectAction]:
        return [
            DirectAction(
                name="image_convert",
                path="/media/image/convert",
                request_model=ImageConvertRequest,
                handler=self.handle_image_convert,
                summary="Convert an S3-hosted image to another format",
                description="Downloads an image from S3, optionally converts mode, and re-encodes it to the requested format.",
                tags=("media",),
                media_type="application/octet-stream",
            ),
        ]

    async def handle_image_convert(self, payload: ImageConvertRequest):
        """Convert an image to the requested format."""

        raw_bytes = await fetch_s3_bytes(payload.source_uri)

        def _convert() -> bytes:
            with Image.open(io.BytesIO(raw_bytes)) as img:
                if payload.mode:
                    img = img.convert(payload.mode)
                buffer = io.BytesIO()
                img.save(buffer, format=payload.target_format.upper())
                buffer.seek(0)
                return buffer.read()

        try:
            converted = await run_blocking(_convert)
        except ValueError as exc:
            logger.error("Failed to convert image %s: %s", payload.source_uri, exc)
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        media_type = f"image/{payload.target_format.lower()}"
        return render_bytes(converted, media_type)
