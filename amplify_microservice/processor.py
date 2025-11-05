"""Base processor interface for IFCB batch processors and direct-action utilities."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel


@dataclass
class DirectAction:
    """
    Definition of a direct-response API route backed by a processor method.

    Attributes:
        name: Short identifier used for logging and OpenAPI docs.
        path: FastAPI route path (e.g., "/transform").
        handler: Callable invoked with the processor instance and validated payload.
        request_model: Pydantic model for request validation.
        response_model: Optional Pydantic model for response serialization.
        methods: HTTP methods to expose (defaults to POST).
        summary: Optional OpenAPI summary.
        description: Optional longer description.
        tags: Optional OpenAPI tags.
        media_type: Optional override for response media type.
    """

    name: str
    path: str
    handler: Callable[["BaseProcessor", BaseModel], Awaitable[Any] | Any]
    request_model: type[BaseModel]
    response_model: type[BaseModel] | None = None
    methods: tuple[str, ...] = ("POST",)
    summary: str | None = None
    description: str | None = None
    tags: tuple[str, ...] | None = None
    media_type: str | None = None


class BaseProcessor(ABC):
    """Shared hook point for queued IFCB jobs and generic direct actions.

    - Batch/IFCB services implement :meth:`process_bin` and may also expose direct routes.
    - Pure direct-response utilities can rely solely on :meth:`get_direct_actions` and
      leave :meth:`process_bin` unimplemented (raise NotImplementedError).
    """

    @abstractmethod
    def process_bin(
        self,
        bin_id: str,
        bin_files: Dict[str, Path]
    ) -> Tuple[pd.DataFrame, List[Any]]:
        """
        Process a single IFCB bin.

        This is the only method you need to implement for your algorithm.

        Args:
            bin_id: Bin identifier (e.g., "D20230101T120000_IFCB123")
            bin_files: Dict mapping file extension to local file path
                      Example: {
                          '.adc': Path('/tmp/D20230101T120000_IFCB123.adc'),
                          '.roi': Path('/tmp/D20230101T120000_IFCB123.roi'),
                          '.hdr': Path('/tmp/D20230101T120000_IFCB123.hdr')
                      }

        Returns:
            Tuple of:
                - DataFrame: Results with one row per ROI. Must include 'roi_number' column.
                - List[Any]: Optional artifacts (e.g., masks, segmentations, images).
                            Can be empty list if algorithm produces no artifacts.

        Raises:
            ValueError: If bin cannot be processed
            FileNotFoundError: If required files are missing
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Processor name (e.g., 'features', 'classifier', 'segmentation').

        This is used for logging and identification.
        """
        pass

    @property
    def version(self) -> str:
        """
        Processor version string.

        Override this to specify your algorithm version.
        Default: '1.0.0'
        """
        return "1.0.0"

    def get_output_schema(self) -> Dict[str, str]:
        """
        Optional: Define the expected output schema for the DataFrame.

        This is used for validation and documentation.

        Returns:
            Dict mapping column name to type hint
            Example: {
                'roi_number': 'int64',
                'area': 'float64',
                'perimeter': 'float64',
                'class': 'string'
            }

        Default: {} (no schema validation)
        """
        return {}

    def validate_bin_files(self, bin_files: Dict[str, Path], required_extensions: set) -> None:
        """
        Helper method to validate that required files are present.

        Args:
            bin_files: Dict of available files
            required_extensions: Set of required extensions (e.g., {'.adc', '.roi', '.hdr'})

        Raises:
            ValueError: If required files are missing
        """
        missing = required_extensions - set(bin_files.keys())
        if missing:
            raise ValueError(
                f"Missing required files: {missing}. "
                f"Available: {set(bin_files.keys())}"
            )

    def validate_output(self, df: pd.DataFrame) -> None:
        """
        Helper method to validate output DataFrame.

        Args:
            df: Output DataFrame to validate

        Raises:
            ValueError: If DataFrame is invalid
        """
        if df.empty:
            raise ValueError("Output DataFrame is empty")

        if 'roi_number' not in df.columns:
            raise ValueError("Output DataFrame must include 'roi_number' column")

        # Validate against schema if provided
        schema = self.get_output_schema()
        if schema:
            missing_cols = set(schema.keys()) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing expected columns: {missing_cols}")

    # ==========================================================================
    # Progress Reporting
    # ==========================================================================

    def set_progress_callback(self, callback: Optional[Callable[[Dict[str, Any]], None]]) -> None:
        """
        Set a progress callback used during processing.

        Args:
            callback: Callable receiving progress payloads, or None to disable.
        """
        self._progress_callback = callback  # type: ignore[attr-defined]

    def report_progress(self, stage: str, **data: Any) -> None:
        """
        Report progress for the current processing operation.

        Args:
            stage: Progress stage identifier
            **data: Additional metadata for progress payload
        """
        callback: Optional[Callable[[Dict[str, Any]], None]] = getattr(self, "_progress_callback", None)
        if not callback:
            return

        payload = {"stage": stage}
        payload.update(data)
        callback(payload)

    # ==========================================================================
    # Direct Actions
    # ==========================================================================

    def get_direct_actions(self) -> List[DirectAction]:
        """
        Return the list of direct-response actions provided by this processor.

        Override in subclasses to expose synchronous microservice endpoints.
        Default: no direct actions.
        """
        return []
