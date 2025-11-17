"""Base processor interface for asynchronous job inputs."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

from pydantic import BaseModel


@dataclass
class JobInput:
    """Runtime context passed to processors."""

    job_id: str
    local_paths: List[str]


class DefaultResult(BaseModel):
    """Empty result model used when processors don't define their own."""

    status: str = "ok"


class BaseProcessor(ABC):
    """Shared hook point for queued long-running jobs."""

    result_model: Type[BaseModel] = DefaultResult

    @abstractmethod
    def process_input(
        self,
        job_input: JobInput,
    ) -> Optional[BaseModel]:
        """
        Process a single logical input payload and return structured results.

        Args:
            job_input: Runtime input data containing the job ID and local file paths

        Returns:
            Instance of `result_model`. Return None to defer emitting job-level
            results until later inputs have been processed.

        Raises:
            ValueError: If input cannot be processed
            FileNotFoundError: If required files are missing
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        """Processor name (e.g., 'features', 'classifier', 'thumbnailer')."""
        raise NotImplementedError

    @property
    def version(self) -> str:
        """Processor version string (override as needed)."""
        return "1.0.0"

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
