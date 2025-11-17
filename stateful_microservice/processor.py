"""Base processor interface for asynchronous job inputs."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel


@dataclass
class JobInput:
    """Runtime context passed to processors."""

    job_id: str
    local_paths: List[str]


class DefaultResult(BaseModel):
    """Default result model when processors don't define their own."""

    status: str = "ok"


class BaseProcessor(ABC):
    """Base class for job processors."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Processor name (e.g., 'features', 'classifier')."""
        raise NotImplementedError

    @property
    def version(self) -> str:
        """Processor version string."""
        return "1.0.0"

    @abstractmethod
    def process_input(self, job_input: JobInput) -> BaseModel:
        """
        Process job input files and return results.

        Args:
            job_input: Job context with local file paths

        Returns:
            Result as a Pydantic BaseModel (custom or DefaultResult)

        Raises:
            ValueError: If input cannot be processed
            FileNotFoundError: If required files are missing
        """
        raise NotImplementedError

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
