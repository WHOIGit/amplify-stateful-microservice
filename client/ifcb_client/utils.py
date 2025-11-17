"""Utility functions for IFCB client."""

import time
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Tuple, TypeVar

T = TypeVar('T')

DEFAULT_EXTENSIONS = {'.adc', '.roi', '.hdr'}


def _normalize_extensions(extensions: Iterable[str]) -> set[str]:
    normalized: set[str] = set()
    for ext in extensions:
        if not ext:
            continue
        ext = ext.lower()
        if not ext.startswith('.'):
            ext = f'.{ext}'
        normalized.add(ext)
    return normalized


def calculate_part_size(file_size: int, min_part_size: int = 5 * 1024 * 1024) -> Tuple[int, int]:
    """Calculate optimal part size and number of parts for multipart upload."""
    max_parts = 10000

    part_size = min_part_size
    num_parts = (file_size + part_size - 1) // part_size

    if num_parts > max_parts:
        part_size = (file_size + max_parts - 1) // max_parts
        num_parts = (file_size + part_size - 1) // part_size

    return part_size, num_parts


def poll_until(
    check_fn: Callable[[], T],
    condition_fn: Callable[[T], bool],
    interval: float = 5.0,
    timeout: Optional[float] = None,
    timeout_message: str = "Operation timed out",
) -> T:
    """Poll a function until a condition is met or timeout occurs."""

    start_time = time.time()

    while True:
        result = check_fn()

        if condition_fn(result):
            return result

        if timeout is not None and (time.time() - start_time) >= timeout:
            raise TimeoutError(timeout_message)

        time.sleep(interval)


def validate_bin_files(file_paths: Dict[str, Path], required_extensions: Optional[Iterable[str]] = None) -> None:
    """Validate that bin files exist and contain the required extensions."""

    required = _normalize_extensions(required_extensions or DEFAULT_EXTENSIONS)
    provided_extensions = {ext.lower() for ext in file_paths.keys()}

    missing = required - provided_extensions
    if missing:
        raise ValueError(f"Missing required files: {sorted(missing)}")

    for ext, path in file_paths.items():
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        if not path.is_file():
            raise ValueError(f"Not a file: {path}")


def discover_bins(
    root: Path | str,
    *,
    recursive: bool = True,
    required_extensions: Optional[Iterable[str]] = None,
    skip_incomplete: bool = False,
) -> Dict[str, Dict[str, Path]]:
    """Discover bins under a directory by grouping files that share the same stem."""

    root_path = Path(root)
    if not root_path.exists() or not root_path.is_dir():
        raise ValueError(f"Directory not found: {root_path}")

    required = _normalize_extensions(required_extensions or DEFAULT_EXTENSIONS)

    bin_files: Dict[str, Dict[str, Path]] = {}
    iterator = root_path.rglob('*') if recursive else root_path.glob('*')

    for path in iterator:
        if not path.is_file():
            continue

        ext = path.suffix.lower()
        if ext not in required:
            continue

        bin_id = path.stem
        entry = bin_files.setdefault(bin_id, {})

        if ext in entry:
            raise ValueError(
                f"Duplicate file detected for bin '{bin_id}' with extension '{ext}': {path}"
            )

        entry[ext] = path

    if not bin_files:
        return {}

    missing = {
        bin_id: sorted(required - files.keys())
        for bin_id, files in bin_files.items()
        if required - files.keys()
    }

    if missing:
        if skip_incomplete:
            for bin_id in missing.keys():
                bin_files.pop(bin_id, None)
        else:
            details = "; ".join(
                f"{bin_id} missing {', '.join(exts)}" for bin_id, exts in missing.items()
            )
            raise ValueError(f"Incomplete bins found: {details}")

    return dict(sorted(bin_files.items()))


__all__ = [
    "calculate_part_size",
    "poll_until",
    "validate_bin_files",
    "discover_bins",
]
