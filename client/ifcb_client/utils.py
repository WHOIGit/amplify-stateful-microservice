"""Utility functions for IFCB client."""

import time
from typing import Callable, TypeVar, Optional
from pathlib import Path

T = TypeVar('T')


def calculate_part_size(file_size: int, min_part_size: int = 5 * 1024 * 1024) -> tuple[int, int]:
    """
    Calculate optimal part size and number of parts for multipart upload.

    S3 limits: 5MB minimum part size, 10,000 max parts.

    Args:
        file_size: Total file size in bytes
        min_part_size: Minimum part size (default 5MB)

    Returns:
        Tuple of (part_size, num_parts)
    """
    max_parts = 10000

    # Start with minimum part size
    part_size = min_part_size
    num_parts = (file_size + part_size - 1) // part_size  # Ceiling division

    # If too many parts, increase part size
    if num_parts > max_parts:
        part_size = (file_size + max_parts - 1) // max_parts
        num_parts = (file_size + part_size - 1) // part_size

    return part_size, num_parts


def poll_until(
    check_fn: Callable[[], T],
    condition_fn: Callable[[T], bool],
    interval: float = 5.0,
    timeout: Optional[float] = None,
    timeout_message: str = "Operation timed out"
) -> T:
    """
    Poll a function until a condition is met.

    Args:
        check_fn: Function to call repeatedly
        condition_fn: Function that returns True when done
        interval: Seconds between checks
        timeout: Maximum seconds to wait (None = no timeout)
        timeout_message: Error message if timeout occurs

    Returns:
        Result from check_fn when condition is met

    Raises:
        TimeoutError: If timeout is exceeded
    """
    start_time = time.time()

    while True:
        result = check_fn()

        if condition_fn(result):
            return result

        if timeout is not None:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(timeout_message)

        time.sleep(interval)


def validate_bin_files(file_paths: dict[str, Path]) -> None:
    """
    Validate that bin files exist and have correct extensions.

    Args:
        file_paths: Dict mapping extension to file path

    Raises:
        ValueError: If files are invalid
        FileNotFoundError: If files don't exist
    """
    required_extensions = {'.adc', '.roi', '.hdr'}
    provided_extensions = set(file_paths.keys())

    # Check all required files are present
    missing = required_extensions - provided_extensions
    if missing:
        raise ValueError(f"Missing required files: {missing}")

    # Check files exist
    for ext, path in file_paths.items():
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if not path.is_file():
            raise ValueError(f"Not a file: {path}")
