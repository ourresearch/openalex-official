"""Utility functions and enums for the OpenAlex content downloader."""

from __future__ import annotations

from enum import Enum
from pathlib import Path


class ContentFormat(Enum):
    """Supported content formats."""

    PDF = "pdf"
    XML = "xml"
    BOTH = "both"


class StorageType(Enum):
    """Supported storage backends."""

    LOCAL = "local"
    S3 = "s3"


def work_id_to_path(work_id: str, extension: str) -> Path:
    """
    Convert a work ID to a nested path structure.

    Example: W2741809807 -> W27/41/W2741809807.pdf

    This prevents filesystem issues with millions of files in one directory.
    """
    # Remove the W prefix for path calculation
    numeric_part = work_id[1:] if work_id.startswith("W") else work_id

    # Use first 2 and next 2 digits for nesting
    # Pad with zeros if needed
    numeric_part = numeric_part.zfill(4)
    level1 = f"W{numeric_part[:2]}"
    level2 = numeric_part[2:4]

    return Path(level1) / level2 / f"{work_id}.{extension}"


def parse_work_id(identifier: str) -> str:
    """
    Parse a work identifier into a clean work ID.

    Accepts:
    - W2741809807
    - https://openalex.org/W2741809807
    - 2741809807

    Returns: W2741809807
    """
    # Strip whitespace
    identifier = identifier.strip()

    # Handle full URL
    if identifier.startswith("https://openalex.org/"):
        identifier = identifier.replace("https://openalex.org/", "")

    # Add W prefix if missing
    if not identifier.startswith("W"):
        identifier = f"W{identifier}"

    return identifier


def format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def format_rate(bytes_per_second: float) -> str:
    """Format download rate as human-readable string."""
    return f"{format_bytes(int(bytes_per_second))}/s"


def format_count(count: int) -> str:
    """Format large numbers with K/M suffixes."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.1f}K"
    return str(count)
