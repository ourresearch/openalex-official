"""Utility functions and enums for the OpenAlex content downloader."""

from __future__ import annotations

from enum import Enum
from pathlib import Path


class ContentFormat(Enum):
    """Supported content formats."""

    NONE = "none"  # Metadata only, no content files
    PDF = "pdf"
    XML = "xml"
    BOTH = "both"


class StorageType(Enum):
    """Supported storage backends."""

    LOCAL = "local"
    S3 = "s3"


def work_id_to_path(work_id: str, extension: str, nested: bool = False) -> Path:
    """
    Convert a work ID to a file path.

    Args:
        work_id: The OpenAlex work ID (e.g., W2741809807)
        extension: File extension (e.g., 'pdf', 'json')
        nested: If True, use nested structure (W27/41/W2741809807.pdf)
                If False (default), use flat structure (W2741809807.pdf)

    The nested structure prevents filesystem issues with millions of files.
    """
    if nested:
        # Remove the W prefix for path calculation
        numeric_part = work_id[1:] if work_id.startswith("W") else work_id

        # Use first 2 and next 2 digits for nesting
        # Pad with zeros if needed
        numeric_part = numeric_part.zfill(4)
        level1 = f"W{numeric_part[:2]}"
        level2 = numeric_part[2:4]

        return Path(level1) / level2 / f"{work_id}.{extension}"
    else:
        # Flat: just the filename
        return Path(f"{work_id}.{extension}")


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


def parse_identifier(value: str) -> tuple[str, str]:
    """
    Parse an identifier and return (type, normalized_value).

    Types:
    - 'openalex': OpenAlex work ID
    - 'doi': Digital Object Identifier

    Accepts:
    - W2741809807 -> ('openalex', 'W2741809807')
    - https://openalex.org/W2741809807 -> ('openalex', 'W2741809807')
    - 2741809807 -> ('openalex', 'W2741809807')
    - 10.1038/nature12373 -> ('doi', '10.1038/nature12373')
    - https://doi.org/10.1038/nature12373 -> ('doi', '10.1038/nature12373')
    """
    value = value.strip()

    # DOI detection: starts with "10." or is a doi.org URL
    if value.startswith("https://doi.org/"):
        doi = value.replace("https://doi.org/", "")
        return ("doi", doi)
    if value.startswith("http://doi.org/"):
        doi = value.replace("http://doi.org/", "")
        return ("doi", doi)
    if value.startswith("10."):
        return ("doi", value)

    # OpenAlex ID handling
    if value.startswith("https://openalex.org/"):
        value = value.split("/")[-1]
    if value.startswith("W"):
        return ("openalex", value)
    if value.isdigit():
        return ("openalex", f"W{value}")

    raise ValueError(f"Cannot parse identifier: {value}")


def doi_to_filename(doi: str) -> str:
    """
    Convert a DOI to a safe filename.

    Example: 10.1038/nature12373 -> 10.1038_nature12373

    DOIs can contain characters that are invalid in filenames,
    so we replace / and : with underscores.
    """
    return doi.replace("/", "_").replace(":", "_")


def format_bytes(num_bytes: float) -> str:
    """Format bytes as human-readable string."""
    value = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


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
