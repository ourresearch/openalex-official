"""Filter file parsing and management for multi-filter downloads."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .api_client import OpenAlexAPIClient


def _generate_filter_id(filter_str: str) -> str:
    """Generate a stable 8-character hash ID from a filter string."""
    return hashlib.sha256(filter_str.encode("utf-8")).hexdigest()[:8]


def parse_filters_file(file_path: str | Path) -> list[dict]:
    """Parse a filter file (.txt or .json) and return a list of filter configs.

    Args:
        file_path: Path to the filter file

    Returns:
        List of filter configurations with keys: id, name, filter

    Raises:
        ValueError: If file format is unsupported or content is invalid
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".txt":
        return _parse_txt_file(path)
    elif suffix == ".json":
        return _parse_json_file(path)
    else:
        raise ValueError(
            f"Unsupported filter file format: '{suffix}'. "
            f"Use .txt (one filter per line) or .json (structured format)."
        )


def _parse_txt_file(path: Path) -> list[dict]:
    """Parse a plain text filter file.

    Format:
        # This is a comment
        publication_year:>2020,topics.id:T123
        # Another comment
        publication_year:2024,type:article
    """
    filters = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            # Strip whitespace
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Generate ID and name
            filter_id = _generate_filter_id(line)
            name = f"filter_{i:03d}"

            filters.append(
                {
                    "id": filter_id,
                    "name": name,
                    "filter": line,
                }
            )

    if not filters:
        raise ValueError(
            f"No valid filters found in {path}. File is empty or contains only comments."
        )

    return filters


def _parse_json_file(path: Path) -> list[dict]:
    """Parse a JSON filter file.

    Expected format:
    {
        "filters": [
            {
                "id": "abc123",      # Optional, auto-generated if missing
                "name": "my_filter",  # Optional, auto-generated if missing
                "filter": "publication_year:>2020"
            }
        ]
    }
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(
            f"Invalid JSON format in {path}: expected object, got {type(data).__name__}"
        )

    raw_filters = data.get("filters", [])
    if not isinstance(raw_filters, list):
        raise ValueError(f"Invalid JSON format in {path}: 'filters' must be an array")

    if not raw_filters:
        raise ValueError(f"No filters found in {path}. 'filters' array is empty.")

    filters = []
    seen_ids = set()
    for i, raw_filter in enumerate(raw_filters, 1):
        if not isinstance(raw_filter, dict):
            raise ValueError(f"Invalid filter at index {i} in {path}: expected object")

        filter_str = raw_filter.get("filter")
        if not filter_str:
            raise ValueError(f"Filter at index {i} in {path}: missing 'filter' field")

        # Auto-generate ID if missing
        filter_id = raw_filter.get("id")
        if not filter_id:
            filter_id = _generate_filter_id(filter_str)

        # Check for duplicate IDs
        if filter_id in seen_ids:
            raise ValueError(
                f"Duplicate filter ID '{filter_id}' at index {i} in {path}. "
                f"Each filter must have a unique 'id' field."
            )
        seen_ids.add(filter_id)

        # Auto-generate name if missing
        name = raw_filter.get("name")
        if not name:
            name = f"filter_{i:03d}"

        filters.append(
            {
                "id": filter_id,
                "name": name,
                "filter": filter_str,
            }
        )

    return filters


def auto_convert_txt_to_json(txt_path: str | Path) -> Path:
    """Auto-convert a .txt filter file to a .json sidecar file.

    The generated JSON includes metadata and auto-generated names/IDs.
    Users can edit the JSON later to customize names.

    Args:
        txt_path: Path to the .txt filter file

    Returns:
        Path to the generated .json file
    """
    txt_path = Path(txt_path)
    json_path = txt_path.with_suffix(".json")

    filters = parse_filters_file(txt_path)

    data = {
        "source": txt_path.name,
        "auto_generated": True,
        "note": (
            "This file was auto-generated from a .txt filter file. "
            "You can edit 'name' fields freely. "
            "If you edit a 'filter' field, that filter will be treated as new and progress will reset."
        ),
        "filters": filters,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return json_path


async def validate_filters(
    filters: list[dict],
    api_key: str,
    progress_tracker=None,
) -> list[dict]:
    """Validate filter strings by checking them against the OpenAlex API.

    Args:
        filters: List of filter configurations
        api_key: OpenAlex API key
        progress_tracker: Optional progress tracker for logging warnings

    Returns:
        List of valid filters (invalid ones are logged and excluded)
    """
    client = OpenAlexAPIClient(api_key=api_key)
    valid_filters = []
    warnings = []

    try:
        for filter_config in filters:
            filter_str = filter_config["filter"]
            name = filter_config.get("name", "unnamed")

            try:
                # Check if filter returns any results
                count = await client.get_work_count(filter_str=filter_str)
                if count is not None and count > 0:
                    valid_filters.append(filter_config)
                else:
                    msg = f"Filter '{name}' returned no results, skipping: {filter_str}"
                    warnings.append(msg)
                    if progress_tracker:
                        progress_tracker.log_warning(msg)
            except Exception as e:
                msg = f"Filter '{name}' validation failed, skipping: {e}"
                warnings.append(msg)
                if progress_tracker:
                    progress_tracker.log_warning(msg)
    finally:
        await client.close()

    # If no progress tracker, print warnings at end
    if not progress_tracker and warnings:
        print("\n".join(warnings))

    return valid_filters
