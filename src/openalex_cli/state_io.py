"""Helpers for durable local state writes."""

from __future__ import annotations

import json
import os
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

WINDOWS_REPLACE_RETRIES = 8
WINDOWS_RETRY_DELAY_SECONDS = 0.05


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Atomically replace a file with the provided bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )

    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())

        _replace_with_retry(tmp_path, path)
    except Exception:
        with suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


def atomic_write_json(path: Path, data: Any) -> None:
    """Atomically replace a JSON file."""
    atomic_write_bytes(path, json.dumps(data, indent=2).encode("utf-8"))


def _replace_with_retry(tmp_path: str, path: Path) -> None:
    """Retry os.replace for transient Windows file-lock contention."""
    attempts = WINDOWS_REPLACE_RETRIES if os.name == "nt" else 1
    last_error: PermissionError | None = None

    for attempt in range(1, attempts + 1):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(WINDOWS_RETRY_DELAY_SECONDS * attempt)

    if last_error is not None:
        raise last_error
