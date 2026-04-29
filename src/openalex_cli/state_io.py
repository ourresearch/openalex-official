"""Helpers for durable local state writes."""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any


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

        os.replace(tmp_path, path)
    except Exception:
        with suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


def atomic_write_json(path: Path, data: Any) -> None:
    """Atomically replace a JSON file."""
    atomic_write_bytes(path, json.dumps(data, indent=2).encode("utf-8"))
