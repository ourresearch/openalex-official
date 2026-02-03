"""Local filesystem storage backend."""

from __future__ import annotations

import asyncio
from pathlib import Path

from .base import StorageBackend


class LocalStorage(StorageBackend):
    """Store content on the local filesystem."""

    def __init__(self, base_path: str | Path):
        """
        Initialize local storage.

        Args:
            base_path: Base directory for storing files
        """
        self.base_path = Path(base_path).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def save(
        self,
        path: str,
        content: bytes,
        content_type: str | None = None,
    ) -> None:
        """Save content to a local file."""
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Use asyncio to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_file, full_path, content)

    def _write_file(self, path: Path, content: bytes) -> None:
        """Synchronous file write."""
        path.write_bytes(content)

    async def exists(self, path: str) -> bool:
        """Check if a file exists."""
        full_path = self.base_path / path
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, full_path.exists)

    async def close(self) -> None:
        """No cleanup needed for local storage."""
        pass

    def get_full_path(self, path: str) -> str:
        """Get the absolute filesystem path."""
        return str(self.base_path / path)
