"""Abstract base class for storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """Abstract interface for content storage."""

    @abstractmethod
    async def save(
        self,
        path: str,
        content: bytes,
        content_type: str | None = None,
    ) -> None:
        """
        Save content to the storage backend.

        Args:
            path: Relative path for the file (e.g., "W27/41/W2741809807.pdf")
            content: File content as bytes
            content_type: Optional MIME type
        """
        pass

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if a file exists at the given path."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up any resources."""
        pass

    @abstractmethod
    def get_full_path(self, path: str) -> str:
        """Get the full path/URI for a file."""
        pass
