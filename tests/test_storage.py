"""Tests for storage backends."""

import tempfile
from pathlib import Path

import pytest

from openalex_content.storage.local import LocalStorage


class TestLocalStorage:
    @pytest.fixture
    def storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield LocalStorage(tmpdir)

    async def test_save_and_exists(self, storage):
        await storage.save("test/file.pdf", b"test content")
        assert await storage.exists("test/file.pdf")
        assert not await storage.exists("test/other.pdf")

    async def test_nested_directories(self, storage):
        await storage.save("W27/41/W2741809807.pdf", b"pdf content")
        assert await storage.exists("W27/41/W2741809807.pdf")

    async def test_get_full_path(self, storage):
        path = storage.get_full_path("test/file.pdf")
        assert path.endswith("test/file.pdf")
        assert Path(path).is_absolute()

    async def test_overwrite_existing(self, storage):
        await storage.save("test.pdf", b"original")
        await storage.save("test.pdf", b"updated")

        full_path = Path(storage.get_full_path("test.pdf"))
        assert full_path.read_bytes() == b"updated"
