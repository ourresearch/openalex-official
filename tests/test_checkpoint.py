"""Tests for checkpoint system."""

import tempfile

from openalex_cli.checkpoint import Checkpoint, CheckpointManager


class TestCheckpoint:
    def test_to_dict_and_from_dict(self):
        checkpoint = Checkpoint(
            filter_str="publication_year:2024",
            content_format="pdf",
            current_cursor="abc123",
            pages_completed=5,
            completed_work_ids={"W1", "W2", "W3"},
            failed_work_ids={"W4"},
        )
        checkpoint.stats.total_downloaded = 3
        checkpoint.stats.total_failed = 1

        data = checkpoint.to_dict()
        restored = Checkpoint.from_dict(data)

        assert restored.filter_str == "publication_year:2024"
        assert restored.content_format == "pdf"
        assert restored.current_cursor == "abc123"
        assert restored.pages_completed == 5
        assert restored.completed_work_ids == {"W1", "W2", "W3"}
        assert restored.failed_work_ids == {"W4"}
        assert restored.stats.total_downloaded == 3
        assert restored.stats.total_failed == 1


class TestCheckpointManager:
    def test_create_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)

            # Create new checkpoint
            checkpoint = manager.create(
                filter_str="type:article",
                content_format="xml",
            )
            assert checkpoint.filter_str == "type:article"
            assert checkpoint.content_format == "xml"

            # Load should return same data
            manager2 = CheckpointManager(tmpdir)
            loaded = manager2.load()
            assert loaded is not None
            assert loaded.filter_str == "type:article"
            assert loaded.content_format == "xml"

    def test_mark_completed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create()

            manager.mark_completed("W123", file_size=1000, credits=100)

            assert manager.is_completed("W123")
            assert not manager.is_completed("W456")
            assert manager.get().stats.total_downloaded == 1
            assert manager.get().stats.total_bytes == 1000

    def test_mark_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create()

            manager.mark_failed("W123")

            assert "W123" in manager.get().failed_work_ids
            assert manager.get().stats.total_failed == 1

    def test_update_cursor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create()

            manager.update_cursor("cursor_abc")

            assert manager.get().current_cursor == "cursor_abc"
            assert manager.get().pages_completed == 1

    def test_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create()

            assert manager.exists()
            manager.delete()
            assert not manager.exists()

    def test_force_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create()

            # Add many items without triggering auto-save
            for i in range(100):
                manager.get().completed_work_ids.add(f"W{i}")

            manager.force_save()

            # Verify saved to disk
            manager2 = CheckpointManager(tmpdir)
            loaded = manager2.load()
            assert len(loaded.completed_work_ids) == 100

    def test_commit_page(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            manager.create(filter_str="type:article", content_format="none")

            manager.commit_page(
                cursor="cursor_abc",
                completed_entries=[("W1", 100, 0), ("W2", 200, 0)],
                failed_work_ids=["W3"],
            )

            checkpoint = manager.get()
            assert checkpoint.current_cursor == "cursor_abc"
            assert checkpoint.pages_completed == 1
            assert checkpoint.completed_work_ids == {"W1", "W2"}
            assert checkpoint.failed_work_ids == {"W3"}
            assert checkpoint.stats.total_downloaded == 2
            assert checkpoint.stats.total_failed == 1
            assert checkpoint.stats.total_bytes == 300
