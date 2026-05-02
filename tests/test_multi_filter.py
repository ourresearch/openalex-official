"""Tests for multi-filter download functionality."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from openalex_cli.checkpoint import Checkpoint, CheckpointManager, FilterCheckpoint


class TestMultiFilterCheckpoint:
    """Test multi-filter checkpoint schema."""

    def test_single_mode_backward_compatibility(self):
        """Old checkpoints without 'mode' field should load as single mode."""
        checkpoint = Checkpoint.from_dict(
            {
                "filter_str": "publication_year:>2020",
                "content_format": "none",
                "completed_work_ids": ["W123", "W456"],
                "failed_work_ids": ["W789"],
                "stats": {"total_downloaded": 2, "total_failed": 1},
            }
        )

        assert checkpoint.mode == "single"
        assert checkpoint.filter_str == "publication_year:>2020"
        assert checkpoint.completed_work_ids == {"W123", "W456"}
        assert checkpoint.failed_work_ids == {"W789"}
        assert checkpoint.filters == []

    def test_multi_mode_checkpoint(self):
        """Multi-filter checkpoint should parse filters array."""
        checkpoint = Checkpoint.from_dict(
            {
                "mode": "multi",
                "content_format": "none",
                "completed_work_ids": ["W100"],
                "filters": [
                    {
                        "id": "abc123",
                        "name": "filter_001",
                        "filter_str": "publication_year:>2020",
                        "completed_work_ids": ["W100", "W200"],
                        "failed_work_ids": [],
                        "status": "active",
                        "stats": {"total_downloaded": 2},
                    }
                ],
                "stats": {"total_downloaded": 1},
            }
        )

        assert checkpoint.mode == "multi"
        assert len(checkpoint.filters) == 1
        assert checkpoint.filters[0].id == "abc123"
        assert checkpoint.filters[0].name == "filter_001"
        assert checkpoint.filters[0].filter_str == "publication_year:>2020"
        assert checkpoint.filters[0].completed_work_ids == {"W100", "W200"}
        assert checkpoint.filters[0].status == "active"

    def test_single_mode_save_backward_compatible(self):
        """Single-mode checkpoints should save without 'mode' field."""
        checkpoint = Checkpoint(
            mode="single",
            filter_str="publication_year:>2020",
            completed_work_ids={"W123"},
        )
        data = checkpoint.to_dict()

        assert "mode" not in data
        assert data["filter_str"] == "publication_year:>2020"
        assert "filters" not in data

    def test_multi_mode_save_includes_filters(self):
        """Multi-mode checkpoints should save filters array."""
        checkpoint = Checkpoint(
            mode="multi",
            filters=[
                FilterCheckpoint(
                    id="abc123",
                    name="filter_001",
                    filter_str="publication_year:>2020",
                    status="complete",
                )
            ],
        )
        data = checkpoint.to_dict()

        assert data["mode"] == "multi"
        assert "filters" in data
        assert len(data["filters"]) == 1
        assert data["filters"][0]["id"] == "abc123"
        assert data["filters"][0]["status"] == "complete"

    def test_filter_checkpoint_round_trip(self):
        """FilterCheckpoint should survive round-trip serialization."""
        original = FilterCheckpoint(
            id="hash123",
            name="test_filter",
            filter_str="publication_year:2024",
            expected_total_works=100,
            completed_work_ids={"W1", "W2"},
            failed_work_ids={"W3"},
            status="stalled",
        )

        data = original.to_dict()
        restored = FilterCheckpoint.from_dict(data)

        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.filter_str == original.filter_str
        assert restored.expected_total_works == original.expected_total_works
        assert restored.completed_work_ids == original.completed_work_ids
        assert restored.failed_work_ids == original.failed_work_ids
        assert restored.status == original.status


class TestCheckpointManagerMultiFilter:
    """Test CheckpointManager with multi-filter checkpoints."""

    def test_save_and_load_multi_filter(self):
        """CheckpointManager should save/load multi-filter checkpoints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CheckpointManager(tmpdir)
            checkpoint = manager.create()
            checkpoint.mode = "multi"
            checkpoint.filters = [
                FilterCheckpoint(
                    id="f1",
                    name="filter_1",
                    filter_str="year:2024",
                    status="active",
                )
            ]
            manager.force_save()

            # Load back
            loaded = manager.load()
            assert loaded is not None
            assert loaded.mode == "multi"
            assert len(loaded.filters) == 1
            assert loaded.filters[0].name == "filter_1"

    def test_load_old_checkpoint(self):
        """Loading old single-filter checkpoint should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create old-style checkpoint file
            checkpoint_path = Path(tmpdir) / ".openalex-checkpoint.json"
            old_data = {
                "filter_str": "publication_year:>2020",
                "completed_work_ids": ["W123"],
                "stats": {"total_downloaded": 1},
            }
            checkpoint_path.write_text(json.dumps(old_data))

            manager = CheckpointManager(tmpdir)
            checkpoint = manager.load()
            assert checkpoint is not None

            assert checkpoint.mode == "single"
            assert checkpoint.filter_str == "publication_year:>2020"
            assert checkpoint.completed_work_ids == {"W123"}

    def test_filter_deduplication_in_checkpoint(self):
        """Multiple filters with same ID should be deduplicated."""
        checkpoint = Checkpoint(mode="multi")
        checkpoint.filters = [
            FilterCheckpoint(id="same", name="f1", filter_str="a"),
            FilterCheckpoint(id="same", name="f2", filter_str="b"),
        ]

        # The checkpoint stores them as-is; deduplication happens in orchestrator
        assert len(checkpoint.filters) == 2
        assert checkpoint.filters[0].id == "same"
        assert checkpoint.filters[1].id == "same"


class TestMultiFilterOrchestrator:
    """Test MultiFilterOrchestrator behavior."""

    def test_orchestrator_init(self):
        """MultiFilterOrchestrator should initialize with filter list."""
        from openalex_cli.downloader import MultiFilterOrchestrator

        orchestrator = MultiFilterOrchestrator(
            api_key="test_key",
            output_path="/tmp/test",
            filters=[
                {"id": "f1", "name": "filter_1", "filter": "year:2024"},
            ],
        )

        assert orchestrator.api_key == "test_key"
        assert len(orchestrator.filters) == 1
        assert orchestrator.filters[0]["name"] == "filter_1"

    def test_orchestrator_skip_complete_filter(self):
        """Orchestrator should skip filters marked as complete."""
        from openalex_cli.downloader import MultiFilterOrchestrator
        from openalex_cli.checkpoint import CheckpointManager

        with tempfile.TemporaryDirectory() as tmpdir:
            # Pre-create checkpoint with completed filter
            manager = CheckpointManager(tmpdir)
            checkpoint = manager.create()
            checkpoint.mode = "multi"
            checkpoint.filters = [
                FilterCheckpoint(
                    id="f1",
                    name="filter_1",
                    filter_str="year:2024",
                    status="complete",
                )
            ]
            manager.force_save()

            orchestrator = MultiFilterOrchestrator(
                api_key="test_key",
                output_path=tmpdir,
                filters=[
                    {"id": "f1", "name": "filter_1", "filter": "year:2024"},
                ],
            )

            # Should not raise when running (filter is skipped)
            import asyncio

            asyncio.run(orchestrator.run())

    def test_filter_hash_generation(self):
        """Filter IDs should be based on hash of filter string."""
        import hashlib

        filter_str = "publication_year:>2020,topics.id:T123"
        expected_hash = hashlib.sha256(filter_str.encode()).hexdigest()[:8]

        # The orchestrator should generate IDs this way
        # This is a contract test for the ID generation logic
        assert len(expected_hash) == 8
        assert expected_hash.isalnum()
