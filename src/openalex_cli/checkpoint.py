"""Checkpoint system for resumable downloads."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .state_io import atomic_write_json


@dataclass
class DownloadStats:
    """Statistics for the download session."""

    started_at: float = field(default_factory=time.time)
    last_updated_at: float = field(default_factory=time.time)
    total_downloaded: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    total_bytes: int = 0
    credits_used: int = 0


@dataclass
class Checkpoint:
    """Checkpoint state for resumable downloads."""

    # Filter and format used for this download
    filter_str: str | None = None
    content_format: str = "pdf"

    # Pagination state
    current_cursor: str = "*"
    pages_completed: int = 0

    # Completed work IDs (for deduplication)
    completed_work_ids: set[str] = field(default_factory=set)

    # Failed work IDs (for potential retry)
    failed_work_ids: set[str] = field(default_factory=set)

    # Statistics
    stats: DownloadStats = field(default_factory=DownloadStats)

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dict."""
        return {
            "filter_str": self.filter_str,
            "content_format": self.content_format,
            "current_cursor": self.current_cursor,
            "pages_completed": self.pages_completed,
            "completed_work_ids": list(self.completed_work_ids),
            "failed_work_ids": list(self.failed_work_ids),
            "stats": asdict(self.stats),
        }

    @classmethod
    def from_dict(cls, data: dict) -> Checkpoint:
        """Create from a dict."""
        stats_data = data.get("stats", {})
        return cls(
            filter_str=data.get("filter_str"),
            content_format=data.get("content_format", "pdf"),
            current_cursor=data.get("current_cursor", "*"),
            pages_completed=data.get("pages_completed", 0),
            completed_work_ids=set(data.get("completed_work_ids", [])),
            failed_work_ids=set(data.get("failed_work_ids", [])),
            stats=DownloadStats(
                started_at=stats_data.get("started_at", time.time()),
                last_updated_at=stats_data.get("last_updated_at", time.time()),
                total_downloaded=stats_data.get("total_downloaded", 0),
                total_failed=stats_data.get("total_failed", 0),
                total_skipped=stats_data.get("total_skipped", 0),
                total_bytes=stats_data.get("total_bytes", 0),
                credits_used=stats_data.get("credits_used", 0),
            ),
        )


class CheckpointManager:
    """Manages checkpoint persistence."""

    CHECKPOINT_FILENAME = ".openalex-checkpoint.json"

    def __init__(self, output_dir: str | Path):
        """
        Initialize the checkpoint manager.

        Args:
            output_dir: Directory where checkpoint file is stored
        """
        self.output_dir = Path(output_dir)
        self.checkpoint_path = self.output_dir / self.CHECKPOINT_FILENAME
        self._checkpoint: Checkpoint | None = None
        self._pending_saves = 0
        self._save_threshold = 1000  # Save every N downloads

    def exists(self) -> bool:
        """Check if a checkpoint file exists."""
        return self.checkpoint_path.exists()

    def load(self) -> Checkpoint | None:
        """Load checkpoint from disk if it exists."""
        if not self.exists():
            return None

        try:
            with open(self.checkpoint_path) as f:
                data = json.load(f)
            self._checkpoint = Checkpoint.from_dict(data)
            return self._checkpoint
        except (json.JSONDecodeError, KeyError):
            # Corrupted checkpoint - return None to start fresh
            return None

    def create(
        self,
        filter_str: str | None = None,
        content_format: str = "pdf",
    ) -> Checkpoint:
        """Create a new checkpoint."""
        self._checkpoint = Checkpoint(
            filter_str=filter_str,
            content_format=content_format,
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._save()
        return self._checkpoint

    def get(self) -> Checkpoint:
        """Get the current checkpoint (must be loaded or created first)."""
        if self._checkpoint is None:
            raise RuntimeError("Checkpoint not loaded or created")
        return self._checkpoint

    def update_cursor(self, cursor: str | None) -> None:
        """Update the pagination cursor."""
        checkpoint = self.get()
        checkpoint.current_cursor = cursor or "*"
        checkpoint.pages_completed += 1
        self._maybe_save()

    def mark_completed(self, work_id: str, file_size: int, credits: int) -> None:
        """Mark a work as successfully downloaded."""
        checkpoint = self.get()
        checkpoint.completed_work_ids.add(work_id)
        checkpoint.failed_work_ids.discard(work_id)
        checkpoint.stats.total_downloaded += 1
        checkpoint.stats.total_bytes += file_size
        checkpoint.stats.credits_used += credits
        checkpoint.stats.last_updated_at = time.time()
        self._maybe_save()

    def mark_failed(self, work_id: str) -> None:
        """Mark a work as failed to download."""
        checkpoint = self.get()
        checkpoint.failed_work_ids.add(work_id)
        checkpoint.stats.total_failed += 1
        checkpoint.stats.last_updated_at = time.time()
        self._maybe_save()

    def mark_skipped(self, work_id: str) -> None:
        """Mark a work as skipped (already exists)."""
        checkpoint = self.get()
        checkpoint.completed_work_ids.add(work_id)
        checkpoint.stats.total_skipped += 1
        checkpoint.stats.last_updated_at = time.time()
        self._maybe_save()

    def is_completed(self, work_id: str) -> bool:
        """Check if a work ID has already been completed."""
        return work_id in self.get().completed_work_ids

    def get_failed_work_ids(self) -> list[str]:
        """Return unresolved failed work IDs in stable order."""
        return sorted(self.get().failed_work_ids)

    def resolve_failed_work(self, work_id: str, file_size: int, credits: int) -> None:
        """Move a failed work into the completed set after a successful retry."""
        checkpoint = self.get()
        checkpoint.failed_work_ids.discard(work_id)
        checkpoint.completed_work_ids.add(work_id)
        checkpoint.stats.total_downloaded += 1
        checkpoint.stats.total_bytes += file_size
        checkpoint.stats.credits_used += credits
        checkpoint.stats.last_updated_at = time.time()
        self._maybe_save()

    def record_failed_retry(self, work_id: str) -> None:
        """Keep a failed work unresolved after another failed retry attempt."""
        checkpoint = self.get()
        checkpoint.failed_work_ids.add(work_id)
        checkpoint.stats.last_updated_at = time.time()
        self._maybe_save()

    def terminal_work_ids(self) -> set[str]:
        """Return all work IDs with a committed terminal outcome."""
        checkpoint = self.get()
        return checkpoint.completed_work_ids | checkpoint.failed_work_ids

    def commit_page(
        self,
        cursor: str | None,
        completed_entries: list[tuple[str, int, int]],
        failed_work_ids: list[str],
    ) -> None:
        """Commit a fully terminal page and advance the durable cursor."""
        checkpoint = self.get()

        for work_id, file_size, credits in completed_entries:
            checkpoint.completed_work_ids.add(work_id)
            checkpoint.failed_work_ids.discard(work_id)
            checkpoint.stats.total_downloaded += 1
            checkpoint.stats.total_bytes += file_size
            checkpoint.stats.credits_used += credits

        for work_id in failed_work_ids:
            checkpoint.failed_work_ids.add(work_id)
            checkpoint.stats.total_failed += 1

        checkpoint.current_cursor = cursor or "*"
        checkpoint.pages_completed += 1
        checkpoint.stats.last_updated_at = time.time()
        self._save()
        self._pending_saves = 0

    def _maybe_save(self) -> None:
        """Save checkpoint if threshold reached."""
        self._pending_saves += 1
        if self._pending_saves >= self._save_threshold:
            self._save()
            self._pending_saves = 0

    def _save(self) -> None:
        """Save checkpoint to disk."""
        checkpoint = self.get()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.checkpoint_path, checkpoint.to_dict())

    def force_save(self) -> None:
        """Force an immediate save (e.g., on shutdown)."""
        self._save()
        self._pending_saves = 0

    def delete(self) -> None:
        """Delete the checkpoint file."""
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
        self._checkpoint = None
