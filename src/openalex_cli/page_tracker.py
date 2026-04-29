"""Persisted page tracking for safe metadata-only resume."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .checkpoint import CheckpointManager
from .state_io import atomic_write_json


@dataclass
class PageWorkState:
    """Terminal state for a work admitted from a page."""

    state: str = "pending"
    file_size: int = 0
    credits: int = 0
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "state": self.state,
            "file_size": self.file_size,
            "credits": self.credits,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PageWorkState:
        return cls(
            state=data.get("state", "pending"),
            file_size=data.get("file_size", 0),
            credits=data.get("credits", 0),
            error=data.get("error"),
        )


@dataclass
class PageManifest:
    """Durable record of a listed page and each admitted work."""

    seq: int
    cursor_in: str
    cursor_out: str | None
    work_ids: list[str]
    work_states: dict[str, PageWorkState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for work_id in self.work_ids:
            self.work_states.setdefault(work_id, PageWorkState())

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "cursor_in": self.cursor_in,
            "cursor_out": self.cursor_out,
            "work_ids": self.work_ids,
            "work_states": {
                work_id: state.to_dict() for work_id, state in self.work_states.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> PageManifest:
        return cls(
            seq=data["seq"],
            cursor_in=data.get("cursor_in", "*"),
            cursor_out=data.get("cursor_out"),
            work_ids=list(data.get("work_ids", [])),
            work_states={
                work_id: PageWorkState.from_dict(state)
                for work_id, state in data.get("work_states", {}).items()
            },
        )

    def is_terminal(self) -> bool:
        return all(self.work_states[work_id].state != "pending" for work_id in self.work_ids)

    def pending_work_ids(self) -> list[str]:
        return [
            work_id for work_id in self.work_ids if self.work_states[work_id].state == "pending"
        ]

    def completed_entries(self) -> list[tuple[str, int, int]]:
        entries: list[tuple[str, int, int]] = []
        for work_id in self.work_ids:
            state = self.work_states[work_id]
            if state.state == "completed":
                entries.append((work_id, state.file_size, state.credits))
        return entries

    def failed_work_ids(self) -> list[str]:
        return [work_id for work_id in self.work_ids if self.work_states[work_id].state == "failed"]


@dataclass
class ReplayWorkItem:
    """Pending work reconstructed from a durable page manifest."""

    page_seq: int
    work_id: str


@dataclass
class CommitOutcome:
    """Result of recording a terminal work outcome."""

    committed_pages: int = 0
    current_cursor: str | None = None
    pages_completed: int = 0


class PageStateStore:
    """Durable local storage for admitted page manifests."""

    DIRNAME = ".openalex-pages"

    def __init__(self, output_dir: str | Path):
        self.base_dir = Path(output_dir) / self.DIRNAME

    def list_manifests(self) -> list[PageManifest]:
        if not self.base_dir.exists():
            return []

        manifests: list[PageManifest] = []
        for path in sorted(self.base_dir.glob("page-*.json")):
            with open(path, encoding="utf-8") as f:
                manifests.append(PageManifest.from_dict(json.load(f)))
        manifests.sort(key=lambda manifest: manifest.seq)
        return manifests

    def has_manifests(self) -> bool:
        """Return True when any persisted page manifests exist."""
        return any(self.base_dir.glob("page-*.json")) if self.base_dir.exists() else False

    def save_manifest(self, manifest: PageManifest) -> None:
        atomic_write_json(self._manifest_path(manifest.seq), manifest.to_dict())

    def delete_manifest(self, seq: int) -> None:
        path = self._manifest_path(seq)
        if path.exists():
            path.unlink()

    def next_seq(self) -> int:
        manifests = self.list_manifests()
        return (manifests[-1].seq + 1) if manifests else 1

    def _manifest_path(self, seq: int) -> Path:
        return self.base_dir / f"page-{seq:08d}.json"


class PageTracker:
    """Own durable state for uncommitted metadata-only pages."""

    def __init__(self, output_dir: str | Path, checkpoint_manager: CheckpointManager):
        self.checkpoint_manager = checkpoint_manager
        self.state_store = PageStateStore(output_dir)

    def startup_reconcile(self) -> CommitOutcome:
        for manifest in self.state_store.list_manifests():
            if self._manifest_is_already_committed(manifest):
                self.state_store.delete_manifest(manifest.seq)

        return self._commit_ready_pages()

    def has_uncommitted_pages(self) -> bool:
        """Return True when durable uncommitted page manifests remain."""
        return self.state_store.has_manifests()

    def resume_cursor(self, default_cursor: str) -> str | None:
        """Return the cursor to use for fresh pagination after replay."""
        manifests = self.state_store.list_manifests()
        if not manifests:
            return default_cursor
        return manifests[-1].cursor_out

    def register_page(
        self,
        cursor_in: str,
        cursor_out: str | None,
        work_ids: list[str],
    ) -> PageManifest:
        manifest = PageManifest(
            seq=self.state_store.next_seq(),
            cursor_in=cursor_in,
            cursor_out=cursor_out,
            work_ids=work_ids,
        )
        self.state_store.save_manifest(manifest)
        return manifest

    def replay_pending_work(self) -> list[ReplayWorkItem]:
        replay: list[ReplayWorkItem] = []
        for manifest in self.state_store.list_manifests():
            for work_id in manifest.pending_work_ids():
                replay.append(ReplayWorkItem(page_seq=manifest.seq, work_id=work_id))
        return replay

    def record_work_result(
        self,
        page_seq: int,
        work_id: str,
        success: bool,
        file_size: int,
        credits: int,
        error: str | None,
    ) -> CommitOutcome:
        manifest = self._load_manifest(page_seq)
        if manifest is None:
            return CommitOutcome()

        state = manifest.work_states.get(work_id)
        if state is None or state.state != "pending":
            return CommitOutcome()

        manifest.work_states[work_id] = PageWorkState(
            state="completed" if success else "failed",
            file_size=file_size,
            credits=credits,
            error=error,
        )
        self.state_store.save_manifest(manifest)
        return self._commit_ready_pages()

    def _commit_ready_pages(self) -> CommitOutcome:
        committed_pages = 0
        checkpoint = self.checkpoint_manager.get()

        for manifest in self.state_store.list_manifests():
            if self._manifest_is_already_committed(manifest):
                self.state_store.delete_manifest(manifest.seq)
                continue

            if not manifest.is_terminal():
                break

            self.checkpoint_manager.commit_page(
                cursor=manifest.cursor_out,
                completed_entries=manifest.completed_entries(),
                failed_work_ids=manifest.failed_work_ids(),
            )
            self.state_store.delete_manifest(manifest.seq)
            committed_pages += 1
            checkpoint = self.checkpoint_manager.get()

        return CommitOutcome(
            committed_pages=committed_pages,
            current_cursor=checkpoint.current_cursor,
            pages_completed=checkpoint.pages_completed,
        )

    def _manifest_is_already_committed(self, manifest: PageManifest) -> bool:
        terminal_ids = self.checkpoint_manager.terminal_work_ids()
        return bool(manifest.work_ids) and all(
            work_id in terminal_ids for work_id in manifest.work_ids
        )

    def _load_manifest(self, seq: int) -> PageManifest | None:
        for manifest in self.state_store.list_manifests():
            if manifest.seq == seq:
                return manifest
        return None
