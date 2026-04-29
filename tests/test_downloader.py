"""Tests for downloader orchestration safety boundaries."""

import asyncio
from typing import Any, cast

import pytest

from openalex_cli.api_client import WorkItem
from openalex_cli.downloader import DownloadConfig, DownloadOrchestrator, QueuedWork
from openalex_cli.progress import ProgressTracker
from openalex_cli.utils import ContentFormat, StorageType


class _DummyProgress:
    def log_warning(self, message: str) -> None:
        self.last_warning = message

    def update_pagination(self, pages: int, cursor: str | None) -> None:
        self.last_pagination = (pages, cursor)

    def update_download(self, **kwargs) -> None:
        self.last_download = kwargs

    def log_info(self, message: str) -> None:
        self.last_info = message

    def log_error(self, message: str) -> None:
        self.last_error = message


@pytest.mark.asyncio
async def test_metadata_failure_records_failed_result(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.progress_tracker = cast(ProgressTracker, _DummyProgress())
    orchestrator._work_queue = asyncio.Queue()
    orchestrator._results_queue = asyncio.Queue()

    async def failing_metadata(work_id: str) -> dict[str, Any]:
        del work_id
        raise RuntimeError("metadata fetch failed")

    orchestrator.api_client.get_work_metadata = failing_metadata  # type: ignore[method-assign]

    await orchestrator._work_queue.put(QueuedWork(work=WorkItem(work_id="W1"), page_seq=7))
    await orchestrator._work_queue.put(None)

    await orchestrator._download_worker(0)

    result = await orchestrator._results_queue.get()
    assert result.work_id == "W1"
    assert result.success is False
    assert result.page_seq == 7
    assert result.error is not None
    assert "metadata fetch failed" in result.error


@pytest.mark.asyncio
async def test_producer_replays_pending_page_before_pagination(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    page = orchestrator.page_tracker.register_page("*", "cursor-1", ["W1", "W2"])
    orchestrator._work_queue = asyncio.Queue()

    async def empty_pages(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format, cursor
        if False:
            yield [], None

    orchestrator.api_client.list_works = empty_pages  # type: ignore[method-assign]

    await orchestrator._produce_work()

    queued = []
    while not orchestrator._work_queue.empty():
        item = await orchestrator._work_queue.get()
        assert item is not None
        queued.append((item.page_seq, item.work.work_id))

    assert queued == [(page.seq, "W1"), (page.seq, "W2")]


@pytest.mark.asyncio
async def test_producer_resumes_fresh_pagination_after_highest_uncommitted_cursor(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.page_tracker.register_page("*", "cursor-1", ["W1"])
    orchestrator.page_tracker.register_page("cursor-1", "cursor-2", ["W2"])
    orchestrator._work_queue = asyncio.Queue()

    seen_cursors = []

    async def empty_pages(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format
        seen_cursors.append(cursor)
        if False:
            yield [], None

    orchestrator.api_client.list_works = empty_pages  # type: ignore[method-assign]

    await orchestrator._produce_work()

    assert seen_cursors == ["cursor-2"]


@pytest.mark.asyncio
async def test_run_commits_terminal_manifest_during_startup_reconcile(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.progress_tracker = cast(ProgressTracker, _DummyProgress())
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.page_tracker.register_page("*", "cursor-1", ["W1"])
    manifest = orchestrator.page_tracker.state_store.list_manifests()[0]
    manifest.work_states["W1"].state = "completed"
    manifest.work_states["W1"].file_size = 10
    orchestrator.page_tracker.state_store.save_manifest(manifest)

    async def empty_pages(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format, cursor
        if False:
            yield [], None

    orchestrator.api_client.list_works = empty_pages  # type: ignore[method-assign]
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    await orchestrator.run(orchestrator.progress_tracker)

    checkpoint = orchestrator.checkpoint_manager.get()
    assert checkpoint.current_cursor == "cursor-1"
    assert checkpoint.pages_completed == 1
    assert checkpoint.completed_work_ids == {"W1"}


async def _async_noop():
    return None
