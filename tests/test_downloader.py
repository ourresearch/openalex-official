"""Tests for downloader orchestration safety boundaries."""

import asyncio
from typing import Any, cast

import pytest

from openalex_cli.api_client import DownloadResult, WorkItem
from openalex_cli.downloader import DownloadConfig, DownloadOrchestrator, QueuedWork
from openalex_cli.progress import ProgressTracker
from openalex_cli.utils import ContentFormat, StorageType


class _DummyProgress:
    def __init__(self) -> None:
        self.info_messages: list[str] = []

    def log_warning(self, message: str) -> None:
        self.last_warning = message

    def initialize_totals(
        self,
        starting_completed: int,
        expected_total_works: int | None,
    ) -> None:
        self.last_totals = (starting_completed, expected_total_works)

    def sync_checkpoint_state(
        self,
        completed_count: int,
        unresolved_failed_count: int | None = None,
    ) -> None:
        self.last_sync = (completed_count, unresolved_failed_count)

    def update_pagination(self, pages: int, cursor: str | None) -> None:
        self.last_pagination = (pages, cursor)

    def update_download(self, **kwargs) -> None:
        self.last_download = kwargs

    def log_info(self, message: str) -> None:
        self.last_info = message
        self.info_messages.append(message)

    def log_error(self, message: str) -> None:
        self.last_error = message

    def record_retry_summary(self, attempted: int, recovered: int, remaining: int) -> None:
        self.retry_summary = (attempted, recovered, remaining)


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


async def _drain_metadata_only_run(orchestrator: DownloadOrchestrator) -> None:
    """Run producer, one worker, and result processor to convergence."""
    orchestrator._work_queue = asyncio.Queue()
    orchestrator._results_queue = asyncio.Queue()

    worker = asyncio.create_task(orchestrator._download_worker(0))
    result_processor = asyncio.create_task(orchestrator._process_results())

    await orchestrator._produce_work()
    await orchestrator._work_queue.put(None)
    await worker
    await orchestrator._results_queue.put(
        DownloadResult(work_id="__DONE__", format=ContentFormat.NONE, success=True)
    )
    await result_processor


def _metadata_doc(work_id: str) -> dict[str, Any]:
    return {
        "id": f"https://openalex.org/{work_id}",
        "has_content": {},
        "title": work_id,
    }


@pytest.mark.asyncio
async def test_failpoint_triggers_after_page_register(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENALEX_FAILPOINTS", "after_page_register")

    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator._work_queue = asyncio.Queue()

    async def one_page(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format, cursor
        yield [WorkItem(work_id="W1")], "cursor-1"

    orchestrator.api_client.list_works = one_page  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="after_page_register"):
        await orchestrator._produce_work()

    manifests = orchestrator.page_tracker.state_store.list_manifests()
    assert len(manifests) == 1
    assert manifests[0].work_ids == ["W1"]


@pytest.mark.asyncio
async def test_failpoint_triggers_after_page_commit(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENALEX_FAILPOINTS", "after_page_commit")

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
    page = orchestrator.page_tracker.register_page("*", "cursor-1", ["W1"])
    orchestrator._results_queue = asyncio.Queue()
    await orchestrator._results_queue.put(
        DownloadResult(
            work_id="W1",
            format=ContentFormat.NONE,
            success=True,
            file_size=10,
            credits_cost=0,
            page_seq=page.seq,
        )
    )
    await orchestrator._results_queue.put(
        DownloadResult(work_id="__DONE__", format=ContentFormat.NONE, success=True)
    )

    with pytest.raises(RuntimeError, match="after_page_commit"):
        await orchestrator._process_results()

    checkpoint = orchestrator.checkpoint_manager.get()
    assert checkpoint.current_cursor == "cursor-1"
    assert checkpoint.completed_work_ids == {"W1"}
    assert orchestrator.page_tracker.state_store.list_manifests() == []


@pytest.mark.asyncio
async def test_restart_after_page_register_replays_from_disk_and_converges(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENALEX_FAILPOINTS", "after_page_register")

    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )

    first = DownloadOrchestrator(config)
    first.checkpoint_manager.create(filter_str=None, content_format="none")
    first._work_queue = asyncio.Queue()

    async def one_page_then_stop(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format
        if cursor == "*":
            yield [WorkItem(work_id="W1")], "cursor-1"

    first.api_client.list_works = one_page_then_stop  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="after_page_register"):
        await first._produce_work()

    assert first.page_tracker.state_store.list_manifests()[0].work_ids == ["W1"]

    monkeypatch.delenv("OPENALEX_FAILPOINTS")

    second = DownloadOrchestrator(config)
    await second._setup_checkpoint()
    fetches: list[str] = []
    second.api_client.close = _async_noop  # type: ignore[method-assign]
    second.storage.close = _async_noop  # type: ignore[method-assign]

    async def no_new_pages(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format
        assert cursor == "cursor-1"
        if False:
            yield [], None

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        fetches.append(work_id)
        return _metadata_doc(work_id)

    second.api_client.list_works = no_new_pages  # type: ignore[method-assign]
    second.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]

    await _drain_metadata_only_run(second)

    checkpoint = second.checkpoint_manager.get()
    assert fetches == ["W1"]
    assert checkpoint.current_cursor == "cursor-1"
    assert checkpoint.completed_work_ids == {"W1"}
    assert second.page_tracker.state_store.list_manifests() == []
    assert (tmp_path / "W1.json").exists()


@pytest.mark.asyncio
async def test_restart_after_result_record_replays_only_remaining_work(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENALEX_FAILPOINTS", "after_result_record")

    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        workers=1,
    )

    first = DownloadOrchestrator(config)
    first.checkpoint_manager.create(filter_str=None, content_format="none")
    page = first.page_tracker.register_page("*", "cursor-1", ["W1", "W2"])
    first._results_queue = asyncio.Queue()
    await first._results_queue.put(
        DownloadResult(
            work_id="W1",
            format=ContentFormat.NONE,
            success=True,
            file_size=10,
            credits_cost=0,
            page_seq=page.seq,
        )
    )

    with pytest.raises(RuntimeError, match="after_result_record"):
        await first._process_results()

    cached_manifest = first.page_tracker._load_manifest(page.seq)
    assert cached_manifest is not None
    assert cached_manifest.work_states["W1"].state == "completed"
    assert cached_manifest.work_states["W2"].state == "pending"

    persisted_manifest = first.page_tracker.state_store.list_manifests()[0]
    assert persisted_manifest.work_states["W1"].state == "completed"

    monkeypatch.delenv("OPENALEX_FAILPOINTS")

    second = DownloadOrchestrator(config)
    await second._setup_checkpoint()
    fetches: list[str] = []
    second.api_client.close = _async_noop  # type: ignore[method-assign]
    second.storage.close = _async_noop  # type: ignore[method-assign]

    async def no_new_pages(
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.NONE,
        cursor: str = "*",
    ):
        del filter_str, content_format
        assert cursor == "cursor-1"
        if False:
            yield [], None

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        fetches.append(work_id)
        return _metadata_doc(work_id)

    second.api_client.list_works = no_new_pages  # type: ignore[method-assign]
    second.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]

    await _drain_metadata_only_run(second)

    checkpoint = second.checkpoint_manager.get()
    assert fetches == ["W2"]
    assert checkpoint.current_cursor == "cursor-1"
    assert checkpoint.completed_work_ids == {"W1", "W2"}
    assert second.page_tracker.state_store.list_manifests() == []
    assert (tmp_path / "W2.json").exists()


@pytest.mark.asyncio
async def test_run_failed_retry_phase_skips_when_no_failed_ids(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")

    called = False

    async def fake_run_direct_work_id_phase(work_ids, workers):
        nonlocal called
        del work_ids, workers
        called = True

    orchestrator._run_direct_work_id_phase = fake_run_direct_work_id_phase  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    assert called is False


@pytest.mark.asyncio
async def test_run_failed_retry_phase_uses_retry_workers(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
        retry_workers=10,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.checkpoint_manager.mark_failed("W1")

    seen = {}
    original_rate_limiter = orchestrator.rate_limiter

    async def fake_run_direct_work_id_phase(work_ids, workers):
        seen["work_ids"] = work_ids
        seen["workers"] = workers
        seen["limiter_max"] = orchestrator.rate_limiter.max_workers

    orchestrator._run_direct_work_id_phase = fake_run_direct_work_id_phase  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    assert seen["work_ids"] == ["W1"]
    assert seen["workers"] == 10
    assert seen["limiter_max"] == 10
    assert orchestrator.rate_limiter is original_rate_limiter


@pytest.mark.asyncio
async def test_run_failed_retry_phase_skips_when_only_historical_failures_exist(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
    )
    orchestrator = DownloadOrchestrator(config)
    checkpoint = orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    checkpoint.stats.total_failed = 558
    checkpoint.failed_work_ids.clear()
    orchestrator.checkpoint_manager.force_save()

    called = False

    async def fake_run_direct_work_id_phase(work_ids, workers):
        nonlocal called
        del work_ids, workers
        called = True

    orchestrator._run_direct_work_id_phase = fake_run_direct_work_id_phase  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    assert called is False


@pytest.mark.asyncio
async def test_retry_failed_success_resolves_failed_work(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
        retry_workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.checkpoint_manager.mark_failed("W1")

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        return _metadata_doc(work_id)

    orchestrator.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    checkpoint = orchestrator.checkpoint_manager.get()
    assert "W1" not in checkpoint.failed_work_ids
    assert "W1" in checkpoint.completed_work_ids


@pytest.mark.asyncio
async def test_retry_failed_records_retry_summary(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
        retry_workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    dummy = _DummyProgress()
    orchestrator.progress_tracker = cast(ProgressTracker, dummy)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.checkpoint_manager.mark_failed("W1")

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        return _metadata_doc(work_id)

    orchestrator.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    assert dummy.retry_summary == (1, 1, 0)


@pytest.mark.asyncio
async def test_retry_failed_failure_leaves_id_failed(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
        retry_workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.checkpoint_manager.mark_failed("W1")

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        del work_id
        raise RuntimeError("still broken")

    orchestrator.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()

    checkpoint = orchestrator.checkpoint_manager.get()
    assert "W1" in checkpoint.failed_work_ids


@pytest.mark.asyncio
async def test_retry_failed_does_not_invoke_page_tracker(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        retry_failed=True,
        retry_workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.checkpoint_manager.create(filter_str=None, content_format="none")
    orchestrator.checkpoint_manager.mark_failed("W1")

    def explode(*args, **kwargs):
        del args, kwargs
        raise AssertionError("page tracker should not be used in retry-failed mode")

    orchestrator.page_tracker.register_page = explode  # type: ignore[method-assign]

    async def fetch_metadata(work_id: str) -> dict[str, Any]:
        return _metadata_doc(work_id)

    orchestrator.api_client.get_work_metadata = fetch_metadata  # type: ignore[method-assign]
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    await orchestrator._run_failed_retry_phase()


@pytest.mark.asyncio
async def test_setup_checkpoint_prefetches_total_count_for_new_filter_run(tmp_path):
    config = DownloadConfig(
        api_key="test-key",
        output_path=str(tmp_path),
        storage_type=StorageType.LOCAL,
        content_format=ContentFormat.NONE,
        filter_str="publication_year:2024",
        workers=1,
    )
    orchestrator = DownloadOrchestrator(config)
    orchestrator.progress_tracker = cast(ProgressTracker, _DummyProgress())

    calls: list[str | None] = []

    async def fake_get_work_count(
        filter_str: str | None = None, content_format: ContentFormat = ContentFormat.NONE
    ) -> int:
        calls.append(filter_str)
        assert content_format == ContentFormat.NONE
        return 321

    orchestrator.api_client.get_work_count = fake_get_work_count  # type: ignore[method-assign]

    checkpoint = await orchestrator._setup_checkpoint()

    assert checkpoint is not None
    assert checkpoint.expected_total_works == 321
    assert calls == ["publication_year:2024"]


@pytest.mark.asyncio
async def test_setup_checkpoint_reuses_stored_total_on_resume(tmp_path):
    manager = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    manager.checkpoint_manager.create(
        filter_str="publication_year:2024",
        content_format="none",
        expected_total_works=456,
    )

    orchestrator = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    orchestrator.progress_tracker = cast(ProgressTracker, _DummyProgress())

    async def fail_get_work_count(*args, **kwargs) -> int:
        del args, kwargs
        raise AssertionError("count preflight should not run on resume")

    orchestrator.api_client.get_work_count = fail_get_work_count  # type: ignore[method-assign]

    checkpoint = await orchestrator._setup_checkpoint()

    assert checkpoint is not None
    assert checkpoint.expected_total_works == 456


@pytest.mark.asyncio
async def test_setup_checkpoint_backfills_missing_total_on_resume(tmp_path):
    existing = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    existing.checkpoint_manager.create(
        filter_str="publication_year:2024",
        content_format="none",
        expected_total_works=None,
    )

    orchestrator = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    orchestrator.progress_tracker = cast(ProgressTracker, _DummyProgress())

    calls: list[str | None] = []

    async def fake_get_work_count(
        filter_str: str | None = None, content_format: ContentFormat = ContentFormat.NONE
    ) -> int:
        calls.append(filter_str)
        assert content_format == ContentFormat.NONE
        return 654

    orchestrator.api_client.get_work_count = fake_get_work_count  # type: ignore[method-assign]

    checkpoint = await orchestrator._setup_checkpoint()

    assert checkpoint is not None
    assert checkpoint.expected_total_works == 654
    assert calls == ["publication_year:2024"]


@pytest.mark.asyncio
async def test_setup_checkpoint_resume_log_uses_unresolved_failed_count(tmp_path):
    orchestrator = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    dummy = _DummyProgress()
    orchestrator.progress_tracker = cast(ProgressTracker, dummy)

    checkpoint = orchestrator.checkpoint_manager.create(
        filter_str="publication_year:2024",
        content_format="none",
        expected_total_works=456,
    )
    checkpoint.completed_work_ids = {"W1", "W2"}
    checkpoint.failed_work_ids.clear()
    checkpoint.stats.total_failed = 558
    orchestrator.checkpoint_manager.force_save()

    loaded = await orchestrator._setup_checkpoint()

    assert loaded is not None
    assert (
        dummy.last_info
        == "Resuming from checkpoint: 2 completed, 0 unresolved failed of 456 expected"
    )


@pytest.mark.asyncio
async def test_run_exits_early_when_checkpoint_is_already_complete(tmp_path):
    existing = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    checkpoint = existing.checkpoint_manager.create(
        filter_str="publication_year:2024",
        content_format="none",
        expected_total_works=2,
    )
    checkpoint.completed_work_ids = {"W1", "W2"}
    existing.checkpoint_manager.force_save()

    orchestrator = DownloadOrchestrator(
        DownloadConfig(
            api_key="test-key",
            output_path=str(tmp_path),
            storage_type=StorageType.LOCAL,
            content_format=ContentFormat.NONE,
            filter_str="publication_year:2024",
            workers=1,
        )
    )
    dummy = _DummyProgress()
    orchestrator.api_client.close = _async_noop  # type: ignore[method-assign]
    orchestrator.storage.close = _async_noop  # type: ignore[method-assign]

    async def fail_list_works(*args, **kwargs):
        del args, kwargs
        raise AssertionError("list_works should not run when checkpoint is already complete")
        yield

    orchestrator.api_client.list_works = fail_list_works  # type: ignore[method-assign]

    await orchestrator.run(progress_tracker=cast(ProgressTracker, dummy))

    assert dummy.last_totals == (2, 2)
    assert dummy.last_sync == (2, 0)
    assert (
        dummy.last_info == "Nothing left to download; checkpoint already covers all expected works."
    )


def test_progress_tracker_summary_includes_retry_recovery(tmp_path, caplog):
    tracker = ProgressTracker(output_dir=tmp_path, quiet=True, verbose=False)
    tracker.record_retry_summary(attempted=571, recovered=571, remaining=0)

    with caplog.at_level("INFO", logger="openalex-content"):
        tracker.stop()

    assert "Retry recovery: 571 / 571 succeeded (0 unresolved remaining)" in caplog.text


def test_progress_tracker_initializes_known_total(tmp_path):
    tracker = ProgressTracker(output_dir=tmp_path, quiet=True, verbose=False)

    tracker.initialize_totals(starting_completed=100, expected_total_works=250)
    tracker.update_download("W1", success=True, file_size=100)

    assert tracker.stats.starting_completed == 100
    assert tracker.stats.expected_total_works == 250
    assert "100 / 250" in tracker._format_stats()


def test_progress_tracker_sync_uses_checkpoint_completed_count(tmp_path):
    tracker = ProgressTracker(output_dir=tmp_path, quiet=True, verbose=False)

    tracker.initialize_totals(starting_completed=100, expected_total_works=250)
    tracker.sync_checkpoint_state(completed_count=150, unresolved_failed_count=3)

    assert tracker.stats.authoritative_completed == 150
    assert tracker.stats.authoritative_unresolved_failed == 3
    assert "150 / 250" in tracker._format_stats()
