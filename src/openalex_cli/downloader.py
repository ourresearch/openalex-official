"""Core async download orchestrator."""

from __future__ import annotations

import asyncio
import json
import signal
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Callable

from .api_client import CreditsExhaustedError, DownloadResult, OpenAlexAPIClient, WorkItem
from .checkpoint import CheckpointManager
from .fault_injection import FailureInjector
from .page_tracker import PageTracker
from .progress import ProgressTracker
from .rate_limiter import AdaptiveRateLimiter
from .storage import LocalStorage, S3Storage, StorageBackend
from .utils import ContentFormat, StorageType, doi_to_filename, work_id_to_path


@dataclass
class DownloadConfig:
    """Configuration for the download job."""

    api_key: str
    output_path: str
    storage_type: StorageType = StorageType.LOCAL
    s3_bucket: str | None = None
    s3_prefix: str = ""
    filter_str: str | None = None
    content_format: ContentFormat = ContentFormat.NONE  # Default: metadata only
    workers: int = 50
    resume: bool = True
    fresh: bool = False
    quiet: bool = False
    verbose: bool = False
    nested: bool = False
    work_ids: list[str] | None = None
    original_identifiers: dict[str, str] | None = None  # Maps work_id -> original input (e.g., DOI)
    sample: int | None = None  # Random sample size (max 10,000)
    seed: int | None = None  # Seed for reproducible random samples
    retry_failed: bool = False
    retry_workers: int | None = None


@dataclass
class QueuedWork:
    """Work queued for download with optional page-tracking context."""

    work: WorkItem
    page_seq: int | None = None


class DownloadOrchestrator:
    """Orchestrates the bulk download process."""

    def __init__(
        self,
        config: DownloadConfig,
        progress_callback: Callable[[str], None] | None = None,
    ):
        self.config = config
        self.progress_callback = progress_callback

        # Initialize components
        self.api_client = OpenAlexAPIClient(api_key=config.api_key)
        self.rate_limiter = AdaptiveRateLimiter(max_workers=config.workers)
        self.checkpoint_manager = CheckpointManager(config.output_path)
        self.page_tracker = PageTracker(config.output_path, self.checkpoint_manager)
        self.failure_injector = FailureInjector()
        self.progress_tracker: ProgressTracker | None = None

        # Initialize storage backend
        if config.storage_type == StorageType.S3:
            if not config.s3_bucket:
                raise ValueError("S3 bucket required for S3 storage")
            if S3Storage is None:
                raise RuntimeError(
                    "S3 storage requires the optional aiobotocore dependency to be installed"
                )
            self.storage: StorageBackend = S3Storage(
                bucket=config.s3_bucket,
                prefix=config.s3_prefix,
            )
        else:
            self.storage = LocalStorage(config.output_path)

        # Control flags
        self._shutdown_requested = False
        self._credits_exhausted = False
        self._retry_failed_phase = False
        self._page_tracking_enabled = (
            self.config.content_format == ContentFormat.NONE
            and not self.config.sample
            and not self.config.work_ids
        )
        # Queues are created in run() to ensure they're in the correct event loop
        self._work_queue: asyncio.Queue[QueuedWork | None] | None = None
        self._results_queue: asyncio.Queue[DownloadResult] | None = None

    async def run(self, progress_tracker: ProgressTracker | None = None) -> None:
        """Run the download job."""
        self.progress_tracker = progress_tracker

        # Create queues in the running event loop (Python 3.9 compatibility)
        self._work_queue = asyncio.Queue()
        self._results_queue = asyncio.Queue()

        # Set up signal handlers for graceful shutdown.
        # Windows event loops do not support add_signal_handler().
        loop = asyncio.get_running_loop()
        signal_handlers_installed = False
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._request_shutdown)
                signal_handlers_installed = True
            except NotImplementedError:
                signal_handlers_installed = False
                break

        try:
            # Initialize or resume checkpoint
            checkpoint = await self._setup_checkpoint()
            if checkpoint is None:
                return

            if self.progress_tracker:
                self.progress_tracker.initialize_totals(
                    starting_completed=len(checkpoint.completed_work_ids),
                    expected_total_works=checkpoint.expected_total_works,
                )
                self._sync_progress_checkpoint_state()

            if self._should_exit_without_work(checkpoint):
                if self.progress_tracker:
                    self.progress_tracker.log_info(
                        "Nothing left to download; checkpoint already covers all expected works."
                    )
                return

            if self._page_tracking_enabled:
                commit = self.page_tracker.startup_reconcile()
                if self.progress_tracker and commit.committed_pages:
                    self.progress_tracker.update_pagination(
                        pages=commit.pages_completed,
                        cursor=commit.current_cursor,
                    )

            # Log start
            if self.progress_tracker:
                self.progress_tracker.log_info(
                    f"Starting download with filter: {self.config.filter_str or '(all works)'}"
                )
                content_desc = (
                    "metadata only"
                    if self.config.content_format == ContentFormat.NONE
                    else f"metadata + {self.config.content_format.value}"
                )
                self.progress_tracker.log_info(
                    f"Content: {content_desc}, Workers: {self.config.workers}"
                )

            # Start worker tasks
            workers = [
                asyncio.create_task(self._download_worker(i)) for i in range(self.config.workers)
            ]

            # Start result processor
            result_processor = asyncio.create_task(self._process_results())

            # Start work producer (paginate through API)
            producer = asyncio.create_task(self._produce_work())

            # Wait for producer to finish
            await producer

            # Signal workers to stop
            for _ in range(self.config.workers):
                await self._work_queue.put(None)

            # Wait for workers to finish
            await asyncio.gather(*workers)

            # Signal result processor to stop
            await self._results_queue.put(
                DownloadResult(work_id="__DONE__", format=ContentFormat.PDF, success=True)
            )
            await result_processor

            if (
                self.config.retry_failed
                and not self._shutdown_requested
                and not self._credits_exhausted
            ):
                await self._run_failed_retry_phase()

        finally:
            # Clean up
            if self._page_tracking_enabled:
                self.page_tracker.flush_dirty_manifests()
            self.checkpoint_manager.force_save()
            await self.api_client.close()
            await self.storage.close()

            # Remove signal handlers only if they were installed.
            if signal_handlers_installed:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.remove_signal_handler(sig)

    def _request_shutdown(self) -> None:
        """Handle shutdown signal."""
        if self._shutdown_requested:
            # Second signal - force exit
            raise KeyboardInterrupt()
        self._shutdown_requested = True
        if self.progress_tracker:
            self.progress_tracker.log_warning("Shutdown requested. Finishing current downloads...")

    def _handle_credits_exhausted(self) -> None:
        """Stop all work when credits are exhausted."""
        if self._credits_exhausted:
            return  # Already handled
        self._credits_exhausted = True
        self._shutdown_requested = True
        if self.progress_tracker:
            self.progress_tracker.log_error(
                "Credits exhausted! Credits reset daily at midnight UTC. "
                "Purchase more at https://openalex.org/pricing. "
                "Progress has been saved — use --resume to continue later."
            )

    def _get_retry_workers(self) -> int:
        """Return worker count for the failed-ID retry phase."""
        return self.config.retry_workers or 10

    def _should_prefetch_total_count(self) -> bool:
        """Return True when this run should do a one-shot total count preflight."""
        return (
            self.config.content_format == ContentFormat.NONE
            and self.config.filter_str is not None
            and not self.config.sample
            and not self.config.work_ids
        )

    def _sync_progress_checkpoint_state(self) -> None:
        """Sync progress display from durable checkpoint state."""
        if not self.progress_tracker:
            return

        checkpoint = self.checkpoint_manager.get()
        self.progress_tracker.sync_checkpoint_state(
            completed_count=len(checkpoint.completed_work_ids),
            unresolved_failed_count=len(checkpoint.failed_work_ids),
        )

    def _should_exit_without_work(self, checkpoint) -> bool:
        """Return True when resume state proves this run has no work left to do."""
        if checkpoint.failed_work_ids and self.config.retry_failed:
            return False

        if self.config.work_ids:
            return all(
                self.checkpoint_manager.is_completed(work_id) for work_id in self.config.work_ids
            )

        expected_total = checkpoint.expected_total_works
        if expected_total is None:
            return False

        return (
            len(checkpoint.completed_work_ids) >= expected_total and not checkpoint.failed_work_ids
        )

    async def _setup_checkpoint(self):
        """Set up or resume checkpoint."""
        # Check for existing checkpoint
        if self.checkpoint_manager.exists() and not self.config.fresh:
            existing = self.checkpoint_manager.load()
            if existing:
                # Verify compatibility
                format_match = existing.content_format == self.config.content_format.value
                filter_match = existing.filter_str == self.config.filter_str

                if not format_match or not filter_match:
                    if self.progress_tracker:
                        self.progress_tracker.log_warning(
                            "Existing checkpoint has different settings."
                        )
                        self.progress_tracker.log_warning(
                            f"  Checkpoint: format={existing.content_format}, filter={existing.filter_str}"
                        )
                        self.progress_tracker.log_warning(
                            f"  Current: format={self.config.content_format.value}, filter={self.config.filter_str}"
                        )
                        self.progress_tracker.log_warning(
                            "Use --fresh to start over or match the checkpoint settings."
                        )
                    return None

                if self.config.resume:
                    if (
                        existing.expected_total_works is None
                        and self._should_prefetch_total_count()
                    ):
                        try:
                            existing.expected_total_works = await self.api_client.get_work_count(
                                filter_str=self.config.filter_str,
                                content_format=self.config.content_format,
                            )
                            self.checkpoint_manager.force_save()
                            if self.progress_tracker:
                                self.progress_tracker.log_info(
                                    "Backfilled expected total works for this existing checkpoint: "
                                    f"{existing.expected_total_works:,}"
                                )
                        except Exception as e:
                            if self.progress_tracker:
                                self.progress_tracker.log_warning(
                                    "Could not backfill total work count for this existing checkpoint: "
                                    f"{e}"
                                )

                    if self.progress_tracker:
                        completed = len(existing.completed_work_ids)
                        unresolved_failed = len(existing.failed_work_ids)
                        expected_total = existing.expected_total_works
                        total_suffix = (
                            f" of {expected_total:,} expected" if expected_total is not None else ""
                        )
                        self.progress_tracker.log_info(
                            f"Resuming from checkpoint: {completed} completed, "
                            f"{unresolved_failed} unresolved failed{total_suffix}"
                        )
                    return existing

        # Create new checkpoint
        expected_total_works = None
        if self._should_prefetch_total_count():
            try:
                expected_total_works = await self.api_client.get_work_count(
                    filter_str=self.config.filter_str,
                    content_format=self.config.content_format,
                )
                if self.progress_tracker:
                    self.progress_tracker.log_info(
                        f"Estimated total works for this download: {expected_total_works:,}"
                    )
            except Exception as e:
                if self.progress_tracker:
                    self.progress_tracker.log_warning(
                        f"Could not determine total work count for progress estimation: {e}"
                    )

        return self.checkpoint_manager.create(
            filter_str=self.config.filter_str,
            content_format=self.config.content_format.value,
            expected_total_works=expected_total_works,
        )

    async def _produce_work(self) -> None:
        """Paginate through API and queue work items, or yield provided IDs directly."""
        # ID-list mode: yield provided work IDs directly without API pagination
        if self.config.work_ids:
            await self._produce_work_from_ids()
            return

        # Sample mode: use page-based pagination with ?sample=N
        if self.config.sample:
            await self._produce_work_from_sample()
            return

        # Standard mode: paginate through API
        checkpoint = self.checkpoint_manager.get()
        cursor = checkpoint.current_cursor
        warned_about_flat = False
        total_count = 0

        if self._page_tracking_enabled:
            assert self._work_queue is not None
            for replay in self.page_tracker.replay_pending_work():
                await self._work_queue.put(
                    QueuedWork(
                        work=WorkItem(work_id=replay.work_id),
                        page_seq=replay.page_seq,
                    )
                )
                self.failure_injector.hit("after_replay_enqueue")
            cursor = self.page_tracker.resume_cursor(cursor) or cursor

        page_iter = self.api_client.list_works(
            filter_str=self.config.filter_str,
            content_format=self.config.content_format,
            cursor=cursor,
        )
        closable_page_iter = page_iter if isinstance(page_iter, AsyncGenerator) else None

        try:
            async for works, next_cursor in page_iter:
                if self._shutdown_requested:
                    break

                # Track total and warn about flat directory for large downloads
                total_count += len(works)
                if (
                    not warned_about_flat
                    and total_count > 10000
                    and not self.config.nested
                    and self.progress_tracker
                ):
                    self.progress_tracker.log_warning(
                        f"Downloading {total_count:,}+ files to flat directory. "
                        "Consider using --nested for better filesystem performance."
                    )
                    warned_about_flat = True

                page_seq: int | None = None
                queueable_works = [
                    work for work in works if not self.checkpoint_manager.is_completed(work.work_id)
                ]
                work_ids = [work.work_id for work in queueable_works]
                if self._page_tracking_enabled and work_ids:
                    page_seq = self.page_tracker.register_page(
                        cursor_in=cursor,
                        cursor_out=next_cursor,
                        work_ids=work_ids,
                    ).seq
                    self.failure_injector.hit("after_page_register")

                for work in queueable_works:
                    if self._shutdown_requested:
                        break

                    assert self._work_queue is not None
                    await self._work_queue.put(QueuedWork(work=work, page_seq=page_seq))

                if not self._page_tracking_enabled:
                    self.checkpoint_manager.update_cursor(next_cursor)

                if self.progress_tracker and not self._page_tracking_enabled:
                    self.progress_tracker.update_pagination(
                        pages=checkpoint.pages_completed,
                        cursor=next_cursor,
                    )

                cursor = next_cursor or "*"

        except CreditsExhaustedError:
            self._handle_credits_exhausted()
        except Exception as e:
            if self.progress_tracker:
                self.progress_tracker.log_error(f"Error listing works: {e}")
            raise
        finally:
            if closable_page_iter is not None:
                await closable_page_iter.aclose()

    async def _produce_work_from_sample(self) -> None:
        """Queue work items from a random sample (page-based pagination)."""
        total_count = 0
        warned_about_flat = False
        sample_size = self.config.sample

        if sample_size is None:
            return

        try:
            async for works in self.api_client.list_works_sample(
                sample_size=sample_size,
                seed=self.config.seed,
                filter_str=self.config.filter_str,
                content_format=self.config.content_format,
            ):
                if self._shutdown_requested:
                    break

                total_count += len(works)
                if (
                    not warned_about_flat
                    and total_count > 10000
                    and not self.config.nested
                    and self.progress_tracker
                ):
                    self.progress_tracker.log_warning(
                        f"Downloading {total_count:,}+ files to flat directory. "
                        "Consider using --nested for better filesystem performance."
                    )
                    warned_about_flat = True

                for work in works:
                    if self._shutdown_requested:
                        break

                    # Skip already completed
                    if self.checkpoint_manager.is_completed(work.work_id):
                        continue

                    assert self._work_queue is not None
                    await self._work_queue.put(QueuedWork(work=work))

                if self.progress_tracker:
                    self.progress_tracker.log_info(f"Queued {total_count} works from sample...")

        except CreditsExhaustedError:
            self._handle_credits_exhausted()
        except Exception as e:
            if self.progress_tracker:
                self.progress_tracker.log_error(f"Error listing sample works: {e}")

    async def _produce_work_from_ids(self) -> None:
        """Queue work items from provided ID list (no API pagination)."""
        if not self.config.work_ids:
            return

        for work_id in self.config.work_ids:
            if self._shutdown_requested:
                break

            if self.checkpoint_manager.is_completed(work_id):
                continue

            assert self._work_queue is not None
            await self._work_queue.put(QueuedWork(work=WorkItem(work_id=work_id)))

    async def _run_direct_work_id_phase(self, work_ids: list[str], workers: int) -> None:
        """Run a work-list phase using direct work IDs and existing worker/result logic."""
        self._work_queue = asyncio.Queue()
        self._results_queue = asyncio.Queue()

        worker_tasks = [asyncio.create_task(self._download_worker(i)) for i in range(workers)]
        result_processor = asyncio.create_task(self._process_results())

        try:
            for work_id in work_ids:
                if self._shutdown_requested:
                    break

                if self.checkpoint_manager.is_completed(work_id):
                    continue

                await self._work_queue.put(QueuedWork(work=WorkItem(work_id=work_id)))

            for _ in range(workers):
                await self._work_queue.put(None)

            await asyncio.gather(*worker_tasks)

            await self._results_queue.put(
                DownloadResult(work_id="__DONE__", format=ContentFormat.NONE, success=True)
            )
            await result_processor
        finally:
            for task in worker_tasks:
                if not task.done():
                    task.cancel()
            if not result_processor.done():
                result_processor.cancel()

    async def _run_failed_retry_phase(self) -> None:
        """Retry unresolved failed metadata downloads using a smaller worker pool."""
        failed_ids = self.checkpoint_manager.get_failed_work_ids()
        if not failed_ids:
            if self.progress_tracker:
                self.progress_tracker.log_info("No failed works to retry.")
            return

        retry_workers = self._get_retry_workers()
        original_rate_limiter = self.rate_limiter
        self.rate_limiter = AdaptiveRateLimiter(max_workers=retry_workers)

        if self.progress_tracker:
            self.progress_tracker.log_info(
                f"Retrying {len(failed_ids)} unresolved failed works with {retry_workers} workers"
            )

        self._retry_failed_phase = True
        try:
            await self._run_direct_work_id_phase(
                work_ids=failed_ids,
                workers=retry_workers,
            )
        finally:
            self._retry_failed_phase = False
            self.rate_limiter = original_rate_limiter

        remaining = len(self.checkpoint_manager.get_failed_work_ids())
        recovered = len(failed_ids) - remaining

        if self.progress_tracker:
            self.progress_tracker.record_retry_summary(
                attempted=len(failed_ids),
                recovered=recovered,
                remaining=remaining,
            )
            self.progress_tracker.log_info(
                f"Retry phase complete: recovered {recovered}, remaining unresolved {remaining}"
            )

    async def _download_worker(self, worker_id: int) -> None:
        """Worker that downloads metadata and optionally content."""
        while not self._shutdown_requested:
            try:
                assert self._work_queue is not None
                queued = await asyncio.wait_for(self._work_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if queued is None:
                break

            work = queued.work

            # Determine filename base: use original DOI if provided, else work_id
            filename_base = work.work_id
            if (
                self.config.original_identifiers
                and work.work_id in self.config.original_identifiers
            ):
                orig = self.config.original_identifiers[work.work_id]
                # If original was a DOI, use DOI-based filename
                if orig.startswith("10."):
                    filename_base = doi_to_filename(orig)

            # Always save full metadata (fetch from singleton API)
            meta_content = b""
            try:
                full_metadata = await self.api_client.get_work_metadata_with_retry(work.work_id)
                meta_path = str(work_id_to_path(filename_base, "json", nested=self.config.nested))
                meta_content = json.dumps(full_metadata, indent=2).encode()
                await self.storage.save(meta_path, meta_content, "application/json")
            except CreditsExhaustedError:
                self._handle_credits_exhausted()
                break
            except Exception as e:
                assert self._results_queue is not None
                await self._results_queue.put(
                    DownloadResult(
                        work_id=work.work_id,
                        format=ContentFormat.NONE,
                        success=False,
                        error=f"Failed to fetch metadata: {e}",
                        page_seq=queued.page_seq,
                    )
                )
                continue

            # If no content requested, we're done with this work
            if self.config.content_format == ContentFormat.NONE:
                # Report success for metadata-only download
                result = DownloadResult(
                    work_id=work.work_id,
                    format=ContentFormat.NONE,
                    success=True,
                    file_size=len(meta_content),
                    credits_cost=0,  # Singleton API is free
                    page_seq=queued.page_seq,
                )
                assert self._results_queue is not None
                await self._results_queue.put(result)
                continue

            # Determine content formats to download
            formats = []
            if (
                self.config.content_format in (ContentFormat.PDF, ContentFormat.BOTH)
                and work.has_pdf
            ):
                formats.append(ContentFormat.PDF)
            if (
                self.config.content_format in (ContentFormat.XML, ContentFormat.BOTH)
                and work.has_xml
            ):
                formats.append(ContentFormat.XML)

            # If no content available for requested formats, report success (metadata was saved)
            if not formats:
                result = DownloadResult(
                    work_id=work.work_id,
                    format=self.config.content_format,
                    success=True,
                    file_size=len(meta_content),
                    credits_cost=0,
                    page_seq=queued.page_seq,
                )
                assert self._results_queue is not None
                await self._results_queue.put(result)
                continue

            for fmt in formats:
                if self._credits_exhausted:
                    break

                await self.rate_limiter.acquire()
                start_time = time.time()

                try:
                    result = await self.api_client.download_content(work.work_id, fmt)
                    latency_ms = (time.time() - start_time) * 1000

                    await self.rate_limiter.record_request(
                        latency_ms=latency_ms,
                        success=result.success,
                        rate_limit_remaining=result.rate_limit_remaining,
                    )

                    if result.error == "Credits exhausted":
                        self._handle_credits_exhausted()
                        break

                    if result.success and result.content:
                        # Save content
                        ext = "pdf" if fmt == ContentFormat.PDF else "tei.xml"
                        path = str(work_id_to_path(filename_base, ext, nested=self.config.nested))
                        content_type = (
                            "application/pdf" if fmt == ContentFormat.PDF else "application/xml"
                        )
                        await self.storage.save(path, result.content, content_type)

                    result.page_seq = queued.page_seq
                    assert self._results_queue is not None
                    await self._results_queue.put(result)

                except Exception as e:
                    result = DownloadResult(
                        work_id=work.work_id,
                        format=fmt,
                        success=False,
                        error=str(e),
                        page_seq=queued.page_seq,
                    )
                    assert self._results_queue is not None
                    await self._results_queue.put(result)

                finally:
                    self.rate_limiter.release()

    async def _process_results(self) -> None:
        """Process download results and update progress."""
        while True:
            assert self._results_queue is not None
            result = await self._results_queue.get()

            if result.work_id == "__DONE__":
                break

            if self._page_tracking_enabled and result.page_seq is not None:
                commit = self.page_tracker.record_work_result(
                    page_seq=result.page_seq,
                    work_id=result.work_id,
                    success=result.success,
                    file_size=result.file_size,
                    credits=result.credits_cost,
                    error=result.error,
                )
                self.page_tracker.flush_manifest(result.page_seq)
                self.failure_injector.hit("after_result_record")
                if self.progress_tracker and commit.committed_pages:
                    self.progress_tracker.update_pagination(
                        pages=commit.pages_completed,
                        cursor=commit.current_cursor,
                    )
                    self.failure_injector.hit("after_page_commit")
                self._sync_progress_checkpoint_state()
            else:
                if self._retry_failed_phase:
                    if result.success:
                        self.checkpoint_manager.resolve_failed_work(
                            result.work_id,
                            result.file_size,
                            result.credits_cost,
                        )
                    else:
                        self.checkpoint_manager.record_failed_retry(result.work_id)
                else:
                    if result.success:
                        self.checkpoint_manager.mark_completed(
                            result.work_id,
                            result.file_size,
                            result.credits_cost,
                        )
                    else:
                        self.checkpoint_manager.mark_failed(result.work_id)
                self._sync_progress_checkpoint_state()

            if self.progress_tracker:
                self.progress_tracker.update_download(
                    work_id=result.work_id,
                    success=result.success,
                    file_size=result.file_size,
                    error=result.error,
                    rate_limiter_state=self.rate_limiter.get_state(),
                )


class MultiFilterOrchestrator:
    """Orchestrates multiple filter downloads into a single output directory."""

    def __init__(
        self,
        api_key: str,
        output_path: str,
        filters: list[dict],
        content_format: ContentFormat = ContentFormat.NONE,
        workers: int = 50,
        resume: bool = True,
        fresh: bool = False,
        quiet: bool = False,
        verbose: bool = False,
        nested: bool = False,
        retry_failed: bool = False,
        retry_workers: int | None = None,
        resume_filter: str | None = None,
    ):
        self.api_key = api_key
        self.output_path = output_path
        self.filters = filters
        self.content_format = content_format
        self.workers = workers
        self.resume = resume
        self.fresh = fresh
        self.quiet = quiet
        self.verbose = verbose
        self.nested = nested
        self.retry_failed = retry_failed
        self.retry_workers = retry_workers
        self.resume_filter = resume_filter

    async def run(self, progress_tracker: ProgressTracker | None = None) -> None:
        """Run downloads for all filters."""
        from .checkpoint import CheckpointManager

        checkpoint_manager = CheckpointManager(self.output_path)

        # Load or create checkpoint
        if checkpoint_manager.exists() and not self.fresh:
            checkpoint = checkpoint_manager.load()
        else:
            checkpoint = None

        if checkpoint is None:
            # Create new multi-filter checkpoint
            from .checkpoint import Checkpoint

            checkpoint = Checkpoint(mode="multi")
            checkpoint_manager._checkpoint = checkpoint
            checkpoint_manager.force_save()

        # If resume_filter specified, only run that filter
        filters_to_run = self.filters
        if self.resume_filter:
            filters_to_run = [f for f in self.filters if f.get("name") == self.resume_filter]
            if not filters_to_run:
                if progress_tracker:
                    progress_tracker.log_error(
                        f"Filter '{self.resume_filter}' not found in configuration"
                    )
                return

        # Run each filter
        for filter_config in filters_to_run:
            filter_name = filter_config.get("name", "unnamed")
            filter_str = filter_config.get("filter", "")
            filter_id = filter_config.get("id", "")

            if progress_tracker:
                progress_tracker.log_info(f"Starting filter: {filter_name}")

            # Find existing filter by ID (hash-based matching)
            existing_filter = None
            for f in checkpoint.filters:
                if f.id == filter_id:
                    existing_filter = f
                    break

            # Handle stalled filters: retry them automatically
            if existing_filter:
                if existing_filter.status == "complete":
                    if progress_tracker:
                        progress_tracker.log_info(
                            f"Filter '{filter_name}' already complete, skipping"
                        )
                    continue
                elif existing_filter.status == "stalled":
                    retry_count = getattr(existing_filter.stats, "retry_count", 0)
                    if retry_count >= 3:
                        if progress_tracker:
                            progress_tracker.log_warning(
                                f"Filter '{filter_name}' stalled after {retry_count} retries, skipping"
                            )
                        continue
                    if progress_tracker:
                        progress_tracker.log_info(
                            f"Filter '{filter_name}' was stalled, retrying (attempt {retry_count + 1}/3)"
                        )
                    existing_filter.status = "active"

            # Create config for this filter
            config = DownloadConfig(
                api_key=self.api_key,
                output_path=self.output_path,
                filter_str=filter_str,
                content_format=self.content_format,
                workers=self.workers,
                resume=self.resume,
                fresh=self.fresh,
                quiet=self.quiet,
                verbose=self.verbose,
                nested=self.nested,
                retry_failed=self.retry_failed,
                retry_workers=self.retry_workers,
            )

            # Create orchestrator and run
            orchestrator = DownloadOrchestrator(config)

            try:
                if progress_tracker:
                    progress_tracker.set_current_filter(filter_name)
                await orchestrator.run(progress_tracker=progress_tracker)
            except Exception as e:
                if progress_tracker:
                    progress_tracker.log_error(
                        f"Filter '{filter_name}' failed: {e}. Continuing with next filter."
                    )
                # Mark filter as stalled and increment retry count
                if existing_filter:
                    existing_filter.status = "stalled"
                    existing_filter.stats.retry_count = (
                        getattr(existing_filter.stats, "retry_count", 0) + 1
                    )
                else:
                    from .checkpoint import FilterCheckpoint

                    new_filter = FilterCheckpoint(
                        id=filter_id,
                        name=filter_name,
                        filter_str=filter_str,
                        status="stalled",
                    )
                    new_filter.stats.retry_count = 1
                    checkpoint.filters.append(new_filter)
                checkpoint_manager.force_save()
                continue

            # Sync per-filter state from child orchestrator
            child_checkpoint = orchestrator.checkpoint_manager.get()
            if existing_filter:
                existing_filter.completed_work_ids.update(child_checkpoint.completed_work_ids)
                existing_filter.failed_work_ids.update(child_checkpoint.failed_work_ids)
                existing_filter.current_cursor = child_checkpoint.current_cursor
                existing_filter.pages_completed = child_checkpoint.pages_completed
                existing_filter.status = "complete"
            else:
                from .checkpoint import FilterCheckpoint

                new_filter = FilterCheckpoint(
                    id=filter_id,
                    name=filter_name,
                    filter_str=filter_str,
                    completed_work_ids=set(child_checkpoint.completed_work_ids),
                    failed_work_ids=set(child_checkpoint.failed_work_ids),
                    current_cursor=child_checkpoint.current_cursor,
                    pages_completed=child_checkpoint.pages_completed,
                    status="complete",
                )
                checkpoint.filters.append(new_filter)
            checkpoint_manager.force_save()

            if progress_tracker:
                progress_tracker.log_info(f"Filter '{filter_name}' complete")

        if progress_tracker:
            progress_tracker.set_current_filter(None)
