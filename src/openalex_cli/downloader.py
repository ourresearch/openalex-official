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
            checkpoint = self._setup_checkpoint()
            if checkpoint is None:
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

        finally:
            # Clean up
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

    def _setup_checkpoint(self):
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
                    if self.progress_tracker:
                        stats = existing.stats
                        self.progress_tracker.log_info(
                            f"Resuming from checkpoint: {stats.total_downloaded} downloaded, "
                            f"{stats.total_failed} failed"
                        )
                    return existing

        # Create new checkpoint
        return self.checkpoint_manager.create(
            filter_str=self.config.filter_str,
            content_format=self.config.content_format.value,
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

        # Fetch metadata for each work ID from the list API
        # We need basic info like has_pdf/has_xml
        for work_id in self.config.work_ids:
            if self._shutdown_requested:
                break

            # Skip already completed
            if self.checkpoint_manager.is_completed(work_id):
                continue

            try:
                # Fetch work metadata from singleton API
                metadata = await self.api_client.get_work_metadata_with_retry(work_id)
                work = WorkItem.from_api_response(metadata)
                assert self._work_queue is not None
                await self._work_queue.put(QueuedWork(work=work))
            except CreditsExhaustedError:
                self._handle_credits_exhausted()
                break
            except Exception as e:
                if self.progress_tracker:
                    self.progress_tracker.log_error(f"Error fetching metadata for {work_id}: {e}")
                assert self._results_queue is not None
                await self._results_queue.put(
                    DownloadResult(
                        work_id=work_id,
                        format=ContentFormat.NONE,
                        success=False,
                        error=f"Failed to fetch metadata: {e}",
                    )
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
                self.failure_injector.hit("after_result_record")
                if self.progress_tracker and commit.committed_pages:
                    self.progress_tracker.update_pagination(
                        pages=commit.pages_completed,
                        cursor=commit.current_cursor,
                    )
                    self.failure_injector.hit("after_page_commit")
            else:
                if result.success:
                    self.checkpoint_manager.mark_completed(
                        result.work_id,
                        result.file_size,
                        result.credits_cost,
                    )
                else:
                    self.checkpoint_manager.mark_failed(result.work_id)

            if self.progress_tracker:
                self.progress_tracker.update_download(
                    work_id=result.work_id,
                    success=result.success,
                    file_size=result.file_size,
                    error=result.error,
                    rate_limiter_state=self.rate_limiter.get_state(),
                )
