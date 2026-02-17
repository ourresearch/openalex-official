"""Core async download orchestrator."""

from __future__ import annotations

import asyncio
import json
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .api_client import CreditsExhaustedError, DownloadResult, OpenAlexAPIClient, WorkItem
from .checkpoint import CheckpointManager
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
        self.progress_tracker: ProgressTracker | None = None

        # Initialize storage backend
        if config.storage_type == StorageType.S3:
            if not config.s3_bucket:
                raise ValueError("S3 bucket required for S3 storage")
            self.storage: StorageBackend = S3Storage(
                bucket=config.s3_bucket,
                prefix=config.s3_prefix,
            )
        else:
            self.storage = LocalStorage(config.output_path)

        # Control flags
        self._shutdown_requested = False
        self._credits_exhausted = False
        # Queues are created in run() to ensure they're in the correct event loop
        self._work_queue: asyncio.Queue[WorkItem | None] | None = None
        self._results_queue: asyncio.Queue[DownloadResult] | None = None

    async def run(self, progress_tracker: ProgressTracker | None = None) -> None:
        """Run the download job."""
        self.progress_tracker = progress_tracker

        # Create queues in the running event loop (Python 3.9 compatibility)
        self._work_queue = asyncio.Queue()
        self._results_queue = asyncio.Queue()

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_shutdown)

        try:
            # Initialize or resume checkpoint
            checkpoint = self._setup_checkpoint()
            if checkpoint is None:
                return

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
                asyncio.create_task(self._download_worker(i))
                for i in range(self.config.workers)
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

            # Remove signal handlers
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    def _request_shutdown(self) -> None:
        """Handle shutdown signal."""
        if self._shutdown_requested:
            # Second signal - force exit
            raise KeyboardInterrupt()
        self._shutdown_requested = True
        if self.progress_tracker:
            self.progress_tracker.log_warning(
                "Shutdown requested. Finishing current downloads..."
            )

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

        # Standard mode: paginate through API
        checkpoint = self.checkpoint_manager.get()
        cursor = checkpoint.current_cursor
        warned_about_flat = False
        total_count = 0

        try:
            async for works, next_cursor in self.api_client.list_works(
                filter_str=self.config.filter_str,
                content_format=self.config.content_format,
                cursor=cursor,
            ):
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

                for work in works:
                    if self._shutdown_requested:
                        break

                    # Skip already completed
                    if self.checkpoint_manager.is_completed(work.work_id):
                        continue

                    await self._work_queue.put(work)

                # Update cursor checkpoint
                self.checkpoint_manager.update_cursor(next_cursor)

                if self.progress_tracker:
                    self.progress_tracker.update_pagination(
                        pages=checkpoint.pages_completed,
                        cursor=next_cursor,
                    )

        except CreditsExhaustedError:
            self._handle_credits_exhausted()
        except Exception as e:
            if self.progress_tracker:
                self.progress_tracker.log_error(f"Error listing works: {e}")

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
                metadata = await self.api_client.get_work_metadata(work_id)
                work = WorkItem.from_api_response(metadata)
                await self._work_queue.put(work)
            except Exception as e:
                if self.progress_tracker:
                    self.progress_tracker.log_error(f"Error fetching metadata for {work_id}: {e}")

    async def _download_worker(self, worker_id: int) -> None:
        """Worker that downloads metadata and optionally content."""
        while not self._shutdown_requested:
            try:
                work = await asyncio.wait_for(self._work_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if work is None:
                break

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
            try:
                full_metadata = await self.api_client.get_work_metadata(work.work_id)
                meta_path = str(
                    work_id_to_path(filename_base, "json", nested=self.config.nested)
                )
                meta_content = json.dumps(full_metadata, indent=2).encode()
                await self.storage.save(meta_path, meta_content, "application/json")
            except CreditsExhaustedError:
                self._handle_credits_exhausted()
                break
            except Exception as e:
                if self.progress_tracker:
                    self.progress_tracker.log_warning(
                        f"Failed to fetch metadata for {work.work_id}: {e}"
                    )

            # If no content requested, we're done with this work
            if self.config.content_format == ContentFormat.NONE:
                # Report success for metadata-only download
                result = DownloadResult(
                    work_id=work.work_id,
                    format=ContentFormat.NONE,
                    success=True,
                    file_size=len(meta_content) if meta_content else 0,
                    credits_cost=0,  # Singleton API is free
                )
                await self._results_queue.put(result)
                continue

            # Determine content formats to download
            formats = []
            if self.config.content_format in (ContentFormat.PDF, ContentFormat.BOTH):
                if work.has_pdf:
                    formats.append(ContentFormat.PDF)
            if self.config.content_format in (ContentFormat.XML, ContentFormat.BOTH):
                if work.has_xml:
                    formats.append(ContentFormat.XML)

            # If no content available for requested formats, report success (metadata was saved)
            if not formats:
                result = DownloadResult(
                    work_id=work.work_id,
                    format=self.config.content_format,
                    success=True,
                    file_size=len(meta_content) if meta_content else 0,
                    credits_cost=0,
                )
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

                    await self._results_queue.put(result)

                except Exception as e:
                    result = DownloadResult(
                        work_id=work.work_id,
                        format=fmt,
                        success=False,
                        error=str(e),
                    )
                    await self._results_queue.put(result)

                finally:
                    self.rate_limiter.release()

    async def _process_results(self) -> None:
        """Process download results and update progress."""
        while True:
            result = await self._results_queue.get()

            if result.work_id == "__DONE__":
                break

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
