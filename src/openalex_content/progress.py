"""Progress tracking and display."""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from .rate_limiter import APIHealth, RateLimitState
from .utils import format_bytes, format_count, format_rate


@dataclass
class ProgressStats:
    """Current progress statistics."""

    total_downloaded: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    total_bytes: int = 0
    start_time: float = 0.0
    pages_completed: int = 0
    current_cursor: str | None = None


class ProgressTracker:
    """Tracks and displays download progress."""

    def __init__(
        self,
        output_dir: str | Path,
        quiet: bool = False,
        verbose: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.quiet = quiet
        self.verbose = verbose
        self.is_tty = sys.stdout.isatty() and not quiet

        # Statistics
        self.stats = ProgressStats(start_time=time.time())

        # Rate limiter state
        self._rate_state: RateLimitState | None = None

        # Set up logging
        self._setup_logging()

        # Rich components for TTY mode
        self._console = Console()
        self._live: Live | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None

    def _setup_logging(self) -> None:
        """Set up file and console logging."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.output_dir / "openalex-download.log"

        # Create logger
        self._logger = logging.getLogger("openalex-content")
        self._logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        self._logger.handlers = []

        # File handler (always)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        self._logger.addHandler(file_handler)

        # Console handler (headless mode only)
        if not self.is_tty:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter("%(asctime)s - %(message)s")
            console_handler.setFormatter(console_formatter)
            self._logger.addHandler(console_handler)

    def start(self) -> None:
        """Start the progress display."""
        self._logger.info("Download started")

        if self.is_tty:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]Downloading"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("{task.fields[stats]}"),
                TimeElapsedColumn(),
                console=self._console,
                expand=True,
            )
            self._task_id = self._progress.add_task(
                "download",
                total=None,  # Unknown total
                stats="Starting...",
            )
            self._live = Live(
                self._make_display(),
                console=self._console,
                refresh_per_second=4,
            )
            self._live.start()

    def stop(self) -> None:
        """Stop the progress display and show summary."""
        if self._live:
            self._live.stop()

        self._log_summary()

    def update_download(
        self,
        work_id: str,
        success: bool,
        file_size: int = 0,
        error: str | None = None,
        rate_limiter_state: RateLimitState | None = None,
    ) -> None:
        """Update progress after a download attempt."""
        if success:
            self.stats.total_downloaded += 1
            self.stats.total_bytes += file_size
            if self.verbose:
                self._logger.debug(f"Downloaded: {work_id} ({format_bytes(file_size)})")
        else:
            self.stats.total_failed += 1
            self._logger.warning(f"Failed: {work_id} - {error}")

        self._rate_state = rate_limiter_state
        self._refresh_display()

        # Log periodic summary in headless mode
        if not self.is_tty:
            total = self.stats.total_downloaded + self.stats.total_failed
            if total % 1000 == 0:
                self._log_periodic_summary()

    def update_pagination(self, pages: int, cursor: str | None) -> None:
        """Update pagination progress."""
        self.stats.pages_completed = pages
        self.stats.current_cursor = cursor
        self._refresh_display()

    def log_info(self, message: str) -> None:
        """Log an info message."""
        self._logger.info(message)
        if self.is_tty and self._live:
            self._console.print(f"[blue]ℹ[/blue] {message}")

    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        self._logger.warning(message)
        if self.is_tty and self._live:
            self._console.print(f"[yellow]⚠[/yellow] {message}")

    def log_error(self, message: str) -> None:
        """Log an error message."""
        self._logger.error(message)
        if self.is_tty and self._live:
            self._console.print(f"[red]✗[/red] {message}")

    def _refresh_display(self) -> None:
        """Refresh the Rich display."""
        if self._live and self._progress and self._task_id is not None:
            stats = self._format_stats()
            self._progress.update(self._task_id, stats=stats)
            self._live.update(self._make_display())

    def _make_display(self) -> Panel:
        """Create the display panel."""
        table = Table.grid(padding=(0, 2))
        table.add_column(justify="left")
        table.add_column(justify="right")

        # Stats
        elapsed = time.time() - self.stats.start_time
        rate = self.stats.total_bytes / elapsed if elapsed > 0 else 0
        files_per_sec = self.stats.total_downloaded / elapsed if elapsed > 0 else 0

        table.add_row(
            "Downloaded:",
            f"[green]{format_count(self.stats.total_downloaded)}[/green] files "
            f"({format_bytes(self.stats.total_bytes)})",
        )
        table.add_row(
            "Failed:",
            f"[red]{format_count(self.stats.total_failed)}[/red] files",
        )
        table.add_row(
            "Speed:",
            f"{format_rate(rate)} • {files_per_sec:.1f} files/s",
        )
        table.add_row(
            "Pages:",
            f"{self.stats.pages_completed} completed",
        )

        # API health
        if self._rate_state:
            health_color = {
                APIHealth.GREEN: "green",
                APIHealth.YELLOW: "yellow",
                APIHealth.RED: "red",
            }[self._rate_state.health]

            health_text = Text()
            health_text.append("●", style=health_color)
            health_text.append(
                f" {self._rate_state.health.value} "
                f"(p95: {self._rate_state.p95_latency_ms:.0f}ms, "
                f"workers: {self._rate_state.current_workers})"
            )
            table.add_row("API:", health_text)

            if self._rate_state.rate_limit_remaining is not None:
                table.add_row(
                    "Credits:",
                    f"{format_count(self._rate_state.rate_limit_remaining)} remaining",
                )

        # Add progress bar
        if self._progress:
            table.add_row("", "")
            table.add_row(self._progress, "")

        return Panel(
            table,
            title="[bold]OpenAlex Content Downloader[/bold]",
            border_style="blue",
        )

    def _format_stats(self) -> str:
        """Format stats for the progress bar."""
        elapsed = time.time() - self.stats.start_time
        files_per_sec = self.stats.total_downloaded / elapsed if elapsed > 0 else 0
        return (
            f"{format_count(self.stats.total_downloaded)} OK • "
            f"{format_count(self.stats.total_failed)} failed • "
            f"{files_per_sec:.1f}/s"
        )

    def _log_periodic_summary(self) -> None:
        """Log a periodic summary in headless mode."""
        elapsed = time.time() - self.stats.start_time
        rate = self.stats.total_bytes / elapsed if elapsed > 0 else 0
        files_per_sec = self.stats.total_downloaded / elapsed if elapsed > 0 else 0

        self._logger.info(
            f"Progress: {format_count(self.stats.total_downloaded)} downloaded, "
            f"{format_count(self.stats.total_failed)} failed, "
            f"{format_rate(rate)}, {files_per_sec:.1f} files/s"
        )

    def _log_summary(self) -> None:
        """Log the final summary."""
        elapsed = time.time() - self.stats.start_time
        rate = self.stats.total_bytes / elapsed if elapsed > 0 else 0

        summary = (
            f"\nDownload complete:\n"
            f"  Downloaded: {format_count(self.stats.total_downloaded)} files "
            f"({format_bytes(self.stats.total_bytes)})\n"
            f"  Failed: {format_count(self.stats.total_failed)} files\n"
            f"  Duration: {elapsed:.1f}s\n"
            f"  Average speed: {format_rate(rate)}"
        )

        self._logger.info(summary)
        if self.is_tty:
            self._console.print(summary)
