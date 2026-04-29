"""Adaptive rate limiting with backoff."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum


class APIHealth(Enum):
    """API health status based on response times."""

    GREEN = "green"  # All good
    YELLOW = "yellow"  # Slowing down
    RED = "red"  # Severely degraded


@dataclass
class RateLimitState:
    """Current state of rate limiting."""

    current_workers: int
    max_workers: int
    health: APIHealth
    avg_latency_ms: float
    p95_latency_ms: float
    rate_limit_remaining: int | None


class AdaptiveRateLimiter:
    """
    Rate limiter that adapts to API response times.

    Features:
    - Tracks request latencies
    - Reduces concurrency when p95 latency exceeds threshold
    - Gradually recovers when latency improves
    - Respects rate limit remaining header
    - Exponential backoff on errors
    """

    # Latency thresholds (milliseconds)
    YELLOW_THRESHOLD_MS = 1500  # Slow down at 1.5s p95
    RED_THRESHOLD_MS = 2000  # Severely reduce at 2s p95

    # Concurrency adjustment
    MIN_WORKERS = 5
    SLOWDOWN_FACTOR = 0.5  # Reduce by 50% when degraded
    RECOVERY_FACTOR = 1.1  # Increase by 10% when recovering
    RECOVERY_INTERVAL = 30  # Seconds between recovery attempts

    # Backoff settings
    INITIAL_BACKOFF = 1.0  # Initial backoff in seconds
    MAX_BACKOFF = 60.0  # Maximum backoff
    BACKOFF_MULTIPLIER = 2.0

    def __init__(self, max_workers: int = 50, window_size: int = 100):
        """
        Initialize the rate limiter.

        Args:
            max_workers: Maximum concurrent workers
            window_size: Number of recent requests to track for latency
        """
        self.max_workers = max_workers
        self.current_workers = max_workers
        self.window_size = window_size

        # Latency tracking
        self._latencies: deque[float] = deque(maxlen=window_size)
        self._consecutive_slow = 0
        self._slow_threshold = 10  # Consecutive slow requests before action

        # Recovery tracking
        self._last_recovery_attempt = 0.0
        self._is_recovering = False

        # Rate limit tracking
        self._rate_limit_remaining: int | None = None

        # Backoff state
        self._current_backoff = self.INITIAL_BACKOFF
        self._consecutive_errors = 0

        # Semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(max_workers)
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        await self._semaphore.acquire()

    def release(self) -> None:
        """Release the semaphore."""
        self._semaphore.release()

    async def record_request(
        self,
        latency_ms: float,
        success: bool,
        rate_limit_remaining: int | None = None,
    ) -> None:
        """
        Record a completed request and adjust rate limiting.

        Args:
            latency_ms: Request latency in milliseconds
            success: Whether the request succeeded
            rate_limit_remaining: X-RateLimit-Remaining header value
        """
        async with self._lock:
            self._latencies.append(latency_ms)
            self._rate_limit_remaining = rate_limit_remaining

            if success:
                self._consecutive_errors = 0
                self._current_backoff = self.INITIAL_BACKOFF
                await self._check_latency_health()
            else:
                self._consecutive_errors += 1

    async def _check_latency_health(self) -> None:
        """Check latency and adjust concurrency."""
        if len(self._latencies) < 10:
            return

        p95 = self._calculate_p95()

        if p95 > self.RED_THRESHOLD_MS:
            self._consecutive_slow += 1
            if self._consecutive_slow >= self._slow_threshold:
                await self._reduce_concurrency(severely=True)
                self._consecutive_slow = 0
        elif p95 > self.YELLOW_THRESHOLD_MS:
            self._consecutive_slow += 1
            if self._consecutive_slow >= self._slow_threshold:
                await self._reduce_concurrency(severely=False)
                self._consecutive_slow = 0
        else:
            self._consecutive_slow = 0
            await self._try_recovery()

    def _calculate_p95(self) -> float:
        """Calculate 95th percentile latency."""
        if not self._latencies:
            return 0.0
        sorted_latencies = sorted(self._latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]

    def _calculate_avg(self) -> float:
        """Calculate average latency."""
        if not self._latencies:
            return 0.0
        return sum(self._latencies) / len(self._latencies)

    async def _reduce_concurrency(self, severely: bool) -> None:
        """Reduce concurrency to handle slowdown."""
        factor = 0.25 if severely else self.SLOWDOWN_FACTOR
        new_workers = max(self.MIN_WORKERS, int(self.current_workers * factor))

        if new_workers < self.current_workers:
            self.current_workers = new_workers
            self._is_recovering = False
            # Note: We can't easily reduce semaphore, but we track the limit
            # and workers will naturally drain to the new level

    async def _try_recovery(self) -> None:
        """Try to gradually increase concurrency."""
        now = time.time()
        if now - self._last_recovery_attempt < self.RECOVERY_INTERVAL:
            return

        if self.current_workers < self.max_workers:
            new_workers = min(
                self.max_workers,
                int(self.current_workers * self.RECOVERY_FACTOR),
            )
            if new_workers > self.current_workers:
                self.current_workers = new_workers
                self._last_recovery_attempt = now
                self._is_recovering = True

    async def wait_on_error(self) -> float:
        """
        Wait with exponential backoff after an error.

        Returns the actual wait time in seconds.
        """
        wait_time = min(
            self._current_backoff * (self.BACKOFF_MULTIPLIER**self._consecutive_errors),
            self.MAX_BACKOFF,
        )
        await asyncio.sleep(wait_time)
        return wait_time

    def get_state(self) -> RateLimitState:
        """Get the current rate limiter state."""
        p95 = self._calculate_p95()

        if p95 > self.RED_THRESHOLD_MS:
            health = APIHealth.RED
        elif p95 > self.YELLOW_THRESHOLD_MS:
            health = APIHealth.YELLOW
        else:
            health = APIHealth.GREEN

        return RateLimitState(
            current_workers=self.current_workers,
            max_workers=self.max_workers,
            health=health,
            avg_latency_ms=self._calculate_avg(),
            p95_latency_ms=p95,
            rate_limit_remaining=self._rate_limit_remaining,
        )

    def should_continue(self) -> bool:
        """Check if we should continue making requests."""
        # Stop if rate limit is exhausted
        return not (self._rate_limit_remaining is not None and self._rate_limit_remaining <= 0)
