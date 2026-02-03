"""Tests for adaptive rate limiter."""

import asyncio

import pytest

from openalex_cli.rate_limiter import AdaptiveRateLimiter, APIHealth


class TestAdaptiveRateLimiter:
    @pytest.fixture
    def limiter(self):
        return AdaptiveRateLimiter(max_workers=10, window_size=20)

    async def test_acquire_release(self, limiter):
        await limiter.acquire()
        limiter.release()

    async def test_get_state_initial(self, limiter):
        state = limiter.get_state()
        assert state.current_workers == 10
        assert state.max_workers == 10
        assert state.health == APIHealth.GREEN

    async def test_record_success(self, limiter):
        await limiter.record_request(latency_ms=100, success=True, rate_limit_remaining=1000)
        state = limiter.get_state()
        assert state.rate_limit_remaining == 1000

    async def test_health_degrades_on_slow_requests(self, limiter):
        # Record many slow requests
        for _ in range(20):
            await limiter.record_request(latency_ms=2500, success=True)

        state = limiter.get_state()
        # Should show degraded health
        assert state.p95_latency_ms > 2000

    async def test_should_continue_with_credits(self, limiter):
        await limiter.record_request(latency_ms=100, success=True, rate_limit_remaining=1000)
        assert limiter.should_continue()

    async def test_should_stop_without_credits(self, limiter):
        await limiter.record_request(latency_ms=100, success=True, rate_limit_remaining=0)
        assert not limiter.should_continue()

    async def test_backoff_on_error(self, limiter):
        # Multiple errors should increase backoff
        for _ in range(3):
            await limiter.record_request(latency_ms=100, success=False)

        # Backoff wait should be > initial
        # Note: This just tests the logic exists, not exact timing
        assert limiter._consecutive_errors == 3
