"""Controlled failure hooks for smoke-testing downloader recovery."""

from __future__ import annotations

import os


class InjectedFailure(RuntimeError):
    """Raised when a configured failure hook is triggered."""


class FailureInjector:
    """One-shot failure injection controlled by environment variables."""

    POINTS_ENV = "OPENALEX_FAILPOINTS"
    COUNTS_ENV = "OPENALEX_FAILCOUNTS"

    def __init__(self) -> None:
        self._points = self._parse_points(os.getenv(self.POINTS_ENV, ""))
        self._counts = self._parse_counts(os.getenv(self.COUNTS_ENV, ""))
        self._hits: dict[str, int] = {}

    def hit(self, point: str) -> None:
        """Trigger a configured failure once the failpoint threshold is reached."""
        if point not in self._points:
            return

        hit_count = self._hits.get(point, 0) + 1
        self._hits[point] = hit_count
        target = self._counts.get(point, 1)
        if hit_count >= target:
            raise InjectedFailure(f"Injected failure at failpoint '{point}'")

    @staticmethod
    def _parse_points(raw: str) -> set[str]:
        return {point.strip() for point in raw.split(",") if point.strip()}

    @staticmethod
    def _parse_counts(raw: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in raw.split(","):
            item = entry.strip()
            if not item or ":" not in item:
                continue

            name, value = item.split(":", 1)
            point = name.strip()
            if not point:
                continue

            try:
                count = int(value)
            except ValueError:
                continue

            if count > 0:
                counts[point] = count
        return counts
