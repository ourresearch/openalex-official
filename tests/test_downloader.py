"""Tests for downloader metadata failure handling."""

from __future__ import annotations

import asyncio

import pytest

from openalex_cli.api_client import DownloadResult, WorkItem
from openalex_cli.downloader import DownloadConfig, DownloadOrchestrator
from openalex_cli.utils import ContentFormat


class DummyCheckpointManager:
    def is_completed(self, work_id: str) -> bool:
        return False


class DummyStorage:
    async def save(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_download_worker_reports_failure_when_metadata_fetch_fails(monkeypatch):
    config = DownloadConfig(api_key="test", output_path=".", workers=1)
    orchestrator = DownloadOrchestrator(config)

    async def fake_get_work_metadata_with_retry(_work_id: str):
        raise Exception("Rate limited")

    monkeypatch.setattr(
        orchestrator.api_client,
        "get_work_metadata_with_retry",
        fake_get_work_metadata_with_retry,
    )

    orchestrator.storage = DummyStorage()
    orchestrator.checkpoint_manager = DummyCheckpointManager()
    orchestrator._work_queue = asyncio.Queue()
    orchestrator._results_queue = asyncio.Queue()
    await orchestrator._work_queue.put(WorkItem(work_id="W123"))
    await orchestrator._work_queue.put(None)

    await orchestrator._download_worker(worker_id=0)

    result = await orchestrator._results_queue.get()
    assert isinstance(result, DownloadResult)
    assert result.work_id == "W123"
    assert result.success is False
    assert result.format == ContentFormat.NONE
    assert "Failed to fetch metadata" in (result.error or "")
