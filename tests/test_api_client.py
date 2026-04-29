"""Tests for API client retry behavior."""

from __future__ import annotations

import asyncio

import aiohttp
import pytest

from openalex_cli.api_client import CreditsExhaustedError, OpenAlexAPIClient


@pytest.mark.asyncio
async def test_get_work_metadata_with_retry_retries_429(monkeypatch):
    client = OpenAlexAPIClient(api_key="test")
    calls = {"count": 0}

    async def fake_get_work_metadata(work_id: str):
        calls["count"] += 1
        if calls["count"] < 3:
            raise aiohttp.ClientResponseError(
                request_info=None,
                history=(),
                status=429,
                message="Rate limited",
            )
        return {"id": f"https://openalex.org/{work_id}"}

    async def fake_sleep(_: float):
        return None

    monkeypatch.setattr(client, "get_work_metadata", fake_get_work_metadata)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    result = await client.get_work_metadata_with_retry("W123", max_attempts=3)

    assert result == {"id": "https://openalex.org/W123"}
    assert calls["count"] == 3


@pytest.mark.asyncio
async def test_get_work_metadata_with_retry_does_not_retry_credits_exhausted(monkeypatch):
    client = OpenAlexAPIClient(api_key="test")
    calls = {"count": 0}

    async def fake_get_work_metadata(_: str):
        calls["count"] += 1
        raise CreditsExhaustedError("Insufficient credits")

    monkeypatch.setattr(client, "get_work_metadata", fake_get_work_metadata)

    with pytest.raises(CreditsExhaustedError):
        await client.get_work_metadata_with_retry("W123", max_attempts=3)

    assert calls["count"] == 1
