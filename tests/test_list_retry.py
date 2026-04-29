"""Tests for paginated list retry behavior."""

from __future__ import annotations

import asyncio

import aiohttp
import pytest

from openalex_cli.api_client import CreditsExhaustedError, OpenAlexAPIClient
from openalex_cli.utils import ContentFormat


@pytest.mark.asyncio
async def test_list_works_retries_429(monkeypatch):
    client = OpenAlexAPIClient(api_key="test")
    calls = {"count": 0}

    class FakeResponse:
        def __init__(self, status: int, payload: dict, headers: dict[str, str] | None = None):
            self.status = status
            self._payload = payload
            self.headers = headers or {}
            self.request_info = None
            self.history = ()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            if self.status >= 400:
                raise aiohttp.ClientResponseError(
                    self.request_info,
                    self.history,
                    status=self.status,
                    message="request failed",
                )

        async def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, params=None):
            calls["count"] += 1
            if calls["count"] < 3:
                return FakeResponse(
                    429,
                    {},
                    {"X-RateLimit-Remaining": "10", "X-RateLimit-Credits-Required": "1"},
                )
            return FakeResponse(
                200,
                {
                    "results": [
                        {
                            "id": "https://openalex.org/W123",
                            "has_content": {"pdf": False, "grobid_xml": False},
                        }
                    ],
                    "meta": {"next_cursor": None},
                },
            )

    async def fake_sleep(_: float):
        return None

    async def fake_get_session():
        return FakeSession()

    monkeypatch.setattr(client, "_get_session", fake_get_session)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    pages = []
    async for works, next_cursor in client.list_works(
        filter_str="publication_year:>2020",
        content_format=ContentFormat.NONE,
        cursor="*",
        max_attempts=3,
        base_delay_seconds=0.01,
    ):
        pages.append((works, next_cursor))

    assert calls["count"] == 3
    assert len(pages) == 1
    works, next_cursor = pages[0]
    assert len(works) == 1
    assert works[0].work_id == "W123"
    assert next_cursor is None


@pytest.mark.asyncio
async def test_list_works_raises_credits_exhausted_without_retry(monkeypatch):
    client = OpenAlexAPIClient(api_key="test")
    calls = {"count": 0}

    class FakeResponse:
        def __init__(self):
            self.status = 429
            self.headers = {
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Credits-Required": "1",
            }
            self.request_info = None
            self.history = ()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return {}

    class FakeSession:
        def get(self, url, params=None):
            calls["count"] += 1
            return FakeResponse()

    async def fake_get_session():
        return FakeSession()

    monkeypatch.setattr(client, "_get_session", fake_get_session)

    with pytest.raises(CreditsExhaustedError):
        async for _works, _cursor in client.list_works(
            filter_str="publication_year:>2020",
            content_format=ContentFormat.NONE,
            cursor="*",
            max_attempts=3,
            base_delay_seconds=0.01,
        ):
            pass

    assert calls["count"] == 1
