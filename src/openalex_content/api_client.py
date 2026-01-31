"""OpenAlex API client for works listing and content download."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator

import aiohttp

from .utils import ContentFormat


@dataclass
class WorkItem:
    """A work item with its metadata."""

    work_id: str
    title: str | None = None
    doi: str | None = None
    publication_date: str | None = None
    authors: list[dict] | None = None
    type: str | None = None
    has_pdf: bool = False
    has_xml: bool = False
    raw_data: dict = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict) -> WorkItem:
        """Create a WorkItem from OpenAlex API response."""
        work_id = data.get("id", "").replace("https://openalex.org/", "")
        has_content = data.get("has_content", {})
        return cls(
            work_id=work_id,
            title=data.get("title"),
            doi=data.get("doi"),
            publication_date=data.get("publication_date"),
            authors=data.get("authorships"),
            type=data.get("type"),
            has_pdf=has_content.get("pdf", False),
            has_xml=has_content.get("tei_xml", False),
            raw_data=data,
        )


@dataclass
class DownloadResult:
    """Result of a content download."""

    work_id: str
    format: ContentFormat
    success: bool
    content: bytes | None = None
    error: str | None = None
    credits_cost: int = 0
    rate_limit_remaining: int | None = None
    file_size: int = 0


@dataclass
class APIStatus:
    """Status information from the OpenAlex API."""

    credits_remaining: int
    credits_used: int
    credits_limit: int
    rate_limit_remaining: int


class OpenAlexAPIClient:
    """Client for interacting with OpenAlex APIs."""

    WORKS_API_BASE = "https://api.openalex.org"
    CONTENT_API_BASE = "https://content.openalex.org"

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession | None = None,
        per_page: int = 200,
    ):
        self.api_key = api_key
        self._session = session
        self._owns_session = session is None
        self.per_page = per_page

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the session if we own it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "openalex-content-downloader/0.1.0",
        }

    async def get_status(self) -> APIStatus:
        """Get API key status and credit information."""
        session = await self._get_session()
        url = f"{self.WORKS_API_BASE}/works"
        params = {"per-page": 1, "api_key": self.api_key}

        async with session.get(url, params=params) as response:
            response.raise_for_status()
            # Extract rate limit info from headers
            rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
            # For now, we estimate based on rate limit
            # Full credit info would come from a dedicated endpoint
            return APIStatus(
                credits_remaining=rate_limit_remaining,
                credits_used=0,
                credits_limit=rate_limit_remaining,
                rate_limit_remaining=rate_limit_remaining,
            )

    async def list_works(
        self,
        filter_str: str | None = None,
        content_format: ContentFormat = ContentFormat.PDF,
        cursor: str = "*",
    ) -> AsyncIterator[tuple[list[WorkItem], str | None]]:
        """
        List works with content, using cursor pagination.

        Yields tuples of (works_list, next_cursor).
        When next_cursor is None, pagination is complete.
        """
        session = await self._get_session()

        # Build filter with has_content requirement
        content_filter = (
            "has_content.pdf:true"
            if content_format in (ContentFormat.PDF, ContentFormat.BOTH)
            else "has_content.tei_xml:true"
        )
        if filter_str:
            full_filter = f"{content_filter},{filter_str}"
        else:
            full_filter = content_filter

        while cursor:
            params = {
                "filter": full_filter,
                "cursor": cursor,
                "per-page": self.per_page,
                "api_key": self.api_key,
            }
            url = f"{self.WORKS_API_BASE}/works"

            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

            results = data.get("results", [])
            works = [WorkItem.from_api_response(r) for r in results]

            meta = data.get("meta", {})
            next_cursor = meta.get("next_cursor")

            yield works, next_cursor
            cursor = next_cursor

            if not next_cursor:
                break

    async def download_content(
        self,
        work_id: str,
        content_format: ContentFormat,
    ) -> DownloadResult:
        """
        Download content for a specific work.

        Follows the redirect to the signed R2 URL and downloads the content.
        """
        session = await self._get_session()

        # Determine file extension
        ext = "pdf" if content_format == ContentFormat.PDF else "tei.xml"
        url = f"{self.CONTENT_API_BASE}/works/{work_id}.{ext}"

        try:
            # First request to get the redirect
            async with session.get(
                url,
                headers=self._get_headers(),
                allow_redirects=False,
            ) as response:
                if response.status == 404:
                    return DownloadResult(
                        work_id=work_id,
                        format=content_format,
                        success=False,
                        error="Content not found",
                    )

                if response.status == 429:
                    return DownloadResult(
                        work_id=work_id,
                        format=content_format,
                        success=False,
                        error="Rate limited",
                        rate_limit_remaining=0,
                    )

                if response.status not in (301, 302, 307, 308):
                    return DownloadResult(
                        work_id=work_id,
                        format=content_format,
                        success=False,
                        error=f"Unexpected status: {response.status}",
                    )

                # Extract metadata from response headers
                credits_cost = int(response.headers.get("X-Credits-Cost", 100))
                rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                redirect_url = response.headers.get("Location")

                if not redirect_url:
                    return DownloadResult(
                        work_id=work_id,
                        format=content_format,
                        success=False,
                        error="No redirect URL in response",
                    )

            # Download from the signed URL
            async with session.get(redirect_url) as content_response:
                content_response.raise_for_status()
                content = await content_response.read()

            return DownloadResult(
                work_id=work_id,
                format=content_format,
                success=True,
                content=content,
                credits_cost=credits_cost,
                rate_limit_remaining=rate_limit_remaining,
                file_size=len(content),
            )

        except aiohttp.ClientError as e:
            return DownloadResult(
                work_id=work_id,
                format=content_format,
                success=False,
                error=str(e),
            )
        except asyncio.TimeoutError:
            return DownloadResult(
                work_id=work_id,
                format=content_format,
                success=False,
                error="Request timed out",
            )
