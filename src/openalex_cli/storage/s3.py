"""S3 storage backend using aiobotocore."""

from __future__ import annotations

from contextlib import asynccontextmanager

from aiobotocore.session import get_session  # type: ignore[import-untyped]

from .base import StorageBackend


class S3Storage(StorageBackend):
    """Store content in Amazon S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
    ):
        """
        Initialize S3 storage.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix for all objects
            region: AWS region (uses default if not specified)
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.region = region
        self._session = get_session()
        self._client = None

    @asynccontextmanager
    async def _get_client(self):
        """Get an S3 client context."""
        async with self._session.create_client(
            "s3",
            region_name=self.region,
        ) as client:
            yield client

    async def save(
        self,
        path: str,
        content: bytes,
        content_type: str | None = None,
    ) -> None:
        """Upload content to S3."""
        key = f"{self.prefix}{path}"

        put_kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": content,
        }
        if content_type:
            put_kwargs["ContentType"] = content_type

        async with self._get_client() as client:
            await client.put_object(**put_kwargs)

    async def exists(self, path: str) -> bool:
        """Check if an object exists in S3."""
        key = f"{self.prefix}{path}"

        async with self._get_client() as client:
            try:
                await client.head_object(Bucket=self.bucket, Key=key)
                return True
            except client.exceptions.ClientError:
                return False

    async def close(self) -> None:
        """Clean up resources."""
        # aiobotocore handles cleanup via context managers
        pass

    def get_full_path(self, path: str) -> str:
        """Get the S3 URI for a file."""
        key = f"{self.prefix}{path}"
        return f"s3://{self.bucket}/{key}"
