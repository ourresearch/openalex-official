"""Storage backends for the OpenAlex content downloader."""

from .base import StorageBackend
from .local import LocalStorage
from .s3 import S3Storage

__all__ = ["StorageBackend", "LocalStorage", "S3Storage"]
