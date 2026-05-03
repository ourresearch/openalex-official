"""Storage backends for the OpenAlex content downloader."""

from importlib import import_module

from .base import StorageBackend
from .local import LocalStorage

try:
    S3Storage = import_module("openalex_cli.storage.s3").S3Storage
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local-only environments
    S3Storage = None

__all__ = ["StorageBackend", "LocalStorage", "S3Storage"]
