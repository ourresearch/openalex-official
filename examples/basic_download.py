#!/usr/bin/env python3
"""Basic example: Download PDFs for recent articles."""

import asyncio
import os

from openalex_content.api_client import OpenAlexAPIClient
from openalex_content.downloader import DownloadConfig, DownloadOrchestrator
from openalex_content.progress import ProgressTracker
from openalex_content.utils import ContentFormat, StorageType


async def main():
    # Get API key from environment
    api_key = os.environ.get("OPENALEX_API_KEY")
    if not api_key:
        print("Please set OPENALEX_API_KEY environment variable")
        return

    # Configure the download
    config = DownloadConfig(
        api_key=api_key,
        output_path="./downloaded_pdfs",
        storage_type=StorageType.LOCAL,
        filter_str="publication_year:2024,type:article",
        content_format=ContentFormat.PDF,
        workers=20,  # Moderate concurrency
    )

    # Set up progress tracking
    progress = ProgressTracker(
        output_dir=config.output_path,
        quiet=False,
    )

    # Create and run the orchestrator
    orchestrator = DownloadOrchestrator(config)

    try:
        progress.start()
        await orchestrator.run(progress_tracker=progress)
    except KeyboardInterrupt:
        print("\nDownload interrupted. Progress saved.")
    finally:
        progress.stop()


if __name__ == "__main__":
    asyncio.run(main())
