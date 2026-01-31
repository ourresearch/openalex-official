#!/usr/bin/env python3
"""Example: Download content with custom filtering and S3 storage."""

import asyncio
import os

from openalex_content.downloader import DownloadConfig, DownloadOrchestrator
from openalex_content.progress import ProgressTracker
from openalex_content.utils import ContentFormat, StorageType


async def main():
    api_key = os.environ.get("OPENALEX_API_KEY")
    if not api_key:
        print("Please set OPENALEX_API_KEY environment variable")
        return

    # Download TEI XML files for a specific topic to S3
    config = DownloadConfig(
        api_key=api_key,
        output_path="./temp",  # Checkpoint stored here
        storage_type=StorageType.S3,
        s3_bucket="my-research-bucket",
        s3_prefix="openalex-xml/",
        # Filter: machine learning articles from 2023+
        filter_str="topics.id:T10207,publication_year:>2022",
        content_format=ContentFormat.XML,
        with_metadata=True,  # Include JSON metadata
        workers=30,
    )

    progress = ProgressTracker(output_dir=config.output_path)
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
