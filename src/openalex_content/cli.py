"""Command-line interface for OpenAlex content downloader."""

from __future__ import annotations

import asyncio
import sys

import click

from . import __version__
from .downloader import DownloadConfig, DownloadOrchestrator
from .progress import ProgressTracker
from .utils import ContentFormat, StorageType


@click.group()
@click.version_option(version=__version__, prog_name="openalex-content")
def main() -> None:
    """OpenAlex Content Downloader - Bulk download PDFs and TEI XML from OpenAlex."""
    pass


@main.command()
@click.option(
    "--api-key",
    required=True,
    envvar="OPENALEX_API_KEY",
    help="OpenAlex API key (or set OPENALEX_API_KEY env var)",
)
@click.option(
    "--output",
    "-o",
    default="./openalex-content",
    help="Output directory for downloaded files",
    type=click.Path(),
)
@click.option(
    "--storage",
    type=click.Choice(["local", "s3"]),
    default="local",
    help="Storage backend",
)
@click.option(
    "--s3-bucket",
    help="S3 bucket name (required for S3 storage)",
)
@click.option(
    "--s3-prefix",
    default="",
    help="S3 key prefix",
)
@click.option(
    "--filter",
    "filter_str",
    help="OpenAlex filter string (e.g., 'publication_year:>2020,type:article')",
)
@click.option(
    "--format",
    "content_format",
    type=click.Choice(["pdf", "xml", "both"]),
    default="pdf",
    help="Content format to download",
)
@click.option(
    "--with-metadata",
    is_flag=True,
    help="Save JSON metadata alongside each file",
)
@click.option(
    "--workers",
    default=50,
    help="Number of concurrent download workers",
    type=click.IntRange(1, 200),
)
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Resume from checkpoint if available",
)
@click.option(
    "--fresh",
    is_flag=True,
    help="Ignore existing checkpoint and start fresh",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Minimal output (log file only)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Verbose debug output",
)
def download(
    api_key: str,
    output: str,
    storage: str,
    s3_bucket: str | None,
    s3_prefix: str,
    filter_str: str | None,
    content_format: str,
    with_metadata: bool,
    workers: int,
    resume: bool,
    fresh: bool,
    quiet: bool,
    verbose: bool,
) -> None:
    """Download OpenAlex content (PDFs and/or TEI XML files)."""
    # Validate S3 options
    if storage == "s3" and not s3_bucket:
        raise click.UsageError("--s3-bucket is required when using S3 storage")

    # Warn if no filter provided
    if not filter_str and not quiet:
        click.echo(
            click.style("Warning: ", fg="yellow")
            + "No filter specified. This will download ALL available content."
        )
        click.echo("Use --filter to narrow down the download. Example:")
        click.echo('  --filter "publication_year:>2020,type:article"')
        click.echo()
        if not click.confirm("Continue with full download?"):
            raise click.Abort()

    # Build config
    config = DownloadConfig(
        api_key=api_key,
        output_path=output,
        storage_type=StorageType.S3 if storage == "s3" else StorageType.LOCAL,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        filter_str=filter_str,
        content_format=ContentFormat(content_format),
        with_metadata=with_metadata,
        workers=workers,
        resume=resume,
        fresh=fresh,
        quiet=quiet,
        verbose=verbose,
    )

    # Create progress tracker
    progress = ProgressTracker(
        output_dir=output,
        quiet=quiet,
        verbose=verbose,
    )

    # Create orchestrator and run
    orchestrator = DownloadOrchestrator(config)

    try:
        progress.start()
        asyncio.run(orchestrator.run(progress_tracker=progress))
    except KeyboardInterrupt:
        click.echo("\nDownload interrupted. Progress saved to checkpoint.")
    finally:
        progress.stop()


@main.command()
@click.option(
    "--api-key",
    required=True,
    envvar="OPENALEX_API_KEY",
    help="OpenAlex API key (or set OPENALEX_API_KEY env var)",
)
def status(api_key: str) -> None:
    """Check API key status and credit information."""
    from .api_client import OpenAlexAPIClient

    async def _check_status():
        client = OpenAlexAPIClient(api_key=api_key)
        try:
            status = await client.get_status()
            click.echo(f"API Key Status:")
            click.echo(f"  Rate limit remaining: {status.rate_limit_remaining:,}")
            click.echo()
            click.echo("Note: Full credit information requires a premium API key.")
        except Exception as e:
            click.echo(f"Error checking status: {e}", err=True)
            sys.exit(1)
        finally:
            await client.close()

    asyncio.run(_check_status())


if __name__ == "__main__":
    main()
