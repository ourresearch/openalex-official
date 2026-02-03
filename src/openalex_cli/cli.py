"""Command-line interface for OpenAlex."""

from __future__ import annotations

import asyncio
import sys

import click

from . import __version__
from .api_client import OpenAlexAPIClient
from .downloader import DownloadConfig, DownloadOrchestrator
from .progress import ProgressTracker
from .utils import ContentFormat, StorageType, parse_identifier


def _parse_input_ids(ids_str: str | None, use_stdin: bool) -> list[str]:
    """Parse work IDs from --ids option or stdin."""
    raw_ids: list[str] = []

    if use_stdin:
        import sys

        input_text = sys.stdin.read()
        # Split by newlines and commas, strip whitespace
        for line in input_text.split("\n"):
            for id_part in line.split(","):
                stripped = id_part.strip()
                if stripped:
                    raw_ids.append(stripped)

    if ids_str:
        for id_part in ids_str.split(","):
            stripped = id_part.strip()
            if stripped:
                raw_ids.append(stripped)

    return raw_ids


async def _resolve_identifiers(
    raw_ids: list[str], api_key: str, quiet: bool
) -> tuple[list[str], dict[str, str]]:
    """
    Parse and resolve identifiers to OpenAlex work IDs.

    Returns:
        Tuple of (work_ids, original_identifiers_map)
        - work_ids: List of OpenAlex work IDs
        - original_identifiers_map: Maps work_id -> original input (for DOI-based filenames)
    """
    openalex_ids: list[str] = []
    dois: list[str] = []
    original_map: dict[str, str] = {}

    # Categorize identifiers
    for raw_id in raw_ids:
        try:
            id_type, value = parse_identifier(raw_id)
            if id_type == "openalex":
                openalex_ids.append(value)
            elif id_type == "doi":
                dois.append(value)
        except ValueError as e:
            if not quiet:
                click.echo(click.style("Warning: ", fg="yellow") + str(e))

    # Resolve DOIs to work IDs
    if dois:
        if not quiet:
            click.echo(f"Resolving {len(dois)} DOI(s) to OpenAlex work IDs...")

        client = OpenAlexAPIClient(api_key=api_key)
        try:
            doi_to_work = await client.resolve_dois(dois)

            for doi in dois:
                if doi in doi_to_work:
                    work_id = doi_to_work[doi]
                    openalex_ids.append(work_id)
                    original_map[work_id] = doi  # Remember original DOI for filename
                elif not quiet:
                    click.echo(
                        click.style("Warning: ", fg="yellow")
                        + f"DOI not found in OpenAlex: {doi}"
                    )
        finally:
            await client.close()

    # Remove duplicates while preserving order
    seen = set()
    unique_ids = []
    for work_id in openalex_ids:
        if work_id not in seen:
            seen.add(work_id)
            unique_ids.append(work_id)

    return unique_ids, original_map


@click.group()
@click.version_option(version=__version__, prog_name="openalex")
def main() -> None:
    """OpenAlex CLI - Official command-line interface for OpenAlex."""
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
    default="./openalex-downloads",
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
    "--content",
    "content_types",
    help="Content types to download: pdf, xml, or both (comma-separated). If omitted, only metadata is downloaded.",
)
@click.option(
    "--nested",
    is_flag=True,
    help="Use nested folder structure (W##/##/filename). Recommended for >10K files.",
)
@click.option(
    "--ids",
    "ids_str",
    help="Comma-separated list of work IDs or DOIs to download",
)
@click.option(
    "--stdin",
    "use_stdin",
    is_flag=True,
    help="Read work IDs or DOIs from stdin (one per line or comma-separated)",
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
    content_types: str | None,
    nested: bool,
    ids_str: str | None,
    use_stdin: bool,
    workers: int,
    resume: bool,
    fresh: bool,
    quiet: bool,
    verbose: bool,
) -> None:
    """Download OpenAlex work metadata and optionally content (PDFs/XML).

    By default, downloads JSON metadata for each work. Use --content to also
    download PDFs and/or TEI XML files.

    \b
    Examples:
      # Metadata only (default)
      openalex download --filter "topics.id:T10325" -o ./frogs

      # Metadata + PDFs
      openalex download --filter "topics.id:T10325" --content pdf -o ./frogs

      # Metadata + PDFs + XML
      openalex download --filter "topics.id:T10325" --content pdf,xml -o ./frogs

    \b
    Input modes:
      1. Filter mode: --filter "publication_year:2024,type:article"
      2. ID list: --ids "W2741809807,W3203546474"
      3. Stdin: echo "W2741809807" | openalex download --stdin

    DOIs are auto-detected and resolved: --ids "10.1038/nature12373"
    """
    # Validate S3 options
    if storage == "s3" and not s3_bucket:
        raise click.UsageError("--s3-bucket is required when using S3 storage")

    # Parse IDs from stdin or --ids option
    work_ids: list[str] | None = None
    original_identifiers: dict[str, str] | None = None

    if use_stdin or ids_str:
        raw_ids = _parse_input_ids(ids_str, use_stdin)
        if raw_ids:
            work_ids, original_identifiers = asyncio.run(
                _resolve_identifiers(raw_ids, api_key, quiet)
            )
            if not work_ids:
                click.echo(
                    click.style("Error: ", fg="red") + "No valid work IDs found.", err=True
                )
                sys.exit(1)
            if not quiet:
                click.echo(f"Found {len(work_ids)} work(s) to download.")

    # Parse content types
    content_format = ContentFormat.NONE
    if content_types:
        types = [t.strip().lower() for t in content_types.split(",")]
        if "pdf" in types and "xml" in types:
            content_format = ContentFormat.BOTH
        elif "both" in types:
            content_format = ContentFormat.BOTH
        elif "pdf" in types:
            content_format = ContentFormat.PDF
        elif "xml" in types:
            content_format = ContentFormat.XML

    # Warn if no filter and no IDs provided
    if not filter_str and not work_ids and not quiet:
        click.echo(
            click.style("Warning: ", fg="yellow")
            + "No filter specified. This will download ALL works."
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
        content_format=content_format,
        workers=workers,
        resume=resume,
        fresh=fresh,
        quiet=quiet,
        verbose=verbose,
        nested=nested,
        work_ids=work_ids,
        original_identifiers=original_identifiers,
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
