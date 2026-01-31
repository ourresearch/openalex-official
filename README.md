# OpenAlex Content Downloader

Bulk download PDFs and TEI XML files from OpenAlex. Designed for premium customers who need to download large portions of the 60M PDF / 43M TEI XML corpus.

## Installation

```bash
pip install openalex-content-downloader
```

## Quick Start

```bash
# Download PDFs with a filter
openalex-content download \
  --api-key YOUR_API_KEY \
  --output ./pdfs \
  --filter "publication_year:>2020,type:article"

# Download to S3
openalex-content download \
  --api-key YOUR_API_KEY \
  --storage s3 \
  --s3-bucket my-bucket \
  --s3-prefix openalex/ \
  --filter "topics.id:T12345"

# Download both PDFs and TEI XML with metadata
openalex-content download \
  --api-key YOUR_API_KEY \
  --output ./content \
  --format both \
  --with-metadata

# Check API key status
openalex-content status --api-key YOUR_API_KEY
```

## Features

- **High-throughput async downloads** - Targets 5M downloads/day with configurable concurrency
- **Automatic checkpointing** - Resume interrupted downloads without re-downloading
- **Adaptive rate limiting** - Automatically adjusts to API conditions
- **Multiple storage backends** - Local filesystem or S3
- **Progress tracking** - Rich terminal UI with live stats, or headless logging
- **Flexible filtering** - Use any OpenAlex filter syntax

## CLI Reference

### `openalex-content download`

Download content from OpenAlex.

| Option | Description | Default |
|--------|-------------|---------|
| `--api-key` | OpenAlex API key (required) | `$OPENALEX_API_KEY` |
| `--output`, `-o` | Output directory | `./openalex-content` |
| `--storage` | Storage backend: `local` or `s3` | `local` |
| `--s3-bucket` | S3 bucket name | - |
| `--s3-prefix` | S3 key prefix | `""` |
| `--filter` | OpenAlex filter string | None (all content) |
| `--format` | Content format: `pdf`, `xml`, `both` | `pdf` |
| `--with-metadata` | Save JSON metadata with each file | `false` |
| `--workers` | Concurrent download workers (1-200) | `50` |
| `--resume/--no-resume` | Resume from checkpoint | `true` |
| `--fresh` | Ignore checkpoint, start fresh | `false` |
| `--quiet`, `-q` | Minimal output (log file only) | `false` |
| `--verbose`, `-v` | Extra debug output | `false` |

### `openalex-content status`

Check API key status and credit information.

| Option | Description |
|--------|-------------|
| `--api-key` | OpenAlex API key (required) |

## Filter Examples

```bash
# Recent articles
--filter "publication_year:>2020,type:article"

# Specific topic
--filter "topics.id:T12345"

# From a specific institution
--filter "authorships.institutions.id:I123456789"

# Open access only
--filter "open_access.is_oa:true"

# Combined filters
--filter "publication_year:2023,type:article,open_access.is_oa:true"
```

See [OpenAlex filter documentation](https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists) for all available filters.

## File Organization

Downloaded files are organized in a nested structure to avoid filesystem issues with millions of files:

```
output/
├── W27/
│   └── 41/
│       ├── W2741809807.pdf
│       └── W2741809807.json  # if --with-metadata
├── W12/
│   └── 34/
│       └── W1234567890.pdf
└── .openalex-checkpoint.json
```

## Checkpointing

The downloader automatically saves progress to `.openalex-checkpoint.json` in the output directory. If interrupted, run the same command again to resume.

To start fresh and ignore the checkpoint:
```bash
openalex-content download --api-key KEY --output ./pdfs --fresh
```

## Logging

All activity is logged to `openalex-download.log` in the output directory, regardless of terminal mode.

## Requirements

- Python 3.9+
- OpenAlex API key with sufficient credits

## Documentation

Full documentation: [docs.openalex.org/download/bulk-content-tool](https://docs.openalex.org/download/bulk-content-tool)

## License

MIT
