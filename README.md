# OpenAlex Official CLI

Official command-line interface for [OpenAlex](https://openalex.org). Download work metadata and full-text content (PDFs, TEI XML) in bulk.

> **Note:** This package was formerly known as `openalex-content-downloader`. If you have that installed, please switch to `openalex-official`.

## Installation

```bash
pip install openalex-official
```

## Quick Start

```bash
# Download metadata for works matching a filter
openalex download \
  --api-key YOUR_API_KEY \
  --output ./frogs \
  --filter "topics.id:T10325"

# Download metadata + PDFs
openalex download \
  --api-key YOUR_API_KEY \
  --output ./frogs \
  --filter "topics.id:T10325" \
  --content pdf

# Download metadata + PDFs + TEI XML
openalex download \
  --api-key YOUR_API_KEY \
  --output ./frogs \
  --filter "topics.id:T10325" \
  --content pdf,xml

# Download specific works by ID or DOI
openalex download \
  --api-key YOUR_API_KEY \
  --output ./papers \
  --ids "W2741809807,10.1038/nature12373"

# Download from a list of IDs via stdin
cat work_ids.txt | openalex download \
  --api-key YOUR_API_KEY \
  --output ./papers \
  --stdin

# Download to S3
openalex download \
  --api-key YOUR_API_KEY \
  --storage s3 \
  --s3-bucket my-bucket \
  --s3-prefix openalex/ \
  --filter "topics.id:T12345"

# Check API key status
openalex status --api-key YOUR_API_KEY
```

## Features

- **Metadata-first approach** - JSON metadata is always saved; content files are optional
- **High-throughput async downloads** - Configurable concurrency for millions of works
- **Automatic checkpointing** - Resume interrupted downloads without re-downloading
- **Adaptive rate limiting** - Automatically adjusts to API conditions
- **Multiple storage backends** - Local filesystem or S3
- **Progress tracking** - Rich terminal UI with live stats, or headless logging
- **Flexible filtering** - Use any OpenAlex filter syntax
- **Multiple input modes** - Filter, explicit IDs, or piped stdin
- **DOI support** - Auto-detects and resolves DOIs to OpenAlex work IDs

## CLI Reference

### `openalex download`

Download work metadata and optionally content (PDFs, TEI XML).

| Option | Description | Default |
|--------|-------------|---------|
| `--api-key` | OpenAlex API key (required) | `$OPENALEX_API_KEY` |
| `--output`, `-o` | Output directory | `./openalex-downloads` |
| `--storage` | Storage backend: `local` or `s3` | `local` |
| `--s3-bucket` | S3 bucket name | - |
| `--s3-prefix` | S3 key prefix | `""` |
| `--filter` | OpenAlex filter string | None (all works) |
| `--ids` | Comma-separated work IDs or DOIs | - |
| `--stdin` | Read work IDs/DOIs from stdin | `false` |
| `--content` | Content to download: `pdf`, `xml`, or `pdf,xml` | None (metadata only) |
| `--nested` | Use nested folder structure (W##/##/) | `false` |
| `--workers` | Concurrent download workers (1-200) | `50` |
| `--resume/--no-resume` | Resume from checkpoint | `true` |
| `--fresh` | Ignore checkpoint, start fresh | `false` |
| `--quiet`, `-q` | Minimal output (log file only) | `false` |
| `--verbose`, `-v` | Extra debug output | `false` |

### `openalex status`

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

By default, files are saved flat in the output directory. Metadata is always saved as JSON:

```
output/
├── W2741809807.json     # metadata (always saved)
├── W2741809807.pdf      # content (if --content pdf)
├── W2741809807.tei.xml  # content (if --content xml)
├── W1234567890.json
└── .openalex-checkpoint.json
```

For large downloads (>10,000 files), use `--nested` to organize files in a nested structure that avoids filesystem issues:

```
output/
├── W27/
│   └── 41/
│       ├── W2741809807.json
│       └── W2741809807.pdf
├── W12/
│   └── 34/
│       └── W1234567890.json
└── .openalex-checkpoint.json
```

When downloading by DOI, files are named using the DOI (with `/` replaced by `_`):

```
output/
├── 10.1038_nature12373.json
└── 10.1038_nature12373.pdf
```

## Checkpointing

The downloader automatically saves progress to `.openalex-checkpoint.json` in the output directory. If interrupted, run the same command again to resume.

To start fresh and ignore the checkpoint:
```bash
openalex download --api-key KEY --output ./data --fresh
```

## Logging

All activity is logged to `openalex-download.log` in the output directory, regardless of terminal mode.

## High-Throughput Deployment

The download speed is typically limited by **network bandwidth**, not the tool or API. On a typical home connection (~400 Mbps), expect ~10-15 files/sec (~1M files/day). To achieve higher throughput, deploy from a cloud environment.

**Performance scaling:**

| Environment | Bandwidth | Workers | Expected Rate |
|-------------|-----------|---------|---------------|
| Home connection | 400 Mbps | 50 | ~10-15 files/sec |
| Cloud VM (standard) | 1-5 Gbps | 100-150 | ~30-50 files/sec |
| Cloud VM (high-perf) | 10+ Gbps | 200-300 | ~60+ files/sec |

**Recommendations for large-scale downloads:**

1. **Run from cloud** - Deploy on AWS EC2, GCP, or Azure VMs with high network bandwidth. Instances close to Cloudflare edge locations will have lower latency.

2. **Increase workers** - Use `--workers 150` or higher to saturate available bandwidth. Monitor with verbose mode to find the optimal setting.

3. **Use S3 storage** - For very large downloads, stream directly to S3 instead of local disk:
   ```bash
   openalex download \
     --api-key KEY \
     --storage s3 \
     --s3-bucket my-corpus \
     --workers 200
   ```

4. **Parallelize across machines** - For the full corpus, run multiple instances with different filters (e.g., by publication year) on separate machines.

## Roadmap

We plan to add more commands to the CLI, including:
- CSV/JSON export of search results
- More entity types beyond works

Have a feature request? [Open an issue](https://github.com/ourresearch/openalex-official/issues).

## Requirements

- Python 3.9+
- OpenAlex API key with sufficient credits

## Documentation

Full documentation: [docs.openalex.org](https://docs.openalex.org)

## License

MIT
