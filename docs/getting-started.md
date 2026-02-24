# Getting Started

## Prerequisites

- Python 3.12 or later
- A [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) (free — needed for the SAE state-level processing step)
- Optionally, a [BLS API key](https://www.bls.gov/developers/) to raise the daily request limit

## Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/lowmason/bls-revisions.git
cd bls-revisions
pip install -e .
```

To install with documentation dependencies:

```bash
pip install -e ".[docs]"
```

To install with notebook support:

```bash
pip install -e ".[notebooks]"
```

## Environment variables

Create a `.env` file (or export these in your shell):

```bash
# Required for the SAE (state-level) processing step
FRED_API_KEY=your_fred_api_key_here

# Optional — raises BLS daily request limit from 25 to 500
BLS_API_KEY=your_bls_api_key_here
```

## Running the pipeline

### Full pipeline

Run all three stages in order:

```bash
bls-revisions
```

This is equivalent to running `release`, `download`, and `process` sequentially.

### Stage 1: Scrape release dates

```bash
bls-revisions release
```

Downloads BLS news-release archive pages for CES, SAE, and QCEW, extracts the embargo (vintage) date from each release, and writes:

- `data/release_dates.parquet` — one row per (publication, ref_date) with the vintage date
- `data/vintage_dates.parquet` — expanded with revision codes and benchmark flags

### Stage 2: Download data files

```bash
bls-revisions download
```

Fetches:

- CES triangular-revision spreadsheets → `data/ces/`
- QCEW revisions CSV → `data/qcew/`

### Stage 3: Process

```bash
bls-revisions process
```

Runs four processing steps:

1. **CES national** — reads triangular CSVs, extracts revision diagonals → `data/ces_revisions.parquet`
2. **CES states (SAE)** — fetches initial/latest vintages from ALFRED → `data/sae_revisions.parquet`
3. **QCEW** — reshapes the revisions CSV → `data/qcew_revisions.parquet`
4. **Vintage series** — combines all three and aggregates to region/division → `data/revisions.parquet`

## Using the Python API

```python
import polars as pl
from bls_revisions import read_release_dates, read_vintage_dates

# Load the release dates
release_dates = read_release_dates()
print(release_dates)

# Load vintage dates with revision codes
vintage_dates = read_vintage_dates()
print(vintage_dates.filter(pl.col('publication') == 'ces').head())
```

## Output schema

All final Parquet files share a common schema:

| Column | Type | Description |
|--------|------|-------------|
| `source` | string | Survey source (`ces`, `sae`, `qcew`) |
| `seasonally_adjusted` | bool | Whether the series is seasonally adjusted |
| `geographic_type` | string | `national`, `state`, `region`, or `division` |
| `geographic_code` | string | FIPS code or region/division code |
| `industry_type` | string | `domain`, `supersector`, or `sector` |
| `industry_code` | string | Two-digit industry code |
| `ref_date` | date | Reference period (12th of the month) |
| `vintage_date` | date | Date the estimate was published |
| `revision` | uint8 | Revision number (0 = initial, 1 = first revision, ...) |
| `benchmark_revision` | uint8 | Benchmark flag (0 = none, 1 = first, 2 = second) |
| `employment` | float64 | Employment level (thousands) |

## Project layout

```
bls-revisions/
├── src/bls_revisions/
│   ├── __init__.py          # Public API
│   ├── __main__.py          # CLI entry point
│   ├── _client.py           # Shared HTTP client
│   ├── release_dates/       # Release date scraping
│   ├── download/            # Data file downloaders
│   └── processing/          # Data processing pipelines
├── data/                    # Downloaded and processed data
├── docs/                    # Documentation (this site)
└── mkdocs.yml               # MkDocs configuration
```
