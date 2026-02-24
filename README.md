# bls-revisions

BLS revisions: release date scraping, vintage downloads, and processing.

## Installation

```bash
pip install -e .
```

## CLI

```bash
# Run all steps (release + download + process)
bls-revisions

# Run individual steps
bls-revisions release    # Scrape BLS news releases, build release_dates + vintage_dates
bls-revisions download   # Download CES and QCEW data files
bls-revisions process    # Process CES national, CES states, QCEW, and combine
```

The `process` step requires the `FRED_API_KEY` environment variable for SAE state-level data.

## Python API

```python
from bls_revisions import (
    download_ces,
    download_qcew,
    read_release_dates,
    read_vintage_dates,
    build_vintage_dates,
)
```

## Package Structure

```
src/bls_revisions/
    __init__.py               # Public API
    __main__.py               # Unified CLI (release, download, process)
    _client.py                # Shared HTTP client with retry
    release_dates/            # BLS news release scraping
        config.py             # Publication definitions and paths
        parser.py             # HTML release date extraction
        scraper.py            # Async index page fetching and downloading
        read.py               # Parquet readers
        vintage_dates.py      # Revision code assignment
    download/                 # Data file downloaders
        ces.py                # CES vintage data
        qcew.py               # QCEW revisions CSV
    processing/               # Data processing pipelines
        ces_national.py       # CES triangular revision files
        ces_states.py         # SAE revisions from FRED/ALFRED
        qcew.py               # QCEW revision CSV processing
        vintage_series.py     # Combine all sources
```
