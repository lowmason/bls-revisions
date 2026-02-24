'''BLS news-release scraper: discover, download, and parse release dates.

This subpackage scrapes the BLS news-release archive pages for three
publications (CES, SAE, QCEW), downloads the individual release HTML
files, and extracts the embargo (vintage) date from each one.  It then
builds two Parquet datasets:

- ``release_dates.parquet`` -- one row per *(publication, ref_date)*
  with the vintage date when that reference period was first published.
- ``vintage_dates.parquet`` -- expanded with publication-specific
  revision codes (0 = initial, 1 = first revision, ..., 9 = benchmark)
  and ``benchmark_revision`` flags.

Key public functions:

- :func:`read_release_dates` / :func:`read_vintage_dates` -- load the
  Parquet files into Polars DataFrames.
- :func:`build_vintage_dates` -- rebuild ``vintage_dates.parquet`` from
  ``release_dates.parquet``.
- :func:`download_all_publications` -- fetch all release HTMLs.
'''

from .config import DATA_DIR, PARQUET_PATH, PUBLICATIONS, Publication, VINTAGE_DATES_PATH
from .__main__ import main, build_dataframe, download_all_publications
from .read import read_release_dates, read_vintage_dates
from .vintage_dates import build_vintage_dates

__all__ = [
    'DATA_DIR',
    'PARQUET_PATH',
    'VINTAGE_DATES_PATH',
    'PUBLICATIONS',
    'Publication',
    'main',
    'build_dataframe',
    'build_vintage_dates',
    'download_all_publications',
    'read_release_dates',
    'read_vintage_dates',
]
