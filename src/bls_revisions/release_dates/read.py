'''Read release_dates or vintage_dates parquet files if they exist.'''

from pathlib import Path

import polars as pl

from .config import PARQUET_PATH, VINTAGE_DATES_PATH


def read_release_dates(path: Path | str | None = None) -> pl.DataFrame | None:
    '''Read release_dates parquet if it exists.

    Args:
        path: Optional path to the parquet file. Defaults to data/release_dates.parquet
            relative to the current working directory.

    Returns:
        Polars DataFrame with columns publication, ref_date, vintage_date, or None
        if the file has not been created yet.
    '''
    p = Path(path) if path is not None else PARQUET_PATH
    if not p.exists():
        return None
    return pl.read_parquet(p)


def read_vintage_dates(path: Path | str | None = None) -> pl.DataFrame | None:
    '''Read vintage_dates parquet if it exists.

    Args:
        path: Optional path to the parquet file. Defaults to data/vintage_dates.parquet
            relative to the current working directory.

    Returns:
        Polars DataFrame with columns publication, ref_date, vintage_date, revision,
        benchmark_revision, or None if the file has not been created yet.
    '''
    p = Path(path) if path is not None else VINTAGE_DATES_PATH
    if not p.exists():
        return None
    return pl.read_parquet(p)
