'''BLS revisions: release date scraping, vintage downloads, and processing.

This package provides a complete pipeline for tracking revisions to Bureau
of Labor Statistics employment data across three surveys:

- **CES** (Current Employment Statistics) -- national nonfarm payrolls
- **SAE** (State and Area Employment) -- state-level nonfarm payrolls
- **QCEW** (Quarterly Census of Employment and Wages) -- quarterly employment

The pipeline has three stages, each runnable independently:

1. **Release date scraping** -- download BLS news-release archive pages and
   extract the embargo (vintage) dates that tell us *when* each data point
   was first published.
2. **Data downloading** -- fetch the CES triangular-revision spreadsheets
   and the QCEW revisions CSV from bls.gov.
3. **Processing** -- read the raw files, attach vintage dates, and produce
   tidy Parquet datasets suitable for revision analysis.

Typical usage::

    from bls_revisions import (
        download_ces,
        download_qcew,
        read_release_dates,
        read_vintage_dates,
        build_vintage_dates,
    )
'''

from bls_revisions.download import download_ces, download_qcew
from bls_revisions.release_dates import (
    read_release_dates,
    read_vintage_dates,
    build_vintage_dates,
)

__all__ = [
    'download_ces',
    'download_qcew',
    'read_release_dates',
    'read_vintage_dates',
    'build_vintage_dates',
]
