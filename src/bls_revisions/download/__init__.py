'''BLS data downloaders for CES and QCEW.

This subpackage fetches raw revision data from bls.gov:

- :func:`download_ces` scrapes the CES vintage-data page for the
  triangular-revision spreadsheets (``cesvinall.zip`` and individual
  ``cesvin*.xlsx`` files).
- :func:`download_qcew` downloads the single QCEW revisions CSV.

Both functions accept an optional ``data_dir`` and/or ``client`` so
they can be called from the CLI or from user code.
'''

from .ces import download_ces
from .qcew import download_qcew

__all__ = [
    'download_ces',
    'download_qcew',
]
