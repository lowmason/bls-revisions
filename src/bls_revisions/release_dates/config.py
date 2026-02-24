'''Publication definitions and shared path constants.

All paths are relative to the current working directory (the repository
root at runtime).

Attributes:
    BASE_URL: Root URL for bls.gov (used to resolve relative links).
    DATA_DIR: Directory where downloaded release HTML files are stored,
        organised by publication name (e.g. ``data/releases/ces/``).
    PARQUET_PATH: Output path for ``release_dates.parquet``.
    VINTAGE_DATES_PATH: Output path for ``vintage_dates.parquet``.
    START_YEAR: Earliest year to scrape (2010).
    PUBLICATIONS: List of :class:`Publication` definitions.
'''

from dataclasses import dataclass
from pathlib import Path

BASE_URL = 'https://www.bls.gov'
DATA_DIR = Path('data/releases')
PARQUET_PATH = Path('data/release_dates.parquet')
VINTAGE_DATES_PATH = Path('data/vintage_dates.parquet')
START_YEAR = 2010


@dataclass(frozen=True)
class Publication:
    '''BLS publication: name, series code, index URL, and frequency.

    Attributes:
        name: Short name (e.g. 'ces', 'sae', 'qcew').
        series: BLS series code used in archive URLs (e.g. 'empsit', 'laus').
        index_url: Full URL of the news release archive index page.
        frequency: Either 'monthly' or 'quarterly'.
    '''

    name: str
    series: str
    index_url: str
    frequency: str  # 'monthly' | 'quarterly'


PUBLICATIONS = [
    Publication(
        name='ces',
        series='empsit',
        index_url=f'{BASE_URL}/bls/news-release/empsit.htm',
        frequency='monthly',
    ),
    Publication(
        name='sae',
        series='laus',
        index_url=f'{BASE_URL}/bls/news-release/laus.htm',
        frequency='monthly',
    ),
    Publication(
        name='qcew',
        series='cewqtr',
        index_url=f'{BASE_URL}/bls/news-release/cewqtr.htm',
        frequency='quarterly',
    ),
]
