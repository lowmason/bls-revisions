'''Fetch BLS archive index pages and download release HTML files.'''

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag

from .config import BASE_URL, DATA_DIR, PUBLICATIONS, START_YEAR

MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
]
MONTH_TO_NUM = {name: i for i, name in enumerate(MONTH_NAMES, 1)}
QUARTER_TO_MONTH = {'First': 3, 'Second': 6, 'Third': 9, 'Fourth': 12}

# Match 'Month YYYY' in list item or link text (CES/SAE)
MONTH_YEAR_RE = re.compile(
    r'(January|February|March|April|May|June|July|August|'
    r'September|October|November|December)\s+(\d{4})',
    re.IGNORECASE,
)
# Match 'First/Second/Third/Fourth Quarter' (QCEW)
QUARTER_RE = re.compile(
    r'(First|Second|Third|Fourth)\s+Quarter',
    re.IGNORECASE,
)
# Year in heading
YEAR_RE = re.compile(r'\b(20\d{2})\b')


# Archive link: /archives/{series}_MMDDYYYY.htm
def archive_href_re(series: str) -> re.Pattern:
    '''Build a regex that matches archive hrefs for the given BLS series.

    Args:
        series: BLS series code (e.g. 'empsit', 'laus', 'cewqtr').

    Returns:
        Compiled regex matching paths like /news.release/archives/{series}_MMDDYYYY.htm.
    '''
    return re.compile(rf'/news\.release/archives/{re.escape(series)}_\d{{8}}\.htm')


@dataclass
class ReleaseEntry:
    '''A single release: reference year, month, and archive URL.

    Attributes:
        ref_year: Reference year (e.g. 2010).
        ref_month: Reference month 1-12.
        url: Full URL to the release HTML (e.g. .../archives/empsit_04022010.htm).
    '''

    ref_year: int
    ref_month: int
    url: str


def _find_next_ul(element: Tag) -> Tag | None:
    '''Find the next <ul> sibling after the given element.

    Stops at the next heading (h1, h2, ...) without returning a ul.

    Args:
        element: A BeautifulSoup Tag (e.g. an h4).

    Returns:
        The next <ul> sibling if found, None otherwise.
    '''
    sibling = element.find_next_sibling()
    while sibling:
        if sibling.name == 'ul':
            return sibling
        if sibling.name and sibling.name.startswith('h'):
            break
        sibling = sibling.find_next_sibling()
    return None


def _resolve_url(url: str) -> str:
    '''Turn a possibly relative URL into an absolute URL.

    Args:
        url: URL that may be relative (e.g. /bls/news-release/...) or absolute.

    Returns:
        Absolute URL using BASE_URL for relative paths.
    '''
    if url.startswith('http'):
        return url
    base = BASE_URL.rstrip('/')
    path = url if url.startswith('/') else f'/{url}'
    return f'{base}{path}'


def parse_index_page(html: str, publication_name: str, series: str, frequency: str) -> list[ReleaseEntry]:
    '''Parse an archive index page into release entries.

    Only includes entries for years >= START_YEAR. For monthly publications,
    parses 'Month YYYY' from list/link text; for quarterly, parses 'First/Second/
    Third/Fourth Quarter' and uses the section year.

    Args:
        html: Raw HTML of the BLS news release archive index page.
        publication_name: Publication name (e.g. 'ces', 'sae', 'qcew').
        series: BLS series code used to match archive links.
        frequency: 'monthly' or 'quarterly'.

    Returns:
        List of ReleaseEntry (ref_year, ref_month, url) for each release found.
    '''
    soup = BeautifulSoup(html, 'lxml')
    archive_re = archive_href_re(series)
    entries: list[ReleaseEntry] = []

    for h4 in soup.find_all('h4'):
        year_match = YEAR_RE.search(h4.get_text())
        if not year_match:
            continue
        year = int(year_match.group(1))
        if year < START_YEAR:
            continue

        ul = _find_next_ul(h4)
        if not ul:
            continue

        for li in ul.find_all('li', recursive=False):
            li_text = li.get_text()
            # Find archive .htm link
            anchor = None
            for a in li.find_all('a', href=True):
                if archive_re.search(a.get('href', '')):
                    anchor = a
                    break
            if not anchor:
                continue

            href = anchor.get('href', '')
            if not archive_re.search(href):
                continue
            url = _resolve_url(href)

            if frequency == 'monthly':
                month_match = MONTH_YEAR_RE.search(li_text) or MONTH_YEAR_RE.search(anchor.get_text() or '')
                if not month_match:
                    continue
                month_name, year_str = month_match.group(1), month_match.group(2)
                ref_year = int(year_str)
                ref_month = MONTH_TO_NUM.get(month_name)
                if ref_month is None:
                    continue
            else:
                quarter_match = QUARTER_RE.search(li_text)
                if not quarter_match:
                    continue
                quarter_name = quarter_match.group(1)
                ref_year = year
                ref_month = QUARTER_TO_MONTH.get(quarter_name)
                if ref_month is None:
                    continue

            entries.append(ReleaseEntry(ref_year=ref_year, ref_month=ref_month, url=url))

    return entries


DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
}


async def fetch_index(client: httpx.AsyncClient, url: str) -> str:
    '''Fetch index page HTML.

    Args:
        client: HTTP client to use.
        url: URL of the archive index page.

    Returns:
        Response body text. Raises on HTTP errors.
    '''
    r = await client.get(url, headers=DEFAULT_HEADERS)
    r.raise_for_status()
    return r.text


async def download_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    entry: ReleaseEntry,
    publication_name: str,
    out_dir: Path,
) -> Path | None:
    '''Download one release HTML to out_dir/{pub}_{yyyy}_{mm}.htm.

    Skips download if the file already exists. Uses the semaphore to limit
    concurrency when called from download_all.

    Args:
        client: HTTP client to use.
        semaphore: Semaphore for concurrency control.
        entry: Release entry with ref_year, ref_month, and url.
        publication_name: Publication name for the filename.
        out_dir: Directory to write the .htm file into.

    Returns:
        Path to the written or existing file, or None if skipped.
    '''
    out_dir.mkdir(parents=True, exist_ok=True)
    mm = f'{entry.ref_month:02d}'
    path = out_dir / f'{publication_name}_{entry.ref_year}_{mm}.htm'
    if path.exists():
        return path

    async with semaphore:
        try:
            r = await client.get(entry.url)
            r.raise_for_status()
            path.write_text(r.text, encoding='utf-8')
            return path
        except Exception:
            raise


async def download_all(
    entries: list[ReleaseEntry],
    publication_name: str,
    concurrency: int = 5,
) -> list[Path]:
    '''Download all release HTMLs for a publication; skip existing files.

    Args:
        entries: List of ReleaseEntry from parse_index_page.
        publication_name: Publication name (e.g. 'ces', 'sae', 'qcew').
        concurrency: Max concurrent requests (default 5).

    Returns:
        List of paths to written or already-existing .htm files.
    '''
    out_dir = DATA_DIR / publication_name
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        http2=True,
        base_url=BASE_URL,
        follow_redirects=True,
        timeout=30.0,
        headers=DEFAULT_HEADERS,
    ) as client:
        tasks = [
            download_one(client, semaphore, e, publication_name, out_dir)
            for e in entries
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    paths: list[Path] = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            raise r
        if r is not None:
            paths.append(r)
    return paths
