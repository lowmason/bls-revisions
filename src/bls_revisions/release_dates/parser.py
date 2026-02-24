'''Extract release (vintage) dates from downloaded BLS release HTML files.

Each BLS news release contains an embargo line near the top of the page
reading something like *"... until 8:30 A.M. (ET) Friday, April 2,
2010"*.  The :data:`VINTAGE_DATE_RE` regex captures the date portion,
and :func:`parse_vintage_date` converts it to a :class:`~datetime.date`.

Attributes:
    VINTAGE_DATE_RE: Compiled regex matching the embargo-line date.
    MONTH_NAMES: Ordered list of English month names.
    MONTH_TO_NUM: Mapping from month name to 1-based integer.
'''

import re
from collections.abc import Iterator
from datetime import date
from pathlib import Path

VINTAGE_DATE_RE = re.compile(
    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+'
    r'(January|February|March|April|May|June|July|August|'
    r'September|October|November|December)\s+'
    r'(\d{1,2}),\s+(\d{4})',
    re.IGNORECASE,
)

MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
]
MONTH_TO_NUM = {name: i for i, name in enumerate(MONTH_NAMES, 1)}


def parse_vintage_date(html_content: str) -> date | None:
    '''Extract release (vintage) date from embargo line in HTML.

    Args:
        html_content: Raw HTML of a BLS release page.

    Returns:
        The release (vintage) date if found in the embargo line, None otherwise.
    '''
    match = VINTAGE_DATE_RE.search(html_content)
    if not match:
        return None
    month_name, day_str, year_str = match.group(1), match.group(2), match.group(3)
    month = MONTH_TO_NUM.get(month_name)
    if month is None:
        return None
    try:
        day = int(day_str)
        year = int(year_str)
        return date(year, month, day)
    except (ValueError, TypeError):
        return None


def parse_ref_from_path(path: Path) -> tuple[int, int] | None:
    '''Parse reference year and month from a release filename.

    Args:
        path: Path to a file named like {pub}_{yyyy}_{mm}.htm (e.g. ces_2010_03.htm).

    Returns:
        (year, month) if the stem matches the expected pattern and values are valid,
        None otherwise. Month is 1-12, year is 2000-2100.
    '''
    # filename: {pub}_{yyyy}_{mm}.htm
    stem = path.stem
    parts = stem.split('_')
    if len(parts) != 3:
        return None
    try:
        yyyy, mm = int(parts[1]), int(parts[2])
        if 1 <= mm <= 12 and 2000 <= yyyy <= 2100:
            return (yyyy, mm)
    except ValueError:
        pass
    return None


def ref_date_from_year_month(year: int, month: int) -> date:
    '''Return the reference date for a given year and month.

    The reference date is always the 12th of the reference month.

    Args:
        year: Reference year.
        month: Reference month (1-12).

    Returns:
        date(year, month, 12).
    '''
    return date(year, month, 12)


def parse_release_file(path: Path, publication_name: str) -> tuple[str, date, date] | None:
    '''Read a release HTML file and extract publication, ref_date, and vintage_date.

    ref_date is the 12th of the reference month (from the filename); vintage_date
    is parsed from the embargo line in the HTML.

    Args:
        path: Path to the release .htm file.
        publication_name: Publication name (e.g. 'ces', 'sae', 'qcew').

    Returns:
        (publication_name, ref_date, vintage_date) if both dates could be parsed,
        None otherwise.
    '''
    ref = parse_ref_from_path(path)
    if ref is None:
        return None
    ref_year, ref_month = ref
    ref_d = ref_date_from_year_month(ref_year, ref_month)

    try:
        content = path.read_text(encoding='utf-8')
    except OSError:
        return None

    vintage_d = parse_vintage_date(content)
    if vintage_d is None:
        return None

    return (publication_name, ref_d, vintage_d)


def collect_release_dates(publication_name: str, releases_dir: Path) -> Iterator[tuple[str, date, date]]:
    '''Walk a publication's release directory and yield parsed release rows.

    Glob pattern used: {publication_name}_*.htm. Logs a warning and skips files
    where the vintage date cannot be parsed.

    Args:
        publication_name: Publication name (e.g. 'ces', 'sae', 'qcew').
        releases_dir: Directory containing release .htm files.

    Yields:
        Tuples of (publication_name, ref_date, vintage_date) for each valid file.
    '''
    import logging

    log = logging.getLogger(__name__)
    pattern = f'{publication_name}_*.htm'
    for path in sorted(releases_dir.glob(pattern)):
        row = parse_release_file(path, publication_name)
        if row is None:
            log.warning('Could not parse release date from %s', path)
            continue
        yield row
