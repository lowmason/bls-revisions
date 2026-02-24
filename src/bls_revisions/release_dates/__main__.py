'''CLI entry point: download BLS releases, build release_dates and vintage_dates.'''

import asyncio
from pathlib import Path

import polars as pl

from .config import DATA_DIR, PARQUET_PATH, PUBLICATIONS
from .parser import collect_release_dates
from .scraper import download_all, fetch_index, parse_index_page
from .vintage_dates import build_vintage_dates, VINTAGE_DATES_PATH

import httpx


async def download_all_publications() -> None:
    '''Download release HTML files for all configured publications.'''
    async with httpx.AsyncClient(
        http2=True, follow_redirects=True, timeout=30.0,
    ) as client:
        for pub in PUBLICATIONS:
            print(f'Fetching index for {pub.name}...')
            html = await fetch_index(client, pub.index_url)
            entries = parse_index_page(html, pub.name, pub.series, pub.frequency)
            print(f'  Found {len(entries)} releases for {pub.name}')
            paths = await download_all(entries, pub.name)
            print(f'  Downloaded {len(paths)} new files for {pub.name}')


def build_dataframe() -> pl.DataFrame:
    '''Parse all downloaded HTML files into a release_dates DataFrame.'''
    rows = []
    for pub in PUBLICATIONS:
        pub_dir = DATA_DIR / pub.name
        if not pub_dir.exists():
            continue
        for row in collect_release_dates(pub.name, pub_dir):
            rows.append(row)

    df = pl.DataFrame(
        rows, schema={'publication': pl.Utf8, 'ref_date': pl.Date, 'vintage_date': pl.Date},
        orient='row',
    ).sort('publication', 'ref_date')
    return df


def main() -> None:
    '''Run full pipeline: download, build release_dates, build vintage_dates.'''
    asyncio.run(download_all_publications())

    print('Building release_dates...')
    df = build_dataframe()
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(PARQUET_PATH)
    print(f'Wrote {PARQUET_PATH} ({len(df)} rows)')

    print('Building vintage_dates...')
    vdf = build_vintage_dates()
    VINTAGE_DATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    vdf.write_parquet(VINTAGE_DATES_PATH)
    print(f'Wrote {VINTAGE_DATES_PATH} ({len(vdf)} rows)')


if __name__ == '__main__':
    main()
