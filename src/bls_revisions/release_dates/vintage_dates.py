'''Build vintage_dates dataset from release_dates.parquet with revision codes.

Revision semantics (publication-specific):

- revision: 0 = initial release; 1 = first revision (1 month later); 2 = second
  revision (2 months later) for CES. SAE has only 0 and 1. Never 9.
- benchmark_revision: 0 = not benchmarked. When Jan Y+1 is published (Feb),
  all year Y months get benchmark_revision=1 for CES. SAE: two benchmarks
  (1 and 2), same idea.
- **CES:** 0, 1, 2 + benchmark rows (revision=2, benchmark_revision=1) for
  all months of each benchmarked year.
- **SAE:** 0, 1 + benchmark rows (revision=1, benchmark_revision=1 or 2) for
  all months.
- **QCEW:** by quarter of ref_date - Q1: 0,1,2,3,4; Q2: 0,1,2,3; Q3: 0,1,2;
  Q4: 0,1. No benchmarks (benchmark_revision=0).
'''

from datetime import date
from pathlib import Path

import polars as pl

from .config import PARQUET_PATH, VINTAGE_DATES_PATH

# Publication-specific revision sets (benchmark rows reuse the max revision with benchmark_revision>0).
CES_MONTHLY_REVISIONS = [0, 1, 2]
SAE_MONTHLY_REVISIONS = [0, 1]

# Supplemental release dates for ranges sometimes missing from scraped HTML
# (e.g. CES and SAE Jan–Mar 2016). Merged into release_dates in build_vintage_dates.
SUPPLEMENTAL_RELEASE_DATES = [
    ('ces', date(2016, 1, 12), date(2016, 2, 5)),
    ('ces', date(2016, 2, 12), date(2016, 3, 4)),
    ('sae', date(2016, 1, 12), date(2016, 3, 14)),
    ('sae', date(2016, 2, 12), date(2016, 3, 25)),
    ('sae', date(2016, 3, 12), date(2016, 4, 15)),
]
# QCEW max revision by quarter (ref_date month): Q1=4, Q2=3, Q3=2, Q4=1


def _add_ces_revisions(df: pl.DataFrame) -> pl.DataFrame:
    '''Add CES revisions 0, 1, 2 (benchmark rows added separately).'''
    parts = []
    for n in CES_MONTHLY_REVISIONS:
        if n == 0:
            parts.append(
                df.filter(pl.col('publication') == 'ces').with_columns(
                    pl.lit(0).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                )
            )
        else:
            parts.append(
                df.filter(pl.col('publication') == 'ces').with_columns(
                    pl.col('vintage_date').dt.offset_by(f'{n}mo').alias('vintage_date'),
                    pl.lit(n).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                )
            )
    return pl.concat(parts)


def _add_sae_revisions(df: pl.DataFrame) -> pl.DataFrame:
    '''Add SAE revisions 0, 1 (benchmark rows added separately).'''
    parts = []
    for n in SAE_MONTHLY_REVISIONS:
        if n == 0:
            parts.append(
                df.filter(pl.col('publication') == 'sae').with_columns(
                    pl.lit(0).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                )
            )
        else:
            parts.append(
                df.filter(pl.col('publication') == 'sae').with_columns(
                    pl.col('vintage_date').dt.offset_by(f'{n}mo').alias('vintage_date'),
                    pl.lit(n).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                )
            )
    return pl.concat(parts)


def _add_qcew_revisions(df: pl.DataFrame) -> pl.DataFrame:
    '''Add QCEW revisions 0..max by quarter (Q1=4, Q2=3, Q3=2, Q4=1).

    Args:
        df: Release dates DataFrame with publication, ref_date, vintage_date.

    Returns:
        DataFrame with QCEW rows expanded by quarter-specific revision counts.
    '''
    qcew = df.filter(pl.col('publication') == 'qcew').with_columns(
        pl.col('ref_date').dt.month().alias('month'),
    )
    max_rev = (
        pl.when(pl.col('month').is_between(1, 3))
        .then(4)
        .when(pl.col('month').is_between(4, 6))
        .then(3)
        .when(pl.col('month').is_between(7, 9))
        .then(2)
        .otherwise(1)
        .alias('max_rev')
    )
    qcew = qcew.with_columns(max_rev)
    parts = []
    for n in range(5):  # 0..4
        if n == 0:
            parts.append(
                qcew.with_columns(
                    pl.lit(0).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                ).select('publication', 'ref_date', 'vintage_date', 'revision', 'benchmark_revision')
            )
        else:
            subset = qcew.filter(pl.col('max_rev') >= n)
            parts.append(
                subset.with_columns(
                    pl.col('vintage_date').dt.offset_by(f'{n * 3}mo').alias('vintage_date'),
                    pl.lit(n).alias('revision'),
                    pl.lit(0).alias('benchmark_revision'),
                ).select('publication', 'ref_date', 'vintage_date', 'revision', 'benchmark_revision')
            )
    return pl.concat(parts)


def _ces_benchmark_vintage_dates(release_df: pl.DataFrame) -> pl.DataFrame:
    '''When Jan Y+1 is published, all year Y ref_dates get benchmark_revision=1.

    Args:
        release_df: Release dates DataFrame (publication, ref_date, vintage_date).

    Returns:
        DataFrame of CES benchmark rows (revision=max, benchmark_revision=1).
    '''
    ces = release_df.filter(pl.col('publication') == 'ces')
    jan_releases = ces.filter(pl.col('ref_date').dt.month() == 1).select(
        pl.col('ref_date').dt.year().alias('benchmark_year'),
        pl.col('vintage_date').alias('benchmark_vintage'),
    ).unique()
    ces_refs = ces.select('publication', 'ref_date').unique().with_columns(
        (pl.col('ref_date').dt.year() + 1).alias('benchmark_year'),
    )
    return ces_refs.join(
        jan_releases,
        on='benchmark_year',
        how='inner',
    ).select(
        pl.col('publication'),
        pl.col('ref_date'),
        pl.col('benchmark_vintage').alias('vintage_date'),
        pl.lit(max(CES_MONTHLY_REVISIONS)).alias('revision'),
        pl.lit(1).alias('benchmark_revision'),
    )


def _sae_benchmark_vintage_dates(release_df: pl.DataFrame) -> pl.DataFrame:
    '''Two benchmarks per ref_date: benchmark_revision=1 (March Y+1) and 2 (March Y+2).

    Applies to all SAE months. Never uses revision=9.

    Args:
        release_df: Release dates DataFrame (publication, ref_date, vintage_date).

    Returns:
        DataFrame of SAE benchmark rows (revision=max, benchmark_revision 1 or 2).
    '''
    sae = release_df.filter(pl.col('publication') == 'sae')
    march_releases = sae.filter(pl.col('ref_date').dt.month() == 3).select(
        pl.col('ref_date').dt.year().alias('year'),
        pl.col('vintage_date').alias('benchmark_vintage'),
    )
    sae_refs = sae.select('publication', 'ref_date').unique().with_columns(
        pl.col('ref_date').dt.year().alias('ref_year'),
    )
    first = sae_refs.with_columns((pl.col('ref_year') + 1).alias('benchmark_year')).join(
        march_releases,
        left_on='benchmark_year',
        right_on='year',
        how='inner',
    ).select(
        pl.col('publication'),
        pl.col('ref_date'),
        pl.col('benchmark_vintage').alias('vintage_date'),
        pl.lit(max(SAE_MONTHLY_REVISIONS)).alias('revision'),
        pl.lit(1).alias('benchmark_revision'),
    )
    second = sae_refs.with_columns((pl.col('ref_year') + 2).alias('benchmark_year')).join(
        march_releases,
        left_on='benchmark_year',
        right_on='year',
        how='inner',
    ).select(
        pl.col('publication'),
        pl.col('ref_date'),
        pl.col('benchmark_vintage').alias('vintage_date'),
        pl.lit(max(SAE_MONTHLY_REVISIONS)).alias('revision'),
        pl.lit(2).alias('benchmark_revision'),
    )
    return pl.concat([first, second])


def build_vintage_dates(release_dates_path: Path | None = None) -> pl.DataFrame:
    '''Build vintage_dates DataFrame from release_dates parquet.

    Applies publication-specific revision logic (CES 0,1,2 + benchmark;
    SAE 0,1 + benchmarks; QCEW 0..max by quarter), filters to vintage_date
    <= today, and sorts by publication, ref_date, vintage_date, revision,
    benchmark_revision.

    Args:
        release_dates_path: Path to release_dates.parquet. Defaults to
            config.PARQUET_PATH.

    Returns:
        Polars DataFrame with columns publication, ref_date, vintage_date,
        revision, benchmark_revision.
    '''
    path = release_dates_path or PARQUET_PATH
    df = pl.read_parquet(path)

    # Merge supplemental release dates for ranges sometimes missing from scraped data
    supplemental = pl.DataFrame(
        [
            {'publication': p, 'ref_date': ref, 'vintage_date': vint}
            for p, ref, vint in SUPPLEMENTAL_RELEASE_DATES
        ],
        schema={'publication': pl.Utf8, 'ref_date': pl.Date, 'vintage_date': pl.Date},
    )
    existing_keys = df.select('publication', 'ref_date').unique()
    supplemental = supplemental.join(
        existing_keys, on=['publication', 'ref_date'], how='anti'
    )
    if supplemental.height > 0:
        df = pl.concat([df, supplemental]).unique(subset=['publication', 'ref_date'])

    # Publication-specific revisions: CES 0,1,2; SAE 0,1; QCEW 0..max by quarter
    with_revisions = pl.concat([
        _add_ces_revisions(df),
        _add_sae_revisions(df),
        _add_qcew_revisions(df),
    ])

    # Benchmark revisions (CES all months, SAE all months)
    ces_bench = _ces_benchmark_vintage_dates(df)
    sae_bench = _sae_benchmark_vintage_dates(df)
    benchmark_rows = pl.concat([ces_bench, sae_bench])

    out = (
        pl.concat([with_revisions, benchmark_rows])
        .filter(pl.col('vintage_date') <= pl.lit(date.today()))
        .sort(['publication', 'ref_date', 'vintage_date', 'revision', 'benchmark_revision'])
    )
    return out


def main() -> None:
    '''Build vintage_dates from release_dates and write data/vintage_dates.parquet.

    Reads data/release_dates.parquet, applies revision logic, and writes
    data/vintage_dates.parquet. Creates the output directory if needed.
    '''
    df = build_vintage_dates()
    VINTAGE_DATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(VINTAGE_DATES_PATH)
    print(f'Wrote {VINTAGE_DATES_PATH} ({len(df)} rows)')


if __name__ == '__main__':
    main()
