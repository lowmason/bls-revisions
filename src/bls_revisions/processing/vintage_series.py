'''Combine QCEW, CES, and SAE revision outputs into a single revisions dataset.'''

from __future__ import annotations

import polars as pl

from bls_revisions.processing import DATA_DIR, STATES

GROUP_COLS = [
    'source',
    'seasonally_adjusted',
    'geographic_type',
    'geographic_code',
    'industry_type',
    'industry_code',
    'ref_date',
    'vintage_date',
    'revision',
    'benchmark_revision',
]


def _load_geo_lookups() -> tuple[dict[str, str], dict[str, str]]:
    geo_codes = (
        pl.read_csv(
            DATA_DIR / 'reference' / 'geographic_codes.csv',
            schema_overrides={
                'region': pl.Utf8,
                'division': pl.Utf8,
                'state_fips': pl.Utf8,
            },
        )
        .filter(pl.col('state_fips').is_in(STATES))
        .select(
            region=pl.when(pl.col('state_fips').eq('72'))
            .then(pl.lit('3'))
            .otherwise(pl.col('region')),
            division=pl.when(pl.col('state_fips').eq('72'))
            .then(pl.lit('05'))
            .otherwise(pl.col('division')),
            state_fips=pl.col('state_fips'),
        )
        .unique()
        .sort('state_fips')
    )

    region_dict = {
        d['state_fips']: d['region'] for d in geo_codes.iter_rows(named=True)
    }
    division_dict = {
        d['state_fips']: d['division'] for d in geo_codes.iter_rows(named=True)
    }
    return region_dict, division_dict


def build_revisions(*, save: bool = True) -> pl.DataFrame:
    '''Combine QCEW, CES, and SAE revisions and aggregate to region/division.

    Parameters
    ----------
    save : bool
        If True (default), write the result to ``data/revisions.parquet``.

    Returns
    -------
    pl.DataFrame
        The combined revisions dataset.
    '''
    region_dict, division_dict = _load_geo_lookups()

    qcew = pl.read_parquet(DATA_DIR / 'qcew_revisions.parquet')
    ces = pl.read_parquet(DATA_DIR / 'ces_revisions.parquet').with_columns(
        revision=pl.col('revision').cast(pl.UInt8),
        benchmark_revision=pl.col('revision').cast(pl.UInt8),
    )
    sae = pl.read_parquet(DATA_DIR / 'sae_revisions.parquet')

    revisions_1 = pl.concat([qcew, ces, sae])

    revisions_national = revisions_1.filter(
        pl.col('geographic_type').eq('national')
    )
    revisions_state = revisions_1.filter(
        pl.col('geographic_type').eq('state'),
        pl.col('geographic_code').ne('00'),
    )
    assert revisions_state.height + revisions_national.height == revisions_1.height

    revisions_region = (
        revisions_state.with_columns(
            geographic_type=pl.lit('region', pl.Utf8),
            geographic_code=pl.col('geographic_code').replace_strict(
                region_dict, default=None
            ),
        )
        .group_by(GROUP_COLS)
        .agg(employment=pl.col('employment').sum())
    )

    revisions_division = (
        revisions_state.with_columns(
            geographic_type=pl.lit('division', pl.Utf8),
            geographic_code=pl.col('geographic_code').replace_strict(
                division_dict, default=None
            ),
        )
        .group_by(GROUP_COLS)
        .agg(employment=pl.col('employment').sum())
    )

    revisions_df = (
        pl.concat([
            revisions_national,
            revisions_state,
            revisions_region,
            revisions_division,
        ])
        .sort(*GROUP_COLS)
    )

    revisions_dups = revisions_df.unique(subset=GROUP_COLS)
    assert revisions_df.height == revisions_dups.height

    print(f'Revisions: {revisions_df.height:,} rows')

    if save:
        out_path = DATA_DIR / 'revisions.parquet'
        revisions_df.write_parquet(out_path)
        print(f'Wrote {out_path}')

    return revisions_df


def main() -> None:
    build_revisions(save=True)


if __name__ == '__main__':
    main()
