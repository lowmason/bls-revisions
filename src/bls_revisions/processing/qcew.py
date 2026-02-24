'''Process the QCEW revisions CSV into ``qcew_revisions.parquet``.

The raw CSV (downloaded by :mod:`bls_revisions.download.qcew`) has columns
for the initial value and up to four revised values per state x quarter x
month.  This module:

1. Filters to employment rows and maps area names to FIPS codes.
2. Unpivots the revision columns into a tidy long format.
3. Joins vintage dates from ``vintage_dates.parquet``.
4. Writes ``data/qcew_revisions.parquet``.
'''

from __future__ import annotations

import polars as pl

from bls_revisions.processing import DATA_DIR, STATES
from bls_revisions.release_dates.config import VINTAGE_DATES_PATH


def main() -> None:
    '''Read the QCEW revisions CSV, reshape, join vintage dates, and write Parquet.

    Writes ``data/qcew_revisions.parquet`` with the standard revision schema.
    '''
    geos = (
        pl.read_csv(
            DATA_DIR / 'reference' / 'geographic_codes.csv',
            schema_overrides={
                'region': pl.Utf8,
                'division': pl.Int64,
                'state_fips': pl.Utf8,
                'state_name': pl.Utf8,
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
            state_name=pl.col('state_name'),
        )
        .unique()
        .sort('state_fips')
    )

    states_dict = {'United States': '00'} | {
        d['state_name']: d['state_fips'] for d in geos.iter_rows(named=True)
    }

    qcew_1 = (
        pl.read_csv(
            DATA_DIR / 'qcew' / 'qcew-revisions.csv',
            schema_overrides={
                'Year': pl.Utf8,
                'Quarter': pl.Int64,
                'Area': pl.Utf8,
                'Field': pl.Utf8,
                'Initial Value': pl.Utf8,
                'First Revised Value': pl.Utf8,
                'Second Revised Value': pl.Utf8,
                'Third Revised Value': pl.Utf8,
                'Fourth Revised Value': pl.Utf8,
                'Final Value': pl.Utf8,
            },
        )
        .filter(
            pl.col('Area').is_in(states_dict.keys()),
            pl.col('Field').str.contains('Employment'),
        )
        .with_columns(
            qtr_date=pl.concat_str(
                pl.lit('12'),
                pl.col('Quarter').mul(3),
                pl.col('Year'),
                separator=' ',
            ).str.to_date(format='%d %m %Y'),
            ref_date=pl.concat_str(
                pl.lit('12'),
                pl.col('Field').str.replace(' Employment', ''),
                pl.col('Year'),
                separator=' ',
            ).str.to_date(format='%d %B %Y'),
            geographic_code=pl.col('Area').replace_strict(
                states_dict, default=None
            ),
        )
        .select(
            qtr_date=pl.col('qtr_date'),
            geographic_type=pl.when(pl.col('geographic_code').eq('00'))
            .then(pl.lit('national'))
            .otherwise(pl.lit('state')),
            geographic_code=pl.col('geographic_code'),
            industry_type=pl.lit('national'),
            industry_code=pl.lit('00'),
            ref_date=pl.col('ref_date'),
            emp_0=pl.col('Initial Value'),
            emp_1=pl.col('First Revised Value'),
            emp_2=pl.col('Second Revised Value'),
            emp_3=pl.col('Third Revised Value'),
            emp_4=pl.col('Fourth Revised Value'),
        )
        .unpivot(
            ['emp_0', 'emp_1', 'emp_2', 'emp_3', 'emp_4'],
            index=[
                'qtr_date',
                'geographic_type',
                'geographic_code',
                'industry_type',
                'industry_code',
                'ref_date',
            ],
            value_name='employment',
            variable_name='revision',
        )
        .filter(
            ~pl.col('employment').is_in(['Not yet published', 'Not applicable'])
        )
        .with_columns(
            revision=pl.col('revision')
            .str.replace('emp_', '')
            .cast(pl.UInt8),
            employment=pl.col('employment').cast(pl.Float64).truediv(1000),
        )
        .sort('geographic_code', 'ref_date', 'revision')
    )

    vintage_dates = (
        pl.read_parquet(VINTAGE_DATES_PATH)
        .filter(pl.col('publication').eq('qcew'))
        .select(
            qtr_date=pl.col('ref_date'),
            revision=pl.col('revision').cast(pl.UInt8),
            benchmark_revision=pl.col('benchmark_revision').cast(pl.UInt8),
            vintage_date=pl.col('vintage_date'),
        )
    )

    qcew_2 = qcew_1.join(vintage_dates, on=['qtr_date', 'revision'], how='left')

    output = (
        qcew_2.with_columns(
            source=pl.lit('qcew', pl.Utf8),
            seasonally_adjusted=pl.lit(False, pl.Boolean),
        ).select(
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
            'employment',
        )
    )

    out_path = DATA_DIR / 'qcew_revisions.parquet'
    output.write_parquet(out_path)
    print(f'Wrote {output.height:,} rows to {out_path}')


if __name__ == '__main__':
    main()
