'''Process downloaded CES triangular revision files into a clean parquet file.'''

from __future__ import annotations

from pathlib import Path

import polars as pl
from polars import selectors as cs

from bls_revisions.processing import DATA_DIR
from bls_revisions.release_dates.config import VINTAGE_DATES_PATH

CES_DIR = DATA_DIR / 'ces' / 'cesvinall'

CES_DOMAIN = [
    ('000000', '00', 'Total Non-Farm'),
    ('050000', '05', 'Total Private'),
    ('060000', '06', 'Goods-Producing Industries'),
    ('070000', '07', 'Service-Providing Industries'),
    ('080000', '08', 'Private Service-Providing'),
]

CES_SUPERSECTOR = [
    ('100000', '10', 'Natural Resources and Mining'),
    ('200000', '20', 'Construction'),
    ('300000', '30', 'Manufacturing'),
    ('400000', '40', 'Trade, Transportation, and Utilities'),
    ('500000', '50', 'Information'),
    ('550000', '55', 'Financial Activities'),
    ('600000', '60', 'Professional and Business Services'),
    ('650000', '65', 'Education and Health Services'),
    ('700000', '70', 'Leisure and Hospitality'),
    ('800000', '80', 'Other Services'),
    ('900000', '90', 'Government'),
]

CES_SECTOR = [
    ('102100', '21', 'Mining, quarrying, and oil and gas extraction'),
    ('310000', '31', 'Durable goods'),
    ('320000', '32', 'Nondurable goods'),
    ('414200', '41', 'Wholesale trade'),
    ('420000', '42', 'Retail trade'),
    ('430000', '43', 'Transportation and warehousing'),
    ('442200', '22', 'Utilities'),
    ('555200', '52', 'Finance and insurance'),
    ('555300', '53', 'Real estate and rental and leasing'),
    ('605400', '54', 'Professional, scientific, and technical services'),
    ('605500', '55', 'Management of companies and enterprises'),
    (
        '605600',
        '56',
        'Administrative and support and waste management and remediation services',
    ),
    ('656100', '61', 'Private educational services'),
    ('656200', '62', 'Health care and social assistance'),
    ('707100', '71', 'Arts, entertainment, and recreation'),
    ('707200', '72', 'Accommodation and food services'),
    ('909100', '91', 'Federal'),
    ('909200', '92', 'State government'),
    ('909300', '93', 'Local government'),
]


def _build_schema(path: Path) -> tuple[dict[str, pl.DataType], list[str], dict[str, str]]:
    '''Build the column schema, selection list, and rename mapping from a sample file.'''
    rows = pl.read_csv(path / 'tri_050000_SA.csv')
    columns = rows.columns

    schema: dict[str, pl.DataType] = {'year': pl.UInt16, 'month': pl.UInt8}
    schema.update({col: pl.Float64 for col in columns if col not in schema})

    years = list(range(2010, 2025))
    month_names = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
    ]

    selected: list[str] = ['year', 'month']
    renamed: dict[str, str] = {}
    for yr in years:
        for m, mo in enumerate(month_names):
            y = str(yr)[2:]
            selected.append(f'{mo}_{y}')
            renamed[f'{mo}_{y}'] = f'emp_{yr}_{m + 1}'

    return schema, selected, renamed


def read_triangular_ces(
    path: Path,
    file: str,
    industry_type: str,
    industry_code: str,
    schema: dict[str, pl.DataType],
    selected: list[str],
    renamed: dict[str, str],
) -> pl.DataFrame:
    tri_df = (
        pl.read_csv(f'{path}/{file}.csv', schema_overrides=schema)
        .select(selected)
        .rename(renamed)
        .with_columns(
            ref_date=pl.date(pl.col('year'), pl.col('month'), pl.lit(12, pl.UInt8)),
            ref_year=pl.col('year'),
            ref_month=pl.col('month'),
        )
        .filter(pl.col('ref_date').gt(pl.date(2015, 12, 12)))
        .select(cs.starts_with('ref_'), cs.starts_with('emp_'))
    )

    emp_cols = [c for c in tri_df.columns if c.startswith('emp_')]
    n_cols = len(emp_cols)
    n_rows = len(tri_df)

    revisions = []
    for k in range(3):
        n = min(n_cols, n_rows - k)
        col_years: list[int] = []
        col_months: list[int] = []
        diag_values: list[float | None] = []

        for j in range(n):
            parts = emp_cols[j].split('_')
            col_years.append(int(parts[1]))
            col_months.append(int(parts[2]))
            diag_values.append(tri_df[j + k, emp_cols[j]])

        revisions.append(
            pl.DataFrame(
                {
                    'year': col_years,
                    'month': col_months,
                    'revision': k,
                    'employment': diag_values,
                }
            )
        )

    return (
        pl.concat(revisions)
        .with_columns(
            ref_date=pl.date(
                pl.col('year'), pl.col('month'), pl.lit(12, pl.UInt8)
            ),
            employment=pl.col('employment').cast(pl.Float64),
        )
        .sort('ref_date', 'revision')
        .select(
            ref_date=pl.col('ref_date'),
            ref_year=pl.col('year'),
            ref_month=pl.col('month'),
            revision=pl.col('revision'),
            geographic_type=pl.lit('national', pl.Utf8),
            geographic_code=pl.lit('00', pl.Utf8),
            industry_type=pl.lit(industry_type, pl.Utf8),
            industry_code=pl.lit(industry_code, pl.Utf8),
            employment=pl.col('employment'),
        )
    )


def main() -> None:
    ces_files = {p.stem for p in CES_DIR.iterdir()}
    print(f'Number of CES files: {len(ces_files)}')

    schema, selected, renamed = _build_schema(CES_DIR)

    codes: list[tuple[str, str, str, str]] = []
    for adj in ['NSA', 'SA']:
        for d in CES_DOMAIN:
            codes.append((f'tri_{d[0]}_{adj}', d[1], d[2], 'domain'))
        for ss in CES_SUPERSECTOR:
            codes.append((f'tri_{ss[0]}_{adj}', ss[1], ss[2], 'supersector'))
        for s in CES_SECTOR:
            codes.append((f'tri_{s[0]}_{adj}', s[1], s[2], 'sector'))

    realized_codes = []
    for file, industry_code, industry_name, level in codes:
        industry_type = 'national' if industry_code == '00' else level
        if file in ces_files:
            realized_codes.append((file, industry_type, industry_code, industry_name))
    print(f'Number of realized industry codes: {len(realized_codes)}')

    nsa_ces_national: list[pl.DataFrame] = []
    sa_ces_national: list[pl.DataFrame] = []
    for file, industry_type, industry_code, _name in realized_codes:
        tri_df = read_triangular_ces(
            CES_DIR, file, industry_type, industry_code, schema, selected, renamed
        )
        if 'NSA' in file:
            nsa_ces_national.append(tri_df)
        else:
            sa_ces_national.append(tri_df)

    print(f'Number of NSA revisions: {len(nsa_ces_national)}')
    print(f'Number of SA revisions: {len(sa_ces_national)}')

    ces_national_nsa_df = pl.concat(nsa_ces_national).with_columns(
        source=pl.lit('ces', pl.Utf8),
        seasonally_adjusted=pl.lit(False, pl.Boolean),
    )
    ces_national_sa_df = pl.concat(sa_ces_national).with_columns(
        source=pl.lit('ces', pl.Utf8),
        seasonally_adjusted=pl.lit(True, pl.Boolean),
    )

    print(f'Number of NSA revision observations: {ces_national_nsa_df.height:,}')
    print(f'Number of SA revision observations: {ces_national_sa_df.height:,}')

    vintage_dates = (
        pl.read_parquet(VINTAGE_DATES_PATH)
        .filter(pl.col('publication') == 'ces')
        .drop('publication')
    )

    ces_national_df = (
        pl.concat([ces_national_nsa_df, ces_national_sa_df])
        .join(vintage_dates, on=['ref_date', 'revision'], how='left')
        .select(
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

    print(f'Number of CES revision observations: {ces_national_df.height:,}')

    ces_national_dups = ces_national_df.unique(
        subset=[
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
    )
    assert ces_national_df.height == ces_national_dups.height

    out_path = DATA_DIR / 'ces_revisions.parquet'
    ces_national_df.write_parquet(out_path)
    print(f'Wrote {ces_national_df.height:,} rows to {out_path}')


if __name__ == '__main__':
    main()
