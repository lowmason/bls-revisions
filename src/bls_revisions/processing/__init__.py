'''Data processing pipelines for CES, SAE, and QCEW revisions.

Each processing module reads raw downloaded files plus the
``vintage_dates.parquet`` lookup, then produces a tidy Parquet dataset:

- :mod:`~bls_revisions.processing.ces_national` -- CES triangular
  revision matrices (national, all industries).
- :mod:`~bls_revisions.processing.ces_states` -- SAE state-level
  revisions fetched from the FRED/ALFRED API.
- :mod:`~bls_revisions.processing.qcew` -- QCEW quarterly revisions.
- :mod:`~bls_revisions.processing.vintage_series` -- combines all three
  sources and aggregates to region / division geography.

Attributes:
    PROJECT_ROOT: Absolute path to the repository root.
    DATA_DIR: Absolute path to the ``data/`` directory.
    STATES: FIPS codes for all 50 US states, DC, and Puerto Rico.
'''

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'

STATES = [
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12',
    '13', '15', '16', '17', '18', '19', '20', '21', '22', '23',
    '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
    '34', '35', '36', '37', '38', '39', '40', '41', '42', '44',
    '45', '46', '47', '48', '49', '50', '51', '53', '54', '55',
    '56', '72',
]
