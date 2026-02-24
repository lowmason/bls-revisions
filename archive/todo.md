# TODO: Merge bls-release-dates into bls-revisions

Merge the `bls-release-dates` repo into `bls-revisions` as a subpackage so there is one unified repo. The `bls-release-dates` repo lives at `../bls-release-dates` relative to this repo.

## Conventions

- Single quotes for all string literals
- Triple single-quote docstrings
- Polars over pandas
- Two blank lines after class/function definitions at module level

## 1. Restructure the package layout

Rename `src/bls_revisions/` download modules into a `download/` subpackage, and absorb `bls-release-dates` as a `release_dates/` subpackage.

Target structure:

```
src/bls_revisions/
    __init__.py               # Public API (re-exports from subpackages)
    __main__.py               # Unified CLI with subcommands: release, download, process
    _client.py                # Shared HTTP client with retry (already exists)
    release_dates/            # Absorbed from bls-release-dates
        __init__.py
        __main__.py           # From bls_release_dates/__main__.py
        config.py             # From bls_release_dates/config.py
        parser.py             # From bls_release_dates/parser.py
        read.py               # From bls_release_dates/read.py
        scraper.py            # From bls_release_dates/scraper.py
        vintage_dates.py      # From bls_release_dates/vintage_dates.py
    download/                 # Current ces.py + qcew.py become a subpackage
        __init__.py           # Exports download_ces, download_qcew
        ces.py                # From current bls_revisions/ces.py
        qcew.py               # From current bls_revisions/qcew.py
    processing/               # Former top-level scripts moved into package
        __init__.py           # Exports DATA_DIR, STATES constants
        ces_national.py       # From src/process_ces_national.py
        ces_states.py         # From src/process_ces_states.py
        qcew.py               # From src/process_qcew.py
        vintage_series.py     # From src/vintage_series.py
```

## 2. Copy bls-release-dates source files

Copy the following files from `../bls-release-dates/src/bls_release_dates/` into `src/bls_revisions/release_dates/`:

- `config.py`
- `parser.py`
- `scraper.py`
- `read.py`
- `vintage_dates.py`
- `__main__.py`
- `__init__.py`

These files use relative imports internally (e.g. `from .config import ...`), which should continue to work as-is inside the new `release_dates/` subpackage.

Update `release_dates/__init__.py` to export `build_vintage_dates` and `VINTAGE_DATES_PATH` in addition to the existing exports.

## 3. Move download modules into download/ subpackage

Move `src/bls_revisions/ces.py` → `src/bls_revisions/download/ces.py` and `src/bls_revisions/qcew.py` → `src/bls_revisions/download/qcew.py`.

Create `download/__init__.py` that exports `download_ces` and `download_qcew`.

Update the `_client` import in both files: `from bls_revisions._client import ...` → `from .._client import ...`.

## 4. Move processing scripts into processing/ subpackage

Move these top-level scripts into `src/bls_revisions/processing/`:

- `src/process_ces_national.py` → `processing/ces_national.py`
- `src/process_ces_states.py` → `processing/ces_states.py`
- `src/process_qcew.py` → `processing/qcew.py`
- `src/vintage_series.py` → `processing/vintage_series.py`

Create `processing/__init__.py` with shared constants that these modules all duplicate:

```python
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
```

## 5. Fix imports in processing modules

In each processing module, replace:

| Old | New |
|-----|-----|
| Hardcoded `PROJECT_ROOT` / `DATA_DIR` definitions | `from bls_revisions.processing import DATA_DIR` |
| Hardcoded `STATES` list | `from bls_revisions.processing import STATES` |
| Hardcoded `VINTAGE_DATES_PATH = Path('/Users/lowell/Projects/bls-release-dates/data/vintage_dates.parquet')` | `from bls_revisions.release_dates.config import VINTAGE_DATES_PATH` |

Also remove the duplicated `STATES` list from each file since it now lives in `processing/__init__.py`.

## 6. Update the top-level __init__.py

Replace `src/bls_revisions/__init__.py` with a unified public API:

```python
'''BLS revisions: release date scraping, vintage downloads, and processing.'''

from bls_revisions.download import download_ces, download_qcew
from bls_revisions.release_dates import (
    read_release_dates,
    read_vintage_dates,
    build_vintage_dates,
)

__all__ = [
    'download_ces',
    'download_qcew',
    'read_release_dates',
    'read_vintage_dates',
    'build_vintage_dates',
]
```

## 7. Create unified CLI entry point

Replace `src/bls_revisions/__main__.py` with a unified CLI that supports subcommands:

- `bls-revisions` (no args) → run all three steps in order
- `bls-revisions release` → run `release_dates.__main__.main()` (scrape + build parquets)
- `bls-revisions download` → run `download_ces()` + `download_qcew()`
- `bls-revisions process` → run `ces_national.main()`, `ces_states.main()`, `qcew.main()`, `vintage_series.main()`

Use lazy imports inside each subcommand function so the full dep tree isn't loaded when only running one step.

## 8. Update pyproject.toml

- Remove the `bls-release-dates @ git+https://github.com/lowmason/bls_release_dates.git` dependency (it's now internal)
- Add the deps that `bls-release-dates` brought: `lxml>=5.0.0` (if not already present)
- Merge the `[project.optional-dependencies]` docs extras from `bls-release-dates` (`mkdocs`, `mkdocs-material`, `mkdocstrings[python]`)
- Update the `[project.scripts]` entry point: `bls-revisions = "bls_revisions.__main__:main"`
- Bump version to `0.2.0`

## 9. Update README.md

Update README to document the unified CLI, the three subcommands, the Python API (`from bls_revisions import ...`), and the new package structure.

## 10. Fix string quoting

Convert all double-quoted strings in the absorbed and moved files to single quotes to match the project convention. The `bls-release-dates` source files use double quotes throughout.

## 11. Remove hardcoded FRED API key

In `processing/ces_states.py`, the original `process_ces_states.py` has a hardcoded FRED API key as a default. Remove it and require `FRED_API_KEY` to be set as an env variable:

```python
fred_api_key = os.environ.get('FRED_API_KEY', '')
if not fred_api_key:
    raise RuntimeError('FRED_API_KEY environment variable is required')
```

## 12. Verify

- `python -c "import ast; [ast.parse(open(f).read()) for f in __import__('pathlib').Path('src').rglob('*.py')]"` — all files parse
- `grep -rn 'from bls_release_dates' src/` — no stale imports
- `grep -rn "Users/lowell" src/` — no hardcoded local paths
- `grep -rn '8d08f0f04f7d3e53' src/` — no hardcoded API keys