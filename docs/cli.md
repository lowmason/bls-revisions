# CLI Reference

The `bls-revisions` command is installed as a console script via the `[project.scripts]` entry in `pyproject.toml`.

## Usage

```
bls-revisions [release|download|process]
```

Running without arguments executes all three stages in order.

## Subcommands

### `bls-revisions release`

Scrape BLS news-release archive pages and build the release/vintage date datasets.

**What it does:**

1. For each publication (CES, SAE, QCEW), fetches the archive index page from bls.gov.
2. Parses the index to discover individual release URLs.
3. Downloads each release HTML file (skipping files that already exist).
4. Extracts the embargo (vintage) date from each HTML file.
5. Writes `data/release_dates.parquet`.
6. Applies publication-specific revision logic and writes `data/vintage_dates.parquet`.

**Output files:**

| File | Description |
|------|-------------|
| `data/releases/{pub}/{pub}_{YYYY}_{MM}.htm` | Raw release HTML files |
| `data/release_dates.parquet` | Publication, ref_date, vintage_date |
| `data/vintage_dates.parquet` | Expanded with revision codes |

---

### `bls-revisions download`

Download raw data files from bls.gov.

**What it does:**

1. Scrapes the CES vintage-data page and downloads `cesvinall.zip` (extracted) and `cesvin*.xlsx` workbooks.
2. Downloads the QCEW revisions CSV.

**Output files:**

| File | Description |
|------|-------------|
| `data/ces/cesvinall/*.csv` | Triangular revision CSV matrices |
| `data/ces/cesvin*.xlsx` | Excel workbooks |
| `data/qcew/qcew-revisions.csv` | QCEW revisions CSV |

---

### `bls-revisions process`

Run all data processing pipelines.

**What it does:**

1. **CES national** — reads triangular CSVs, extracts revision diagonals, joins vintage dates.
2. **CES states (SAE)** — queries ALFRED for each state × industry series, extracts initial and latest levels.
3. **QCEW** — reshapes the revisions CSV from wide to long format, joins vintage dates.
4. **Vintage series** — combines all three sources, adds region and division aggregations.

**Environment variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `FRED_API_KEY` | Yes | FRED API key for the SAE processing step |
| `BLS_API_KEY` | No | Raises the BLS daily request limit |

**Output files:**

| File | Description |
|------|-------------|
| `data/ces_revisions.parquet` | CES national revisions |
| `data/sae_revisions.parquet` | SAE state-level revisions |
| `data/qcew_revisions.parquet` | QCEW revisions |
| `data/revisions.parquet` | Combined dataset with aggregations |

## Implementation

The CLI is implemented in `bls_revisions.__main__` ([source](https://github.com/lowmason/bls-revisions/blob/main/src/bls_revisions/__main__.py)). Each subcommand uses lazy imports so only the required dependencies are loaded.

::: bls_revisions.__main__
    options:
      show_root_heading: true
      show_source: true
