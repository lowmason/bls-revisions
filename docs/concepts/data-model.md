# Data Model

## Core concept: the revision

A **revision** occurs whenever the BLS publishes a new estimate for an already-released reference period.  For example, the January 2024 employment number is first published in early February 2024 (revision 0), then revised in the March release (revision 1), the April release (revision 2), and potentially again during an annual benchmark (revision 9).

Each row in the output dataset represents a single *(reference period, vintage, survey)* observation.

## Schema

All output Parquet files share a common set of columns:

| Column | Type | Description |
|--------|------|-------------|
| `source` | `Utf8` | Survey identifier: `ces`, `sae`, or `qcew` |
| `seasonally_adjusted` | `Boolean` | `true` for SA series, `false` for NSA |
| `geographic_type` | `Utf8` | One of `national`, `state`, `region`, `division` |
| `geographic_code` | `Utf8` | FIPS code (states), Census code (regions/divisions), or `00` (national) |
| `industry_type` | `Utf8` | Aggregation level: `domain`, `supersector`, or `sector` |
| `industry_code` | `Utf8` | Two-digit NAICS-based code |
| `ref_date` | `Date` | Reference period — always the 12th of the month |
| `vintage_date` | `Date` | Date the estimate was published |
| `revision` | `UInt8` | Revision number (0 = initial, 1+ = revisions, 9 = benchmark) |
| `benchmark_revision` | `UInt8` | 0 = not a benchmark, 1 = first benchmark, 2 = second (SAE only) |
| `employment` | `Float64` | Employment level in thousands |

## Intermediate datasets

### `release_dates.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `publication` | `Utf8` | `ces`, `sae`, or `qcew` |
| `ref_date` | `Date` | Reference period |
| `vintage_date` | `Date` | Date the reference period was first published |

### `vintage_dates.parquet`

Expands `release_dates.parquet` with publication-specific revision logic:

| Column | Type | Description |
|--------|------|-------------|
| `publication` | `Utf8` | `ces`, `sae`, or `qcew` |
| `ref_date` | `Date` | Reference period |
| `vintage_date` | `Date` | Date this particular revision was published |
| `revision` | `Int64` | Revision number |
| `benchmark_revision` | `Int64` | Benchmark flag |

## Geographic hierarchy

State-level data is aggregated up to Census regions and divisions:

```
National (00)
├── Region 1: Northeast
│   ├── Division 01: New England
│   └── Division 02: Middle Atlantic
├── Region 2: Midwest
│   ├── Division 03: East North Central
│   └── Division 04: West North Central
├── Region 3: South
│   ├── Division 05: South Atlantic
│   ├── Division 06: East South Central
│   └── Division 07: West South Central
└── Region 4: West
    ├── Division 08: Mountain
    └── Division 09: Pacific
```

## Industry hierarchy

Industries follow the CES/NAICS classification at three levels:

- **Domain** (codes 00, 05–08): broad aggregates like Total Nonfarm, Total Private
- **Supersector** (codes 10–90): e.g. Construction, Manufacturing, Government
- **Sector** (codes 21–93): detailed sectors like Mining, Durable Goods, Retail Trade

## Reference date convention

All reference dates use the **12th of the month** as a convention.  This avoids end-of-month ambiguity and makes date arithmetic straightforward.
