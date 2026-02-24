# Revision Logic

The `vintage_dates.parquet` dataset assigns revision codes to each *(publication, ref_date)* pair based on publication-specific rules.  This page documents those rules.

## Revision numbers

| Code | Meaning |
|------|---------|
| 0 | Initial release |
| 1 | First revision |
| 2 | Second revision |
| 3, 4 | Third/fourth revision (QCEW only) |
| 9 | Benchmark revision (CES and SAE only) |

## CES (Current Employment Statistics)

**Frequency:** Monthly

**Revisions per reference period:** 0, 1, 2, and optionally 9 (benchmark).

| Revision | When published |
|----------|---------------|
| 0 | Same month as the initial release |
| 1 | One month after the initial release |
| 2 | Two months after the initial release |
| 9 | Benchmark: March ref_date only, published in the January release of the following year |

The benchmark revision (9) only applies to **March** reference dates.  The vintage date is taken from the January release of the next year (e.g., March 2023 → January 2024 release).  `benchmark_revision` is set to 1.

## SAE (State and Area Employment)

**Frequency:** Monthly

**Revisions per reference period:** 0, 1, and optionally 9 (benchmark).

| Revision | When published |
|----------|---------------|
| 0 | Same month as the initial release |
| 1 | One month after the initial release |
| 9 | Benchmark: April–September ref_dates, up to two benchmarks |

SAE benchmarks are more complex than CES:

- **First benchmark** (`benchmark_revision=1`): April–September ref_dates are re-benchmarked in the March release of Y+1.
- **Second benchmark** (`benchmark_revision=2`): The same ref_dates get a *second* re-benchmark in the March release of Y+2 (the "re-replacement" revision).

This double-benchmark pattern is unique to SAE.

## QCEW (Quarterly Census of Employment and Wages)

**Frequency:** Quarterly

**Revisions per reference period:** Varies by quarter.

| Quarter | Max revision | Revisions |
|---------|-------------|-----------|
| Q1 (Jan–Mar) | 4 | 0, 1, 2, 3, 4 |
| Q2 (Apr–Jun) | 3 | 0, 1, 2, 3 |
| Q3 (Jul–Sep) | 2 | 0, 1, 2 |
| Q4 (Oct–Dec) | 1 | 0, 1 |

QCEW has **no benchmark revisions** — `benchmark_revision` is always 0.

The asymmetry arises because QCEW data for a given year is progressively revised through the following year's quarterly releases.  Q1 data has the most revision opportunities because it was published earliest.

## Vintage date computation

For revisions 1, 2, etc., the vintage date is computed by offsetting the initial vintage date by the corresponding number of months:

```
vintage_date(revision=n) = vintage_date(revision=0) + n months
```

For benchmark revisions (code 9), the vintage date is looked up from the release schedule:

- **CES:** January release of Y+1 for March ref_date of year Y
- **SAE:** March release of Y+1 (first benchmark) and March release of Y+2 (second benchmark) for April–September ref_dates of year Y

## Implementation

The revision logic is implemented in [`bls_revisions.release_dates.vintage_dates`][bls_revisions.release_dates.vintage_dates].
