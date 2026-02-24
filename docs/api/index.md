# API Reference

This section documents the full Python API for `bls-revisions`.

## Top-level exports

The package re-exports the most commonly used functions from its subpackages:

::: bls_revisions
    options:
      members:
        - download_ces
        - download_qcew
        - read_release_dates
        - read_vintage_dates
        - build_vintage_dates
      show_root_heading: false
      show_source: false

## Subpackages

- **[Client](client.md)** — shared HTTP client with retry logic
- **[Download](download/index.md)** — CES and QCEW data file downloaders
- **[Release Dates](release-dates/index.md)** — BLS release date scraping and parsing
- **[Processing](processing/index.md)** — data processing pipelines
