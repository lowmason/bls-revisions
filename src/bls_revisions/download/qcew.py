'''Download the QCEW revisions CSV (2017-present) from BLS.

The QCEW publishes a single CSV at :data:`QCEW_CSV_URL` containing
initial and revised employment counts by state and quarter.  This
module saves it to ``data/qcew/qcew-revisions.csv``.

Attributes:
    QCEW_CSV_URL: Direct URL to the CSV file on bls.gov.
    QCEW_FILENAME: Local filename used when saving the download.
'''

from __future__ import annotations

from pathlib import Path

import httpx

from .._client import get_with_retry

QCEW_CSV_URL = 'https://www.bls.gov/cew/revisions/qcew-revisions.csv'
QCEW_FILENAME = 'qcew-revisions.csv'


def download_qcew(
    data_dir: Path | None = None,
    *,
    client: httpx.Client | None = None,
) -> None:
    '''Download the QCEW revisions CSV to ``data/qcew/qcew-revisions.csv``.

    Args:
        data_dir: Root data directory.  Defaults to ``./data``.
        client: Optional pre-built :class:`httpx.Client`.  A new client
            is created (and closed on exit) if not provided.
    '''
    base = data_dir or Path.cwd() / 'data'
    qcew_dir = base / 'qcew'
    qcew_dir.mkdir(parents=True, exist_ok=True)
    out_path = qcew_dir / QCEW_FILENAME

    own_client = client is None
    if client is None:
        from .._client import create_client

        client = create_client()

    try:
        r = get_with_retry(client, QCEW_CSV_URL)
        r.raise_for_status()
        out_path.write_bytes(r.content)
        print(f'  saved {QCEW_FILENAME}')
    finally:
        if own_client:
            client.close()
