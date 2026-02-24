'''Shared HTTP client for BLS requests: HTTP/2 and polite headers with retry.'''

from __future__ import annotations

import time
from typing import Optional

import httpx

USER_AGENT = 'Mozilla/5.0 (compatible; bls-revisions/0.2.0)'
DEFAULT_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-us,en;q=0.5',
}
DEFAULT_TIMEOUT = 60.0
MAX_RETRIES = 6


def create_client(
    *,
    http2: bool = True,
    headers: Optional[dict[str, str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Client:
    '''Build an httpx client with HTTP/2 and BLS-friendly headers.'''
    merged = {**DEFAULT_HEADERS}
    if headers:
        merged.update(headers)
    return httpx.Client(
        http2=http2,
        headers=merged,
        timeout=timeout,
    )


def get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = MAX_RETRIES,
) -> httpx.Response:
    '''GET with exponential backoff on 429 and transient 5xx.'''
    for attempt in range(max_retries):
        r = client.get(url, timeout=timeout)
        if r.status_code == 429 or r.status_code >= 500:
            wait = min(2**attempt, 60)
            print(f'    [{r.status_code}] retrying in {wait}s ...')
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r
