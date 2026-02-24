'''Shared HTTP client with retry logic for BLS and FRED requests.

Provides a pre-configured :class:`httpx.Client` that speaks HTTP/2,
sends browser-like headers, and automatically retries on rate-limit
(429) or transient server errors (5xx) using exponential back-off.

If the ``BLS_API_KEY`` environment variable is set, the key is
appended as a ``registrationkey`` query parameter on requests to
``bls.gov`` domains, raising the daily rate limit from 25 to 500
requests.

Attributes:
    USER_AGENT: User-Agent string sent with every request.
    DEFAULT_HEADERS: Default header dict merged into every client.
    DEFAULT_TIMEOUT: Per-request timeout in seconds (60).
    MAX_RETRIES: Maximum retry attempts before giving up (8).
'''

from __future__ import annotations

import os
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
MAX_RETRIES = 8


def _bls_api_key() -> str:
    '''Return ``BLS_API_KEY`` from the environment, or an empty string.'''
    return os.environ.get('BLS_API_KEY', '')


def create_client(
    *,
    http2: bool = True,
    headers: Optional[dict[str, str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Client:
    '''Build an :class:`httpx.Client` with HTTP/2 and BLS-friendly headers.

    Args:
        http2: Enable HTTP/2 negotiation (default ``True``).
        headers: Extra headers merged on top of :data:`DEFAULT_HEADERS`.
        timeout: Per-request timeout in seconds.

    Returns:
        A configured ``httpx.Client``.  Caller is responsible for closing it.
    '''
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
    '''GET *url* with exponential back-off on 429 and transient 5xx errors.

    If ``BLS_API_KEY`` is set and the URL contains ``bls.gov``, the API
    key is sent as a ``registrationkey`` query parameter.

    Args:
        client: An open ``httpx.Client``.
        url: Absolute URL to fetch.
        timeout: Per-request timeout in seconds.
        max_retries: Maximum number of retry attempts.

    Returns:
        The successful :class:`httpx.Response`.

    Raises:
        httpx.HTTPStatusError: After exhausting retries or on a
            non-retryable HTTP error.
    '''
    params: dict[str, str] = {}
    api_key = _bls_api_key()
    if api_key and 'bls.gov' in url:
        params['registrationkey'] = api_key

    for attempt in range(max_retries):
        r = client.get(url, timeout=timeout, params=params)
        if r.status_code == 429 or r.status_code >= 500:
            wait = min(2**attempt, 120)
            print(f'    [{r.status_code}] retrying in {wait}s ...')
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r
