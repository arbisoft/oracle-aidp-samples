# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Minimal Notion REST client: auth headers, pagination, throttling, retries."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterator, Optional

BASE_URL = "https://api.notion.com"
PAGE_SIZE = 100
_RETRY_STATUSES = (500, 502, 503, 504)
_MAX_BACKOFF_SECONDS = 30


class NotionApiError(Exception):
    """Notion answered with an error status that is not worth retrying."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"Notion API error {status} ({code}): {message}")
        self.status = status
        self.code = code
        self.message = message


class NotionClient:
    def __init__(
        self,
        token: str,
        api_version: str = "2026-03-11",
        requests_per_second: float = 3.0,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        max_attempts: int = 8,
        max_rate_limit_waits: int = 20,
        timeout: int = 60,
    ):
        if not token:
            raise ValueError("a Notion token is required")
        if session is None:
            import requests

            session = requests.Session()
        self._session = session
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": api_version,
            "Content-Type": "application/json",
        }
        self._sleep = sleep
        self._clock = clock
        self._min_interval = 1.0 / requests_per_second
        self._next_allowed: Optional[float] = None
        self._max_attempts = max_attempts
        self._max_rate_limit_waits = max_rate_limit_waits
        self._timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = BASE_URL + path
        failures = 0
        rate_limit_waits = 0
        while True:
            self._throttle()
            try:
                response = self._session.request(
                    method, url, headers=self._headers, json=body, params=params, timeout=self._timeout
                )
            except OSError:
                # requests' connection and timeout errors are OSError subclasses.
                failures += 1
                if failures >= self._max_attempts:
                    raise
                self._sleep(self._backoff(failures))
                continue

            status = response.status_code
            if status == 429:
                rate_limit_waits += 1
                if rate_limit_waits > self._max_rate_limit_waits:
                    raise self._error(response)
                self._sleep(self._retry_after(response))
                continue
            if status in _RETRY_STATUSES:
                failures += 1
                if failures >= self._max_attempts:
                    raise self._error(response)
                self._sleep(self._backoff(failures))
                continue
            if status >= 400:
                raise self._error(response)
            return response.json()

    def paginate(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield every item of ``results`` across all pages of a list endpoint."""
        is_get = method.upper() == "GET"
        cursor: Optional[str] = None
        seen_cursors = set()
        while True:
            page_body, page_params = body, params
            if is_get:
                page_params = dict(params or {}, page_size=PAGE_SIZE)
                if cursor:
                    page_params["start_cursor"] = cursor
            else:
                page_body = dict(body or {}, page_size=PAGE_SIZE)
                if cursor:
                    page_body["start_cursor"] = cursor
            payload = self.request(method, path, body=page_body, params=page_params)
            for item in payload.get("results") or []:
                yield item
            cursor = payload.get("next_cursor")
            # Guard against a missing or repeated cursor so a bad response cannot loop forever.
            if not payload.get("has_more") or not cursor or cursor in seen_cursors:
                return
            seen_cursors.add(cursor)

    def _throttle(self) -> None:
        now = self._clock()
        if self._next_allowed is not None and now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._clock()
        self._next_allowed = now + self._min_interval

    @staticmethod
    def _backoff(failures: int) -> int:
        return min(2 ** (failures - 1), _MAX_BACKOFF_SECONDS)

    @staticmethod
    def _retry_after(response: Any) -> float:
        try:
            return max(float(response.headers.get("Retry-After", 1)), 0.0)
        except (TypeError, ValueError):
            return 1.0

    @staticmethod
    def _error(response: Any) -> NotionApiError:
        code, message = "unknown", ""
        try:
            payload = response.json()
            code = payload.get("code") or code
            message = payload.get("message") or ""
        except (ValueError, AttributeError):
            message = (getattr(response, "text", "") or "")[:500]
        return NotionApiError(response.status_code, code, message)
