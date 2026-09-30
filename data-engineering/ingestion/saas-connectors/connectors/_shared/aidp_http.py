"""A bounded-retry JSON request engine shared by every REST-based connector.

Named ``aidp_http`` rather than ``http`` to avoid shadowing the Python
standard library package of that name once this directory is on ``sys.path``
(the same collision risk flagged for ``aidp_secrets`` vs. ``secrets``).

Unlike Oracle's own shown pattern for this repo's connectors
(`oracle_ai_data_platform_connectors.auth.user_principal.http_basic_session`,
which mounts a `urllib3.Retry` adapter forcelisting only 500/502/503/504),
this engine retries HTTP 429 explicitly and honours `Retry-After` — the REST
APIs this repo targets rate-limit with 429, which Oracle's shown helper does
not cover. The trade-off: this hand-rolled loop is
less idiomatic than configuring retry on the session/transport, but it lets a
test inject a fake `sleep` and assert exact backoff behaviour, which caught
real design questions during this repo's Jira Cloud build.
"""

from __future__ import annotations

import time


class ConnectorError(Exception):
    """Any failure talking to a connector's API. Messages never contain credentials."""


class ConnectorAuthError(ConnectorError):
    """HTTP 401 or 403."""


class ConnectorRateLimitError(ConnectorError):
    """HTTP 429 persisted after the bounded number of retries."""


MAX_BACKOFF_SECONDS = 60.0


def _retry_after_seconds(response, fallback: float) -> float:
    value = response.headers.get("Retry-After")
    try:
        return min(max(float(value), 0.0), MAX_BACKOFF_SECONDS)
    except (TypeError, ValueError):
        return fallback


def _request_json(make_request, *, max_retries=5, sleep=time.sleep) -> dict:
    """Shared 429/auth/error/JSON handling for one GET or POST call.

    ``make_request`` is a zero-argument callable that performs the actual
    ``session.get(...)`` / ``session.post(...)`` and returns the response.
    """
    attempt = 0
    while True:
        response = make_request()
        status = response.status_code
        if status == 429:
            if attempt >= max_retries:
                raise ConnectorRateLimitError(
                    "rate limited (HTTP 429) after {} retries".format(max_retries)
                )
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status in (401, 403):
            raise ConnectorAuthError("HTTP {}: check the credentials".format(status))
        if status >= 400:
            raise ConnectorError(
                "HTTP {} from the API: {}".format(status, _safe_body(response))
            )
        try:
            payload = response.json()
        except ValueError:
            raise ConnectorError(
                "response was not JSON; check the site/instance name and URL"
            ) from None
        if not isinstance(payload, dict):
            raise ConnectorError(
                "response JSON was a {}, not an object — every caller expects"
                " a dict".format(type(payload).__name__)
            )
        return payload


def _safe_body(response) -> str:
    try:
        return str(response.json())[:300]
    except ValueError:
        return "<non-JSON body>"


def post_json(session, url, body, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """POST ``body`` to ``url`` and return the parsed JSON, with bounded 429 retries."""
    return _request_json(
        lambda: session.post(url, json=body, timeout=timeout),
        max_retries=max_retries, sleep=sleep,
    )


def get_json(session, url, *, timeout=60, max_retries=5, sleep=time.sleep) -> dict:
    """GET ``url`` and return the parsed JSON, with the same retry/error handling as post_json."""
    return _request_json(
        lambda: session.get(url, timeout=timeout),
        max_retries=max_retries, sleep=sleep,
    )


def redact(text: str, *secrets: str) -> str:
    """Replace every non-empty secret in ``text`` with ``***``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text
