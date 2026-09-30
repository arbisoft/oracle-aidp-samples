"""Jira Cloud REST API v3 reader for AIDP notebooks.

Read-only. Uses only ``requests``. Credentials come from OCI Vault or
environment variables and are never logged. Upload this single file to a
workspace folder, put that folder on ``sys.path`` and ``import jira_client``;
the example notebook shows the steps.

Sections: HTTP retry engine, credential lookup, Jira query/paging, and
conversion to a Spark DataFrame.
"""

from __future__ import annotations

import base64
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ENV_SITE = "JIRA_SITE"
ENV_EMAIL = "JIRA_EMAIL"
ENV_API_TOKEN = "JIRA_API_TOKEN"

# --------------------------------------------------------------------------
# HTTP retry engine
# --------------------------------------------------------------------------

class JiraError(Exception):
    """Any failure talking to Jira Cloud. Messages never contain credentials."""


class JiraAuthError(JiraError):
    """HTTP 401 or 403."""


class JiraRateLimitError(JiraError):
    """HTTP 429 persisted after the bounded number of retries."""


MAX_BACKOFF_SECONDS = 60.0
TRANSIENT_STATUSES = (502, 503, 504)


def _is_permanent_request_error(exc) -> bool:
    """A requests error that retrying cannot fix, e.g. InvalidURL or MissingSchema."""
    try:
        import requests
    except ImportError:
        return False
    permanent = (
        requests.exceptions.InvalidURL,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidSchema,
        requests.exceptions.InvalidHeader,
        requests.exceptions.URLRequired,
    )
    return isinstance(exc, permanent)


def _retry_after_seconds(response, fallback: float) -> float:
    value = response.headers.get("Retry-After")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(seconds):  # NaN/inf would crash or stall sleep()
        return fallback
    return min(max(seconds, 0.0), MAX_BACKOFF_SECONDS)


def _request_json(make_request, *, max_retries=5, sleep=time.sleep) -> dict:
    """Shared 429/auth/error/JSON handling for one GET or POST call.

    ``make_request`` is a zero-argument callable that performs the actual
    ``session.get(...)`` / ``session.post(...)`` and returns the response.
    """
    attempt = 0
    while True:
        try:
            response = make_request()
        except OSError as exc:  # requests' connection/timeout errors subclass OSError
            if _is_permanent_request_error(exc):
                raise JiraError(
                    "request failed ({}); check the site name".format(type(exc).__name__)
                ) from None
            if attempt >= max_retries:
                raise JiraError(
                    "network error ({}) after {} retries".format(type(exc).__name__, max_retries)
                ) from None
            sleep(min(2 ** attempt, MAX_BACKOFF_SECONDS))
            attempt += 1
            continue
        status = response.status_code
        if status in TRANSIENT_STATUSES and attempt < max_retries:
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status == 429:
            if attempt >= max_retries:
                raise JiraRateLimitError(
                    "rate limited (HTTP 429) after {} retries".format(max_retries)
                )
            sleep(_retry_after_seconds(response, min(2 ** attempt, MAX_BACKOFF_SECONDS)))
            attempt += 1
            continue
        if status in (401, 403):
            raise JiraAuthError("HTTP {}: check the credentials".format(status))
        if status >= 400:
            raise JiraError(
                "HTTP {} from the API: {}".format(status, _safe_body(response))
            )
        try:
            payload = response.json()
        except ValueError:
            raise JiraError(
                "response was not JSON; check the site/instance name and URL"
            ) from None
        if not isinstance(payload, dict):
            raise JiraError(
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


# --------------------------------------------------------------------------
# Credential lookup: OCI Vault, then environment, then default
# --------------------------------------------------------------------------

_MISSING = object()


class SecretNotFoundError(KeyError):
    """No value found anywhere; ``vault_error`` holds the Vault failure, if any."""

    def __init__(self, message, vault_error=None):
        super().__init__(message)
        self.vault_error = vault_error


def get_secret(name: str, default: Any = _MISSING, vault_id: Optional[str] = None) -> str:
    """Resolve ``name`` from OCI Vault, then an environment variable, then ``default``.

    Args:
        name: The secret name. Used as the Vault secret name (case-sensitive)
            and, uppercased, as the environment-variable fallback.
        default: Returned if the secret can't be resolved anywhere. Raises
            ``KeyError`` instead if left unspecified.
        vault_id: OCI Vault OCID. Defaults to the ``OCI_VAULT_ID`` environment
            variable; if neither is set, the Vault step is skipped entirely.

    Returns:
        The resolved secret value.

    Raises:
        KeyError: If no value can be resolved and ``default`` is not provided.
    """
    vault_id = vault_id or os.environ.get("OCI_VAULT_ID")
    vault_error = None
    if vault_id:
        try:
            return _get_from_vault(name, vault_id)
        except Exception as exc:
            vault_error = exc  # Vault is best-effort; fall through to the environment.

    env_value = os.environ.get(name.upper())
    if env_value is not None:
        return env_value

    if default is _MISSING:
        detail = " (Vault lookup failed: {})".format(vault_error) if vault_error else ""
        raise SecretNotFoundError(
            "secret {!r} not found in OCI Vault, environment, or default{}".format(name, detail),
            vault_error,
        )
    return default


def _get_from_vault(name: str, vault_id: str) -> str:
    """Look up ``name`` as a secret in the given OCI Vault.

    Imported lazily so unit tests, and any connector spike, never need the
    ``oci`` package installed unless they actually reach this path.
    """
    import oci

    config = oci.config.from_file()
    vaults_client = oci.vault.VaultsClient(config)
    secrets_client = oci.secrets.SecretsClient(config)

    secrets = oci.pagination.list_call_get_all_results(
        vaults_client.list_secrets,
        compartment_id=os.environ.get("OCI_COMPARTMENT_ID") or config.get("tenancy"),
        vault_id=vault_id,
    ).data
    match = next(
        (s for s in secrets
         if s.secret_name == name and getattr(s, "lifecycle_state", "ACTIVE") == "ACTIVE"),
        None,
    )
    if match is None:
        raise KeyError("secret {!r} not in vault {!r}".format(name, vault_id))

    bundle = secrets_client.get_secret_bundle(secret_id=match.id).data
    content = bundle.secret_bundle_content.content  # base64-encoded
    return base64.b64decode(content).decode("utf-8")


# --------------------------------------------------------------------------
# Jira Cloud
# --------------------------------------------------------------------------


_JIRA_CLOUD_HOST = re.compile(r"^[a-z0-9][a-z0-9-]*\.atlassian\.net$")


def normalize_site(site: str) -> str:
    """Return a bare host name from ``site`` or raise ValueError."""
    host = re.sub(r"^https?://", "", (site or "").strip(), flags=re.I).rstrip("/").lower()
    # Basic auth sends the API token to this host, so accept only Jira Cloud sites.
    if not _JIRA_CLOUD_HOST.match(host):
        raise ValueError(
            "site must be a Jira Cloud host name such as <site>.atlassian.net"
        )
    return host


def credentials_from_env() -> Tuple[str, str, str]:
    """Return ``(site, email, api_token)``.

    Resolved via OCI Vault first (if ``OCI_VAULT_ID`` is set; secrets are looked
    up in ``OCI_COMPARTMENT_ID`` if set, else the tenancy root), falling back to
    plain environment variables.
    Despite the name (kept for backward compatibility), this is no longer
    environment-only.
    """
    values, missing, notes = {}, [], []
    for name in (ENV_SITE, ENV_EMAIL, ENV_API_TOKEN):
        try:
            values[name] = get_secret(name)
            if not values[name].strip():  # blank counts as missing
                raise SecretNotFoundError("secret {!r} is empty".format(name))
        except KeyError as exc:
            missing.append(name)
            if getattr(exc, "vault_error", None) and not notes:
                notes.append(str(exc.vault_error))
    if missing:
        hint = " (Vault lookup failed: {})".format(notes[0]) if notes else ""
        raise JiraError("missing environment variable(s): " + ", ".join(missing) + hint)
    return (
        normalize_site(values[ENV_SITE]),
        values[ENV_EMAIL].strip(),
        values[ENV_API_TOKEN].strip(),
    )


def jira_session(email: str, api_token: str):
    """A requests.Session with HTTP Basic auth (email:api_token) and JSON Accept."""
    import requests

    session = requests.Session()
    session.auth = (email, api_token)
    session.headers.update({"Accept": "application/json"})
    return session


_FORBIDDEN_IN_QUERY = re.compile(r"order\s+by", re.I)


def format_jql_timestamp(value: datetime, tz=timezone.utc) -> str:
    """``YYYY-MM-DD HH:MM`` in ``tz`` (JQL date-time literal).

    Jira interprets a JQL date-time literal in the *searching account's own*
    timezone, not UTC — pass the account's zone (see ``account_timezone()``) as
    ``tz``, not just the default. A naive ``value`` is treated as already being
    in UTC before conversion to ``tz``.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(tz).strftime("%Y-%m-%d %H:%M")


def build_jql(*, since=None, until=None, query=None, tz=timezone.utc) -> str:
    """JQL for one search. Caller query is ANDed with the watermark window.

    ``tz`` must be the searching account's own timezone (``account_timezone()``)
    for the bounds to mean what they say; the default UTC is only correct for
    an account whose Jira profile timezone is UTC.
    """
    if query and _FORBIDDEN_IN_QUERY.search(query):
        raise ValueError("query must not contain ORDER BY; paging adds its own ordering")
    parts = []
    # query.strip() guards a whitespace-only query, which would otherwise
    # produce a bare, invalid "()" clause after stripping.
    if query and query.strip():
        parts.append("(%s)" % query.strip())
    if since is not None:
        parts.append('updated >= "%s"' % format_jql_timestamp(since, tz))
    if until is not None:
        parts.append('updated <= "%s"' % format_jql_timestamp(until, tz))
    parts.append("ORDER BY updated ASC, key ASC")
    return " AND ".join(parts[:-1]) + (" " if parts[:-1] else "") + parts[-1]


def account_timezone(session, site, *, timeout=60, max_retries=5, sleep=time.sleep) -> str:
    """The searching account's IANA timezone name, from GET /rest/api/3/myself.

    Jira interprets JQL date-time literals in this timezone, not UTC. Assuming
    UTC silently skips or delays issues for any account whose Jira profile
    timezone is not UTC, so ``search_issues`` calls this itself when ``tz_name``
    is omitted. Call it directly only to reuse one lookup across several searches.
    """
    host = normalize_site(site)
    url = "https://{}/rest/api/3/myself".format(host)
    payload = get_json(session, url, timeout=timeout, max_retries=max_retries, sleep=sleep)
    tz_name = payload.get("timeZone")
    if not tz_name:
        raise JiraError("account profile response has no timeZone field")
    return tz_name


MAX_PAGE_SIZE = 5000


def search_issues(
    session,
    site,
    *,
    query=None,
    fields=None,
    since=None,
    overlap_seconds=300,
    until=None,
    page_size=100,
    tz_name=None,
    timeout=60,
    max_retries=5,
    sleep=time.sleep,
    now=None,
) -> Iterator[dict]:
    """Yield every issue matching ``query`` and updated within a fixed window.

    Pages via the server's opaque ``nextPageToken``; never builds or assumes an
    offset. ``until`` is fixed at the first request (default: now), so issues
    updated during the read fall into the next run rather than this one.

    The recommended watermark contract, used by the example notebook: after
    writing the result, read ``MAX(updated)`` back from the target table and
    pass it as the next run's ``since`` — this needs no extra state beyond the
    table itself. Passing this call's ``until`` as the next call's ``since`` is
    an equally correct alternative for a caller that already tracks it, since
    both name the same instant; pick one contract per caller and be
    consistent. De-duplicate on issue ``key`` either way, since the overlap
    window re-reads a few issues on purpose.

    ``tz_name`` is the searching account's IANA timezone name (from
    ``account_timezone(session, site)``), e.g. ``"Asia/Karachi"``. Jira compares
    the JQL watermark bounds in that timezone, not UTC. When omitted, it is
    fetched from the account profile (one extra GET /myself); pass it to reuse a
    lookup or to force a zone, e.g. ``"UTC"``.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be between 1 and {}".format(MAX_PAGE_SIZE))
    build_jql(query=query)  # validates the caller's query before any request
    if tz_name is None:
        # Jira reads JQL date-times in the account's own timezone; never guess UTC.
        tz_name = account_timezone(
            session, site, timeout=timeout, max_retries=max_retries, sleep=sleep
        )

    host = normalize_site(site)
    url = "https://{}/rest/api/3/search/jql".format(host)
    upper = until if until is not None else (now() if now else datetime.now(timezone.utc))
    lower = since - timedelta(seconds=overlap_seconds) if since is not None else None
    try:
        tz = ZoneInfo(tz_name) if tz_name else timezone.utc
    except (ZoneInfoNotFoundError, ValueError):
        raise JiraError("unknown timezone name {!r}".format(tz_name)) from None
    # Default to every typed column, not just key+updated — a caller who passes
    # no fields= must still get summary/status/etc. populated in to_dataframe.
    if fields is None:
        fields = [name for name, _ in TYPED_FIELDS if name not in ("key", "updated")]
    field_list = list(dict.fromkeys(["key", "updated", *fields]))
    jql = build_jql(since=lower, until=upper, query=query, tz=tz)

    token = None
    seen_tokens = set()
    while True:
        body = {"jql": jql, "fields": field_list, "maxResults": page_size}
        if token is not None:
            body["nextPageToken"] = token
        payload = post_json(
            session, url, body, timeout=timeout, max_retries=max_retries, sleep=sleep
        )
        yield from payload.get("issues", [])
        if payload.get("isLast"):
            return
        next_token = payload.get("nextPageToken")
        if not next_token:
            raise JiraError("server did not report isLast but returned no nextPageToken")
        if next_token in seen_tokens:
            raise JiraError("paging did not advance; the server repeated a nextPageToken")
        seen_tokens.add(next_token)
        token = next_token


_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

TYPED_FIELDS = [
    ("key", "STRING"), ("summary", "STRING"), ("status", "STRING"),
    ("priority", "STRING"), ("assignee", "STRING"), ("reporter", "STRING"),
    ("issuetype", "STRING"), ("project", "STRING"),
    ("created", "TIMESTAMP"), ("updated", "TIMESTAMP"),
]

_NAME_FIELDS = {"status", "priority", "issuetype"}
_PERSON_FIELDS = {"assignee", "reporter"}


def _parse_jira_timestamp(name, text):
    try:
        return datetime.strptime(text, _TIMESTAMP_FORMAT)
    except (TypeError, ValueError):
        raise JiraError("field {} is not a Jira timestamp".format(name)) from None


def normalize_issue(issue: dict):
    """One issue as a tuple following TYPED_FIELDS order, plus a trailing
    ``raw_fields`` JSON string holding any requested field not in TYPED_FIELDS
    (e.g. a custom field) — never silently dropped."""
    fields = issue.get("fields") or {}
    out = []
    for name, sql_type in TYPED_FIELDS:
        if name == "key":
            out.append(issue.get("key"))
            continue
        value = fields.get(name)
        if value is None:
            out.append(None)
        elif name in _NAME_FIELDS:
            out.append(value.get("name"))
        elif name in _PERSON_FIELDS:
            out.append(value.get("displayName"))
        elif name == "project":
            out.append(value.get("key"))
        elif sql_type == "TIMESTAMP":
            out.append(_parse_jira_timestamp(name, value))
        else:
            out.append(value)
    typed_names = {name for name, _ in TYPED_FIELDS}
    extra = {k: v for k, v in fields.items() if k not in typed_names}
    out.append(json.dumps(extra, sort_keys=True, ensure_ascii=False) if extra else None)
    return tuple(out)


def to_dataframe(spark, issues):
    """A Spark DataFrame: one typed column per TYPED_FIELDS entry, plus a
    trailing ``raw_fields`` JSON-string column for any other requested field."""
    ddl = ", ".join("{} {}".format(n, t) for n, t in TYPED_FIELDS)
    ddl += ", raw_fields STRING"
    # The overlap window (or an issue updated mid-paging) can return one key
    # twice; keep only the newest row so a later MERGE never sees duplicates.
    updated_at = [n for n, _ in TYPED_FIELDS].index("updated")
    latest = {}
    for row in map(normalize_issue, issues):
        seen = latest.get(row[0])
        if seen is None or (row[updated_at] or datetime.min.replace(tzinfo=timezone.utc)) >= (
            seen[updated_at] or datetime.min.replace(tzinfo=timezone.utc)
        ):
            latest[row[0]] = row
    return spark.createDataFrame(list(latest.values()), schema=ddl)


def parse_watermark(text):
    """Turn a ``yyyy-MM-dd HH:mm:ss[.SSS]`` UTC string into an aware UTC datetime.

    PySpark hands TIMESTAMP values back as naive datetimes in the *driver's*
    local timezone, so the notebook reads the watermark as a UTC string with
    ``date_format`` and parses it here instead. ``None`` (empty table) stays None.
    """
    if text is None:
        return None
    fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in text else "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
