"""Jira Cloud REST API v3 reader for AIDP notebooks.

Read-only. Uses only ``requests``. Credentials come from OCI Vault or
environment variables and are never logged. See
connectors/jira/REQUIREMENTS.md.

HTTP retry/error handling and credential resolution live in the sibling
``_shared`` package (``aidp_http.py``, ``aidp_secrets.py``), uploaded once to
the same workspace folder — see connectors/jira/CLAUDE.md for the exact
upload/``sys.path`` steps.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Tuple
from zoneinfo import ZoneInfo

import aidp_http
import aidp_secrets

ENV_SITE = "JIRA_SITE"
ENV_EMAIL = "JIRA_EMAIL"
ENV_API_TOKEN = "JIRA_API_TOKEN"

# Re-exported under Jira-specific names for callers and this module's own
# code; these are exactly aidp_http's generic classes/functions, not
# subclasses or wrappers — every connector in this repo shares one engine.
JiraError = aidp_http.ConnectorError
JiraAuthError = aidp_http.ConnectorAuthError
JiraRateLimitError = aidp_http.ConnectorRateLimitError
MAX_BACKOFF_SECONDS = aidp_http.MAX_BACKOFF_SECONDS
post_json = aidp_http.post_json
get_json = aidp_http.get_json
redact = aidp_http.redact


def normalize_site(site: str) -> str:
    """Return a bare host name from ``site`` or raise ValueError."""
    host = re.sub(r"^https?://", "", (site or "").strip(), flags=re.I).rstrip("/")
    if not host or "/" in host or " " in host:
        raise ValueError(
            "site must be a bare host name such as <site>.atlassian.net"
        )
    return host


def credentials_from_env() -> Tuple[str, str, str]:
    """Return ``(site, email, api_token)``.

    Resolved via OCI Vault first (if ``OCI_VAULT_ID`` is set), falling back to
    plain environment variables — see ``connectors/_shared/aidp_secrets.py``.
    Despite the name (kept for backward compatibility), this is no longer
    environment-only.
    """
    values, missing = {}, []
    for name in (ENV_SITE, ENV_EMAIL, ENV_API_TOKEN):
        try:
            values[name] = aidp_secrets.get_secret(name)
        except KeyError:
            missing.append(name)
    if missing:
        raise JiraError("missing environment variable(s): " + ", ".join(missing))
    return (
        normalize_site(values[ENV_SITE]),
        values[ENV_EMAIL],
        values[ENV_API_TOKEN],
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

    Jira interprets JQL date-time literals in this timezone, not UTC. Fetch it
    once and pass it to ``search_issues(..., tz_name=...)`` — there is no safe
    automatic default, since assuming UTC silently skips or delays issues for
    any account whose Jira profile timezone is not UTC.
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
    the JQL watermark bounds in that timezone, not UTC. Omitting it defaults to
    UTC, which silently skips or delays issues for any account not on UTC —
    pass it explicitly for a correct incremental load.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be between 1 and {}".format(MAX_PAGE_SIZE))
    build_jql(query=query)  # validates the caller's query before any request

    host = normalize_site(site)
    url = "https://{}/rest/api/3/search/jql".format(host)
    upper = until if until is not None else (now() if now else datetime.now(timezone.utc))
    lower = since - timedelta(seconds=overlap_seconds) if since is not None else None
    tz = ZoneInfo(tz_name) if tz_name else timezone.utc
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
    data = [normalize_issue(i) for i in issues]
    return spark.createDataFrame(data, schema=ddl)
