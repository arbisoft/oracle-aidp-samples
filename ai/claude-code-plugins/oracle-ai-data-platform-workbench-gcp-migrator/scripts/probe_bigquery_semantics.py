#!/usr/bin/env python3
"""Confirm the BigQuery side of the translator rules with zero-byte queries.

Every probe is a query on literals. Each one is dry-run first and refused
unless BigQuery reports 0 bytes processed, so nothing is billed. The Spark
column is what Apache Spark 3.5.9 returned for the translated expression
(session time zone UTC); a difference means the rule must stay a flag or a
caveat.

    python3 scripts/probe_bigquery_semantics.py --project <project>

This is the only part of the plugin that runs a query, and it reads no table.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"

# (rule, GoogleSQL probe, Spark 3.5 result of the translated or nearest expression)
PROBES = [
    ("G04_COUNTIF", "SELECT COUNTIF(x) FROM UNNEST([TRUE, FALSE, NULL, TRUE]) AS x", "2"),
    ("G07_DATE_DIFF", "SELECT DATE_DIFF(DATE '2026-03-01', DATE '2026-02-01', DAY)", "28"),
    ("G03_SAFE_DIVIDE", "SELECT SAFE_DIVIDE(NUMERIC '1', NUMERIC '3')", "0.333333"),
    ("G03_SAFE_DIVIDE", "SELECT SAFE_DIVIDE(1, 0)", "NULL"),
    ("G06_TIMESTAMP_TRUNC", "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-03-01 03:00:00+00', DAY)", "2026-03-01 00:00:00"),
    ("G06_TIMESTAMP_TRUNC", "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-03-04 00:00:00+00', WEEK)", "2026-03-02 00:00:00 (Monday)"),
    ("G08_FORMAT_DATE", "SELECT FORMAT_DATE('%Y', DATE '0005-01-05')", "0005"),
    ("G08_FORMAT_DATE", "SELECT FORMAT_DATE('%Y-%m-%d', DATE '2026-01-05')", "2026-01-05"),
    ("G08_PARSE_DATE", "SELECT SAFE.PARSE_DATE('%Y-%m-%d', '2026-1-5')", "error: strict parser"),
    ("G05_GENERATE_ARRAY", "SELECT GENERATE_ARRAY(5, 1)", "[5, 4, 3, 2, 1]"),
    ("G09_ARRAY_LENGTH", "SELECT ARRAY_LENGTH(CAST(NULL AS ARRAY<INT64>))", "-1"),
    ("G02_SAFE_CAST", "SELECT SAFE_CAST(' 12 ' AS INT64)", "12"),
    ("G02_SAFE_CAST", "SELECT SAFE_CAST('1.5' AS INT64)", "NULL"),
    ("G02_SAFE_CAST", "SELECT SAFE_CAST('0x1A' AS INT64)", "NULL"),
    ("G02_SAFE_CAST", "SELECT SAFE_CAST('yes' AS BOOL)", "true"),
    ("G02_SAFE_CAST", "SELECT SAFE_CAST('1' AS BOOL)", "true"),
    ("G22_SAME_NAME", "SELECT SPLIT('a.b', '.')", "['', '', '', '']"),
]


def _value(cell):
    v = cell.get("v") if isinstance(cell, dict) else cell
    if isinstance(v, list):
        return [_value(x) for x in v]
    return "NULL" if v is None else v


def main() -> int:
    ap = argparse.ArgumentParser(description="Zero-byte BigQuery semantic probes")
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", default="US")
    args = ap.parse_args()
    # Same .env handling as the gcp-aidp CLI: run from the plugin directory.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from gcp_aidp._env import load_dotenv
    load_dotenv()
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
    except ImportError:
        print("error: pip install -e '.[gcp]' first", file=sys.stderr)
        return 2
    try:
        credentials, _ = google.auth.default(scopes=[BIGQUERY_SCOPE])
    except Exception as exc:  # noqa: BLE001 - reported without echoing any credential
        print(f"error: cannot authenticate to Google Cloud: {type(exc).__name__}. Set "
              "GOOGLE_APPLICATION_CREDENTIALS in .env (run from the plugin directory) or in your shell.",
              file=sys.stderr)
        return 2
    session = AuthorizedSession(credentials)
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{args.project}/queries"

    print(f"{'rule':<22} {'BigQuery':<32} {'Spark 3.5':<30} probe")
    for rule, sql, spark in PROBES:
        body = {"query": sql, "useLegacySql": False, "location": args.location}
        dry = session.post(url, json={**body, "dryRun": True}, timeout=60).json()
        if "error" in dry:
            print(f"{rule:<22} {'ERROR: ' + dry['error'].get('message', '')[:60]:<32} {spark:<30} {sql}")
            continue
        if str(dry.get("totalBytesProcessed", "?")) != "0":
            print(f"refused: {sql!r} would process {dry.get('totalBytesProcessed')} bytes", file=sys.stderr)
            return 1
        result = session.post(url, json={**body, "formatOptions": {"useInt64Timestamp": True}}, timeout=60).json()
        if "error" in result:
            answer = "ERROR: " + result["error"].get("message", "")[:60]
        elif not result.get("rows"):
            answer = "(no rows)"
        else:
            value = _value(result["rows"][0]["f"][0])
            if result["schema"]["fields"][0]["type"] == "TIMESTAMP" and value != "NULL":
                # REST returns TIMESTAMP as epoch microseconds with useInt64Timestamp.
                stamp = datetime.fromtimestamp(int(value) / 1e6, tz=timezone.utc)
                value = stamp.strftime("%Y-%m-%d %H:%M:%S (%A)")
            answer = json.dumps(value)
        print(f"{rule:<22} {answer:<32} {spark:<30} {sql}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
