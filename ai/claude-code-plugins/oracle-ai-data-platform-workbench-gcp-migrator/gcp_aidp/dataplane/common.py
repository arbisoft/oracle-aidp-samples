"""Shared helpers for the data-plane notebooks. Runs INSIDE AIDP.

Inlined into every generated notebook, so a
notebook needs nothing else uploaded beside it. `spark`, `oidlUtils` and
`aidputils` are globals the AIDP runtime provides; nothing here imports them.

Read path (verified by hand on AIDP, Spark 3.5): the open-source Spark
BigQuery connector, installed on the cluster as a JAR, reading TABLES only.
`viewsEnabled` stays false: reading a view or a query result makes the
connector write a temporary table in BigQuery.
"""
from __future__ import annotations

import base64
import json
import pathlib
import re
import time

DEFAULT_REPORTS_DIR = "/Workspace/gcp-aidp-migration/reports"
_MISS = "\x00__no_parameter__"


def resolve_params(params: dict) -> dict:
    """A job task's parameters override the PARAMS literals, by the same names."""
    out = dict(params)
    for name in params:
        for spelling in dict.fromkeys((name, name.replace("-", "_"))):
            try:
                value = oidlUtils.parameters.getParameter(spelling, _MISS)  # noqa: F821
            except NameError:  # outside AIDP: no oidlUtils
                value = _MISS
            if value != _MISS and value is not None and str(value).strip() != "":
                out[name] = str(value).strip()
                break
    for name, value in out.items():  # switches arrive from a job as text
        if isinstance(params.get(name), bool) and isinstance(value, str):
            if value.lower() not in ("true", "false"):
                raise ValueError(f"parameter {name}={value!r}: give true or false")
            out[name] = value.lower() == "true"
    return out


def as_list(value) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return [v.strip() for v in str(value).split(",") if v.strip()]


def gcp_credentials(params: dict) -> str:
    """The service account key, base64-encoded, as the connector's `credentials` option takes it.

    From the AIDP credential store by default; a workspace key file only if
    `key-file` is set. The value is checked and never printed.
    """
    if params.get("key-file"):
        raw = pathlib.Path(params["key-file"]).read_bytes()
        value = base64.b64encode(raw).decode()
        source = "key file"
    else:
        value = aidputils.secrets.get(name=params["credential-name"], key=params["credential-key"])  # noqa: F821
        value = (value or "").strip()
        if value.startswith("{"):  # stored as raw JSON rather than base64
            value = base64.b64encode(value.encode()).decode()
        source = f"credential store entry {params['credential-name']}/{params['credential-key']}"
    try:
        key = json.loads(base64.b64decode(value, validate=True))
    except ValueError as exc:
        raise ValueError(f"{source}: not a base64-encoded JSON key (length {len(value)})") from exc
    missing = [f for f in ("type", "client_email", "private_key") if not key.get(f)]
    if key.get("type") != "service_account" or missing:
        raise ValueError(f"{source}: not a complete service account key (missing {missing or 'type'})")
    print(f"credentials: {source}, service account {key['client_email'].split('@')[0]}@…, "
          f"{len(value)} base64 characters")
    return value


def bigquery_reader(spark, credentials: str, project: str):
    """read(table_fqn) -> DataFrame over a BigQuery TABLE (never a view or a query)."""
    def read(fqn: str):
        return (spark.read.format("bigquery")
                .option("credentials", credentials)
                .option("parentProject", project)
                .option("viewsEnabled", "false")
                .load(fqn))
    return read


def quote(*parts: str) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def normalize_type(text: str) -> str:
    """`STRUCT<`a`: BIGINT>` and `struct<a:bigint>` compare equal."""
    return re.sub(r"[`\s]", "", str(text)).lower()


def reports_dir(params: dict) -> pathlib.Path:
    d = pathlib.Path(params.get("reports-dir") or DEFAULT_REPORTS_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d


def read_json(path: pathlib.Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def read_report(path: pathlib.Path, catalog: str) -> dict | None:
    """A stage's earlier report, only if it is for this catalog.

    A reports folder can outlive a plan. Merging a report written for another
    catalog would carry its failures (or its successes) into this migration.
    A report that does not name its catalog predates the rule and is dropped.
    """
    report = read_json(path)
    return report if report and report.get("catalog") == catalog else None


def write_json(path: pathlib.Path, value: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, default=str))
    tmp.replace(path)


def error_text(exc: BaseException, limit: int = 300) -> str:
    """The useful line of an error. A Py4J error's first line is only
    'An error occurred while calling o123.insertInto.'; the Spark error
    class and message are on a later line."""
    lines = [line.strip() for line in str(exc).strip().splitlines() if line.strip()]
    causes = [line[len("Caused by:"):].strip() for line in lines if line.startswith("Caused by:")]
    if causes:  # the root cause is the last one; [TASK_WRITE_FAILED] and the like only wrap it
        return causes[-1][:limit]
    useful = [line for line in lines if not line.startswith(("An error occurred while calling", "at "))]
    best = next((line for line in useful if re.search(r"\[[A-Z_.]+\]|Exception: ", line)), None)
    return (best or (useful or lines or [type(exc).__name__])[0]).lstrip(": ")[:limit]


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def table_exists(spark, fqn: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return True
    except Exception as exc:  # noqa: BLE001
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "not found" in str(exc).lower():
            return False
        raise  # "could not look" is never recorded as "absent"


def target_columns(spark, fqn: str) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in spark.table(fqn).schema.fields]
