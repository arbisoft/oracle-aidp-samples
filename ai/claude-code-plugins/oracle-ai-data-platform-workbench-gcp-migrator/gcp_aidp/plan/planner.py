"""Read a manifest, emit a migration plan: one row per source asset.

Every row carries an `action`:

  MIGRATE  -- migrated in this version; `transform_chain` says how
  REPORT   -- inventoried and reported (with an effort band), never translated
  SKIP     -- planned for a later version; `reason` says which

Every source the inventory knows about gets a row, so the plan never
understates the size of the estate. Two assets that would land on one target
name halt the plan: the planner does not pick a winner.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from collections import Counter
from pathlib import Path

from gcp_aidp._atomic import write_text_atomic
from gcp_aidp.inventory.manifest import ALL_SOURCES
from gcp_aidp.translate.ddl import table_layout
from gcp_aidp.translate.schedules import from_bigquery, from_unix_cron
from gcp_aidp.translate.types import MODES, OPTIONS, map_column

OCI_NAMESPACE_DEFAULT = "<your-oci-namespace>"
MIGRATE, REPORT, SKIP = "MIGRATE", "REPORT", "SKIP"

# The manifest contract the planner consumes: per source, per collection, the
# fields every row must carry. An unknown source or collection fails closed --
# silently ignoring it would drop assets from the plan.
_REQUIRED = {
    "bigquery": {
        "datasets": ("name",),
        "tables": ("dataset", "name"),
        "views": ("dataset", "name", "query"),
        "materialized_views": ("dataset", "name", "query"),
        "external_tables": ("dataset", "name"),
        "routines": ("dataset", "name", "routine_type", "language"),
        "models": ("dataset", "name"),
        "saved_queries": ("name", "query"),
        "scheduled_queries": ("id", "name", "query"),
        "access_policies": ("kind", "dataset"),
        "notebooks": ("id", "name"),
        "pipelines": ("id", "name"),
    },
    "gcs": {"buckets": ("name",)},
    "dataproc": {"clusters": ("name",), "jobs": ("id",), "batches": ("id",), "workflow_templates": ("id",),
                 "schedules": ("name", "schedule")},
    "composer": {"environments": ("name",), "dags": ("environment", "dag_id")},
    "dataform": {"repositories": ("name",)},
    "dataflow": {"jobs": ("id", "name")},
    "vertex": {"models": ("id", "name"), "endpoints": ("id", "name"), "pipelines": ("id", "name")},
}
assert set(_REQUIRED) == set(ALL_SOURCES)

_SKIP_REASONS = {
    "0.3": "planned for 0.3; inventoried only so far",
    "later": "not on the 0.x roadmap; inventoried only",
}


def _validated_namespace(namespace: str) -> str:
    if not isinstance(namespace, str) or not namespace:
        raise ValueError("OCI namespace must be a non-empty string")
    if namespace != OCI_NAMESPACE_DEFAULT and not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,254}", namespace):
        raise ValueError("OCI namespace must contain only lowercase letters, digits, underscores, or hyphens")
    return namespace


def _validated_catalog(catalog: str) -> str:
    if not isinstance(catalog, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", catalog):
        raise ValueError(f"catalog name {catalog!r} must be a letter or underscore followed by letters, digits or underscores")
    return catalog


def default_catalog(project_id: str) -> str:
    """`northwind-analytics-demo` → `northwind_analytics_demo`."""
    name = re.sub(r"[^a-z0-9_]", "_", str(project_id or "").lower())
    return name if re.match(r"[a-z_]", name) else f"p_{name}"


def job_name(name: str) -> str:
    """An AIDP job name: a letter followed by letters, digits or underscores."""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    return slug if re.match(r"[A-Za-z]", slug) else f"q_{slug}"


def _validated_items(manifest: dict) -> tuple[dict[str, dict[str, list[dict]]], dict[str, str]]:
    """(items per source per collection, scan error per source)."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a JSON object")
    sources = manifest.get("sources")
    if not isinstance(sources, dict):
        raise ValueError("manifest field 'sources' must be a JSON object")
    unknown = sorted(set(sources) - set(ALL_SOURCES))
    if unknown:
        raise ValueError("unsupported manifest source(s): " + ", ".join(unknown))

    out: dict[str, dict[str, list[dict]]] = {}
    errors: dict[str, str] = {}
    for source, data in sources.items():
        if not isinstance(data, dict):
            raise ValueError(f"manifest source {source!r} must be a JSON object")
        summary = data.get("summary", {})
        if isinstance(summary, dict):
            if summary.get("error"):
                errors[source] = str(summary["error"])
            for what, why in (summary.get("not_scanned") or {}).items():
                errors[f"{source}.{what}"] = f"not scanned: {why}"
            for i, warning in enumerate(summary.get("warnings") or []):
                errors[f"{source}.warning{i + 1}"] = str(warning)
        items = data.get("items", {})
        if not isinstance(items, dict):
            raise ValueError(f"manifest source {source!r}.items must be a JSON object")
        extra = sorted(set(items) - set(_REQUIRED[source]))
        if extra:
            raise ValueError(f"unsupported collection(s) in {source}.items: " + ", ".join(extra))
        out[source] = {}
        for collection, fields in _REQUIRED[source].items():
            rows = items.get(collection, [])
            context = f"{source}.items.{collection}"
            if not isinstance(rows, list):
                raise ValueError(f"manifest {context} must be a JSON array")
            for i, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise ValueError(f"manifest {context}[{i}] must be a JSON object")
                for field in fields:
                    if not isinstance(row.get(field), str) or not row[field].strip():
                        raise ValueError(f"manifest {context}[{i}].{field} must be a non-empty string")
            out[source][collection] = rows
    return out, errors


def effort_band(text: str | None) -> str:
    """S / M / L for an asset that is reported, not translated."""
    # ponytail: line count is a naive proxy; replace with a construct count when one exists.
    lines = len((text or "").strip().splitlines())
    return "S" if lines <= 10 else "M" if lines <= 50 else "L"


def _row(id_, kind, version, action, source, target, *, chain=(), reason="", notes=(), effort=None):
    row = {"id": id_, "kind": kind, "version": version, "action": action,
           "source": source, "target": target, "transform_chain": list(chain)}
    if reason:
        row["reason"] = reason
    if notes:
        row["notes"] = list(notes)
    if effort:
        row["effort"] = effort
    return row


def _skip(id_, kind, version, source, target_type, name):
    return _row(id_, kind, version, SKIP, source, {"type": target_type, "name": name},
                reason=_SKIP_REASONS[version])


def ingestion_columns(t: dict) -> list[dict]:
    """An ingestion-time partitioned table's pseudo-columns, carried as columns of the same names:
    _PARTITIONTIME (load time) always, _PARTITIONDATE for DAY partitions. BigQuery reserves the
    names, so no real column can clash; the connector and EXPORT DATA both deliver them."""
    part = t.get("partitioning") or {}
    if not part or part.get("field"):
        return []
    cols = [{"name": "_PARTITIONTIME", "type": "TIMESTAMP", "mode": "NULLABLE",
             "description": "BigQuery ingestion time (pseudo-column)"}]
    if part.get("type", "DAY") == "DAY":
        cols.append({"name": "_PARTITIONDATE", "type": "DATE", "mode": "NULLABLE",
                     "description": "BigQuery ingestion date (pseudo-column)"})
    return cols


def _table_target(t: dict, base: dict, mapping: dict) -> tuple[dict, list[str]]:
    """The planned table: every column with its type rules, the layout, and plan notes."""
    columns = [map_column(c, **mapping) for c in t.get("columns", []) + ingestion_columns(t)]
    layout = table_layout(t, columns)
    target = {"type": "aidp_delta_table", **base, "columns": columns,
              "partitioned_by": layout["partitioned_by"], "cluster_by": layout["cluster_by"],
              "layout_findings": [list(f) for f in layout["findings"]]}
    if t.get("description"):
        target["comment"] = t["description"]
    notes = [f"{sev.upper()} {rule}: {detail}" for rule, sev, detail in layout["findings"] if sev != "rewrite"]
    for c in columns:
        if c["severity"] != "map":
            notes.append(f"{c['severity'].upper()} column {c['name']} ({', '.join(c['rules'])}): "
                         + "; ".join(c["details"]))
    if not columns:
        notes.append("BLOCK: no columns in the manifest")
    return target, notes


def _schedule_note(expr, quartz, why, tz) -> str:
    if quartz:
        return f"schedule {expr!r} → Quartz {quartz!r} ({tz}), created PAUSED" + (f"; {why}" if why else "")
    return f"schedule {expr!r} not carried ({why or 'none'}): the job is created unscheduled"


# Types the export cannot write as Parquet that Spark reads, each exported as text instead
# (EXPORT DATA ... SELECT * REPLACE); the copy notebook casts the text to the planned type.
# BigQuery refuses JSON in a Parquet export ("Type JSON is not currently supported for
# parquet exports", measured); Spark cannot read a Parquet TIME column or a DECIMAL(76,38).
# TIME gets the connector path's text form, HH:MM:SS.ffffff.
_AS_TEXT = {"JSON": "TO_JSON_STRING({c})", "TIME": "FORMAT_TIME('%H:%M:%E6S', {c})",
            "BIGNUMERIC": "CAST({c} AS STRING)", "BIGDECIMAL": "CAST({c} AS STRING)",
            "GEOGRAPHY": "ST_ASTEXT({c})", "INTERVAL": "CAST({c} AS STRING)",
            # not text: a RANGE leaves as its two bounds, the planned STRUCT<start, end>
            "RANGE": "IF({c} IS NULL, NULL, STRUCT(RANGE_START({c}) AS `start`, RANGE_END({c}) AS `end`))"}


def _export_route(t: dict, export: dict | None) -> tuple[dict | None, list[str]]:
    """Large tables go BigQuery → Parquet on GCS → OCI → loaded into Delta, not through the connector."""
    size = t.get("num_bytes")
    if not export or size is None or size < export["over_bytes"]:
        return None, []
    gb = f"{size / 1e9:.1f} GB"
    as_text = {c["name"]: e for c in t.get("columns", []) if (e := _text_expr(c, _q(c["name"])))}
    rel = f"{export['prefix']}/{t['dataset']}/{t['name']}/"
    route = {"gcs": f"gs://{export['bucket']}/{rel}", "oci": f"oci://{export['oci_bucket']}@{export['namespace']}/{rel}"}
    how = "Parquet"
    pseudo = [c["name"] for c in ingestion_columns(t)]
    if pseudo:  # an extract job writes the table's columns only
        route["pseudo_columns"] = pseudo
    if as_text:
        route["as_text"] = as_text
    if as_text or pseudo:
        how = ("Parquet by EXPORT DATA (a billed query; "
               + "; ".join(x for x in (f"{', '.join(as_text)} as text" if as_text else "",
                                       f"with {', '.join(pseudo)}" if pseudo else "") if x) + ")")
    return route, [f"export route ({gb}): {how} to {route['gcs']}, transferred to {route['oci']}, loaded into Delta"]


def _q(name: str) -> str:
    return "`" + name.replace("`", "\\`") + "`"


def _text_expr(c: dict, ref: str, depth: int = 0) -> str | None:
    """The GoogleSQL expression that exports column `c` (at `ref`) with its _AS_TEXT types as
    text, at any depth of STRUCT and ARRAY; None when nothing in it needs converting. Struct
    fields keep their order, which the notebook's positional CAST to the planned type relies on."""
    if str(c.get("mode", "")).upper() == "REPEATED":
        e, o = f"_gcp_aidp_e{depth}", f"_gcp_aidp_o{depth}"
        inner = _text_expr({**c, "mode": "NULLABLE"}, e, depth + 1)
        return inner and f"ARRAY(SELECT {inner} FROM UNNEST({ref}) AS {e} WITH OFFSET AS {o} ORDER BY {o})"
    kind = str(c.get("type", "")).upper()
    if kind in _AS_TEXT:
        return _AS_TEXT[kind].format(c=ref)
    fields = [(f, _text_expr(f, f"{ref}.{_q(f['name'])}", depth)) for f in c.get("fields", [])]
    if not any(e for _, e in fields):
        return None
    body = ", ".join(f"{e or ref + '.' + _q(f['name'])} AS {_q(f['name'])}" for f, e in fields)
    return f"IF({ref} IS NULL, NULL, STRUCT({body}))"  # STRUCT(NULL.a, ...) would be a non-null struct


def _bigquery(it: dict[str, list[dict]], catalog: str, mapping: dict, export: dict | None = None) -> list[dict]:
    out = []

    def rel(ds, name):
        return {"catalog": catalog, "schema": ds, "name": name}

    for d in it["datasets"]:
        out.append(_row(f"bigquery.dataset.{d['name']}", "setup", "0.1", MIGRATE,
                        {"type": "bq_dataset", "name": d["name"], "location": d.get("location"),
                         "description": d.get("description", "")},
                        {"type": "aidp_schema", "catalog": catalog, "name": d["name"]},
                        chain=["create_schema"]))
    for t in it["tables"]:
        target, notes = _table_target(t, rel(t["dataset"], t["name"]), mapping)
        route, route_notes = _export_route(t, export)
        chain = ["map_types", "create_delta_table", "copy_dataset_job"]
        if route:
            target["load_from"] = route
            chain[2:] = ["export_parquet_to_gcs", "transfer_to_oci", "load_dataset_job"]
        out.append(_row(f"bigquery.table.{t['dataset']}.{t['name']}", "data", "0.1", MIGRATE,
                        {"type": "bq_table", "dataset": t["dataset"], "name": t["name"],
                         "num_rows": t.get("num_rows"), "num_bytes": t.get("num_bytes"),
                         "partitioning": t.get("partitioning"), "clustering": t.get("clustering"),
                         "columns": t.get("columns", [])},
                        target, chain=chain, notes=notes + route_notes))
    for v in it["views"]:
        out.append(_row(f"bigquery.view.{v['dataset']}.{v['name']}", "code", "0.1", MIGRATE,
                        {"type": "bq_view", "dataset": v["dataset"], "name": v["name"], "query": v["query"],
                         "legacy_sql": bool(v.get("legacy_sql"))},
                        {"type": "aidp_view", **rel(v["dataset"], v["name"])},
                        chain=["googlesql_to_spark", "create_view"]))
    for m in it["materialized_views"]:
        out.append(_row(f"bigquery.materialized_view.{m['dataset']}.{m['name']}", "data+code", "0.1", MIGRATE,
                        {"type": "bq_materialized_view", "dataset": m["dataset"], "name": m["name"],
                         "query": m["query"], "refresh_interval_minutes": m.get("refresh_interval_minutes")},
                        {"type": "aidp_delta_table", **rel(m["dataset"], m["name"]),
                         "refresh_job": job_name(f"refresh_{m['dataset']}_{m['name']}")},
                        chain=["copy_snapshot", "googlesql_to_spark", "create_refresh_job"],
                        notes=["migrates as a table snapshot plus an unscheduled refresh job"]))
    for e in it["external_tables"]:
        out.append(_row(f"bigquery.external_table.{e['dataset']}.{e['name']}", "data+setup", "0.1", MIGRATE,
                        {"type": "bq_external_table", "dataset": e["dataset"], "name": e["name"],
                         "format": e.get("format"), "source_uris": e.get("source_uris", [])},
                        {"type": "aidp_external_table", **rel(e["dataset"], e["name"])},
                        chain=["gcs_to_oci_location", "create_external_table"]))
    for r in it["routines"]:
        rid = f"bigquery.routine.{r['dataset']}.{r['name']}"
        src = {"type": f"bq_{r['routine_type'].lower()}", "dataset": r["dataset"], "name": r["name"],
               "language": r["language"], "body": r.get("body", ""),
               "arguments": r.get("arguments", []), "return_type": r.get("return_type")}
        if r["routine_type"] == "SCALAR_FUNCTION" and r["language"] == "SQL":
            out.append(_row(rid, "code", "0.1", MIGRATE, src,
                            {"type": "aidp_function", **rel(r["dataset"], r["name"])},
                            chain=["googlesql_to_spark", "create_function"]))
        else:
            what = {"PROCEDURE": "stored procedure", "TABLE_VALUED_FUNCTION": "table function"}.get(
                r["routine_type"], f"{r['language'].lower()} function")
            out.append(_row(rid, "code", "0.1", REPORT, src, {"type": "report"},
                            reason=f"{what}: inventoried with an effort band, not translated",
                            effort=effort_band(r.get("body"))))
    for m in it["models"]:
        out.append(_row(f"bigquery.model.{m['dataset']}.{m['name']}", "code", "0.1", REPORT,
                        {"type": "bq_ml_model", "dataset": m["dataset"], "name": m["name"],
                         "model_type": m.get("model_type")},
                        {"type": "report"},
                        reason="BigQuery ML model: retrain on AIDP; inventoried, not translated",
                        effort="M"))
    for q in it["saved_queries"]:
        out.append(_row(f"bigquery.saved_query.{job_name(q['name'])}", "code", "0.1", MIGRATE,
                        {"type": "bq_saved_query", "name": q["name"], "query": q["query"]},
                        {"type": "spark_sql_file", "name": f"{job_name(q['name'])}.sql"},
                        chain=["googlesql_to_spark", "write_sql_file"]))
    for q in it["scheduled_queries"]:
        out.append(_row(f"bigquery.scheduled_query.{q['id']}", "code+setup", "0.1", MIGRATE,
                        {"type": "bq_scheduled_query", "id": q["id"], "name": q["name"],
                         "schedule": q.get("schedule"), "destination_dataset": q.get("destination_dataset"),
                         "next_run_time": q.get("next_run_time"), "disabled": q.get("disabled", False),
                         "query": q["query"]},
                        {"type": "aidp_job", "name": job_name(q["name"])},
                        chain=["googlesql_to_spark", "create_job"],
                        notes=[_schedule_note(q.get("schedule"), *from_bigquery(q.get("schedule"),
                                                                                q.get("next_run_time")), "UTC")]))
    for a in it["access_policies"]:
        detail = a.get("name") or a.get("column") or a.get("role") or "policy"
        out.append(_row(f"bigquery.access.{a['kind']}.{a['dataset']}.{a.get('table') or '-'}.{detail}",
                        "setup", "0.1", REPORT,
                        {"type": f"bq_{a['kind']}", **{k: v for k, v in a.items() if k != "kind"}},
                        {"type": "report"},
                        reason="access rules are reported, not carried: recreate them in AIDP"))
    for n in it["notebooks"]:
        src = {"type": "bq_notebook", "id": n["id"], "name": n["name"]}
        if not n.get("cells"):  # listed, not exported
            out.append(_row(f"bigquery.notebook.{n['id']}", "code", "0.2", SKIP, src,
                            {"type": "aidp_notebook", "name": n["name"]},
                            reason="no content in the inventory: export it as .ipynb and pass --notebooks-dir"))
            continue
        out.append(_row(f"bigquery.notebook.{n['id']}", "code", "0.2", MIGRATE, {**src, "cells": n["cells"]},
                        {"type": "aidp_notebook", "name": job_name(n["name"])},
                        chain=["bigquery_magic_to_spark_sql", "write_notebook"]))
    for p in it["pipelines"]:
        out.append(_skip(f"bigquery.pipeline.{p['id']}", "code", "0.3",
                         {"type": "bq_pipeline", "id": p["id"], "name": p["name"]}, "aidp_job", p["name"]))
    return out


# A GCS bucket name, as Google allows it. The name goes into API calls and file
# paths, so anything else in a manifest fails closed.
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]")


# An OCI bucket name, as OCI allows it.
_OCI_BUCKET = re.compile(r"[A-Za-z0-9._-]{1,256}")


def bucket_mapping(mapping: dict, namespace: str | None) -> tuple[dict[str, str], str | None]:
    """A `--bucket-mapping` file → ({gcs bucket: oci bucket}, namespace).

    `{"buckets": {"<gcs>": {"oci_bucket": "<oci>", "oci_namespace": "<ns>"}}, "default_namespace": "<ns>"}`.
    One namespace per plan: `--namespace`, else `default_namespace`; a bucket naming another fails.
    """
    buckets = mapping.get("buckets")
    if not isinstance(buckets, dict):
        raise ValueError('bucket mapping: "buckets" must be an object of {gcs bucket: {"oci_bucket": ...}}')
    ns = namespace or mapping.get("default_namespace")
    out = {}
    for gcs, entry in buckets.items():
        oci = entry.get("oci_bucket") if isinstance(entry, dict) else None
        if not _BUCKET.fullmatch(gcs) or not isinstance(oci, str) or not _OCI_BUCKET.fullmatch(oci):
            raise ValueError(f"bucket mapping: {gcs!r} needs a valid \"oci_bucket\" (got {oci!r})")
        if entry.get("oci_namespace") and entry["oci_namespace"] != ns:
            raise ValueError(f"bucket mapping: {gcs!r} names namespace {entry['oci_namespace']!r}, the plan "
                             f"uses {ns!r}; one namespace per plan")
        out[gcs] = oci
    return out, ns


def _gcs(it, ns, buckets: dict[str, str]):
    bad = [b["name"] for b in it["buckets"] if not _BUCKET.fullmatch(b["name"])]
    if bad:
        raise ValueError(f"not a Cloud Storage bucket name: {bad}")
    return [_row(f"gcs.bucket.{b['name']}", "data", "0.1", MIGRATE,
                 {"type": "gcs_bucket", "name": b["name"], "location": b.get("location"),
                  "storage_class": b.get("storage_class")},
                 {"type": "oci_bucket", "name": buckets.get(b["name"], b["name"]), "namespace": ns},
                 chain=["copy_gcs_to_oci"]) for b in it["buckets"]]


# 0.2 translates PySpark and Spark SQL. A JAR job needs a JAR_TASK, which no migration
# has run on AIDP yet; the other engines have no translation in 0.2.
_DATAPROC_MIGRATE = ("pyspark", "spark_sql")
_DATAPROC_REASONS = {"spark": "Spark JAR job: an AIDP JAR_TASK is not yet proven on AIDP; recreate it by hand "
                              "with the JAR copied to OCI"}


def _starts(schedule: dict, row: dict, kind: str) -> bool:
    """Does this Cloud Scheduler job start this definition?"""
    t = schedule.get("target") or {}
    if t.get("kind") != kind or t.get("region") != row.get("region"):
        return False
    if kind == "workflow_template":
        return t.get("id") == row["id"]
    return t.get("main_file") == row.get("main_file") and (kind == "batch" or t.get("cluster") == row.get("cluster"))


def _scheduled(row: dict, kind: str, schedules: list[dict], used: set[str]) -> tuple[dict, list[str]]:
    """The definition with the schedules that start it, and a plan note for each."""
    mine = [sc for sc in schedules if _starts(sc, row, kind)]
    used.update(sc["name"] for sc in mine)
    if not mine:
        return row, []
    if len(mine) > 1:
        return {**row, "schedules": mine}, [(f"{len(mine)} Cloud Scheduler jobs start it "
                                             f"({', '.join(sc['name'] for sc in mine)}); an AIDP job holds one "
                                             "schedule, so none is carried")]
    sc = mine[0]
    return {**row, "schedules": mine}, [f"Cloud Scheduler job {sc['name']} ({sc.get('state') or '?'}): "
                                        + _schedule_note(sc["schedule"], *from_unix_cron(sc["schedule"]),
                                                         sc.get("time_zone") or "UTC")]


def _dataproc_definition(row: dict, collection: str) -> dict:
    stype = {"jobs": "dataproc_job", "batches": "dataproc_batch"}[collection]
    id_ = f"dataproc.{_SINGULAR.get(collection, collection[:-1])}.{row['id']}"
    source = {**row, "type": stype}
    kind = row.get("job_type", "unknown")
    if kind not in _DATAPROC_MIGRATE:
        return _row(id_, "code+setup", "0.2", REPORT, source, {"type": "report"},
                    reason=_DATAPROC_REASONS.get(kind, f"{kind} job: not translated in 0.2"),
                    effort=effort_band(row.get("code")) if row.get("code") else "M")
    name = row["main_file"].rstrip("/").rsplit("/", 1)[-1] if row.get("main_file") else row["id"]
    notes = [f"runs {row['runs']} times; last run {row.get('last_run')} ({row.get('last_state')})"] if row.get("runs") else []
    if row.get("args_vary"):
        notes.append(f"arguments differ between runs (latest: {row.get('args')}): the job takes them as parameters")
    if row.get("code_not_read"):
        notes.append(f"code not read: {row['code_not_read']}")
    chain = ["pyspark_to_aidp" if kind == "pyspark" else "spark_sql_to_aidp", "create_job"]
    return _row(id_, "code+setup", "0.2", MIGRATE, source,
                {"type": "aidp_job", "name": job_name(f"{row.get('cluster') or 'serverless'}_{name}")},
                chain=chain, notes=notes)


def _cluster_sizing(c: dict) -> list[str]:
    """The source cluster's size, to size the AIDP cluster from (no AIDP shape is invented)."""
    if c.get("worker_count"):
        notes = [(f"{c['worker_count']} workers {c.get('machine_type', '?')}, master "
                  f"{c.get('master_machine_type', '?')}, image {c.get('image_version', '?')}")]
    else:  # a single-node cluster has no workers: the master runs everything
        notes = [f"single node: master {c.get('master_machine_type', '?')}, image {c.get('image_version', '?')}"]
    if c.get("secondary_worker_count"):
        notes.append(f"{c['secondary_worker_count']} secondary workers")
    if c.get("autoscaling_policy"):
        notes.append(f"autoscaling policy {c['autoscaling_policy'].rsplit('/', 1)[-1]}")
    if c.get("properties"):
        notes.append("Spark properties: " + ", ".join(f"{k}={v}" for k, v in sorted(c["properties"].items())))
    if c.get("init_actions"):
        notes.append(f"{len(c['init_actions'])} init action(s) to recreate as cluster libraries or setup")
    return notes


def _dataproc(it: dict[str, list[dict]]) -> list[dict]:
    """0.2: job definitions and templates migrate; clusters and other engines are reported."""
    out = [_row(f"dataproc.cluster.{c['name']}", "setup", "0.2", REPORT,
                {**c, "type": "dataproc_cluster"}, {"type": "aidp_spark_cluster", "name": c["name"]},
                reason="cluster: create an AIDP cluster sized from the notes", notes=_cluster_sizing(c), effort="S")
           for c in it["clusters"]]
    used: set[str] = set()
    for collection, kind in (("jobs", "job"), ("batches", "batch")):
        for r in it[collection]:
            r, notes = _scheduled(r, kind, it["schedules"], used)
            row = _dataproc_definition(r, collection)
            if notes:
                row.setdefault("notes", []).extend(notes)
            out.append(row)
    for t in it["workflow_templates"]:
        t, schedule_notes = _scheduled(t, "workflow_template", it["schedules"], used)
        kinds = sorted({s.get("job_type", "unknown") for s in t.get("steps", [])} - set(_DATAPROC_MIGRATE))
        notes = [f"{len(t.get('steps', []))} steps as tasks, prerequisites as dependsOn", *schedule_notes]
        if kinds:
            notes.append(f"steps of type {', '.join(kinds)} are not translated: the job is not created until they are")
        if t.get("managed_cluster"):
            notes += [f"managed cluster: {n}" for n in _cluster_sizing(t["managed_cluster"])]
        out.append(_row(f"dataproc.workflow_template.{t['id']}", "code+setup", "0.2", MIGRATE,
                        {**t, "type": "dataproc_workflow_template"},
                        {"type": "aidp_job", "name": job_name(f"template_{t['id']}")},
                        chain=["dataproc_template_to_aidp", "create_job"], notes=notes))
    for sc in it["schedules"]:  # a schedule that starts nothing in the plan is still part of the estate
        if sc["name"] in used:
            continue
        t = sc.get("target") or {}
        what = t.get("id") or t.get("main_file") or t.get("uri") or t.get("kind")
        out.append(_row(f"dataproc.schedule.{sc.get('region')}/{sc['name']}", "setup", "0.2", REPORT,
                        {**sc, "type": "cloud_scheduler_job"}, {"type": "report"},
                        reason=f"Cloud Scheduler job starts Dataproc {t.get('kind')} {what}, which is not in the "
                               f"plan; recreate its schedule ({sc['schedule']!r}, {sc.get('time_zone')}) by hand",
                        effort="S"))
    return out


_SINGULAR = {"batches": "batch", "repositories": "repository", "workflow_templates": "workflow_template"}


def _later(source: str, it: dict[str, list[dict]]) -> list[dict]:
    """Sources not migrated in this version: one SKIP row per asset."""
    spec = {  # collection: (source type, target type, version, kind, id field, name field)
        "composer": {"environments": ("composer_environment", "aidp_workflow", "0.3", "setup", "name", "name"),
                     "dags": ("composer_dag", "aidp_workflow", "0.3", "code+setup", "dag_id", "dag_id")},
        "dataform": {"repositories": ("dataform_repository", "aidp_job", "0.3", "code", "name", "name")},
        "dataflow": {"jobs": ("dataflow_job", "none", "later", "code", "id", "name")},
        "vertex": {"models": ("vertex_model", "none", "later", "code", "id", "name"),
                   "endpoints": ("vertex_endpoint", "none", "later", "code", "id", "name"),
                   "pipelines": ("vertex_pipeline", "none", "later", "code", "id", "name")},
    }[source]
    out = []
    for collection, (stype, ttype, version, kind, id_f, name_f) in spec.items():
        for row in it[collection]:
            prefix = f"{row['environment']}." if collection == "dags" else ""
            out.append(_skip(f"{source}.{_SINGULAR.get(collection, collection[:-1])}.{prefix}{row[id_f]}",
                             kind, version,
                             {**row, "type": stype}, ttype, row[name_f]))
    return out


def _collisions(assets: list[dict]) -> list[str]:
    """Targets two MIGRATE rows would both create, compared as Spark compares them."""
    seen: dict[tuple, str] = {}
    problems = []
    for a in assets:
        if a["action"] != MIGRATE:
            continue
        t = a["target"]
        keys: list[tuple]
        if t["type"] in ("aidp_delta_table", "aidp_view", "aidp_external_table"):
            keys = [("relation", t["catalog"], t["schema"].casefold(), t["name"].casefold())]
        elif t["type"] == "aidp_function":
            keys = [("function", t["catalog"], t["schema"].casefold(), t["name"].casefold())]
        elif t["type"] == "aidp_schema":
            keys = [("schema", t["catalog"], t["name"].casefold())]
        elif t["type"] == "oci_bucket":
            keys = [("bucket", t["namespace"], t["name"])]  # OCI bucket names are case-sensitive
        elif t["type"] == "spark_sql_file":
            keys = [("sql_file", t["name"].casefold())]
        elif t["type"] == "aidp_notebook":
            keys = [("notebook", t["name"].casefold())]
        elif t["type"] == "aidp_job":
            keys = [("job", t["name"].casefold())]
        else:
            keys = []
        if t.get("refresh_job"):
            keys.append(("job", t["refresh_job"].casefold()))
        for key in keys:
            if key in seen:
                problems.append(f"{seen[key]} and {a['id']} both map to {key[0]} {'.'.join(key[1:])}")
            else:
                seen[key] = a["id"]
    return problems


def _scope_to_datasets(assets: list[dict], items: dict, datasets: list[str]) -> list[str]:
    """--datasets: BigQuery assets of any other dataset become SKIP, so the plan
    still shows the whole estate. A name the inventory does not hold fails closed."""
    known = {d["name"] for d in items.get("bigquery", {}).get("datasets", [])}
    unknown = sorted(set(datasets) - known)
    if unknown:
        raise ValueError(f"--datasets not in the inventory: {', '.join(unknown)}; "
                         f"its datasets are: {', '.join(sorted(known)) or 'none'}")
    for a in assets:
        src = a["source"]
        ds = src["name"] if src["type"] == "bq_dataset" else src.get("dataset")
        if a["id"].startswith("bigquery.") and ds and ds not in datasets and a["action"] != SKIP:
            a.update(action=SKIP, transform_chain=[], reason=f"dataset {ds} is outside --datasets")
            a.pop("effort", None)
    return sorted(set(datasets))


def build_plan(manifest: dict, *, oci_namespace: str = OCI_NAMESPACE_DEFAULT,
               catalog: str | None = None, bignumeric: str = "block", geography: str = "block",
               interval: str = "block", range_: str = "block",
               datasets: list[str] | None = None, export_over_gb: float | None = None,
               export_bucket: str | None = None, buckets: dict[str, str] | None = None) -> dict:
    """`datasets`: migrate only these BigQuery datasets (None: every dataset).

    `export_over_gb` + `export_bucket`: tables of at least that size are exported
    to Parquet in that GCS bucket, transferred to the OCI bucket of the same
    name (or the one `buckets` maps it to), and loaded from there; smaller ones
    still go through the connector.

    `buckets`: GCS bucket → OCI bucket, from `--bucket-mapping`; an unmapped
    bucket keeps its name. Every gs:// → oci:// rewrite follows it.
    """
    buckets = buckets or {}
    if (export_over_gb is None) != (export_bucket is None):
        raise ValueError("--export-over-gb and --export-bucket go together")
    if export_bucket is not None and not _BUCKET.fullmatch(export_bucket):
        raise ValueError(f"not a Cloud Storage bucket name: {export_bucket!r}")
    if export_over_gb is not None and export_over_gb < 0:
        raise ValueError("--export-over-gb must be 0 or more")
    mapping = {"bignumeric": bignumeric, "geography": geography, "interval": interval, "range": range_}
    for mode, value in mapping.items():
        if value not in MODES[mode]:
            raise ValueError(f"{mode} must be {'|'.join(MODES[mode])}, not {value!r}")
    oci_namespace = _validated_namespace(oci_namespace)
    items, scan_errors = _validated_items(manifest)
    project = manifest.get("project_id") or ""
    catalog = _validated_catalog(catalog or default_catalog(project))
    plan_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + f"-{uuid.uuid4().hex[:12]}"
    export = None if export_bucket is None else {
        "bucket": export_bucket, "oci_bucket": buckets.get(export_bucket, export_bucket),
        "namespace": oci_namespace, "over_bytes": export_over_gb * 1e9,
        "prefix": f"gcp-aidp-export/{plan_id}"}

    assets: list[dict] = []
    for source in ALL_SOURCES:
        if source not in items:
            continue
        if source == "bigquery":
            assets += _bigquery(items[source], catalog, mapping, export)
        elif source == "gcs":
            assets += _gcs(items[source], oci_namespace, buckets)
        elif source == "dataproc":
            assets += _dataproc(items[source])
        else:
            assets += _later(source, items[source])

    if datasets is not None:
        datasets = _scope_to_datasets(assets, items, datasets)
    dupes = sorted(i for i, n in Counter(a["id"] for a in assets).items() if n > 1)
    if dupes:
        raise ValueError("duplicate asset id(s) in manifest: " + ", ".join(dupes))
    collisions = _collisions(assets)
    if collisions:
        raise ValueError("name collision(s); rename one side and re-run, the planner does not pick a winner:\n  "
                         + "\n  ".join(collisions))

    by_action = {MIGRATE: 0, REPORT: 0, SKIP: 0}
    for a in assets:
        by_action[a["action"]] += 1
    return {
        "plan_id": plan_id,
        "source_project": project,
        "source_scanned_at": manifest.get("scanned_at"),
        "sources_scanned": manifest.get("sources_scanned", sorted(items)),
        "scan_errors": scan_errors,
        "target": {"catalog": catalog, "catalog_type": "INTERNAL", "oci_namespace": oci_namespace},
        "scope": {"datasets": datasets,
                  "inventoried": sorted(d["name"] for d in items.get("bigquery", {}).get("datasets", []))},
        "type_modes": mapping,
        "export": export and {k: export[k] for k in ("bucket", "oci_bucket", "prefix")} | {"over_gb": export_over_gb},
        "summary": {"asset_count": len(assets), "by_action": by_action},
        "assets": assets,
    }


def write_plan(plan: dict, out: str | Path) -> Path:
    return write_text_atomic(out, json.dumps(plan, indent=2))


def _groups(plan: dict) -> list[tuple[tuple, int]]:
    """(action, source type, target type, version, reason) → count, in plan order."""
    counts: dict[tuple, int] = {}
    for a in plan["assets"]:
        key = (a["action"], a["source"]["type"], a["target"]["type"], a["version"], a.get("reason", ""))
        counts[key] = counts.get(key, 0) + 1
    order = {MIGRATE: 0, REPORT: 1, SKIP: 2}
    return sorted(counts.items(), key=lambda kv: order[kv[0][0]])


def _scope_line(plan: dict) -> str:
    scope = plan.get("scope") or {}
    total = len(scope.get("inventoried") or [])
    if scope.get("datasets") is None:
        return f"Datasets: all {total}"
    chosen = scope["datasets"]
    return f"Datasets: {', '.join(chosen)} ({len(chosen)} of {total}; the rest are SKIP)"


def decisions(plan: dict) -> list[str]:
    """What the plan blocks that a person must decide, one line per choice: the re-plan option
    and what it gives up, with every column it would carry; or what no option carries."""
    modes = plan.get("type_modes") or {}
    by_option: dict[str, list[str]] = {}
    by_hand = []
    for a in plan["assets"]:
        tgt = a["target"]
        if a["action"] != MIGRATE or "columns" not in tgt:
            continue
        table = f"{tgt['schema']}.{tgt['name']}"
        if not tgt["columns"]:
            by_hand.append(f"{table}: no columns in the manifest")
        for c in tgt["columns"]:
            if c["severity"] != "block":
                continue
            fixable = [r for r in c["rules"] if r in OPTIONS and modes.get(OPTIONS[r][0], "block") == "block"]
            for r in fixable:
                by_option.setdefault(r, []).append(f"{table}.{c['name']}")
            if not fixable:
                by_hand.append(f"{table}.{c['name']} ({', '.join(c['rules'])}): {'; '.join(c['details'])}")
    lines = [f"--{OPTIONS[r][0]} {OPTIONS[r][1]}: {OPTIONS[r][2]}. Blocked now: {', '.join(cols)}"
             for r, cols in by_option.items()]
    return lines + [f"no option, a person rewrites it: {x}" for x in by_hand]


def summarize_plan(plan: dict) -> str:
    s = plan["summary"]
    t = plan["target"]
    lines = [
        f"plan {plan['plan_id']}",
        f"  source project: {plan['source_project']}",
        f"  target catalog: {t['catalog']} ({t['catalog_type']})   OCI namespace: {t['oci_namespace']}",
        f"  {_scope_line(plan)}",
        f"  total assets:   {s['asset_count']}   "
        + "   ".join(f"{k}={v}" for k, v in s["by_action"].items()),
        "",
    ]
    for source, err in plan["scan_errors"].items():
        lines.append(f"  ! {source}: {err}" if "." in source else
                     f"  ! {source} scan failed, its assets are missing from this plan: {err}")
    for (action, stype, ttype, version, reason), n in _groups(plan):
        line = f"  {action:<7s} {n:4d}  {stype:<26s} → {ttype:<20s} {version:<5s}"
        lines.append(line + (f"  {reason}" if action != MIGRATE else ""))
    todo = decisions(plan)
    if todo:
        lines += ["", (f"  NEEDS A DECISION ({len(todo)}): these tables are not created until it is made "
                       "(plan.md, top):")] + [f"  - {d}" for d in todo]
    return "\n".join(lines)


def write_plan_markdown(plan: dict, out: str | Path) -> Path:
    """The approval document: what will be created, reported and skipped."""
    t = plan["target"]
    md = [
        f"# Migration plan `{plan['plan_id']}`",
        "",
        f"- Source project: `{plan['source_project']}` (scanned {plan['source_scanned_at']})",
        f"- Target catalog: `{t['catalog']}` ({t['catalog_type']}); OCI namespace `{t['oci_namespace']}`",
        f"- {_scope_line(plan)}",
        "- Counts: " + ", ".join(f"{k} {v}" for k, v in plan["summary"]["by_action"].items()),
        "",
        "Nothing is written to AIDP by `plan` or `migrate`. Only `publish --apply` writes.",
        "",
    ]
    for source, err in plan["scan_errors"].items():
        md.append(f"> **{source}**: {err}\n" if "." in source else
                  f"> **{source} scan failed**; its assets are missing from this plan: {err}\n")
    todo = decisions(plan)
    if todo:
        md += [f"## Needs a decision ({len(todo)})", "",
               ("These tables are not created as planned. Choose for each line, then run `plan` again with "
                "the options you pick; leaving one out keeps the column blocked."), ""]
        md += [f"- {d}" for d in todo] + [""]
    md += [("SQL that cannot be translated (views, queries, routines) is listed by `migrate`, in "
            "`report.md` under *Blocked*."), ""]
    for action, title in ((MIGRATE, "To migrate"), (REPORT, "Reported, not translated"),
                          (SKIP, "Skipped in this version")):
        rows = [a for a in plan["assets"] if a["action"] == action]
        if not rows:
            continue
        md += [f"## {title} ({len(rows)})", "",
               "| Asset | Source type | Target | Version | Notes |", "|---|---|---|---|---|"]
        for a in rows:
            tgt = a["target"]
            name = ".".join(str(tgt[k]) for k in ("catalog", "schema", "name") if tgt.get(k))
            target = f"{tgt['type']} `{name}`" if name else tgt["type"]
            notes = "; ".join(a.get("notes", []) + ([a["reason"]] if a.get("reason") else []))
            if a.get("effort"):
                notes = f"effort {a['effort']}; {notes}"
            md.append(f"| `{a['id']}` | {a['source']['type']} | {target} | {a['version']} | "
                      f"{notes.replace('|', '/')} |")
        md.append("")
    md += ["## Approval", "", "Approved by: ____________________   Date: ____________", ""]
    return write_text_atomic(out, "\n".join(md))
