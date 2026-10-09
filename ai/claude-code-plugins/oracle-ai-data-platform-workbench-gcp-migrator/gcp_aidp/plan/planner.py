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
from gcp_aidp.translate.dataform import schedule_text
from gcp_aidp.translate.ddl import table_layout
from gcp_aidp.translate.types import map_column

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
    "dataproc": {"clusters": ("name",), "jobs": ("id",)},
    "composer": {"environments": ("name",), "dags": ("environment", "dag_id")},
    "dataform": {"repositories": ("name",)},
    "dataflow": {"jobs": ("id", "name")},
    "vertex": {"models": ("id", "name"), "endpoints": ("id", "name"), "pipelines": ("id", "name")},
}
assert set(_REQUIRED) == set(ALL_SOURCES)

_SKIP_REASONS = {
    "0.2": "planned for 0.2; inventoried only in 0.1",
    "0.3": "planned for 0.3; inventoried only in 0.1",
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


def _table_target(t: dict, base: dict, mapping: dict) -> tuple[dict, list[str]]:
    """The planned table: every column with its type rules, the layout, and plan notes."""
    columns = [map_column(c, **mapping) for c in t.get("columns", [])]
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


def _bigquery(it: dict[str, list[dict]], catalog: str, mapping: dict) -> list[dict]:
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
        out.append(_row(f"bigquery.table.{t['dataset']}.{t['name']}", "data", "0.1", MIGRATE,
                        {"type": "bq_table", "dataset": t["dataset"], "name": t["name"],
                         "num_rows": t.get("num_rows"), "num_bytes": t.get("num_bytes"),
                         "partitioning": t.get("partitioning"), "clustering": t.get("clustering"),
                         "columns": t.get("columns", [])},
                        target, chain=["map_types", "create_delta_table", "copy_dataset_job"], notes=notes))
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
                            reason=f"{what}: inventoried with an effort band, not translated in 0.1",
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
                         "query": q["query"]},
                        {"type": "aidp_job", "name": job_name(q["name"])},
                        chain=["googlesql_to_spark", "create_job_unscheduled"],
                        notes=[f"source schedule {q.get('schedule')!r} is not applied; the job is created unscheduled"]))
    for a in it["access_policies"]:
        detail = a.get("name") or a.get("column") or a.get("role") or "policy"
        out.append(_row(f"bigquery.access.{a['kind']}.{a['dataset']}.{a.get('table') or '-'}.{detail}",
                        "setup", "0.1", REPORT,
                        {"type": f"bq_{a['kind']}", **{k: v for k, v in a.items() if k != "kind"}},
                        {"type": "report"},
                        reason="access rules are reported, not carried: recreate them in AIDP"))
    for n in it["notebooks"]:
        out.append(_skip(f"bigquery.notebook.{n['id']}", "code", "0.2",
                         {"type": "bq_notebook", "id": n["id"], "name": n["name"]}, "aidp_notebook", n["name"]))
    for p in it["pipelines"]:
        out.append(_skip(f"bigquery.pipeline.{p['id']}", "code", "0.3",
                         {"type": "bq_pipeline", "id": p["id"], "name": p["name"]}, "aidp_job", p["name"]))
    return out


# A GCS bucket name, as Google allows it. The name is written into the rclone
# transfer job, a shell script, so anything else in a manifest fails closed.
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]")


def _gcs(it, ns):
    bad = [b["name"] for b in it["buckets"] if not _BUCKET.fullmatch(b["name"])]
    if bad:
        raise ValueError(f"not a Cloud Storage bucket name: {bad}")
    return [_row(f"gcs.bucket.{b['name']}", "data", "0.1", MIGRATE,
                 {"type": "gcs_bucket", "name": b["name"], "location": b.get("location"),
                  "storage_class": b.get("storage_class")},
                 {"type": "oci_bucket", "name": b["name"], "namespace": ns},
                 chain=["copy_gcs_to_oci"]) for b in it["buckets"]]


def _later(source: str, it: dict[str, list[dict]]) -> list[dict]:
    """Sources migrated after 0.1: one SKIP row per asset."""
    spec = {  # collection: (source type, target type, version, kind, id field, name field)
        "dataproc": {"clusters": ("dataproc_cluster", "aidp_spark_cluster", "0.2", "setup", "name", "name"),
                     "jobs": ("dataproc_job", "aidp_job", "0.2", "code+setup", "id", "id")},
        "dataflow": {"jobs": ("dataflow_job", "none", "later", "code", "id", "name")},
        "vertex": {"models": ("vertex_model", "none", "later", "code", "id", "name"),
                   "endpoints": ("vertex_endpoint", "none", "later", "code", "id", "name"),
                   "pipelines": ("vertex_pipeline", "none", "later", "code", "id", "name")},
    }[source]
    out = []
    for collection, (stype, ttype, version, kind, id_f, name_f) in spec.items():
        for row in it[collection]:
            out.append(_skip(f"{source}.{collection[:-1]}.{row[id_f]}", kind, version,
                             {**row, "type": stype}, ttype, row[name_f]))
    return out


def _dataform(it: dict[str, list[dict]]) -> list[dict]:
    """One job per repository; its schedules are recorded in the notes and never applied."""
    out = []
    for r in it["repositories"]:
        notes = [f"source schedule: {schedule_text(r)}; recorded in the job description, not applied: "
                 "the job is created unscheduled",
                 f"{len(r.get('actions') or [])} compiled action(s), one task each"]
        if r.get("actions_not_scanned"):
            notes.append(f"actions not scanned: {r['actions_not_scanned']}")
        out.append(_row(f"dataform.repository.{r['name']}", "code", "0.3", MIGRATE,
                        {**r, "type": "dataform_repository"},
                        {"type": "aidp_job", "name": job_name(f"dataform_{r['name']}")},
                        chain=["dataform_compiled_actions", "googlesql_to_spark", "create_job_unscheduled"],
                        notes=notes))
    return out


def _composer(it: dict[str, list[dict]]) -> list[dict]:
    """Environments are reported; each DAG file is one job, its schedule recorded and never applied."""
    out = []
    for e in it["environments"]:
        out.append(_row(f"composer.environment.{e['name']}", "setup", "0.3", REPORT,
                        {**e, "type": "composer_environment"}, {"type": "report"},
                        reason="Airflow itself is not migrated; its DAGs are", effort="S"))
    for d in it["dags"]:
        notes = ["the DAG file is read as text and never run; no job is created if anything in it is flagged",
                 "source schedule is recorded in the job description, not applied: the job is created unscheduled"]
        if d.get("code_not_scanned"):
            notes.append(f"file not scanned: {d['code_not_scanned']}")
        out.append(_row(f"composer.dag.{d['environment']}.{d['dag_id']}", "code+setup", "0.3", MIGRATE,
                        {**d, "type": "composer_dag"},
                        {"type": "aidp_job", "name": job_name(f"composer_{d['environment']}_{d['dag_id']}")},
                        chain=["parse_dag_ast", "googlesql_to_spark", "create_job_unscheduled"], notes=notes))
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


def _scope_to_dataform_repos(assets: list[dict], items: dict, repos: list[str]) -> list[str]:
    """--dataform-repos: the other repositories become SKIP. A name the inventory does not hold fails closed."""
    known = {r["name"] for r in items.get("dataform", {}).get("repositories", [])}
    unknown = sorted(set(repos) - known)
    if unknown:
        raise ValueError(f"--dataform-repos not in the inventory: {', '.join(unknown)}; "
                         f"its repositories are: {', '.join(sorted(known)) or 'none'}")
    for a in assets:
        src = a["source"]
        if src["type"] == "dataform_repository" and src["name"] not in repos and a["action"] != SKIP:
            a.update(action=SKIP, transform_chain=[], reason=f"repository {src['name']} is outside --dataform-repos")
    return sorted(set(repos))


def _scope_to_dags(assets: list[dict], items: dict, dags: list[str]) -> list[str]:
    """--dags: the other DAGs become SKIP. A name the inventory does not hold fails closed."""
    known = {d["dag_id"] for d in items.get("composer", {}).get("dags", [])}
    unknown = sorted(set(dags) - known)
    if unknown:
        raise ValueError(f"--dags not in the inventory: {', '.join(unknown)}; "
                         f"its DAGs are: {', '.join(sorted(known)) or 'none'}")
    for a in assets:
        src = a["source"]
        if src["type"] == "composer_dag" and src["dag_id"] not in dags and a["action"] != SKIP:
            a.update(action=SKIP, transform_chain=[], reason=f"DAG {src['dag_id']} is outside --dags")
    return sorted(set(dags))


def build_plan(manifest: dict, *, oci_namespace: str = OCI_NAMESPACE_DEFAULT,
               catalog: str | None = None, bignumeric: str = "block", geography: str = "block",
               datasets: list[str] | None = None, dataform_repos: list[str] | None = None,
               dags: list[str] | None = None) -> dict:
    """`datasets`: migrate only these BigQuery datasets (None: every dataset).
    `dataform_repos`: migrate only these Dataform repositories (None: every repository).
    `dags`: migrate only the Composer DAGs with these ids, in any environment (None: every DAG)."""
    if bignumeric not in ("block", "string") or geography not in ("block", "wkt"):
        raise ValueError("bignumeric must be block|string and geography block|wkt")
    mapping = {"bignumeric": bignumeric, "geography": geography}
    oci_namespace = _validated_namespace(oci_namespace)
    items, scan_errors = _validated_items(manifest)
    project = manifest.get("project_id") or ""
    catalog = _validated_catalog(catalog or default_catalog(project))

    assets: list[dict] = []
    for source in ALL_SOURCES:
        if source not in items:
            continue
        if source == "bigquery":
            assets += _bigquery(items[source], catalog, mapping)
        elif source == "gcs":
            assets += _gcs(items[source], oci_namespace)
        elif source == "dataform":
            assets += _dataform(items[source])
        elif source == "composer":
            assets += _composer(items[source])
        else:
            assets += _later(source, items[source])

    if datasets is not None:
        datasets = _scope_to_datasets(assets, items, datasets)
    if dataform_repos is not None:
        dataform_repos = _scope_to_dataform_repos(assets, items, dataform_repos)
    if dags is not None:
        dags = _scope_to_dags(assets, items, dags)
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
        "plan_id": time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + f"-{uuid.uuid4().hex[:12]}",
        "source_project": project,
        "source_scanned_at": manifest.get("scanned_at"),
        "sources_scanned": manifest.get("sources_scanned", sorted(items)),
        "scan_errors": scan_errors,
        "target": {"catalog": catalog, "catalog_type": "INTERNAL", "oci_namespace": oci_namespace},
        "scope": {"datasets": datasets,
                  "inventoried": sorted(d["name"] for d in items.get("bigquery", {}).get("datasets", [])),
                  "dataform_repos": dataform_repos,
                  "dataform_inventoried": sorted(r["name"] for r in items.get("dataform", {}).get("repositories", [])),
                  "dags": dags,
                  "dags_inventoried": sorted({d["dag_id"] for d in items.get("composer", {}).get("dags", [])})},
        "type_modes": mapping,
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
        line = f"Datasets: all {total}"
    else:
        chosen = scope["datasets"]
        line = f"Datasets: {', '.join(chosen)} ({len(chosen)} of {total}; the rest are SKIP)"
    repos = scope.get("dataform_inventoried") or []
    if scope.get("dataform_repos") is not None:
        chosen = scope["dataform_repos"]
        line += f"; Dataform repositories: {', '.join(chosen) or 'none'} ({len(chosen)} of {len(repos)}; the rest are SKIP)"
    elif repos:
        line += f"; Dataform repositories: all {len(repos)}"
    dags = scope.get("dags_inventoried") or []
    if scope.get("dags") is not None:
        chosen = scope["dags"]
        line += f"; Composer DAGs: {', '.join(chosen) or 'none'} ({len(chosen)} of {len(dags)}; the rest are SKIP)"
    elif dags:
        line += f"; Composer DAGs: all {len(dags)}"
    return line


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
