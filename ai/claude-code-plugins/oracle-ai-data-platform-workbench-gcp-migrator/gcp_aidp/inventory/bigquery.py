"""BigQuery: datasets, tables, views, routines, models, access rules, scheduled queries.

Metadata calls only (`datasets.get`, `tables.get`, `routines.get`, ...): no
query runs and no bytes are billed. Row counts are the table metadata's
`numRows`.

Saved queries, BigQuery Studio notebooks and BigQuery pipelines have no read
API this tool relies on in 0.1. They are recorded as not scanned, so the plan
says so. Saved queries exported as .sql files can be read with
`--saved-queries-dir`.
"""
from __future__ import annotations

from pathlib import Path

from gcp_aidp.gcp_client import GcpClient, GcpError

BQ = "https://bigquery.googleapis.com/bigquery/v2"
DTS = "https://bigquerydatatransfer.googleapis.com/v1"
_MEMBER_KEYS = ("userByEmail", "groupByEmail", "domain", "specialGroup", "iamMember")


def _int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _policy_tags(dataset: str, table: str, fields: list[dict], prefix: str = "") -> list[dict]:
    out = []
    for f in fields:
        path = f"{prefix}{f['name']}"
        for tag in (f.get("policyTags") or {}).get("names", []):
            out.append({"kind": "column_policy_tag", "dataset": dataset, "table": table,
                        "column": path, "policy_tag": tag})
        out += _policy_tags(dataset, table, f.get("fields", []), f"{path}.")
    return out


def _dataset_iam(dataset: str, access: list[dict]) -> list[dict]:
    by_role: dict[str, list[str]] = {}
    for entry in access:
        member = next((f"{k}:{entry[k]}" for k in _MEMBER_KEYS if entry.get(k)), None)
        if member is None:
            for k in ("view", "routine", "dataset"):
                if entry.get(k):
                    member = f"{k}:{entry[k]}"
        by_role.setdefault(entry.get("role") or "authorized", []).append(member or str(entry))
    return [{"kind": "dataset_iam", "dataset": dataset, "role": role, "members": sorted(members)}
            for role, members in sorted(by_role.items())]


def _table(dataset: str, t: dict) -> tuple[str, dict]:
    """(collection, manifest row) for one `tables.get` response."""
    kind = t.get("type", "TABLE")
    name = t["tableReference"]["tableId"]
    base = {"dataset": dataset, "name": name, "description": t.get("description", ""),
            "labels": t.get("labels", {})}
    if kind == "VIEW":
        view = t.get("view", {})
        return "views", {**base, "query": view.get("query", ""), "legacy_sql": bool(view.get("useLegacySql"))}
    if kind == "MATERIALIZED_VIEW":
        mv = t.get("materializedView", {})
        interval = _int(mv.get("refreshIntervalMs"))
        return "materialized_views", {**base, "query": mv.get("query", ""),
                                      "refresh_interval_minutes": interval // 60000 if interval else None}
    if kind == "EXTERNAL":
        ext = t.get("externalDataConfiguration", {})
        return "external_tables", {**base, "format": ext.get("sourceFormat"),
                                   "source_uris": ext.get("sourceUris", []),
                                   "columns": t.get("schema", {}).get("fields", [])}
    # TABLE, SNAPSHOT, CLONE: stored data, read through the connector.
    row = {**base, "type": kind, "columns": t.get("schema", {}).get("fields", []),
           "num_rows": _int(t.get("numRows")), "num_bytes": _int(t.get("numBytes"))}
    if t.get("timePartitioning"):
        tp = t["timePartitioning"]
        row["partitioning"] = {"type": tp.get("type", "DAY"), "field": tp.get("field")}
    elif t.get("rangePartitioning"):
        rp = t["rangePartitioning"]
        row["partitioning"] = {"type": "RANGE", "field": rp.get("field"), "range": rp.get("range", {})}
    if t.get("clustering"):
        row["clustering"] = t["clustering"].get("fields", [])
    if t.get("expirationTime"):
        row["expiration_ms"] = _int(t["expirationTime"])
    return "tables", row


def _type_name(data_type: dict | None) -> str | None:
    if not data_type:
        return None
    kind = data_type.get("typeKind")
    if kind == "ARRAY":
        return f"ARRAY<{_type_name(data_type.get('arrayElementType'))}>"
    if kind == "STRUCT":
        fields = data_type.get("structType", {}).get("fields", [])
        return "STRUCT<" + ", ".join(f"{f.get('name')} {_type_name(f.get('type'))}" for f in fields) + ">"
    return kind


def _saved_queries(directory: str | None) -> list[dict]:
    if not directory:
        return []
    return [{"name": p.stem, "query": p.read_text(encoding="utf-8")}
            for p in sorted(Path(directory).glob("*.sql"))]


def _dataset(client: GcpClient, p: str, ds: str, items: dict, not_scanned: dict, warnings: list,
             row_policies: dict) -> None:
    """One dataset: its metadata, tables, routines and models, added to `items`."""
    meta = client.get(f"{BQ}/projects/{p}/datasets/{ds}")
    items["datasets"].append({"name": ds, "location": meta.get("location"),
                              "description": meta.get("description", ""), "labels": meta.get("labels", {})})
    items["access_policies"] += _dataset_iam(ds, meta.get("access", []))

    for tref in client.pages(f"{BQ}/projects/{p}/datasets/{ds}/tables", "tables"):
        name = tref["tableReference"]["tableId"]
        try:
            t = client.get(f"{BQ}/projects/{p}/datasets/{ds}/tables/{name}")
        except GcpError as exc:
            warnings.append(f"table {ds}.{name} not read: {exc}")
            continue
        collection, row = _table(ds, t)
        items[collection].append(row)
        items["access_policies"] += _policy_tags(ds, name, t.get("schema", {}).get("fields", []))
        if collection == "tables" and row_policies["ok"]:
            try:
                policies = client.pages(f"{BQ}/projects/{p}/datasets/{ds}/tables/{name}/rowAccessPolicies",
                                        "rowAccessPolicies")
            except GcpError as exc:
                row_policies["ok"] = False
                not_scanned["row_access_policies"] = f"rowAccessPolicies.list refused: {exc}"
                policies = []
            items["access_policies"] += [
                {"kind": "row_access_policy", "dataset": ds, "table": name,
                 "name": pol.get("rowAccessPolicyReference", {}).get("policyId", "?"),
                 "filter": pol.get("filterPredicate", "")} for pol in policies]

    for rref in client.pages(f"{BQ}/projects/{p}/datasets/{ds}/routines", "routines"):
        rid = rref["routineReference"]["routineId"]
        try:
            r = client.get(f"{BQ}/projects/{p}/datasets/{ds}/routines/{rid}")
        except GcpError as exc:
            warnings.append(f"routine {ds}.{rid} not read: {exc}")
            continue
        items["routines"].append({
            "dataset": ds, "name": rid, "routine_type": r.get("routineType", "ROUTINE_TYPE_UNSPECIFIED"),
            "language": r.get("language", "SQL"), "body": r.get("definitionBody", ""),
            "arguments": [{"name": a.get("name"), "type": _type_name(a.get("dataType"))}
                          for a in r.get("arguments", [])],
            "return_type": _type_name(r.get("returnType"))})

    for m in client.pages(f"{BQ}/projects/{p}/datasets/{ds}/models", "models"):
        items["models"].append({"dataset": ds, "name": m["modelReference"]["modelId"],
                                "model_type": m.get("modelType")})


def scan(client: GcpClient, *, saved_queries_dir: str | None = None, log=None) -> dict:
    p = client.project
    items: dict[str, list] = {k: [] for k in (
        "datasets", "tables", "views", "materialized_views", "external_tables", "routines", "models",
        "saved_queries", "scheduled_queries", "access_policies", "notebooks", "pipelines")}
    not_scanned: dict[str, str] = {
        "notebooks": "BigQuery Studio notebooks have no read API this tool uses in 0.1; list them by hand",
        "pipelines": "BigQuery pipelines have no read API this tool uses in 0.1; list them by hand",
    }
    warnings: list[str] = []
    if saved_queries_dir:
        items["saved_queries"] = _saved_queries(saved_queries_dir)
    else:
        not_scanned["saved_queries"] = ("saved queries have no read API this tool uses in 0.1; "
                                        "export them as .sql files and pass --saved-queries-dir")
    row_policies = {"ok": True}

    for ref in client.pages(f"{BQ}/projects/{p}/datasets", "datasets"):
        ds = ref["datasetReference"]["datasetId"]
        if log:
            log(f"    dataset {ds}")
        try:  # one dataset the account cannot read costs that dataset, not the whole scan
            _dataset(client, p, ds, items, not_scanned, warnings, row_policies)
        except GcpError as exc:
            not_scanned[f"dataset {ds}"] = f"not read: {exc}"
            if log:
                log(f"    dataset {ds}: not read: {exc}")
    # Scheduled queries live in the Data Transfer Service, per location.
    for location in sorted({str(d["location"]).lower() for d in items["datasets"] if d.get("location")}):
        try:
            configs = client.pages(f"{DTS}/projects/{p}/locations/{location}/transferConfigs",
                                   "transferConfigs", {"dataSourceIds": "scheduled_query"})
        except GcpError as exc:
            if exc.api_disabled:
                continue  # the API is off, so the project has no scheduled queries
            not_scanned["scheduled_queries"] = f"transferConfigs.list in {location} refused: {exc}"
            continue
        for c in configs:
            items["scheduled_queries"].append({
                "id": c["name"].rsplit("/", 1)[-1], "name": c.get("displayName") or c["name"],
                "query": (c.get("params") or {}).get("query", ""), "schedule": c.get("schedule"),
                "destination_dataset": c.get("destinationDatasetId"), "location": location})

    summary: dict = {k: len(v) for k, v in items.items() if k not in not_scanned}
    if not_scanned:
        summary["not_scanned"] = not_scanned
    if warnings:
        summary["warnings"] = warnings
    return {"summary": summary, "items": items}
