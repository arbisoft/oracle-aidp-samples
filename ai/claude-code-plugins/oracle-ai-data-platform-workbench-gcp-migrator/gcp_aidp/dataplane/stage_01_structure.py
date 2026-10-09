"""01 structure: create the schemas and empty Delta tables, read each back, then the views.

Every statement is CREATE ... IF NOT EXISTS: nothing is dropped or replaced.
External tables are created only with `external-tables` = True, once the
transfer job has copied their files to OCI Object Storage: Spark reads the
files to infer their schema.
`IF NOT EXISTS` does nothing on a table that is already there, so each table is
read back and compared with the plan, column by column and in order: a stale
layout is `type_drift`, never certified as created. Blocked tables are not
created; reconcile lists them.

Statuses: created, already_existed, type_drift, failed, dry_run (tables);
created, already_existed, failed, dry_run (external tables, views).
"""
from gcp_aidp.dataplane.common import *  # noqa: F403 - inlined above in the notebook


def _layout_drift(spark, t) -> list[str]:
    actual = target_columns(spark, quote(*t["target"]))
    planned = [(c["name"], c["target_type"]) for c in t["columns"]]
    drift = []
    for i in range(max(len(actual), len(planned))):
        a = actual[i] if i < len(actual) else ("<none>", "")
        p = planned[i] if i < len(planned) else ("<none>", "")
        if a[0].lower() != p[0].lower() or normalize_type(a[1]) != normalize_type(p[1]):
            drift.append(f"column {i + 1}: target {a[0]} {a[1]}, plan {p[0]} {p[1]}")
    return drift


def main(spark, params: dict, plan: dict) -> list[str]:
    datasets = set(as_list(params.get("datasets"))) or {s["name"] for s in plan["schemas"]}
    dry = params.get("dry-run")
    report = {"stage": "structure", "catalog": plan["catalog"], "started": now(), "dry_run": dry,
              "schemas": {}, "tables": {}, "external_tables": {}, "views": {}}

    for s in plan["schemas"]:
        if s["name"] not in datasets:
            continue
        try:
            if not dry:
                spark.sql(s["ddl"])
            report["schemas"][s["name"]] = "dry_run" if dry else "ok"
        except Exception as exc:  # noqa: BLE001
            report["schemas"][s["name"]] = f"failed: {error_text(exc, 200)}"

    for t in plan["tables"]:
        if t["dataset"] not in datasets:
            continue
        key = f"{t['dataset']}.{t['name']}"
        fqn = quote(*t["target"])
        try:
            existed = table_exists(spark, fqn)
            if dry:
                rec = {"status": "dry_run", "existed": existed}
            else:
                spark.sql(t["ddl"])
                drift = _layout_drift(spark, t)
                if drift:
                    rec = {"status": "type_drift", "drift": drift}
                else:
                    rec = {"status": "already_existed" if existed else "created"}
        except Exception as exc:  # noqa: BLE001
            rec = {"status": "failed", "reason": error_text(exc, 300)}
        report["tables"][key] = rec
        detail = rec.get("reason") or rec.get("drift") or ""
        print(f"  {rec['status']:<16} {key}" + (f"  {detail}" if detail else ""))

    for e in plan.get("external_tables", []) if params.get("external-tables") else []:
        if e["dataset"] not in datasets:
            continue
        key = f"{e['dataset']}.{e['name']}"
        try:
            existed = table_exists(spark, quote(*e["target"]))  # IF NOT EXISTS leaves an existing one as it is
            if not dry:
                spark.sql(e["ddl"])
            report["external_tables"][key] = {"status": "dry_run" if dry else "already_existed" if existed else "created"}
        except Exception as exc:  # noqa: BLE001
            report["external_tables"][key] = {"status": "failed", "reason": error_text(exc, 300)}
        print(f"  external {report['external_tables'][key]['status']:<15} {key}")

    for v in plan["views"]:  # after every table exists
        if v["dataset"] not in datasets:
            continue
        key = f"{v['dataset']}.{v['name']}"
        try:
            existed = table_exists(spark, quote(*v["target"]))  # IF NOT EXISTS leaves an existing one as it is
            if not dry:
                spark.sql(v["ddl"])
            report["views"][key] = {"status": "dry_run" if dry else "already_existed" if existed else "created"}
        except Exception as exc:  # noqa: BLE001
            report["views"][key] = {"status": "failed", "reason": error_text(exc, 300)}
        print(f"  view {report['views'][key]['status']:<15} {key}")

    report["finished"] = now()
    path = reports_dir(params) / "structure_report.json"
    previous = read_report(path, plan["catalog"]) or {}
    for section in ("schemas", "tables", "external_tables", "views"):  # a run over some datasets keeps the others' records
        report[section] = {**previous.get(section, {}), **report[section]}
    write_json(path, report)
    failed = [f"schema {k}: {r}" for k, r in report["schemas"].items() if str(r).startswith("failed")]
    failed += [f"table {k}: {r['status']}: {r.get('reason') or '; '.join(r.get('drift', []))}"
               for k, r in report["tables"].items() if r["status"] in ("failed", "type_drift")]
    failed += [f"external table {k}: {r.get('reason', '')}"
               for k, r in report["external_tables"].items() if r["status"] == "failed"]
    failed += [f"view {k}: {r.get('reason', '')}" for k, r in report["views"].items() if r["status"] == "failed"]
    print(f"\nstructure: {len(failed)} problem(s); report {path}")
    return failed
