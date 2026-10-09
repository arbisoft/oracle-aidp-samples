"""02 copy: copy ONE dataset's tables from BigQuery into Delta, and verify them.

One job per dataset (the `dataset` parameter), table by table through the
BigQuery connector. Per table:

  1. read the table (a TABLE, never a view or a query);
  2. compare its columns with the plan by name: renamed, dropped or added
     since the plan is `type_drift`, and nothing is copied;
  3. skip-existing (default): a target that already holds rows is not
     touched: `skipped_nonempty` when its count equals the source's,
     `count_mismatch` otherwise. overwrite: INSERT OVERWRITE rewrites the
     ROWS, never drops the table. append: adds the rows;
  4. convert each column to the planned type, with ANSI casts, so a value
     that does not fit fails the table instead of becoming NULL;
  5. verify: the target count against a fresh source count; with
     `counts+sums`, exact sums of every DECIMAL and BIGINT column.

Each table is copied at its own moment: a live source is consistent per table,
not across tables. A per-table failure is recorded and the run continues.
"""
from decimal import Decimal

from gcp_aidp.dataplane.common import *  # noqa: F403 - inlined above in the notebook

PROBLEMS = ("count_mismatch", "sum_mismatch", "type_drift", "failed")


def column_expr(column: dict, source_type: str) -> str:
    """The SELECT expression that turns the connector's column into the planned type."""
    name = "`" + column["name"].replace("`", "``") + "`"
    if column["source_type"] == "TIME" and source_type in ("bigint", "long"):
        # The connector may deliver TIME as microseconds since midnight.
        return f"date_format(timestamp_micros({name}), 'HH:mm:ss.SSSSSS') AS {name}"
    return f"CAST({name} AS {column['target_type']}) AS {name}"


def _dec(value):
    return None if value is None else Decimal(value)


def _sums(df, columns) -> dict:
    cols = [c["name"] for c in columns if c["target_type"].upper().startswith(("DECIMAL", "BIGINT"))]
    if not cols:
        return {}
    row = df.selectExpr(*[f"CAST(SUM(`{c}`) AS STRING) AS `{c}`" for c in cols]).collect()[0]
    return {c: row[c] for c in cols}


def copy_table(spark, read, t: dict, mode: str, verify: str) -> dict:
    fqn = quote(*t["target"])
    started = time.time()
    source = read(t["source"])
    source_types = {f.name: f.dataType.simpleString() for f in source.schema.fields}
    planned = [c["name"] for c in t["columns"]]
    if set(source_types) != set(planned):
        return {"status": "type_drift",
                "layout_drift": {"not_in_source": sorted(set(planned) - set(source_types)),
                                 "not_in_plan": sorted(set(source_types) - set(planned))}}
    if not table_exists(spark, fqn):
        return {"status": "target_missing", "reason": "run 01_structure first"}

    existing = spark.table(fqn).count()
    if existing and mode == "skip-existing":
        src = source.count()
        return {"status": "skipped_nonempty" if src == existing else "count_mismatch",
                "source_rows": src, "target_rows": existing, "copied": False}

    exprs = [column_expr(c, source_types[c["name"]]) for c in t["columns"]]
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        source.selectExpr(*exprs).write.insertInto(fqn, overwrite=(mode == "overwrite"))
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi)

    target = spark.table(fqn)
    rec = {"source_rows": read(t["source"]).count(), "target_rows": target.count(), "copied": True,
           "seconds": round(time.time() - started, 1), "plan_rows": t.get("num_rows")}
    if mode == "append":
        rec["target_rows_before"] = existing
    expected = rec["source_rows"] + (existing if mode == "append" else 0)
    if rec["target_rows"] != expected:
        rec["status"] = "count_mismatch"
    elif verify == "counts+sums":
        rec["source_sums"], rec["target_sums"] = _sums(read(t["source"]), t["columns"]), _sums(target, t["columns"])
        # Compared as exact decimals: the source's scale (DECIMAL(38,9)) need not be the target's.
        differ = [c for c in rec["source_sums"] if _dec(rec["source_sums"][c]) != _dec(rec["target_sums"].get(c))]
        rec["status"] = "sum_mismatch" if differ else "verified"
        if differ:
            rec["sum_mismatch_columns"] = differ
    else:
        rec["status"] = "verified"
    return rec


def main(spark, params: dict, plan: dict) -> list[str]:
    dataset = params.get("dataset")
    if not dataset:
        raise ValueError("parameter `dataset` is required: one copy job per dataset")
    mode, verify = params.get("mode") or "skip-existing", params.get("verify") or "counts"
    if mode not in ("skip-existing", "overwrite", "append") or verify not in ("counts", "counts+sums"):
        raise ValueError(f"mode {mode!r} / verify {verify!r}: see the PARAMS cell for the choices")
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # DATETIME is carried as TIMESTAMP in UTC
    only = set(as_list(params.get("tables")))
    tables = [t for t in plan["tables"] if t["dataset"] == dataset and (not only or t["name"] in only)]
    if not tables:
        raise ValueError(f"the plan has no copyable table in dataset {dataset!r}")

    path = reports_dir(params) / f"copy_report_{dataset}.json"
    report = read_report(path, plan["catalog"]) or {"stage": "copy", "dataset": dataset, "tables": {}}
    report.update(catalog=plan["catalog"], started=now(), mode=mode, verify=verify)
    structure = (read_report(reports_dir(params) / "structure_report.json", plan["catalog"]) or {}).get("tables", {})
    read = bigquery_reader(spark, gcp_credentials(params), plan["project"])

    # ponytail: one table at a time; add a thread pool if a dataset's wall time matters.
    for t in tables:
        key = f"{t['dataset']}.{t['name']}"
        if structure.get(key, {}).get("status") in ("failed", "type_drift"):
            rec = {"status": "failed", "reason": f"structure step recorded {structure[key]['status']}"}
        elif params.get("dry-run"):
            rec = {"status": "dry_run"}
        else:
            try:
                rec = copy_table(spark, read, t, mode, verify)
            except Exception as exc:  # noqa: BLE001 - recorded; the run continues
                rec = {"status": "failed", "reason": error_text(exc, 300)}
        previous = report["tables"].get(key, {})
        if previous.get("status") in PROBLEMS and rec.get("copied") is False:
            rec = previous  # a run that copies nothing never softens a recorded failure
        rec["at"] = now()
        report["tables"][key] = rec
        write_json(path, report)
        print(f"  {rec['status']:<17} {key}  source={rec.get('source_rows')} target={rec.get('target_rows')}"
              + (f"  {rec.get('reason')}" if rec.get("reason") else ""))

    report["finished"] = now()
    write_json(path, report)
    bad = [f"table {k}: {r['status']}: "
           + (r.get("reason") or f"source={r.get('source_rows')} target={r.get('target_rows')}")
           for k, r in report["tables"].items() if r["status"] in PROBLEMS]
    print(f"\ncopy {dataset}: {len(bad)} problem(s); report {path}")
    print("Each table was copied at its own moment: a source that kept changing is consistent per "
          "table, not across tables.")
    return bad
