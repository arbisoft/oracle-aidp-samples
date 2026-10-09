"""03 reconcile: plan versus reality. Reads the plan, the reports and the catalog; writes reports only.

One verdict per planned table and view. With `counts`, each verified table
is recounted, so a table changed since its copy shows as COUNT_DRIFT.
Problem verdicts make the stage exit 1.
"""
from gcp_aidp.dataplane.common import *  # noqa: F403 - inlined above in the notebook

PROBLEM_VERDICTS = ("STRUCTURE_FAILED", "STRUCTURE_TYPE_DRIFT", "COPY_FAILED", "MISSING_DESPITE_REPORT",
                    "COUNT_DRIFT", "VIEW_FAILED", "VIEW_MISSING_DESPITE_REPORT", "EXTERNAL_FAILED")


def _verdict(spark, fqn, structure, copy, recount) -> tuple[str, str]:
    s, c = (structure or {}).get("status"), (copy or {}).get("status")
    if s == "failed":
        return "STRUCTURE_FAILED", structure.get("reason", "")
    if s == "type_drift":
        return "STRUCTURE_TYPE_DRIFT", "; ".join(structure.get("drift", []))
    present = table_exists(spark, fqn)
    if not present:
        return ("MISSING_DESPITE_REPORT", "a report says created") if s or c else ("NOT_MIGRATED", "")
    if c in ("verified", "skipped_nonempty"):
        if recount:
            now_rows = spark.table(fqn).count()
            if now_rows != copy.get("target_rows"):
                return "COUNT_DRIFT", f"verified at {copy.get('target_rows')} rows, now {now_rows}"
        verdict = "MIGRATED_VERIFIED" if c == "verified" else "PRESENT_NOT_REVERIFIED"
        return verdict, f"{copy.get('target_rows')} rows"
    if c in ("count_mismatch", "sum_mismatch", "type_drift", "failed", "target_missing"):
        return "COPY_FAILED", f"{c}: {copy.get('reason') or ''} source={copy.get('source_rows')} target={copy.get('target_rows')}"
    return "STRUCTURE_ONLY", "no copy yet"


def main(spark, params: dict, plan: dict) -> list[str]:
    rdir = reports_dir(params)
    structure = read_report(rdir / "structure_report.json", plan["catalog"]) or {}
    copies = {}
    for s in plan["schemas"]:
        copies.update((read_report(rdir / f"copy_report_{s['name']}.json", plan["catalog"]) or {}).get("tables", {}))
    recount = bool(params.get("counts"))

    rows = []
    for t in plan["tables"]:
        key = f"{t['dataset']}.{t['name']}"
        verdict, detail = _verdict(spark, quote(*t["target"]), structure.get("tables", {}).get(key),
                                   copies.get(key), recount)
        rows.append({"object": key, "kind": "table", "verdict": verdict, "detail": detail,
                     "plan_rows": t.get("num_rows")})
    for b in plan["not_created"]:  # BLOCKED, NEEDS_REVIEW (flagged view) or DEFERRED (materialized view)
        verdict, detail = b["verdict"], b["reason"]
        if verdict == "DEFERRED" and b.get("target") and table_exists(spark, quote(*b["target"])):
            verdict, detail = "SNAPSHOT_BUILT", "built by its refresh job; refresh it with that job"
        rows.append({"object": f"{b['dataset']}.{b['name']}", "kind": b["kind"], "verdict": verdict,
                     "detail": detail})
    for e in plan.get("external_tables", []):
        key = f"{e['dataset']}.{e['name']}"
        rec = structure.get("external_tables", {}).get(key, {})
        verdict = {"created": "EXTERNAL_CREATED", "already_existed": "EXTERNAL_CREATED",
                   "failed": "EXTERNAL_FAILED"}.get(
            rec.get("status"), "EXTERNAL_NOT_CREATED_YET")
        rows.append({"object": key, "kind": "external_table", "verdict": verdict,
                     "detail": rec.get("reason", "" if rec else "run 01_structure with external-tables=True "
                                                               "after the transfer job")})
    for v in plan["views"]:
        key = f"{v['dataset']}.{v['name']}"
        rec = structure.get("views", {}).get(key, {})
        if rec.get("status") == "failed":
            verdict, detail = "VIEW_FAILED", rec.get("reason", "")
        elif rec.get("status") in ("created", "already_existed"):
            present = table_exists(spark, quote(*v["target"]))
            verdict, detail = ("VIEW_CREATED", "") if present else ("VIEW_MISSING_DESPITE_REPORT", "")
        else:
            verdict, detail = "VIEW_NOT_CREATED_YET", ""
        rows.append({"object": key, "kind": "view", "verdict": verdict, "detail": detail})

    counts = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    report = {"stage": "reconcile", "catalog": plan["catalog"], "at": now(), "recount": recount,
              "counts": counts, "objects": rows}
    write_json(rdir / "MIGRATION_REPORT.json", report)
    md = [f"# Migration report — catalog `{plan['catalog']}`", "", f"Generated {report['at']}.", "",
          "| Verdict | Count |", "|---|---|"] + [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    md += ["", "| Object | Kind | Verdict | Detail |", "|---|---|---|---|"]
    md += [f"| `{r['object']}` | {r['kind']} | {r['verdict']} | {str(r['detail']).replace('|', '/')} |" for r in rows]
    md += ["", "Each table was copied at its own moment: a source that kept changing during the copy is "
           "consistent per table, not across tables."]
    (rdir / "MIGRATION_REPORT.md").write_text("\n".join(md) + "\n")

    for r in rows:
        print(f"  {r['verdict']:<28} {r['object']}  {r['detail']}")
    problems = [r for r in rows if r["verdict"] in PROBLEM_VERDICTS]
    print(f"\nreconcile: {counts}; {len(problems)} problem(s); report {rdir / 'MIGRATION_REPORT.md'}")
    return [f"{r['kind']} {r['object']}: {r['verdict']}: {r['detail']}" for r in problems]
