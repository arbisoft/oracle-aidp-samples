"""Execute a plan offline: write one reviewable artifact per asset, plus a report.

Nothing here contacts Google Cloud or AIDP. `publish --apply` is the only verb
that writes to AIDP. Re-running into the same directory rewrites the same files
from the same plan, so a stopped run is resumed by running it again; an
in-progress marker stops `verify` (and later `publish`) from trusting a
directory a run did not finish.

Statuses, and the verdict `verify` gives each:
  ok                   → PASS    translated, no known issue
  needs_manual_review  → REVIEW  a caveat or flag needs a human
  blocked              → REVIEW  not translated; the original is kept, commented out
  reported             → SKIP    inventoried only (REPORT in the plan)
  skipped              → SKIP    a later version (SKIP in the plan)
  error                → FAIL    the migrator itself failed on this asset
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Callable

from gcp_aidp._atomic import write_text_atomic
from gcp_aidp.dataplane import data_plan, write_jobs, write_notebooks
from gcp_aidp.translate import ddl
from gcp_aidp.translate.gcs_to_oci import build_transfer, location_for, rewrite_uri
from gcp_aidp.translate.googlesql_to_spark import Context, statements, translate
from gcp_aidp.translate.pyspark_to_aidp import translate_pyspark, translate_spark_sql

IN_PROGRESS_MARKER = ".gcp-aidp-migration-in-progress"
STATUSES = ("ok", "needs_manual_review", "blocked", "reported", "skipped", "error")
_RELATIONS = ("aidp_delta_table", "aidp_view", "aidp_external_table")


def _f(rule: str, severity: str, detail: str) -> dict:
    return {"rule": rule, "severity": severity, "detail": detail}


def _status(findings: list[dict]) -> str:
    sev = {f["severity"] for f in findings}
    if "block" in sev:
        return "blocked"
    return "needs_manual_review" if sev & {"caveat", "flag"} else "ok"


def context_from_plan(plan: dict) -> Context:
    ctx = Context(project=plan.get("source_project") or "",
                  namespace=plan.get("target", {}).get("oci_namespace") or "<your-oci-namespace>")
    for a in plan["assets"]:
        if a["action"] != "MIGRATE":
            continue
        src, tgt = a["source"], a["target"]
        if tgt["type"] in _RELATIONS:
            ctx.relations[(src["dataset"], src["name"])] = (tgt["catalog"], tgt["schema"], tgt["name"])
        elif tgt["type"] == "aidp_function":
            ctx.functions[(src["dataset"], src["name"])] = (tgt["catalog"], tgt["schema"], tgt["name"])
        elif tgt["type"] == "oci_bucket":
            ctx.buckets[src["name"]] = tgt["name"]
    return ctx


def _sql_findings(result) -> list[dict]:
    return [_f(x.rule, x.severity, x.detail) for x in result.findings]


class _Writer:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.used: set[str] = set()

    def path(self, category: str, name: str, suffix: str, asset_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", name).strip(".") or "unnamed"
        rel = f"{category}/{safe}{suffix}"
        if rel.casefold() in self.used:  # two names that sanitize alike
            rel = f"{category}/{safe}-{hashlib.sha256(asset_id.encode()).hexdigest()[:10]}{suffix}"
        self.used.add(rel.casefold())
        return self.out_dir / rel

    def sql(self, category: str, name: str, asset_id: str, findings: list[dict], body: str,
            *, blocked: bool = False, comment: str = "--", suffix: str = ".sql",
            original: str = "GoogleSQL") -> str:
        c = comment
        lines = [f"{c} gcp-aidp: {asset_id}", f"{c} status: {_status(findings)}"]
        lines += [f"{c} {f['severity'].upper()} {f['rule']}: {_one_line(f['detail'])}"
                  for f in findings if f["severity"] != "rewrite"]
        if blocked:
            lines.append(f"{c} NOT TRANSLATED. The original {original} follows, commented out.")
            body = "\n".join(f"{c} {line}" if line else c for line in body.splitlines())
        p = self.path(category, name, suffix, asset_id)
        write_text_atomic(p, "\n".join(lines) + "\n\n" + body.rstrip() + "\n")
        return p.relative_to(self.out_dir).as_posix()


_DATAPROC = ("dataproc_job", "dataproc_batch", "dataproc_workflow_template")
_TRANSLATED = ("pyspark", "spark_sql")
_SETTINGS = ("step_id", "prerequisites", "job_type", "cluster", "main_file", "main_class", "args", "python_files",
             "jars", "files", "archives", "properties", "variables", "runtime_version", "code_not_read")


def _job_title(src: dict) -> str:
    runs = f"; {src['runs']} runs on Dataproc" if src.get("runs") else ""
    return f"Dataproc {src.get('job_type', 'workflow template')} {src['id']}{runs}"


def _dataproc_task(key: str, spec: dict, path: str, code: str) -> dict:
    """A job task: a Python task runs the translated script with the job's arguments; Spark SQL runs
    its statements in order in a notebook."""
    if spec.get("job_type") == "pyspark":
        return {"key": key, "python_file": path, "arguments": [str(a) for a in spec.get("args") or []]}
    return {"key": key, "statements": statements(code)}


def _settings(spec: dict) -> str:
    """A job's or step's settings as inventoried, one per line."""
    return "\n".join(f"{k}: {' '.join(map(str, v)) if isinstance(v, list) else v}" for k in _SETTINGS
                     if (v := spec.get(k)) not in (None, [], {}))


def _dataproc_step(spec: dict, ctx: Context, w: _Writer, category: str, name: str, aid: str):
    """(findings, translated code, artifact path, source) for one PySpark or Spark SQL job or step."""
    kind = spec.get("job_type")
    python = kind == "pyspark"
    if kind not in _TRANSLATED:
        findings = [_f("P97_NOT_TRANSLATED_TYPE", "block", f"{kind} is not translated in 0.2")]
        body = _settings(spec)
        return findings, "", w.sql(category, name, aid, findings, body, blocked=True, comment="#", suffix=".txt",
                                    original="Dataproc step"), body
    if python:
        source = spec.get("code") or ""
        r = translate_pyspark(spec.get("code"), ctx)
    else:
        source = spec.get("code") or ";\n".join(spec.get("queries") or [])
        r = translate_spark_sql(source, ctx, spec.get("variables"))
    findings = _sql_findings(r)
    staged = {k: spec[k] for k in ("python_files", "files", "archives") if spec.get(k)}
    jars = [j for j in spec.get("jars") or [] if "spark-bigquery" not in j]
    if staged or jars:
        findings.append(_f("P09_LOCAL_FILES", "flag", "Dataproc staged these beside the job, and AIDP does not: "
                           + "; ".join(f"{k}: {' '.join(v)}" for k, v in {**staged, "jars": jars}.items() if v)
                           + ". Upload them to the workspace or add them as cluster libraries"))
    if spec.get("code_not_read"):
        findings = [{**f, "detail": f"{f['detail']}: {spec['code_not_read']}"} if f["rule"] == "P99_CODE_NOT_READ"
                    else f for f in findings]
    c = "#" if python else "--"
    settings = "\n".join(f"{c} {line}" for line in _settings(spec).splitlines())
    blocked = r.status == "blocked"
    body = (r.sql if not blocked else (source or _settings(spec)))
    path = w.sql(category, name, aid, findings, body if blocked else f"{settings}\n\n{body}", blocked=blocked,
                 comment=c, suffix=".py" if python else ".sql", original="Dataproc job")
    return findings, ("" if blocked else r.sql), path, source


def _one_line(text: str) -> str:
    return re.sub(r"[\x00-\x1f\x7f]+", " ", str(text)).strip()


def _migrate_one(a: dict, ctx: Context, w: _Writer) -> dict:
    src, tgt, aid = a["source"], a["target"], a["id"]
    row = {"asset_id": aid, "kind": src["type"], "action": a["action"]}

    if a["action"] == "SKIP":
        return {**row, "status": "skipped", "note": a.get("reason", "")}
    if a["action"] == "REPORT":
        note = a.get("reason", "") + (f" (effort {a['effort']})" if a.get("effort") else "")
        return {**row, "status": "reported", "note": note}

    findings: list[dict] = []
    source_sql = translated = ""
    t = tgt["type"]
    stem = f"{src.get('dataset', '')}.{src.get('name') or src.get('id', '')}".lstrip(".")

    if t == "aidp_schema":
        translated = ddl.create_schema(tgt["catalog"], tgt["name"], src.get("description", ""))
        path = w.sql("schemas", src["name"], aid, findings, translated)
    elif t == "aidp_delta_table" and src["type"] == "bq_table":
        for c in tgt["columns"]:
            for rule in c["rules"]:
                findings.append(_f(rule, "rewrite" if c["severity"] == "map" else c["severity"],
                                   f"{c['name']} {c['source_type']} → {c['target_type'] or 'not carried'}"
                                   + (": " + "; ".join(c["details"]) if c["details"] else "")))
        findings = list({(f["rule"], f["detail"]): f for f in findings}.values())
        findings += [_f(*f) for f in tgt.get("layout_findings", [])]
        if not tgt["columns"]:
            findings.append(_f("TY99_UNKNOWN", "block", "no columns in the manifest"))
        if _status(findings) == "blocked":
            translated = ""
            path = w.sql("tables", stem, aid, findings, "-- table not created: a column is blocked")
        else:
            translated = ddl.create_table(tgt, tgt["columns"], partitioned_by=tgt["partitioned_by"],
                                          cluster_by=tgt["cluster_by"], comment=tgt.get("comment", ""))
            path = w.sql("tables", stem, aid, findings, translated)
    elif t == "aidp_view" and src.get("legacy_sql"):
        source_sql = src["query"]
        findings = [_f("G97_LEGACY_SQL", "block", "legacy SQL view: rewrite it in GoogleSQL first")]
        path = w.sql("views", stem, aid, findings, src["query"], blocked=True)
    elif t == "aidp_view":
        r = translate(src["query"], ctx)
        source_sql, findings = src["query"], _sql_findings(r)
        if r.status == "blocked":
            path = w.sql("views", stem, aid, findings, src["query"], blocked=True)
        else:
            translated = ddl.create_view(tgt, r.sql)
            path = w.sql("views", stem, aid, findings, translated)
    elif t == "aidp_delta_table" and src["type"] == "bq_materialized_view":
        r = translate(src["query"], ctx)
        source_sql, findings = src["query"], _sql_findings(r)
        findings.append(_f("M01_MATERIALIZED_VIEW", "info",
                           f"snapshot computed on AIDP from the migrated base tables; refresh job "
                           f"{tgt['refresh_job']} runs the INSERT OVERWRITE and is created unscheduled"))
        if r.status == "blocked":
            path = w.sql("materialized_views", stem, aid, findings, src["query"], blocked=True)
        else:
            snapshot, refresh = ddl.materialized_view(tgt, r.sql)
            translated = f"{snapshot};\n\n-- refresh job {tgt['refresh_job']}:\n{refresh};"
            path = w.sql("materialized_views", stem, aid, findings, translated)
            row["job"] = {"name": tgt["refresh_job"], "statements": [snapshot, refresh],
                          "title": f"Refresh {stem} (materialized view snapshot)"}
    elif t == "aidp_external_table":
        source_sql = "\n".join(src.get("source_uris", []))
        location, note = location_for(src.get("source_uris", []))
        oci = rewrite_uri(location, ctx.buckets, ctx.namespace) if location else None
        if location is None:
            findings.append(_f("GS02_LOCATION", "block", note))
        elif oci is None:
            findings.append(_f("GS01_BUCKET_MAP", "flag", f"{location}: bucket not in the bucket map"))
        else:
            findings.append(_f("GS01_BUCKET_MAP", "rewrite", f"{location} → {oci}"))
            if note:
                findings.append(_f("GS02_LOCATION", "caveat", note))
        statement, more = ddl.create_external_table(tgt, src.get("format"), oci or location or "")
        findings += [_f(*f) for f in more]
        if _status(findings) == "blocked" or statement is None:
            path = w.sql("external_tables", stem, aid, findings, f"-- source URIs:\n{source_sql}", blocked=True)
        else:
            translated = statement
            path = w.sql("external_tables", stem, aid, findings, translated)
    elif t == "aidp_function":
        r = translate(src["body"], ctx)
        source_sql, findings = src["body"], _sql_findings(r)
        if r.status == "blocked":
            path = w.sql("functions", stem, aid, findings, src["body"], blocked=True)
        else:
            translated, more = ddl.create_function(tgt, src.get("arguments", []), src.get("return_type"), r.sql)
            findings += [_f(*f) for f in more]
            path = w.sql("functions", stem, aid, findings, translated)
    elif src["type"] in _DATAPROC:
        steps = src.get("steps") if src["type"] == "dataproc_workflow_template" else None
        if steps is None:
            findings, translated, path, source_sql = _dataproc_step(src, ctx, w, "dataproc", tgt["name"], aid)
            if _status(findings) != "blocked":
                row["job"] = {"name": tgt["name"], "title": _job_title(src),
                              "tasks": [_dataproc_task("main", src, path, translated)]}
        else:
            findings, done = [], []
            for st in steps:
                f, step_code, step_path, _ = _dataproc_step(st, ctx, w, "dataproc", f"{tgt['name']}_{st['step_id']}",
                                                            aid)
                findings += [_f(x["rule"], x["severity"], f"step {st['step_id']}: {x['detail']}") for x in f]
                done.append({"step_id": st["step_id"], "prerequisites": st.get("prerequisites", []),
                             "job_type": st.get("job_type"), "status": _status(f), "output_path": step_path,
                             "task": _dataproc_task(st["step_id"], st, step_path, step_code)})
            if src.get("parameters"):
                findings.append(_f("P10_TEMPLATE_PARAMETER", "flag", f"template parameters {src['parameters']}: "
                                   "Dataproc substitutes them when the template runs; on AIDP they would reach "
                                   "the steps as literal text. Pass the values as arguments by hand"))
            summary = "\n".join(f"# step {d['step_id']}: {d['job_type']}, {d['status']}, after "
                                 f"{', '.join(d['prerequisites']) or 'nothing'} -> {d['output_path']}" for d in done)
            path = w.sql("dataproc", tgt["name"], aid, findings, summary, comment="#", suffix=".txt")
            row["steps"] = [{k: v for k, v in d.items() if k != "task"} for d in done]
            source_sql = translated = ""
            if _status(findings) != "blocked":  # a step that cannot run blocks the whole job
                row["job"] = {"name": tgt["name"], "title": _job_title(src),
                              "tasks": [{**d["task"], "after": d["prerequisites"]} for d in done]}
    elif t in ("spark_sql_file", "aidp_job"):
        category = "saved_queries" if t == "spark_sql_file" else "scheduled_queries"
        r = translate(src["query"], ctx)
        source_sql, findings = src["query"], _sql_findings(r)
        if t == "aidp_job" and src.get("destination_dataset"):
            findings.append(_f("J02_DESTINATION_TABLE", "flag",
                               f"writes its result into dataset {src['destination_dataset']!r}; the inventory does "
                               "not record the table or write mode, so no job is created: add the INSERT by hand"))
        elif t == "aidp_job":
            findings.append(_f("J01_UNSCHEDULED", "info",
                               f"job {tgt['name']} is created unscheduled; source schedule {src.get('schedule')!r}"))
            if r.status != "blocked":
                row["job"] = {"name": tgt["name"], "statements": [r.sql],
                              "title": f"Scheduled query {src['name']!r} (schedule {src.get('schedule')!r} not applied)"}
        name = tgt["name"].removesuffix(".sql")
        if r.status == "blocked":
            path = w.sql(category, name, aid, findings, src["query"], blocked=True)
        else:
            translated = r.sql
            path = w.sql(category, name, aid, findings, translated)
    elif t == "oci_bucket":
        translated = build_transfer(src["name"], tgt["name"], tgt["namespace"])
        source_sql = f"gs://{src['name']}/  ({src.get('location')}, {src.get('storage_class')})"
        findings = [_f("GS03_TRANSFER", "rewrite", f"rclone copy gs://{src['name']} → oci://{tgt['name']}@{tgt['namespace']}"),
                    _f("GS03_TRANSFER", "flag", "before running: install rclone, authenticate to Google Cloud "
                       "(read-only), and set a real OCI region, compartment and ~/.oci/config profile")]
        p = w.path("transfers", src["name"], ".transfer.sh", aid)
        write_text_atomic(p, translated)
        path = p.relative_to(w.out_dir).as_posix()
    else:
        raise ValueError(f"no migration for {src['type']} → {t}")

    return {**row, "status": _status(findings), "output_path": path,
            "source_sql": source_sql, "translated_sql": translated, "findings": findings,
            "changes": sum(f["severity"] in ("rewrite", "caveat") for f in findings),
            "flags": sum(f["severity"] in ("caveat", "flag", "block") for f in findings)}


def migrate(plan: dict, *, out_dir: Path, log: Callable[[str], None] | None = None) -> dict:
    if not isinstance(plan.get("assets"), list):
        raise ValueError("plan field 'assets' must be a JSON array")
    out_dir.mkdir(parents=True, exist_ok=True)
    marker = out_dir / IN_PROGRESS_MARKER
    marker.write_text(plan.get("plan_id", ""), encoding="utf-8")
    ctx = context_from_plan(plan)
    w = _Writer(out_dir)
    results = []
    for a in plan["assets"]:
        try:
            r = _migrate_one(a, ctx, w)
        except Exception as exc:  # one asset's failure is recorded; the run continues
            r = {"asset_id": a.get("id", "?"), "kind": a.get("source", {}).get("type"),
                 "status": "error", "note": f"{type(exc).__name__}: {exc}"}
        results.append(r)
        if log:
            log(f"  {r['status']:<20s} {r['asset_id']}")
    counts = {s: sum(r["status"] == s for r in results) for s in STATUSES}
    dp = data_plan(plan, results)
    write_text_atomic(out_dir / "notebooks" / "data_plan.json", json.dumps(dp, indent=2))
    notebooks = write_notebooks(dp, out_dir)
    jobs = write_jobs(dp, results, out_dir)
    notebooks += sorted({t["notebook"] for j in jobs for t in j["tasks"] if "notebook" in t} - set(notebooks))
    files = sorted({t["python_file"] for j in jobs for t in j["tasks"] if "python_file" in t})
    if log:
        log(f"  notebooks: {len(notebooks)} ({len(dp['tables'])} tables to copy, "
            f"{len(dp['views'])} views to create); jobs: {', '.join(j['name'] for j in jobs)}")
    report = {"plan_id": plan.get("plan_id"), "source_project": plan.get("source_project"),
              "complete": True, "counts": counts, "results": results, "notebooks": notebooks, "files": files,
              "jobs": jobs}
    write_text_atomic(out_dir / "report.json", json.dumps(report, indent=2))
    write_text_atomic(out_dir / "report.md", _markdown(report))
    marker.unlink()
    return report


def _markdown(report: dict) -> str:
    c = report["counts"]
    md = [f"# Migration report — plan `{report['plan_id']}`", "",
          "| ok | needs review | blocked | reported | skipped | error |", "|---|---|---|---|---|---|",
          f"| {c['ok']} | {c['needs_manual_review']} | {c['blocked']} | {c['reported']} | {c['skipped']} | {c['error']} |",
          "", "`ok` means translated with no known issue. It is **not execution-verified**: nothing here "
          "runs the artifacts. Review before running them.", ""]
    for status, title in (("blocked", "Blocked — not translated"), ("needs_manual_review", "Needs review"),
                          ("error", "Errors"), ("ok", "Translated"), ("reported", "Reported only"),
                          ("skipped", "Skipped (later version)")):
        rows = [r for r in report["results"] if r["status"] == status]
        if not rows:
            continue
        md += [f"## {title} ({len(rows)})", ""]
        for r in rows:
            md.append(f"### `{r['asset_id']}`" + (f" → `{r['output_path']}`" if r.get("output_path") else ""))
            if r.get("note"):
                md.append(f"\n{r['note']}")
            for f in r.get("findings", []):
                if f["severity"] != "rewrite":
                    md.append(f"- **{f['severity']}** `{f['rule']}` {f['detail']}")
            if r.get("source_sql") and r.get("translated_sql") and status in ("ok", "needs_manual_review"):
                md += ["", "<details><summary>before / after</summary>", "", "```sql", r["source_sql"].strip(),
                       "```", "", "```sql", r["translated_sql"].strip(), "```", "", "</details>"]
            md.append("")
    return "\n".join(md)
