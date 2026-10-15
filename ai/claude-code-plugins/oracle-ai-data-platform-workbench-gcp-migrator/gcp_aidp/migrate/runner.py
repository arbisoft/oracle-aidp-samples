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
from gcp_aidp.dataplane import (
    data_plan,
    routine_notebook,
    sql_notebook,
    write_jobs,
    write_notebooks,
)
from gcp_aidp.translate import dataform, ddl
from gcp_aidp.translate.gcs_to_oci import location_for, rewrite_uri
from gcp_aidp.translate.googlesql_to_spark import (
    Context,
    Result,
    bind_parameters,
    statements,
    translate,
)
from gcp_aidp.translate.notebook_to_aidp import translate_notebook
from gcp_aidp.translate.pyspark_to_aidp import (
    spark_properties,
    translate_pyspark,
    translate_spark_sql,
)
from gcp_aidp.translate.schedules import from_bigquery, from_unix_cron
from gcp_aidp.translate.types import quote_ident

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
                  namespace=plan.get("target", {}).get("oci_namespace") or "<your-oci-namespace>",
                  geography=(plan.get("type_modes") or {}).get("geography", "block"))
    for a in plan["assets"]:
        if a["action"] != "MIGRATE":
            continue
        src, tgt = a["source"], a["target"]
        if tgt["type"] in _RELATIONS:
            ctx.relations[(src["dataset"], src["name"])] = (tgt["catalog"], tgt["schema"], tgt["name"])
        elif tgt["type"] == "aidp_function":
            ctx.functions[(src["dataset"], src["name"])] = (tgt["catalog"], tgt["schema"], tgt["name"])
        if src.get("type") == "bq_table" and tgt["type"] in _RELATIONS:
            ctx.columns[(tgt["catalog"], tgt["schema"], tgt["name"])] = {c["name"] for c in src.get("columns", [])}
        elif tgt["type"] == "oci_bucket":
            ctx.buckets[src["name"]] = tgt["name"]
        elif src["type"] == "dataform_repository":
            dataform.register_outputs(src, ctx, (plan.get("target") or {}).get("catalog", ""))
    return ctx


_CREATES = re.compile(r"(?is)\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                      r"`?(?P<ds>\w+)`?\.`?(?P<name>\w+)`?")
_CTAS = re.compile(r"(?is)^(?P<head>\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w.`]+)\s+AS\b")
_SCALAR_ARGUMENTS = {"INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "BOOL", "BOOLEAN", "STRING",
                     "BYTES", "DATE", "DATETIME", "TIMESTAMP"}


def _routine(src: dict, tgt: dict, ctx: Context) -> tuple[list[dict], list[str], list[str]]:
    """(findings, Spark statements, parameters used) for a SQL table function or procedure."""
    findings, args = [], src.get("arguments", [])
    for a in args:
        if str(a.get("type") or "").upper() not in _SCALAR_ARGUMENTS:
            findings.append(_f("G25_ROUTINE_PARAMETER", "block", f"argument {a.get('name')} {a.get('type')}: a Spark "
                                                                 "named parameter takes a single value"))
        if a.get("mode") in ("OUT", "INOUT"):
            findings.append(_f("D08_PROCEDURE", "block", f"{a['mode']} argument {a['name']}: a notebook returns "
                                                         "no values to its caller"))
    if tgt["type"] == "aidp_procedure" and args and any("mode" not in a for a in args):
        findings.append(_f("D08_PROCEDURE", "flag", "argument modes are not in the manifest: run inventory again"))
    body = src.get("body", "").strip()
    if tgt["type"] == "aidp_procedure":  # BEGIN ... END around the statements
        body = re.sub(r"(?is)^BEGIN\b(.*)\bEND\s*;?$", r"\1", body).strip()
    parts = statements(body) if tgt["type"] == "aidp_procedure" else [body]
    # A table the procedure creates in a migrated dataset maps as every table does: dataset → schema.
    schemas = {(d, c) for (d, _), (c, _, _) in ctx.relations.items()}
    created = {}
    for part in parts:
        m = _CREATES.match(part)
        if m and (m["ds"], tgt["catalog"]) in schemas:
            created[(m["ds"], m["name"])] = (tgt["catalog"], m["ds"], m["name"])
    if created:
        ctx = Context(**{**vars(ctx), "relations": {**ctx.relations, **created}})
    out, used = [], []
    for part in parts:
        if _CTAS.match(part):  # Spark's CTAS without USING takes the session's default format
            part = _CTAS.sub(r"\g<head> USING DELTA AS", part, count=1)
            findings.append(_f("D08_PROCEDURE", "rewrite", "CREATE TABLE ... AS SELECT → USING DELTA"))
        r = translate(part, ctx)
        findings += _sql_findings(r)
        if r.status == "blocked":
            continue
        reads = set().union(*[cols for fqn, cols in ctx.columns.items()
                              if ".".join(quote_ident(x) for x in fqn) in r.sql] or [set()])
        bound, more = bind_parameters(r.sql, [a["name"] for a in args], reads)
        findings += _sql_findings(Result("", "", more))
        out.append(bound)
        used += [a["name"] for a in args if f":{a['name']}" in bound]
    rule = "D07_TABLE_FUNCTION" if tgt["type"] == "aidp_table_function" else "D08_PROCEDURE"
    if any(f["severity"] == "block" for f in findings):
        findings.append(_f(rule, "block", "not translated as a whole: rewrite it by hand (the blocks above say why)"))
    else:
        what = ("a Python function returning a DataFrame" if rule == "D07_TABLE_FUNCTION"
                else "a notebook, its arguments in PARAMS")
        findings.append(_f(rule, "rewrite", f"{len(out)} statement(s) as {what}; SQL that calls it is not rewritten"))
    return list({(f["rule"], f["severity"], f["detail"]): f for f in findings}.values()), out, list(dict.fromkeys(used))


def _sql_findings(result) -> list[dict]:
    return [_f(x.rule, x.severity, x.detail) for x in result.findings]


class _Writer:
    def __init__(self, out_dir: Path, plan_id: str = ""):
        self.out_dir, self.plan_id = out_dir, plan_id
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
    if r.status != "blocked" and spec.get("properties"):
        code, more = spark_properties(r.sql, spec["properties"], python)
        r = Result(r.source_sql, code, r.findings + more)
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


def _carry(what: str, quartz: str | None, why: str, tz: str) -> tuple[dict | None, list[dict]]:
    """(AIDP schedule, findings): a translated schedule rides on the job, created PAUSED."""
    if quartz:
        return ({"quartzCronExpression": quartz, "timezoneId": tz},
                [_f("J03_SCHEDULE", "caveat" if why else "rewrite",
                    f"{what} → Quartz {quartz!r} ({tz}); the job is created with it PAUSED: turn it on in AIDP "
                    "once the job has been checked" + (f" ({why})" if why else ""))])
    return None, [_f("J01_UNSCHEDULED", "flag", f"{what} not carried: {why}. The job is created unscheduled")]


def _dataproc_schedule(src: dict) -> tuple[list[dict], list[dict]]:
    """(AIDP schedules, findings). An AIDP job holds one schedule, so each schedule becomes a job of
    its own: one per Cloud Scheduler job, and two for a cron that runs on a day of the month *or*
    a day of the week, which one Quartz expression cannot say."""
    schedules, findings = [], []
    for sc in src.get("schedules") or []:
        what = f"Cloud Scheduler job {sc['name']} ({sc.get('state') or '?'}) {sc['schedule']!r}"
        tz = sc.get("time_zone") or "UTC"
        parts = sc["schedule"].split()
        if len(parts) == 5 and "*" not in (parts[2], parts[4]):
            for half, part in (("day of month", parts[:4] + ["*"]), ("day of week", parts[:2] + ["*"] + parts[3:])):
                schedule, f = _carry(f"{what}, its {half} half", *from_unix_cron(" ".join(part)), tz)
                schedules += [schedule] if schedule else []
                findings += f
            continue
        schedule, f = _carry(what, *from_unix_cron(sc["schedule"]), tz)
        schedules += [schedule] if schedule else []
        findings += f
    if len(schedules) > 1:
        findings.append(_f("J04_ONE_JOB_PER_SCHEDULE", "rewrite",
                           f"{len(schedules)} schedules → {len(schedules)} AIDP jobs, numbered _1 to _{len(schedules)}, "
                           "running the same tasks"))
    return schedules, findings


def _one_line(text: str) -> str:
    return re.sub(r"[\x00-\x1f\x7f]+", " ", str(text)).strip()


def _dataform_sql(job: dict, blocked: bool) -> tuple[str, str]:
    """(the repository's file, its original queries). Each action is commented with its status; a
    blocked repository keeps only the original queries, which the writer comments out."""
    note = (lambda text: text) if blocked else (lambda text: f"-- {text}")
    parts, originals = [], []
    for rec in job["actions"]:
        head = f"action {rec['label']} ({rec['what']})"
        if rec.get("taskKey"):
            head += f" -> task {rec['taskKey']}" + (f", after {', '.join(rec['dependsOn'])}" if rec["dependsOn"] else "")
        else:
            head += ": no task"
        lines = [note(head)] + [note(f"{f['severity'].upper()} {f['rule']}: {_one_line(f['detail'])}")
                                for f in rec["findings"] if f["severity"] != "rewrite"]
        if blocked:
            lines += rec["original"]
        else:
            lines += [st + ";\n" for st in rec["statements"]]
        if rec["original"]:
            originals.append(f"-- {rec['label']}\n" + "\n".join(rec["original"]))
        parts.append("\n".join(lines).rstrip())
    return "\n\n".join(parts), "\n\n".join(originals)


def _migrate_one(a: dict, ctx: Context, w: _Writer, catalog: str = "") -> dict:
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
        if tgt.get("needs_export"):
            findings.append(_f("X02_NEEDS_EXPORT", "block",
                               f"the BigQuery connector cannot read {', '.join(tgt['needs_export'])}; plan with "
                               "--export-over-gb and --export-bucket so it goes through Parquet files"))
        if tgt.get("load_from") and _status(findings) != "blocked":
            findings.append(_f("X01_EXPORT_ROUTE", "flag",
                               f"before the copy job, `gcp-aidp export --apply` exports it to {tgt['load_from']['gcs']} "
                               f"and copies the files to {tgt['load_from']['oci']}; the job loads them and "
                               "checks the count against BigQuery"))
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
        statement, more = ddl.create_external_table(tgt, src.get("format"), oci or location or "",
                                                    tgt.get("columns"), src.get("csv_options"), src.get("autodetect"))
        findings += [_f(*f) for f in more]
        if _status(findings) == "blocked" or statement is None:
            path = w.sql("external_tables", stem, aid, findings, f"-- source URIs:\n{source_sql}", blocked=True)
        else:
            translated = statement
            path = w.sql("external_tables", stem, aid, findings, translated)
            row["location"] = oci  # 01_structure creates the table once files are there
    elif t == "aidp_function" and (udf := ddl.python_udf(src["name"], src.get("arguments", []),
                                                         src.get("return_type"), src["body"])):
        source_sql, translated = src["body"], udf
        findings = [_f("D06_SQL_FUNCTION", "rewrite", f"arithmetic SQL function → Python UDF `{src['name']}`, "
                       "registered for the session by running its notebook; SQL that calls it is not rewritten")]
        path = w.sql("functions", stem, aid, findings, udf, comment="#", suffix=".py")
        p = w.path("functions", stem, ".ipynb", aid)
        write_text_atomic(p, json.dumps(routine_notebook(t, stem, src.get("arguments", []), [udf], [], w.plan_id),
                                        indent=1) + "\n")
        row["notebook"] = p.relative_to(w.out_dir).as_posix()
    elif t == "aidp_function":
        r = translate(src["body"], ctx)
        source_sql, findings = src["body"], _sql_findings(r)
        if r.status == "blocked":
            path = w.sql("functions", stem, aid, findings, src["body"], blocked=True)
        else:
            translated, more = ddl.create_function(tgt, src.get("arguments", []), src.get("return_type"), r.sql)
            findings += [_f(*f) for f in more]
            path = w.sql("functions", stem, aid, findings, translated)
    elif t in ("aidp_table_function", "aidp_procedure"):
        findings, statements_out, used = _routine(src, tgt, ctx)
        source_sql = src.get("body", "")
        if _status(findings) == "blocked":
            path = w.sql("functions", stem, aid, findings, source_sql, blocked=True)
        else:
            translated = ";\n\n".join(statements_out)
            path = w.sql("functions", stem, aid, findings, translated)
            p = w.path("functions", stem, ".ipynb", aid)
            write_text_atomic(p, json.dumps(routine_notebook(t, stem, src.get("arguments", []), statements_out, used,
                                                             w.plan_id), indent=1) + "\n")
            row["notebook"] = p.relative_to(w.out_dir).as_posix()
    elif t == "aidp_notebook":
        cells, notebook_findings = translate_notebook(src["cells"], ctx)
        findings = _sql_findings(Result("", "", notebook_findings))
        header = [(f"*Migrated from BigQuery Studio notebook `{src['name']}` by gcp-aidp. Status: "
                   f"{_status(findings)}.*")] + [f"- **{f['severity']}** `{f['rule']}` {_one_line(f['detail'])}"
                                               for f in findings if f["severity"] != "rewrite"]
        nb = {"cells": [{"cell_type": "markdown", "metadata": {}, "source": "\n".join(header)}]
              + [{"cell_type": c["cell_type"], "metadata": {}, "source": c["source"],
                  **({"execution_count": None, "outputs": []} if c["cell_type"] == "code" else {})} for c in cells],
              "metadata": {"gcp_aidp": {"source": src["name"]}, "language_info": {"name": "python"}},
              "nbformat": 4, "nbformat_minor": 5}
        p = w.path("bq_notebooks", tgt["name"], ".ipynb", aid)
        write_text_atomic(p, json.dumps(nb, indent=1) + "\n")
        path = p.relative_to(w.out_dir).as_posix()
        source_sql = "\n\n".join(c.get("source", "") for c in src["cells"])
        translated = "" if _status(findings) == "blocked" else "\n\n".join(c["source"] for c in cells)
        if _status(findings) != "blocked":  # a notebook with a blocked cell stays local for a person to finish
            row["notebook"] = path
    elif src["type"] in _DATAPROC:
        steps = src.get("steps") if src["type"] == "dataproc_workflow_template" else None
        if steps is None:
            findings, translated, path, source_sql = _dataproc_step(src, ctx, w, "dataproc", tgt["name"], aid)
            if _status(findings) != "blocked":
                schedules, more = _dataproc_schedule(src)
                findings += more
                row["job"] = {"name": tgt["name"], "title": _job_title(src), "schedules": schedules,
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
                schedules, more = _dataproc_schedule(src)
                findings += more
                row["job"] = {"name": tgt["name"], "title": _job_title(src), "schedules": schedules,
                              "tasks": [{**d["task"], "after": d["prerequisites"]} for d in done]}
    elif src["type"] == "dataform_repository":
        job, findings, creatable = dataform.translate_repository(src, ctx, catalog, tgt["name"])
        blocked = any(f["severity"] == "block" for f in findings)
        translated, source_sql = _dataform_sql(job, blocked)
        schedule = dataform.schedule_text(src)
        if creatable:
            findings.append(_f("J01_UNSCHEDULED", "info", f"job {tgt['name']} is created unscheduled; "
                                                           f"source schedule: {schedule}"))
            row["job"] = {"name": tgt["name"], "tasks": job["tasks"],
                          "title": f"Dataform repository {src['name']!r} (schedule: {schedule}; not applied)"}
        path = w.sql("dataform", src["name"], aid, findings, translated or "-- no actions were read",
                     blocked=blocked)
        if blocked:
            translated = ""
    elif t in ("spark_sql_file", "aidp_job"):
        category = "saved_queries" if t == "spark_sql_file" else "scheduled_queries"
        r = translate(src["query"], ctx)
        source_sql, findings = src["query"], _sql_findings(r)
        if t == "aidp_job" and src.get("destination_dataset"):
            findings.append(_f("J02_DESTINATION_TABLE", "flag",
                               f"writes its result into dataset {src['destination_dataset']!r}; the inventory does "
                               "not record the table or write mode, so no job is created: add the INSERT by hand"))
        elif t == "aidp_job" and r.status != "blocked":  # no job, no schedule to carry
                schedule, more = _carry(f"schedule {src.get('schedule')!r} (UTC"
                                        + (", disabled at the source)" if src.get("disabled") else ")"),
                                        *from_bigquery(src.get("schedule"), src.get("next_run_time")), "UTC")
                findings += more
                row["job"] = {"name": tgt["name"], "statements": [r.sql], "schedule": schedule,
                              "title": f"Scheduled query {src['name']!r}"}
        name = tgt["name"].removesuffix(".sql")
        if r.status == "blocked":
            path = w.sql(category, name, aid, findings, src["query"], blocked=True)
        else:
            translated = r.sql
            path = w.sql(category, name, aid, findings, translated)
            if t == "spark_sql_file":  # opened and run in the workspace, as in BigQuery; no job runs it
                p = w.path("saved_queries", name, ".ipynb", aid)
                write_text_atomic(p, json.dumps(sql_notebook(f"Saved query {src['name']!r}", statements(r.sql),
                                                             w.plan_id, show_last=True), indent=1) + "\n")
                row["notebook"] = p.relative_to(w.out_dir).as_posix()
    elif t == "oci_bucket":
        ns = tgt.get("namespace") or ""
        ns = "" if ns.startswith("<") else ns  # the plan's <your-oci-namespace> placeholder
        spec = {"gcs_bucket": src["name"], "oci_bucket": tgt["name"], "namespace": ns,
                "gcs": f"gs://{src['name']}/", "oci": f"oci://{tgt['name']}@{ns}/"}
        translated = json.dumps(spec, indent=2)
        source_sql = f"gs://{src['name']}/  ({src.get('location')}, {src.get('storage_class')})"
        findings = [_f("GS03_TRANSFER", "rewrite", f"gs://{src['name']} → oci://{tgt['name']}@{ns or '?'}: copied by "
                                                   "`gcp-aidp transfer --apply` (read-only on Cloud Storage; never "
                                                   "deletes). The OCI bucket must exist")]
        if not ns:
            findings.append(_f("GS03_TRANSFER", "flag", "no OCI namespace: set OCI_NAMESPACE (or plan --namespace), "
                                                        "then plan and migrate again"))
        p = w.path("transfers", src["name"], ".transfer.json", aid)
        write_text_atomic(p, translated + "\n")
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
    w = _Writer(out_dir, plan.get("plan_id", ""))
    results = []
    for a in plan["assets"]:
        try:
            r = _migrate_one(a, ctx, w, (plan.get("target") or {}).get("catalog", ""))
        except Exception as exc:  # noqa: BLE001 - one asset's failure is recorded; the run continues
            r = {"asset_id": a.get("id", "?"), "kind": a.get("source", {}).get("type"),
                 "status": "error", "note": f"{type(exc).__name__}: {exc}"}
        results.append(r)
        if log:
            log(f"  {r['status']:<20s} {r['asset_id']}")
    counts = {s: sum(r["status"] == s for r in results) for s in STATUSES}
    export = _export_spec(plan, results)
    dp = data_plan(plan, results)
    write_text_atomic(out_dir / "notebooks" / "data_plan.json", json.dumps(dp, indent=2))
    notebooks = write_notebooks(dp, out_dir)
    jobs = write_jobs(dp, results, out_dir)
    notebooks += sorted({t["notebook"] for j in jobs for t in j["tasks"] if "notebook" in t} - set(notebooks))
    notebooks += [r["notebook"] for r in results  # Studio notebooks, saved queries: their own job is the check
                  if r.get("notebook") and r["notebook"] not in notebooks]
    files = sorted({t["python_file"] for j in jobs for t in j["tasks"] if "python_file" in t})
    if log:
        log(f"  notebooks: {len(notebooks)} ({len(dp['tables'])} tables to copy, "
            f"{len(dp['views'])} views to create); jobs: {', '.join(j['name'] for j in jobs)}")
    report = {"plan_id": plan.get("plan_id"), "source_project": plan.get("source_project"),
              "complete": True, "counts": counts, "results": results, "notebooks": notebooks, "files": files,
              "jobs": jobs, "export": export}
    write_text_atomic(out_dir / "report.json", json.dumps(report, indent=2))
    write_text_atomic(out_dir / "report.md", _markdown(report))
    marker.unlink()
    return report


def _export_spec(plan: dict, results: list[dict]) -> dict | None:
    """What `gcp-aidp export` exports and copies: the routed tables that were not blocked."""
    export = plan.get("export")
    created = {r["asset_id"] for r in results if r.get("status") in ("ok", "needs_manual_review")}
    tables = [{"dataset": a["source"]["dataset"], "name": a["source"]["name"], **a["target"]["load_from"]}
              for a in plan["assets"] if a["id"] in created and a["target"].get("load_from")]
    if not export or not tables:
        return None
    return {"project": plan["source_project"], "bucket": export["bucket"], "oci_bucket": export["oci_bucket"],
            "namespace": plan["target"]["oci_namespace"], "prefix": export["prefix"], "tables": tables}


def _markdown(report: dict) -> str:
    c = report["counts"]
    md = [f"# Migration report — plan `{report['plan_id']}`", "",
          "| ok | needs review | blocked | reported | skipped | error |", "|---|---|---|---|---|---|",
          f"| {c['ok']} | {c['needs_manual_review']} | {c['blocked']} | {c['reported']} | {c['skipped']} | {c['error']} |",
          "", ("`ok` means translated with no known issue. It is **not execution-verified**: nothing here "
               "runs the artifacts. Review before running them."), ""]
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
