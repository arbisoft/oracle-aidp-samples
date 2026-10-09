"""Dataproc: clusters, job definitions, serverless batches, workflow templates, and the
Cloud Scheduler jobs that start them, per region.

`jobs.list` returns run history: a nightly job appears once per run, usually with
a new date argument each time. Runs are grouped into job definitions (region, type,
main file or class or query, cluster), each with its latest arguments, a run count
and its last run. Batches are grouped the same way.

The main `.py` or `.sql` file of each PySpark and Spark SQL definition is read
from Cloud Storage (an object read, at most MAX_CODE_BYTES), so `migrate` can
translate it. JARs and other files are recorded, not read.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from urllib.parse import quote

from gcp_aidp.gcp_client import GcpClient, GcpError
from gcp_aidp.inventory._common import GCS, _per_region, _scan

API = "https://dataproc.googleapis.com/v1/projects"
SCHEDULER = "https://cloudscheduler.googleapis.com/v1/projects"
_TEMPLATE_URI = re.compile(r"/projects/[^/]+/(?:regions|locations)/([^/]+)/workflowTemplates/([^/:]+):instantiate$")
_JOB_URI = re.compile(r"/projects/[^/]+/regions/([^/]+)/jobs:submit$")
_BATCH_URI = re.compile(r"/projects/[^/]+/locations/([^/]+)/batches(?:\?.*)?$")
MAX_CODE_BYTES = 1_000_000
# The job (and batch) type fields of the API, and the name the manifest uses.
_KIND = {"pysparkJob": "pyspark", "sparkJob": "spark", "sparkSqlJob": "spark_sql", "sparkRJob": "spark_r",
         "hiveJob": "hive", "pigJob": "pig", "hadoopJob": "hadoop", "prestoJob": "presto",
         "trinoJob": "trino", "flinkJob": "flink",
         "pysparkBatch": "pyspark", "sparkBatch": "spark", "sparkSqlBatch": "spark_sql",
         "sparkRBatch": "spark_r", "pysparkNotebookBatch": "pyspark_notebook"}
_READ = {"pyspark": (".py",), "spark_sql": (".sql", ".hql")}  # the code migrate translates


def _spec(container: dict) -> dict:
    """What a job, batch or template step runs, from its type field."""
    key = next((k for k in _KIND if k in container), None)
    if key is None:
        return {"job_type": "unknown"}
    j = container[key]
    spec = {"main_file": (j.get("mainPythonFileUri") or j.get("mainJarFileUri") or j.get("queryFileUri")
                          or j.get("mainRFileUri") or j.get("notebookFileUri")),
            "main_class": j.get("mainClass"),
            "queries": (j.get("queryList") or {}).get("queries"),
            "args": j.get("args"), "python_files": j.get("pythonFileUris"), "jars": j.get("jarFileUris"),
            "files": j.get("fileUris"), "archives": j.get("archiveUris"), "properties": j.get("properties"),
            "variables": j.get("scriptVariables") or j.get("queryVariables") or j.get("params")}
    return {"job_type": _KIND[key], **{k: v for k, v in spec.items() if v not in (None, [], {})}}


def _read_code(client: GcpClient, spec: dict) -> dict:
    """{"code": text} or {"code_not_read": why} for a definition migrate translates; {} otherwise."""
    uri = spec.get("main_file") or ""
    if not uri.lower().endswith(_READ.get(spec["job_type"], ())):
        return {}
    if not uri.startswith("gs://"):
        return {"code_not_read": f"{uri} is not on Cloud Storage"}
    bucket, _, name = uri[5:].partition("/")
    url = f"{GCS}/b/{bucket}/o/{quote(name, safe='')}"
    try:
        size = int(client.get(url).get("size") or 0)
        if size > MAX_CODE_BYTES:
            return {"code_not_read": f"{size} bytes, over the {MAX_CODE_BYTES}-byte limit"}
        return {"code": client.get_text(url, {"alt": "media"})}
    except GcpError as exc:
        return {"code_not_read": str(exc)}


def _submitted(job: dict) -> str | None:
    """When a run was submitted: its first status. Its current status time is when it last changed,
    which for a run killed early comes before an older run's finish (seen live, 2026-10-09)."""
    history = [h.get("stateStartTime") for h in job.get("statusHistory", []) if h.get("stateStartTime")]
    return min(history) if history else job.get("status", {}).get("stateStartTime")


def _digest(value) -> str:
    return hashlib.sha256(repr(value).encode()).hexdigest()[:10]


def _definitions(runs: list[dict], client: GcpClient) -> list[dict]:
    """Runs → one definition per (region, type, what it runs, cluster), newest run's settings."""
    groups: dict[tuple, list[dict]] = {}
    for run in runs:
        what = run.get("main_file") or run.get("main_class") or f"sql-{_digest(run.get('queries'))}"
        groups.setdefault((run["region"], run["job_type"], what, run.get("cluster")), []).append(run)
    out, seen = [], set()
    for (region, _, what, cluster), rs in groups.items():
        rs.sort(key=lambda r: r.get("time") or "")
        last = rs[-1]
        name = what.rstrip("/").rsplit("/", 1)[-1]
        id_ = f"{region}/{cluster or 'serverless'}/{name}"
        if id_ in seen:  # one file run as two job types on one cluster
            id_ = f"{id_}-{_digest((last['job_type'], what))}"
        seen.add(id_)
        d = {k: v for k, v in last.items() if k not in ("run_id", "state", "time")}
        d.update(id=id_, runs=len(rs), last_run=last.get("time"), last_state=last.get("state"),
                 last_run_id=last["run_id"])
        if len({tuple(r.get("args") or ()) for r in rs}) > 1:
            d["args_vary"] = True  # e.g. a date per run: the job takes it as a parameter
        d.update(_read_code(client, d))
        out.append({k: v for k, v in d.items() if v is not None})
    return out


def _cluster(c: dict, region: str) -> dict:
    cfg = c.get("config", {})
    sw, master, workers = cfg.get("softwareConfig", {}), cfg.get("masterConfig", {}), cfg.get("workerConfig", {})
    row = {"name": c["clusterName"], "region": region, "state": c.get("status", {}).get("state"),
           "image_version": sw.get("imageVersion"),
           "master_machine_type": (master.get("machineTypeUri") or "").rsplit("/", 1)[-1] or None,
           "machine_type": (workers.get("machineTypeUri") or "").rsplit("/", 1)[-1] or None,
           "worker_count": workers.get("numInstances"),
           "secondary_worker_count": cfg.get("secondaryWorkerConfig", {}).get("numInstances"),
           "autoscaling_policy": cfg.get("autoscalingConfig", {}).get("policyUri"),
           "properties": sw.get("properties"), "optional_components": sw.get("optionalComponents"),
           "init_actions": [a.get("executableFile") for a in cfg.get("initializationActions", [])]}
    return {k: v for k, v in row.items() if v not in (None, [], {})}


def _template(t: dict, region: str, client: GcpClient) -> dict:
    placement = t.get("placement", {})
    managed = placement.get("managedCluster")
    steps = []
    for job in t.get("jobs", []):
        spec = _spec(job)
        steps.append({"step_id": job.get("stepId"), "prerequisites": job.get("prerequisiteStepIds", []),
                      **spec, **_read_code(client, spec)})
    row = {"id": t["id"], "region": region, "steps": steps,
           "cluster": (managed or {}).get("clusterName"),
           "managed_cluster": _cluster({"clusterName": managed.get("clusterName", "?"),
                                        "config": managed.get("config", {})}, region) if managed else None,
           "cluster_selector": (placement.get("clusterSelector") or {}).get("clusterLabels"),
           "parameters": [p.get("name") for p in t.get("parameters", [])]}
    return {k: v for k, v in row.items() if v not in (None, [], {})}


def _starts(http: dict) -> dict | None:
    """What a scheduler job's HTTP call starts on Dataproc; None if it does not call Dataproc."""
    uri = http.get("uri") or ""
    if "dataproc.googleapis.com" not in uri:
        return None
    if m := _TEMPLATE_URI.search(uri):
        return {"kind": "workflow_template", "region": m[1], "id": m[2]}
    job, batch = _JOB_URI.search(uri), _BATCH_URI.search(uri)
    if not (job or batch):
        return {"kind": "other", "uri": uri}
    try:
        body = json.loads(base64.b64decode(http.get("body") or "") or b"{}")
    except ValueError:
        body = None
    if not isinstance(body, dict):
        return {"kind": "other", "uri": uri, "why": "request body not readable"}
    if job:
        spec = _spec(body.get("job") or {})
        return {"kind": "job", "region": job[1], "job_type": spec["job_type"], "main_file": spec.get("main_file"),
                "cluster": ((body.get("job") or {}).get("placement") or {}).get("clusterName")}
    spec = _spec(body)
    return {"kind": "batch", "region": batch[1], "job_type": spec["job_type"], "main_file": spec.get("main_file")}


def _schedule(j: dict, region: str) -> dict | None:
    starts = _starts(j.get("httpTarget") or {})
    if starts is None:
        return None
    return {"name": j["name"].rsplit("/", 1)[-1], "region": region, "schedule": j.get("schedule"),
            "time_zone": j.get("timeZone") or "UTC", "state": j.get("state"), "target": starts}


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    p = client.project

    def clusters():
        return _per_region(regions, lambda r: [
            _cluster(c, r) for c in client.pages(f"{API}/{p}/regions/{r}/clusters", "clusters")])

    def jobs():
        runs = _per_region(regions, lambda r: [
            {"region": r, "cluster": j.get("placement", {}).get("clusterName"),
             "run_id": j["reference"]["jobId"], "state": j.get("status", {}).get("state"),
             "time": _submitted(j), **_spec(j)}
            for j in client.pages(f"{API}/{p}/regions/{r}/jobs", "jobs")])
        return _definitions(runs, client)

    def batches():
        runs = _per_region(regions, lambda r: [
            {"region": r, "run_id": b["name"].rsplit("/", 1)[-1], "state": b.get("state"),
             "time": b.get("createTime"), "runtime_version": b.get("runtimeConfig", {}).get("version"), **_spec(b)}
            for b in client.pages(f"{API}/{p}/locations/{r}/batches", "batches")])
        return _definitions(runs, client)

    def workflow_templates():
        return _per_region(regions, lambda r: [
            _template(t, r, client)
            for t in client.pages(f"{API}/{p}/regions/{r}/workflowTemplates", "templates")])

    def schedules():
        try:
            return _per_region(regions, lambda r: [
                s for j in client.pages(f"{SCHEDULER}/{p}/locations/{r}/jobs", "jobs") if (s := _schedule(j, r))])
        except GcpError as exc:
            if exc.api_disabled:  # Cloud Scheduler off: nothing is scheduled, and Dataproc itself may be on
                return []
            raise

    return _scan({"clusters": clusters, "jobs": jobs, "batches": batches,
                  "workflow_templates": workflow_templates, "schedules": schedules})
