"""Run the export route (`export`): large BigQuery tables → Parquet in Cloud Storage → OCI.

The rules, as for `publish`:

  * Dry run by default; `--apply` is the only thing that sends anything.
  * Two Google identities. The export (BigQuery jobs that write to the staging
    bucket) uses the key in GCP_EXPORT_CREDENTIALS, never the migration's
    read-only key. Reading the files back for the copy uses the read-only one
    (GOOGLE_APPLICATION_CREDENTIALS). OCI writes use ~/.oci/config.
  * Never overwrite. A table whose folder already holds files is not exported
    again; a file already in OCI with the same MD5 is not copied again. A rerun
    resumes where the last one stopped.
  * OCI checks every file's MD5 on upload. The copy is `_copy`, shared with `transfer`.

What is exported is what `migrate` recorded in report.json (`export`).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from gcp_aidp import _copy
from gcp_aidp._atomic import write_text_atomic
from gcp_aidp._copy import CopyError, check, gcs_objects
from gcp_aidp.gcp_client import CLOUD_PLATFORM_SCOPE, READ_ONLY_SCOPE
from gcp_aidp.migrate.runner import IN_PROGRESS_MARKER

BQ = "https://bigquery.googleapis.com/bigquery/v2"
POLL_SECONDS = 5
JOB_TIMEOUT_SECONDS = 6 * 3600


class ExportError(CopyError):
    """The export cannot run as it stands; nothing was sent."""


def load(out_dir: str | Path) -> dict:
    out = Path(out_dir)
    if (out / IN_PROGRESS_MARKER).exists():
        raise ExportError(f"{out}: migrate did not finish; run it again first")
    try:
        report = json.loads((out / "report.json").read_text())
    except (OSError, ValueError) as exc:
        raise ExportError(f"{out}/report.json: {exc}") from exc
    spec = report.get("export")
    if not spec or not spec.get("tables"):
        raise ExportError("no table takes the export route in this migration (plan --export-over-gb)")
    return spec


def job_config(project: str, t: dict) -> dict:
    """The BigQuery job for one table: a free extract, or an EXPORT DATA query that
    turns the columns Parquet cannot carry into text (billed by bytes scanned)."""
    uri = t["gcs"] + "part-*.parquet"
    if not t.get("as_text"):
        return {"extract": {"sourceTable": {"projectId": project, "datasetId": t["dataset"], "tableId": t["name"]},
                            "destinationUris": [uri], "destinationFormat": "PARQUET", "compression": "SNAPPY"}}
    replace = ", ".join(f"{expr} AS `{name}`" for name, expr in t["as_text"].items())
    sql = (f"EXPORT DATA OPTIONS (uri='{uri}', format='PARQUET', compression='SNAPPY') AS "
           f"SELECT * REPLACE ({replace}) FROM `{project}.{t['dataset']}.{t['name']}`")
    return {"query": {"query": sql, "useLegacySql": False}}


def _export_tables(writer, reader, spec: dict, log) -> dict[str, dict]:
    """Start one BigQuery job per table not yet exported, then wait for all of them."""
    records, running = {}, {}
    spec_gcs = {f"{t['dataset']}.{t['name']}": t["gcs"] for t in spec["tables"]}
    for t in spec["tables"]:
        key = f"{t['dataset']}.{t['name']}"
        if gcs_objects(reader, t["gcs"]):
            records[key] = {"export": "already_exported"}
            log(f"  already exported  {key}  ({t['gcs']} holds files; delete them to export again)")
            continue
        job = check(writer.post(f"{BQ}/projects/{spec['project']}/jobs",
                                 json={"configuration": job_config(spec["project"], t)}, timeout=60))
        ref = job["jobReference"]
        running[key] = ref
        records[key] = {"export": "running", "job": ref["jobId"], "billed_query": bool(t.get("as_text"))}
        log(f"  export started    {key}  job {ref['jobId']}" + ("  (EXPORT DATA)" if t.get("as_text") else ""))
    deadline = time.time() + JOB_TIMEOUT_SECONDS
    while running and time.time() < deadline:
        time.sleep(POLL_SECONDS)
        for key, ref in list(running.items()):
            status = check(writer.get(f"{BQ}/projects/{ref['projectId']}/jobs/{ref['jobId']}",
                                       params={"location": ref.get("location")}, timeout=60))["status"]
            if status["state"] != "DONE":
                continue
            del running[key]
            if status.get("errorResult"):
                records[key].update(export="failed", reason=status["errorResult"].get("message", "")
                                    + f"; delete {spec_gcs[key]} before running export again: it may hold "
                                      "part of the files")
                log(f"  export FAILED     {key}  {records[key]['reason']}")
            else:
                records[key]["export"] = "exported"
                log(f"  exported          {key}")
    for key, ref in running.items():  # a re-run now would copy a folder BigQuery is still writing
        records[key].update(export="failed", reason=f"BigQuery job {ref['jobId']} still running at the time "
                                                    "limit; run export again once it has finished")
    return records


def export(out_dir: str | Path, *, apply: bool = False, writer=None, reader=None, oci_client=None,
           log=print) -> dict:
    """Dry run: what would be exported and copied. With apply: export, then copy to OCI."""
    spec = load(out_dir)
    result = {"spec": spec, "applied": apply, "tables": {}}
    if not apply:
        return result
    key_file = os.environ.get("GCP_EXPORT_CREDENTIALS")
    if writer is None and not key_file:
        raise ExportError("set GCP_EXPORT_CREDENTIALS to the export service account's key file (BigQuery Job "
                          "User + Data Viewer, Storage Object Creator + Viewer on the staging bucket); the migration's "
                          "read-only key is never used to write")
    writer = writer or _copy.session(CLOUD_PLATFORM_SCOPE, key_file)
    reader = reader or _copy.session(READ_ONLY_SCOPE)
    oci_client = oci_client or _copy.oci_client()

    log(f"export: {len(spec['tables'])} table(s) to gs://{spec['bucket']}/{spec['prefix']}/")
    records = _export_tables(writer, reader, spec, log)
    log(f"copy to oci://{spec['oci_bucket']}@{spec['namespace']}/{spec['prefix']}/")
    for t in spec["tables"]:
        key = f"{t['dataset']}.{t['name']}"
        rec = records[key]
        if rec["export"] in ("exported", "already_exported"):
            rec.update(_copy.copy(reader, oci_client, spec["namespace"], t["gcs"], t["oci"], key, log))
            log(_copy.copied_line(key, rec))
        result["tables"][key] = rec
    path = write_text_atomic(Path(out_dir) / "exports" / "export_report.json", json.dumps(result["tables"], indent=2))
    result["report"] = str(path)
    return result
