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
  * OCI checks every file's MD5 on upload.

What is exported is what `migrate` recorded in report.json (`export`).
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import time
from pathlib import Path
from urllib.parse import quote

from gcp_aidp._atomic import write_text_atomic
from gcp_aidp.gcp_client import CLOUD_PLATFORM_SCOPE, READ_ONLY_SCOPE, _error
from gcp_aidp.migrate.runner import IN_PROGRESS_MARKER

BQ = "https://bigquery.googleapis.com/bigquery/v2"
GCS = "https://storage.googleapis.com/storage/v1"
POLL_SECONDS = 5
COPY_WORKERS = 32
JOB_TIMEOUT_SECONDS = 6 * 3600


class ExportError(Exception):
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


def _split(url: str, scheme: str) -> tuple[str, str]:
    """gs://b/p/ → (b, p/); oci://b@ns/p/ → (b, p/)."""
    bucket, _, path = url[len(scheme):].partition("/")
    return bucket.split("@")[0], path


def _session(scope: str, key_file: str | None = None):
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account
    except ImportError as exc:
        raise ExportError("export needs google-auth: pip install -e '.[gcp,oci]'") from exc
    if key_file:
        credentials = service_account.Credentials.from_service_account_file(key_file, scopes=[scope])
    else:
        credentials, _ = google.auth.default(scopes=[scope])
    return AuthorizedSession(credentials)


def _oci_client():
    try:
        import oci
    except ImportError as exc:
        raise ExportError("export needs the OCI SDK: pip install -e '.[gcp,oci]'") from exc
    config = oci.config.from_file(profile_name=os.environ.get("OCI_CLI_PROFILE") or "DEFAULT")
    if os.environ.get("OCI_REGION"):
        config["region"] = os.environ["OCI_REGION"]
    return oci.object_storage.ObjectStorageClient(config, retry_strategy=oci.retry.NoneRetryStrategy())


def _check(response) -> dict:
    if response.status_code >= 400:
        raise _error(response)
    return response.json() if response.content else {}


def _gcs_objects(reader, gcs_url: str) -> list[dict]:
    bucket, prefix = _split(gcs_url, "gs://")
    items, params = [], {"prefix": prefix, "fields": "items(name,size,md5Hash),nextPageToken"}
    while True:
        body = _check(reader.get(f"{GCS}/b/{bucket}/o", params=params, timeout=60))
        items += body.get("items", [])
        if not body.get("nextPageToken"):
            return items
        params["pageToken"] = body["nextPageToken"]


def _oci_md5s(oci_client, namespace: str, oci_url: str) -> dict[str, str]:
    bucket, prefix = _split(oci_url, "oci://")
    out, start = {}, None
    while True:
        page = oci_client.list_objects(namespace, bucket, prefix=prefix, fields="md5,size", start=start, limit=1000).data
        out.update((o.name, o.md5) for o in page.objects)
        start = page.next_start_with
        if not start:
            return out


def _export_tables(writer, reader, spec: dict, log) -> dict[str, dict]:
    """Start one BigQuery job per table not yet exported, then wait for all of them."""
    records, running = {}, {}
    spec_gcs = {f"{t['dataset']}.{t['name']}": t["gcs"] for t in spec["tables"]}
    for t in spec["tables"]:
        key = f"{t['dataset']}.{t['name']}"
        if _gcs_objects(reader, t["gcs"]):
            records[key] = {"export": "already_exported"}
            log(f"  already exported  {key}  ({t['gcs']} holds files; delete them to export again)")
            continue
        job = _check(writer.post(f"{BQ}/projects/{spec['project']}/jobs",
                                 json={"configuration": job_config(spec["project"], t)}, timeout=60))
        ref = job["jobReference"]
        running[key] = ref
        records[key] = {"export": "running", "job": ref["jobId"], "billed_query": bool(t.get("as_text"))}
        log(f"  export started    {key}  job {ref['jobId']}" + ("  (EXPORT DATA)" if t.get("as_text") else ""))
    deadline = time.time() + JOB_TIMEOUT_SECONDS
    while running and time.time() < deadline:
        time.sleep(POLL_SECONDS)
        for key, ref in list(running.items()):
            status = _check(writer.get(f"{BQ}/projects/{ref['projectId']}/jobs/{ref['jobId']}",
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


def _copy_file(reader, oci_client, namespace: str, gcs_bucket: str, oci_bucket: str, obj: dict, oci_name: str):
    url = f"{GCS}/b/{gcs_bucket}/o/{quote(obj['name'], safe='')}"
    last: Exception = RuntimeError("not attempted")
    for _ in range(3):  # each attempt streams the file again from the start
        response = reader.get(url, params={"alt": "media"}, stream=True, timeout=600)
        try:
            if response.status_code >= 400:
                raise _error(response)
            kwargs = {"content_md5": obj["md5Hash"]} if obj.get("md5Hash") else {}
            oci_client.put_object(namespace, oci_bucket, oci_name, response.raw,
                                  content_length=int(obj["size"]), **kwargs)
            return
        except Exception as exc:  # noqa: BLE001 - retried, then reported
            last = exc
        finally:
            response.close()
    raise last


def _copy_table(reader, oci_client, namespace: str, t: dict) -> dict:
    gcs_bucket, gcs_prefix = _split(t["gcs"], "gs://")
    oci_bucket, oci_prefix = _split(t["oci"], "oci://")
    objects = _gcs_objects(reader, t["gcs"])
    have = _oci_md5s(oci_client, namespace, t["oci"])
    todo = [(o, oci_prefix + o["name"][len(gcs_prefix):]) for o in objects]
    todo = [(o, name) for o, name in todo if not (o.get("md5Hash") and have.get(name) == o["md5Hash"])]
    failed = []
    with concurrent.futures.ThreadPoolExecutor(COPY_WORKERS) as pool:
        futures = {pool.submit(_copy_file, reader, oci_client, namespace, gcs_bucket, oci_bucket, o, name): name
                   for o, name in todo}
        for f in concurrent.futures.as_completed(futures):
            if f.exception():
                failed.append(f"{futures[f]}: {f.exception()}")
    return {"files": len(objects), "copied": len(todo) - len(failed), "already_in_oci": len(objects) - len(todo),
            "bytes": sum(int(o["size"]) for o in objects), "copy_failed": failed[:5], "copy_failures": len(failed)}


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
                          "User + Data Viewer, Storage Object Creator on the staging bucket); the migration's "
                          "read-only key is never used to write")
    writer = writer or _session(CLOUD_PLATFORM_SCOPE, key_file)
    reader = reader or _session(READ_ONLY_SCOPE)
    oci_client = oci_client or _oci_client()

    log(f"export: {len(spec['tables'])} table(s) to gs://{spec['bucket']}/{spec['prefix']}/")
    records = _export_tables(writer, reader, spec, log)
    log(f"copy to oci://{spec['oci_bucket']}@{spec['namespace']}/{spec['prefix']}/")
    for t in spec["tables"]:
        key = f"{t['dataset']}.{t['name']}"
        rec = records[key]
        if rec["export"] in ("exported", "already_exported"):
            rec.update(_copy_table(reader, oci_client, spec["namespace"], t))
            log(f"  {'copied' if not rec['copy_failures'] else 'copy FAILED':<17} {key}  {rec['files']} file(s), "
                f"{rec['bytes'] / 1e6:.1f} MB; {rec['copied']} copied, {rec['already_in_oci']} already in OCI"
                + (f", {rec['copy_failures']} failed: {rec['copy_failed'][0]}" if rec["copy_failures"] else ""))
        result["tables"][key] = rec
    path = write_text_atomic(Path(out_dir) / "exports" / "export_report.json", json.dumps(result["tables"], indent=2))
    result["report"] = str(path)
    return result
