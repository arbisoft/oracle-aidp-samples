"""The inventory manifest: one JSON object per scan, one entry per source."""
from __future__ import annotations

import json
import time
from pathlib import Path

from gcp_aidp._atomic import write_text_atomic

# Every source the migrator knows about, migrated or not. A source that is
# only inventoried still appears here so the plan never understates the estate.
ALL_SOURCES = ("bigquery", "gcs", "dataproc", "composer", "dataform", "dataflow", "vertex")
# These APIs refuse a read-only token, so they are scanned only with --scan-services.
BROAD_SCOPE_SOURCES = ("dataproc", "composer", "dataform", "dataflow", "vertex")
NEEDS_SCAN_SERVICES = ("this API refuses a read-only token; re-run with --scan-services "
                       "(and give the service account a viewer role for it) to list it")


def build_manifest(client, sources=ALL_SOURCES, *, regions=("us-central1",),
                   saved_queries_dir: str | None = None, notebooks_dir: str | None = None,
                   scan_services: bool = False,
                   service_client=None, log=None) -> dict:
    """Scan each source; one source failing is recorded and the rest continue.

    `service_client` scans BROAD_SCOPE_SOURCES when `scan_services` is set; it
    defaults to a client with the cloud-platform scope.
    """
    from gcp_aidp.gcp_client import CLOUD_PLATFORM_SCOPE, GcpClient
    from gcp_aidp.inventory import bigquery, composer, dataflow, dataform, dataproc, gcs, vertex

    scanners = {"bigquery": bigquery.scan, "gcs": gcs.scan, "dataproc": dataproc.scan, "composer": composer.scan,
                "dataform": dataform.scan, "dataflow": dataflow.scan, "vertex": vertex.scan}
    data = {}
    for source in sources:
        if source in BROAD_SCOPE_SOURCES and not scan_services:
            data[source] = {"summary": {"not_scanned": {"all": NEEDS_SCAN_SERVICES}}, "items": {}}
            continue
        if log:
            log(f"  scanning {source}...")
        try:
            if source == "bigquery":
                data[source] = bigquery.scan(client, saved_queries_dir=saved_queries_dir,
                                             notebooks_dir=notebooks_dir, log=log)
                if scan_services and not notebooks_dir:  # BigQuery Studio notebooks, through Dataform
                    if service_client is None:
                        service_client = GcpClient(client.project, scope=CLOUD_PLATFORM_SCOPE)
                    _studio_notebooks(data[source], service_client, log)
            elif source in BROAD_SCOPE_SOURCES:
                if service_client is None:
                    service_client = GcpClient(client.project, scope=CLOUD_PLATFORM_SCOPE)
                data[source] = scanners[source](service_client, regions=regions)
            else:
                data[source] = scanners[source](client, regions=regions)
        except Exception as exc:  # recorded in the manifest; the plan shows it at the top
            data[source] = {"summary": {"error": f"{type(exc).__name__}: {exc}"}, "items": {}}
            if log:
                log(f"  {source}: failed: {exc}")
    return {"project_id": client.project, "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "regions": list(regions), "sources_scanned": list(sources),
            "scan_services": scan_services, "sources": data}


def _studio_notebooks(bq: dict, client, log=None) -> None:
    """Fill the BigQuery source's notebooks from the Dataform API, or say why not."""
    from gcp_aidp.gcp_client import GcpError
    from gcp_aidp.inventory import studio_notebooks
    summary = bq["summary"]
    try:
        notebooks, warnings = studio_notebooks.scan(client)
    except GcpError as exc:
        summary.setdefault("not_scanned", {})["notebooks"] = (f"Dataform API refused ({exc}): grant Dataform "
                                                               "Viewer, or export them and pass --notebooks-dir")
        return
    bq["items"]["notebooks"] = notebooks
    summary["notebooks"] = len(notebooks)
    (summary.get("not_scanned") or {}).pop("notebooks", None)
    if not summary.get("not_scanned"):
        summary.pop("not_scanned", None)
    if warnings:
        summary.setdefault("warnings", []).extend(warnings)
    if log:
        log(f"    {len(notebooks)} BigQuery Studio notebook(s) read through Dataform")


def write_manifest(manifest: dict, out_path: str | Path) -> Path:
    return write_text_atomic(out_path, json.dumps(manifest, indent=2, default=str))


def summarize(manifest: dict) -> str:
    """One-screen summary string for the user."""
    lines = [
        f"GCP project: {manifest.get('project_id')}",
        f"scanned at:  {manifest.get('scanned_at')}",
        "",
    ]
    for src, data in manifest.get("sources", {}).items():
        s = data.get("summary", {})
        if "error" in s:
            lines.append(f"  {src:10s}  ERROR: {s['error']}")
            continue
        counts = ", ".join(f"{k}={v}" for k, v in s.items() if isinstance(v, int) and not isinstance(v, bool))
        lines.append(f"  {src:10s}  {counts}" + ("  (API not enabled: none)" if s.get("api_disabled") else ""))
        for what, why in (s.get("not_scanned") or {}).items():
            lines.append(f"  {'':10s}  ! {what} not scanned: {why}")
        for w in s.get("warnings") or []:
            lines.append(f"  {'':10s}  ! {w}")
    return "\n".join(lines)
