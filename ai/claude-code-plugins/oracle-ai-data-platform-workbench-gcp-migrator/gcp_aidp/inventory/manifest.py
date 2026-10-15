"""The inventory manifest: one JSON object per scan, one entry per source."""
from __future__ import annotations

import json
import time
from pathlib import Path

from gcp_aidp._atomic import write_text_atomic

# Every source the migrator knows about, migrated or not. A source that is
# only inventoried still appears here so the plan never understates the estate.
ALL_SOURCES = ("bigquery", "gcs", "dataproc", "dataform")
# These APIs refuse a read-only token, so they are scanned only with --scan-services.
BROAD_SCOPE_SOURCES = ("dataproc", "dataform")
NEEDS_SCAN_SERVICES = ("this API refuses a read-only token; re-run with --scan-services "
                       "(and give the service account a viewer role for it) to list it")


DEFAULT_REGIONS = ("us-central1",)


def build_manifest(client, sources=ALL_SOURCES, *, regions=None,
                   saved_queries_dir: str | None = None, notebooks_dir: str | None = None,
                   scan_services: bool = False,
                   service_client=None, log=None) -> dict:
    """Scan each source; one source failing is recorded and the rest continue.

    `service_client` scans BROAD_SCOPE_SOURCES when `scan_services` is set; it
    defaults to a client with the cloud-platform scope.
    """
    from gcp_aidp.gcp_client import CLOUD_PLATFORM_SCOPE, GcpClient
    from gcp_aidp.inventory import (
        bigquery,
        dataform,
        dataproc,
        gcs,
    )

    defaulted, regions = regions is None, tuple(regions or DEFAULT_REGIONS)
    scanners = {"bigquery": bigquery.scan, "gcs": gcs.scan, "dataproc": dataproc.scan, "dataform": dataform.scan}
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
                if defaulted:  # nothing says where a project's jobs live: an empty answer must not read as "none"
                    data[source]["summary"].setdefault("warnings", []).append(
                        f"scanned in {', '.join(regions)} only (the default); resources in other regions are "
                        "not listed: pass --regions")
            else:
                data[source] = scanners[source](client, regions=regions)
        except Exception as exc:  # noqa: BLE001 - recorded in the manifest; the plan shows it at the top
            data[source] = {"summary": {"error": f"{type(exc).__name__}: {exc}"}, "items": {}}
            if log:
                log(f"  {source}: failed: {exc}")
    return {"project_id": client.project, "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "regions": list(regions), "sources_scanned": list(sources),
            "scan_services": scan_services, "sources": data}


STUDIO_ASSETS = ("notebooks", "saved_queries", "pipelines")


def _studio_notebooks(bq: dict, client, log=None) -> None:
    """Fill the BigQuery source's notebooks, saved queries and pipelines from the Dataform API.

    A failure here marks those three not scanned; the rest of the BigQuery scan is kept."""
    from gcp_aidp.inventory import studio_notebooks
    summary = bq["summary"]
    try:
        assets, warnings = studio_notebooks.scan(client)
    except Exception as exc:  # noqa: BLE001 - a refusal or a timeout
        not_scanned = summary.setdefault("not_scanned", {})
        for what in STUDIO_ASSETS:
            if what == "saved_queries" and bq["items"]["saved_queries"]:
                continue  # read from --saved-queries-dir
            not_scanned[what] = (f"Dataform API failed ({type(exc).__name__}: {exc}): grant Dataform Viewer or "
                                 "run again; or export notebooks and saved queries and pass --notebooks-dir "
                                 "and --saved-queries-dir")
        return
    # A query also given in --saved-queries-dir keeps that copy.
    known = {q["name"] for q in bq["items"]["saved_queries"]}
    assets["saved_queries"] = bq["items"]["saved_queries"] + [q for q in assets["saved_queries"]
                                                             if q["name"] not in known]
    for what in STUDIO_ASSETS:
        bq["items"][what] = assets[what]
        summary[what] = len(assets[what])
        (summary.get("not_scanned") or {}).pop(what, None)
    if not summary.get("not_scanned"):
        summary.pop("not_scanned", None)
    if warnings:
        summary.setdefault("warnings", []).extend(warnings)
    if log:
        names = {"notebooks": ("notebook", "notebooks"), "saved_queries": ("saved query", "saved queries"),
                 "pipelines": ("pipeline", "pipelines")}
        log("    read through Dataform: " + ", ".join(f"{len(assets[w])} {names[w][len(assets[w]) != 1]}"
                                                     for w in STUDIO_ASSETS))


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
