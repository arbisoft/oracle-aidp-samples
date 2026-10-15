"""BigQuery Studio notebooks, saved queries and pipelines, read through the Dataform API (GET calls only).

Each is a Dataform repository. A notebook is labelled `single-file-asset-type: notebook`
and holds one file, `content.ipynb` (seen live, 2026-10-09); a saved query is labelled
`single-file-asset-type: sql` and holds `content.sql`; a pipeline is labelled
`bigquery-workflow` and is listed by name only (both seen live, 2026-10-13). It lands in whatever region BigQuery Studio chose, which
need not be the project's default code region, so every Dataform location is listed.
Cell sources are kept; outputs, which can hold query results, are dropped. The
Dataform API refuses a read-only token, so this runs with --scan-services and needs
Dataform Viewer.
"""
from __future__ import annotations

import base64
import json

from gcp_aidp.gcp_client import GcpClient

API = "https://dataform.googleapis.com/v1/projects"
ASSET_LABEL = "single-file-asset-type"
PIPELINE_LABEL = "bigquery-workflow"


def cells(raw: bytes | str) -> list[dict]:
    nb = json.loads(raw)
    return [{"cell_type": c["cell_type"],
             "source": "".join(c["source"]) if isinstance(c.get("source"), list) else c.get("source", "")}
            for c in nb["cells"] if c.get("cell_type") in ("code", "markdown")]


def scan(client: GcpClient) -> tuple[dict[str, list[dict]], list[str]]:
    """({"notebooks", "saved_queries", "pipelines"}, warnings). Any other single-file asset is named
    in a warning. One file that cannot be read is a warning; failing to list raises."""
    notebooks, saved_queries, pipelines, warnings = [], [], [], []
    for loc in client.pages(f"{API}/{client.project}/locations", "locations"):
        base = f"{API}/{client.project}/locations/{loc['locationId']}/repositories"
        for repo in client.pages(base, "repositories"):
            labels = repo.get("labels") or {}
            kind = labels.get(ASSET_LABEL)
            rid = repo["name"].rsplit("/", 1)[-1]
            name = repo.get("displayName") or rid
            if PIPELINE_LABEL in labels:
                pipelines.append({"id": rid, "name": name, "region": loc["locationId"]})
                continue
            if kind is None:
                continue  # a Dataform repository proper: the dataform source lists it
            if kind == "sql":
                try:
                    body = client.get(f"{base}/{rid}:readFile", {"path": "content.sql"})
                    saved_queries.append({"name": name,
                                          "query": base64.b64decode(body.get("contents", "")).decode("utf-8")})
                except Exception as exc:  # noqa: BLE001 - a timeout too: one asset, not the scan
                    warnings.append(f"saved query {name} not read: {type(exc).__name__}: {exc}")
                continue
            if kind != "notebook":
                warnings.append(f"BigQuery Studio {kind} asset {name} "
                                f"({loc['locationId']}) is not read")
                continue
            try:
                body = client.get(f"{base}/{rid}:readFile", {"path": "content.ipynb"})
                notebook_cells = cells(base64.b64decode(body.get("contents", "")))
            except Exception as exc:  # noqa: BLE001 - a timeout too: one asset, not the scan
                warnings.append(f"notebook {name} not read: {type(exc).__name__}: {exc}")
                continue
            notebooks.append({"id": rid, "name": name, "region": loc["locationId"],
                              "cells": notebook_cells})
    return {"notebooks": notebooks, "saved_queries": saved_queries, "pipelines": pipelines}, warnings
