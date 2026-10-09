"""BigQuery Studio notebooks, read through the Dataform API (GET calls only).

A notebook is a Dataform repository labelled `single-file-asset-type: notebook`,
holding one file, `content.ipynb` (seen live, 2026-10-09). It lands in whatever
region BigQuery Studio chose, which need not be the project's default code region,
so every Dataform location is listed. Cell sources are kept; outputs, which can
hold query results, are dropped. The Dataform API refuses a read-only token, so
this runs with --scan-services and needs Dataform Viewer.
"""
from __future__ import annotations

import base64
import json

from gcp_aidp.gcp_client import GcpClient, GcpError

API = "https://dataform.googleapis.com/v1/projects"
ASSET_LABEL = "single-file-asset-type"


def cells(raw: bytes | str) -> list[dict]:
    nb = json.loads(raw)
    return [{"cell_type": c["cell_type"],
             "source": "".join(c["source"]) if isinstance(c.get("source"), list) else c.get("source", "")}
            for c in nb["cells"] if c.get("cell_type") in ("code", "markdown")]


def scan(client: GcpClient) -> tuple[list[dict], list[str]]:
    """(notebooks, warnings). Any other single-file asset is named in a warning, not dropped silently."""
    notebooks, warnings = [], []
    for loc in client.pages(f"{API}/{client.project}/locations", "locations"):
        base = f"{API}/{client.project}/locations/{loc['locationId']}/repositories"
        for repo in client.pages(base, "repositories"):
            kind = (repo.get("labels") or {}).get(ASSET_LABEL)
            rid = repo["name"].rsplit("/", 1)[-1]
            if kind is None:
                continue  # a Dataform repository proper: the dataform source lists it
            if kind != "notebook":
                warnings.append(f"BigQuery Studio {kind} asset {repo.get('displayName') or rid} "
                                f"({loc['locationId']}) is not read")
                continue
            try:
                body = client.get(f"{base}/{rid}:readFile", {"path": "content.ipynb"})
                notebook_cells = cells(base64.b64decode(body.get("contents", "")))
            except (GcpError, ValueError, KeyError, TypeError) as exc:
                warnings.append(f"notebook {repo.get('displayName') or rid} not read: {type(exc).__name__}: {exc}")
                continue
            notebooks.append({"id": rid, "name": repo.get("displayName") or rid, "region": loc["locationId"],
                              "cells": notebook_cells})
    return notebooks, warnings
