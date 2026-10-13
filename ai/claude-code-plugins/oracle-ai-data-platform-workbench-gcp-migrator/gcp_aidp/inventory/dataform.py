"""Dataform: repositories, per region. Migrated in 0.3."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import _per_region, _scan


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    def repositories():
        return _per_region(regions, lambda r: [
            {"name": x["name"].rsplit("/", 1)[-1], "region": r}
            for x in client.pages(f"https://dataform.googleapis.com/v1beta1/projects/{client.project}"
                                  f"/locations/{r}/repositories", "repositories")
            # BigQuery Studio notebooks, saved queries and pipelines are Dataform repositories too;
            # the BigQuery source reads them, so they are not counted here.
            if not {"single-file-asset-type", "bigquery-workflow"} & set(x.get("labels") or {})])
    return _scan({"repositories": repositories})
