"""Dataform: repositories, per region. Migrated in 0.3."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import _per_region, _scan


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    def repositories():
        return _per_region(regions, lambda r: [
            {"name": x["name"].rsplit("/", 1)[-1], "region": r}
            for x in client.pages(f"https://dataform.googleapis.com/v1beta1/projects/{client.project}"
                                  f"/locations/{r}/repositories", "repositories")])
    return _scan({"repositories": repositories})
