"""Cloud Storage: buckets."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import GCS, _scan


def scan(client: GcpClient, **_) -> dict:
    def buckets():
        return [{"name": b["name"], "location": b.get("location"), "storage_class": b.get("storageClass")}
                for b in client.pages(f"{GCS}/b", "items", {"project": client.project})]
    return _scan({"buckets": buckets})
