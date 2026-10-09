"""Vertex AI: models, endpoints and pipelines, per region. Inventoried only."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import _per_region, _scan


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    def collection(path: str, key: str):
        def fetch():
            return _per_region(regions, lambda r: [
                {"id": x["name"].rsplit("/", 1)[-1], "name": x.get("displayName") or x["name"], "region": r}
                for x in client.pages(f"https://{r}-aiplatform.googleapis.com/v1/projects/{client.project}"
                                      f"/locations/{r}/{path}", key)])
        return fetch
    return _scan({"models": collection("models", "models"),
                  "endpoints": collection("endpoints", "endpoints"),
                  "pipelines": collection("pipelineJobs", "pipelineJobs")})
