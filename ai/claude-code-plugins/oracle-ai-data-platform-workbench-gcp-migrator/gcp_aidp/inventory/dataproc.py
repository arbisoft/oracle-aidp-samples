"""Dataproc: clusters and jobs, per region. Inventoried in 0.1; migrated in 0.2."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import _per_region, _scan


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    base = f"https://dataproc.googleapis.com/v1/projects/{client.project}/regions"

    def clusters():
        return _per_region(regions, lambda r: [
            {"name": c["clusterName"], "region": r,
             "image_version": c.get("config", {}).get("softwareConfig", {}).get("imageVersion"),
             "worker_count": c.get("config", {}).get("workerConfig", {}).get("numInstances")}
            for c in client.pages(f"{base}/{r}/clusters", "clusters")])

    def jobs():
        return _per_region(regions, lambda r: [
            {"id": j["reference"]["jobId"], "region": r, "cluster": j.get("placement", {}).get("clusterName"),
             "job_type": next((k.removesuffix("Job") for k in j if k.endswith("Job")), "unknown")}
            for j in client.pages(f"{base}/{r}/jobs", "jobs")])

    return _scan({"clusters": clusters, "jobs": jobs})
