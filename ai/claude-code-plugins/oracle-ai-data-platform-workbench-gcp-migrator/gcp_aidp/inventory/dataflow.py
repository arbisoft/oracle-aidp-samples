"""Dataflow: jobs, across regions. Inventoried only."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import _scan


def scan(client: GcpClient, **_) -> dict:
    def jobs():
        return [{"id": j["id"], "name": j.get("name", j["id"]), "job_type": j.get("type"),
                 "region": j.get("location"), "state": j.get("currentState")}
                for j in client.pages(f"https://dataflow.googleapis.com/v1b3/projects/{client.project}"
                                      "/jobs:aggregated", "jobs")]
    return _scan({"jobs": jobs})
