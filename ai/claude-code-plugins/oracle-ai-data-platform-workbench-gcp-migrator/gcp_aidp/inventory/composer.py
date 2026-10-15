"""Cloud Composer: environments, and the DAG files in each one's bucket. Migrated in 0.3."""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient
from gcp_aidp.inventory._common import GCS, _scan


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    envs: list[dict] = []

    def environments():
        for r in regions:
            for e in client.pages(f"https://composer.googleapis.com/v1/projects/{client.project}"
                                  f"/locations/{r}/environments", "environments"):
                envs.append({"name": e["name"].rsplit("/", 1)[-1], "region": r,
                             "image_version": e.get("config", {}).get("softwareConfig", {}).get("imageVersion"),
                             "dag_prefix": e.get("config", {}).get("dagGcsPrefix")})
        return envs

    def dags():
        # DAG files listed from the environment's bucket (object metadata only).
        # ponytail: one DAG per .py file is assumed; a file defining several DAGs is counted once.
        out = []
        for e in envs:
            prefix = e.get("dag_prefix") or ""
            if not prefix.startswith("gs://"):
                continue
            bucket, _, path = prefix[5:].partition("/")
            for o in client.pages(f"{GCS}/b/{bucket}/o", "items", {"prefix": path.rstrip("/") + "/"}):
                if o["name"].endswith(".py"):
                    out.append({"environment": e["name"], "dag_id": o["name"].rsplit("/", 1)[-1][:-3],
                                "file": f"gs://{bucket}/{o['name']}"})
        return out

    return _scan({"environments": environments, "dags": dags}, depends={"dags": "environments"})
