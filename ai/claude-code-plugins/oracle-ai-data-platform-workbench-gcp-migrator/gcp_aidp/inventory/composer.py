"""Cloud Composer: environments, and the DAG files in each one's bucket. Migrated in 0.3.

GET only. A DAG file is read as text (Cloud Storage object read, `alt=media`) and
never run. The manifest therefore holds DAG source code: treat it as sensitive.
"""
from __future__ import annotations

from urllib.parse import quote

from gcp_aidp.gcp_client import GcpClient, GcpError
from gcp_aidp.inventory._common import GCS, _scan

MAX_DAG_BYTES = 1_000_000


def _skipped(name: str) -> bool:
    parts = name.split("/")
    return not name.endswith(".py") or parts[-1] == "airflow_monitoring.py" or "__pycache__" in parts


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

    not_scanned: dict[str, str] = {}

    def read(bucket: str, o: dict) -> tuple[str, str]:
        """(code, why it was not read). A file that cannot be read is kept, not dropped."""
        size = int(o.get("size") or 0)
        if size > MAX_DAG_BYTES:
            return "", f"{size} bytes is over the {MAX_DAG_BYTES} byte limit"
        try:
            text = client.get_text(f"{GCS}/b/{bucket}/o/{quote(o['name'], safe='')}", {"alt": "media"})
        except GcpError as exc:
            return "", str(exc)
        if len(text.encode("utf-8")) > MAX_DAG_BYTES:
            return "", f"the file is over the {MAX_DAG_BYTES} byte limit"
        return text, ""

    def dags():
        # ponytail: one DAG per .py file is assumed; a file defining several DAGs is counted once.
        out = []
        for e in envs:
            prefix = e.get("dag_prefix") or ""
            if not prefix.startswith("gs://"):
                continue
            bucket, _, path = prefix[5:].partition("/")
            for o in client.pages(f"{GCS}/b/{bucket}/o", "items", {"prefix": path.rstrip("/") + "/"}):
                if _skipped(o["name"]):
                    continue
                code, why = read(bucket, o)
                item = {"environment": e["name"], "dag_id": o["name"].rsplit("/", 1)[-1][:-3],
                        "file": f"gs://{bucket}/{o['name']}", "code": code}
                if why:
                    item["code_not_scanned"] = why
                    not_scanned[f"code of {e['name']}/{o['name']}"] = why
                out.append(item)
        return out

    out = _scan({"environments": environments, "dags": dags}, depends={"dags": "environments"})
    if not_scanned:
        out["summary"].setdefault("not_scanned", {}).update(not_scanned)
    return out
