"""`gcp-aidp export` against fake BigQuery, Cloud Storage and OCI endpoints."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import gcp_aidp.export as export_mod
from gcp_aidp.export import ExportError, export
from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan

C = lambda name, type_: {"name": name, "type": type_, "mode": "NULLABLE"}  # noqa: E731
MANIFEST = {"project_id": "proj-1", "sources": {"bigquery": {"items": {
    "datasets": [{"name": "shop"}],
    "tables": [{"dataset": "shop", "name": "orders", "num_bytes": 10, "columns": [C("id", "INTEGER")]},
               {"dataset": "shop", "name": "events", "num_bytes": 10,
                "columns": [C("id", "INTEGER"), C("payload", "JSON")]}],
}}}}


def md5(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data).digest()).decode()


class Response:
    def __init__(self, body=None, status=200, raw=b""):
        self.status_code, self._body, self.raw = status, body, io.BytesIO(raw)
        self.content = json.dumps(body).encode() if body is not None else b""
        self.text = self.content.decode()

    def json(self):
        return self._body

    def close(self):
        pass


class FakeGoogle:
    """BigQuery jobs (writer) and Cloud Storage reads (reader) over one in-memory bucket."""

    def __init__(self, fail_tables=()):
        self.objects: dict[str, bytes] = {}  # "bucket/name" → content
        self.jobs: dict[str, dict] = {}
        self.posted: list[dict] = []
        self.fail_tables = set(fail_tables)

    def post(self, url, json=None, timeout=None):
        job_id = f"job{len(self.jobs)}"
        self.jobs[job_id] = json["configuration"]
        self.posted.append(json["configuration"])
        return Response({"jobReference": {"projectId": "proj-1", "jobId": job_id, "location": "US"}})

    def get(self, url, params=None, timeout=None, stream=False):
        if "/jobs/" in url:
            config = self.jobs[url.rsplit("/", 1)[1]]
            table = (config.get("extract", {}).get("sourceTable", {}).get("tableId")
                     or config["query"]["query"].rsplit(".", 1)[1].strip("`"))
            if table in self.fail_tables:
                return Response({"status": {"state": "DONE", "errorResult": {"message": "Access Denied"}}})
            uri = (config.get("extract", {}).get("destinationUris") or
                   [config.get("query", {}).get("query", "").split("uri='")[1].split("'")[0]])[0]
            folder = uri[len("gs://"):].rsplit("/", 1)[0]
            for n in (0, 1):
                self.objects[f"{folder}/part-{n:012d}.parquet"] = f"{table}-{n}".encode()
            return Response({"status": {"state": "DONE"}})
        bucket = url.split("/b/")[1].split("/")[0]
        if params and params.get("alt") == "media":
            from urllib.parse import unquote
            return Response(raw=self.objects[f"{bucket}/{unquote(url.rsplit('/o/', 1)[1])}"])
        prefix = f"{bucket}/{params['prefix']}"
        items = [{"name": k[len(bucket) + 1:], "size": str(len(v)), "md5Hash": md5(v)}
                 for k, v in sorted(self.objects.items()) if k.startswith(prefix)]
        return Response({"items": items} if items else {})


class FakeOci:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}  # name → (content, md5)
        self.puts: list[str] = []

    def list_objects(self, namespace, bucket, prefix, fields, start, limit):
        objs = [SimpleNamespace(name=n, md5=m) for n, (_, m) in sorted(self.objects.items()) if n.startswith(prefix)]
        return SimpleNamespace(data=SimpleNamespace(objects=objs, next_start_with=None))

    def put_object(self, namespace, bucket, name, body, content_length, content_md5=None):
        data = body.read()
        assert len(data) == content_length and md5(data) == content_md5  # what OCI checks on upload
        self.objects[name] = (data, content_md5)
        self.puts.append(name)


class Export(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "migrated"
        self.plan = build_plan(MANIFEST, oci_namespace="ns", export_over_gb=0, export_bucket="stage-bkt")
        migrate(self.plan, out_dir=self.out)
        self.prefix = self.plan["export"]["prefix"]
        mock.patch.object(export_mod, "POLL_SECONDS", 0).start()
        self.addCleanup(mock.patch.stopall)
        self.addCleanup(self.tmp.cleanup)

    def run_export(self, google, oci, **kw):
        return export(self.out, apply=True, writer=google, reader=google, oci_client=oci, log=lambda line: None, **kw)

    def test_dry_run_sends_nothing(self):
        result = export(self.out)
        self.assertFalse(result["applied"])
        self.assertEqual(sorted(t["name"] for t in result["spec"]["tables"]), ["events", "orders"])

    def test_apply_needs_the_export_key(self):
        with mock.patch.dict("os.environ", {"GCP_EXPORT_CREDENTIALS": ""}), self.assertRaises(ExportError):
            export(self.out, apply=True)

    def test_exports_then_copies_with_md5_and_resumes(self):
        google, oci = FakeGoogle(), FakeOci()
        # One file of orders is already in OCI from an earlier run.
        oci.objects[f"{self.prefix}/shop/orders/part-000000000000.parquet"] = (b"orders-0", md5(b"orders-0"))
        result = self.run_export(google, oci)

        kinds = {next(iter(c)) for c in google.posted}
        self.assertEqual(kinds, {"extract", "query"})  # orders: free extract; events (JSON): EXPORT DATA
        query = next(c["query"]["query"] for c in google.posted if "query" in c)
        self.assertIn("TO_JSON_STRING(`payload`) AS `payload`", query)
        self.assertEqual(result["tables"]["shop.orders"]["export"], "exported")
        self.assertEqual((result["tables"]["shop.orders"]["copied"], result["tables"]["shop.orders"]["already_in_oci"]),
                         (1, 1))
        self.assertEqual(result["tables"]["shop.events"]["copied"], 2)
        self.assertEqual(oci.objects[f"{self.prefix}/shop/events/part-000000000001.parquet"][0], b"events-1")
        self.assertTrue(Path(result["report"]).exists())

        # A rerun exports nothing again and copies nothing again.
        google.posted.clear()
        oci.puts.clear()
        again = self.run_export(google, oci)
        self.assertEqual(google.posted, [])
        self.assertEqual(oci.puts, [])
        self.assertEqual({r["export"] for r in again["tables"].values()}, {"already_exported"})

    def test_a_failed_export_is_reported_and_not_copied(self):
        google, oci = FakeGoogle(fail_tables={"events"}), FakeOci()
        result = self.run_export(google, oci)
        self.assertEqual(result["tables"]["shop.events"]["export"], "failed")
        self.assertIn("Access Denied", result["tables"]["shop.events"]["reason"])
        self.assertIn("delete gs://", result["tables"]["shop.events"]["reason"])  # a re-run would skip it
        self.assertNotIn("copied", result["tables"]["shop.events"])
        self.assertFalse(any("/events/" in n for n in oci.objects))
        self.assertEqual(result["tables"]["shop.orders"]["copied"], 2)


if __name__ == "__main__":
    unittest.main()
