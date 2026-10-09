"""Live inventory against canned Google Cloud API responses (no network)."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gcp_aidp.gcp_client import READ_ONLY_SCOPE, GcpClient, GcpError
from gcp_aidp.inventory.manifest import ALL_SOURCES, BROAD_SCOPE_SOURCES, build_manifest
from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan

P = "proj"
BQ = f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}"
DS = f"{BQ}/datasets/migration_test"


def _table(name, type_, **extra):
    return {"tableReference": {"tableId": name}, "type": type_, **extra}


RESPONSES = {
    f"{BQ}/datasets": {"datasets": [{"datasetReference": {"datasetId": "migration_test"}}]},
    DS: {"location": "US", "description": "seed", "access": [
        {"role": "OWNER", "specialGroup": "projectOwners"},
        {"role": "READER", "groupByEmail": "analysts@example.com"},
        {"view": {"projectId": P, "datasetId": "other", "tableId": "v"}}]},
    f"{DS}/tables": {"tables": [{"tableReference": {"tableId": t}} for t in
                                ("orders", "v_simple", "v_legacy", "mv_daily", "ext_files")],
                     "nextPageToken": "page2"},
    f"{DS}/tables?pageToken=page2": {"tables": [{"tableReference": {"tableId": "type_zoo"}}]},
    f"{DS}/tables/orders": _table(
        "orders", "TABLE", numRows="125000", numBytes="9000000",
        schema={"fields": [{"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
                           {"name": "order_date", "type": "DATE"},
                           {"name": "user_id", "type": "INTEGER",
                            "policyTags": {"names": ["projects/proj/taxonomies/1/policyTags/pii"]}}]},
        timePartitioning={"type": "DAY", "field": "order_date"}, clustering={"fields": ["user_id"]},
        expirationTime="1893456000000"),
    f"{DS}/tables/type_zoo": _table("type_zoo", "TABLE", numRows="1", schema={"fields": [
        {"name": "n", "type": "NUMERIC", "precision": "10", "scale": "2"}]},
        rangePartitioning={"field": "n", "range": {"start": "0", "end": "10", "interval": "1"}}),
    f"{DS}/tables/v_simple": _table("v_simple", "VIEW", view={"query": "SELECT 1 AS x"}),
    f"{DS}/tables/v_legacy": _table("v_legacy", "VIEW", view={"query": "SELECT x FROM [proj:ds.t]",
                                                              "useLegacySql": True}),
    f"{DS}/tables/mv_daily": _table("mv_daily", "MATERIALIZED_VIEW",
                                    materializedView={"query": "SELECT 1 AS x", "refreshIntervalMs": "1800000"}),
    f"{DS}/tables/ext_files": _table("ext_files", "EXTERNAL", externalDataConfiguration={
        "sourceFormat": "PARQUET", "sourceUris": ["gs://landing-bucket/x/*.parquet"]}),
    f"{DS}/tables/orders/rowAccessPolicies": {"rowAccessPolicies": [
        {"rowAccessPolicyReference": {"policyId": "us_only"}, "filterPredicate": "country = 'US'"}]},
    f"{DS}/tables/type_zoo/rowAccessPolicies": {},
    f"{DS}/routines": {"routines": [{"routineReference": {"routineId": "net_price"}},
                                    {"routineReference": {"routineId": "parse_utm"}}]},
    f"{DS}/routines/net_price": {"routineType": "SCALAR_FUNCTION", "language": "SQL", "definitionBody": "p * 2",
                                 "arguments": [{"name": "p", "dataType": {"typeKind": "FLOAT64"}}],
                                 "returnType": {"typeKind": "FLOAT64"}},
    f"{DS}/routines/parse_utm": {"routineType": "SCALAR_FUNCTION", "language": "JAVASCRIPT",
                                 "definitionBody": "return 1;",
                                 "arguments": [{"name": "a", "dataType": {"typeKind": "ARRAY",
                                                                         "arrayElementType": {"typeKind": "STRING"}}}]},
    f"{DS}/models": {"models": [{"modelReference": {"modelId": "churn"}, "modelType": "LOGISTIC_REGRESSION"}]},
    f"https://bigquerydatatransfer.googleapis.com/v1/projects/{P}/locations/us/transferConfigs?dataSourceIds=scheduled_query":
        {"transferConfigs": [{"name": f"projects/{P}/locations/us/transferConfigs/abc123",
                              "displayName": "Nightly refresh", "schedule": "every 24 hours",
                              "params": {"query": "SELECT 1"}, "destinationDatasetId": "migration_test"}]},
    f"https://storage.googleapis.com/storage/v1/b?project={P}":
        {"items": [{"name": "landing-bucket", "location": "US", "storageClass": "STANDARD"}]},
    f"https://dataflow.googleapis.com/v1b3/projects/{P}/jobs:aggregated":
        {"jobs": [{"id": "j1", "name": "stream", "type": "JOB_TYPE_STREAMING", "location": "us-central1"}]},
}
DISABLED = ("dataproc.googleapis.com", "dataform.googleapis.com", "aiplatform.googleapis.com")
REFUSED = ("composer.googleapis.com",)


class FakeClient(GcpClient):
    """Serves RESPONSES. Like the real client, it can only GET."""

    def __init__(self):
        super().__init__(P)
        self.calls: list[str] = []

    def get(self, url, params=None):
        key = url + ("?" + "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else "")
        self.calls.append(key)
        if any(host in url for host in DISABLED):
            raise GcpError("HTTP 403 SERVICE_DISABLED", status=403, reason="SERVICE_DISABLED")
        if any(host in url for host in REFUSED):
            raise GcpError("HTTP 403 PERMISSION_DENIED: composer.environments.list", status=403,
                           reason="PERMISSION_DENIED")
        if key not in RESPONSES:
            raise GcpError(f"HTTP 404 notFound: {key}", status=404, reason="notFound")
        return RESPONSES[key]


class RefusingClient(FakeClient):
    """Two datasets; the service account may not read the second."""

    def get(self, url, params=None):
        if url == f"{BQ}/datasets":
            return {"datasets": [{"datasetReference": {"datasetId": d}} for d in ("migration_test", "restricted")]}
        if "/datasets/restricted" in url:
            raise GcpError("HTTP 403 accessDenied", status=403, reason="accessDenied")
        return super().get(url, params)


class OneRefusedDataset(unittest.TestCase):
    def test_costs_that_dataset_not_the_whole_scan(self):
        bq = build_manifest(RefusingClient(), ("bigquery",))["sources"]["bigquery"]
        self.assertNotIn("error", bq["summary"])
        self.assertIn("orders", [t["name"] for t in bq["items"]["tables"]])
        self.assertIn("dataset restricted", bq["summary"]["not_scanned"])


class LiveInventory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = FakeClient()
        cls.manifest = build_manifest(cls.client, ALL_SOURCES, scan_services=True, service_client=cls.client)
        cls.bq = cls.manifest["sources"]["bigquery"]

    def test_every_seeded_object_is_listed(self):
        it = self.bq["items"]
        self.assertEqual([t["name"] for t in it["tables"]], ["orders", "type_zoo"])  # second page followed
        self.assertEqual([v["name"] for v in it["views"]], ["v_simple", "v_legacy"])
        self.assertEqual(it["materialized_views"][0]["refresh_interval_minutes"], 30)
        self.assertEqual(it["external_tables"][0]["source_uris"], ["gs://landing-bucket/x/*.parquet"])
        self.assertEqual({r["name"]: r["language"] for r in it["routines"]},
                         {"net_price": "SQL", "parse_utm": "JAVASCRIPT"})
        self.assertEqual(it["routines"][1]["arguments"][0]["type"], "ARRAY<STRING>")
        self.assertEqual(it["models"][0]["name"], "churn")
        self.assertEqual(it["scheduled_queries"][0]["id"], "abc123")

    def test_table_metadata(self):
        orders = self.bq["items"]["tables"][0]
        self.assertEqual((orders["num_rows"], orders["partitioning"], orders["clustering"]),
                         (125000, {"type": "DAY", "field": "order_date"}, ["user_id"]))
        self.assertEqual(self.bq["items"]["tables"][1]["partitioning"]["type"], "RANGE")

    def test_access_rules(self):
        kinds = sorted(a["kind"] for a in self.bq["items"]["access_policies"])
        self.assertEqual(kinds, ["column_policy_tag", "dataset_iam", "dataset_iam", "dataset_iam",
                                 "row_access_policy"])

    def test_gaps_are_recorded_not_hidden(self):
        self.assertIn("saved_queries", self.bq["summary"]["not_scanned"])
        self.assertIn("notebooks", self.bq["summary"]["not_scanned"])
        sources = self.manifest["sources"]
        self.assertTrue(sources["dataproc"]["summary"]["api_disabled"])
        self.assertEqual(sources["dataproc"]["items"], {"clusters": [], "jobs": []})
        self.assertIn("environments", sources["composer"]["summary"]["not_scanned"])
        # DAGs are found through the environments, so they are not "0": they are unknown.
        self.assertEqual(sources["composer"]["summary"]["not_scanned"]["dags"], "environments were not scanned")
        self.assertNotIn("dags", sources["composer"]["summary"])
        self.assertEqual(sources["gcs"]["items"]["buckets"][0]["name"], "landing-bucket")

    def test_metadata_only(self):
        """No BigQuery call runs a query or reads rows: no jobs, queries or tabledata endpoint."""
        bigquery_calls = [c for c in self.client.calls if "bigquery.googleapis.com" in c]
        self.assertGreater(len(bigquery_calls), 10)
        for call in bigquery_calls:
            self.assertNotRegex(call, r"/(jobs|queries|data)(\?|/|$)")
        self.assertEqual(READ_ONLY_SCOPE, "https://www.googleapis.com/auth/cloud-platform.read-only")
        self.assertFalse(any(hasattr(GcpClient, m) for m in ("post", "put", "patch", "delete")))

    def test_plan_flags_the_gaps_and_migrate_blocks_legacy_sql(self):
        plan = build_plan(self.manifest)
        self.assertIn("bigquery.saved_queries", plan["scan_errors"])
        self.assertIn("composer.environments", plan["scan_errors"])
        with tempfile.TemporaryDirectory() as tmp:
            report = migrate(plan, out_dir=Path(tmp))
        by_id = {r["asset_id"]: r for r in report["results"]}
        legacy = by_id["bigquery.view.migration_test.v_legacy"]
        self.assertEqual((legacy["status"], legacy["findings"][0]["rule"]), ("blocked", "G97_LEGACY_SQL"))
        self.assertEqual(by_id["bigquery.view.migration_test.v_simple"]["status"], "ok")

    def test_services_need_the_flag(self):
        client = FakeClient()
        m = build_manifest(client, ALL_SOURCES)
        for source in BROAD_SCOPE_SOURCES:
            self.assertIn("--scan-services", m["sources"][source]["summary"]["not_scanned"]["all"])
        self.assertFalse(any(host in call for call in client.calls
                             for host in ("dataproc", "composer", "dataform", "dataflow", "aiplatform")))
        self.assertIn("dataproc.all", build_plan(m)["scan_errors"])

    def test_scan_services_uses_a_separate_broader_token(self):
        from gcp_aidp.gcp_client import CLOUD_PLATFORM_SCOPE

        seen = []

        class Recorder(FakeClient):
            def __init__(self, project, scope):
                super().__init__()
                seen.append(scope)

        from unittest import mock
        with mock.patch("gcp_aidp.gcp_client.GcpClient", Recorder):
            build_manifest(FakeClient(), ("dataflow",), scan_services=True)
        self.assertEqual(seen, [CLOUD_PLATFORM_SCOPE])

    def test_saved_queries_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "Top products.sql").write_text("SELECT 1")
            m = build_manifest(FakeClient(), ("bigquery",), saved_queries_dir=tmp)
        bq = m["sources"]["bigquery"]
        self.assertEqual(bq["items"]["saved_queries"], [{"name": "Top products", "query": "SELECT 1"}])
        self.assertNotIn("saved_queries", bq["summary"]["not_scanned"])


if __name__ == "__main__":
    unittest.main()
