"""migrate --demo labels and verify's fail-closed checks."""
from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.migrate import IN_PROGRESS_MARKER, migrate
from gcp_aidp.plan import build_plan
from gcp_aidp.verify import verify

ROOT = Path(__file__).parents[1]
DEMO = json.loads((ROOT / "gcp_aidp/fixtures/demo-manifest.json").read_text())

# The verdict the demo must give each asset that a rule is written for.
EXPECTED = {
    "bigquery.table.sales.orders": "PASS",
    "bigquery.table.sales.customers": "PASS",
    "bigquery.table.sales.returns": "REVIEW",           # ingestion-time partitioning
    "bigquery.table.sales.store_visits": "REVIEW",      # blocked: GEOGRAPHY, BIGNUMERIC, INTERVAL, RANGE
    "bigquery.table.finance.fx_rates": "REVIEW",        # blocked: BIGNUMERIC
    "bigquery.view.sales.v_active_customers": "PASS",
    "bigquery.view.marketing.v_campaign_days": "PASS",
    "bigquery.view.sales.v_order_kpis": "REVIEW",       # caveats
    "bigquery.view.sales.v_latest_order_per_customer": "PASS",  # QUALIFY → subquery
    "bigquery.view.logs.v_all_app_events": "REVIEW",    # blocked: wildcard
    "bigquery.materialized_view.sales.mv_daily_sales": "PASS",
    "bigquery.routine.sales.net_price": "REVIEW",       # no SQL UDFs on Spark 3.5
    "bigquery.routine.finance.close_month": "SKIP",     # reported
    "bigquery.saved_query.Churn_scoring": "REVIEW",     # blocked: ML.PREDICT
    "bigquery.scheduled_query.6512f0a1-0000-2b8e-a1d4-001a11440002": "REVIEW",  # J02: destination table
    "gcs.bucket.northwind-landing": "REVIEW",           # transfer prerequisites
    "dataproc.cluster.etl-nightly": "SKIP",
    "vertex.model.vm-001": "SKIP",
}


class Demo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        cls.report = migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=cls.out)
        cls.result = verify(cls.out / "report.json")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_verdicts(self):
        got = {r["asset_id"]: r["verdict"] for r in self.result["rows"]}
        for aid, verdict in EXPECTED.items():
            self.assertEqual(got[aid], verdict, aid)
        self.assertEqual(self.result["summary"]["FAIL"], 0)
        self.assertEqual(sum(self.result["summary"].values()), len(self.report["results"]))

    def test_blocked_artifacts_are_not_runnable(self):
        for r in self.report["results"]:
            if r["status"] == "blocked" and r["kind"] != "bq_table":
                body = (self.out / r["output_path"]).read_text()
                self.assertTrue(all(l.startswith("--") or not l.strip() for l in body.splitlines()), r["asset_id"])

    def test_J01_UNSCHEDULED_J02_DESTINATION_TABLE_and_M01_MATERIALIZED_VIEW_are_recorded(self):
        by_id = {r["asset_id"]: r for r in self.report["results"]}
        job = by_id["bigquery.scheduled_query.6512f0a1-0000-2b8e-a1d4-001a11440003"]  # no destination
        self.assertIn("J01_UNSCHEDULED", [f["rule"] for f in job["findings"]])
        into = by_id["bigquery.scheduled_query.6512f0a1-0000-2b8e-a1d4-001a11440002"]
        self.assertIn(("J02_DESTINATION_TABLE", "flag"), [(f["rule"], f["severity"]) for f in into["findings"]])
        self.assertNotIn("job", into)  # running only its SELECT would write nothing
        mv = by_id["bigquery.materialized_view.sales.mv_daily_sales"]
        self.assertIn("M01_MATERIALIZED_VIEW", [f["rule"] for f in mv["findings"]])

    def test_marker_removed_and_rerun_is_identical(self):
        self.assertFalse((self.out / IN_PROGRESS_MARKER).exists())
        first = {p: p.read_text() for p in self.out.rglob("*.sql")}
        migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=self.out)
        self.assertEqual(first, {p: p.read_text() for p in self.out.rglob("*.sql")})


class VerifyFailsClosed(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)
        migrate({"plan_id": "x", "assets": build_plan(DEMO)["assets"][:3]}, out_dir=self.out)
        self.path = self.out / "report.json"

    def tearDown(self):
        self.tmp.cleanup()

    def edit(self, fn):
        report = json.loads(self.path.read_text())
        fn(report)
        self.path.write_text(json.dumps(report))

    def test_in_progress_marker_refused(self):
        (self.out / IN_PROGRESS_MARKER).write_text("")
        with self.assertRaisesRegex(ValueError, "incomplete"):
            verify(self.path)

    def test_count_mismatch_refused(self):
        self.edit(lambda r: r["counts"].update(ok=99))
        with self.assertRaisesRegex(ValueError, "count mismatch"):
            verify(self.path)

    def test_missing_artifact_fails(self):
        (self.out / json.loads(self.path.read_text())["results"][0]["output_path"]).unlink()
        self.assertEqual(verify(self.path)["rows"][0]["verdict"], "FAIL")

    def test_escaping_path_fails(self):
        self.edit(lambda r: r["results"][0].update(output_path="../../etc/passwd"))
        self.assertEqual(verify(self.path)["rows"][0]["verdict"], "FAIL")

    def test_ok_with_flags_fails(self):
        self.edit(lambda r: r["results"][0].update(flags=1))
        self.assertEqual(verify(self.path)["rows"][0]["verdict"], "FAIL")


class EveryRuleHasATest(unittest.TestCase):
    def test_rule_ids_in_code_appear_in_tests(self):
        rule = re.compile(r"(?<![A-Z0-9])(?:TY|G|D|GS|M|J)\d\d(?:_[A-Z0-9]+)+")
        code = "".join(p.read_text() for p in (ROOT / "gcp_aidp").rglob("*.py"))
        tests = "".join(p.read_text() for p in (ROOT / "tests").glob("test_*.py"))
        ids = set(rule.findall(code))
        self.assertGreater(len(ids), 40)  # the guard is not passing on an empty set
        self.assertEqual(sorted(ids - set(rule.findall(tests))), [])


if __name__ == "__main__":
    unittest.main()
