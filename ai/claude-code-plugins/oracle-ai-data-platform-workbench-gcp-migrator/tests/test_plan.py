"""Plan invariants on the demo fixture and on hand-built manifests."""
from __future__ import annotations

import copy
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from gcp_aidp.cli import main
from gcp_aidp.plan import build_plan

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())

# Section 2 of the design: every source type, its action and version.
EXPECTED = {
    "bq_dataset": ("MIGRATE", "0.1"),
    "bq_table": ("MIGRATE", "0.1"),
    "bq_view": ("MIGRATE", "0.1"),
    "bq_materialized_view": ("MIGRATE", "0.1"),
    "bq_saved_query": ("MIGRATE", "0.1"),
    "bq_scheduled_query": ("MIGRATE", "0.1"),
    "bq_external_table": ("MIGRATE", "0.1"),
    "gcs_bucket": ("MIGRATE", "0.1"),
    "bq_procedure": ("REPORT", "0.1"),
    "bq_table_valued_function": ("REPORT", "0.1"),
    "bq_ml_model": ("REPORT", "0.1"),
    "bq_row_access_policy": ("REPORT", "0.1"),
    "bq_column_policy_tag": ("REPORT", "0.1"),
    "bq_dataset_iam": ("REPORT", "0.1"),
    "dataproc_cluster": ("REPORT", "0.2"),
    "dataproc_batch": ("MIGRATE", "0.2"),
    "dataproc_workflow_template": ("MIGRATE", "0.2"),
    "composer_environment": ("SKIP", "0.3"),
    "composer_dag": ("SKIP", "0.3"),
    "dataform_repository": ("SKIP", "0.3"),
    "bq_pipeline": ("SKIP", "0.3"),
    "dataflow_job": ("SKIP", "later"),
    "vertex_model": ("SKIP", "later"),
    "vertex_endpoint": ("SKIP", "later"),
    "vertex_pipeline": ("SKIP", "later"),
}


def _manifest(**bigquery):
    return {"project_id": "p", "sources": {"bigquery": {"items": bigquery}}}


class DemoPlan(unittest.TestCase):
    def setUp(self):
        self.plan = build_plan(DEMO)

    def test_every_source_type_has_its_action_and_version(self):
        seen = {}
        for a in self.plan["assets"]:
            seen.setdefault(a["source"]["type"], set()).add((a["action"], a["version"]))
        for stype, expected in EXPECTED.items():
            self.assertEqual(seen.get(stype), {expected}, stype)

    def test_scalar_functions_split_by_language(self):
        rows = {a["source"]["name"]: a["action"] for a in self.plan["assets"]
                if a["source"]["type"] == "bq_scalar_function"}
        self.assertEqual(rows, {"net_price": "MIGRATE", "safe_ratio": "MIGRATE", "parse_utm": "REPORT"})

    def test_every_manifest_asset_has_a_row(self):
        manifest_count = sum(len(rows) for src in DEMO["sources"].values() for rows in src["items"].values())
        self.assertEqual(self.plan["summary"]["asset_count"], manifest_count)

    def test_skip_and_report_rows_say_why(self):
        for a in self.plan["assets"]:
            if a["action"] != "MIGRATE":
                self.assertTrue(a.get("reason"), a["id"])
            if a["action"] == "REPORT" and a["source"]["type"] != "bq_ml_model" and "access" not in a["id"]:
                self.assertIn(a["effort"], ("S", "M", "L"), a["id"])

    def test_target_catalog_is_a_valid_name(self):
        self.assertEqual(self.plan["target"]["catalog"], "northwind_analytics_demo")


class Datasets(unittest.TestCase):
    """--datasets narrows what is migrated; the rest of the estate stays in the plan as SKIP."""

    def test_default_is_every_dataset(self):
        plan = build_plan(DEMO)
        self.assertIsNone(plan["scope"]["datasets"])
        self.assertIn("Datasets: all 4", Path(self._md(plan)).read_text())

    def test_other_datasets_become_skip_with_the_reason(self):
        plan = build_plan(DEMO, datasets=["sales", "logs"])
        for a in plan["assets"]:
            if not a["id"].startswith("bigquery."):
                continue
            ds = a["source"].get("dataset") or (a["source"]["name"] if a["source"]["type"] == "bq_dataset" else None)
            if ds in ("marketing", "finance"):
                self.assertEqual(a["action"], "SKIP", a["id"])
                self.assertIn("outside --datasets", a["reason"])
        migrated = {a["source"]["dataset"] for a in plan["assets"]
                    if a["source"]["type"] == "bq_table" and a["action"] == "MIGRATE"}
        self.assertEqual(migrated, {"sales", "logs"})
        self.assertEqual(plan["scope"]["datasets"], ["logs", "sales"])
        self.assertIn("Datasets: logs, sales (2 of 4; the rest are SKIP)", Path(self._md(plan)).read_text())

    def test_the_copy_covers_only_the_chosen_datasets(self):
        from gcp_aidp.migrate import migrate
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(DEMO, datasets=["sales"]), out_dir=Path(d))
        copies = [t["taskKey"] for t in report["jobs"][0]["tasks"] if t["taskKey"].startswith("copy_")]
        self.assertEqual(copies, ["copy_sales"])

    def test_unknown_dataset_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "not in the inventory: sale"):
            build_plan(DEMO, datasets=["sale"])

    def _md(self, plan):
        from gcp_aidp.plan import write_plan_markdown
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        return write_plan_markdown(plan, Path(self._tmp.name) / "plan.md")


class NotebookPlan(unittest.TestCase):
    def test_exported_notebooks_migrate_and_listed_ones_say_how(self):
        by_id = {a["id"]: a for a in build_plan(DEMO)["assets"]}
        nb = by_id["bigquery.notebook.nb-0001"]
        self.assertEqual((nb["action"], nb["version"], nb["target"]["type"], nb["target"]["name"]),
                         ("MIGRATE", "0.2", "aidp_notebook", "Customer_segmentation_exploration"))
        listed = by_id["bigquery.notebook.nb-0003"]
        self.assertEqual(listed["action"], "SKIP")
        self.assertIn("--notebooks-dir", listed["reason"])


class DataprocPlan(unittest.TestCase):
    """0.2: PySpark and Spark SQL definitions migrate; the rest is reported with the reason."""

    def setUp(self):
        self.by_id = {a["id"]: a for a in build_plan(DEMO)["assets"]}

    def test_job_definitions_by_type(self):
        def row(name):
            return self.by_id[f"dataproc.job.us-central1/{name}"]
        for name in ("etl-nightly/sessionize.py", "adhoc-analytics/cohorts.sql"):
            self.assertEqual((row(name)["action"], row(name)["version"], row(name)["target"]["type"]),
                             ("MIGRATE", "0.2", "aidp_job"), name)
        self.assertEqual(row("etl-nightly/sessionize.py")["target"]["name"], "etl_nightly_sessionize_py")
        self.assertEqual(row("etl-nightly/gl-export.jar")["action"], "REPORT")
        self.assertIn("JAR_TASK", row("etl-nightly/gl-export.jar")["reason"])
        self.assertEqual(row("adhoc-analytics/legacy_rollup.hql")["action"], "REPORT")
        self.assertIn("hive", row("adhoc-analytics/legacy_rollup.hql")["reason"])

    def test_template_and_cluster(self):
        t = self.by_id["dataproc.workflow_template.month-end-close"]
        self.assertEqual((t["action"], t["target"]["name"]), ("MIGRATE", "template_month_end_close"))
        c = self.by_id["dataproc.cluster.etl-nightly"]
        self.assertEqual(c["action"], "REPORT")
        self.assertIn("8 workers n2-standard-8", "; ".join(c["notes"]))

    def test_one_script_on_two_clusters_does_not_collide(self):
        m = copy.deepcopy(DEMO)
        jobs = m["sources"]["dataproc"]["items"]["jobs"]
        twin = {**jobs[0], "id": "us-central1/adhoc-analytics/sessionize.py", "cluster": "adhoc-analytics"}
        jobs.append(twin)
        build_plan(m)  # etl_nightly_sessionize_py and adhoc_analytics_sessionize_py

    def test_two_definitions_with_one_job_name_halt(self):
        m = copy.deepcopy(DEMO)
        jobs = m["sources"]["dataproc"]["items"]["jobs"]
        jobs.append({**jobs[0], "id": "us-central1/etl-nightly/sessionize-py", "main_file": "gs://b/sessionize-py"})
        with self.assertRaisesRegex(ValueError, "etl_nightly_sessionize_py"):
            build_plan(m)


class FailClosed(unittest.TestCase):
    def test_case_only_dataset_collision_halts(self):
        m = _manifest(datasets=[{"name": "Sales"}, {"name": "sales"}])
        with self.assertRaisesRegex(ValueError, "collision"):
            build_plan(m)

    def test_view_and_table_with_one_name_halt(self):
        m = _manifest(tables=[{"dataset": "d", "name": "T"}],
                      views=[{"dataset": "d", "name": "t", "query": "SELECT 1"}])
        with self.assertRaisesRegex(ValueError, "collision"):
            build_plan(m)

    def test_job_names_colliding_after_slugging_halt(self):
        m = _manifest(scheduled_queries=[{"id": "1", "name": "Daily load", "query": "SELECT 1"},
                                         {"id": "2", "name": "daily-load", "query": "SELECT 1"}])
        with self.assertRaisesRegex(ValueError, "collision"):
            build_plan(m)

    def test_skipped_assets_do_not_collide(self):
        m = _manifest(notebooks=[{"id": "1", "name": "x"}, {"id": "2", "name": "X"}])
        self.assertEqual(build_plan(m)["summary"]["by_action"]["SKIP"], 2)

    def test_unknown_collection_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "unsupported collection"):
            build_plan(_manifest(reservations=[]))

    def test_unknown_source_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "unsupported manifest source"):
            build_plan({"sources": {"bigtable": {"items": {}}}})

    def test_missing_required_field_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "views\\[0\\].query"):
            build_plan(_manifest(views=[{"dataset": "d", "name": "v"}]))

    def test_scan_error_is_carried_into_the_plan(self):
        m = copy.deepcopy(DEMO)
        m["sources"]["gcs"] = {"summary": {"error": "403 storage.buckets.list"}, "items": {}}
        self.assertEqual(build_plan(m)["scan_errors"], {"gcs": "403 storage.buckets.list"})

    def test_bucket_name_that_is_not_a_gcs_name_fails_closed(self):
        # The name goes into a shell script (the rclone transfer job).
        manifest = {"project_id": "p", "sources": {"gcs": {"items": {"buckets": [{"name": "b$(touch x)"}]}}}}
        with self.assertRaisesRegex(ValueError, "bucket"):
            build_plan(manifest)

    def test_bad_namespace_and_catalog_rejected(self):
        with self.assertRaises(ValueError):
            build_plan(DEMO, oci_namespace="Bad NS")
        with self.assertRaises(ValueError):
            build_plan(DEMO, catalog="1bad")


class Cli(unittest.TestCase):
    def test_live_inventory_fails_fast_without_credentials(self):
        from unittest import mock
        from gcp_aidp.gcp_client import GcpClient

        with tempfile.TemporaryDirectory() as tmp, redirect_stderr(io.StringIO()) as err, \
                mock.patch.object(GcpClient, "session", new_callable=mock.PropertyMock,
                                  side_effect=RuntimeError("no google-auth")):
            out = Path(tmp) / "inv.json"
            self.assertEqual(main(["inventory", "--project", "my-proj-1", "-o", str(out)]), 2)
            self.assertFalse(out.exists())
        self.assertIn("cannot authenticate", err.getvalue())

    def test_a_project_id_that_is_not_one_is_refused_before_any_call(self):
        from gcp_aidp.gcp_client import GcpClient
        for bad in ("https://github.com/x/y", "My Project", "ab", "proj-"):
            with self.assertRaisesRegex(ValueError, "not a Google Cloud project id"):
                GcpClient(bad)
        for good in ("my-project-123456", "my-proj1", "example.com:my-proj"):
            GcpClient(good)

    def test_fixture_name_cannot_traverse(self):
        with redirect_stderr(io.StringIO()):
            self.assertEqual(main(["inventory", "--fixture", "../x"]), 2)


if __name__ == "__main__":
    unittest.main()
