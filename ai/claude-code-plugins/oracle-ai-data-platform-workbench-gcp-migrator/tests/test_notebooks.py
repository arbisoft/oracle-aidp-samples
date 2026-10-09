"""The generated data-plane notebooks, without Spark."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.dataplane import STAGES
from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())


class Notebooks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        cls.report = migrate(build_plan(DEMO), out_dir=cls.out)
        cls.dp = json.loads((cls.out / "notebooks/data_plan.json").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_one_self_contained_notebook_per_stage(self):
        self.assertEqual(self.report["notebooks"][:4], [f"notebooks/{s}.ipynb" for s in STAGES])
        for path in self.report["notebooks"][:4]:  # the job notebooks fail on their own: spark.sql raises
            nb = json.loads((self.out / path).read_text())
            self.assertEqual((nb["nbformat"], nb["metadata"]["kernelspec"]["name"]), (4, "python3"))
            code = ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]
            for cell in code:
                compile(cell, path, "exec")  # every cell is valid Python
                self.assertNotIn("from gcp_aidp", cell)  # nothing to upload beside the notebook
            self.assertIn("raise RuntimeError", code[-1])  # a failed stage fails the task
            self.assertNotIn("SystemExit", code[-1].split("\n", 3)[-1])

    def test_jobs(self):
        jobs = {j["name"]: j for j in self.report["jobs"]}
        chain = jobs["gcp_aidp_migration"]["tasks"]
        keys = [t["taskKey"] for t in chain]
        self.assertEqual(keys[:2] + keys[-1:], ["diagnose", "structure", "reconcile"])
        self.assertIn("copy_sales", keys)
        self.assertIn("snapshot_refresh_sales_mv_daily_sales", keys)
        self.assertEqual([t.get("dependsOn") for t in chain[1:]], [[k] for k in keys[:-1]])  # one at a time
        copy = next(t for t in chain if t["taskKey"] == "copy_sales")
        self.assertEqual(copy["parameters"], [{"name": "dataset", "value": "sales"}])
        # Each materialized view also gets its own refresh job, running its snapshot and refresh SQL.
        mv = jobs["refresh_sales_mv_daily_sales"]["tasks"][0]["notebook"]
        cells = [c for c in json.loads((self.out / mv).read_text())["cells"] if c["cell_type"] == "code"]
        self.assertEqual(len(cells), 2)
        self.assertIn("INSERT OVERWRITE", "".join(cells[1]["source"]))
        for j in self.report["jobs"]:
            for t in j["tasks"]:
                self.assertIn(t["notebook"], self.report["notebooks"])

    def test_data_plan_holds_only_what_can_run(self):
        tables = {f"{t['dataset']}.{t['name']}" for t in self.dp["tables"]}
        self.assertIn("sales.orders", tables)
        self.assertNotIn("sales.store_visits", tables)  # blocked: GEOGRAPHY and more
        views = {v["name"] for v in self.dp["views"]}
        self.assertEqual(views, {"v_active_customers", "v_campaign_days", "v_order_kpis", "v_payments_masked",
                                 "v_latest_order_per_customer"})
        verdicts = {x["name"]: x["verdict"] for x in self.dp["not_created"]}
        self.assertEqual(verdicts["v_all_app_events"], "BLOCKED")
        self.assertEqual(verdicts["v_customer_tags"], "NEEDS_REVIEW")  # flagged UNNEST
        self.assertEqual(verdicts["mv_daily_sales"], "DEFERRED")
        self.assertEqual(self.dp["tables"][0]["source"], "northwind-analytics-demo.sales.orders")

    def test_credential_defaults_and_no_secret_in_notebooks(self):
        text = (self.out / "notebooks/02_copy_dataset.ipynb").read_text()
        self.assertIn("'credential-name': 'gcp_bigquery_reader'", text)
        self.assertIn("'viewsEnabled', 'false'", text.replace('\\"', "'").replace('"', "'"))
        self.assertNotIn("private_key\\\": \\\"-----", text)



class Reports(unittest.TestCase):
    def test_a_report_for_another_catalog_is_not_carried_over(self):
        from gcp_aidp.dataplane.common import read_report, write_json
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "copy_report_sales.json"
            write_json(path, {"catalog": "old_cat", "tables": {"sales.orders": {"status": "count_mismatch"}}})
            self.assertIsNone(read_report(path, "new_cat"))
            self.assertEqual(read_report(path, "old_cat")["tables"]["sales.orders"]["status"], "count_mismatch")
            write_json(path, {"tables": {}})  # written before reports named their catalog
            self.assertIsNone(read_report(path, "new_cat"))


if __name__ == "__main__":
    unittest.main()
