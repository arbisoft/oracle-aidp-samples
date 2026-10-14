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
        # A Cloud Scheduler job rides on the row of the job it starts; only one that starts nothing has its own.
        carried = sum("schedules" in a["source"] for a in self.plan["assets"])
        self.assertEqual(self.plan["summary"]["asset_count"], manifest_count - carried)
        self.assertEqual([a["id"] for a in self.plan["assets"] if a["source"]["type"] == "cloud_scheduler_job"],
                         ["dataproc.schedule.us-central1/legacy-rollup-weekly"])

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

    def test_a_single_node_cluster_reads_as_one(self):
        from gcp_aidp.plan.planner import _cluster_sizing
        self.assertEqual(_cluster_sizing({"master_machine_type": "n4-standard-2", "image_version": "2.3.37-debian12"}),
                         ["single node: master n4-standard-2, image 2.3.37-debian12"])

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
        # The name goes into API calls and file paths.
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


class ExportRoute(unittest.TestCase):
    """Tables of at least --export-over-gb go BigQuery → Parquet on GCS → OCI → loaded into Delta."""

    def test_large_tables_take_the_export_route(self):
        plan = build_plan(DEMO, oci_namespace="ns", export_over_gb=1, export_bucket="stage-bkt")
        routed = {a["id"]: a["target"]["load_from"] for a in plan["assets"] if a["target"].get("load_from")}
        self.assertEqual(sorted(routed), ["bigquery.table.finance.gl_entries", "bigquery.table.marketing.web_events",
                                          "bigquery.table.sales.order_items"])
        prefix = f"gcp-aidp-export/{plan['plan_id']}"
        self.assertEqual(routed["bigquery.table.sales.order_items"],
                         {"gcs": f"gs://stage-bkt/{prefix}/sales/order_items/",
                          "oci": f"oci://stage-bkt@ns/{prefix}/sales/order_items/"})
        self.assertEqual(plan["export"], {"bucket": "stage-bkt", "oci_bucket": "stage-bkt", "prefix": prefix,
                                          "over_gb": 1})
        self.assertIsNone(build_plan(DEMO)["export"])

    def test_columns_parquet_cannot_carry_are_exported_as_text(self):
        plan = build_plan(DEMO, export_over_gb=0, export_bucket="stage-bkt", bignumeric="string")
        by_id = {a["id"]: a["target"] for a in plan["assets"]}
        self.assertEqual(by_id["bigquery.table.logs.app_events_20260901"]["load_from"]["as_text"],
                         {"attributes": "TO_JSON_STRING(`attributes`)"})
        self.assertEqual(by_id["bigquery.table.finance.fx_rates"]["load_from"]["as_text"],
                         {"rate": "CAST(`rate` AS STRING)"})
        self.assertNotIn("as_text", by_id["bigquery.table.sales.orders"]["load_from"])  # plain: bq extract
        with tempfile.TemporaryDirectory() as d:
            from gcp_aidp.migrate import migrate

            report = migrate(plan, out_dir=Path(d))
        from gcp_aidp.export import job_config

        blocked = next(r for r in report["results"] if r["asset_id"] == "bigquery.table.sales.store_visits")
        self.assertIn("load_from", by_id["bigquery.table.sales.store_visits"])  # routed, but blocked (INTERVAL):
        self.assertEqual(blocked["status"], "blocked")
        self.assertNotIn("X01_EXPORT_ROUTE", [f["rule"] for f in blocked["findings"]])  # no export instruction
        tables = {t["name"]: t for t in report["export"]["tables"]}
        self.assertNotIn("store_visits", tables)
        query = job_config("northwind-analytics-demo", tables["fx_rates"])["query"]
        self.assertFalse(query["useLegacySql"])
        self.assertIn("SELECT * REPLACE (CAST(`rate` AS STRING) AS `rate`) FROM `northwind-analytics-demo.finance.fx_rates`",
                      query["query"])
        extract = job_config("northwind-analytics-demo", tables["orders"])["extract"]  # nothing to convert: free
        self.assertEqual((extract["sourceTable"]["tableId"], extract["destinationFormat"]), ("orders", "PARQUET"))

    def test_text_types_and_nesting(self):
        C = lambda name, type_, mode="NULLABLE", **kw: {"name": name, "type": type_, "mode": mode, **kw}
        t = {"dataset": "d", "name": "t", "num_bytes": 10**10,
             "columns": [C("id", "INTEGER"), C("j", "JSON"), C("tm", "TIME"), C("g", "GEOGRAPHY")]}
        nested = dict(t, name="n", columns=[
            C("id", "INTEGER"), C("plain", "RECORD", fields=[C("a", "STRING")]),
            C("s", "RECORD", fields=[C("a", "STRING"), C("j", "JSON")]),
            C("items", "RECORD", "REPEATED", fields=[C("sku", "STRING"), C("tms", "TIME", "REPEATED")])])
        repeated = dict(t, name="r", columns=[C("tags", "JSON", "REPEATED")])
        plan = build_plan(_manifest(datasets=[{"name": "d"}], tables=[t, nested, repeated]),
                          export_over_gb=1, export_bucket="b-1", geography="wkt")
        by = {a["source"]["name"]: a for a in plan["assets"] if a["source"]["type"] == "bq_table"}
        self.assertEqual(by["t"]["target"]["load_from"]["as_text"],
                         {"j": "TO_JSON_STRING(`j`)", "tm": "FORMAT_TIME('%H:%M:%E6S', `tm`)", "g": "ST_ASTEXT(`g`)"})
        self.assertEqual(by["n"]["target"]["load_from"]["as_text"], {  # plain struct: exported as is
            "s": "IF(`s` IS NULL, NULL, STRUCT(`s`.`a` AS `a`, TO_JSON_STRING(`s`.`j`) AS `j`))",
            "items": "ARRAY(SELECT IF(_gcp_aidp_e0 IS NULL, NULL, STRUCT(_gcp_aidp_e0.`sku` AS `sku`, "
                     "ARRAY(SELECT FORMAT_TIME('%H:%M:%E6S', _gcp_aidp_e1) FROM UNNEST(_gcp_aidp_e0.`tms`) "
                     "AS _gcp_aidp_e1 WITH OFFSET AS _gcp_aidp_o1 ORDER BY _gcp_aidp_o1) AS `tms`)) "
                     "FROM UNNEST(`items`) AS _gcp_aidp_e0 WITH OFFSET AS _gcp_aidp_o0 ORDER BY _gcp_aidp_o0)"})
        self.assertEqual(by["r"]["target"]["load_from"]["as_text"],
                         {"tags": "ARRAY(SELECT TO_JSON_STRING(_gcp_aidp_e0) FROM UNNEST(`tags`) AS _gcp_aidp_e0 "
                                  "WITH OFFSET AS _gcp_aidp_o0 ORDER BY _gcp_aidp_o0)"})

    def test_interval_and_range_carried_by_choice_and_exported_as_planned(self):
        C = lambda name, type_, **kw: {"name": name, "type": type_, "mode": "NULLABLE", **kw}
        t = {"dataset": "d", "name": "t", "num_bytes": 10**10,
             "columns": [C("id", "INTEGER"), C("dwell", "INTERVAL"), C("w", "RANGE", rangeElementType={"type": "DATE"})]}
        manifest = _manifest(datasets=[{"name": "d"}], tables=[t])
        with self.assertRaisesRegex(ValueError, "range must be block|struct"):
            build_plan(manifest, range_="string")
        plan = build_plan(manifest, export_over_gb=1, export_bucket="b-1", interval="string", range_="struct")
        target = next(a for a in plan["assets"] if a["source"]["type"] == "bq_table")["target"]
        self.assertEqual([c["target_type"] for c in target["columns"]],
                         ["BIGINT", "STRING", "STRUCT<`start`: DATE, `end`: DATE>"])
        self.assertEqual(target["load_from"]["as_text"], {
            "dwell": "CAST(`dwell` AS STRING)",
            "w": "IF(`w` IS NULL, NULL, STRUCT(RANGE_START(`w`) AS `start`, RANGE_END(`w`) AS `end`))"})

    def test_blocked_types_are_a_decision_at_the_top_of_the_plan(self):
        from gcp_aidp.plan.planner import decisions, summarize_plan
        plan = build_plan(DEMO)
        todo = decisions(plan)
        self.assertTrue(any(d.startswith("--interval string:") and "sales.store_visits." in d for d in todo), todo)
        self.assertTrue(any(d.startswith("--geography wkt:") for d in todo), todo)
        self.assertIn("NEEDS A DECISION", summarize_plan(plan))
        from gcp_aidp.plan import write_plan_markdown
        with tempfile.TemporaryDirectory() as d:
            md = write_plan_markdown(plan, Path(d) / "plan.md").read_text()
        self.assertLess(md.index("## Needs a decision"), md.index("## To migrate"))
        chosen = build_plan(DEMO, bignumeric="string", geography="wkt", interval="string", range_="struct")
        self.assertFalse([d for d in decisions(chosen) if d.startswith("--")])  # chosen: no longer asked

    def test_bucket_mapping_sends_files_and_paths_to_the_mapped_oci_bucket(self):
        from gcp_aidp.plan.planner import bucket_mapping

        buckets, ns = bucket_mapping({"buckets": {"stage-bkt": {"oci_bucket": "aidp-ws-bucket"},
                                                  "northwind-landing": {"oci_bucket": "Landing_OCI",
                                                                        "oci_namespace": "tenancy"}},
                                      "default_namespace": "tenancy"}, None)
        plan = build_plan(DEMO, oci_namespace=ns, export_over_gb=1, export_bucket="stage-bkt", buckets=buckets)
        by_id = {a["id"]: a for a in plan["assets"]}
        self.assertTrue(by_id["bigquery.table.sales.order_items"]["target"]["load_from"]["oci"]
                        .startswith("oci://aidp-ws-bucket@tenancy/gcp-aidp-export/"))
        self.assertEqual(by_id["gcs.bucket.northwind-landing"]["target"]["name"], "Landing_OCI")
        with tempfile.TemporaryDirectory() as d:
            from gcp_aidp.migrate import migrate

            report = migrate(plan, out_dir=Path(d))
        self.assertEqual((report["export"]["bucket"], report["export"]["oci_bucket"]), ("stage-bkt", "aidp-ws-bucket"))
        for bad in ({"buckets": {"b-1": {"oci_bucket": "x; rm -rf /"}}},
                    {"buckets": {"b-1": {"oci_bucket": "x", "oci_namespace": "other"}}, "default_namespace": "ns"},
                    {"buckets": ["b-1"]}):
            with self.assertRaises(ValueError, msg=bad):
                bucket_mapping(bad, None)

    def test_D03_ingestion_time_tables_carry_their_pseudo_columns(self):
        from gcp_aidp.export import job_config

        def table(name, kind, size=1):
            return {"dataset": "d", "name": name, "num_bytes": size, "partitioning": {"type": kind, "field": None},
                    "columns": [{"name": "n", "type": "INTEGER", "mode": "NULLABLE"}]}
        manifest = {"project_id": "p", "sources": {"bigquery": {"items": {
            "datasets": [{"name": "d"}], "tables": [table("daily", "DAY", 10**10), table("hourly", "HOUR")]}}}}
        plan = build_plan(manifest, oci_namespace="ns", export_over_gb=1, export_bucket="stage-1")
        target = {a["source"]["name"]: a["target"] for a in plan["assets"] if a["source"]["type"] == "bq_table"}
        self.assertEqual([c["name"] for c in target["daily"]["columns"]], ["n", "_PARTITIONTIME", "_PARTITIONDATE"])
        self.assertEqual([c["name"] for c in target["hourly"]["columns"]], ["n", "_PARTITIONTIME"])  # no date
        self.assertEqual(target["daily"]["cluster_by"], ["_PARTITIONTIME"])
        # the export route: an extract writes real columns only, so EXPORT DATA selects them
        route = target["daily"]["load_from"]
        self.assertEqual(route["pseudo_columns"], ["_PARTITIONTIME", "_PARTITIONDATE"])
        query = job_config("p", {"dataset": "d", "name": "daily", **route})["query"]["query"]
        self.assertIn("SELECT *, _PARTITIONTIME AS _PARTITIONTIME, _PARTITIONDATE AS _PARTITIONDATE FROM", query)

    def test_GS03_TRANSFER_writes_a_spec_for_the_transfer_verb(self):
        from gcp_aidp.migrate import migrate

        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=Path(d))
            bucket = next(r for r in report["results"] if r["kind"] == "gcs_bucket")
            spec = json.loads((Path(d) / bucket["output_path"]).read_text())
        self.assertTrue(bucket["output_path"].endswith(".transfer.json"))
        self.assertEqual(spec["gcs"], f"gs://{spec['gcs_bucket']}/")
        self.assertEqual(spec["oci"], f"oci://{spec['oci_bucket']}@ns/")
        self.assertEqual(bucket["status"], "ok")
        self.assertIn("gcp-aidp transfer --apply", bucket["findings"][0]["detail"])

        with tempfile.TemporaryDirectory() as d:  # no namespace: the copy cannot be addressed
            report = migrate(build_plan(DEMO), out_dir=Path(d))
            bucket = next(r for r in report["results"] if r["kind"] == "gcs_bucket")
        self.assertEqual(bucket["status"], "needs_manual_review")

    def test_flags_go_together_and_are_checked(self):
        for kw in ({"export_over_gb": 1}, {"export_bucket": "b-1"},
                   {"export_over_gb": 1, "export_bucket": "Bad Bucket"}, {"export_over_gb": -1, "export_bucket": "b-1"}):
            with self.assertRaises(ValueError, msg=kw):
                build_plan(DEMO, **kw)

    def test_migrate_records_what_export_runs_and_the_load_path(self):
        from gcp_aidp.migrate import migrate

        plan = build_plan(DEMO, oci_namespace="ns", export_over_gb=1, export_bucket="stage-bkt")
        with tempfile.TemporaryDirectory() as d:
            report = migrate(plan, out_dir=Path(d))
            dp = json.loads((Path(d) / "notebooks/data_plan.json").read_text())
            self.assertFalse((Path(d) / "exports").exists())  # nothing for a person to run by hand
        prefix = f"gcp-aidp-export/{plan['plan_id']}"
        spec = report["export"]
        self.assertEqual({k: spec[k] for k in ("project", "bucket", "oci_bucket", "namespace", "prefix")},
                         {"project": DEMO["project_id"], "bucket": "stage-bkt", "oci_bucket": "stage-bkt",
                          "namespace": "ns", "prefix": prefix})
        self.assertEqual(sorted(t["name"] for t in spec["tables"]), ["gl_entries", "order_items", "web_events"])
        files = {t["name"]: t["files"] for t in dp["tables"]}
        self.assertEqual(files["order_items"], f"oci://stage-bkt@ns/{prefix}/sales/order_items/")
        self.assertIsNone(files["orders"])
        row = next(r for r in report["results"] if r["asset_id"] == "bigquery.table.sales.order_items")
        self.assertIn("X01_EXPORT_ROUTE", [f["rule"] for f in row["findings"]])
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(migrate(build_plan(DEMO), out_dir=Path(d))["export"])



if __name__ == "__main__":
    unittest.main()
