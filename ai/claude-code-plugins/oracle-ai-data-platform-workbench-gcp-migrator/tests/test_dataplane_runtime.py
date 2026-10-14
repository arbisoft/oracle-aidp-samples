"""Run the generated data-plane notebooks on a local Spark 3.5 + Delta.

Skipped unless `pyspark` and `delta-spark` are installed (see
test_spark_runtime.py). Every code cell of each generated .ipynb is executed
as AIDP would run it; only two things are faked: `aidputils`/`oidlUtils`
(AIDP runtime globals), and the BigQuery connector, replaced by a reader that
returns local DataFrames typed the way connector 0.45.0 typed them on AIDP
(00_diagnose, Spark 3.5.0): TIME as microseconds, DATETIME as an ISO string.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover - optional dependency
    SparkSession = None

from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan

C = lambda name, type_, mode="NULLABLE", **kw: {"name": name, "type": type_, "mode": mode, **kw}
MANIFEST = {"project_id": "proj", "sources": {"bigquery": {"items": {
    "datasets": [{"name": "shop"}],
    "tables": [
        {"dataset": "shop", "name": "orders", "num_rows": 3,
         "columns": [C("order_id", "INTEGER", "REQUIRED"), C("status", "STRING"), C("order_date", "DATE"),
                     C("total", "NUMERIC", precision="10", scale="2")],
         "partitioning": {"type": "DAY", "field": "order_date"}, "clustering": ["status"]},
        {"dataset": "shop", "name": "profiles", "num_rows": 2,
         "columns": [C("id", "INTEGER"), C("address", "RECORD", fields=[C("city", "STRING")]),
                     C("tags", "STRING", "REPEATED")]},
        {"dataset": "shop", "name": "type_carried", "num_rows": 2,
         "columns": [C("id", "INTEGER"), C("payload", "JSON"), C("local_time", "DATETIME"), C("opening", "TIME"),
                     C("precise", "BIGNUMERIC", precision="30", scale="10")]},
        {"dataset": "shop", "name": "geo", "num_rows": 1, "columns": [C("id", "INTEGER"), C("g", "GEOGRAPHY")]},
    ],
    "external_tables": [{"dataset": "shop", "name": "ext_files", "format": "PARQUET",
                         "source_uris": ["gs://shop-landing/files/*.parquet"]}],
    "views": [{"dataset": "shop", "name": "v_paid", "query": "SELECT order_id FROM shop.orders WHERE status = 'paid'"},
              {"dataset": "shop", "name": "v_kpi", "query": "SELECT COUNTIF(status = 'paid') AS paid, "
                                                            "SAFE_DIVIDE(SUM(total), COUNT(*)) AS avg_total FROM shop.orders"},
              {"dataset": "shop", "name": "v_split", "query": "SELECT SPLIT(status, '.') AS parts FROM shop.orders"}],
    "materialized_views": [{"dataset": "shop", "name": "mv_daily",
                            "query": "SELECT order_date, COUNT(*) AS n FROM shop.orders GROUP BY order_date"}],
}}, "gcs": {"items": {"buckets": [{"name": "shop-landing"}]}}}}

SOURCE = {  # what the connector is expected to return, per table
    "proj.shop.orders": ("order_id long, status string, order_date date, total decimal(38,9)",
                         [(1, "paid", dt.date(2026, 1, 1), Decimal("10.50")),
                          (2, "open", dt.date(2026, 1, 2), Decimal("0.01")),
                          (3, "paid", dt.date(2026, 1, 2), Decimal("99999999.99"))]),
    "proj.shop.profiles": ("id long, address struct<city:string>, tags array<string>",
                           [(1, ("Lahore",), ["a", "b"]), (2, ("Austin",), [])]),
    # As connector 0.45.0 delivered it on AIDP (Spark 3.5.0), checked by 00_diagnose:
    # DATETIME as an ISO string, TIME as microseconds, BIGNUMERIC(30,10) as decimal(30,10).
    "proj.shop.type_carried": ("id long, payload string, local_time string, opening long, precise decimal(30,10)",
                               [(1, '{"a":1}', "2026-01-01T10:00:00", 37_800_123_456,
                                 Decimal("12345678901234567890.0123456789")),
                                (2, '"x"', "2026-06-30T23:59:59.999999", 0, Decimal(-1))]),
}
FAKE_KEY = base64.b64encode(json.dumps({"type": "service_account", "client_email": "reader@proj.iam",
                                        "private_key": "-----not-a-real-key-----"}).encode()).decode()


class _Params:
    def __init__(self, values):
        self.values = values

    def getParameter(self, name, default):  # the AIDP API's spelling
        return self.values.get(name, default)


@unittest.skipIf(SparkSession is None, "pyspark and delta-spark are not installed")
class DataPlaneNotebooks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable  # local Python workers match the driver
        cls.tmp = tempfile.TemporaryDirectory()
        builder = (SparkSession.builder.master("local[1]").appName("gcp-aidp-dataplane")
                   .config("spark.ui.enabled", "false")
                   .config("spark.sql.warehouse.dir", f"{cls.tmp.name}/warehouse")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("OFF")
        cls.source = dict(SOURCE)
        out = Path(cls.tmp.name) / "migrated"
        migrate(build_plan(MANIFEST, catalog="spark_catalog", oci_namespace="ns"), out_dir=out)
        # The external table's files: a local folder stands in for oci://shop-landing@ns/files/,
        # empty until test_1 "transfers" them.
        files = cls.files = Path(cls.tmp.name) / "landing" / "files"
        cls.notebooks = {}
        for p in (out / "notebooks").rglob("*.ipynb"):
            text = p.read_text().replace("oci://shop-landing@ns/files/", files.as_uri())
            cls.notebooks[p.stem] = json.loads(text)
        cls.reports = Path(cls.tmp.name) / "reports"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp.cleanup()

    def run_notebook(self, stage: str, **params) -> int:
        """Execute every code cell; the RUN cell's RuntimeError becomes exit code 1."""
        params.setdefault("reports-dir", str(self.reports))
        secrets = type("S", (), {"get": staticmethod(lambda name, key: FAKE_KEY)})
        ns = {"spark": self.spark, "oidlUtils": type("O", (), {"parameters": _Params(params)}),
              "aidputils": type("A", (), {"secrets": secrets})}
        cells = [c for c in self.notebooks[stage]["cells"] if c["cell_type"] == "code"]
        for cell in cells[:-1]:
            exec("".join(cell["source"]), ns)  # noqa: S102 - runs the generated notebook
        source = self.source
        ns["bigquery_reader"] = lambda spark, credentials, project: (
            lambda fqn: spark.createDataFrame(source[fqn][1], source[fqn][0]))
        try:
            exec("".join(cells[-1]["source"]), ns)  # noqa: S102 - runs the generated notebook
        except RuntimeError as exc:
            if "problem(s)" not in str(exc):
                raise
            return 1
        return 0

    def report(self, name):
        return json.loads((self.reports / name).read_text())

    def test_1_structure_copy_reconcile(self):
        self.assertEqual(self.run_notebook("01_structure"), 0)
        s = self.report("structure_report.json")
        self.assertEqual({k: v["status"] for k, v in s["tables"].items()},
                         {"shop.orders": "created", "shop.profiles": "created", "shop.type_carried": "created"})
        self.assertEqual(s["views"], {"shop.v_paid": {"status": "created"}, "shop.v_kpi": {"status": "created"}})

        self.assertEqual(self.run_notebook("02_copy_dataset", dataset="shop", verify="counts+sums"), 0)
        copy = self.report("copy_report_shop.json")["tables"]
        self.assertEqual({k: v["status"] for k, v in copy.items()},
                         {k: "verified" for k in ("shop.orders", "shop.profiles", "shop.type_carried")})
        self.assertEqual(copy["shop.orders"]["target_sums"]["total"], "100000010.50")

        row = self.spark.table("spark_catalog.shop.type_carried").where("id = 1").collect()[0]
        self.assertEqual(row["opening"], "10:30:00.123456")  # TIME micros → text
        self.assertEqual(row["precise"], Decimal("12345678901234567890.0123456789"))
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        self.assertEqual(self.spark.sql("SELECT CAST(local_time AS STRING) FROM spark_catalog.shop.type_carried "
                                        "WHERE id = 2").collect()[0][0], "2026-06-30 23:59:59.999999")

        self.assertEqual(self.run_notebook("03_reconcile", counts="true"), 0)
        verdicts = {o["object"]: o["verdict"] for o in self.report("MIGRATION_REPORT.json")["objects"]}
        self.assertEqual(verdicts, {"shop.orders": "MIGRATED_VERIFIED", "shop.profiles": "MIGRATED_VERIFIED",
                                    "shop.type_carried": "MIGRATED_VERIFIED", "shop.geo": "BLOCKED",
                                    "shop.ext_files": "EXTERNAL_NOT_CREATED_YET",
                                    "shop.v_paid": "VIEW_CREATED", "shop.v_kpi": "VIEW_CREATED",
                                    "shop.v_split": "NEEDS_REVIEW", "shop.mv_daily": "DEFERRED"})

        # The materialized view's job notebook (a task of the migration job) builds the snapshot.
        for cell in self.notebooks["refresh_shop_mv_daily"]["cells"]:
            if cell["cell_type"] == "code":
                exec("".join(cell["source"]), {"spark": self.spark})  # noqa: S102 - runs the generated notebook
        self.assertEqual(self.spark.table("spark_catalog.shop.mv_daily").count(), 2)
        self.run_notebook("03_reconcile")
        verdicts = {o["object"]: o["verdict"] for o in self.report("MIGRATION_REPORT.json")["objects"]}
        self.assertEqual(verdicts["shop.mv_daily"], "SNAPSHOT_BUILT")

        detail = {o["object"]: o["detail"] for o in self.report("MIGRATION_REPORT.json")["objects"]}
        self.assertIn("run `gcp-aidp transfer --apply`", detail["shop.ext_files"])  # no files yet: says what to do

        # After the transfer: the next structure run creates the external table, readable, and reconciled.
        self.spark.createDataFrame([(1, "a"), (2, "b")], "id long, v string").write.parquet(str(self.files))
        self.assertEqual(self.run_notebook("01_structure"), 0)
        self.assertEqual(self.spark.table("spark_catalog.shop.ext_files").count(), 2)
        self.run_notebook("03_reconcile")
        verdicts = {o["object"]: o["verdict"] for o in self.report("MIGRATION_REPORT.json")["objects"]}
        self.assertEqual(verdicts["shop.ext_files"], "EXTERNAL_CREATED")
        # The rerun left the views as they were, and says so; reconcile still finds them.
        self.assertEqual(self.report("structure_report.json")["views"]["shop.v_paid"]["status"], "already_existed")
        self.assertEqual(verdicts["shop.v_paid"], "VIEW_CREATED")

    def test_2_rerun_skips_and_drift_is_caught(self):
        # Re-running the copy never duplicates rows.
        self.assertEqual(self.run_notebook("02_copy_dataset", dataset="shop"), 0)
        copy = self.report("copy_report_shop.json")["tables"]
        self.assertEqual(copy["shop.orders"]["status"], "skipped_nonempty")
        self.assertEqual(self.spark.table("spark_catalog.shop.orders").count(), 3)

        # The source grew since the copy: the target is not touched, and the mismatch is recorded.
        schema, rows = self.source["proj.shop.orders"]
        self.source["proj.shop.orders"] = (schema, rows + [(4, "paid", dt.date(2026, 1, 3), Decimal(1))])
        self.assertEqual(self.run_notebook("02_copy_dataset", dataset="shop", tables="orders"), 1)
        self.assertEqual(self.report("copy_report_shop.json")["tables"]["shop.orders"]["status"], "count_mismatch")

        # A column renamed at the source: nothing is copied.
        self.source["proj.shop.profiles"] = ("id long, address struct<city:string>, labels array<string>", [])
        self.run_notebook("02_copy_dataset", dataset="shop", tables="profiles", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.profiles"]
        self.assertEqual(rec["status"], "type_drift")
        self.assertEqual(rec["layout_drift"], {"not_in_source": ["tags"], "not_in_plan": ["labels"]})
        self.assertIn("planned but not read: tags; read but not planned: labels", rec["reason"])  # named, not None
        self.assertEqual(self.spark.table("spark_catalog.shop.profiles").count(), 2)

        # Columns the plan does not have (as the pseudo-columns were before D03 carried them) are
        # never dropped silently: the table stops, and the report names them.
        schema, grown = self.source["proj.shop.orders"]
        self.source["proj.shop.orders"] = (schema + ", _PARTITIONTIME timestamp, _PARTITIONDATE date",
                                           [(*r, dt.datetime(2026, 1, 1), dt.date(2026, 1, 1)) for r in grown])
        self.run_notebook("02_copy_dataset", dataset="shop", tables="orders", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.orders"]
        self.assertEqual(rec["status"], "type_drift")
        self.assertIn("read but not planned: _PARTITIONDATE, _PARTITIONTIME", rec["reason"])
        self.source["proj.shop.orders"] = (schema, grown)

        # Overwrite fixes the grown table; reconcile then agrees.
        self.assertEqual(self.run_notebook("02_copy_dataset", dataset="shop", tables="orders", mode="overwrite"), 1)
        self.assertEqual(self.report("copy_report_shop.json")["tables"]["shop.orders"]["status"], "verified")
        self.assertEqual(self.spark.table("spark_catalog.shop.orders").count(), 4)

    def test_3_value_that_does_not_fit_fails_instead_of_null(self):
        schema, _rows = SOURCE["proj.shop.orders"]
        self.source["proj.shop.orders"] = (schema, [(9, "paid", dt.date(2026, 2, 1), Decimal("123456789.00"))])
        self.run_notebook("02_copy_dataset", dataset="shop", tables="orders", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.orders"]
        self.assertEqual(rec["status"], "failed")  # 9 integer digits do not fit DECIMAL(10,2)
        self.assertIn("NUMERIC_VALUE_OUT_OF_RANGE", rec["reason"])

    def test_4_diagnose_passes_when_the_connector_reads(self):
        import contextlib
        import io
        self.source = dict(SOURCE)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.run_notebook("00_diagnose")
        self.assertEqual((code, [l for l in out.getvalue().splitlines() if l.startswith("PROBLEM:")]), (0, []))
        self.assertIn("type_carried.opening", out.getvalue())
        self.assertIn("diagnose: OK", out.getvalue())
        self.assertNotIn("not-a-real-key", out.getvalue())  # the key is never printed

    def test_4_diagnose_finds_what_would_fail_later(self):
        import contextlib
        import io
        schema, _rows = SOURCE["proj.shop.orders"]
        self.source = {**SOURCE, "proj.shop.orders": (schema + ", _PARTITIONTIME timestamp", [])}
        kept = self.spark.catalog.tableExists("spark_catalog.shop.profiles")
        if kept:
            self.spark.sql("ALTER TABLE spark_catalog.shop.profiles RENAME TO shop.profiles_kept")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.shop")
        self.spark.sql("CREATE TABLE spark_catalog.shop.profiles (id BIGINT) USING DELTA")  # an earlier layout
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(self.run_notebook("00_diagnose"), 1)
        finally:
            self.spark.sql("DROP TABLE spark_catalog.shop.profiles")
            if kept:
                self.spark.sql("ALTER TABLE spark_catalog.shop.profiles_kept RENAME TO shop.profiles")
        problems = [l for l in out.getvalue().splitlines() if l.startswith("PROBLEM:")]
        self.assertEqual(len(problems), 2, problems)
        self.assertIn("proj.shop.orders: the connector's columns differ from the plan: "
                      "read but not planned: _PARTITIONTIME", problems[0])
        self.assertIn("spark_catalog.shop.profiles already exists with another layout: column 2:", problems[1])

    def test_5_export_route_loads_the_files_and_checks_bigquery(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"][0]["num_bytes"] = 2_000_000_000  # orders
        plan = build_plan(manifest, catalog="spark_catalog", oci_namespace="ns", export_over_gb=1,
                          export_bucket="stage-bkt")
        out = Path(self.tmp.name) / "migrated-export"
        migrate(plan, out_dir=out)
        # The exported files, "transferred": a local folder stands in for the oci:// folder.
        oci = f"oci://stage-bkt@ns/gcp-aidp-export/{plan['plan_id']}/shop/orders/"
        files = Path(self.tmp.name) / "exported" / "orders"
        schema, rows = SOURCE["proj.shop.orders"]  # BigQuery's Parquet: NUMERIC as decimal(38,9)
        self.spark.createDataFrame(rows, schema).write.parquet(str(files))
        text = (out / "notebooks/02_copy_dataset.ipynb").read_text().replace(oci, files.as_uri())
        self.assertIn(files.as_uri(), text)
        self.notebooks["02_copy_dataset"] = json.loads(text)

        self.source["proj.shop.orders"] = SOURCE["proj.shop.orders"]
        # The exit code also counts the profiles type_drift test 2 left in this dataset's report.
        self.run_notebook("02_copy_dataset", dataset="shop", tables="orders", mode="overwrite", verify="counts+sums")
        rec = self.report("copy_report_shop.json")["tables"]["shop.orders"]
        self.assertEqual((rec["status"], rec["export_rows"], rec["source_rows"], rec["target_rows"]),
                         ("verified", 3, 3, 3))
        self.assertEqual(rec["target_sums"]["total"], "100000010.50")

        # BigQuery moved on after the export: the load is not passed as verified.
        self.source["proj.shop.orders"] = (schema, rows + [(4, "paid", dt.date(2026, 1, 3), Decimal(1))])
        self.assertEqual(self.run_notebook("02_copy_dataset", dataset="shop", tables="orders", mode="overwrite"), 1)
        rec = self.report("copy_report_shop.json")["tables"]["shop.orders"]
        self.assertEqual((rec["status"], rec["export_rows"], rec["source_rows"]), ("count_mismatch", 3, 4))
        self.assertIn("changed since the export", rec["reason"])

    def test_6_export_route_text_columns_land_in_their_planned_types(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"][2]["num_bytes"] = 2_000_000_000  # type_carried
        plan = build_plan(manifest, catalog="spark_catalog", oci_namespace="ns", export_over_gb=1,
                          export_bucket="stage-bkt")
        route = next(a["target"]["load_from"] for a in plan["assets"] if a["source"]["name"] == "type_carried")
        self.assertEqual(sorted(route["as_text"]), ["opening", "payload", "precise"])
        out = Path(self.tmp.name) / "migrated-text"
        migrate(plan, out_dir=out)
        # What EXPORT DATA writes: the converted columns as text, DATETIME as a local timestamp.
        files = Path(self.tmp.name) / "exported" / "type_carried"
        self.spark.createDataFrame(
            [(1, '{"a":1}', dt.datetime(2026, 1, 1, 10), "10:30:00.123456", "12345678901234567890.0123456789"),
             (2, '"x"', dt.datetime(2026, 6, 30, 23, 59, 59, 999999), "00:00:00.000000", "-1")],
            "id long, payload string, local_time timestamp_ntz, opening string, precise string",
        ).write.parquet(str(files))
        text = (out / "notebooks/02_copy_dataset.ipynb").read_text().replace(route["oci"], files.as_uri())
        self.notebooks["02_copy_dataset"] = json.loads(text)

        self.run_notebook("02_copy_dataset", dataset="shop", tables="type_carried", mode="overwrite",
                          verify="counts+sums")  # exit code: see test 5
        rec = self.report("copy_report_shop.json")["tables"]["shop.type_carried"]
        self.assertEqual((rec["status"], rec["export_rows"], rec["target_rows"]), ("verified", 2, 2))
        row = self.spark.table("spark_catalog.shop.type_carried").where("id = 1").collect()[0]
        self.assertEqual((row["payload"], row["opening"]), ('{"a":1}', "10:30:00.123456"))
        self.assertEqual(row["precise"], Decimal("12345678901234567890.0123456789"))  # text → DECIMAL(30,10), exact
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        self.assertEqual(self.spark.sql("SELECT CAST(local_time AS STRING) FROM spark_catalog.shop.type_carried "
                                        "WHERE id = 2").collect()[0][0], "2026-06-30 23:59:59.999999")


    def test_6b_a_table_the_connector_cannot_read_is_counted_from_its_export(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"].append(
            {"dataset": "shop", "name": "spans", "num_rows": 2, "num_bytes": 1000, "columns": [  # small: routed anyway
                C("id", "INTEGER"), C("dwell", "INTERVAL"), C("w", "RANGE", rangeElementType={"type": "DATE"})]})
        plan = build_plan(manifest, catalog="spark_catalog", oci_namespace="ns", export_over_gb=1,
                          export_bucket="stage-bkt", interval="string", range_="struct")
        route = next(a["target"]["load_from"] for a in plan["assets"] if a["source"]["name"] == "spans")
        self.assertEqual(route["connector_unreadable"], ["dwell INTERVAL", "w RANGE"])
        out = Path(self.tmp.name) / "migrated-spans"
        migrate(plan, out_dir=out)
        # What EXPORT DATA writes, and the row count `export` puts beside the files.
        files = Path(self.tmp.name) / "exported" / "spans"
        self.spark.createDataFrame(
            [(1, "0-0 1 2:30:00", (dt.date(2026, 1, 1), dt.date(2026, 2, 1))), (2, "1-2 0 0:0:0", (None, None))],
            "id long, dwell string, w struct<start:date,end:date>").write.parquet(str(files))
        files.with_name("spans.rows.json").write_text('{"rows": 2, "job": "job0"}')
        self.notebooks.update({stage: json.loads((out / f"notebooks/{stage}.ipynb").read_text()
                                                 .replace(route["oci"].rstrip("/"), files.as_uri()))
                               for stage in ("00_diagnose", "02_copy_dataset")})

        class Refuses(dict):  # connector 0.45 on AIDP: "Unexpected type: INTERVAL", reading the schema
            def __getitem__(self, fqn):
                if fqn == "proj.shop.spans":
                    raise RuntimeError("java.lang.IllegalStateException: Unexpected type: INTERVAL")
                return super().__getitem__(fqn)
        self.source = Refuses(SOURCE)
        import contextlib
        import io
        out_text = io.StringIO()
        with contextlib.redirect_stdout(out_text):
            self.run_notebook("00_diagnose")
        self.assertNotIn("Unexpected type", out_text.getvalue())  # not read through the connector
        self.assertIn("2 rows exported", out_text.getvalue())
        ddl = "\n".join(l for l in (out / "tables/shop.spans.sql").read_text().splitlines() if not l.startswith("--"))
        self.spark.sql(ddl.replace("`spark_catalog`.", "spark_catalog."))  # what 01_structure runs
        self.run_notebook("02_copy_dataset", dataset="shop", tables="spans", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.spans"]
        self.assertEqual((rec["status"], rec["source_rows"], rec["target_rows"]), ("verified", 2, 2))
        row = self.spark.table("spark_catalog.shop.spans").where("id = 1").collect()[0]
        self.assertEqual((row["dwell"], row["w"]["start"]), ("0-0 1 2:30:00", dt.date(2026, 1, 1)))

    def test_7_export_route_text_inside_structs_and_arrays(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"].append(
            {"dataset": "shop", "name": "nested_text", "num_rows": 2, "num_bytes": 2_000_000_000, "columns": [
                C("id", "INTEGER"),
                C("s", "RECORD", fields=[C("j", "JSON"), C("t", "TIME"),
                                         C("p", "BIGNUMERIC", precision="30", scale="10")]),
                C("items", "RECORD", "REPEATED", fields=[C("sku", "STRING"), C("tms", "TIME", "REPEATED")])]})
        plan = build_plan(manifest, catalog="spark_catalog", oci_namespace="ns", export_over_gb=1,
                          export_bucket="stage-bkt")
        route = next(a["target"]["load_from"] for a in plan["assets"] if a["source"]["name"] == "nested_text")
        self.assertEqual(sorted(route["as_text"]), ["items", "s"])
        out = Path(self.tmp.name) / "migrated-nested"
        migrate(plan, out_dir=out)
        # What EXPORT DATA writes for that SELECT: the same nesting, the converted fields as text.
        files = Path(self.tmp.name) / "exported" / "nested_text"
        self.spark.createDataFrame(
            [(1, ('{"a":1}', "10:30:00.123456", "12345678901234567890.0123456789"),
              [("x", ["01:02:03.000000", "23:59:59.999999"]), ("y", [])]),
             (2, None, [])],
            "id long, s struct<j:string,t:string,p:string>, items array<struct<sku:string,tms:array<string>>>",
        ).write.parquet(str(files))
        for stage in ("01_structure", "02_copy_dataset"):
            text = (out / f"notebooks/{stage}.ipynb").read_text().replace(route["oci"], files.as_uri())
            self.notebooks[stage] = json.loads(text)
        self.run_notebook("01_structure")
        self.source["proj.shop.nested_text"] = ("id long", [(1,), (2,)])  # only its count is read
        self.run_notebook("02_copy_dataset", dataset="shop", tables="nested_text", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.nested_text"]
        self.assertEqual((rec["status"], rec["export_rows"], rec["target_rows"]), ("verified", 2, 2))
        rows = {r["id"]: r for r in self.spark.table("spark_catalog.shop.nested_text").collect()}
        self.assertEqual(rows[1]["s"]["p"], Decimal("12345678901234567890.0123456789"))  # text → DECIMAL(30,10)
        self.assertEqual((rows[1]["s"]["j"], rows[1]["s"]["t"]), ('{"a":1}', "10:30:00.123456"))
        self.assertEqual([(i["sku"], i["tms"]) for i in rows[1]["items"]],
                         [("x", ["01:02:03.000000", "23:59:59.999999"]), ("y", [])])
        self.assertIsNone(rows[2]["s"])  # a NULL struct stays NULL

    def test_8_connector_time_inside_structs_and_arrays_lands_as_text(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"].append(
            {"dataset": "shop", "name": "nested_time", "num_rows": 2, "columns": [
                C("id", "INTEGER"), C("s", "RECORD", fields=[C("t", "TIME"), C("n", "INTEGER")]),
                C("items", "RECORD", "REPEATED", fields=[C("sku", "STRING"), C("tms", "TIME", "REPEATED")])]})
        out = Path(self.tmp.name) / "migrated-nested-time"
        migrate(build_plan(manifest, catalog="spark_catalog", oci_namespace="ns"), out_dir=out)
        for stage in ("01_structure", "02_copy_dataset"):
            self.notebooks[stage] = json.loads((out / f"notebooks/{stage}.ipynb").read_text())
        self.run_notebook("01_structure")
        # As the connector delivers TIME at any depth: microseconds since midnight.
        self.source["proj.shop.nested_time"] = (
            "id long, s struct<t:bigint,n:bigint>, items array<struct<sku:string,tms:array<bigint>>>",
            [(1, (37_800_123_456, 7), [("x", [3_723_000_000, 86_399_999_999]), ("y", [])]), (2, None, [])])
        self.run_notebook("02_copy_dataset", dataset="shop", tables="nested_time", mode="overwrite")
        rec = self.report("copy_report_shop.json")["tables"]["shop.nested_time"]
        self.assertEqual((rec["status"], rec["target_rows"]), ("verified", 2))
        rows = {r["id"]: r for r in self.spark.table("spark_catalog.shop.nested_time").collect()}
        self.assertEqual((rows[1]["s"]["t"], rows[1]["s"]["n"]), ("10:30:00.123456", 7))
        self.assertEqual([(i["sku"], i["tms"]) for i in rows[1]["items"]],
                         [("x", ["01:02:03.000000", "23:59:59.999999"]), ("y", [])])
        self.assertIsNone(rows[2]["s"])

    def test_9_diagnose_checks_the_export_files_are_on_oci(self):
        manifest = json.loads(json.dumps(MANIFEST))
        manifest["sources"]["bigquery"]["items"]["tables"][0]["num_bytes"] = 2_000_000_000  # orders
        plan = build_plan(manifest, catalog="spark_catalog", oci_namespace="ns", export_over_gb=1,
                          export_bucket="stage-bkt")
        out = Path(self.tmp.name) / "migrated-diagnose"
        migrate(plan, out_dir=out)
        oci = next(a["target"]["load_from"]["oci"] for a in plan["assets"] if a["source"]["name"] == "orders")
        files = Path(self.tmp.name) / "exported-diagnose" / "orders"  # not written yet: export has not run
        text = (out / "notebooks/00_diagnose.ipynb").read_text().replace(oci, files.as_uri())
        self.notebooks["00_diagnose"] = json.loads(text)
        self.source = dict(SOURCE)
        self.assertEqual(self.run_notebook("00_diagnose"), 1)
        schema, rows = SOURCE["proj.shop.orders"]
        self.spark.createDataFrame(rows, schema).write.parquet(str(files))
        self.assertEqual(self.run_notebook("00_diagnose"), 0)

if __name__ == "__main__":
    unittest.main()
