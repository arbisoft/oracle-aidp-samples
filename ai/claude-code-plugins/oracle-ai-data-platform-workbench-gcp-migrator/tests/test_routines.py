"""SQL table functions and procedures (D07, D08), their arguments as Spark named parameters (G25)."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan
from gcp_aidp.translate.googlesql_to_spark import bind_parameters
from tests.test_plan import _manifest

ORDERS = {"dataset": "d", "name": "orders", "columns": [{"name": "id", "type": "INTEGER"},
                                                        {"name": "status", "type": "STRING"},
                                                        {"name": "order_date", "type": "DATE"}]}


def routine(name, kind, body, *arguments):
    return {"dataset": "d", "name": name, "routine_type": kind, "language": "SQL", "body": body,
            "arguments": list(arguments)}


def migrated(*routines, return_type=None):
    """{routine name: (result, notebook or None, SQL file text)} after plan and migrate."""
    routines = [{**r, "return_type": return_type} if return_type else r for r in routines]
    plan = build_plan(_manifest(datasets=[{"name": "d"}], tables=[ORDERS], routines=list(routines)), catalog="cat")
    with tempfile.TemporaryDirectory() as tmp:
        report = migrate(plan, out_dir=Path(tmp))
        out = {}
        for r in report["results"]:
            if ".routine." in r["asset_id"]:
                nb = json.loads((Path(tmp) / r["notebook"]).read_text()) if r.get("notebook") else None
                out[r["asset_id"].rsplit(".", 1)[1]] = (r, nb, (Path(tmp) / r["output_path"]).read_text())
        return out


def code(nb) -> list[str]:
    return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]


class Parameters(unittest.TestCase):
    def test_G25_ROUTINE_PARAMETER(self):
        sql, f = bind_parameters("SELECT * FROM t WHERE order_date >= d AND t.d = 1 AND D < 2", ["d"])
        self.assertEqual(sql, "SELECT * FROM t WHERE order_date >= :d AND t.d = 1 AND :d < 2")  # t.d: a column
        self.assertEqual([x.severity for x in f], ["rewrite"])
        # Never guessed: a column of the same name, an alias, a function, a keyword.
        self.assertEqual(bind_parameters("SELECT d FROM t", ["d"], {"D"})[1][0].severity, "block")
        self.assertEqual(bind_parameters("SELECT x AS d FROM t", ["d"])[1][0].severity, "block")
        self.assertEqual(bind_parameters("SELECT d(1)", ["d"])[1][0].severity, "block")
        self.assertEqual(bind_parameters("SELECT 1", ["select"])[1][0].severity, "block")


class TableFunction(unittest.TestCase):
    def test_D07_TABLE_FUNCTION_is_a_python_function_over_spark_sql(self):
        r, nb, _ = migrated(routine("since", "TABLE_VALUED_FUNCTION",
                                    "SELECT * FROM d.orders WHERE order_date >= start_day",
                                    {"name": "start_day", "type": "DATE"}))["since"]
        self.assertEqual(r["status"], "ok")
        self.assertIn("D07_TABLE_FUNCTION", [f["rule"] for f in r["findings"]])
        self.assertEqual(code(nb), [('def since(start_day):\n'
                                     '    """d.since(start_day DATE), migrated from BigQuery: its rows as a DataFrame."""\n'
                                     '    return spark.sql("SELECT * FROM `cat`.`d`.`orders` WHERE order_date >= :start_day", '
                                     'args={"start_day": start_day})')])
        self.assertIn("pass a Python `datetime.date`", "".join(nb["cells"][0]["source"]))

    def test_a_parameter_named_like_a_column_it_reads_blocks(self):
        r, nb, _ = migrated(routine("by_status", "TABLE_VALUED_FUNCTION", "SELECT * FROM d.orders WHERE status = status",
                                    {"name": "status", "type": "STRING"}))["by_status"]
        self.assertEqual((r["status"], nb), ("blocked", None))  # not published

    def test_an_array_argument_blocks(self):
        r, _, _ = migrated(routine("f", "TABLE_VALUED_FUNCTION", "SELECT * FROM d.orders WHERE id IN UNNEST(ids)",
                                   {"name": "ids", "type": "ARRAY<INT64>"}))["f"]
        self.assertEqual(r["status"], "blocked")


class Procedure(unittest.TestCase):
    def test_D08_PROCEDURE_statements_run_in_order_with_params(self):
        body = ("BEGIN\n  DELETE FROM d.orders WHERE id = old_id;\n"
                "  CREATE OR REPLACE TABLE d.status_summary AS SELECT status, COUNT(*) AS n FROM d.orders GROUP BY status;\n"
                "  SELECT * FROM d.status_summary;\nEND")
        r, nb, sql = migrated(routine("tidy", "PROCEDURE", body, {"name": "old_id", "type": "INT64", "mode": "IN"}))["tidy"]
        self.assertEqual(r["status"], "ok", r["findings"])
        cells = code(nb)
        self.assertEqual(cells[0], "# ── PARAMETERS: the procedure's arguments ──\nPARAMS = {\n    'old_id': None,  # INT64\n}")
        self.assertEqual(cells[1], 'spark.sql("DELETE FROM `cat`.`d`.`orders` WHERE id = :old_id", '
                                   "args={'old_id': PARAMS['old_id']})")
        # The table it creates lands in the plan's catalog, as Delta; later statements find it there.
        self.assertIn("CREATE OR REPLACE TABLE `cat`.`d`.`status_summary` USING DELTA AS", cells[2])
        self.assertEqual(cells[3], 'spark.sql("SELECT * FROM `cat`.`d`.`status_summary`")')
        self.assertIn("USING DELTA", sql)

    def test_procedural_logic_and_out_arguments_block(self):
        got = migrated(routine("loop", "PROCEDURE", "BEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT x;\nEND"),
                       routine("out", "PROCEDURE", "BEGIN\n  SELECT 1;\nEND", {"name": "n", "type": "INT64", "mode": "OUT"}),
                       routine("old", "PROCEDURE", "BEGIN\n  SELECT 1;\nEND", {"name": "n", "type": "INT64"}))
        self.assertEqual((got["loop"][0]["status"], got["loop"][1]), ("blocked", None))
        self.assertIn("D08_PROCEDURE", [f["rule"] for f in got["loop"][0]["findings"] if f["severity"] == "block"])
        self.assertEqual(got["out"][0]["status"], "blocked")
        self.assertIn("run inventory again", " ".join(f["detail"] for f in got["old"][0]["findings"]))  # no mode


try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover - optional dependency
    SparkSession = None


@unittest.skipIf(SparkSession is None, "pyspark and delta-spark are not installed")
class RunsOnSpark(unittest.TestCase):
    """The generated notebooks run on local Spark 3.5 + Delta, the named parameters bound."""

    def test_table_function_and_procedure_notebooks_run(self):
        import datetime as dt
        with tempfile.TemporaryDirectory() as tmp:
            builder = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
                       .config("spark.sql.warehouse.dir", f"{tmp}/wh")
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.sparkContext.setLogLevel("OFF")
            try:
                spark.sql("CREATE SCHEMA IF NOT EXISTS d")
                spark.sql("CREATE TABLE d.orders (id BIGINT, status STRING, order_date DATE) USING DELTA")
                spark.sql("INSERT INTO d.orders VALUES (1, 'a', DATE'2026-01-01'), (2, 'b', DATE'2026-02-01'), "
                          "(3, 'b', DATE'2026-03-01')")
                got = migrated(
                    routine("since", "TABLE_VALUED_FUNCTION", "SELECT * FROM d.orders WHERE order_date >= start_day",
                            {"name": "start_day", "type": "DATE"}),
                    routine("tidy", "PROCEDURE", "BEGIN\n  DELETE FROM d.orders WHERE id = old_id;\n"
                            "  INSERT INTO d.orders SELECT id + 10, status, order_date FROM d.orders WHERE status = s;\nEND",
                            {"name": "old_id", "type": "INT64", "mode": "IN"}, {"name": "s", "type": "STRING", "mode": "IN"}))
                ns = {"spark": spark}
                for source in code(got["since"][1]):
                    exec(source.replace("`cat`.", "`spark_catalog`."), ns)  # noqa: S102 - runs the generated notebook
                self.assertEqual(sorted(r.id for r in ns["since"](dt.date(2026, 2, 1)).collect()), [2, 3])
                cells = [c.replace("`cat`.", "`spark_catalog`.") for c in code(got["tidy"][1])]
                exec(cells[0], ns)  # noqa: S102
                ns["PARAMS"].update(old_id=1, s="b")
                for source in cells[1:]:
                    exec(source, ns)  # noqa: S102
                self.assertEqual(sorted(r.id for r in spark.table("d.orders").collect()), [2, 3, 12, 13])
                udf = migrated(routine("net_price", "SCALAR_FUNCTION", "price * (1 - IFNULL(discount, 0))",
                                       {"name": "price", "type": "FLOAT64"}, {"name": "discount", "type": "FLOAT64"}),
                               return_type="FLOAT64")["net_price"]
                self.assertEqual(udf[0]["status"], "ok")
                exec(code(udf[1])[0], ns)  # noqa: S102
                self.assertEqual(spark.sql("SELECT net_price(10, 0.25), net_price(10, NULL), net_price(NULL, 1)").first()[:],
                                 (7.5, 10.0, None))
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
