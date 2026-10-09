"""One test per Dataproc → AIDP rule (references/dataproc-translation.md)."""
from __future__ import annotations

import ast
import unittest

from gcp_aidp.translate.googlesql_to_spark import Context
from gcp_aidp.translate.pyspark_to_aidp import spark_properties, translate_pyspark, translate_spark_sql

CTX = Context(project="proj", relations={("sales", "orders"): ("cat", "sales", "orders")},
              buckets={"landing": "landing-oci"}, namespace="ns")
ORDERS = "`cat`.`sales`.`orders`"


def py(code):
    return translate_pyspark(code, CTX)


def rules(r, severity=None):
    return {(f.rule, f.severity) for f in r.findings if severity is None or f.severity == severity}


class Rewrites(unittest.TestCase):
    def test_P01_GCS_PATH(self):
        r = py('df = spark.read.json(f"gs://landing/in/{day}/")\nx = "gs://other/y"\n')
        self.assertIn('f"oci://landing-oci@ns/in/{day}/"', r.sql)
        self.assertIn('"gs://other/y"', r.sql)  # an unmapped bucket stays, flagged
        self.assertEqual(rules(r), {("P01_GCS_PATH", "rewrite"), ("P01_GCS_PATH", "flag")})

    def test_P01_docstrings_and_comments_are_left(self):
        code = '"""Reads gs://landing/in."""\n# copies gs://landing/out\nx = 1\n'
        self.assertEqual(py(code).sql, code)

    def test_P02_BQ_READ(self):
        for read in ('spark.read.format("bigquery").load("proj.sales.orders")',
                     'spark.read.format("bigquery").option("table", "proj:sales.orders").load()',
                     'spark.read.format("bigquery").option("dataset", "sales").load("orders")',
                     'spark.read.format("bigquery").load("sales.orders")'):
            r = py(f"df = ({read}\n      .where('x > 1'))\n")
            self.assertEqual(r.sql, f"df = (spark.table(\"{ORDERS}\")\n      .where('x > 1'))\n", read)
            self.assertEqual(r.status, "ok", read)

    def test_P02_reads_it_cannot_rewrite_are_flagged(self):
        for read, why in (('spark.read.format("bigquery").load("proj.sales.missing")', "not in the migration plan"),
                          ('spark.read.format("bigquery").load("other.sales.orders")', "another project"),
                          ('spark.read.format("bigquery").load(name)', "not a string literal"),
                          ('spark.read.format("bigquery").option("filter", "a > 1").load("sales.orders")', "filter")):
            r = py(f"df = {read}\n")
            self.assertEqual(r.sql, f"df = {read}\n", read)
            self.assertIn(why, " ".join(f.detail for f in r.findings), read)

    def test_P02_a_read_across_variables_is_never_reported_clean(self):
        r = py('reader = spark.read.format("bigquery")\ndf = reader.load("proj.sales.orders")\n')
        self.assertEqual(r.status, "needs_manual_review")
        self.assertIn("line 1", r.findings[0].detail)

    def test_P05_CONNECTOR_SETTING(self):
        code = ('spark = (SparkSession.builder.appName("a")\n'
                '         .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.45.0")\n'
                '         .getOrCreate())\n'
                'if True:\n    spark.conf.set("temporaryGcsBucket", "landing")\n')
        r = py(code)
        self.assertNotIn("spark-bigquery", r.sql)
        self.assertIn('.appName("a")\n         .getOrCreate()', r.sql)
        self.assertIn("    pass  # gcp-aidp: removed the BigQuery connector setting temporaryGcsBucket", r.sql)
        ast.parse(r.sql)  # the if block keeps a statement
        self.assertEqual(rules(r), {("P05_CONNECTOR_SETTING", "rewrite")})

    def test_P06_HIVE_NAME_in_spark_sql(self):
        r = py('spark.sql("""SELECT *\nFROM sales.orders""")\nspark.sql(\'SELECT 1 FROM hive_db.t\')\n')
        self.assertIn(f'spark.sql("""SELECT *\nFROM {ORDERS}""")', r.sql)  # its own quotes kept
        self.assertEqual(rules(r), {("P06_HIVE_NAME", "caveat"), ("P06_HIVE_NAME", "flag")})
        self.assertIn(("P06_HIVE_NAME", "flag"), rules(py('spark.sql(f"SELECT * FROM {t}")\n')))

    def test_a_path_in_spark_sql_is_edited_once(self):
        r = py("spark.sql(\"INSERT OVERWRITE DIRECTORY 'gs://landing/o/' SELECT * FROM sales.orders\")\n")
        self.assertEqual(r.sql, f"spark.sql(\"INSERT OVERWRITE DIRECTORY 'oci://landing-oci@ns/o/' "
                                f"SELECT * FROM {ORDERS}\")\n")

    def test_text_before_an_edit_may_be_non_ascii(self):
        r = py('note = "café"; df = spark.read.json("gs://landing/x")\n')
        self.assertEqual(r.sql, 'note = "café"; df = spark.read.json("oci://landing-oci@ns/x")\n')


class Flags(unittest.TestCase):
    def test_P03_BQ_QUERY_READ(self):
        for read in ('spark.read.format("bigquery").option("query", "SELECT 1").load()',
                     'spark.read.format("bigquery").load("SELECT * FROM sales.orders")'):
            self.assertIn(("P03_BQ_QUERY_READ", "flag"), rules(py(f"df = {read}\n")), read)

    def test_P04_BQ_WRITE(self):
        r = py('df.write.format("bigquery").option("table", "sales.out").mode("overwrite").save()\n')
        self.assertEqual(rules(r), {("P04_BQ_WRITE", "flag")})
        self.assertIn("sales.out", r.findings[0].detail)

    def test_P07_GOOGLE_LIBRARY(self):
        r = py("from google.cloud import storage\nimport gcsfs, json\n")
        self.assertEqual({f.detail.split(":")[0] for f in r.findings}, {"imports google.cloud", "imports gcsfs"})

    def test_P09_LOCAL_FILES(self):
        r = py("import os\nconf = open(os.path.join(os.path.dirname(__file__), 'conf.yaml'))\n")
        self.assertEqual(rules(r), {("P09_LOCAL_FILES", "flag")})

    def test_P08_SQL_VARIABLE(self):
        r = translate_spark_sql("SELECT * FROM sales.orders WHERE d = '${run_date}'", CTX)
        self.assertIn(("P08_SQL_VARIABLE", "flag"), rules(r))
        self.assertIn(("P08_SQL_VARIABLE", "flag"), rules(translate_spark_sql("SELECT 1", CTX, {"d": "x"})))


class Properties(unittest.TestCase):
    PROPS = {"spark.sql.shuffle.partitions": "400", "spark.executor.memory": "8g",
             "spark.dataproc.driver.compute.tier": "premium", "spark.sql.extensions": "x.Ext", "spark.jars": "a.jar"}

    def test_P11_SPARK_PROPERTY(self):
        code, f = spark_properties('"""Doc."""\nfrom __future__ import annotations\nx = 1\n', self.PROPS, True)
        self.assertEqual(code, '"""Doc."""\nfrom __future__ import annotations\n'
                         "spark.conf.set('spark.sql.shuffle.partitions', '400')  # gcp-aidp: Dataproc job property\n"
                         "x = 1\n")
        ast.parse(code)
        self.assertEqual(sorted((x.detail.split("=")[0].split(":")[0], x.severity) for x in f), [
            ("spark.dataproc.driver.compute.tier", "info"), ("spark.executor.memory", "info"),
            ("spark.jars", "flag"), ("spark.sql.extensions", "flag"), ("spark.sql.shuffle.partitions", "rewrite")])

    def test_P11_in_spark_sql_and_without_a_runtime_key(self):
        code, _ = spark_properties("SELECT 1", {"spark.sql.ansi.enabled": "true", "spark.sql.x": "a;b"}, False)
        self.assertEqual(code, "SET spark.sql.ansi.enabled=true;\nSELECT 1")
        self.assertEqual(spark_properties("x = 1", {"spark.executor.cores": "4"}, True)[0], "x = 1")


class Blocks(unittest.TestCase):
    def test_P98_UNPARSABLE(self):
        r = py('print "python 2"\n')
        self.assertEqual((r.status, r.sql), ("blocked", 'print "python 2"\n'))
        self.assertEqual(rules(r), {("P98_UNPARSABLE", "block")})

    def test_P99_CODE_NOT_READ(self):
        self.assertEqual(rules(py(None)), {("P99_CODE_NOT_READ", "block")})
        self.assertEqual(rules(translate_spark_sql("", CTX)), {("P99_CODE_NOT_READ", "block")})


class SparkSql(unittest.TestCase):
    def test_names_and_paths_only(self):
        r = translate_spark_sql("SELECT a FROM sales.orders; INSERT OVERWRITE DIRECTORY 'gs://landing/r/' "
                                "SELECT 1 # not a comment in Spark", CTX)
        self.assertIn(ORDERS, r.sql)
        self.assertIn("'oci://landing-oci@ns/r/'", r.sql)
        self.assertEqual(rules(r), {("P06_HIVE_NAME", "caveat"), ("G21_GCS_PATH", "rewrite")})


if __name__ == "__main__":
    unittest.main()
