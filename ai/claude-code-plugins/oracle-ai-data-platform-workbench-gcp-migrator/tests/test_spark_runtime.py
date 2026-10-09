"""Run the demo's generated SQL on a local Spark 3.5 + Delta.

Skipped unless `pyspark` and `delta-spark` are importable (the first run
downloads the Delta jars from Maven):

    pip install pyspark==3.5.9 delta-spark==3.2.1
    python -m unittest tests.test_spark_runtime

The invariant: every artifact whose findings are only rewrites and caveats
must run. A caveat is a rewrite that is exact under a stated condition, so it
must at least be valid Spark. Flagged and blocked artifacts may fail; that is
what the flag is for.
"""
from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover - optional dependency
    SparkSession = None

from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan
from gcp_aidp.translate.googlesql_to_spark import translate

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())
ORDER = ("schemas", "tables", "materialized_views", "views", "saved_queries", "scheduled_queries", "functions")


def _statements(text: str, catalog: str) -> list[str]:
    body = "\n".join(line for line in text.splitlines() if not line.startswith("--"))
    body = body.replace(f"`{catalog}`.", "`spark_catalog`.")  # one local catalog stands in for AIDP's
    return [s.strip() for s in re.split(r";\s*\n", body) if s.strip().strip(";")]


@unittest.skipIf(SparkSession is None, "pyspark and delta-spark are not installed")
class SparkRuntime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        builder = (SparkSession.builder.master("local[1]").appName("gcp-aidp-runtime")
                   .config("spark.ui.enabled", "false")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.warehouse.dir", f"{cls.tmp.name}/warehouse")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("OFF")
        plan = build_plan(DEMO)
        cls.catalog = plan["target"]["catalog"]
        cls.out = Path(cls.tmp.name) / "migrated"
        cls.report = migrate(plan, out_dir=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp.cleanup()

    def test_rewrites_and_caveats_run_on_spark(self):
        outcomes = {}
        results = sorted((r for r in self.report["results"] if r.get("output_path")),
                         key=lambda r: ORDER.index(r["output_path"].split("/")[0])
                         if r["output_path"].split("/")[0] in ORDER else len(ORDER))
        for r in results:
            if r["output_path"].split("/")[0] not in ORDER or r["status"] == "blocked":
                continue
            error = None
            for stmt in _statements((self.out / r["output_path"]).read_text(), self.catalog):
                try:
                    self.spark.sql(stmt).collect()
                except Exception as exc:  # noqa: BLE001 - any Spark failure is the outcome
                    error = (str(exc).strip().splitlines() or [type(exc).__name__])[0][:200]
                    break
            outcomes[r["asset_id"]] = (r, error)

        must_run = {aid: err for aid, (r, err) in outcomes.items()
                    if all(f["severity"] in ("rewrite", "caveat", "info") for f in r["findings"])}
        failed = {aid: err for aid, err in must_run.items() if err}
        self.assertFalse(failed, f"artifacts with no flag failed on Spark: {failed}")
        self.assertGreater(len(must_run), 40)
        # The flagged SQL function really does not parse: the flag is not a false alarm.
        self.assertTrue(outcomes["bigquery.routine.sales.net_price"][1])
        print(f"\n{len(must_run)} artifacts ran on Spark; flagged ones that failed:")
        for aid, (r, err) in sorted(outcomes.items()):
            if err and aid not in must_run:
                print(f"  {aid}: {err}")

    def test_G15_QUALIFY_keeps_the_same_rows(self):
        self.spark.sql("CREATE OR REPLACE TEMP VIEW q AS SELECT * FROM VALUES (1, 10, 5), (2, 10, 7), "
                       "(3, 11, 5), (4, 11, 5), (5, 12, 1) AS q(id, k, v)")
        for sql, rows in [
            ("SELECT * FROM q QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC, id) = 1",
             [(2, 10, 7), (3, 11, 5), (5, 12, 1)]),
            ("SELECT k, COUNT(*) AS n FROM q GROUP BY k QUALIFY RANK() OVER (ORDER BY COUNT(*) DESC) = 1",
             [(10, 2), (11, 2)]),
            ("SELECT DISTINCT k, v FROM q QUALIFY COUNT(*) OVER (PARTITION BY k) > 1", [(10, 5), (10, 7), (11, 5)]),
            ("SELECT id, RANK() OVER (ORDER BY v) AS r FROM q QUALIFY r = 1 ORDER BY id", [(5, 1)]),
        ]:
            r = translate(sql)
            self.assertNotEqual(r.status, "blocked", r.findings)  # the ORDER BY one carries a caveat
            self.assertEqual(sorted(tuple(x) for x in self.spark.sql(r.sql).collect()), rows, r.sql)


if __name__ == "__main__":
    unittest.main()
