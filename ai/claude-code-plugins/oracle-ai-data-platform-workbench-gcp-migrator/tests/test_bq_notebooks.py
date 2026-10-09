"""BigQuery Studio notebooks → AIDP notebooks (references/notebook-translation.md)."""
from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan
from gcp_aidp.publish import plan_publish
from gcp_aidp.translate.googlesql_to_spark import Context
from gcp_aidp.translate.notebook_to_aidp import translate_notebook

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())
CTX = Context(project="proj", relations={("sales", "orders"): ("cat", "sales", "orders")},
              buckets={"landing": "landing-oci"}, namespace="ns")
ORDERS = "`cat`.`sales`.`orders`"


def cell(source):
    out, findings = translate_notebook([{"cell_type": "code", "source": source}], CTX)
    return out[0]["source"], {(f.rule, f.severity) for f in findings}


class Rules(unittest.TestCase):
    def test_N01_BIGQUERY_MAGIC(self):
        code, rules = cell("%%bigquery orders\nSELECT * FROM `proj.sales.orders`")
        self.assertEqual(code, f'orders = spark.sql("""SELECT * FROM {ORDERS}""").toPandas()')
        self.assertIn(("N01_BIGQUERY_MAGIC", "rewrite"), rules)
        code, _ = cell("%%bigquery\nSELECT 1")
        self.assertEqual(code, 'spark.sql("""SELECT 1""").toPandas()')  # no variable: the result is shown
        code, _ = cell("%%bigquery t\nproj.sales.orders")
        self.assertEqual(code, f't = spark.table("{ORDERS}").toPandas()')
        self.assertIn(("N01_BIGQUERY_MAGIC", "flag"), cell("%%bigquery t\nproj.sales.missing")[1])
        ast.parse(code)

    def test_N02_MAGIC_OPTION(self):
        code, rules = cell('%%bigquery r --params {"x": 1, "y": "a b"}\nSELECT @x')
        self.assertTrue(code.startswith("r = spark.sql("))  # the JSON's spaces do not break the line
        self.assertIn(("N02_MAGIC_OPTION", "flag"), rules)
        self.assertIn(("N02_MAGIC_OPTION", "flag"), cell("%%bigquery --project other\nSELECT 1")[1])
        code, rules = cell("%%bigquery r --use_legacy_sql\nSELECT 1")
        self.assertIn(("N02_MAGIC_OPTION", "block"), rules)
        self.assertTrue(all(line.startswith("#") for line in code.splitlines()))

    def test_N03_NOTEBOOK_COMMAND(self):
        code, rules = cell("%load_ext bigquery_magics\n!pip install seaborn\nx = 1")
        self.assertEqual(code, "# gcp-aidp: !pip install seaborn\nx = 1")
        self.assertEqual(rules, {("N03_NOTEBOOK_COMMAND", "rewrite"), ("N03_NOTEBOOK_COMMAND", "flag")})
        self.assertIn(("N03_NOTEBOOK_COMMAND", "block"), cell("%%bash\nls")[1])

    def test_python_cells_take_the_dataproc_rules_and_markdown_is_kept(self):
        out, findings = translate_notebook([{"cell_type": "markdown", "source": "# gs://landing/x"},
                                            {"cell_type": "code", "source": "import pandas as pd\n"
                                             "df = pd.read_csv('gs://landing/x.csv')"}], CTX)
        self.assertEqual(out[0]["source"], "# gs://landing/x")
        self.assertIn("oci://landing-oci@ns/x.csv", out[1]["source"])
        self.assertEqual({f.rule for f in findings}, {"P01_GCS_PATH"})


class Inventory(unittest.TestCase):
    def test_notebooks_dir_keeps_sources_and_drops_outputs(self):
        from gcp_aidp.inventory.bigquery import _notebooks
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "Churn.ipynb").write_text(json.dumps({"cells": [
                {"cell_type": "code", "source": ["%%bigquery x\n", "SELECT 1"],
                 "outputs": [{"data": {"text/plain": "customer emails here"}}]}]}))
            (Path(d) / "broken.ipynb").write_text("{not json")
            warnings = []
            rows = _notebooks(d, warnings)
        self.assertEqual(rows, [{"id": "Churn", "name": "Churn",
                                 "cells": [{"cell_type": "code", "source": "%%bigquery x\nSELECT 1"}]}])
        self.assertIn("broken.ipynb", warnings[0])


class Migrate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        cls.report = migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=cls.out)
        cls.by_id = {r["asset_id"]: r for r in cls.report["results"]}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_a_translated_notebook_is_written_and_published(self):
        r = self.by_id["bigquery.notebook.nb-0001"]
        self.assertEqual(r["status"], "needs_manual_review")  # !pip and bigframes are flagged
        nb = json.loads((self.out / r["output_path"]).read_text())
        code = [c["source"] for c in nb["cells"] if c["cell_type"] == "code"]
        self.assertIn("segments = spark.sql(", code[1])
        self.assertIn("count_if", code[1])
        for source in code:
            ast.parse(source)
        remote = [n["remote"] for n in plan_publish(self.out, prefix="ana", cluster_key="ck")["notebooks"]]
        self.assertIn("/Workspace/ana/Customer_segmentation_exploration.ipynb", remote)

    def test_a_notebook_with_a_blocked_cell_stays_local(self):
        r = self.by_id["bigquery.notebook.nb-0002"]
        self.assertEqual(r["status"], "blocked")
        self.assertNotIn(r["output_path"], self.report["notebooks"])


if __name__ == "__main__":
    unittest.main()
