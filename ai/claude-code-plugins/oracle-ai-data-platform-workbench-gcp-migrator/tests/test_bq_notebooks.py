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
        # dataset.table: the magic reads it in the notebook's project (seen live: became spark.sql of a bare name)
        self.assertEqual(cell("%%bigquery\nsales.orders")[0], f'spark.table("{ORDERS}").toPandas()')
        self.assertIn(("N01_BIGQUERY_MAGIC", "block"), cell("%%bigquery\norders")[1])  # never spark.sql("orders")
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
        out, _ = translate_notebook([{"cell_type": "code", "source": "%load_ext bigquery_magics"},
                                     {"cell_type": "code", "source": ""}], CTX)
        self.assertEqual(out, [{"cell_type": "code", "source": ""}])  # emptied by a removal: dropped; empty: kept

    def test_N04_SQL_CELL(self):
        # A BigQuery Studio SQL cell, as the Dataform API returned it, 2026-10-09.
        sql_cell = ('# sql_engine: bigquery\n# output_variable: df\n# output_mode: table\n# start _sql\n'
                    '_sql = """\nSELECT status, COUNTIF(n > 1) AS c FROM `proj.sales.orders` GROUP BY status\n'
                    '""" # end _sql\nfrom google.colab.sql import bigquery as _bqsqlcell\n'
                    'df = _bqsqlcell.run(_sql)\ndf')
        code, rules = cell(sql_cell)
        self.assertEqual(code, f'df = spark.sql("""SELECT status, count_if(n > 1) AS c FROM {ORDERS} GROUP BY status""")'
                               '.toPandas()\ndf')
        self.assertIn(("N04_SQL_CELL", "rewrite"), rules)
        self.assertIn(("N04_SQL_CELL", "flag"), cell(sql_cell.replace("sql_engine: bigquery", "sql_engine: other"))[1])

    def test_google_colab_is_never_passed_silently(self):
        self.assertIn(("P07_GOOGLE_LIBRARY", "flag"), cell("from google.colab import auth\nauth.authenticate_user()")[1])

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


class StudioNotebooks(unittest.TestCase):
    """Notebooks read through the Dataform API, shaped as it answered live (2026-10-09)."""

    class Fake:
        project = "my-project-123456"
        BASE = "https://dataform.googleapis.com/v1/projects/my-project-123456/locations"

        def __init__(self):
            import base64
            nb = {"cells": [{"cell_type": "code", "source": ["%%bigquery x\n", "SELECT 1"],
                             "outputs": [{"data": {"text/plain": "a query result"}}]}], "nbformat": 4}
            repo = lambda rid, labels: {"name": f"projects/p/locations/us-east1/repositories/{rid}",
                                        "displayName": f"repo {rid}", "labels": labels}
            self.responses = {
                f"{self.BASE}": {"locations": [{"locationId": "us-central1"}, {"locationId": "us-east1"}]},
                f"{self.BASE}/us-central1/repositories": {},
                f"{self.BASE}/us-east1/repositories": {"repositories": [
                    repo("nb1", {"single-file-asset-type": "notebook"}),
                    repo("q1", {"single-file-asset-type": "sql"}),
                    repo("ds1", {"single-file-asset-type": "data_canvas"}),
                    repo("pl1", {"bigquery-workflow": "preview"}),
                    repo("df1", None)]},
                f"{self.BASE}/us-east1/repositories/q1:readFile?path=content.sql":
                    {"contents": base64.b64encode(b"SELECT 2").decode()},
                f"{self.BASE}/us-east1/repositories/nb1:readFile?path=content.ipynb":
                    {"contents": base64.b64encode(json.dumps(nb).encode()).decode()},
            }
            self.calls = []

        def get(self, url, params=None):
            key = url + ("?" + "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else "")
            self.calls.append(key)
            return self.responses[key]

        def pages(self, url, key, params=None):
            return self.get(url, params).get(key, [])

    def test_notebooks_and_saved_queries_are_read_in_any_region_without_outputs(self):
        from gcp_aidp.inventory import studio_notebooks
        assets, warnings = studio_notebooks.scan(self.Fake())
        self.assertEqual(assets["notebooks"], [{"id": "nb1", "name": "repo nb1", "region": "us-east1",
                                                "cells": [{"cell_type": "code", "source": "%%bigquery x\nSELECT 1"}]}])
        self.assertEqual(assets["saved_queries"], [{"name": "repo q1", "query": "SELECT 2"}])
        self.assertEqual(assets["pipelines"], [{"id": "pl1", "name": "repo pl1", "region": "us-east1"}])
        self.assertEqual(warnings, ["BigQuery Studio data_canvas asset repo ds1 (us-east1) is not read"])

    def test_saved_queries_join_the_manifest_and_a_folder_copy_wins(self):
        from gcp_aidp.inventory.manifest import _studio_notebooks
        bq = {"summary": {"not_scanned": {"saved_queries": "x", "pipelines": "y"}},
              "items": {"notebooks": [], "saved_queries": [{"name": "repo q1", "query": "SELECT 'folder'"}]}}
        _studio_notebooks(bq, self.Fake())
        self.assertEqual(bq["items"]["saved_queries"], [{"name": "repo q1", "query": "SELECT 'folder'"}])
        self.assertNotIn("not_scanned", bq["summary"])
        self.assertEqual([bq["summary"][k] for k in ("notebooks", "saved_queries", "pipelines")], [1, 1, 1])

    def test_a_dataform_timeout_costs_those_assets_not_the_bigquery_scan(self):
        try:
            import requests
        except ImportError:
            self.skipTest("needs requests (pip install -e '.[gcp]')")
        from gcp_aidp.inventory.manifest import _studio_notebooks

        class TimingOut(self.Fake):
            def get(self, url, params=None):
                raise requests.ReadTimeout("read timed out")
        bq = {"summary": {"tables": 1, "not_scanned": {"pipelines": "y"}},
              "items": {"tables": [{"name": "t"}], "notebooks": [], "saved_queries": [], "pipelines": []}}
        _studio_notebooks(bq, TimingOut())
        self.assertEqual(bq["items"]["tables"], [{"name": "t"}])
        self.assertEqual(sorted(bq["summary"]["not_scanned"]), ["notebooks", "pipelines", "saved_queries"])
        self.assertIn("ReadTimeout", bq["summary"]["not_scanned"]["notebooks"])

    def test_the_dataform_source_does_not_count_them(self):
        from gcp_aidp.inventory import dataform
        fake = self.Fake()
        fake.responses = {"https://dataform.googleapis.com/v1beta1/projects/my-project-123456/locations/us-east1"
                          "/repositories": fake.responses[f"{fake.BASE}/us-east1/repositories"]}
        names = [r["name"] for r in dataform.scan(fake, regions=("us-east1",))["items"]["repositories"]]
        self.assertEqual(names, ["df1"])  # not the notebook, saved query, data canvas or pipeline


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
