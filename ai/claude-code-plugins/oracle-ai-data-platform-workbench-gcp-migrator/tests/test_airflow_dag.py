"""Composer: inventory, one test per C rule, planning, migrate and the job (references/airflow-translation.md)."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.dataplane import MIGRATION_JOB, noop_notebook, write_jobs
from gcp_aidp.gcp_client import GcpClient, GcpError
from gcp_aidp.inventory import composer as inventory
from gcp_aidp.inventory._common import GCS
from gcp_aidp.inventory.manifest import build_manifest
from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan, summarize_plan, write_plan_markdown
from gcp_aidp.publish import plan_publish, publish
from gcp_aidp.translate.airflow_dag import translate_dag
from gcp_aidp.translate.googlesql_to_spark import Context

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())
P = "proj"
ENV = f"https://composer.googleapis.com/v1/projects/{P}/locations/us-central1/environments"


# ── inventory ─────────────────────────────────────────────────────────────────

class FakeComposer(GcpClient):
    """Serves environments and one bucket. Like the real client, it can only GET."""

    def __init__(self, objects, files, prefix="gs://bkt/dags"):
        super().__init__(P)
        self.objects, self.files, self.prefix = objects, files, prefix
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        if url == ENV:
            return {"environments": [{"name": f"projects/{P}/locations/us-central1/environments/e1",
                                      "config": {"dagGcsPrefix": self.prefix,
                                                 "softwareConfig": {"imageVersion": "composer-2-airflow-2"}}}]}
        if url == f"{GCS}/b/bkt/o":
            return {"items": self.objects}
        raise GcpError(f"HTTP 404 notFound: {url}", status=404, reason="notFound")

    def get_text(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        body = self.files[url.split("/o/")[1]]
        if isinstance(body, Exception):
            raise body
        return body


def _objects(*names, **sizes):
    return [{"name": n, **({"size": str(sizes[n])} if n in sizes else {})} for n in names]


class Inventory(unittest.TestCase):
    def scan(self, objects, files):
        client = FakeComposer(objects, files)
        return client, inventory.scan(client, regions=("us-central1",))

    def test_python_files_are_read_and_the_skip_rules_apply(self):
        client, out = self.scan(
            _objects("dags/", "dags/a.py", "dags/airflow_monitoring.py", "dags/__pycache__/b.py", "dags/readme.txt",
                     "dags/sub dir/helper.py"),
            {"dags%2Fa.py": "A = 1\n", "dags%2Fsub%20dir%2Fhelper.py": "H = 2\n"})
        dags = out["items"]["dags"]
        self.assertEqual([(d["dag_id"], d["file"], d["code"]) for d in dags],
                         [("a", "gs://bkt/dags/a.py", "A = 1\n"),
                          ("helper", "gs://bkt/dags/sub dir/helper.py", "H = 2\n")])
        self.assertEqual({d["environment"] for d in dags}, {"e1"})
        self.assertNotIn("not_scanned", out["summary"])
        self.assertEqual(out["items"]["environments"][0]["dag_prefix"], "gs://bkt/dags")

    def test_the_object_is_read_with_a_percent_encoded_name_and_alt_media_using_get_only(self):
        client, _ = self.scan(_objects("dags/sub/a.py"), {"dags%2Fsub%2Fa.py": "x"})
        reads = [(u, p) for u, p in client.calls if "/o/" in u]
        self.assertEqual(reads, [(f"{GCS}/b/bkt/o/dags%2Fsub%2Fa.py", {"alt": "media"})])
        self.assertFalse(any(hasattr(GcpClient, m) for m in ("post", "put", "patch", "delete")))

    def test_a_failed_read_keeps_the_file_with_no_code_and_records_why(self):
        _, out = self.scan(_objects("dags/ok.py", "dags/denied.py"),
                           {"dags%2Fok.py": "A = 1", "dags%2Fdenied.py": GcpError("HTTP 403 PERMISSION_DENIED: nope")})
        by = {d["dag_id"]: d for d in out["items"]["dags"]}
        self.assertEqual(by["ok"]["code"], "A = 1")
        self.assertEqual((by["denied"]["code"], by["denied"]["code_not_scanned"]),
                         ("", "HTTP 403 PERMISSION_DENIED: nope"))
        self.assertEqual(out["summary"]["not_scanned"], {"code of e1/dags/denied.py": "HTTP 403 PERMISSION_DENIED: nope"})
        self.assertEqual(out["summary"]["dags"], 2)

    def test_a_file_over_the_size_cap_is_kept_not_read_and_not_scanned(self):
        limit = inventory.MAX_DAG_BYTES
        objects = [{"name": "dags/big.py", "size": str(limit + 1)}, {"name": "dags/nosize.py"},
                   {"name": "dags/x.py", "size": "2"}]
        client, out = self.scan(objects, {"dags%2Fnosize.py": "#" * (limit + 1), "dags%2Fx.py": "ok"})
        by = {d["dag_id"]: d for d in out["items"]["dags"]}
        for name in ("big", "nosize"):
            self.assertEqual((by[name]["code"], "byte limit" in by[name]["code_not_scanned"]), ("", True), name)
        self.assertEqual(by["x"]["code"], "ok")
        self.assertFalse([u for u, _ in client.calls if u.endswith("dags%2Fbig.py")])  # too large: never requested
        self.assertEqual(len(out["summary"]["not_scanned"]), 2)

    def test_an_environment_without_a_gcs_prefix_has_no_dags(self):
        _, out = self.scan([], {})
        self.assertEqual(out["items"]["dags"], [])
        client = FakeComposer([], {}, prefix="")
        self.assertEqual(inventory.scan(client, regions=("us-central1",))["items"]["dags"], [])

    def test_scanned_through_the_manifest_and_the_plan_shows_the_gap(self):
        client = FakeComposer(_objects("dags/a.py"), {"dags%2Fa.py": GcpError("HTTP 403 denied")})
        m = build_manifest(client, ("composer",), scan_services=True, service_client=client)
        self.assertIn("code of e1/dags/a.py", m["sources"]["composer"]["summary"]["not_scanned"])
        self.assertIn("composer.code of e1/dags/a.py", build_plan(m)["scan_errors"])


# ── the rules ─────────────────────────────────────────────────────────────────

IMPORTS = ("from airflow import DAG\n"
           "from airflow.operators.empty import EmptyOperator\n"
           "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator\n")
CTX = Context(project=P, relations={("sales", "orders"): ("cat", "sales", "orders")})


def bq(task_id="q", sql="SELECT 1 AS x", extra="", config=None):
    config = config or '{"query": {"query": %r, "useLegacySql": False}}' % sql
    return f"BigQueryInsertJobOperator(task_id={task_id!r}, configuration={config}{extra})"


def dag(body, imports=IMPORTS, args='"d", schedule="@daily"'):
    lines = "\n".join("    " + line for line in body.strip("\n").splitlines())
    return f"{imports}\nwith DAG({args}) as dag:\n{lines}\n"


def run(code, ctx=None, **src):
    return translate_dag({"code": code, "file": "gs://b/d.py", "dag_id": "d", "environment": "e", **src},
                         ctx or CTX, "composer_e_d")


def rules(findings, severity=None):
    return {f["rule"] for f in findings if severity is None or f["severity"] == severity}


def flagged(code, rule, **kw):
    job, findings, ok = run(code, **kw)
    assert not ok and rule in rules(findings, "flag") | rules(findings, "block"), (rule, findings)
    return job, findings


def deps(job):
    return {t["taskKey"]: t["dependsOn"] for t in job["tasks"]}


class Rules(unittest.TestCase):
    def test_C01_BIGQUERY_QUERY_runs_the_translated_sql(self):
        job, findings, ok = run(dag("t = " + bq(sql="SELECT order_id FROM `proj.sales.orders`", extra=', location="US", project_id="proj"')))
        (task,) = job["tasks"]
        self.assertEqual(task["statements"], ["SELECT order_id FROM `cat`.`sales`.`orders`"])
        self.assertIn("C01_BIGQUERY_QUERY", rules(findings, "rewrite"))
        self.assertIn("G01_REFERENCE", rules(findings))  # the SQL translator's findings pass through
        self.assertTrue(ok)
        self.assertEqual(job["actions"][0]["original"], ["SELECT order_id FROM `proj.sales.orders`"])

    def test_C01_use_legacy_sql_may_be_omitted_and_the_sql_may_be_a_multi_line_literal(self):
        code = dag('t = BigQueryInsertJobOperator(task_id="q", configuration={"query": {"query": """\n    SELECT 1\n    """}})')
        job, _, ok = run(code)
        self.assertTrue(ok)
        self.assertEqual(job["tasks"][0]["statements"], ["SELECT 1"])

    def test_C01_import_forms_are_resolved(self):
        alias = ("from airflow import DAG\n"
                 "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator as BQ\n")
        self.assertTrue(run(dag("BQ(task_id='q', configuration={'query': {'query': 'SELECT 1'}})", alias))[2])
        module = ("import airflow\nfrom airflow.providers.google.cloud.operators import bigquery\n")
        code = dag("bigquery.BigQueryInsertJobOperator(task_id='q', configuration={'query': {'query': 'SELECT 1'}})",
                   module, 'dag_id="d"').replace("with DAG", "with airflow.DAG")
        self.assertTrue(run(code)[2])
        plain = ("import airflow.models as m\nfrom airflow.operators.dummy import DummyOperator\n")
        self.assertTrue(run(dag("DummyOperator(task_id='a')", plain, '"d"').replace("with DAG", "with m.DAG"))[2])

    def test_C02_NO_OP_for_empty_and_dummy_operators(self):
        job, findings, ok = run(dag("EmptyOperator(task_id='a')\n"))
        self.assertEqual(job["tasks"], [{"taskKey": "a", "title": "Composer task a", "statements": [],
                                         "assertion": False, "noop": True, "dependsOn": []}])
        self.assertIn("C02_NO_OP", rules(findings, "rewrite"))
        self.assertTrue(ok)
        dummy = dag("DummyOperator(task_id='a')", "from airflow import DAG\nfrom airflow.operators.dummy import DummyOperator\n")
        self.assertTrue(run(dummy)[2])

    def test_C03_DEPENDENCIES_every_way_of_writing_them(self):
        body = ("a = EmptyOperator(task_id='a')\nb = EmptyOperator(task_id='b')\nc = EmptyOperator(task_id='c')\n"
                "d = EmptyOperator(task_id='d')\n")
        cases = {
            "a >> b": {"a": [], "b": ["a"], "c": [], "d": []},
            "b << a": {"a": [], "b": ["a"], "c": [], "d": []},
            "a >> [b, c]": {"a": [], "b": ["a"], "c": ["a"], "d": []},
            "[a, b] >> c": {"a": [], "b": [], "c": ["a", "b"], "d": []},
            "a >> b >> c": {"a": [], "b": ["a"], "c": ["b"], "d": []},
            "c << b << a": {"a": [], "b": ["a"], "c": ["b"], "d": []},
            "a >> [b, c] >> d": {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]},
            "a.set_downstream(b)": {"a": [], "b": ["a"], "c": [], "d": []},
            "b.set_upstream(a)": {"a": [], "b": ["a"], "c": [], "d": []},
            "a.set_downstream([b, c])": {"a": [], "b": ["a"], "c": ["a"], "d": []},
            "chain(a, b, c)": {"a": [], "b": ["a"], "c": ["b"], "d": []},
            "chain(a, [b, c], d)": {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]},
            "chain([a, b], [c, d])": {"a": [], "b": [], "c": ["a"], "d": ["b"]},
            "cross_downstream([a, b], [c, d])": {"a": [], "b": [], "c": ["a", "b"], "d": ["a", "b"]},
        }
        imports = IMPORTS + "from airflow.models.baseoperator import chain, cross_downstream\n"
        for expr, expected in cases.items():
            job, findings, ok = run(dag(body + expr, imports))
            self.assertTrue(ok, (expr, findings))
            self.assertEqual(deps(job), expected, expr)
        job, findings, _ = run(dag(body + "a >> b", imports))
        self.assertIn("C03_DEPENDENCIES", rules(findings, "info"))

    def test_C03_inline_tasks_a_task_group_prefix_and_dependencies_first_order(self):
        body = ("end = EmptyOperator(task_id='end')\n"
                "with TaskGroup('load') as load:\n"
                "    x = EmptyOperator(task_id='x')\n"
                "    with TaskGroup('inner'):\n"
                "        y = EmptyOperator(task_id='y')\n"
                "    x >> y\n"
                "EmptyOperator(task_id='first') >> x\n"
                "y >> end\n")
        job, _, ok = run(dag(body, IMPORTS + "from airflow.utils.task_group import TaskGroup\n"))
        self.assertTrue(ok)
        self.assertEqual(deps(job), {"first": [], "load_x": ["first"], "load_inner_y": ["load_x"], "end": ["load_inner_y"]})
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["first", "load_x", "load_inner_y", "end"])

    def test_C03_the_dag_may_be_assigned_and_attached_with_dag_equals_dag(self):
        code = (IMPORTS + "dag = DAG('d', schedule='@daily')\n"
                "a = EmptyOperator(task_id='a', dag=dag)\nb = EmptyOperator(task_id='b', dag=dag)\na >> b\n")
        job, _, ok = run(code)
        self.assertTrue(ok)
        self.assertEqual(deps(job), {"a": [], "b": ["a"]})

    def test_C04_SCHEDULE_is_recorded_as_text_in_every_form_and_never_applied(self):
        cases = {'schedule="0 3 * * *"': "cron 0 3 * * *", 'schedule="@hourly"': "preset @hourly",
                 'schedule_interval="30 1 * * 1"': "cron 30 1 * * 1", "schedule=None": "none",
                 "schedule=timedelta(hours=6)": "timedelta timedelta(hours=6)",
                 "schedule=SCHEDULE": "expression SCHEDULE", "": "none given"}
        for arg, text in cases.items():
            job, findings, ok = run(dag("EmptyOperator(task_id='a')", IMPORTS + "from datetime import timedelta\n",
                                        f'"d", {arg}'))
            self.assertTrue(ok, arg)
            self.assertEqual(job["schedule"], text, arg)
            self.assertTrue(any(f["rule"] == "C04_SCHEDULE" and text in f["detail"] and "not applied" in f["detail"]
                                for f in findings))
        job, _, _ = run(dag("EmptyOperator(task_id='a')", args='"d", schedule="@daily", start_date=datetime(2026, 1, 1), '
                                                              'catchup=False, max_active_runs=2'))
        self.assertEqual(job["schedule"], "preset @daily; start_date datetime(2026, 1, 1); catchup False; max_active_runs 2")

    def test_C05_DEFAULT_ARGS_and_C06_RETRIES_are_recorded_not_carried(self):
        code = (IMPORTS + "default_args = {'owner': 'me', 'email': ['a@b'], 'retries': 3, 'retry_delay': 5,\n"
                "    'email_on_failure': True, 'email_on_retry': False}\n"
                "with DAG('d', default_args=default_args) as dag:\n    EmptyOperator(task_id='a', retries=1)\n")
        job, findings, ok = run(code)
        self.assertTrue(ok)
        self.assertEqual(rules(findings, "info"), {"C04_SCHEDULE", "C05_DEFAULT_ARGS", "C06_RETRIES"})
        self.assertTrue(any("retries" in f["detail"] and "runs once" in f["detail"] for f in findings if f["rule"] == "C06_RETRIES"))
        zero = IMPORTS + "with DAG('d', default_args={'retries': 0}) as dag:\n    EmptyOperator(task_id='a')\n"
        self.assertNotIn("C06_RETRIES", rules(run(zero)[1]))

    def test_C89_NO_DAG_for_a_helper_module_and_for_a_dag_without_tasks(self):
        job, findings, ok = run("PROJECT = 'x'\n\ndef helper():\n    return 1\n")
        self.assertEqual((rules(findings), ok, job["tasks"]), ({"C89_NO_DAG"}, False, []))
        self.assertEqual({f["severity"] for f in findings}, {"info"})
        _, findings, ok = run(IMPORTS + "with DAG('d') as dag:\n    pass\n")
        self.assertIn("C89_NO_DAG", rules(findings, "info"))
        self.assertFalse(ok)
        _, findings, ok = run("")
        self.assertEqual((rules(findings), ok), ({"C89_NO_DAG"}, False))

    def test_C90_UNSUPPORTED_OPERATOR_is_flagged_by_name_and_path(self):
        imports = (IMPORTS + "from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator\n"
                   "from airflow.operators.bash import BashOperator\nfrom airflow.sensors.external_task import ExternalTaskSensor\n")
        for cls, task in (("DataprocSubmitJobOperator", "x"), ("BashOperator", "x"), ("ExternalTaskSensor", "x")):
            _, findings = flagged(dag(f"{cls}(task_id='{task}')", imports), "C90_UNSUPPORTED_OPERATOR")
            detail = next(f["detail"] for f in findings if f["rule"] == "C90_UNSUPPORTED_OPERATOR")
            self.assertIn(cls, detail)
            self.assertIn("airflow.", detail)  # the import path
        _, findings = flagged(dag("Mystery(task_id='x')", IMPORTS + "from plugins.ops import MysteryOperator as Mystery\n"),
                              "C90_UNSUPPORTED_OPERATOR")
        self.assertIn("plugins.ops.MysteryOperator", findings[-1]["detail"])
        _, findings = flagged(dag("LocalOperator(task_id='x')"), "C90_UNSUPPORTED_OPERATOR")
        self.assertIn("no import found", findings[-1]["detail"])

    def test_C90_a_known_class_from_a_module_path_that_is_not_recognised_is_flagged(self):
        old = dag("DummyOperator(task_id='a')", "from airflow import DAG\nfrom airflow.operators.dummy_operator import DummyOperator\n")
        _, findings = flagged(old, "C90_UNSUPPORTED_OPERATOR")
        self.assertIn("airflow.operators.dummy_operator.DummyOperator", findings[-1]["detail"])
        self.assertIn("airflow.operators.dummy is", findings[-1]["detail"])
        elsewhere = dag(bq(), "from airflow import DAG\nfrom mylib.bigquery import BigQueryInsertJobOperator\n")
        flagged(elsewhere, "C90_UNSUPPORTED_OPERATOR")
        self.assertTrue(run(dag(bq()))[2])  # the confirmed path is accepted
        chain = dag("a = EmptyOperator(task_id='a')\nb = EmptyOperator(task_id='b')\nchain(a, b)",
                    IMPORTS + "from airflow.models.chain import chain\n")
        flagged(chain, "C90_UNSUPPORTED_OPERATOR")

    def test_C91_NON_LITERAL_values(self):
        cases = {
            "f-string": dag(bq(config='{"query": {"query": f"SELECT {1}"}}')),
            "variable": dag("S = 'SELECT 1'\n" + bq(config='{"query": {"query": S}}')),
            ".format": dag(bq(config='{"query": {"query": "SELECT {}".format(1)}}')),
            "concatenation": dag(bq(config='{"query": {"query": "SELECT " + "1"}}')),
            "Variable.get": dag(bq(config='{"query": {"query": Variable.get("q")}}'),
                                IMPORTS + "from airflow.models import Variable\n"),
            "task_id": dag("EmptyOperator(task_id=NAME)"),
            "task_id f-string": dag("EmptyOperator(task_id=f'a_{1}')"),
            "kwargs": dag("EmptyOperator(task_id='a', **OPTS)"),
            "dag_id": dag("EmptyOperator(task_id='a')", args="DAG_ID"),
            "dag(**)": dag("EmptyOperator(task_id='a')", args="'d', **OPTS"),
            "configuration": dag(bq(config="CONFIG")),
            "location": dag(bq(extra=", location=LOC")),
            "dependency on a name": dag("a = EmptyOperator(task_id='a')\na >> unknown"),
            "dataset schedule": dag("EmptyOperator(task_id='a')", IMPORTS + "from airflow.datasets import Dataset\n",
                                    '"d", schedule=[Dataset("gs://b/x")]'),
        }
        for name, code in cases.items():
            _, findings = flagged(code, "C91_NON_LITERAL")
            self.assertTrue(any(f["rule"] == "C91_NON_LITERAL" for f in findings), name)

    def test_C92_DYNAMIC_GRAPH_cases(self):
        cases = {
            "for loop": dag("for r in ['a', 'b']:\n    EmptyOperator(task_id=r)"),
            "while loop": dag("while False:\n    EmptyOperator(task_id='a')"),
            "comprehension": dag("tasks = [EmptyOperator(task_id=str(i)) for i in range(3)]"),
            "list of operators": dag("tasks = [EmptyOperator(task_id='a'), EmptyOperator(task_id='b')]"),
            "conditional": dag("if True:\n    EmptyOperator(task_id='a')"),
            "function": dag("def make():\n    return EmptyOperator(task_id='a')"),
            "try": dag("try:\n    EmptyOperator(task_id='a')\nexcept Exception:\n    pass"),
            "@task": dag("@task\ndef f():\n    return 1\nf()", IMPORTS + "from airflow.decorators import task\n"),
            "@dag": IMPORTS + "from airflow.decorators import dag\n@dag(schedule='@daily')\ndef my():\n    pass\nmy()\n",
            "expand": dag("BigQueryInsertJobOperator.partial(task_id='q').expand(configuration=[{}])"),
            "expand on an instance": dag("EmptyOperator(task_id='a').expand(x=[1])"),
            "xcom": dag("a = EmptyOperator(task_id='a')\nb = EmptyOperator(task_id='b', trigger_rule='all_success')\n"
                        "x = a.output"),
            "xcom_pull": dag("a = EmptyOperator(task_id='a')\ny = ti.xcom_pull(task_ids='a')"),
            "dependencies in a loop": dag("a = EmptyOperator(task_id='a')\nb = EmptyOperator(task_id='b')\n"
                                          "for x in [b]:\n    a >> x"),
            "task flow call": dag("a = EmptyOperator(task_id='a')\na >> compute()"),
            "subscript": dag("a = EmptyOperator(task_id='a')\ntasks = {}\na >> tasks['b']"),
            "starred chain": dag("tasks = []\nchain(*tasks)", IMPORTS + "from airflow.models.baseoperator import chain\n"),
            "TaskGroup in a dependency": dag("a = EmptyOperator(task_id='a')\nwith TaskGroup('g') as g:\n    pass\na >> g",
                                             IMPORTS + "from airflow.utils.task_group import TaskGroup\n"),
        }
        for name, code in cases.items():
            _, findings = flagged(code, "C92_DYNAMIC_GRAPH")
            self.assertIn("C92_DYNAMIC_GRAPH", rules(findings, "flag"), name)

    def test_C93_PARSE_ERROR_blocks_and_nothing_is_run(self):
        job, findings, ok = run("from airflow import DAG\nwith DAG('d'\n    x = 1\n")
        self.assertEqual((rules(findings, "block"), ok, job["tasks"]), ({"C93_PARSE_ERROR"}, False, []))
        self.assertIn("line 3", findings[0]["detail"])
        self.assertEqual(rules(run("x = 1\x00")[1], "block"), {"C93_PARSE_ERROR"})  # a null byte does not raise

    def test_the_code_is_never_imported_executed_or_evaluated(self):
        marker = Path(tempfile.gettempdir()) / "gcp_aidp_must_not_exist"
        marker.unlink(missing_ok=True)
        code = (f"import airflow_does_not_exist\nopen({str(marker)!r}, 'w').write('x')\n"
                "raise SystemExit('executed')\n" + dag("a = EmptyOperator(task_id='a')"))
        job, findings, ok = run(code)
        self.assertFalse(marker.exists())
        self.assertNotIn("airflow", sys.modules)
        self.assertNotIn("airflow_does_not_exist", sys.modules)
        self.assertTrue(ok)

    def test_C94_JINJA_in_a_translated_field(self):
        for name, code in {
            "sql": dag(bq(sql="SELECT * FROM t WHERE d = '{{ ds }}'")),
            "sql block": dag(bq(sql="{% if x %}SELECT 1{% endif %}")),
            "task_id": dag("EmptyOperator(task_id='a_{{ ds_nodash }}')"),
            "location": dag(bq(extra=", location='{{ var.value.loc }}'")),
        }.items():
            _, findings = flagged(code, "C94_JINJA")
            self.assertIn("C94_JINJA", rules(findings, "flag"), name)

    def test_C95_TASK_ARGUMENT_cases(self):
        cases = {
            "trigger_rule": dag("EmptyOperator(task_id='a', trigger_rule='all_done')"),
            "pool": dag("EmptyOperator(task_id='a', pool='x')"),
            "queue": dag(bq(extra=", queue='q'")),
            "deferrable": dag(bq(extra=", deferrable=True")),
            "gcp_conn_id": dag(bq(extra=", gcp_conn_id='x'")),
            "positional": dag("EmptyOperator('a')"),
            "duplicate task_id": dag("EmptyOperator(task_id='a')\nEmptyOperator(task_id='a')"),
            "default_args key": dag("EmptyOperator(task_id='a')", args='"d", default_args={"trigger_rule": "all_done"}'),
            "default_args depends_on_past": dag("EmptyOperator(task_id='a')",
                                                args='"d", default_args={"depends_on_past": True}'),
            "default_args operator argument": dag("EmptyOperator(task_id='a')",
                                                  args='"d", default_args={"gcp_conn_id": "x"}'),
            "dag argument": dag("EmptyOperator(task_id='a')", args='"d", on_failure_callback=notify'),
            "not attached": IMPORTS + "EmptyOperator(task_id='a')\nwith DAG('d') as dag:\n    pass\n",
            "other dag": dag("EmptyOperator(task_id='a', dag=other)"),
            "task group argument": dag("with TaskGroup('g', prefix_group_id=False):\n    EmptyOperator(task_id='a')",
                                       IMPORTS + "from airflow.utils.task_group import TaskGroup\n"),
        }
        for name, code in cases.items():
            _, findings = flagged(code, "C95_TASK_ARGUMENT")
            self.assertIn("C95_TASK_ARGUMENT", rules(findings, "flag"), name)
        job, _, ok = run(dag("EmptyOperator(task_id='a', trigger_rule='all_success', owner='me')"))
        self.assertTrue(ok)  # the default trigger rule and a recorded argument are fine

    def test_C96_MULTIPLE_DAGS_flags(self):
        code = (IMPORTS + "with DAG('one') as one:\n    EmptyOperator(task_id='a')\n"
                "with DAG('two') as two:\n    EmptyOperator(task_id='b')\n")
        flagged(code, "C96_MULTIPLE_DAGS")

    def test_C97_CYCLE_blocks(self):
        body = "a = EmptyOperator(task_id='a')\nb = EmptyOperator(task_id='b')\nc = EmptyOperator(task_id='c')\na >> b >> c >> a"
        job, findings = flagged(dag(body), "C97_CYCLE")
        self.assertIn("C97_CYCLE", rules(findings, "block"))
        self.assertIn("a → c → b → a", next(f["detail"] for f in findings if f["rule"] == "C97_CYCLE"))
        flagged(dag("a = EmptyOperator(task_id='a')\na >> a"), "C97_CYCLE")

    def test_C98_BIGQUERY_CONFIG_only_a_plain_query_job_is_translated(self):
        q = '"query": {"query": "SELECT 1", "useLegacySql": False}'
        cases = {
            "destinationTable": '{%s, "destinationTable": {"tableId": "t"}}' % q,
            "writeDisposition": '{"query": {"query": "SELECT 1", "writeDisposition": "WRITE_TRUNCATE"}}',
            "createDisposition": '{"query": {"query": "SELECT 1", "createDisposition": "CREATE_IF_NEEDED"}}',
            "parameters": '{"query": {"query": "SELECT 1", "queryParameters": []}}',
            "labels": '{%s, "labels": {"a": "b"}}' % q,
            "load job": '{"load": {"sourceUris": ["gs://b/x"]}}',
            "extract job": '{"extract": {"destinationUris": ["gs://b/x"]}}',
            "copy job": '{"copy": {}}',
            "legacy sql": '{"query": {"query": "SELECT 1", "useLegacySql": True}}',
            "no query text": '{"query": {"useLegacySql": False}}',
        }
        for name, config in cases.items():
            _, findings = flagged(dag(bq(config=config)), "C98_BIGQUERY_CONFIG")
            self.assertIn("C98_BIGQUERY_CONFIG", rules(findings, "flag"), name)
        flagged(dag("BigQueryInsertJobOperator(task_id='q')"), "C98_BIGQUERY_CONFIG")

    def test_C99_NOT_SCANNED_blocks_a_file_that_could_not_be_read(self):
        job, findings, ok = run("", code_not_scanned="HTTP 403 denied", file="gs://b/x.py")
        self.assertEqual((rules(findings, "block"), ok), ({"C99_NOT_SCANNED"}, False))
        self.assertIn("HTTP 403 denied", findings[0]["detail"])
        self.assertIn("gs://b/x.py", findings[0]["detail"])

    def test_the_sql_translators_blocks_and_flags_stop_the_job(self):
        _, findings, ok = run(dag(bq(sql="SELECT * FROM `proj.sales.not_in_the_plan`")))
        self.assertIn("G01_REFERENCE", rules(findings, "flag"))  # a table the plan does not hold
        self.assertFalse(ok)

    def test_no_partial_job_one_flagged_task_stops_the_whole_job(self):
        body = ("good = " + bq("good", "SELECT 1") + "\nbad = BashOperator(task_id='bad')\ngood >> bad\n")
        job, findings, ok = run(dag(body, IMPORTS + "from airflow.operators.bash import BashOperator\n"))
        self.assertFalse(ok)
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["good", "bad"])  # reported, but never created
        self.assertEqual(job["actions"][0]["statements"], ["SELECT 1"])  # what was translated stays for review

    def test_task_keys_are_valid_unique_names(self):
        body = "EmptyOperator(task_id='a.b')\nEmptyOperator(task_id='a-b')\nEmptyOperator(task_id='1st')\n"
        job, _, ok = run(dag(body))
        self.assertTrue(ok)
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["a_b", "a_b_2", "q_1st"])


# ── planning ──────────────────────────────────────────────────────────────────

def _manifest(dags, envs=("e1",)):
    return {"project_id": P, "sources": {"composer": {"items": {
        "environments": [{"name": e} for e in envs], "dags": dags}}}}


def _dag(env, dag_id, code="x = 1"):
    return {"environment": env, "dag_id": dag_id, "file": f"gs://b/{dag_id}.py", "code": code}


class Planning(unittest.TestCase):
    def test_environments_are_reported_and_dags_are_migrate_rows(self):
        plan = build_plan(DEMO)
        env = next(a for a in plan["assets"] if a["id"] == "composer.environment.northwind-orchestration")
        self.assertEqual((env["action"], env["reason"]), ("REPORT", "Airflow itself is not migrated; its DAGs are"))
        row = next(a for a in plan["assets"] if a["id"] == "composer.dag.northwind-orchestration.revenue_rollup")
        self.assertEqual((row["action"], row["kind"], row["version"]), ("MIGRATE", "code+setup", "0.3"))
        self.assertEqual(row["source"]["type"], "composer_dag")
        self.assertIn("revenue_rollup", row["source"]["code"])
        self.assertEqual(row["target"], {"type": "aidp_job", "name": "composer_northwind_orchestration_revenue_rollup"})
        self.assertEqual(row["transform_chain"], ["parse_dag_ast", "googlesql_to_spark", "create_job_unscheduled"])
        self.assertIn("never run", row["notes"][0])
        broken = next(a for a in plan["assets"] if a["id"].endswith(".ml_feature_refresh"))
        self.assertIn("file not scanned: HTTP 403", broken["notes"][-1])

    def test_the_plan_text_never_shows_dag_source(self):
        plan = build_plan(DEMO)
        with tempfile.TemporaryDirectory() as d:
            md = write_plan_markdown(plan, Path(d) / "plan.md").read_text()
        for text in (md, summarize_plan(plan)):
            self.assertNotIn("INSERT INTO", text)
            self.assertNotIn("BigQueryInsertJobOperator", text)

    def test_dags_selects_by_dag_id_in_any_environment_and_skips_the_rest(self):
        m = _manifest([_dag("e1", "a"), _dag("e2", "a"), _dag("e1", "b")], envs=("e1", "e2"))
        plan = build_plan(m, dags=["a"])
        by = {a["id"]: a for a in plan["assets"] if a["source"]["type"] == "composer_dag"}
        self.assertEqual({k: v["action"] for k, v in by.items()},
                         {"composer.dag.e1.a": "MIGRATE", "composer.dag.e2.a": "MIGRATE", "composer.dag.e1.b": "SKIP"})
        self.assertEqual(by["composer.dag.e1.b"]["reason"], "DAG b is outside --dags")
        self.assertEqual(by["composer.dag.e1.b"]["transform_chain"], [])
        self.assertEqual(plan["scope"]["dags"], ["a"])
        with tempfile.TemporaryDirectory() as d:
            md = write_plan_markdown(plan, Path(d) / "plan.md").read_text()
        self.assertIn("Composer DAGs: a (1 of 2; the rest are SKIP)", md)

    def test_default_scope_and_unknown_name(self):
        plan = build_plan(DEMO)
        self.assertIsNone(plan["scope"]["dags"])
        self.assertIn("Composer DAGs: all 6", summarize_plan(plan))
        with self.assertRaisesRegex(ValueError, "--dags not in the inventory: nope; its DAGs are: common_settings, "
                                                "customer_profiles, ml_feature_refresh, month_end_close, "
                                                "regional_exports, revenue_rollup"):
            build_plan(DEMO, dags=["nope"])
        with tempfile.TemporaryDirectory() as d:
            self.assertNotIn("Composer", write_plan_markdown(
                build_plan({"project_id": "p", "sources": {"bigquery": {"items": {}}}}), Path(d) / "p.md").read_text())

    def test_two_environments_get_two_jobs_and_a_real_collision_halts_the_plan(self):
        plan = build_plan(_manifest([_dag("e1", "a"), _dag("e2", "a")], envs=("e1", "e2")))
        self.assertEqual(sorted(a["target"]["name"] for a in plan["assets"] if a["source"]["type"] == "composer_dag"),
                         ["composer_e1_a", "composer_e2_a"])
        with self.assertRaisesRegex(ValueError, "collision"):
            build_plan(_manifest([_dag("a-b", "c"), _dag("a", "b_c")], envs=("a-b", "a")))

    def test_cli_flag(self):
        from gcp_aidp.cli import build_parser
        self.assertEqual(build_parser().parse_args(["plan", "m.json", "--dags", "a, b"]).dags, "a, b")

    @unittest.skipUnless(importlib.util.find_spec("mcp"), "install the mcp extra to enable the MCP tool test")
    def test_mcp_tool_passes_dags_on(self):
        from unittest import mock
        from gcp_aidp import mcp_server
        with mock.patch.object(mcp_server, "_run", return_value="ok") as run_:
            mcp_server.plan("inv.json", dags="a,b")
        self.assertEqual(run_.call_args[0][0][-2:], ["--dags", "a,b"])


# ── migrate, jobs, publish ────────────────────────────────────────────────────

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

    def row(self, dag_id):
        return self.by_id[f"composer.dag.northwind-orchestration.{dag_id}"]

    def test_the_translatable_dag_becomes_one_job_with_fan_out_and_fan_in(self):
        r = self.row("revenue_rollup")
        self.assertEqual((r["status"], r["output_path"]),
                         ("ok", "composer/northwind-orchestration.revenue_rollup.sql"))
        job = r["job"]
        self.assertEqual(job["name"], "composer_northwind_orchestration_revenue_rollup")
        self.assertIn("cron 0 3 * * *", job["title"])
        self.assertIn("not applied", job["title"])
        self.assertEqual({t["taskKey"]: t["dependsOn"] for t in job["tasks"]}, {
            "start": [], "load_daily_revenue": ["start"], "load_shipped_revenue": ["start"],
            "combine_revenue": ["load_daily_revenue", "load_shipped_revenue"]})
        text = (self.out / r["output_path"]).read_text()
        self.assertIn("-- tasks: start, load_daily_revenue, load_shipped_revenue, combine_revenue", text)
        self.assertIn("-- edges: start -> load_daily_revenue; start -> load_shipped_revenue; "
                      "load_daily_revenue -> combine_revenue; load_shipped_revenue -> combine_revenue", text)
        self.assertIn("-- task combine_revenue (BigQueryInsertJobOperator), after load_daily_revenue, "
                      "load_shipped_revenue", text)
        self.assertIn("`northwind_analytics_demo`.`sales`.`orders`", text)

    def test_the_job_and_task_notebooks_are_in_the_report(self):
        job = next(j for j in self.report["jobs"] if j["name"] == "composer_northwind_orchestration_revenue_rollup")
        self.assertEqual(len(job["tasks"]), 4)
        for t in job["tasks"]:
            self.assertIn(t["notebook"], self.report["notebooks"])
            self.assertTrue((self.out / t["notebook"]).is_file())
        self.assertEqual({t["taskKey"]: t.get("dependsOn", []) for t in job["tasks"]}["combine_revenue"],
                         ["load_daily_revenue", "load_shipped_revenue"])
        nb = json.loads((self.out / "notebooks/jobs/composer_northwind_orchestration_revenue_rollup__start.ipynb").read_text())
        self.assertEqual("".join(nb["cells"][-1]["source"]), "print('no-op task: nothing to run')")
        sql_nb = json.loads((self.out / "notebooks/jobs/composer_northwind_orchestration_revenue_rollup__"
                                        "load_daily_revenue.ipynb").read_text())
        self.assertIn("INSERT INTO", "".join(sql_nb["cells"][-1]["source"]))

    def test_flagged_blocked_and_unreadable_dags_create_no_job(self):
        expected = {"customer_profiles": ("needs_manual_review", "C90_UNSUPPORTED_OPERATOR"),
                    "regional_exports": ("needs_manual_review", "C92_DYNAMIC_GRAPH"),
                    "month_end_close": ("blocked", "C93_PARSE_ERROR"),
                    "ml_feature_refresh": ("blocked", "C99_NOT_SCANNED"),
                    "common_settings": ("ok", "C89_NO_DAG")}
        for dag_id, (status, rule) in expected.items():
            r = self.row(dag_id)
            self.assertEqual(r["status"], status, dag_id)
            self.assertIn(rule, [f["rule"] for f in r["findings"]])
            self.assertNotIn("job", r)
            self.assertTrue((self.out / r["output_path"]).is_file())
        self.assertEqual([j["name"] for j in self.report["jobs"] if j["name"].startswith("composer_")],
                         ["composer_northwind_orchestration_revenue_rollup"])

    def test_a_blocked_file_is_all_comments_and_keeps_the_original(self):
        text = (self.out / self.row("month_end_close")["output_path"]).read_text()
        self.assertTrue(all(l.startswith("--") or not l.strip() for l in text.splitlines()))
        self.assertIn('-- with DAG("month_end_close"', text)

    def test_the_flagged_operator_is_named_in_the_file_and_the_report(self):
        r = self.row("customer_profiles")
        self.assertIn("DataprocSubmitJobOperator (airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator)",
                      " ".join(f["detail"] for f in r["findings"]))
        self.assertIn("-- FLAG C90_UNSUPPORTED_OPERATOR", (self.out / r["output_path"]).read_text())

    def test_the_environment_is_reported_and_verify_labels_the_results(self):
        self.assertEqual(self.by_id["composer.environment.northwind-orchestration"]["status"], "reported")
        from gcp_aidp.verify import verify
        rows = {r["asset_id"]: r["verdict"] for r in verify(self.out / "report.json")["rows"]}
        self.assertEqual(rows["composer.dag.northwind-orchestration.revenue_rollup"], "PASS")
        self.assertEqual(rows["composer.dag.northwind-orchestration.customer_profiles"], "REVIEW")
        self.assertEqual(rows["composer.dag.northwind-orchestration.month_end_close"], "REVIEW")
        self.assertEqual(rows["composer.environment.northwind-orchestration"], "SKIP")

    def test_a_dag_outside_dags_is_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(DEMO, dags=["customer_profiles"]), out_dir=Path(d))
        r = {x["asset_id"]: x for x in report["results"]}["composer.dag.northwind-orchestration.revenue_rollup"]
        self.assertEqual(r["status"], "skipped")
        self.assertNotIn("composer_northwind_orchestration_revenue_rollup", [j["name"] for j in report["jobs"]])

    def test_two_environments_write_two_files_and_two_jobs(self):
        good = dag("EmptyOperator(task_id='a')")
        m = _manifest([_dag("e1", "d", good), _dag("e2", "d", good)], envs=("e1", "e2"))
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(m), out_dir=Path(d))
            self.assertEqual(sorted(r["output_path"] for r in report["results"] if r.get("output_path")),
                             ["composer/e1.d.sql", "composer/e2.d.sql"])
        self.assertEqual([j["name"] for j in report["jobs"][1:]], ["composer_e1_d", "composer_e2_d"])

    def test_the_manifest_code_stays_out_of_the_report_json(self):
        text = (self.out / "report.json").read_text()
        self.assertNotIn("from airflow import DAG", text)
        self.assertNotIn("EmptyOperator(task_id", text)


class WriteJobs(unittest.TestCase):
    def test_a_noop_task_writes_the_noop_notebook(self):
        dp = {"plan_id": "x", "project": "p", "catalog": "c", "tables": []}
        job = {"name": "j", "title": "J", "tasks": [
            {"taskKey": "a", "title": "A", "statements": [], "assertion": False, "noop": True, "dependsOn": []},
            {"taskKey": "b", "title": "B", "statements": ["SELECT 1"], "assertion": False, "noop": False, "dependsOn": ["a"]}]}
        with tempfile.TemporaryDirectory() as d:
            write_jobs(dp, [{"status": "ok", "kind": "composer_dag", "job": job}], Path(d))
            self.assertEqual((Path(d) / "notebooks/jobs/j__a.ipynb").read_text(),
                             json.dumps(noop_notebook("A", "x"), indent=1) + "\n")
            self.assertIn("spark.sql", (Path(d) / "notebooks/jobs/j__b.ipynb").read_text())
        self.assertNotIn("composer", MIGRATION_JOB)


class Publish(unittest.TestCase):
    def test_the_dry_run_keeps_fan_out_and_fan_in_and_the_schedule_text(self):
        with tempfile.TemporaryDirectory() as d:
            migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=Path(d))
            p = publish(d, prefix="ana", cluster_key="ck")
        self.assertFalse(p["applied"])
        jobs = {j["name"]: j["definition"] for j in p["jobs"]}
        name = "ana_composer_northwind_orchestration_revenue_rollup"
        self.assertIn(name, jobs)
        self.assertFalse([n for n in jobs if "customer_profiles" in n or "month_end" in n])
        tasks = {t["taskKey"]: t for t in jobs[name]["tasks"]}
        self.assertEqual([x["taskKey"] for x in tasks["combine_revenue"]["dependsOn"]],
                         ["load_daily_revenue", "load_shipped_revenue"])
        self.assertEqual([x["taskKey"] for x in tasks["load_shipped_revenue"]["dependsOn"]], ["start"])
        self.assertNotIn("dependsOn", tasks["start"])
        self.assertIn("cron 0 3 * * *", jobs[name]["description"])
        self.assertNotIn("schedule", {k for k in jobs[name] if k != "description"})  # created unscheduled


if __name__ == "__main__":
    unittest.main()
