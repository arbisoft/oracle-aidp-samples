"""publish (dry run, --apply, never overwrite) and run, against a fake AIDP client."""
from __future__ import annotations

import ast
import copy
import json
import tempfile
import unittest
from pathlib import Path

from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan
from gcp_aidp.publish import PublishError, plan_publish, publish, run

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())


class FakeAidp:
    def __init__(self, present=(), jobs=(), runs=None, clusters=("ck",)):
        self.present, self.jobs, self.clusters = set(present), [{"name": n, "key": f"k_{n}"} for n in jobs], clusters
        self.uploaded, self.created, self.started = [], [], []
        self.runs = list(runs or [])

    def folder_names(self, folder):
        return self.present

    def list_jobs(self):
        return self.jobs

    def cluster_problem(self, key):
        return "" if key in self.clusters else f"cluster {key} is not one of this workspace's clusters"

    def put_notebook(self, path, text):
        json.loads(text)  # a real .ipynb
        self.uploaded.append(path)

    def put_file(self, path, local):
        ast.parse(Path(local).read_text())  # a Python task's script, as translated
        self.uploaded.append(path)

    def create_job(self, definition):
        self.created.append(definition)
        return f"k_{definition['name']}"

    def run_job(self, key):
        self.started.append(key)
        return "run1"

    def task_runs(self, run_key):
        return self.runs.pop(0) if len(self.runs) > 1 else self.runs[0]

    def task_output(self, task_run_key):
        return json.dumps({"data": {"errorTrace": "RuntimeError: 02_copy_dataset: 1 problem(s): x"}})


class Publish(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_dry_run_lists_exactly_what_would_be_created(self):
        p = plan_publish(self.out, prefix="ana", cluster_key="ck")
        report = json.loads((self.out / "report.json").read_text())
        self.assertEqual(len(p["notebooks"]), len(report["notebooks"]) + len(report["files"]))
        self.assertTrue(all(n["remote"].startswith("/Workspace/ana/") for n in p["notebooks"]))
        names = [j["name"] for j in p["jobs"]]
        self.assertEqual(names[0], "ana_gcp_aidp_migration")
        self.assertIn("ana_refresh_sales_mv_daily_sales", names)
        chain = p["jobs"][0]["definition"]["tasks"]
        self.assertEqual(chain[1]["dependsOn"], [{"taskKey": "diagnose"}])
        self.assertIn({"name": "reports_dir", "value": "/Workspace/ana/reports"}, chain[0]["parameters"])
        self.assertTrue(all(t["cluster"] == {"clusterKey": "ck"} for t in chain))
        self.assertFalse(publish(self.out, prefix="ana", cluster_key="ck", client=FakeAidp())["applied"])

    def test_apply_needs_a_prefix_and_jobs_need_a_cluster(self):
        with self.assertRaisesRegex(PublishError, "--prefix is required"):
            publish(self.out, cluster_key="ck", apply=True, client=FakeAidp())
        self.assertEqual(plan_publish(self.out, prefix="ana")["jobs"], [])
        with self.assertRaisesRegex(PublishError, "must be a letter"):
            plan_publish(self.out, prefix="a-b")

    def test_apply_uploads_then_creates_and_never_overwrites(self):
        fake = FakeAidp(present={"00_diagnose.ipynb"}, jobs=["ana_refresh_sales_mv_daily_sales"])
        r = publish(self.out, prefix="ana", cluster_key="ck", apply=True, client=fake, log=lambda m: None)
        self.assertNotIn("/Workspace/ana/00_diagnose.ipynb", fake.uploaded)
        status = {j["name"]: j["status"] for j in r["jobs"]}
        self.assertEqual(status["ana_refresh_sales_mv_daily_sales"], "skipped")  # exists: left alone
        self.assertEqual(status["ana_gcp_aidp_migration"], "refused")  # its diagnose notebook was not ours
        self.assertEqual(status["ana_refresh_marketing_mv_events_by_name"], "created")
        fake = FakeAidp(present={"00_diagnose.ipynb"})
        r = publish(self.out, prefix="ana", cluster_key="ck", apply=True, reuse_existing=True, client=fake,
                    log=lambda m: None)
        self.assertEqual({j["status"] for j in r["jobs"]}, {"created"})

    def test_a_bad_cluster_key_sends_nothing(self):
        fake = FakeAidp(clusters=())
        with self.assertRaisesRegex(PublishError, "nothing was sent"):
            publish(self.out, prefix="ana", cluster_key="ck", apply=True, client=fake, log=lambda m: None)
        self.assertEqual((fake.uploaded, fake.created), ([], []))

    def test_run_follows_the_job_to_the_end(self):
        n = len(json.loads((self.out / "report.json").read_text())["jobs"][0]["tasks"])
        first = [("diagnose", "SUCCESS", "", "t0")]  # later tasks not listed yet: not the end
        done = [(f"t{i}", "SUCCESS", "", f"t{i}") for i in range(n)]
        fake = FakeAidp(jobs=["ana_gcp_aidp_migration"], runs=[first, first, done])
        r = run(self.out, prefix="ana", client=fake, sleep=lambda s: None, log=lambda m: None)
        self.assertEqual((r["ok"], fake.started), (True, ["k_ana_gcp_aidp_migration"]))
        self.assertEqual(len(r["tasks"]), n)

        failed = [("diagnose", "SUCCESS", "", "t0"), ("structure", "FAILED", "boom", "t1")]
        lines = []
        r = run(self.out, prefix="ana", client=FakeAidp(jobs=["ana_gcp_aidp_migration"], runs=[failed]),
                sleep=lambda s: None, log=lines.append)
        self.assertEqual((r["finished"], r["ok"]), (True, False))
        self.assertIn("1 problem(s)", "\n".join(lines))

        with self.assertRaisesRegex(PublishError, "publish --apply"):
            run(self.out, prefix="ana", client=FakeAidp(), sleep=lambda s: None, log=lambda m: None)


class ExportWarning(unittest.TestCase):
    def test_run_warns_until_export_put_every_routed_table_on_oci(self):
        from gcp_aidp.publish import export_warning

        spec = {"tables": [{"dataset": "shop", "name": "orders"}, {"dataset": "shop", "name": "events"}]}
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            self.assertIn("has not run here", export_warning(out, spec))
            (out / "exports").mkdir()
            report = {"shop.orders": {"export": "exported", "copy_failures": 0},
                      "shop.events": {"export": "already_exported", "copy_failures": 3}}
            (out / "exports/export_report.json").write_text(json.dumps(report))
            self.assertIn("shop.events", export_warning(out, spec))
            self.assertNotIn("shop.orders", export_warning(out, spec))
            report["shop.events"]["copy_failures"] = 0
            (out / "exports/export_report.json").write_text(json.dumps(report))
            self.assertEqual(export_warning(out, spec), "")


class DataprocJobs(unittest.TestCase):
    """Dataproc definitions publish as Python tasks (shapes proven live on AIDP) and SQL notebooks."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        manifest = copy.deepcopy(DEMO)
        template = manifest["sources"]["dataproc"]["items"]["workflow_templates"][0]
        template["steps"] = [st for st in template["steps"] if st["job_type"] != "spark"]  # no JAR step
        template["steps"][1]["prerequisites"] = ["extract"]
        cls.report = migrate(build_plan(manifest, oci_namespace="ns"), out_dir=cls.out)
        cls.jobs = {j["name"]: j for j in plan_publish(cls.out, prefix="ana", cluster_key="ck")["jobs"]}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_pyspark_is_a_python_task_with_argv_as_dataproc_set_it(self):
        task = self.jobs["ana_etl_nightly_sessionize_py"]["definition"]["tasks"][0]
        self.assertEqual((task["type"], task["filePath"]), ("PYTHON_TASK", "/Workspace/ana/etl_nightly_sessionize_py.py"))
        # Pasted by AIDP as `sys.argv = <value>`: a Python list, the script's own path first.
        self.assertEqual(ast.literal_eval(task["commandLineArguments"]),
                         ["/Workspace/ana/etl_nightly_sessionize_py.py", "2026-10-07"])
        self.assertNotIn("notebookPath", task)

    def test_spark_sql_runs_its_statements_in_a_notebook(self):
        task = self.jobs["ana_adhoc_analytics_cohorts_sql"]["definition"]["tasks"][0]
        self.assertEqual(task["type"], "NOTEBOOK_TASK")
        nb = self.out / "notebooks/jobs/adhoc_analytics_cohorts_sql_main.ipynb"
        cells = [c for c in json.loads(nb.read_text())["cells"] if c["cell_type"] == "code"]
        self.assertEqual(len(cells), 2)  # CREATE TEMPORARY VIEW, then INSERT OVERWRITE DIRECTORY
        self.assertIn("oci://northwind-landing@ns/reports/cohorts/", "".join(cells[1]["source"]))

    def test_template_steps_keep_their_order(self):
        tasks = self.jobs["ana_template_month_end_close"]["definition"]["tasks"]
        self.assertEqual([(t["taskKey"], t["type"], t.get("dependsOn")) for t in tasks],
                         [("extract", "PYTHON_TASK", None), ("report", "NOTEBOOK_TASK", [{"taskKey": "extract"}])])

    def test_P10_TEMPLATE_PARAMETER_and_P09_LOCAL_FILES(self):
        by_id = {r["asset_id"]: r for r in self.report["results"]}
        rules = {f["rule"] for f in by_id["dataproc.workflow_template.month-end-close"]["findings"]}
        self.assertIn("P10_TEMPLATE_PARAMETER", rules)
        job = by_id["dataproc.job.us-central1/etl-nightly/sessionize.py"]
        self.assertNotIn("P09_LOCAL_FILES", {f["rule"] for f in job["findings"]})  # the connector JAR is dropped
        manifest = copy.deepcopy(DEMO)
        manifest["sources"]["dataproc"]["items"]["jobs"][0]["python_files"] = ["gs://northwind-landing/lib/util.py"]
        with tempfile.TemporaryDirectory() as d:
            r = migrate(build_plan(manifest, oci_namespace="ns"), out_dir=Path(d))
        job = next(x for x in r["results"] if x["asset_id"] == "dataproc.job.us-central1/etl-nightly/sessionize.py")
        self.assertIn("python_files: gs://northwind-landing/lib/util.py",
                      next(f["detail"] for f in job["findings"] if f["rule"] == "P09_LOCAL_FILES"))

    def test_a_script_someone_else_left_there_is_not_run(self):
        fake = FakeAidp(present={"etl_nightly_sessionize_py.py"})
        r = publish(self.out, prefix="ana", cluster_key="ck", apply=True, client=fake, log=lambda m: None)
        status = {j["name"]: j["status"] for j in r["jobs"]}
        self.assertEqual(status["ana_etl_nightly_sessionize_py"], "refused")
        self.assertNotIn("/Workspace/ana/etl_nightly_sessionize_py.py", fake.uploaded)

    def test_apply_uploads_scripts_as_files(self):
        fake = FakeAidp()
        publish(self.out, prefix="ana", cluster_key="ck", apply=True, client=fake, log=lambda m: None)
        self.assertIn("/Workspace/ana/etl_nightly_sessionize_py.py", fake.uploaded)


class FailureCause(unittest.TestCase):
    def test_the_root_cause_is_shown_not_the_wrappers(self):
        from gcp_aidp.publish import failure_cause
        # As a Python task's message arrived from AIDP, 2026-10-09 (the script's own lines elided).
        message = ("java.lang.RuntimeException: [Command ab93_3a2b in Context 3a2b] failed with error: \n"
                   "Py4JJavaError: An error occurred while calling o318.json.\n"
                   ": org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme \"gs\"\n"
                   "\tat org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:3585)")
        self.assertEqual(failure_cause(message), 'UnsupportedFileSystemException: No FileSystem for scheme "gs"')
        self.assertEqual(failure_cause("Task timed out"), "")


class CliMessages(unittest.TestCase):
    def test_run_points_to_the_report_only_after_the_migration_job(self):
        import io
        from contextlib import redirect_stdout
        from unittest import mock

        from gcp_aidp.cli import main
        for job, says in (("gcp_aidp_migration", True), ("etl_nightly_sessionize_py", False)):  # success
            with mock.patch("gcp_aidp.publish.run", return_value={"ok": True, "finished": True}), \
                    redirect_stdout(io.StringIO()) as out:
                main(["run", "migrated", "--job", job, "--workspace-key", "w"])
            self.assertEqual("MIGRATION_REPORT.md" in out.getvalue(), says, job)


class DotEnv(unittest.TestCase):
    def test_only_the_plugins_own_keys_are_loaded(self):
        import os
        from unittest import mock

        from gcp_aidp._env import load_dotenv
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {}, clear=True):
            env = Path(d) / ".env"
            env.write_text("PATH=/tmp/evil\nAIDP_PREFIX=ana\nGCP_PROJECT=p\nOCI_NAMESPACE=ns\n")
            load_dotenv(env)
            self.assertEqual(dict(os.environ), {"AIDP_PREFIX": "ana", "GCP_PROJECT": "p", "OCI_NAMESPACE": "ns"})

    def test_an_empty_value_means_unset(self):
        # .env.example ships `OCI_CLI_PROFILE=`: exported as "", aidp-cli looks for a profile named ''
        import os
        from unittest import mock

        from gcp_aidp._env import load_dotenv
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {}, clear=True):
            env = Path(d) / ".env"
            env.write_text("OCI_CLI_PROFILE=\nAIDP_AUTH=api_key\nAIDP_PREFIX=''\n")
            load_dotenv(env)
            self.assertEqual(dict(os.environ), {"AIDP_AUTH": "api_key"})

    def test_working_directory_env_wins_over_the_plugins(self):
        import os
        from unittest import mock

        from gcp_aidp import _env
        with tempfile.TemporaryDirectory() as cwd, tempfile.TemporaryDirectory() as plugin, \
                mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(_env, "PLUGIN_ENV", Path(plugin) / ".env"):
            (Path(cwd) / ".env").write_text("AIDP_PREFIX=here\n")
            (Path(plugin) / ".env").write_text("AIDP_PREFIX=plugin\nAIDP_AUTH=api_key\n")
            old = os.getcwd()
            os.chdir(cwd)
            try:
                _env.load_dotenv()
            finally:
                os.chdir(old)
            self.assertEqual(dict(os.environ), {"AIDP_PREFIX": "here", "AIDP_AUTH": "api_key"})


if __name__ == "__main__":
    unittest.main()
