"""publish (dry run, --apply, never overwrite) and run, against a fake AIDP client."""
from __future__ import annotations

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
        self.assertEqual(len(p["notebooks"]), len(report["notebooks"]))
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
