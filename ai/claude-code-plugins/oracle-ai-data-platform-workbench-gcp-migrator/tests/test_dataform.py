"""Dataform: inventory, one test per DF rule, planning, migrate and the multi-task jobs (references/dataform-translation.md)."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from gcp_aidp.dataplane import MIGRATION_JOB, assertion_notebook, sql_notebook, write_jobs
from gcp_aidp.gcp_client import GcpClient, GcpError
from gcp_aidp.inventory import dataform as inventory
from gcp_aidp.inventory.manifest import build_manifest
from gcp_aidp.migrate import migrate
from gcp_aidp.plan import build_plan, write_plan_markdown
from gcp_aidp.publish import plan_publish, publish
from gcp_aidp.translate.dataform import find_cycle, register_outputs, schedule_text, translate_repository
from gcp_aidp.translate.googlesql_to_spark import Context

DEMO = json.loads((Path(__file__).parents[1] / "gcp_aidp/fixtures/demo-manifest.json").read_text())
P = "proj"
REPO = f"projects/{P}/locations/us-central1/repositories/r1"
API = "https://dataform.googleapis.com/v1"


# ── inventory ─────────────────────────────────────────────────────────────────

def _compiled(name, **kind):
    return {"target": {"database": P, "schema": "s", "name": name, "extra": "dropped"},
            "canonicalTarget": {"name": name}, "filePath": f"definitions/{name}.sqlx", **kind}


QUERY = {
    "compilationResultActions": [
        _compiled("src", declaration={"relationDescriptor": {"description": "dropped"}}),
        _compiled("t", relation={"relationType": "TABLE", "selectQuery": "SELECT 1 AS x", "tags": ["daily"],
                                 "relationDescriptor": {"columns": []}, "canonicalTarget": {},
                                 "dependencyTargets": [{"database": P, "schema": "s", "name": "src", "x": 1}],
                                 "incrementalTableConfig": {"incrementalSelectQuery": "SELECT 2",
                                                            "uniqueKeyParts": ["x"], "incrementalPreOperations": ["a"]}}),
    ],
    "nextPageToken": "p2",
}
QUERY_2 = {"compilationResultActions": [
    _compiled("op", operations={"queries": ["DELETE FROM s.t WHERE true"], "hasOutput": False,
                                "dependencyTargets": [], "relationDescriptor": {}}),
    _compiled("a", assertion={"selectQuery": "SELECT 1 WHERE FALSE", "parentAction": {"name": "t"}}),
    _compiled("nb", notebook={"contents": "big"}),
    _compiled("dp", dataPreparation={"contentsYaml": "big"}),
]}


class FakeDataform(GcpClient):
    """Serves `routes` (url -> body or exception). Like the real client, it can only GET."""

    def __init__(self, routes):
        super().__init__(P)
        self.routes, self.calls = routes, []

    def get(self, url, params=None):
        key = url + ("?" + "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else "")
        self.calls.append(key)
        body = self.routes.get(key)
        if body is None:
            raise GcpError(f"HTTP 404 notFound: {key}", status=404, reason="notFound")
        if isinstance(body, Exception):
            raise body
        return body


def _routes(**over):
    cr = f"{REPO}/compilationResults/c1"
    routes = {
        f"{API}/projects/{P}/locations/us-central1/repositories": {"repositories": [{"name": REPO}]},
        f"{API}/{REPO}/releaseConfigs": {"releaseConfigs": [
            {"name": f"{REPO}/releaseConfigs/off", "cronSchedule": "0 1 * * *", "disabled": True,
             "releaseCompilationResult": f"{REPO}/compilationResults/other"},
            {"name": f"{REPO}/releaseConfigs/prod", "cronSchedule": "0 4 * * *", "timeZone": "Etc/UTC",
             "releaseCompilationResult": cr}]},
        f"{API}/{REPO}/workflowConfigs": {"workflowConfigs": [
            {"name": f"{REPO}/workflowConfigs/nightly", "cronSchedule": "30 4 * * *", "timeZone": "Etc/UTC"}]},
        f"{API}/{cr}": {"name": cr},
        f"{API}/{cr}:query": QUERY,
        f"{API}/{cr}:query?pageToken=p2": QUERY_2,
    }
    routes.update(over)
    return routes


def _scan(**over):
    client = FakeDataform(_routes(**over))
    return inventory.scan(client, regions=("us-central1",)), client


class Inventory(unittest.TestCase):
    def test_actions_of_the_release_compilation_result_with_only_the_fields_we_use(self):
        out, client = _scan()
        repo = out["items"]["repositories"][0]
        self.assertEqual((repo["name"], repo["region"]), ("r1", "us-central1"))
        self.assertNotIn("not_scanned", out["summary"])
        self.assertEqual([a["target"]["name"] for a in repo["actions"]], ["src", "t", "op", "a", "nb", "dp"])  # page 2 followed
        src, t, op, a, nb, dp = repo["actions"]
        self.assertEqual(src, {"target": {"database": P, "schema": "s", "name": "src"},
                               "filePath": "definitions/src.sqlx", "declaration": {}})
        self.assertEqual(t["relation"], {
            "relationType": "TABLE", "selectQuery": "SELECT 1 AS x", "tags": ["daily"],
            "dependencyTargets": [{"database": P, "schema": "s", "name": "src"}],
            "incrementalTableConfig": {"incrementalSelectQuery": "SELECT 2", "uniqueKeyParts": ["x"]}})
        self.assertEqual(op["operations"], {"queries": ["DELETE FROM s.t WHERE true"], "hasOutput": False,
                                            "dependencyTargets": []})
        self.assertEqual(a["assertion"], {"selectQuery": "SELECT 1 WHERE FALSE"})
        self.assertEqual((nb["notebook"], dp["dataPreparation"]), ({}, {}))  # the kind, not the contents
        self.assertIn("compilation result c1", repo["compiled_from"] + " compilation result c1")
        self.assertTrue(repo["compiled_from"].startswith("an enabled release config"))
        self.assertFalse(any(k in x for x in repo["actions"] for k in ("canonicalTarget", "relationDescriptor")))

    def test_schedules_are_recorded_as_text_and_the_disabled_release_config_is_skipped(self):
        repo = _scan()[0]["items"]["repositories"][0]
        self.assertEqual(repo["schedules"], [
            {"kind": "release_config", "name": "off", "cronSchedule": "0 1 * * *", "timeZone": "", "disabled": "true"},
            {"kind": "release_config", "name": "prod", "cronSchedule": "0 4 * * *", "timeZone": "Etc/UTC",
             "disabled": "false"},
            {"kind": "workflow_config", "name": "nightly", "cronSchedule": "30 4 * * *", "timeZone": "Etc/UTC",
             "disabled": "false"}])
        self.assertEqual(schedule_text(repo), "release config off 0 1 * * * (disabled); release config prod "
                         "0 4 * * * Etc/UTC; workflow config nightly 30 4 * * * Etc/UTC")

    def test_only_get_calls_and_no_compilation_is_created(self):
        _, client = _scan()
        self.assertFalse(any(hasattr(GcpClient, m) for m in ("post", "put", "patch", "delete")))
        self.assertFalse(any(":invoke" in c or c.endswith("compilationResults") and "?" in c for c in client.calls))

    def test_without_a_release_config_the_newest_compilation_result_is_read(self):
        old, new = f"{REPO}/compilationResults/old", f"{REPO}/compilationResults/new"
        out, _ = _scan(**{f"{API}/{REPO}/releaseConfigs": {},
                          f"{API}/{REPO}/compilationResults": {"compilationResults": [
                              {"name": new, "createTime": "2026-10-02T00:00:00Z"},
                              {"name": old, "createTime": "2026-10-01T00:00:00Z"}]},
                          f"{API}/{new}": {"name": new}, f"{API}/{new}:query": QUERY_2})
        repo = out["items"]["repositories"][0]
        self.assertEqual(len(repo["actions"]), 4)
        self.assertIn("the newest compilation result (new)", repo["compiled_from"])

    def test_no_compilation_result_is_not_scanned_and_the_repository_is_kept(self):
        out, _ = _scan(**{f"{API}/{REPO}/releaseConfigs": {}, f"{API}/{REPO}/compilationResults": {}})
        repo = out["items"]["repositories"][0]
        self.assertEqual((repo["name"], repo["actions"]), ("r1", []))
        self.assertIn("no compilation results", out["summary"]["not_scanned"]["actions of r1"])
        self.assertEqual(repo["actions_not_scanned"], out["summary"]["not_scanned"]["actions of r1"])
        self.assertEqual(out["summary"]["repositories"], 1)

    def test_compilation_errors_are_not_scanned(self):
        cr = f"{REPO}/compilationResults/c1"
        out, _ = _scan(**{f"{API}/{cr}": {"name": cr, "compilationErrors": [
            {"message": "Could not resolve \"x\"\nstack", "path": "definitions/a.sqlx"}, {"message": "b"}]}})
        repo = out["items"]["repositories"][0]
        self.assertEqual(repo["actions"], [])
        self.assertIn("2 compilation error(s); first: Could not resolve", out["summary"]["not_scanned"]["actions of r1"])

    def test_a_refused_read_costs_only_that_repository(self):
        second = f"projects/{P}/locations/us-central1/repositories/r2"
        c2 = f"{second}/compilationResults/c2"
        out, _ = _scan(**{
            f"{API}/projects/{P}/locations/us-central1/repositories": {"repositories": [{"name": REPO}, {"name": second}]},
            f"{API}/{REPO}/compilationResults/c1:query": GcpError("HTTP 403 PERMISSION_DENIED", status=403),
            f"{API}/{second}/releaseConfigs": {"releaseConfigs": [
                {"name": f"{second}/releaseConfigs/p", "releaseCompilationResult": c2}]},
            f"{API}/{second}/workflowConfigs": {}, f"{API}/{c2}": {"name": c2},
            f"{API}/{c2}:query": QUERY_2})
        r1, r2 = out["items"]["repositories"]
        self.assertEqual((r1["actions"], len(r2["actions"])), ([], 4))
        self.assertEqual(list(out["summary"]["not_scanned"]), ["actions of r1"])

    def test_unreadable_schedules_are_recorded_and_the_actions_still_read(self):
        out, _ = _scan(**{f"{API}/{REPO}/workflowConfigs": GcpError("HTTP 403 PERMISSION_DENIED", status=403),
                          f"{API}/{REPO}/releaseConfigs": GcpError("HTTP 403 PERMISSION_DENIED", status=403),
                          f"{API}/{REPO}/compilationResults": {"compilationResults": [
                              {"name": f"{REPO}/compilationResults/n", "createTime": "2026-10-02T00:00:00Z"}]},
                          f"{API}/{REPO}/compilationResults/n": {}, f"{API}/{REPO}/compilationResults/n:query": QUERY_2})
        repo = out["items"]["repositories"][0]
        self.assertEqual((repo["schedules"], len(repo["actions"])), ([], 4))
        self.assertEqual(sorted(out["summary"]["not_scanned"]), ["releaseConfigs of r1", "workflowConfigs of r1"])

    def test_the_plan_shows_what_was_not_scanned_and_still_plans_the_repository(self):
        out, _ = _scan(**{f"{API}/{REPO}/releaseConfigs": {}, f"{API}/{REPO}/compilationResults": {}})
        plan = build_plan({"project_id": P, "sources": {"dataform": out}})
        self.assertIn("not scanned", plan["scan_errors"]["dataform.actions of r1"])
        self.assertEqual([(a["action"], a["source"]["actions"]) for a in plan["assets"]], [("MIGRATE", [])])

    def test_an_api_that_is_not_enabled_is_recorded_not_failed(self):
        client = FakeDataform({f"{API}/projects/{P}/locations/us-central1/repositories":
                               GcpError("HTTP 403", status=403, reason="SERVICE_DISABLED")})
        out = inventory.scan(client)
        self.assertEqual((out["summary"]["api_disabled"], out["items"]["repositories"]), (True, []))

    def test_scanned_through_the_manifest(self):
        client = FakeDataform(_routes())
        m = build_manifest(client, ("dataform",), scan_services=True, service_client=client)
        self.assertEqual(len(m["sources"]["dataform"]["items"]["repositories"][0]["actions"]), 6)


# ── the rules ─────────────────────────────────────────────────────────────────

def tgt(name, schema="s", database=P):
    return {"database": database, "schema": schema, "name": name}


def table(name, query="SELECT 1 AS x", deps=(), **rel):
    return {"target": tgt(name), "relation": {"relationType": "TABLE", "selectQuery": query,
                                              "dependencyTargets": [tgt(d) for d in deps], **rel}}


def view(name, query="SELECT 1 AS x", deps=(), **rel):
    return {"target": tgt(name), "relation": {"relationType": "VIEW", "selectQuery": query,
                                              "dependencyTargets": [tgt(d) for d in deps], **rel}}


def assertion(name, query="SELECT 1 WHERE FALSE", deps=(), **kw):
    return {"target": tgt(name, "dataform_assertions"),
            "assertion": {"selectQuery": query, "dependencyTargets": [tgt(d) for d in deps], **kw}}


def declaration(name):
    return {"target": tgt(name), "declaration": {}}


def run(*actions, repo="r", ctx=None, **extra):
    item = {"name": repo, "actions": list(actions), **extra}
    ctx = ctx or Context(project=P)
    register_outputs(item, ctx, "cat")
    return translate_repository(item, ctx, "cat", f"dataform_{repo}")


def rules(findings, severity=None):
    return {f["rule"] for f in findings if severity is None or f["severity"] == severity}


class Rules(unittest.TestCase):
    def test_DF01_TABLE_creates_if_not_exists_then_overwrites_and_never_drops(self):
        ctx = Context(project=P, relations={("s", "src"): ("cat", "s", "src")})  # the declared source, migrated from BigQuery
        job, findings, ok = run(table("t", "SELECT id FROM `proj.s.src`"), declaration("src"), ctx=ctx)
        (task,) = job["tasks"]
        self.assertEqual(task["statements"], [
            "CREATE TABLE IF NOT EXISTS `cat`.`s`.`t` USING DELTA AS\nSELECT id FROM `cat`.`s`.`src`",
            "INSERT OVERWRITE TABLE `cat`.`s`.`t`\nSELECT id FROM `cat`.`s`.`src`"])
        self.assertIn(("DF01_TABLE", "rewrite"), {(f["rule"], f["severity"]) for f in findings})
        self.assertTrue(ok)
        text = " ".join(task["statements"]).upper()
        self.assertNotIn("DROP", text)
        self.assertNotIn("OR REPLACE", text)

    def test_DF02_VIEW_reuses_create_view(self):
        job, findings, ok = run(view("v", "SELECT 1 AS x"))
        self.assertEqual(job["tasks"][0]["statements"], ["CREATE VIEW IF NOT EXISTS `cat`.`s`.`v` AS\nSELECT 1 AS x"])
        self.assertIn("DF02_VIEW", rules(findings, "rewrite"))
        self.assertTrue(ok)

    def test_DF03_ASSERTION_task_runs_the_query_and_fails_on_a_row(self):
        job, findings, ok = run(table("t"), assertion("a", "SELECT * FROM `proj.s.t` WHERE x < 0", deps=["t"]))
        task = job["tasks"][1]
        self.assertEqual((task["assertion"], task["dependsOn"]), (True, ["s_t"]))
        self.assertEqual(task["statements"], ["SELECT * FROM `cat`.`s`.`t` WHERE x < 0"])
        self.assertIn("DF03_ASSERTION", rules(findings, "rewrite"))
        self.assertTrue(ok)

    def test_DF04_INCREMENTAL_flags_and_blocks_the_job_without_guessing_a_merge(self):
        job, findings, ok = run(table("i", relationType="INCREMENTAL_TABLE", incrementalTableConfig={
            "incrementalSelectQuery": "SELECT 2", "uniqueKeyParts": ["x"]}), table("t"))
        self.assertIn("DF04_INCREMENTAL", rules(findings, "flag"))
        self.assertIn("not translated yet", next(f["detail"] for f in findings if f["rule"] == "DF04_INCREMENTAL"))
        self.assertFalse(ok)
        self.assertNotIn("MERGE", json.dumps(job))
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["s_t"])  # the incremental one has no task

    def test_DF05_RELATION_TYPE_flags_materialized_external_snapshot_and_unknown(self):
        for rtype in ("MATERIALIZED_VIEW", "EXTERNAL", "SNAPSHOT", "SOMETHING_NEW", ""):
            job, findings, ok = run(table("m", relationType=rtype))
            self.assertIn("DF05_RELATION_TYPE", rules(findings, "flag"), rtype)
            self.assertEqual((job["tasks"], ok), ([], False))

    def test_DF06_OPERATIONS_flags_custom_operations_and_pre_post_operations(self):
        ops = {"target": tgt("o"), "operations": {"queries": ["DECLARE x INT64", "SELECT 1"], "hasOutput": False}}
        for action in (ops, table("t", preOperations=["SELECT 1"]), view("v", postOperations=["GRANT x"])):
            job, findings, ok = run(action)
            self.assertIn("DF06_OPERATIONS", rules(findings, "flag"))
            self.assertEqual((job["tasks"], ok), ([], False))

    def test_DF07_DECLARATION_has_no_task_and_is_info(self):
        job, findings, ok = run(declaration("src"))
        self.assertEqual(job["tasks"], [])
        self.assertEqual({(f["rule"], f["severity"]) for f in findings}, {("DF07_DECLARATION", "info"),
                                                                          ("DF15_NO_TASKS", "info")})
        self.assertFalse(ok)  # nothing to run

    def test_DF08_DISABLED_has_no_task_and_is_info(self):
        job, findings, ok = run(table("t"), table("off", disabled=True), view("v", deps=["off"]))
        self.assertIn(("DF08_DISABLED", "info"), {(f["rule"], f["severity"]) for f in findings})
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["s_t", "s_v"])
        self.assertEqual(job["tasks"][1]["dependsOn"], [])  # it depends on something that does not run
        self.assertTrue(ok)

    def test_DF09_NOT_TRANSLATED_ACTION_flags_notebooks_data_preparation_and_unknown(self):
        for action in ({"target": tgt("n"), "notebook": {}}, {"target": tgt("d"), "dataPreparation": {}},
                       {"target": tgt("u")}):
            job, findings, ok = run(action)
            self.assertIn("DF09_NOT_TRANSLATED_ACTION", rules(findings, "flag"))
            self.assertEqual((job["tasks"], ok), ([], False))

    def test_DF10_CYCLE_blocks(self):
        job, findings, ok = run(table("a", deps=["b"]), table("b", deps=["c"]), table("c", deps=["a"]))
        self.assertIn("DF10_CYCLE", rules(findings, "block"))
        self.assertIn("s.a → s.b → s.c → s.a", next(f["detail"] for f in findings if f["rule"] == "DF10_CYCLE"))
        self.assertFalse(ok)

    def test_find_cycle_is_a_plain_function_over_a_dependency_map(self):
        self.assertEqual(find_cycle({"a": ["b"], "b": ["c"], "c": []}), [])
        self.assertEqual(find_cycle({"a": ["a"]}), ["a", "a"])
        self.assertEqual(find_cycle({"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}), [])  # diamond: no cycle
        self.assertEqual(find_cycle({"x": ["y"], "y": ["z"], "z": ["y"]}), ["y", "z", "y"])
        self.assertEqual(find_cycle({"a": ["missing"]}), [])
        chain = {str(i): [str(i + 1)] for i in range(5000)}  # deeper than the recursion limit
        chain["5000"] = []
        self.assertEqual(find_cycle(chain), [])
        chain["5000"] = ["0"]
        self.assertEqual(len(find_cycle(chain)), 5002)

    def test_DF11_CROSS_PROJECT_flags(self):
        action = table("t")
        action["target"]["database"] = "other-project"
        job, findings, ok = run(action)
        self.assertIn("DF11_CROSS_PROJECT", rules(findings, "flag"))
        self.assertFalse(ok)

    def test_DF12_DEPENDENCY_OUTSIDE_is_info_and_a_declaration_is_not_outside(self):
        job, findings, ok = run(table("t", deps=["elsewhere", "src"]), declaration("src"))
        outside = [f for f in findings if f["rule"] == "DF12_DEPENDENCY_OUTSIDE"]
        self.assertEqual([(f["severity"], "s.elsewhere" in f["detail"]) for f in outside], [("info", True)])
        self.assertTrue(ok)

    def test_DF13_LAYOUT_is_recorded_when_partitioning_is_not_carried(self):
        job, findings, ok = run(table("t", partitionExpression="DATE(ts)", clusterExpressions=["id"]))
        self.assertIn("DF13_LAYOUT", rules(findings, "info"))
        self.assertTrue(ok)

    def test_DF14_NOT_SCANNED_blocks_a_repository_whose_actions_could_not_be_read(self):
        job, findings, ok = run(actions_not_scanned="not scanned: HTTP 403 PERMISSION_DENIED")
        self.assertEqual({(f["rule"], f["severity"]) for f in findings}, {("DF14_NOT_SCANNED", "block")})
        self.assertIn("HTTP 403", findings[0]["detail"])
        self.assertEqual((job["tasks"], ok), ([], False))

    def test_DF15_NO_TASKS_is_info_for_an_empty_repository(self):
        job, findings, ok = run()
        self.assertEqual({(f["rule"], f["severity"]) for f in findings}, {("DF15_NO_TASKS", "info")})
        self.assertFalse(ok)

    def test_the_sql_translators_findings_pass_through_unchanged(self):
        job, findings, ok = run(view("v", "SELECT SAFE_DIVIDE(a, b) AS r, ML.PREDICT(1) FROM x"),
                                table("t", "SELECT SAFE_DIVIDE(a, b) AS r FROM `proj.s.src`"), declaration("src"),
                                ctx=Context(project=P, relations={("s", "src"): ("cat", "s", "src")}))
        self.assertIn(("G18_ML_AI_GEO", "block"), {(f["rule"], f["severity"]) for f in findings})
        self.assertIn("G03_SAFE_DIVIDE", rules(findings, "caveat"))
        self.assertEqual([t["taskKey"] for t in job["tasks"]], ["s_t"])  # the blocked view has no task
        self.assertFalse(ok)

    def test_G01_REFERENCE_resolves_a_view_selecting_from_another_actions_output(self):
        job, findings, ok = run(table("t"), view("v", "SELECT x FROM `proj.s.t`", deps=["t"]))
        self.assertIn("G01_REFERENCE", rules(findings, "rewrite"))
        self.assertNotIn("G01_REFERENCE", rules(findings, "flag"))
        self.assertIn("FROM `cat`.`s`.`t`", job["tasks"][1]["statements"][0])
        self.assertTrue(ok)
        # a name that no action creates is still flagged, as before
        _, findings, ok = run(view("v", "SELECT x FROM `proj.s.nobody`"))
        self.assertIn("G01_REFERENCE", rules(findings, "flag"))
        self.assertFalse(ok)

    def test_a_disabled_or_incremental_output_is_not_a_name_the_sql_can_resolve(self):
        ctx = Context(project=P)
        register_outputs({"actions": [table("off", disabled=True), table("i", relationType="INCREMENTAL_TABLE"),
                                      table("ok"), view("v")]}, ctx, "cat")
        self.assertEqual(sorted(ctx.relations), [("s", "ok"), ("s", "v")])


class DependsOn(unittest.TestCase):
    def test_fan_out_and_fan_in_become_dependsOn_in_dependency_order(self):
        # declared out of order: d needs b and c, which both need a
        job, _, ok = run(table("d", deps=["b", "c"]), table("c", deps=["a"]), table("b", deps=["a"]), table("a"))
        self.assertTrue(ok)
        keys = [t["taskKey"] for t in job["tasks"]]
        self.assertEqual(keys[0], "s_a")
        self.assertEqual(keys[-1], "s_d")
        by_key = {t["taskKey"]: t["dependsOn"] for t in job["tasks"]}
        self.assertEqual(by_key, {"s_a": [], "s_b": ["s_a"], "s_c": ["s_a"], "s_d": ["s_b", "s_c"]})

    def test_a_task_key_is_a_valid_unique_name(self):
        job, _, _ = run(table("a-b"), table("a_b"), table("9"), {"target": tgt("x", "dataform_assertions"),
                                                                 "assertion": {"selectQuery": "SELECT 1 WHERE FALSE"}})
        keys = [t["taskKey"] for t in job["tasks"]]
        self.assertEqual(keys, ["s_a_b", "s_a_b_2", "s_9", "dataform_assertions_x"])

    def test_the_target_is_the_plan_catalog_with_the_dataform_schema_and_name(self):
        job, _, _ = run(table("t"))
        self.assertIn("`cat`.`s`.`t`", job["tasks"][0]["statements"][0])


# ── planning ──────────────────────────────────────────────────────────────────

class Planning(unittest.TestCase):
    def test_a_repository_is_a_migrate_row_with_its_schedule_in_the_notes(self):
        plan = build_plan(DEMO)
        row = next(a for a in plan["assets"] if a["id"] == "dataform.repository.northwind-transformations")
        self.assertEqual((row["action"], row["kind"], row["version"]), ("MIGRATE", "code", "0.3"))
        self.assertEqual(row["source"]["type"], "dataform_repository")
        self.assertEqual(len(row["source"]["actions"]), 5)
        self.assertEqual(row["target"], {"type": "aidp_job", "name": "dataform_northwind_transformations"})
        self.assertEqual(row["transform_chain"], ["dataform_compiled_actions", "googlesql_to_spark",
                                                  "create_job_unscheduled"])
        self.assertIn("release config production 0 4 * * * America/New_York", row["notes"][0])
        self.assertIn("not applied", row["notes"][0])

    def test_dataform_repos_selects_and_skips_the_rest(self):
        plan = build_plan(DEMO, dataform_repos=["northwind-events-incremental"])
        by = {a["source"]["name"]: a for a in plan["assets"] if a["source"]["type"] == "dataform_repository"}
        self.assertEqual(by["northwind-events-incremental"]["action"], "MIGRATE")
        self.assertEqual(by["northwind-transformations"]["action"], "SKIP")
        self.assertEqual(by["northwind-transformations"]["reason"],
                         "repository northwind-transformations is outside --dataform-repos")
        self.assertEqual(by["northwind-transformations"]["transform_chain"], [])
        self.assertEqual(plan["scope"]["dataform_repos"], ["northwind-events-incremental"])
        with tempfile.TemporaryDirectory() as d:
            md = write_plan_markdown(plan, Path(d) / "plan.md").read_text()
        self.assertIn("Dataform repositories: northwind-events-incremental (1 of 2; the rest are SKIP)", md)
        self.assertIn("Datasets: all 4", md)

    def test_default_scope_and_unknown_name(self):
        plan = build_plan(DEMO)
        self.assertIsNone(plan["scope"]["dataform_repos"])
        with tempfile.TemporaryDirectory() as d:
            self.assertIn("Dataform repositories: all 2", write_plan_markdown(plan, Path(d) / "p.md").read_text())
        with self.assertRaisesRegex(ValueError, "not in the inventory: northwind-x; its repositories are: "
                                                "northwind-events-incremental, northwind-transformations"):
            build_plan(DEMO, dataform_repos=["northwind-x"])

    def test_a_manifest_without_dataform_has_no_dataform_scope_text(self):
        plan = build_plan({"project_id": "p", "sources": {"bigquery": {"items": {}}}})
        with tempfile.TemporaryDirectory() as d:
            self.assertNotIn("Dataform", write_plan_markdown(plan, Path(d) / "p.md").read_text())

    def test_two_repositories_cannot_map_to_one_job(self):
        m = {"project_id": "p", "sources": {"dataform": {"items": {"repositories": [{"name": "a-b"}, {"name": "a_b"}]}}}}
        with self.assertRaisesRegex(ValueError, "collision"):
            build_plan(m)

    def test_cli_flag(self):
        from gcp_aidp.cli import build_parser
        args = build_parser().parse_args(["plan", "m.json", "--dataform-repos", "a, b"])
        self.assertEqual(args.dataform_repos, "a, b")


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

    def test_the_repository_becomes_one_job_with_a_task_per_translated_action(self):
        r = self.by_id["dataform.repository.northwind-transformations"]
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["output_path"], "dataform/northwind-transformations.sql")
        job = r["job"]
        self.assertEqual(job["name"], "dataform_northwind_transformations")
        self.assertIn("0 4 * * * America/New_York", job["title"])
        self.assertIn("not applied", job["title"])
        self.assertEqual({t["taskKey"]: t["dependsOn"] for t in job["tasks"]}, {
            "sales_order_totals_by_day": [],
            "sales_v_order_totals_nonempty": ["sales_order_totals_by_day"],
            "dataform_assertions_order_totals_by_day_assertions_uniqueKey_0": ["sales_order_totals_by_day"]})
        text = (self.out / r["output_path"]).read_text()
        self.assertIn("-- action sales.legacy_order_export (table): no task", text)
        self.assertIn("-- INFO DF08_DISABLED", text)
        self.assertIn("-- action sales.v_order_totals_nonempty (view) -> task sales_v_order_totals_nonempty, "
                      "after sales_order_totals_by_day", text)

    def test_the_incremental_repository_is_reported_with_no_job(self):
        r = self.by_id["dataform.repository.northwind-events-incremental"]
        self.assertEqual(r["status"], "needs_manual_review")
        self.assertNotIn("job", r)
        self.assertIn(("DF04_INCREMENTAL", "flag"), [(f["rule"], f["severity"]) for f in r["findings"]])
        self.assertNotIn("events-incremental", " ".join(j["name"] for j in self.report["jobs"]))
        self.assertTrue((self.out / r["output_path"]).is_file())

    def test_the_job_notebooks_and_dependsOn_are_in_the_report(self):
        job = next(j for j in self.report["jobs"] if j["name"] == "dataform_northwind_transformations")
        self.assertEqual(len(job["tasks"]), 3)
        for t in job["tasks"]:
            self.assertIn(t["notebook"], self.report["notebooks"])
            self.assertTrue((self.out / t["notebook"]).is_file())
        deps = {t["taskKey"]: t.get("dependsOn", []) for t in job["tasks"]}
        self.assertEqual(deps["sales_v_order_totals_nonempty"], ["sales_order_totals_by_day"])
        self.assertNotIn("dataform", MIGRATION_JOB)

    def test_the_assertion_notebook_fails_when_the_query_returns_a_row(self):
        path = "notebooks/jobs/dataform_northwind_transformations__dataform_assertions_order_totals_by_day_" \
               "assertions_uniqueKey_0.ipynb"
        cells = [c for c in json.loads((self.out / path).read_text())["cells"] if c["cell_type"] == "code"]
        code = "".join(cells[0]["source"])
        compile(code, path, "exec")

        class Rows:
            def __init__(self, rows):
                self.rows = rows

            def sql(self, query):
                self.query = query
                return self

            def limit(self, n):
                return self

            def collect(self):
                return self.rows

        spark = Rows([("2026-10-01", 2)])
        with self.assertRaisesRegex(RuntimeError, "assertion failed"):
            exec(code, {"spark": spark})
        self.assertIn("index_row_count > 1", spark.query)
        with redirect_stdout(io.StringIO()):
            exec(code, {"spark": Rows([])})  # no rows: the task passes

    def test_verify_labels_the_results(self):
        from gcp_aidp.verify import verify
        rows = {r["asset_id"]: r["verdict"] for r in verify(self.out / "report.json")["rows"]}
        self.assertEqual(rows["dataform.repository.northwind-transformations"], "PASS")
        self.assertEqual(rows["dataform.repository.northwind-events-incremental"], "REVIEW")

    def test_a_repository_outside_dataform_repos_is_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(DEMO, dataform_repos=["northwind-events-incremental"]), out_dir=Path(d))
        r = {x["asset_id"]: x for x in report["results"]}["dataform.repository.northwind-transformations"]
        self.assertEqual(r["status"], "skipped")
        self.assertNotIn("dataform_northwind_transformations", [j["name"] for j in report["jobs"]])

    def test_a_repository_that_could_not_be_read_is_blocked_with_its_reason(self):
        m = {"project_id": P, "sources": {"dataform": {"summary": {"not_scanned": {"actions of r": "HTTP 403"}},
                                                       "items": {"repositories": [
                                                           {"name": "r", "actions": [],
                                                            "actions_not_scanned": "HTTP 403"}]}}}}
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(m), out_dir=Path(d))
            r = report["results"][0]
            self.assertEqual((r["status"], r["findings"][0]["rule"]), ("blocked", "DF14_NOT_SCANNED"))
            self.assertNotIn("job", r)
            self.assertTrue(all(l.startswith("--") or not l.strip() for l in (Path(d) / r["output_path"]).read_text().splitlines()))

    def test_no_partial_job_when_one_action_is_flagged(self):
        repo = {"name": "r", "actions": [table("ok"), table("i", relationType="INCREMENTAL_TABLE")]}
        m = {"project_id": P, "sources": {"dataform": {"items": {"repositories": [repo]}}}}
        with tempfile.TemporaryDirectory() as d:
            report = migrate(build_plan(m), out_dir=Path(d))
            self.assertEqual(report["jobs"][0]["name"], MIGRATION_JOB)
            self.assertEqual(len(report["jobs"]), 1)  # only the migration job: no dataform job, not even for `ok`
            self.assertNotIn("job", report["results"][0])


class WriteJobs(unittest.TestCase):
    def test_the_single_statement_form_is_unchanged(self):
        dp = {"plan_id": "x", "project": "p", "catalog": "c", "tables": []}
        results = [{"status": "ok", "kind": "bq_scheduled_query",
                    "job": {"name": "daily", "title": "Daily", "statements": ["SELECT 1", "SELECT 2"]}}]
        with tempfile.TemporaryDirectory() as d:
            jobs = write_jobs(dp, results, Path(d))
            written = (Path(d) / "notebooks/jobs/daily.ipynb").read_text()
        self.assertEqual(written, json.dumps(sql_notebook("Daily", ["SELECT 1", "SELECT 2"], "x"), indent=1) + "\n")
        self.assertEqual(jobs[1], {"name": "daily", "description": "Daily", "tasks": [
            {"taskKey": "daily", "notebook": "notebooks/jobs/daily.ipynb", "parameters": []}]})

    def test_the_multi_task_form_writes_a_notebook_per_task(self):
        dp = {"plan_id": "x", "project": "p", "catalog": "c", "tables": []}
        job = {"name": "j", "title": "J", "tasks": [
            {"taskKey": "a", "title": "A", "statements": ["CREATE TABLE a", "INSERT a"], "assertion": False, "dependsOn": []},
            {"taskKey": "b", "title": "B", "statements": ["SELECT bad"], "assertion": True, "dependsOn": ["a"]}]}
        with tempfile.TemporaryDirectory() as d:
            jobs = write_jobs(dp, [{"status": "needs_manual_review", "kind": "dataform_repository", "job": job},
                                   {"status": "blocked", "kind": "dataform_repository", "job": dict(job, name="no")}],
                              Path(d))
            self.assertEqual([j["name"] for j in jobs], [MIGRATION_JOB, "j"])  # a blocked result makes no job
            self.assertEqual(jobs[1]["tasks"], [
                {"taskKey": "a", "notebook": "notebooks/jobs/j__a.ipynb", "parameters": []},
                {"taskKey": "b", "notebook": "notebooks/jobs/j__b.ipynb", "parameters": [], "dependsOn": ["a"]}])
            self.assertEqual((Path(d) / "notebooks/jobs/j__a.ipynb").read_text(),
                             json.dumps(sql_notebook("A", ["CREATE TABLE a", "INSERT a"], "x"), indent=1) + "\n")
            self.assertEqual((Path(d) / "notebooks/jobs/j__b.ipynb").read_text(),
                             json.dumps(assertion_notebook("B", "SELECT bad", "x"), indent=1) + "\n")
            self.assertFalse((Path(d) / "notebooks/jobs/no__a.ipynb").exists())


class Publish(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        migrate(build_plan(DEMO, oci_namespace="ns"), out_dir=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_the_dry_run_lists_the_dataform_job_with_its_task_graph(self):
        p = publish(self.out, prefix="ana", cluster_key="ck")
        self.assertFalse(p["applied"])
        jobs = {j["name"]: j["definition"] for j in p["jobs"]}
        self.assertIn("ana_dataform_northwind_transformations", jobs)
        self.assertFalse([n for n in jobs if "events_incremental" in n])
        tasks = {t["taskKey"]: t for t in jobs["ana_dataform_northwind_transformations"]["tasks"]}
        self.assertEqual(tasks["sales_v_order_totals_nonempty"]["dependsOn"], [{"taskKey": "sales_order_totals_by_day"}])
        self.assertNotIn("dependsOn", tasks["sales_order_totals_by_day"])
        self.assertIn("0 4 * * * America/New_York", jobs["ana_dataform_northwind_transformations"]["description"])
        self.assertTrue(all(t["notebookPath"].startswith("/Workspace/ana/dataform_northwind_transformations__")
                            for t in tasks.values()))

    def test_fan_out_and_fan_in_reach_the_job_definition(self):
        repo = {"name": "d", "actions": [table("a"), table("b", deps=["a"]), table("c", deps=["a"]),
                                         table("d", deps=["b", "c"])]}
        m = {"project_id": P, "sources": {"dataform": {"items": {"repositories": [repo]}}}}
        with tempfile.TemporaryDirectory() as d:
            migrate(build_plan(m), out_dir=Path(d))
            jobs = {j["name"]: j["definition"] for j in plan_publish(d, prefix="p", cluster_key="ck")["jobs"]}
        deps = {t["taskKey"]: [x["taskKey"] for x in t.get("dependsOn", [])]
                for t in jobs["p_dataform_d"]["tasks"]}
        self.assertEqual(deps, {"s_a": [], "s_b": ["s_a"], "s_c": ["s_a"], "s_d": ["s_b", "s_c"]})


if __name__ == "__main__":
    unittest.main()
