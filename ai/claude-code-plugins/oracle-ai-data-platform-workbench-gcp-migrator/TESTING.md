# Testing guide

The core pass needs no Google Cloud or OCI account: everything runs offline against
the bundled Northwind Retail fixture. The live passes at the end need a BigQuery
project and an AIDP workspace.

## 1. Setup

```bash
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
cd oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

Python 3.9+. The offline verbs use the standard library only.

## 2. Smoke test: the demo

```bash
./demo.sh
```

It runs `inventory → plan → migrate → verify` on the fixture, prints a view before
and after translation and a blocked view, and ends with a `publish` dry run that
contacts nothing. Output goes to `/tmp/gcp-aidp-demo/` (`OUT=<dir>` to change it).

✅ Pass if it exits 0 and `migrate` ends with `error=0`.

## 3. Unit tests

```bash
python3 -m unittest discover -s tests -t .
```

✅ Pass if every test passes. The Spark and MCP tests are skipped unless their
optional dependencies are installed:

```bash
# Spark 3.5 + Delta (needs Java 17; the first run downloads the Delta jars)
pip install pyspark==3.5.9 delta-spark==3.2.1
python3 -m unittest tests.test_spark_runtime tests.test_dataplane_runtime

# MCP server (Python 3.10+)
pip install -e '.[mcp]'
python3 -m unittest tests.test_mcp_server
```

`test_spark_runtime` runs every generated statement whose findings are only rewrites
and caveats on Spark, and checks the `QUALIFY` rewrite keeps the same rows.
`test_dataplane_runtime` executes every cell of the generated `00`–`03` notebooks,
with the BigQuery connector replaced by a local reader typed the way the connector
delivered on AIDP.

## 4. The verbs by hand

```bash
python3 -m gcp_aidp.cli inventory --fixture demo -o /tmp/inv.json
python3 -m gcp_aidp.cli plan      /tmp/inv.json -o /tmp/plan.json
python3 -m gcp_aidp.cli migrate   /tmp/plan.json -o /tmp/migrated
python3 -m gcp_aidp.cli verify    /tmp/migrated
```

Check that:

- `plan.md` lists every asset with `MIGRATE`, `REPORT` or `SKIP`, and the SKIP rows
  name the version that will migrate them.
- `migrated/report.md` shows before/after SQL, and every caveat, flag and block names
  its rule.
- A blocked view's artifact keeps the original SQL, commented out.
- `verify` exits 0 with no FAIL.

## 5. Live inventory

[`test-estate/MANUAL_STEPS.md`](test-estate/MANUAL_STEPS.md) seeds a small estate
(`seed.sql`), adds what SQL cannot create, and lists what the inventory must find.
The BigQuery sandbox (no billing) covers tables, views, materialized views,
routines, models and policies, but not scheduled queries or Cloud Storage.

```bash
pip install -e '.[gcp]'
export GOOGLE_APPLICATION_CREDENTIALS=/path/outside/the/repo/key.json
gcp-aidp inventory --project <project> -o inv.json
```

✅ Pass if every object the checklist names is listed, and anything not readable is
recorded as *not scanned* rather than missing.

## 6. Live copy on AIDP

Section 8 of `MANUAL_STEPS.md`: install the connector JAR on the cluster, store the key
in the credential store, then run the `00`–`03` notebooks from `migrated/notebooks/`
in order.

✅ Pass if each stage reports what the table at the end of that section expects:
every copied table `MIGRATED_VERIFIED`, three views `VIEW_CREATED`, and
`type_blocked` and `v_blocked` `BLOCKED` (they are seeded with types and a function
that have no Delta or Spark equivalent).

## 7. Live publish and run

Needs `aidp-cli` (`pip install aidp-cli`, ideally in its own venv), `~/.oci/config`,
and in `.env`: `AIDP_INSTANCE_ID`, `AIDP_WORKSPACE_KEY`, `AIDP_CLUSTER_KEY`,
`AIDP_PREFIX` and, with an API key profile, `AIDP_AUTH=api_key`.

```bash
gcp-aidp publish migrated            # dry run
gcp-aidp publish migrated --apply
gcp-aidp publish migrated --apply    # again: everything skipped, nothing overwritten
gcp-aidp run migrated
```

✅ Pass if the dry run lists the notebooks and jobs with nothing refused, `--apply`
creates exactly those, the second `--apply` skips all of them, and `run` ends with
every task succeeded. After a first run, tables already copied show as
`PRESENT_NOT_REVERIFIED`: `skip-existing` left them alone and their counts matched.

## Reporting a problem

Include the command, the full output, `report.json` or `MIGRATION_REPORT.md` where
relevant, and the plugin version (`gcp-aidp --version`). Never include the service
account key, `.env`, or `~/.oci/config`.
