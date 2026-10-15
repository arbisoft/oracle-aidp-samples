# Testing guide

The core pass needs no Google Cloud or OCI account: everything runs offline against
the bundled Northwind Retail fixture. The live passes at the end need a BigQuery
project and an AIDP workspace.


## What has run live (0.3)

Live means run against a real system: a BigQuery sandbox project (no billing) and an
AIDP workspace on Spark 3.5.0. Offline means unit and fixture tests only.

| Area | State | Tested |
|---|---|---|
| `inventory`: BigQuery datasets, tables, views, materialized views, routines, models, row access policies, policy tags, IAM | ✅ built | live |
| `inventory`: external tables, scheduled queries, Cloud Storage | ✅ built | offline only (the sandbox has no billing) |
| `inventory`: Dataproc job definitions, serverless batches, workflow templates, cluster config, job code (`--scan-services`) | ✅ built | live on a billed project, two regions: two runs of one script grouped into one definition (newest arguments kept), a Spark SQL job, a workflow template's steps and order, job code read from Cloud Storage, cluster sizing |
| `inventory` / `plan` / `migrate` / `publish`: BigQuery Studio notebooks, read through the Dataform API (`--scan-services`) or from `--notebooks-dir` | ✅ `%%bigquery` cells to `spark.sql` (`N01`–`N03`), Python cells through the Dataproc rules; translated notebooks uploaded, a notebook with a blocked cell kept local | live: a BigQuery Studio notebook with a SQL cell and a `%%bigquery` cell read through Dataform on a billed project (outputs dropped), translated, published and run on AIDP; an exported notebook through `--notebooks-dir` too |
| `plan` / `migrate` / `publish`: Dataproc | ✅ PySpark and Spark SQL definitions, batches and workflow templates translated (`P01`–`P11`; `P11`, job Spark properties, offline only) and published as unscheduled AIDP jobs: PySpark as a Python task, Spark SQL as a notebook, templates with their step order; JAR, Hive and other engines and clusters reported | live on AIDP: synthetic and then real Dataproc jobs, inventoried from a live project, translated and run on the migrated tables with the expected counts; a write to a bucket not yet on OCI failed where the plan flagged it |
| Dataform (0.3): `inventory` (`--scan-services`), `plan`, `migrate`, `publish`, `run` | ✅ tables, views and assertions as one job, `dependsOn` from Dataform's graph (`DF01`–`DF15`) | live on a billed project: a repository's release-config compilation read (7 actions and their dependencies), translated, published and run on AIDP: the table built, its view and 3 assertions passed. The first run showed two tasks creating one schema at once fail; schemas now come from one first task, and a run from an empty schema passed every task |
| `plan` | ✅ built | live, on the sandbox inventory |
| GoogleSQL → Spark SQL translator | ✅ 29 named rules | Spark side on local Spark 3.5.9; BigQuery side by zero-byte probes; 4 translated views created on AIDP, among them `QUALIFY`, `SAFE_DIVIDE`, `TIMESTAMP_TRUNC` and a constant `ST_GEOGPOINT` as WKT (its text checked against BigQuery's `ST_ASTEXT`) |
| Type mapping and table layout | ✅ built | live: 7 tables created and read back against the plan; a DAY-partitioned table carried as `PARTITIONED BY (d)`, read back from `DESCRIBE DETAIL` |
| Data copy notebooks (`00`–`03`) | ✅ built | live: 7 tables, 293,886 rows, counts matching |
| Materialized view snapshot | ✅ built | live, inside the migration job; the refresh job on its own offline only |
| Scheduled-query jobs | ✅ built | live: a billed project's scheduled query published as a PAUSED job; not run |
| SQL routines: procedures, SQL functions, table functions (`D06`–`D08`, `G25`) | ✅ built | live on AIDP: a procedure's `CREATE OR REPLACE TABLE ... USING DELTA` ran (its totals match the source table); an arithmetic SQL function registered as a Python UDF returned BigQuery's value. Table functions on local Spark only |
| Schedules: scheduled queries and Cloud Scheduler jobs that start Dataproc, translated to Quartz and created PAUSED (`J01`, `J03`) | ✅ built | live for Cloud Scheduler: four scheduler jobs on a billed project inventoried, two carried onto published AIDP jobs and read back PAUSED as planned, one flagged, one reported; no run started. Scheduled queries offline only. Not yet checked that a schedule fires on time once turned on |
| External tables, `transfer` (buckets → OCI) | ✅ built | live: `transfer --apply` copied the files, and `01_structure` created both external tables (Parquet and CSV) once they were on OCI; their row counts match BigQuery's (124,978 and 29,120) |
| Export route for large tables (`plan --export-over-gb`, `export`, the copy notebook loading Parquet) | ✅ built | live on a billed project: 7 tables (20 M rows, `JSON`, `TIME`, `BIGNUMERIC`, arrays, structs) exported by the same BigQuery jobs and copied to OCI, 5,629 files, which `export` then matched by MD5; the generated notebooks loaded them on local Spark 3.5 with the real BigQuery connector, all verified. Then end to end on AIDP: `export --apply` with the export account, 50 files (770 MB, MD5-checked) for a 20 M-row table, loaded and counted, and a table the connector cannot read (`INTERVAL`, `RANGE`, `BIGNUMERIC`, `GEOGRAPHY`) loaded and counted against the export's row count |
| `verify` | ✅ built | offline (it reads the report only) |
| `publish`, `run` | ✅ built | live: 26 notebooks and scripts and 11 jobs created, schedules PAUSED; an upload AIDP timed out on was reused by `--reuse-existing-notebooks`, which then created the job it had held back; every task of the migration job succeeded; a job created seconds earlier needed `run`'s retry; a table a scheduled query kept appending to stopped the copy with the fix named |
| Notebook checks (`check_notebooks`, `check_notebooks_writes`) | ✅ built | live on AIDP: the read-only check ran 6 migrated notebooks (a saved query, 3 BigQuery Studio notebooks, a table function, a SQL function) as one job; 5 passed and the one with an untranslated `DATE_TRUNC` (flagged `G22`) failed with its cause named. With `G27` added, all 6 passed. The procedure went to the writes job and was not run |
| Materialized-view refresh job, scheduled-query job | ✅ built | live: both run on AIDP on their own; the view's 2,781 rows match BigQuery's, and the scheduled query appended exactly one row |
| Dataproc jobs on AIDP | ✅ built | live: a PySpark job (connector read → `spark.table`, its argument passed), a Spark SQL job, a two-step workflow template (steps in order) and a serverless batch (inventoried in its own region with `--regions`) all ran on AIDP |
| MCP server | ✅ built | offline (needs `mcp`) |
| Live cell-by-cell execution and repair | 🚧 not built | |

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
and caveats on Spark, including every task statement of the Dataform job, and checks the `QUALIFY` rewrite keeps the same rows.
`test_dataplane_runtime` executes every cell of the generated `00`–`03` notebooks,
with the BigQuery connector replaced by a local reader typed the way the connector
delivered on AIDP; tests 5 to 7 load export-route tables from Parquet files written the
way BigQuery's export wrote them, test 7 with text fields inside structs and arrays; test 8
checks a `TIME` inside structs and arrays from the connector; test 9 checks that
`00_diagnose` stops when an export-route table's files are missing.
`test_export` (no extra dependencies) runs the `export` verb against fake BigQuery, Cloud
Storage and OCI endpoints.

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

## 8. Live Dataproc jobs and BigQuery Studio notebooks (0.2)

The translated code runs on AIDP; the Dataproc side needs no GCP Dataproc for this. Add
to an inventory, as the scan would write them, a PySpark job, a Spark SQL job and a
two-step workflow template whose code reads the migrated tables (see
`gcp_aidp/fixtures/build_demo_manifest.py` for the shape), and export a BigQuery Studio
notebook with a `%%bigquery` cell and a `%%bigquery` table read into a folder. Then:

```bash
gcp-aidp inventory --project <project> --notebooks-dir <folder> -o inv.json   # or add the jobs by hand
gcp-aidp plan inv.json --catalog <catalog> --datasets <dataset> -o plan.json
gcp-aidp migrate plan.json -o migrated
gcp-aidp publish migrated --prefix <test-prefix> --apply
gcp-aidp run migrated --prefix <test-prefix> --job <dataproc job name>        # each Dataproc job
```

A notebook gets no job; run it once from the workbench, or through a one-off notebook task.

✅ Pass if each job and the notebook succeed and print the counts reconcile verified for
the tables they read, and a template's steps run in their order. Make the code assert
those counts, so a wrong translation fails the task instead of passing quietly.

## 9. Live Dataproc inventory (billed project)

Needs billing, the Dataproc API, Dataproc Viewer on the service account, and a small
cluster with a PySpark job, a Spark SQL job and a workflow template submitted (see
`test-estate/MANUAL_STEPS.md`, step 5b). Delete the cluster the same day.

```bash
gcp-aidp inventory --project <project> --sources dataproc --scan-services -o dp.json
```

✅ Pass if the runs are grouped into one definition per job with their run count, the
`.py` and `.sql` code is read, and the template's steps and prerequisites are listed.

## Reporting a problem

Include the command, the full output, `report.json` or `MIGRATION_REPORT.md` where
relevant, and the plugin version (`gcp-aidp --version`). Never include the service
account key, `.env`, or `~/.oci/config`.
