# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## [0.2.1] - unreleased

### Added
- `P11_SPARK_PROPERTY`: a Dataproc job's or step's Spark properties. Runtime `spark.sql.*`
  keys are set at the start of the AIDP job (`spark.conf.set` in a Python task, `SET` before
  Spark SQL statements); executor and driver sizing is noted for the cluster; Dataproc, YARN
  and connector keys are dropped; anything else, static `spark.sql.*` keys included, is
  flagged. Tested offline (local Spark 3.5), not yet on AIDP.
- Schedules carried, PAUSED (`J03_SCHEDULE`). A scheduled query's schedule (UTC) and the
  Cloud Scheduler job that starts a Dataproc template, job or batch (unix cron, in its own
  time zone) become a Quartz `schedule` on the AIDP job with `pauseStatus: PAUSED`, so
  nothing runs until someone turns it on. A schedule that does not translate exactly
  (cron restricting both day fields, `every 5 hours`, `first sunday of quarter`), or two
  scheduler jobs on one definition, leaves the job unscheduled and is flagged (`J01`).
  `every N hours` is placed by the query's next run time (caveat). Rules in
  `references/schedule-translation.md`. Measured on AIDP: a PAUSED schedule is accepted,
  read back as sent, and starts no run. Live end to end for Cloud Scheduler: four
  scheduler jobs inventoried from a billed project, two published as PAUSED AIDP jobs with
  the planned Quartz and time zone, one flagged (`J01`), one reported.
- Inventory: Cloud Scheduler jobs that call the Dataproc API, with what each starts
  (`--scan-services`, Cloud Scheduler Viewer); a scheduled query's next run time and
  whether it is disabled. A schedule that starts nothing in the plan is a REPORT row.
- `publish` dry run shows each job's schedule.
- Export route for large tables: `plan --export-over-gb N --export-bucket B`, then the new
  verb `gcp-aidp export migrated [--apply]` (dry run by default; `/export` command). Tables
  of at least N GB are exported by BigQuery to Parquet in `gs://B/gcp-aidp-export/<plan id>/`
  and copied to OCI in Python (no `bq`, `gcloud` or rclone; OCI checks each file's MD5),
  then loaded into Delta by `02_copy_dataset` with the same checks and casts as the
  connector path; the count is checked against the files and against BigQuery
  (`X01_EXPORT_ROUTE`). The export runs as a separate export service account
  (`GCP_EXPORT_CREDENTIALS`, new); the migration's key stays read-only and only reads the
  files back. `bq extract` (free) where it can; where a column cannot go into Parquet as
  Spark reads it (`JSON`, `TIME`, `BIGNUMERIC`, `GEOGRAPHY`), an `EXPORT DATA` query
  (billed) exports it as text and the notebook casts it back; such a type inside a
  `STRUCT` or `ARRAY` keeps the table on the connector. A re-run skips exported tables and
  files already in OCI. Loaded, not converted: `CONVERT TO DELTA` fails on AIDP's `oci://`
  paths. New optional extra `[oci]`. Live: the same export jobs and a copy to OCI on a
  billed project (7 tables, 5,629 files; `export` found them all identical by MD5); the
  generated notebooks then loaded the real files on local Spark 3.5 with the real BigQuery
  connector, all verified. Not yet: `export --apply` with an export account end to end,
  and the load on an AIDP cluster. Runbook: `references/export-route.md`.
- rclone transfer scripts for inventoried buckets take their OCI side from `.env`:
  `OCI_REGION`, `OCI_COMPARTMENT_ID` (new keys) and `OCI_CLI_PROFILE`, checked before they
  are written into the script. An unset one stays a placeholder and is flagged
  (`GS03_TRANSFER`). Transfers run 64 files at a time.
- `plan --bucket-mapping <json>`: the OCI bucket each GCS bucket lands in
  (`{"buckets": {"<gcs>": {"oci_bucket": "<oci>"}}, "default_namespace": "<ns>"}`), for
  transfer jobs, the export route and every `gs://` → `oci://` rewrite. Unmapped buckets
  keep their names, as before.

### Changed
- README restructured as a short landing page for someone arriving through Claude Code:
  what it does, when to use it, install, prerequisites, skill index, engine, privacy.
  The setup, the step-by-step migration and the safety posture moved, unchanged, to
  `docs/RUNBOOK.md`; the live-versus-offline status table to `TESTING.md`; the rule tables
  are in `references/` only (`J02_DESTINATION_TABLE` added to
  `references/schedule-translation.md`, where it was missing). New `PRIVACY.md`.
- `J01_UNSCHEDULED` is now a flag, used only when a source schedule is not carried; a
  scheduled query whose job is blocked gets no schedule finding.

## [0.2.0] - unreleased

Dataproc and BigQuery Studio notebooks migrate. Built on 0.1, below.

### Added
- Dataproc inventory: job definitions grouped from the run history (region, type, main
  file or class or query, cluster; the newest run's arguments, a run count, the last run,
  and whether the arguments vary between runs), serverless batches grouped the same way,
  workflow templates with their steps and prerequisites, and cluster configuration
  (machine types, workers, autoscaling, Spark properties, init actions). The `.py` or
  `.sql` file of each PySpark and Spark SQL definition is read from Cloud Storage, at most
  1 MB; JARs are recorded, not read. Still planned as SKIP until the 0.2 translation lands.
- The demo fixture's Dataproc estate carries that shape and real job code.
- Dataproc plan rows: PySpark and Spark SQL definitions (jobs and batches) and workflow
  templates are MIGRATE (version 0.2) to unscheduled AIDP jobs named after cluster and file
  (`etl_nightly_sessionize_py`, `template_month_end_close`); Spark JAR jobs are REPORT until
  an AIDP `JAR_TASK` is proven; Hive, Pig, SparkR and the other engines are REPORT; clusters
  are REPORT with their sizing (workers, machine types, image, autoscaling, Spark properties,
  init actions).
- Dataproc translator (`translate/pyspark_to_aidp.py`): PySpark and Spark SQL code is
  edited only where it reaches outside Spark, by exact source spans found with `ast`, so
  formatting and comments are kept and a result that would not parse is blocked. Rules
  `P01`–`P08` (paths, BigQuery reads to `spark.table`, connector settings removed, Hive
  names as a caveat; query reads, BigQuery writes, Google libraries and SQL variables
  flagged) and `P97`–`P99` (blocks). Any connector use no recognised read or write
  accounts for is flagged, never reported clean. Workflow template steps are translated
  one by one; a step that cannot be blocks the template. `migrate` writes the translated
  script with its settings as a header comment. Rule table:
  `references/dataproc-translation.md`.
- Dataproc jobs on AIDP: a PySpark definition becomes an unscheduled job with one
  `PYTHON_TASK` running the translated script, which `publish` uploads as a workspace file
  (never overwriting, and refusing a job whose script it did not upload); a Spark SQL
  definition becomes a notebook task running its statements in order; a workflow template
  becomes one job, steps as tasks with their prerequisites as `dependsOn`. Measured live on
  AIDP: `commandLineArguments` is pasted as `sys.argv = <value>`, so it is written as a
  Python list starting with the script's own path; the script runs as `__main__` with
  `spark` defined. New flags: `P09_LOCAL_FILES` (`__file__`, and files, modules or JARs
  Dataproc staged beside the job) and `P10_TEMPLATE_PARAMETER`.
- BigQuery Studio notebooks: `inventory --notebooks-dir` reads exported `.ipynb` (cell sources
  only; outputs, which can hold query results, are dropped). A notebook with content is
  MIGRATE to an AIDP notebook: `%%bigquery [var]` cells become `spark.sql(...).toPandas()`
  through the GoogleSQL rules (a table id becomes `spark.table`), magic options with no AIDP
  meaning are flagged and legacy SQL blocks (`N01`, `N02`); `%load_ext` of the magic is
  removed and other `%` and `!` lines are commented out and flagged (`N03`); Python cells take
  the Dataproc rules. Translated notebooks upload with `publish`, without a job; one with a
  blocked cell is kept local for a person to finish. Rule table:
  `references/notebook-translation.md`. Run live: an exported notebook was read with
  `--notebooks-dir` on a real inventory (its saved output dropped), translated, published and
  run on AIDP; its `%%bigquery` query and table read returned the 15,709 orders and 100,000
  users reconcile had verified.
- Run live on AIDP: a translated PySpark job, a Spark SQL job and a two-step workflow
  template, published and run against the migrated tables; the PySpark job read 3,923 of
  the 15,709 `orders` rows reconcile had verified, and the template ran its steps in order.
- `publish` counts scripts as files, not notebooks; `run` points to `MIGRATION_REPORT.md`
  only after the migration job.

- Run live end to end on a billed project: a Dataproc cluster in one region and a workflow
  template in another were inventoried (two runs of one script grouped into one definition,
  a Spark SQL job, the template's steps and order, job code read from Cloud Storage),
  translated and run on AIDP. The Spark SQL job and the PySpark job's BigQuery read returned
  the 15,709 migrated orders; the PySpark job's write to a bucket not yet on OCI failed where
  the plan had flagged it (`P01`), and the template's second step did not run after it.

- BigQuery Studio notebooks are read through the Dataform API with `--scan-services`
  (Dataform Viewer): each is a Dataform repository labelled `single-file-asset-type: notebook`
  holding `content.ipynb`, found in any Dataform location; outputs are dropped, and other
  single-file assets are named in a warning. The `dataform` source no longer counts them as
  repositories. `--notebooks-dir` stays for exports. Run live on a billed project: a notebook
  with a SQL cell and a `%%bigquery` cell was read, translated, published and run on AIDP.
- `N04_SQL_CELL`: a BigQuery Studio SQL cell (generated Colab code running the query through
  `google.colab.sql`) becomes `spark.sql(...).toPandas()` with the query translated.

### Fixed
- A BigQuery Studio SQL cell was translated with no finding, so `verify` called it PASS,
  though AIDP has no `google.colab` to run it; found by reading a real notebook. It is now
  translated (`N04`), and any other `google.colab` import is flagged (`P07`).
- Dataproc: a definition kept the arguments of the run whose state changed last, not the one
  submitted last; a run killed early looked older. Runs are ordered by submission time.
- A single-node Dataproc cluster's sizing read "? workers ?"; it reads "single node: master …".
- `run` showed only "failed with error:" for a failed task; it now shows the exception that
  failed it, past AIDP's and Py4J's wrappers (`UnsupportedFileSystemException: No FileSystem
  for scheme "gs"`). Its "completed copies are skipped" hint now follows only the migration job.
- `inventory` accepted any text as a project id, including a URL, and made a real call
  with it. A value that is not a Google Cloud project id is refused before any call.
- A notebook cell whose only content was the removed `%load_ext` became an empty cell; it
  is now dropped.

### Known gaps
- Dataproc Spark JAR jobs, Hive, Pig, SparkR and the other engines are reported, not
  migrated: an AIDP `JAR_TASK` is not yet proven.
- A Dataproc job's Spark `properties` are recorded, not carried to the AIDP task.
- Cloud Scheduler is not inventoried, so a workflow template triggered from it arrives
  without its schedule (a template holds none itself).
- Files Dataproc stages beside a job (`pythonFileUris`, `fileUris`, `archiveUris`, extra
  JARs) are flagged (`P09`), not uploaded.
- A BigQuery connector read built across variables is flagged, not rewritten.

## [0.1.0] - unreleased

BigQuery and Cloud Storage: inventory, plan, translate, copy, verify, publish and run.

### Added
- Plugin skeleton: `.claude-plugin/plugin.json`, `pyproject.toml`, `gcp-aidp` CLI.
- `inventory --fixture demo`: the bundled Northwind Retail estate, built by
  `gcp_aidp/fixtures/build_demo_manifest.py`.
- `plan`: one row per asset with `MIGRATE` / `REPORT` / `SKIP`, the version
  each source is migrated in, and an approval document (`plan.md`). Halts on
  duplicate ids and on name collisions.
- `demo.sh`: the offline demo, inventory through verify.
- Type mapping (`TY01`–`TY17`, `TY99`) recorded on every planned column, with
  `--bignumeric string` and `--geography wkt` modes; table layout rules
  (`D01`–`D04`): liquid `CLUSTER BY` wherever BigQuery partitioning is not an
  exact Delta partition.
- GoogleSQL → Spark SQL translator: a lexer plus 27 named rules (rewrite,
  caveat, flag, block); a blocked statement is never partially translated.
  Spark-side behaviour checked on Spark 3.5.9.
- `gs://` → `oci://` rewriting through the bucket map, and an rclone transfer
  job per bucket.
- `migrate`: one artifact per asset plus `report.json` / `report.md`; offline.
- `verify`: PASS / REVIEW / SKIP / FAIL, failing closed on an interrupted run,
  a count mismatch, or a missing or escaping artifact.
- Tests for every rule ID, and an optional Spark 3.5 + Delta runtime test.
- Live inventory: BigQuery (datasets, tables, views, materialized views,
  external tables, routines, models, row access policies, column policy tags,
  dataset IAM, scheduled queries), Cloud Storage, and Dataproc, Composer,
  Dataform, Dataflow and Vertex AI for SKIP reporting. Read-only REST GETs with
  a `cloud-platform.read-only` token; `google-auth` is the only dependency.
  Gaps are recorded as *not scanned* and shown in the plan.
- `--scan-services`: those five APIs refuse a read-only token, so they are
  listed only on request, with a `cloud-platform` token used for them alone.
- Run against a seeded sandbox project: every object `seed.sql` creates is
  listed, and the zero-byte probes confirmed or corrected the BigQuery side of
  the translator rules (`G02_SAFE_CAST`'s reason was wrong and is fixed).
- `G97_LEGACY_SQL`: a legacy SQL view is blocked.
- `test-estate/`: `seed.sql`, `MANUAL_STEPS.md`, `teardown.sql`.
- `scripts/probe_bigquery_semantics.py`: zero-byte queries that confirm the
  BigQuery side of the translator rules.
- Data-plane notebooks generated by `migrate`: `00_diagnose`, `01_structure`,
  `02_copy_dataset` (one run per dataset), `03_reconcile`. Self-contained,
  with a PARAMS cell that job parameters override. They read the key from the
  AIDP credential store, read BigQuery tables only (`viewsEnabled=false`),
  cast with ANSI on so a value that does not fit fails instead of becoming
  NULL, and never drop a table. Tested by executing every cell on a local
  Spark 3.5 + Delta, with the connector replaced by a local reader.
- Data-plane notebooks run end to end on AIDP (Spark 3.5.0): 7 tables
  `MIGRATED_VERIFIED`, 3 views created.
- `publish`: uploads the notebooks to `/Workspace/<prefix>/` and creates the
  jobs, unscheduled. Dry run by default; `--apply` needs `--prefix`, checks
  the cluster belongs to the workspace and is not its Default Master, and
  never overwrites. `run` starts `<prefix>_gcp_aidp_migration` and polls its
  tasks. Through `aidp-cli`.
  Run live: 5 notebooks and 2 jobs created, every task succeeded.
- Jobs from `migrate` (`report["jobs"]`, `notebooks/jobs/`): a refresh job per
  materialized view, and one per scheduled query without a destination table
  (`J02` flags those with one). `reconcile` reports `SNAPSHOT_BUILT`.
- `AIDP_AUTH` sets `--auth`: `aidp-cli` defaults to `security_token`, so an
  API-key profile needs `AIDP_AUTH=api_key`.
- MCP server (`gcp_aidp/mcp_server.py`, `gcp-aidp-mcp`, `.mcp.json`) exposing
  `inventory`, `plan`, `migrate` and `verify` to any MCP client. Each tool runs
  the CLI. `publish` and `run` stay
  CLI-only because they change an AIDP workspace. Needs `pip install -e '.[mcp]'`
  (Python 3.10+, `mcp>=1.2,<2`).
- Claude Code skill `gcp-aidp-migrator` (with `references/verbs.md`) and slash
  commands `inventory`, `plan`, `migrate`, `verify`, `publish` and `run`.
- README: what it does, a status table separating live-tested from offline-only,
  translator coverage tables, safety posture, layout and roadmap. `TESTING.md`.
- `plan --datasets a,b`: migrate only those BigQuery datasets. The other
  datasets' assets stay in the plan as SKIP with the reason, and the plan's first
  lines name the scope ("Datasets: all 4", or "sales (1 of 4; the rest are SKIP)").
  A name the inventory does not hold fails the plan. Also on the MCP `plan` tool.
- README **Setup**: the Google service account, what an administrator sets up once
  in AIDP (catalog, cluster and connector JAR, credential, a least-privilege
  migration identity), and this machine (aidp-cli, `~/.oci/config`, finding the IDs,
  `.env`). The least-privilege grants are not yet tested with a restricted user.
- README **Run a migration**: the seven commands in order, with what to check before
  each next step; and a note that the CLI needs no AI, the plugin and MCP server being
  optional layers.
- `gcp-aidp-migrator-bootstrap` skill: a read-only readiness check, from the CLI to
  the cluster's state, that reports shapes and results, never values.

### Changed
- `G15_QUALIFY` is a rewrite, not a block: a subquery filtered on the
  condition, with the row carried as a struct so `SELECT *` works. Shapes it
  does not cover stay blocked with the reason.
- `CLUSTER BY` keys are written unquoted (AIDP's Delta kept the backticks in
  the column name), and a rerun of `01_structure` reports existing views as
  `already_existed`.
- The `.env` loader takes only `GCP_`, `GOOGLE_APPLICATION_CREDENTIALS`,
  `AIDP_` and `OCI_` keys, and reads the working directory's `.env`, then the
  plugin folder's.
- `references/dialect-translation.md` lists `G97_LEGACY_SQL`.
- Plan ids use the right singular: `dataform.repository.…` (was `repositorie`), and the
  new `dataproc.batch.…` and `dataproc.workflow_template.…`.
- Inventory has one module per service (`inventory/gcs.py`, `dataproc.py`, `composer.py`,
  `dataform.py`, `dataflow.py`, `vertex.py`) instead of one `services.py`; behaviour is
  unchanged.

### Fixed
- An empty `.env` line such as `OCI_CLI_PROFILE=` (as `.env.example` ships it) was exported
  as an empty value, and `aidp-cli` then looked for an OCI profile named `''` and crashed:
  `publish --apply` and `run` failed for anyone who had copied the example. An empty value
  now means unset.
- `G19_CAST_TYPE` rewrote words after a nested CAST's `AS`: in
  `CAST(COALESCE(CAST(x AS STRING), bytes) AS STRING)` the column `bytes` became
  `BINARY`, reported clean. A CAST now rewrites only the type after its own
  top-level `AS`, which also ends a false flag on `CAST(CAST(x AS INT64) AS STRING)`.
- `G01_REFERENCE` left DML and DDL targets as written: `INSERT INTO d.t`, `MERGE`,
  `UPDATE`, `DELETE`, `CREATE`/`TRUNCATE TABLE`. A scheduled query's job would have
  written to `d.t` in the session's default catalog, reported clean. A target in the
  plan is now rewritten to the AIDP catalog, and one that is not is flagged.
- `G24_LITERAL` flagged any string starting with the letter b (`'Bought'`) as a
  bytes literal.
- Inventory: one dataset, routine or model the account cannot read no longer fails
  the whole BigQuery scan. It is recorded as *not scanned* and the rest is listed.
- The data-plane reports name their catalog, and a stage ignores an earlier report
  for another catalog, so a reused reports folder no longer carries one migration's
  failures or statuses into the next.
- `plan` checked duplicate ids in quadratic time; 100,000 tables now plan in about
  a second.
- A Cloud Storage bucket name that is not a valid one fails the plan: the name is
  written into the rclone transfer script.
- `.gitignore` covers `*.pem` and the default output names at the plugin root.
- `demo.sh` wiped whatever folder `OUT` named. An `OUT` you set is now wiped only
  if it is empty or holds an earlier demo.
- `test-estate/MANUAL_STEPS.md` is written for anyone testing the plugin: it adds
  the publish-and-run path, and its expected results match the seed (six copyable
  tables, `v_blocked` blocked, the materialized view's verdict per path).

### Known gaps
- `EXTRACT(DAY FROM t.col)` is flagged as a relation not in the plan (safe, but
  noisy).
- Views are created in plan order, so a view on another view fails the first
  `01_structure` run and is created on the next.
- External tables infer their schema on AIDP; the BigQuery column list the
  inventory reads is not yet carried into the DDL.
- Scheduled queries are listed only in locations that hold a dataset.
