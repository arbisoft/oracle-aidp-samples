# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## [0.2.1] - unreleased

### Added
- What to do with a REVIEW asset. `references/review-guide.md` gives, per rule, what to
  check and when it is done. The `gcp-migration-reviewer` agent (`/review`) goes through a
  migration's REVIEW findings and sorts each into accept (with the evidence from the files),
  check (one command for the user, and what each answer means) or change (the edit), in
  `migrated/REVIEW.md`. It edits nothing else and runs nothing. `verify` points to both.
- An arithmetic SQL function becomes a Python UDF (`D06_SQL_FUNCTION`, rewrite): a notebook
  whose one line registers it, e.g. `spark.udf.register('net_price', lambda price, discount:
  ..., 'double')`, NULL in, NULL out as in SQL. Only `+ - * /`, number literals, its
  `INT64`/`FLOAT64` arguments and `IFNULL`/`COALESCE(argument, number)`; anything else stays
  flagged. Tested on local Spark 3.5.
- With `--geography wkt`, `ST_GEOGPOINT(<lon>, <lat>)` of constant coordinates becomes the WKT
  text `'POINT(lon lat)'` that `ST_ASTEXT` gives (`G26_GEOGPOINT_WKT`), matching how GEOGRAPHY
  columns are carried. From columns it stays blocked: the number formatting would differ.
- One AIDP job per schedule (`J04_ONE_JOB_PER_SCHEDULE`): an AIDP job holds one schedule, so
  a definition started by two Cloud Scheduler jobs, or a cron restricting both day of month
  and day of week (`0 9 1 * 1`, the 1st *or* a Monday), becomes jobs `_1`, `_2`, ... with the
  same tasks. Before, neither was carried (`J01`).
- SQL table functions and procedures are migrated, not reported (`D07_TABLE_FUNCTION`,
  `D08_PROCEDURE`, `G25_ROUTINE_PARAMETER`). A table function becomes a notebook defining a
  Python function of the same name that runs the translated query with its arguments as Spark
  named parameters (`spark.sql(sql, args=...)`) and returns a DataFrame. A procedure becomes a
  notebook with its arguments in `PARAMS` and its statements in order; a table it creates in a
  migrated dataset lands in the plan's catalog as Delta. Blocked, never guessed: procedural
  logic, `OUT` arguments, `ARRAY`/`STRUCT` arguments, and a parameter named like a column the
  body reads. Inventory records argument modes. Tested on local Spark 3.5: the generated
  notebooks run with their parameters bound.
- *Needs a decision* at the top of `plan.md`, and in `plan`'s terminal output: each type the
  plan blocks, the option that carries it, what that gives up, and every column it would
  carry. Before, a blocked column was one note in a long table, and the user learned of it
  in `migrate`'s report, after approving the plan.
- `plan --interval string` carries `INTERVAL` as BigQuery's interval text, and
  `plan --range struct` carries `RANGE<T>` as `STRUCT<start, end>` (`TY16`, `TY17`, caveats;
  blocked stays the default). The export route converts them in `EXPORT DATA`.
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
  (billed) exports it as text and the notebook casts it back, at any depth inside a
  `STRUCT` or `ARRAY` (the query rebuilds the column with field names, field order and
  array order kept; checked in BigQuery on literal data). A re-run skips exported tables and
  files already in OCI. Loaded, not converted: `CONVERT TO DELTA` fails on AIDP's `oci://`
  paths. New optional extra `[oci]`. Live: the same export jobs and a copy to OCI on a
  billed project (7 tables, 5,629 files; `export` found them all identical by MD5); the
  generated notebooks then loaded the real files on local Spark 3.5 with the real BigQuery
  connector, all verified. Then `export --apply` end to end with an export account (one
  table, 50 files, 770 MB, `EXPORT DATA`). Not yet: the load on an AIDP cluster. Runbook: `references/export-route.md`.
- `transfer`: Cloud Storage buckets → OCI Object Storage, replacing the generated rclone
  script (the one step without a dry run). `gcp-aidp transfer migrated` is a dry run (per
  bucket, the files and MB still to copy); `--apply` copies; `--bucket` picks buckets. It is
  the copy `export` uses: read-only on Google Cloud, every MD5 checked by OCI, files already
  there skipped, nothing deleted, a composite object compared by size, a report in
  `transfers/transfer_report.json`. `migrate` writes `transfers/<bucket>.transfer.json`
  (`GS03_TRANSFER`); rclone and `OCI_COMPARTMENT_ID` are no longer needed.
- `plan --bucket-mapping <json>`: the OCI bucket each GCS bucket lands in
  (`{"buckets": {"<gcs>": {"oci_bucket": "<oci>"}}, "default_namespace": "<ns>"}`), for
  `transfer`, the export route and every `gs://` → `oci://` rewrite. Unmapped buckets
  keep their names, as before.

### Changed
- `01_structure` creates an external table when its files are on OCI, with no
  `external-tables` parameter to remember: until `transfer --apply` has copied them, the
  table is reported as not created yet (`files_not_on_oci`, not a failure), and the next
  run creates it.
- A data-plane notebook opened and run by hand in the workspace now keeps its reports
  where the job's run does (`/Workspace/<prefix>/reports`): `publish` sets that as its
  `reports-dir`. Before, it wrote to a shared default folder, which reconcile did not read.
- A saved query is also published: `migrate` writes it as a notebook (its translated SQL,
  the result shown with `.toPandas()`) and `publish` uploads it to the workspace, with no
  job, so it opens and runs there as it did in BigQuery. It used to stay a local `.sql` file.
- README restructured as a short landing page for someone arriving through Claude Code:
  what it does, when to use it, install, prerequisites, skill index, engine, privacy.
  The setup, the step-by-step migration and the safety posture moved, unchanged, to
  `docs/RUNBOOK.md`; the live-versus-offline status table to `TESTING.md`; the rule tables
  are in `references/` only (`J02_DESTINATION_TABLE` added to
  `references/schedule-translation.md`, where it was missing). New `PRIVACY.md`.
- `J01_UNSCHEDULED` is now a flag, used only when a source schedule is not carried; a
  scheduled query whose job is blocked gets no schedule finding.
- `00_diagnose` checks that each export-route table's Parquet files can be read on OCI, so
  a missing export stops the run before anything is written; `run` warns when
  `exports/export_report.json` does not show every routed table on OCI.
- The MCP `plan` tool takes the export route's options (`export_over_gb`,
  `export_bucket`) and `bucket_mapping`, as the CLI does.
- `scripts/generate_spark_builtins.py` now ships with this plugin: the Spark vocabulary
  behind `G90_NOT_SPARK_BUILTIN` is regenerated here (unchanged for Spark 3.5.9).

### Fixed
- A `%%bigquery` cell holding only `dataset.table` became `spark.sql("dataset.table")`, which
  fails, graded rewrite. The magic reads it as a table in the notebook's project: it is now
  `spark.table` on the migrated table, and any other one-word body is blocked (`N01`). Found
  by the reviewer agent on the live test project.
- A table with an `INTERVAL`, a `RANGE` or a `BIGNUMERIC` wider than 38 digits could not be
  copied: Spark BigQuery connector 0.45 refuses the whole table while reading its schema
  (seen on AIDP), so `--bignumeric string` failed at run time unless the table happened to
  be large enough for the export route. Such a table now always takes the export route,
  where BigQuery converts the values; without `--export-bucket` it is blocked
  (`X02_NEEDS_EXPORT`) and listed under *Needs a decision*. Its row count is the one the
  `EXPORT DATA` job reports, written beside the files on OCI and checked by `00_diagnose`
  and the copy instead of the connector.
- External tables lost BigQuery's column list and CSV options: Spark inferred the schema
  from the files, so a CSV became `_c0, _c1, …` strings and a header row became a data row.
  The DDL now carries the columns (each by its type rule) and the CSV options (`header`,
  `sep`, `quote`, `encoding`, `nullValue`, `multiLine`); what does not carry is flagged.
  Inventory records `csvOptions` and `autodetect`, so run it again. Checked on local Spark 3.5.
- `00_diagnose` now stops the run, before anything is created, on what would otherwise fail
  later: a source whose columns differ from the plan (the copy stopped it as `type_drift`)
  and a target table already there with another layout (`01_structure` stopped it). Each
  problem names the columns. Before, diagnose printed the connector's columns without
  comparing them (seen on AIDP).
- An ingestion-time partitioned table failed its copy as `type_drift`: the connector adds
  BigQuery's `_PARTITIONTIME` and `_PARTITIONDATE` pseudo-columns, which the plan did not have
  (seen on AIDP). They are now carried as columns of the same names (`D03_INGESTION_TIME`):
  the load time is kept, the target is clustered on `_PARTITIONTIME`, the export route selects
  them with `EXPORT DATA`, and queries filtering on them translate as written (`G16`, now a
  flag; `_TABLE_SUFFIX` stays blocked). A `type_drift` also names the columns that differ,
  where it printed `source=None target=None`.
- A `TIME` inside a `STRUCT` or `ARRAY` copied through the connector landed as its
  microsecond count (`"37800123456"`), and the table still passed as verified: the
  conversion to `HH:MM:SS.ffffff` text applied to top-level columns only. `02_copy_dataset`
  now converts it at any depth, keeping a `NULL` struct `NULL`.
- `export`: after a failed BigQuery job, or one still running at the time limit, the
  message said to run `export` again, which would have copied a partial folder (the
  row-count check would have caught it). It now says to delete the folder, or to wait
  for the named job to finish.
- Saved queries were never read from Google: the inventory said they have no read API and
  asked for `.sql` files. BigQuery Studio stores each one in Dataform, like a notebook, so
  `inventory --scan-services` now reads them there (Dataform Viewer); `--saved-queries-dir`
  stays for anything it cannot read.
- BigQuery pipelines were reported as having no read API, and a pipeline was counted as a
  Dataform repository. They are Dataform repositories labelled `bigquery-workflow`:
  `--scan-services` now lists them under BigQuery, and the plan reports them for 0.3.
- One Dataform call timing out while reading notebooks discarded the whole BigQuery
  inventory. It now marks only notebooks, saved queries and pipelines as not scanned, and
  every Google API call is tried once more after a timeout or a dropped connection.
- The export account's documented roles missed **Storage Object Viewer** on the staging
  bucket: `EXPORT DATA` lists the folder before writing, so with Storage Object Creator
  alone the export failed with `storage.objects.list` denied.
- `export --apply` could not copy any file to OCI: each GCS download was handed to
  `put_object` as a raw stream, which the OCI SDK takes for a file and fails on
  (`'HTTPResponse' object has no attribute 'mode'`). It now passes a read-only body
  with the file's length.
- `export --apply` shows the copy's progress every 10 seconds (files, MB, MB/s, time left),
  and waits up to 600 s on an upload: on a slow uplink with many files in flight, the
  OCI SDK's 60 s default dropped one.

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
