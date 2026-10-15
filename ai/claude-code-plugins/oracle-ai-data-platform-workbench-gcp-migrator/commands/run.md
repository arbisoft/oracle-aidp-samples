---
description: Run the published Google Cloud migration job on AIDP and follow it to the end
argument-hint: "<migrated/> [--prefix <name>] [--job <job name>] [--wait <seconds>]"
allowed-tools: Bash(gcp-aidp run:*), Bash(python3 -m gcp_aidp.cli run:*)
---

Start a job that `publish --apply` created and follow it:

`gcp-aidp run $ARGUMENTS`

This copies data: confirm with the user before starting it. If `report.json` has an
`export` section, check that `gcp-aidp export --apply` finished with every table on OCI
(`migrated/exports/export_report.json`); `run` prints a warning when it has not, and
`00_diagnose` then stops the run before anything is written. By default it runs
`<prefix>_gcp_aidp_migration` (diagnose → structure → one copy per dataset →
materialized view snapshots → reconcile) and polls each task until it ends.
Stopping the command stops the polling, not the run on AIDP.

The copy reads BigQuery tables only, never drops a table, and by default
(`skip-existing`) leaves a table that already has rows untouched. A rerun after a
failure therefore skips the copies that finished.

When it ends, report each task's final state. On success, point the user to
`/Workspace/<prefix>/reports/MIGRATION_REPORT.md` and explain the verdicts: a
`PRESENT_NOT_REVERIFIED` table already had rows and matched the source count, which is
not a failure. On failure, show the failing task's error and do not retry with
different flags to make it green.

`--job` runs another of the migration's jobs: a Dataproc job (its name is in the
`publish` dry run, e.g. `etl_nightly_sessionize_py`), a template's job, or a
materialized view's refresh job, a Dataform repository's job, or `check_notebooks`.
`MIGRATION_REPORT.md` exists only after the migration job.

`--job check_notebooks` runs every migrated notebook that BigQuery runs on demand (saved
queries, BigQuery Studio notebooks, SQL and table functions) once, each as its own task,
and lists the ones that fail with their error. Procedures are left out: they change data,
so the user runs them by hand. Suggest it after the migration job, to prove the notebooks
work on the migrated data.
