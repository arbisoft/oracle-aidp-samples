---
description: Run the published Google Cloud migration job on AIDP and follow it to the end
argument-hint: "<migrated/> [--prefix <name>] [--job gcp_aidp_migration] [--wait <seconds>]"
allowed-tools: Bash(gcp-aidp run:*), Bash(python3 -m gcp_aidp.cli run:*)
---

Start a job that `publish --apply` created and follow it:

`gcp-aidp run $ARGUMENTS`

This copies data: confirm with the user before starting it. By default it runs
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
