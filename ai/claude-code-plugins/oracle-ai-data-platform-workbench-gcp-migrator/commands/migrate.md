---
description: Translate an approved plan into AIDP artifacts (GoogleSQL → Spark SQL, BigQuery types → Delta) and the copy notebooks
argument-hint: "<plan.json> [-o migrated]"
allowed-tools: Bash(gcp-aidp migrate:*), Bash(python3 -m gcp_aidp.cli migrate:*), Read
---

Execute a migration plan:

`gcp-aidp migrate $ARGUMENTS`

Default the output to `-o migrated`. This writes files locally only and contacts
nothing; there is no `--demo` flag to add.

After it runs, Read `migrated/report.md` and surface: the ok / needs-review / blocked
counts, the before/after SQL of reviewed assets, and **every caveat, flag and block**.
A flagged construct is left as written and a blocked statement is not translated at
all; both need a human. Never rewrite one by hand into a silent rewrite.

If the plan used `--export-over-gb`, the large tables (`X01_EXPORT_ROUTE`) load from
files: suggest `/oracle-ai-data-platform-workbench-gcp-migrator:export` after `verify` and
before `publish`. Surface any `GS03_TRANSFER` flag naming `OCI_REGION` or
`OCI_COMPARTMENT_ID` still unset (rclone scripts for inventoried buckets).

Mention the copy notebooks in `migrated/notebooks/` (`00_diagnose` … `03_reconcile`)
and their prerequisites: the Spark BigQuery connector JAR as a cluster library and the
key in the AIDP credential store. Then suggest
`/oracle-ai-data-platform-workbench-gcp-migrator:verify`.
