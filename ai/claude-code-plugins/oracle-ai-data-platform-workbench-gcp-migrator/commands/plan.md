---
description: Turn a Google Cloud inventory manifest into an AIDP mapping plan and approval document
argument-hint: "<inv.json> [-o plan.json] [--catalog <aidp-catalog>] [--namespace <oci-namespace>] [--datasets a,b] [--bignumeric block|string] [--geography block|wkt] [--interval block|string] [--range block|struct] [--export-over-gb GB --export-bucket BUCKET] [--bucket-mapping JSON]"
allowed-tools: Bash(gcp-aidp plan:*), Bash(python3 -m gcp_aidp.cli plan:*), Read
---

Build a migration plan from an inventory manifest:

`gcp-aidp plan $ARGUMENTS`

Default the output to `-o plan.json`. Ask for the target catalog if the user has not
named one; the default is the project id. Ask which datasets to migrate: without
`--datasets` every dataset in the project is copied, and the others then stay in the
plan as SKIP.

After it runs, Read `plan.md` (the approval document) and report: the MIGRATE /
REPORT / SKIP counts, the not-scanned list at the top, and the *Needs a decision*
section first: each type the plan blocks (`BIGNUMERIC`, `GEOGRAPHY`, `INTERVAL`,
`RANGE`), the option that carries it (`--bignumeric string`, `--geography wkt`,
`--interval string`, `--range struct`), what it gives up, and the columns. Ask the user
to choose for each line; re-plan only with the options they pick.

For large tables, `--export-over-gb N --export-bucket B` sends tables of at least N GB
through Parquet files instead of the connector. Only suggest it; it needs an export
service account, a GCS staging bucket and an OCI bucket that AIDP can read
(`references/export-route.md`). If no table reaches N, nothing changes.

A name collision halts the plan. Tell the user which assets collide and that one must
be renamed; never pick a winner. Ask the user to approve the plan before
`/oracle-ai-data-platform-workbench-gcp-migrator:migrate`.
