---
description: Turn a Google Cloud inventory manifest into an AIDP mapping plan and approval document
argument-hint: "<inv.json> [-o plan.json] [--catalog <aidp-catalog>] [--namespace <oci-namespace>] [--datasets a,b] [--dataform-repos a,b] [--dags a,b] [--bignumeric block|string] [--geography block|wkt]"
allowed-tools: Bash(gcp-aidp plan:*), Bash(python3 -m gcp_aidp.cli plan:*), Read
---

Build a migration plan from an inventory manifest:

`gcp-aidp plan $ARGUMENTS`

Default the output to `-o plan.json`. Ask for the target catalog if the user has not
named one; the default is the project id. Ask which datasets to migrate: without
`--datasets` every dataset in the project is copied, and the others then stay in the
plan as SKIP. Dataform repositories work the same way with `--dataform-repos`: each
one becomes one unscheduled job, and the others stay in the plan as SKIP. Composer DAGs
work the same way with `--dags` (DAG file names, any environment). The inventory and the
plan contain DAG source code: tell the user not to commit them.

After it runs, Read `plan.md` (the approval document) and report: the MIGRATE /
REPORT / SKIP counts, the not-scanned list at the top, and every table blocked by a
type (`BIGNUMERIC`, `GEOGRAPHY`, `INTERVAL`, `RANGE`). For `BIGNUMERIC` and
`GEOGRAPHY`, say that `--bignumeric string` or `--geography wkt` carries the column as
text, and what that gives up. Do not re-plan with those flags unless the user asks.

A name collision halts the plan. Tell the user which assets collide and that one must
be renamed; never pick a winner. Ask the user to approve the plan before
`/oracle-ai-data-platform-workbench-gcp-migrator:migrate`.
