---
name: gcp-aidp-migrator
description: "Migrate a Google Cloud data stack (BigQuery datasets, tables, views, materialized views, scheduled and saved queries, Cloud Storage) to Oracle AI Data Platform (AIDP). Use when the user wants to inventory a GCP or BigQuery estate, plan a migration to AIDP, translate GoogleSQL to Spark SQL or BigQuery types to Delta, copy BigQuery tables into AIDP, verify a migration's output, or publish and run it in an AIDP workspace. Wraps the `gcp-aidp` CLI, with the verbs inventory, plan, migrate, verify, publish and run."
---

# Oracle AIDP migrator for Google Cloud

Drives the `gcp-aidp` CLI, which migrates a Google Cloud data estate centred on
BigQuery to Oracle AIDP. The translators are **deterministic**: every rewrite is a
named rule with a reason, and anything that cannot be safely rewritten is **flagged
and left in place**, or **blocks** the statement, never silently changed. Preserve
that principle: never hand-edit a flagged or blocked construct into a silent rewrite.

## Prerequisites

Install the CLI from the plugin root, the directory holding `pyproject.toml`
(`${CLAUDE_PLUGIN_ROOT}` when installed as a plugin):

```bash
pip install -e "${CLAUDE_PLUGIN_ROOT:-.}"            # offline verbs: stdlib only
pip install -e "${CLAUDE_PLUGIN_ROOT:-.}[gcp]"       # live inventory: adds google-auth
gcp-aidp --version
```

`gcp-aidp-migrator` is not a published package; do not suggest a PyPI install.
Every verb also runs as `python3 -m gcp_aidp.cli <verb>`.

- **Live inventory** needs a read-only service account key in
  `GOOGLE_APPLICATION_CREDENTIALS`, kept outside the repository. Without one, use
  `--fixture demo`.
- **`publish` and `run`** need `aidp-cli` on PATH (`pip install aidp-cli`, ideally in
  its own venv), `~/.oci/config`, and `AIDP_INSTANCE_ID`, `AIDP_WORKSPACE_KEY`,
  `AIDP_CLUSTER_KEY` and `AIDP_PREFIX`. With an API key profile also
  `AIDP_AUTH=api_key`: `aidp-cli` defaults to `security_token`. The CLI reads `.env`
  from the working directory, then from the plugin folder.
- **The copy notebooks** need the Spark BigQuery connector JAR as a cluster library and
  the key in the AIDP credential store. See the README's *Copying the data*.

Never read, print or copy the service account key, and never echo a credential back.

Before the first live inventory or publish, or after an auth, config or connection
error, run the `gcp-aidp-migrator-bootstrap` skill: it checks each of these and says
what to fix. The full setup is the README's **Setup** section.

## Workflow

```bash
gcp-aidp inventory --project <id> -o inv.json            # or --fixture demo
gcp-aidp plan      inv.json -o plan.json --catalog <aidp-catalog> --namespace <oci-ns>
gcp-aidp migrate   plan.json -o migrated                 # local files only
gcp-aidp verify    migrated
gcp-aidp publish   migrated                               # dry run; then --apply
gcp-aidp run       migrated                               # runs the published migration job
```

`migrate` has no `--demo` flag: it always writes locally and contacts nothing. Only
`publish --apply` and `run` reach AIDP.

## How to help the user

1. **Confirm scope**: the project, the datasets that matter, and whether they have a
   key or want the fixture. `--sources bigquery,gcs` narrows the scan; `plan
   --datasets a,b` narrows what is migrated. Without it every dataset is copied, so
   ask before planning a large project.
2. **Run the verbs in order**; each reads the previous one's output.
3. **Stop at the plan.** Show `plan.md`, the approval document: the actions, the
   *not scanned* list at the top, and the type decisions. A name collision halts the
   plan; tell the user to rename one side, never pick a winner.
4. **Read the reports.** After `migrate`, open `migrated/report.md` and surface every
   flag, caveat and block; they need a human. After `verify`, give the PASS / REVIEW /
   SKIP / FAIL counts.
5. **Publish only after a dry run the user has read.** See the `/publish` command.
6. **After `run`**, point the user to `MIGRATION_REPORT.md` in `/Workspace/<prefix>/reports/`
   and explain each verdict.

## What it does

| BigQuery | → | AIDP |
|---|---|---|
| Project | → | INTERNAL catalog |
| Dataset | → | Schema |
| Table | → | Managed Delta table, copied by `02_copy_dataset` |
| Partitioning / clustering | → | `PARTITIONED BY` when exact, otherwise liquid `CLUSTER BY` |
| View | → | Spark SQL view |
| Materialized view | → | a table built on AIDP, plus an unscheduled refresh job |
| Scheduled query (no destination) | → | an unscheduled AIDP job |
| External table | → | `USING PARQUET/AVRO/ORC/CSV/JSON` over `oci://` |
| Cloud Storage bucket | → | an rclone transfer job to Object Storage |
| SQL UDF | → | flagged: Spark 3.5 cannot parse `CREATE FUNCTION ... RETURN` |

Reported, not translated: procedures, JavaScript and table functions, BigQuery ML
models, row access policies and policy tags. Skipped in 0.1: BigQuery Studio notebooks
and Dataproc (0.2), Composer and Dataform (0.3), Dataflow and Vertex AI (later).

## Reporting results

State what PASS means: translated, no known issue detected, **not execution-verified**.
`verify` does not parse or run the artifacts, so a construct no rule covers is reported
clean. Do not call a PASS asset "ready".

Report `blocked` alongside ok and needs-review: a blocked asset was refused, not
partly translated, so it is work the user still owes. A table blocked for `BIGNUMERIC`
or `GEOGRAPHY` can be re-planned with `--bignumeric string` or `--geography wkt`;
say what that gives up.

`03_reconcile` verdicts:

- `MIGRATED_VERIFIED`: copied this run, counts (and sums, if asked) match.
- `PRESENT_NOT_REVERIFIED`: already had rows, so `skip-existing` left it alone; the
  count still matched the source. Not a failure.
- `VIEW_CREATED` / `EXTERNAL_CREATED`: created on AIDP.
- `SNAPSHOT_BUILT`: a materialized view built by its refresh job.
- `STRUCTURE_ONLY`, `*_NOT_CREATED_YET`: a stage has not run yet.
- `BLOCKED`, `NEEDS_REVIEW`: not created; the detail names the rule and any re-plan
  option.
- Problems, which fail the stage: `STRUCTURE_FAILED`, `STRUCTURE_TYPE_DRIFT`,
  `COPY_FAILED`, `COUNT_DRIFT`, `VIEW_FAILED`, `EXTERNAL_FAILED`,
  `MISSING_DESPITE_REPORT`, `VIEW_MISSING_DESPITE_REPORT`. Show the detail; a rerun
  skips the copies that finished.

Each table is copied at its own moment: a source that keeps changing is consistent per
table, not across tables. Say so before a cutover.

See `references/verbs.md` for every flag, and the plugin README for the rule tables.
