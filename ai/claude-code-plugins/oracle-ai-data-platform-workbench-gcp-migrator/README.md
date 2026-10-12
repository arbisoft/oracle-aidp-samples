# Oracle AI Data Platform — Migrator for Google Cloud (Claude Code plugin)

> **Migrate a Google Cloud data estate centred on BigQuery onto Oracle AIDP, step by step,
> with a person approving each step.**
> Drives the `gcp-aidp` CLI from Claude Code: inventory the project, plan the mapping,
> translate it with named deterministic rules, copy the data, publish and run it on AIDP.

> **Canonical home:** [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator).
> End users install via Anthropic's community marketplace (see [Install](#install)).

The migrator is a standalone command-line tool (`gcp-aidp`) that needs no AI: every
result comes from named rules, and anything it cannot translate safely is flagged or
blocked with the reason, never guessed. The plugin's skills, commands and MCP server are
optional layers that guide the workflow and help with setup and review.

> **Status:** **v0.2.0**, with 0.2.1 in progress (see [`CHANGELOG.md`](./CHANGELOG.md)).
> What has run against real systems and what is tested offline only, per area:
> [`TESTING.md`](./TESTING.md#what-has-run-live-02). Next: Composer, Dataform and
> BigQuery pipelines (0.3); Dataflow and Vertex AI later.

---

## What it does

| Google Cloud | → | AIDP |
|---|---|---|
| Project | → | INTERNAL catalog |
| Dataset | → | Schema |
| Table | → | Managed Delta table, rows copied by a generated notebook; a large table through Parquet files on OCI (`export`) |
| Partitioning, clustering | → | `PARTITIONED BY` where exact, otherwise liquid `CLUSTER BY` |
| View | → | Spark SQL view |
| Materialized view | → | a table built on AIDP from the migrated tables, plus an unscheduled refresh job |
| Scheduled query without a destination table | → | an AIDP job with its schedule translated to Quartz and PAUSED, or unscheduled and flagged when it does not translate exactly |
| Saved query | → | a Spark SQL file |
| External table | → | `USING PARQUET/AVRO/ORC/CSV/JSON` over `oci://` |
| Cloud Storage bucket | → | an rclone transfer job to OCI Object Storage |
| Procedures, JavaScript and table functions, BigQuery ML, access policies | → | reported with an effort band, not translated |
| BigQuery Studio notebook | → | an AIDP notebook: `%%bigquery` cells run as `spark.sql`, Python cells take the path and connector rules |
| Dataproc PySpark and Spark SQL jobs, batches, workflow templates | → | AIDP jobs: a Python task, a SQL notebook, or one job per template; a Cloud Scheduler schedule that starts one is carried, PAUSED |
| Composer, Dataform, Dataflow, Vertex AI | → | inventoried, planned for a later version |

### The flow

```
Google Cloud project                                         Oracle AIDP workspace
────────────────────                                         ─────────────────────
inventory   read-only metadata scan ──→ inv.json
plan        your approval document  ──→ plan.json + plan.md   (MIGRATE / REPORT / SKIP per asset)
migrate     local, contacts nothing ──→ migrated/             (DDL, Spark SQL, jobs, copy notebooks, report)
verify      PASS / REVIEW / SKIP / FAIL per asset
export      large tables only: BigQuery → Parquet in GCS → OCI     (dry run until --apply)
publish     notebooks + jobs ─────────────────────────────────→  /Workspace/<prefix>/  (dry run until --apply)
run         the migration job ────────────────────────────────→  structure → copy → reconcile
                                                                  MIGRATION_REPORT.md
```

### What sets it apart

- **Deterministic.** Every rewrite is a named rule with a test and a line in the report;
  no LLM in the translation path.
- **Read-only against Google Cloud** except `export --apply`, which writes only to your
  staging bucket through a separate export service account.
- **A person decides at each step.** The plan is an approval document; `export` and
  `publish` are dry runs until `--apply`; jobs are created unscheduled or PAUSED.
- **Verified copies.** Each table's row count is checked against BigQuery, and
  `03_reconcile` writes one verdict per object.

---

## When to use this plugin

You are moving a BigQuery-centred estate onto AIDP and want Claude Code to drive it.
Typical asks:

- *"Inventory my BigQuery project `<project>`"*
- *"Plan a migration of the `sales` and `finance` datasets into catalog `<catalog>`"*
- *"Translate this GoogleSQL view to Spark SQL"*
- *"Migrate my Dataproc jobs and their schedules"*
- *"Copy the tables over 50 GB through Parquet files"*
- *"Publish the migration to my AIDP workspace and run it"*
- *"Is my setup ready to publish?"*

---

## Install

### Via Anthropic's community marketplace (recommended)

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-gcp-migrator
```

### From a local clone

```
/plugin marketplace add ./oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator
/plugin install oracle-ai-data-platform-workbench-gcp-migrator@aidp-gcp-migrator
```

### Try it offline first

```bash
cd oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator
./demo.sh
```

The demo runs `inventory`, `plan`, `migrate` and `verify` on an invented estate
(Northwind Retail: 4 datasets, 35 tables, views, routines, schedules, buckets, Dataproc
and more) and ends with a `publish` dry run. It contacts nothing and needs no credentials.

---

## Prerequisites

The CLI must be installed where the plugin can call it:

```bash
pip install -e .                    # offline verbs: stdlib only
pip install -e '.[gcp]'             # live inventory (google-auth)
pip install -e '.[gcp,oci]'         # export, for large tables (adds the OCI SDK)
```

To migrate a real project you also need, set up once (full steps:
[`docs/RUNBOOK.md`](./docs/RUNBOOK.md#setup-live-use)):

1. **A read-only Google service account** and its key in `GOOGLE_APPLICATION_CREDENTIALS`.
   For large tables only, a separate **export** service account in `GCP_EXPORT_CREDENTIALS`.
2. **On AIDP, once, by an administrator:** the target catalog; a Spark 3.5 cluster with the
   Spark BigQuery connector JAR; the key in the credential store; ideally a dedicated
   migration identity.
3. **On this machine:** `aidp-cli` (in its own venv), an `~/.oci/config` profile, and a
   `.env` with the AIDP instance, workspace, cluster and prefix (see
   [`.env.example`](./.env.example)).

When that is done, ask Claude *"check my setup"*: the `gcp-aidp-migrator-bootstrap` skill
checks every item without printing a secret.

---

## Skill index

### Skills
| Skill | Purpose |
|---|---|
| `gcp-aidp-migrator` | Runs the migration: the verbs in order, stopping at the plan, the report and every dry run for you to decide. |
| `gcp-aidp-migrator-bootstrap` | Readiness check: the CLI, Google keys, `aidp-cli`, the OCI profile, `.env`, and the AIDP cluster. |

### Slash commands
| Command | What it does |
|---|---|
| `/oracle-ai-data-platform-workbench-gcp-migrator:inventory` | Read-only scan of the project into a manifest. |
| `…:plan` | The plan and its approval document. |
| `…:migrate` | Translated artifacts and copy notebooks, locally. |
| `…:verify` | PASS / REVIEW / SKIP / FAIL per asset. |
| `…:export` | Large tables to Parquet in GCS and on to OCI (dry run first). |
| `…:publish` | Notebooks and jobs into the AIDP workspace (dry run first). |
| `…:run` | Starts the migration job and follows it. |

### MCP server
`inventory`, `plan`, `migrate` and `verify` are also exposed as an MCP server
(`gcp-aidp-mcp`, in `.mcp.json`) for Codex, Cursor or any MCP client; run
`pip install -e '.[mcp]'` (Python 3.10+) first. `export`, `publish` and `run` write, so
they are CLI-only.

### References (loaded on demand)
| Reference | Use |
|---|---|
| [`docs/RUNBOOK.md`](./docs/RUNBOOK.md) | Setup, the migration step by step with the check at each step, the copy, publishing, the safety posture. |
| [`references/type-mapping.md`](./references/type-mapping.md) | BigQuery → Delta types (`TY01`–`TY99`) and table layout rules. |
| [`references/dialect-translation.md`](./references/dialect-translation.md) | GoogleSQL → Spark SQL rules (`G01`–`G98`), with the evidence for each. |
| [`references/dataproc-translation.md`](./references/dataproc-translation.md) | Dataproc PySpark and Spark SQL rules (`P01`–`P99`). |
| [`references/notebook-translation.md`](./references/notebook-translation.md) | BigQuery Studio notebook rules (`N01`–`N04`). |
| [`references/schedule-translation.md`](./references/schedule-translation.md) | Scheduled queries and Cloud Scheduler → Quartz, created PAUSED. |
| [`references/export-route.md`](./references/export-route.md) | Large tables through Parquet: steps, accounts, per-type rules. |
| [`skills/gcp-aidp-migrator/references/verbs.md`](./skills/gcp-aidp-migrator/references/verbs.md) | Every verb and flag. |

---

## Engine

Everything goes through the `gcp-aidp` CLI, which runs in any terminal, with or without
Claude Code:

```bash
gcp-aidp inventory --project <project> [--scan-services] -o inv.json      # read-only
gcp-aidp plan      inv.json --catalog <catalog> [--datasets a,b] -o plan.json
gcp-aidp migrate   plan.json -o migrated                                   # local only
gcp-aidp verify    migrated
gcp-aidp export    migrated [--apply]       # only with plan --export-over-gb
gcp-aidp publish   migrated [--apply]
gcp-aidp run       migrated
```

The checks to make between steps are in [`docs/RUNBOOK.md`](./docs/RUNBOOK.md#run-a-migration).
The code is in `gcp_aidp/` (one package per verb, `dataplane/` for the notebooks' code);
tests and the live test checklist are in [`TESTING.md`](./TESTING.md).

---

## Privacy

There is no telemetry, and nothing is sent to Oracle, Anthropic or any third party by the
tool itself: it talks only to your own Google Cloud project, OCI tenancy and AIDP
workspace. What each verb reads, writes and sends: [`PRIVACY.md`](./PRIVACY.md).

---

## License

MIT — see [`LICENSE`](./LICENSE).
