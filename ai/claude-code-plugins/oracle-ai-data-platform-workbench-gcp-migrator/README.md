# gcp-aidp-migrator

Migrate a Google Cloud data estate centred on BigQuery to Oracle AI Data Platform
(AIDP) in six verbs: `inventory`, `plan`, `migrate`, `verify`, `publish` and `run`.
It inventories the estate read-only, plans the mapping to AIDP for your approval,
translates DDL and GoogleSQL with named, deterministic rules, generates the
notebooks that copy BigQuery tables into Delta, verifies the output, and publishes
and runs it in an AIDP workspace on request. Anything it cannot translate safely
is flagged or blocked with the reason, never guessed.

The migrator is a standalone command-line tool (`gcp-aidp`) that needs no AI: every
result comes from the named rules. The Claude Code plugin and the MCP server are
optional layers on top of it that guide the workflow and help with setup and review.

## What it does

| Google Cloud | → | AIDP |
|---|---|---|
| Project | → | INTERNAL catalog |
| Dataset | → | Schema |
| Table | → | Managed Delta table, rows copied by a generated notebook |
| Partitioning, clustering | → | `PARTITIONED BY` where exact, otherwise liquid `CLUSTER BY` |
| View | → | Spark SQL view |
| Materialized view | → | a table built on AIDP from the migrated tables, plus an unscheduled refresh job |
| Scheduled query without a destination table | → | an unscheduled AIDP job; the source schedule is recorded |
| Saved query | → | a Spark SQL file |
| External table | → | `USING PARQUET/AVRO/ORC/CSV/JSON` over `oci://` |
| Cloud Storage bucket | → | an rclone transfer job to OCI Object Storage |
| Procedures, JavaScript and table functions, BigQuery ML, access policies | → | reported with an effort band, not translated |
| Dataform repository | → | one unscheduled AIDP job, a task per compiled action, with Dataform's dependencies as `dependsOn`; tables, views and assertions only, see `references/dataform-translation.md` |
| Composer DAG file | → | one unscheduled AIDP job, a task per Airflow task, with upstream tasks as `dependsOn`; BigQuery query tasks and empty tasks only, see `references/airflow-translation.md` |
| Composer environment | → | reported; Airflow itself is not migrated |
| BigQuery Studio notebooks, Dataproc, Dataflow, Vertex AI | → | inventoried, planned for a later version |

## Status (0.1)

Live means run against a real system: a BigQuery sandbox project (no billing) and an
AIDP workspace on Spark 3.5.0. Offline means unit and fixture tests only.

| Area | State | Tested |
|---|---|---|
| `inventory`: BigQuery datasets, tables, views, materialized views, routines, models, row access policies, policy tags, IAM | ✅ built | live |
| `inventory`: external tables, scheduled queries, Cloud Storage | ✅ built | offline only (the sandbox has no billing) |
| `inventory`: Dataproc, Composer, Dataform, Dataflow, Vertex AI (`--scan-services`) | ✅ listed for SKIP reporting | offline only |
| `plan` | ✅ built | live, on the sandbox inventory |
| GoogleSQL → Spark SQL translator | ✅ 28 named rules | Spark side on local Spark 3.5.9; BigQuery side by zero-byte probes; 3 translated views created on AIDP |
| Type mapping and table layout | ✅ built | live: 7 tables created and read back against the plan |
| Data copy notebooks (`00`–`03`) | ✅ built | live: 7 tables, 293,886 rows, counts matching |
| Materialized view snapshot | ✅ built | live, inside the migration job; the refresh job on its own offline only |
| Scheduled-query jobs | ✅ built | offline only (the sandbox has none) |
| External tables, rclone transfer jobs | ✅ built | offline only |
| `verify` | ✅ built | offline (it reads the report only) |
| `publish`, `run` | ✅ built | live: 5 notebooks and 2 unscheduled jobs created; every task of the migration job succeeded |
| MCP server | ✅ built | offline (needs `mcp`) |
| Live cell-by-cell execution and repair | 🚧 not built | |

> **What PASS means.** PASS = *translated, and no known issue was detected*. It is
> **not execution-verified**: `verify` does not parse or run the generated artifacts,
> so a construct none of the rules cover is reported clean. Treat PASS as "nothing
> the tool knows about is wrong here", and review artifacts before running them in
> production. REVIEW is the honest signal that something needs a human — a low
> REVIEW count is not by itself evidence of a clean migration.

## Quick start (offline, no credentials)

```bash
cd oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator
./demo.sh
```

The demo reads `gcp_aidp/fixtures/demo-manifest.json`, an invented estate
(Northwind Retail): 4 datasets, 35 tables, 8 views, 2 materialized views,
routines, BigQuery ML models, saved and scheduled queries, access policies, 4
Cloud Storage buckets, and Dataproc, Composer, Dataform, Dataflow and Vertex AI
assets. It runs `inventory`, `plan`, `migrate` and `verify`, shows a view before
and after translation and a blocked one, and ends with a `publish` dry run. It
contacts nothing; the output is in `/tmp/gcp-aidp-demo/`.

## Use it as a Claude Code plugin

**Via Anthropic's community marketplace**, once listed:

```bash
# in Claude Code
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-gcp-migrator
```

**From a local clone:**

```bash
# in Claude Code
/plugin marketplace add ./oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-gcp-migrator
/plugin install oracle-ai-data-platform-workbench-gcp-migrator@aidp-gcp-migrator
```

Then drive it with `/oracle-ai-data-platform-workbench-gcp-migrator:inventory`,
`:plan`, `:migrate`, `:verify`, `:publish` and `:run`, or ask in natural language: the
`gcp-aidp-migrator` skill routes the workflow. The CLI must be installed
(`pip install -e .`, plus `'.[gcp]'` for live inventory) so the plugin can call it.

> **Codex / Cursor / any MCP client:** `inventory`, `plan`, `migrate` and
> `verify` are also exposed as an **MCP server** (`gcp-aidp-mcp`, wired up in
> `.mcp.json`). Run `pip install -e '.[mcp]'` (Python 3.10+) before first use;
> without it the server exits and the client reports it as failed in `/mcp`.
> `publish` and `run` change an AIDP workspace, so they are CLI-only.

## Setup (live use)

The offline demo needs none of this. Live inventory needs step 1; the copy needs 1
and 2; `publish` and `run` need all three. When it is done, ask Claude to check your
setup: the `gcp-aidp-migrator-bootstrap` skill checks every item without printing a
secret.

### 1. Google Cloud

1. Create a service account with **BigQuery Data Viewer**, **BigQuery Read Session
   User** and **BigQuery Job User**. Listing buckets (`storage.buckets.list`) and
   scheduled queries (`bigquery.transfers.get`) needs more; without it the inventory
   records that collection as *not scanned*, so add those only if you need them.
2. Create a JSON key for it (**IAM → Service accounts → Keys → Add key**). Keep it
   outside the repository, `chmod 600` it, and
   `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.

### 2. AIDP, once, by an administrator

1. **Target catalog.** Create the standard (INTERNAL) catalog the plan targets.
2. **Cluster.** A Spark 3.5 / Scala 2.12 cluster with
   `spark-bigquery-with-dependencies_2.12-<version>.jar` added as a library (AIDP
   takes the JAR file, not a Maven coordinate).
3. **Credential.** In the credential store, an entry `gcp_bigquery_reader` with key
   `credentials_b64` holding the base64 service account key. On macOS:
   `base64 -i key.json | tr -d '\n' | pbcopy`, paste it, then `pbcopy < /dev/null`.
4. **A migration identity** (recommended). The plugin acts with whatever the OCI
   profile can do, so give it its own user rather than an administrator's key:
   an OCI user in a group `aidp-migrators`, with one policy statement

   ```
   allow group <domain>/aidp-migrators to use ai-data-platforms in compartment id <aidp_compartment_ocid>
   ```

   and, inside the workbench, only these AIDP permissions:

   | Object | Permission | For |
   |---|---|---|
   | The workspace | `USER` | creating its `/Workspace/<prefix>/` folder |
   | The cluster from step 2 | `Use` | running the jobs on it |
   | The catalog from step 1 | `CREATE_SCHEMA`, `MANAGE` | creating schemas and tables, inserting rows |
   | The credential from step 3 | `Use` | reading the BigQuery key in the notebooks |

   Nothing on the master catalog. `use` (not `manage`) cannot change or delete the
   instance; `USER` (not `PRIVILEGED_USER`) cannot create clusters. This set follows
   the [AIDP permissions model](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/permissions-model.html)
   and [IAM policies](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html);
   it has **not yet been run end to end with a restricted user**, so report any
   missing grant.

### 3. This machine

1. **aidp-cli**, in its own venv (it conflicts with `oci-cli`'s pins):
   `python3 -m venv ~/.venvs/aidp-cli && ~/.venvs/aidp-cli/bin/pip install aidp-cli`,
   then put `~/.venvs/aidp-cli/bin` on PATH.
2. **An API key for the migration identity.** In the OCI Console, signed in as that
   user: **Profile → API keys → Add API key → Generate API key pair**. Save the
   private key as `~/.oci/<name>.pem` (`chmod 600`). Paste the **configuration file
   preview** into `~/.oci/config` as it is, including its `[DEFAULT]` header line
   (rename it, e.g. `[gcp_migration]`, to keep it apart from your own), and change
   only `key_file=` to the `.pem` path. `chmod 600 ~/.oci/config`.
3. **The IDs.**
   - Instance OCID: OCI Console → **Analytics & AI → AI Data Platform** → your
     instance → **OCID**. It starts `ocid1.aidataplatform.`; the workbench URL's
     hostname is not it.
   - Workspace key: the workspace's UUID, from its URL or details in the workbench.
   - Cluster key: `aidp cluster list <workspace-key> --instance-id <ocid> --auth api_key`
     lists every cluster with its `key`, `displayName` and `state`.
4. **`.env`** in the plugin folder or your working directory (see `.env.example`):

   ```
   AIDP_INSTANCE_ID=ocid1.aidataplatform.oc1...
   AIDP_WORKSPACE_KEY=<uuid>
   AIDP_CLUSTER_KEY=<uuid>
   AIDP_PREFIX=<letters_digits_underscores>
   OCI_CLI_PROFILE=gcp_migration     # the profile from step 2; unset means DEFAULT
   AIDP_AUTH=api_key                 # aidp-cli otherwise assumes security_token
   ```

## Run a migration

After [Setup](#setup-live-use), seven commands, in order. Stop at each check before
the next one: the plan, the report and the dry run are where a person decides. In
Claude Code you can instead ask for the migration in plain words; the
`gcp-aidp-migrator` skill runs the same steps and stops at the same checks.

1. **Inventory** the project (read-only, metadata only):

   ```bash
   gcp-aidp inventory --project <project> -o inv.json
   ```

   Check: the counts per source, and anything listed as *not scanned*. A gap is
   unknown, not zero.

2. **Plan** the migration and read the approval document:

   ```bash
   gcp-aidp plan inv.json --catalog <catalog> --datasets <a,b> -o plan.json
   ```

   Check `plan.md`: the scope line (which datasets), every table blocked by a type,
   and every `REPORT` and `SKIP` row. Leave out `--datasets` to migrate every dataset.

3. **Migrate**, which writes the artifacts locally and contacts nothing:

   ```bash
   gcp-aidp migrate plan.json -o migrated
   ```

   Check `migrated/report.md`: every caveat, flag and block needs a person.

4. **Verify:**

   ```bash
   gcp-aidp verify migrated
   ```

   Check: no `FAIL`. `PASS` means no known issue, not that the artifact has run.

5. **Publish, dry run first:**

   ```bash
   gcp-aidp publish migrated
   ```

   Check: the folder, the notebooks and the jobs it lists, and nothing `REFUSED`.

6. **Publish for real:**

   ```bash
   gcp-aidp publish migrated --apply
   ```

   This uploads the notebooks to `/Workspace/<prefix>/` and creates the jobs,
   unscheduled. It never overwrites.

7. **Run** the migration job and follow it:

   ```bash
   gcp-aidp run migrated
   ```

   Done when every task succeeds and `/Workspace/<prefix>/reports/MIGRATION_REPORT.md`
   shows `MIGRATED_VERIFIED` (first copy) or `PRESENT_NOT_REVERIFIED` (already copied,
   counts matching) for every table, with blocked objects named and explained.

## Live inventory (read-only)

```bash
pip install -e '.[gcp]'                       # google-auth only
export GOOGLE_APPLICATION_CREDENTIALS=/path/outside/the/repo/key.json
gcp-aidp inventory --project <project> [--regions us-central1] [--saved-queries-dir dir] [--scan-services] -o inv.json
```

Every call is a metadata `GET` made with a `cloud-platform.read-only` token, so
Google refuses any write. Dataproc, Composer, Dataform, Dataflow and Vertex AI
refuse a read-only token, so they are listed only with `--scan-services`. That
flag asks for a `cloud-platform` token for those five services only, and read-only
then rests on the client sending GETs only and on the service account's
viewer roles. Without it, they are recorded as *not scanned*. No query runs, and no bytes are billed. Row counts
come from table metadata. Anything the scan could not read is recorded as *not
scanned* and shown at the top of the plan: saved queries (pass
`--saved-queries-dir` instead), BigQuery Studio notebooks and pipelines in 0.1,
and any API call that was refused. A service whose API is disabled is recorded
as having nothing to migrate.

`test-estate/` recreates a small estate in any project (`seed.sql`,
`MANUAL_STEPS.md`, `teardown.sql`), with a checklist of what the inventory must
list.

Every asset gets a plan row with one of three actions:

| Action | Meaning |
|---|---|
| `MIGRATE` | migrated in this version |
| `REPORT` | inventoried and reported with an effort band; not translated (procedures, JavaScript and table functions, BigQuery ML models, access rules) |
| `SKIP` | planned for a later version, with the reason (notebooks and Dataproc in 0.2, Dataflow and Vertex AI later), or outside `--datasets`, `--dataform-repos` or `--dags` |

**What you choose at `plan`:** the target catalog (`--catalog`, default: the project
id), the OCI namespace for buckets (`--namespace`), the datasets to migrate
(`--datasets sales,finance`), and whether `BIGNUMERIC` and `GEOGRAPHY` columns are
carried as text (`--bignumeric string`, `--geography wkt`). Without `--datasets`,
every dataset in the project is planned and copied. With it, the other datasets'
assets stay in the plan as `SKIP`, so the approval document still shows the whole
estate, and its first lines say which datasets are in scope. A view that reads a
table in an excluded dataset is flagged. Tables within a dataset are not chosen
here: the copy notebook takes a `tables` parameter for a partial run.

`--dags a,b` does the same for Composer DAGs, matched by DAG file name in any
environment. The inventory reads each DAG file as text and never runs it, so the
inventory file then contains DAG source code and the plan carries it too. Treat both
as sensitive and do not commit them.

Two assets that would land on the same target name (compared case-insensitively,
as Spark does) halt the plan. The planner does not pick a winner.

`migrate` writes one reviewable artifact per asset (schemas, tables, views,
materialized views, external tables, functions, saved and scheduled queries,
rclone transfer jobs) plus `report.json` and `report.md`. It writes files
locally only; nothing reaches AIDP before `publish --apply`. `verify` labels
every asset:

| Verdict | Meaning |
|---|---|
| PASS | translated, and no known issue was detected |
| REVIEW | a caveat or flag needs a human, or the asset is blocked (not translated) |
| SKIP | reported only, or planned for a later version |
| FAIL | the migrator failed, or the report contradicts itself |

## Copying the data

`migrate` also writes four self-contained notebooks to `migrated/notebooks/`,
which run on an AIDP cluster:

| Notebook | Reads | Writes |
|---|---|---|
| `00_diagnose` | connector, credential, catalog | nothing; prints the connector's type for every column |
| `01_structure` | the plan | schemas, empty Delta tables (each read back against the plan), views |
| `02_copy_dataset` | one BigQuery dataset, table by table | rows; verifies counts, and with `counts+sums` exact decimal sums |
| `03_reconcile` | plan, reports, catalog | `MIGRATION_REPORT.md` |

Prerequisites: AIDP has no built-in BigQuery connector, so the cluster needs the
open-source Spark BigQuery connector JAR, and the notebooks read the service account
key from the AIDP credential store and never print it. See
[Setup, steps 1 and 2](#setup-live-use).

The copy reads tables only: reading a view or a query result makes the
connector write a temporary table in BigQuery, so views are rebuilt from their
translated SQL and materialized views from their query. Nothing is dropped:
the default `skip-existing` mode leaves a table with rows untouched, and
`overwrite` rewrites rows, never the table.

> **Consistency.** Each table is copied at its own moment. If the source keeps
> changing during the copy, the target is consistent per table but not across
> tables. For a cutover, stop writers or copy from BigQuery table snapshots.

The rule tables are [`references/type-mapping.md`](references/type-mapping.md)
and [`references/dialect-translation.md`](references/dialect-translation.md).

## Publishing to AIDP

```bash
pip install aidp-cli            # provides `aidp`; a venv keeps it apart from oci-cli
gcp-aidp publish migrated       # dry run: lists what would be created, sends nothing
gcp-aidp publish migrated --apply
gcp-aidp run migrated           # starts <prefix>_gcp_aidp_migration and polls it
```

`publish` uploads the notebooks to `/Workspace/<prefix>/` and creates the jobs
unscheduled; it never overwrites. It needs [Setup, step 3](#3-this-machine). The CLI
reads `.env` from the working directory, then from the plugin folder; the shell
overrides both.

## Translator coverage

Every rule has an id, a test, and a line in the report wherever it fires. The full
tables, with the evidence for each decision, are
[`references/type-mapping.md`](references/type-mapping.md) and
[`references/dialect-translation.md`](references/dialect-translation.md).

**Types** (BigQuery → Delta):

| Rule | BigQuery | Delta | Action |
|---|---|---|---|
| `TY01`–`TY07` | `INT64`, `FLOAT64`, `BOOL`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP` | `BIGINT`, `DOUBLE`, `BOOLEAN`, `STRING`, `BINARY`, `DATE`, `TIMESTAMP` | map |
| `TY08` | `NUMERIC`, `NUMERIC(P,S)` | `DECIMAL(38,9)`, `DECIMAL(P,S)` | map |
| `TY09` | `BIGNUMERIC` | none (`--bignumeric string`: text) | **block** |
| `TY10` | `DATETIME` | `TIMESTAMP`, read in UTC | caveat |
| `TY11` | `TIME` | `STRING` | flag |
| `TY12`, `TY13` | `STRUCT`, `ARRAY` | `STRUCT<...>`, `ARRAY<...>` | map |
| `TY14` | `JSON` | `STRING` | caveat |
| `TY15` | `GEOGRAPHY` | none (`--geography wkt`: WKT text) | **block** |
| `TY16`, `TY17`, `TY99` | `INTERVAL`, `RANGE`, anything else | none | **block** |

**Layout and objects:**

| Rule | Source | AIDP |
|---|---|---|
| `D01_PARTITION` | DAY partitioning on a `DATE` column | `PARTITIONED BY (col)` |
| `D02_CLUSTER` | clustering, or any other partitioning | liquid `CLUSTER BY`, at most 4 keys |
| `D03_INGESTION_TIME` | ingestion-time partitioning | not partitioned; flagged |
| `D04_METADATA` | expiration, labels | reported |
| `D05_EXTERNAL_FORMAT` | external table | `USING <format>`; schema inferred (caveat); other formats blocked |
| `D06_SQL_FUNCTION` | SQL UDF | flagged: Spark 3.5 cannot parse `CREATE FUNCTION ... RETURN` |
| `M01_MATERIALIZED_VIEW` | materialized view | snapshot table + unscheduled refresh job |
| `J01_UNSCHEDULED`, `J02_DESTINATION_TABLE` | scheduled query | unscheduled job; one with a destination table is flagged |

**GoogleSQL → Spark SQL:**

| Rule | GoogleSQL | Spark SQL | Type |
|---|---|---|---|
| `G01_REFERENCE` | `` `project.dataset.table` `` | `` `catalog`.`schema`.`table` `` from the plan | rewrite |
| `G03_SAFE_DIVIDE` | `SAFE_DIVIDE(a, b)` | `try_divide(a, b)` | caveat: `NUMERIC` keeps 6 decimals |
| `G04_COUNTIF` | `COUNTIF(c)` | `count_if(c)` | rewrite |
| `G06_TIMESTAMP_TRUNC` | `TIMESTAMP_TRUNC(ts, part)` | `date_trunc('PART', ts)` | caveat: exact in UTC |
| `G07_DATE_DIFF` | `DATE_DIFF(a, b, DAY)` | `datediff(a, b)` | rewrite |
| `G08_FORMAT_DATE` | `FORMAT_DATE('%Y-%m-%d', d)` | `date_format(d, 'yyyy-MM-dd')` | caveat: years before 1000 |
| `G15_QUALIFY` | `... QUALIFY <cond>` | a filtered subquery | rewrite; uncovered shapes block |
| `G19_CAST_TYPE` | `CAST(x AS INT64)` ... | `CAST(x AS BIGINT)` ... | rewrite |
| `G20_HASH_COMMENT` | `# note` | `-- note` | rewrite |
| `G21_GCS_PATH` | `'gs://bucket/path'` | `'oci://bucket@namespace/path'` | rewrite |
| `G02`, `G05`, `G08_PARSE_DATE`, `G09`–`G14`, `G22`–`G24` | `SAFE_CAST`, `GENERATE_ARRAY`, `PARSE_DATE`, `ARRAY_LENGTH`, regex, JSON, `STRING_AGG`, `UNNEST`, `* EXCEPT`, same-name functions, `@param`, `b'...'` | left as written | flag: Spark's meaning differs |
| `G90_NOT_SPARK_BUILTIN` | any other call | left as written | flag |
| `G16`–`G18`, `G97`, `G98` | pseudo-columns and wildcard tables, scripting, `ML.*`/`AI.*`/`ST_*`, legacy SQL, unbalanced text | not translated | **block** |

A blocked statement is never partially translated: the artifact keeps the original,
commented out.

## Safety posture

- **Read-only against Google Cloud.** Inventory makes metadata `GET` calls with a
  `cloud-platform.read-only` token. No query runs and no bytes are billed; row counts
  come from table metadata. The one exception is a maintainer script outside the
  verbs, `scripts/probe_bigquery_semantics.py`: it runs zero-byte queries on literals,
  each dry-run first, to check the translator's BigQuery side.
- **No hidden writes in BigQuery.** The copy reads tables only. Reading a view or a
  query result through the connector would create a temporary table, so views are
  rebuilt from translated SQL and materialized views from their query.
- **Local until you publish.** `inventory`, `plan`, `migrate` and `verify` write files
  only. `publish` is a dry run until `--apply`, and the dry run lists the destination
  and every object; it never overwrites, and creates jobs unscheduled.
- **Nothing is dropped.** Every statement is `CREATE ... IF NOT EXISTS`; the copy's
  default `skip-existing` leaves a table with rows untouched.
- **Secrets stay put.** The key is read from `GOOGLE_APPLICATION_CREDENTIALS` locally
  and from the AIDP credential store on the cluster, and is never printed. `.env`
  takes only `GCP_`, `GOOGLE_APPLICATION_CREDENTIALS`, `AIDP_` and `OCI_` keys.
- **Deterministic.** Named rules only, no LLM in the translation path. A name
  collision halts the plan rather than picking a winner.
- **Resumable.** An interrupted `migrate` is detected and fails `verify`; a rerun of
  the copy skips tables that finished.

### What the agent can reach in AIDP

`publish` and `run` sign every call with the `~/.oci/config` profile you give them,
and that profile carries all of its OCI permissions. The plugin uses it narrowly: it
uploads notebooks, creates unscheduled jobs, starts a job and reads its status. It
never deletes, overwrites or schedules anything. The slash commands allow only
`gcp-aidp publish` and `gcp-aidp run`, and the MCP server leaves both out. But
anything that can run shell commands as you, an agent included, can use the same
profile for any `aidp` or OCI command. To limit that:

- **Use a dedicated migration identity** with only the grants in
  [Setup, step 2.4](#2-aidp-once-by-an-administrator), through its own
  `OCI_CLI_PROFILE`.
- **Prefer `security_token` auth** (the `aidp-cli` default) to a long-lived API key:
  a session token expires within hours. It narrows *how long* a credential works,
  not *what* it can do; only the identity's grants do that.
- **Run `publish --apply` and `run` yourself.** Let the agent prepare and review the
  dry run.

On the Google side, the service account needs read-only BigQuery roles only, and its
key stays in the AIDP credential store.

## Testing

See [`TESTING.md`](TESTING.md). In short:

```bash
python3 -m unittest discover -s tests -t .
./demo.sh
```

## Layout

```
gcp_aidp/
  cli.py               # argparse, 6 verbs
  _env.py              # .env loader (allowed prefixes only)
  gcp_client.py        # read-only REST GETs through google-auth
  inventory/           # BigQuery, Cloud Storage, other services → manifest
  plan/                # manifest → plan.json + plan.md
  translate/           # types, DDL, GoogleSQL, gs:// paths
  migrate/             # writes artifacts, notebooks and report locally
  dataplane/           # the code inside the 00–03 notebooks
  verify/              # PASS / REVIEW / SKIP / FAIL
  publish/             # publish and run, through aidp-cli
  mcp_server.py        # MCP server for the four offline verbs
  fixtures/            # the Northwind Retail demo manifest
commands/  skills/     # Claude Code slash commands and skill
references/            # type mapping and dialect translation rules
test-estate/           # seed.sql, MANUAL_STEPS.md, teardown.sql
demo.sh                # offline end-to-end demo
```

## Roadmap

0.1 automates the pipeline, one command per step, but not repair. `run` executes the
whole migration job and reports the verdicts; a failure stops for a human, and a
rerun skips the copies that finished. There is no execute-check-fix loop, one that
runs migrated code cell by cell on a live cluster and has Claude repair the cells that
fail. That is deliberate while what moves is data and DDL, where a wrong
automatic fix silently corrupts a table. Whether it is worth building for code
migration is decided with 0.2's results.

- 0.2: BigQuery Studio notebooks and Dataproc (clusters, jobs).
  Under consideration: a live execute-check-fix pass for the migrated code.
- 0.3: Composer (Airflow DAGs → unscheduled AIDP jobs) and Dataform are built; BigQuery pipelines are next.
- Later: Dataflow and Vertex AI.
