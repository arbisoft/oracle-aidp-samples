# Runbook: migrating a Google Cloud estate to AIDP

The full steps behind the README: what to set up once, the commands in order with the check at each one, and what each stage does. The Claude Code skill follows the same steps.

- [Setup (live use)](#setup-live-use)
- [Run a migration](#run-a-migration)
- [Live inventory (read-only)](#live-inventory-read-only)
- [Copying the data](#copying-the-data)
- [Publishing to AIDP](#publishing-to-aidp)
- [Safety posture](#safety-posture)
- [Roadmap](#roadmap)

## Setup (live use)

The offline demo needs none of this. Live inventory needs step 1; the copy needs 1
and 2; `publish` and `run` need all three. When it is done, ask Claude to check your
setup: the `gcp-aidp-migrator-bootstrap` skill checks every item without printing a
secret.

### 1. Google Cloud

1. Create a service account with **BigQuery Data Viewer**, **BigQuery Read Session
   User** and **BigQuery Job User**. Listing buckets (`storage.buckets.list`) and
   scheduled queries (`bigquery.transfers.get`) needs more; without it the inventory
   records that collection as *not scanned*, so add those only if you need them. For
   Dataproc add **Dataproc Viewer**, plus **Storage Object Viewer** on the buckets holding
   job code, and **Cloud Scheduler Viewer** to carry the schedules that start Dataproc
   jobs; for BigQuery Studio notebooks, saved queries and pipelines add **Dataform Viewer**. All are read-only; run
   the inventory with `--scan-services` for these.
2. Create a JSON key for it (**IAM → Service accounts → Keys → Add key**). Keep it
   outside the repository, `chmod 600` it, and
   `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.
3. **Only for large tables** (`plan --export-over-gb`): a second, **export** service
   account that may run BigQuery export jobs and write to a staging bucket, with its key
   in `GCP_EXPORT_CREDENTIALS`. The account above stays read-only. Roles and the OCI
   bucket it needs: [`references/export-route.md`](../references/export-route.md).

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
   an OCI user in a group `aidp-migrators` (OCI Console, home region: **Identity &
   Security → Domains → Default → User management**), with an API key as its OCI
   profile (`OCI_CLI_PROFILE`), and one policy statement (**Identity & Security →
   Policies**, in the AIDP compartment)

   ```
   allow group <domain>/aidp-migrators to use ai-data-platforms in compartment id <aidp_compartment_ocid>
   ```

   For the [export route](../references/export-route.md), `export --apply` uploads
   the files to OCI with this same profile, so add, in the bucket's compartment:

   ```
   allow group <domain>/aidp-migrators to manage objects in compartment id <bucket_compartment_ocid> where target.bucket.name='<oci_bucket>'
   allow group <domain>/aidp-migrators to read buckets in compartment id <bucket_compartment_ocid> where target.bucket.name='<oci_bucket>'
   ```

   Then, inside the workbench, create a role (**Roles**), add the user to it, and
   give the role only these AIDP permissions, each on the object's **Permissions** tab:

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

After [Setup](#setup-live-use), seven commands, in order (plus `export` and `transfer` when the plan needs them). Stop at each check before
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

   **Only if the plan used `--export-over-gb`:** now run `gcp-aidp export migrated` (a
   dry run listing the tables and which use a billed `EXPORT DATA` query), then
   `gcp-aidp export migrated --apply`, which exports the large tables to Parquet and
   copies them to OCI. It must finish before step 7. See
   [`references/export-route.md`](../references/export-route.md).

   **Only if the plan has Cloud Storage buckets:** run `gcp-aidp transfer migrated` (a
   dry run: per bucket, the files and MB still to copy), then
   `gcp-aidp transfer migrated --apply [--bucket <gcs-bucket>]`, which copies them to
   OCI, read-only on Google Cloud and never deleting. External tables read these files:
   `01_structure` creates each one once its files are on OCI, and reports it as not
   created yet until then; run it again after a late transfer.

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
   unscheduled or with their source schedule PAUSED. It never overwrites.

7. **Run** the migration job and follow it:

   ```bash
   gcp-aidp run migrated
   ```

   Done when every task succeeds and `/Workspace/<prefix>/reports/MIGRATION_REPORT.md`
   shows `MIGRATED_VERIFIED` (first copy) or `PRESENT_NOT_REVERIFIED` (already copied,
   counts matching) for every table, with blocked objects named and explained.

> **What PASS means.** PASS = *translated, and no known issue was detected*. It is
> **not execution-verified**: `verify` does not parse or run the generated artifacts,
> so a construct none of the rules cover is reported clean. Treat PASS as "nothing
> the tool knows about is wrong here", and review artifacts before running them in
> production. REVIEW is the honest signal that something needs a human — a low
> REVIEW count is not by itself evidence of a clean migration.

## Live inventory (read-only)

```bash
pip install -e '.[gcp]'                       # google-auth only
export GOOGLE_APPLICATION_CREDENTIALS=/path/outside/the/repo/key.json
gcp-aidp inventory --project <project> [--regions us-central1] [--saved-queries-dir dir] [--scan-services] -o inv.json
```

Every call is a metadata `GET` made with a `cloud-platform.read-only` token, so
Google refuses any write. Dataproc, Composer, Dataform, Dataflow and Vertex AI refuse a
read-only token, so they are listed only with `--scan-services`. That flag asks for a
`cloud-platform` token for those services only, which also covers the Cloud Scheduler
jobs read with Dataproc and the BigQuery Studio notebooks read through Dataform; read-only
then rests on the client sending GETs only and on the service account's viewer roles. Without it, they are recorded as *not scanned*. For Dataproc, the scan
also reads the `.py` or `.sql` file each PySpark and Spark SQL job runs from Cloud
Storage (an object read, at most 1 MB), so it can be translated; JARs are recorded, not
read. Dataproc's run history is grouped into job definitions: one per main file and
cluster, with its newest arguments and a run count. No query runs, and no bytes are billed. Row counts
come from table metadata. Anything the scan could not read is recorded as *not
scanned* and shown at the top of the plan: BigQuery Studio notebooks, saved queries and
pipelines without `--scan-services` (or export notebooks and saved queries as `.ipynb` and
`.sql` files and pass `--notebooks-dir` and `--saved-queries-dir`),
and any API call that was refused. A service whose API is disabled is recorded
as having nothing to migrate.

`test-estate/` recreates a small estate in any project (`seed.sql`,
`MANUAL_STEPS.md`, `teardown.sql`), with a checklist of what the inventory must
list.

Every asset gets a plan row with one of three actions:

| Action | Meaning |
|---|---|
| `MIGRATE` | migrated in this version |
| `REPORT` | inventoried and reported with an effort band; not translated (JavaScript functions, BigQuery ML models, access rules) |
| `SKIP` | planned for a later version, with the reason (Composer, Dataflow and Vertex AI), a notebook listed but not exported, or outside `--datasets` or `--dataform-repos` |

**What you choose at `plan`:** the target catalog (`--catalog`, default: the project
id), the OCI namespace for buckets (`--namespace`), the datasets to migrate
(`--datasets sales,finance`), how `BIGNUMERIC`, `GEOGRAPHY`, `INTERVAL` and `RANGE`
columns are carried (`--bignumeric string`, `--geography wkt`, `--interval string`,
`--range struct`; blocked by default and listed under *Needs a decision*), and which tables take
the export route (`--export-over-gb`, below). `--bucket-mapping` names the OCI
bucket each GCS bucket lands in (an unmapped one keeps its name). Without `--datasets`,
every dataset in the project is planned and copied. With it, the other datasets'
assets stay in the plan as `SKIP`, so the approval document still shows the whole
estate, and its first lines say which datasets are in scope. A view that reads a
table in an excluded dataset is flagged. Tables within a dataset are not chosen
here: the copy notebook takes a `tables` parameter for a partial run.

`--dataform-repos a,b` does the same for Dataform repositories.

Two assets that would land on the same target name (compared case-insensitively,
as Spark does) halt the plan. The planner does not pick a winner.

`migrate` writes one reviewable artifact per asset (schemas, tables, views,
materialized views, external tables, functions, saved and scheduled queries,
bucket transfer specs) plus `report.json` and `report.md`. It writes files
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
| `00_diagnose` | connector, credential, catalog, export-route files, existing target tables | nothing; prints the connector's type for every column, and stops the run when a source's columns or an existing target's layout differ from the plan |
| `01_structure` | the plan | schemas, empty Delta tables (each read back against the plan), views |
| `02_copy_dataset` | one BigQuery dataset, table by table (export-route tables from their Parquet files) | rows; verifies counts, and with `counts+sums` exact decimal sums |
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

**Large tables: the export route.** With `plan --export-over-gb N --export-bucket B`,
each table of at least N GB is not read through the connector: `gcp-aidp export --apply`
has BigQuery export it to Parquet in `gs://B/gcp-aidp-export/<plan id>/` and copies the
files to OCI, and `02_copy_dataset` loads them with the same checks and casts, checking
the count against the files and against BigQuery. The export runs as a separate export
service account (`GCP_EXPORT_CREDENTIALS`); the migration's key stays read-only. Steps,
permissions and the per-type rules: [`references/export-route.md`](../references/export-route.md).

**Cloud Storage buckets.** `gcp-aidp transfer --apply` copies each bucket in the plan to
its OCI bucket (`--bucket-mapping`), with the same copy as `export`: read with the
read-only key, written with `~/.oci/config`, every file's MD5 checked by OCI, a file
already there skipped, nothing deleted. The OCI bucket must exist. A dry run (without
`--apply`) lists what is still to copy.

> **Consistency.** Each table is copied at its own moment. If the source keeps
> changing during the copy, the target is consistent per table but not across
> tables. For a cutover, stop writers or copy from BigQuery table snapshots.

The rule tables are [`references/type-mapping.md`](../references/type-mapping.md)
and [`references/dialect-translation.md`](../references/dialect-translation.md).

## Publishing to AIDP

```bash
pip install aidp-cli            # provides `aidp`; a venv keeps it apart from oci-cli
gcp-aidp publish migrated       # dry run: lists what would be created, sends nothing
gcp-aidp publish migrated --apply
gcp-aidp run migrated           # starts <prefix>_gcp_aidp_migration and polls it
```

`publish` uploads the notebooks to `/Workspace/<prefix>/` and creates the jobs,
unscheduled or with their source schedule PAUSED; it never overwrites. It needs [Setup, step 3](#3-this-machine). The CLI
reads `.env` from the working directory, then from the plugin folder; the shell
overrides both.

## Safety posture

What each verb reads, writes and sends is in [`PRIVACY.md`](../PRIVACY.md).

- **Read-only against Google Cloud, except `export --apply`.** Inventory makes `GET`
  calls only: metadata, and for Dataproc the job's own code file in Cloud Storage. It
  uses a `cloud-platform.read-only` token wherever the API accepts one. No query runs
  and no bytes are billed; row counts come from table metadata. The one verb that
  writes on Google Cloud is `export --apply` (large tables only): it runs BigQuery
  export jobs into your staging bucket as a **separate export service account**
  (`GCP_EXPORT_CREDENTIALS`), never with the migration's read-only key, and its dry run
  names every table that uses a billed `EXPORT DATA` query. Outside the verbs, a
  maintainer script, `scripts/probe_bigquery_semantics.py`, runs zero-byte queries on
  literals, each dry-run first, to check the translator's BigQuery side.
- **No hidden writes in BigQuery.** The copy reads tables only. Reading a view or a
  query result through the connector would create a temporary table, so views are
  rebuilt from translated SQL and materialized views from their query.
- **Local until you export or publish.** `inventory`, `plan`, `migrate` and `verify`
  write files only. `export` and `publish` are dry runs until `--apply`, and each dry run
  lists everything it would do. `export` never re-exports a folder or re-copies a file
  already in OCI. `publish` never overwrites, and creates jobs unscheduled or with their
  schedule PAUSED, so nothing runs until you turn it on.
- **Nothing is dropped.** Every statement is `CREATE ... IF NOT EXISTS`; the copy's
  default `skip-existing` leaves a table with rows untouched.
- **Secrets stay put.** The keys are read from `GOOGLE_APPLICATION_CREDENTIALS` (and,
  for `export`, `GCP_EXPORT_CREDENTIALS`) locally and from the AIDP credential store on
  the cluster, and are never printed. `.env`
  takes only `GCP_`, `GOOGLE_APPLICATION_CREDENTIALS`, `AIDP_` and `OCI_` keys.
- **Deterministic.** Named rules only, no LLM in the translation path. A name
  collision halts the plan rather than picking a winner.
- **Resumable.** An interrupted `migrate` is detected and fails `verify`; a rerun of
  the copy skips tables that finished.

### What the agent can reach in AIDP

`publish` and `run` sign every call with the `~/.oci/config` profile you give them,
and that profile carries all of its OCI permissions. The plugin uses it narrowly: it
uploads notebooks, creates jobs (any schedule PAUSED), starts a job and reads its
status. It never deletes, overwrites, or turns a schedule on. Of the slash commands, only
`/publish` and `/run` reach AIDP, and the MCP server leaves both out (and `export`). But
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

On the Google side, the migration's service account needs read-only roles only, and
its key stays in the AIDP credential store. Only the export route adds a second,
writing account, used by `export --apply` alone.

## Roadmap

The pipeline is automated, one command per step, but not repair: `run` executes a job
and reports; a failure stops for a person, and a rerun skips the copies that finished.
There is no execute-check-fix loop, one that runs migrated code cell by cell on a live
cluster and has Claude repair the cells that fail. That is deliberate where what moves
is data and DDL, where a wrong automatic fix silently corrupts a table, and 0.2 did not
need one: its translated Dataproc jobs and notebooks ran on AIDP as written.

- 0.1: BigQuery tables, views, materialized views, scheduled and saved queries, and Cloud
  Storage.
- 0.2: BigQuery Studio notebooks and Dataproc jobs, batches and workflow templates.
- 0.2.1: source schedules carried onto AIDP jobs, PAUSED (scheduled queries and Cloud
  Scheduler jobs that start Dataproc); Dataproc job Spark properties; the export route for
  large tables (`export`). Still to come: Spark JAR jobs, once an AIDP `JAR_TASK` is proven.
- 0.3: Dataform repositories; BigQuery pipelines next.
- Later: Composer (Airflow DAGs), Dataflow and Vertex AI.
- Under consideration: an execute-check-fix pass for migrated code.
