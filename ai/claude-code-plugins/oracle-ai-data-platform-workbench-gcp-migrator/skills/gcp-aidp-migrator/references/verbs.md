# `gcp-aidp` verb reference

Every verb also runs as `python3 -m gcp_aidp.cli <verb>`. Values in `.env` are read
from the working directory, then the plugin folder; the shell overrides both. Only
`GCP_`, `GOOGLE_APPLICATION_CREDENTIALS`, `AIDP_` and `OCI_` keys are taken.

## inventory: read-only scan → manifest
```
gcp-aidp inventory [--project ID] [--sources bigquery,gcs,...] [--regions us-central1]
                   [--saved-queries-dir DIR] [--scan-services] [--fixture demo] [-o inv.json]
```
- `--project`: default `$GCP_PROJECT`. Auth: `GOOGLE_APPLICATION_CREDENTIALS`.
- `--sources`: subset of `bigquery,gcs,dataproc,composer,dataform,dataflow,vertex`.
- `--saved-queries-dir`: saved queries exported as `.sql` files; there is no API for them.
- `--scan-services`: also list Dataproc, Composer, Dataform, Dataflow and Vertex AI.
  Their APIs refuse a read-only token, so this uses a `cloud-platform` token for them
  alone (still GET calls only).
- `--fixture demo`: the bundled Northwind Retail estate; no Google account needed.
- Metadata only: no query runs and no bytes are billed. Exit 2 when it cannot
  authenticate.

## plan: manifest → plan + approval document
```
gcp-aidp plan <inv.json> [-o plan.json] [--catalog NAME] [--namespace OCI_NS]
              [--datasets a,b] [--bignumeric block|string] [--geography block|wkt]
```
- Writes `plan.json` and `plan.md` beside it. Actions: `MIGRATE`, `REPORT`, `SKIP`.
- `--datasets`: migrate only these BigQuery datasets (default: all). The others'
  assets stay in the plan as `SKIP` ("outside --datasets"); a name the inventory
  does not hold fails the plan.
- `--catalog`: target INTERNAL catalog (default: the project id, made a valid name).
- `--namespace`: OCI namespace for target buckets (default `$OCI_NAMESPACE`).
- `--bignumeric string` / `--geography wkt`: carry those columns as text instead of
  blocking the table.
- Halts on a name collision (case-insensitive, as Spark compares) or a duplicate id.

## migrate: plan → artifacts, locally
```
gcp-aidp migrate <plan.json> [-o migrated]
```
- One artifact per asset, `report.json` / `report.md`, the data-plane notebooks
  (`notebooks/00_diagnose` … `03_reconcile`) and job notebooks (`notebooks/jobs/`).
- Contacts nothing. Exit 1 when any asset errored.

## verify: classify the report
```
gcp-aidp verify <migrated/ | report.json>
```
- PASS = translated, no known issue detected · REVIEW = caveat, flag or block needs a
  human · SKIP = reported only or later version · FAIL = migrator error or a report
  that contradicts itself.
- PASS is **not execution-verified**.
- Exit 1 only when FAIL > 0. Exit 2, with an error and no verdicts, when the report
  is not found or `migrate` was interrupted (its in-progress marker is still there):
  re-run `migrate`.

## publish: upload to AIDP (dry run by default)
```
gcp-aidp publish <migrated/> [--prefix NAME] [--apply] [--cluster-key UUID]
                 [--workspace-key UUID] [--instance-id OCID] [--profile P] [--auth api_key]
                 [--reuse-existing-notebooks]
```
- Defaults: `$AIDP_PREFIX`, `$AIDP_CLUSTER_KEY`, `$AIDP_WORKSPACE_KEY`,
  `$AIDP_INSTANCE_ID`, `$OCI_CLI_PROFILE`, `$AIDP_AUTH`.
- Uploads to `/Workspace/<prefix>/`; jobs are named `<prefix>_<job>` and created
  unscheduled. `--prefix` must be a letter followed by letters, digits or underscores.
- Never overwrites: an existing path or job is skipped. A job whose notebooks this
  run did not upload is refused, since it could run someone else's;
  `--reuse-existing-notebooks` allows it (finishing a publish whose job creation
  failed). Those notebooks are still not overwritten.
- `--apply` checks the cluster is one of the workspace's and not its Default Master.
- Exit 1 when anything failed or was refused; exit 2 when it could not start.

## run: start a published job and follow it
```
gcp-aidp run <migrated/> [--prefix NAME] [--job gcp_aidp_migration] [--wait SECONDS]
```
- Starts `<prefix>_<job>` and polls each task until it ends (default wait 6 hours).
  Stopping the command stops the polling, not the run.
- Exit 0 when every task succeeded, 1 otherwise. A rerun skips copies that finished.
