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
- `--sources`: subset of `bigquery,gcs,dataproc,dataform`.
- `--regions`: where Dataproc and Dataform are scanned (default `us-central1`). Without it the
  inventory warns that other regions were not scanned; ask the user where their jobs run.
- `--saved-queries-dir`: saved queries exported as `.sql` files, when `--scan-services` cannot read them
  through Dataform.
- BigQuery Studio notebooks: read through the Dataform API with `--scan-services` (the service
  account needs Dataform Viewer), in every Dataform location. `--notebooks-dir` reads an
  export of `.ipynb` instead. Cell sources are kept, outputs dropped.
- `--scan-services`: also list Dataproc and Dataform, and the
  Cloud Scheduler jobs that start Dataproc (Cloud Scheduler Viewer).
  Their APIs refuse a read-only token, so this uses a `cloud-platform` token for them
  alone (still GET calls only).
- `--fixture demo`: the bundled Northwind Retail estate; no Google account needed.
- Metadata only: no query runs and no bytes are billed. Exit 2 when it cannot
  authenticate.

## plan: manifest → plan + approval document
```
gcp-aidp plan <inv.json> [-o plan.json] [--catalog NAME] [--namespace OCI_NS]
              [--datasets a,b] [--dataform-repos a,b]
              [--bignumeric block|string] [--geography block|wkt]
              [--interval block|string] [--range block|struct]
              [--export-over-gb GB --export-bucket BUCKET] [--bucket-mapping JSON]
```
- Writes `plan.json` and `plan.md` beside it. Actions: `MIGRATE`, `REPORT`, `SKIP`.
- `--datasets`: migrate only these BigQuery datasets (default: all). The others'
  assets stay in the plan as `SKIP` ("outside --datasets"); a name the inventory
  does not hold fails the plan.
- `--dataform-repos`: migrate only these Dataform repositories (default: all). The
  others stay in the plan as `SKIP` ("outside --dataform-repos"); an unknown name
  fails the plan.
- `--catalog`: target INTERNAL catalog (default: the project id, made a valid name).
- `--namespace`: OCI namespace for target buckets (default `$OCI_NAMESPACE`).
- `--bignumeric string` / `--geography wkt` / `--interval string`: carry those columns as
  text instead of blocking the table; `--range struct` carries a RANGE as
  `STRUCT<start, end>`. What stays blocked is listed under *Needs a decision* in `plan.md`.
- `--export-over-gb` + `--export-bucket`: tables of at least GB gigabytes take the
  export route, and so do tables the connector cannot read (`INTERVAL`, `RANGE`, a
  `BIGNUMERIC` wider than 38 digits) whatever their size; without these flags such a
  table is blocked. The route: `gcp-aidp export` (below) puts them on OCI as Parquet, and the copy
  notebook loads them. Steps, permissions and per-type rules:
  `references/export-route.md` at the plugin root.
- `--bucket-mapping`: which OCI bucket each GCS bucket lands in, for `transfer`,
  the export route and every `gs://` → `oci://` rewrite. An unmapped bucket keeps its
  name. One namespace per plan (`--namespace`, else `default_namespace`):
  ```json
  {"buckets": {"<gcs-bucket>": {"oci_bucket": "<oci-bucket>"}}, "default_namespace": "<ns>"}
  ```
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

## export: large tables → Parquet in GCS → OCI
```
gcp-aidp export <migrated/> [--apply]
```
- Only for a plan with `--export-over-gb`; the tables are those `migrate` recorded in
  `report.json` (`export`).
- Dry run by default: each table, its folders, and `bq extract` (free) or `EXPORT DATA`
  (billed; `JSON`, `TIME`, `BIGNUMERIC`, `GEOGRAPHY` as text, also inside structs and arrays).
- `--apply`: BigQuery jobs as `GCP_EXPORT_CREDENTIALS` (required), then a copy to OCI that
  reads with the read-only key and writes with `~/.oci/config` (`OCI_CLI_PROFILE`,
  `OCI_REGION`). Skips exported folders and files already in OCI with the same MD5; OCI
  checks each MD5. Writes `exports/export_report.json`; exits 1 if a table failed.
- Needs `pip install -e '.[gcp,oci]'`.

## transfer: Cloud Storage buckets → OCI
```
gcp-aidp transfer <migrated/> [--apply] [--bucket GCS_BUCKET ...]
```
- One `transfers/<bucket>.transfer.json` per bucket in the plan, written by `migrate`
  (`GS03_TRANSFER`): which OCI bucket it lands in (`--bucket-mapping`).
- Dry run by default: per bucket, files and MB still to copy, and how many are already in
  OCI. It reads both sides and sends nothing.
- `--apply`: reads with the read-only key, writes with `~/.oci/config` (`OCI_CLI_PROFILE`,
  `OCI_REGION`); the same copy as `export` (MD5 checked by OCI, files already there
  skipped, never deletes; a composite object compared by size). The OCI bucket must
  exist. `--bucket` limits it to the named buckets. Writes
  `transfers/transfer_report.json`; exits 1 if a bucket failed.
- Needs `pip install -e '.[gcp,oci]'`.

## publish: upload to AIDP (dry run by default)
```
gcp-aidp publish <migrated/> [--prefix NAME] [--apply] [--cluster-key UUID]
                 [--workspace-key UUID] [--instance-id OCID] [--profile P] [--auth api_key]
                 [--reuse-existing-notebooks]
```
- Defaults: `$AIDP_PREFIX`, `$AIDP_CLUSTER_KEY`, `$AIDP_WORKSPACE_KEY`,
  `$AIDP_INSTANCE_ID`, `$OCI_CLI_PROFILE`, `$AIDP_AUTH`.
- Uploads to `/Workspace/<prefix>/`; jobs are named `<prefix>_<job>` and created
  unscheduled, or with their source schedule as Quartz, PAUSED. `--prefix` must be a letter followed by letters, digits or underscores.
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
