# Test estate

A small BigQuery estate holding every object kind the 0.1 migrator handles, for
testing the plugin end to end against a real project. `seed.sql` creates
everything SQL can create; the steps below add the rest, then run the inventory,
the copy and the reconcile, with the result each one should give. Replace
`<project>` with your project id throughout.

## 0. Before you start

- **Billing.** The BigQuery sandbox (no billing account) runs `seed.sql`, but it
  has no scheduled queries and no Cloud Storage, so steps 2 and 3 need billing
  enabled. In the sandbox, tables and partitions also expire after 60 days, so
  `orders` and `order_items` keep only the last 60 days of thelook's data.
- **Service account** (read-only), for `gcp-aidp inventory` and the AIDP copy:
  BigQuery Data Viewer, BigQuery Read Session User and BigQuery Job User. Listing
  buckets (`storage.buckets.list`) and scheduled queries (`bigquery.transfers.get`)
  needs more than those roles; if one is missing, the inventory records that
  collection as *not scanned* and names the refused permission, so add a role
  holding it only if you need that collection. Keep the JSON key outside the
  repository and point to it from your shell:
  `export GOOGLE_APPLICATION_CREDENTIALS=/path/outside/the/repo/key.json`.

## 1. Seed

Open BigQuery Studio in `<project>`, paste `seed.sql`, and run it as one script. It
creates the dataset `migration_test`. Re-run it after pulling a newer version: it
replaces every object it creates except the materialized view, which it creates
only if missing (drop `mv_daily_orders` first to pick up a changed definition).

## 2. Scheduled query (billing required)

```bash
bq mk --transfer_config --project_id=<project> --location=US \
  --data_source=scheduled_query --target_dataset=migration_test \
  --display_name="migration_test status summary" --schedule="every 24 hours" \
  --params='{"query":"SELECT status, COUNT(*) AS n FROM migration_test.orders GROUP BY status",
             "destination_table_name_template":"status_summary_daily",
             "write_disposition":"WRITE_TRUNCATE"}'
```

Or in the console: run that query, then **Schedule** → *Create new scheduled query*.
It writes into a destination table, so `migrate` flags it (`J02_DESTINATION_TABLE`)
and creates no job for it.

## 3. Cloud Storage and an external table (billing required)

```bash
gcloud storage buckets create gs://<project>-migration-test --location=US
bq query --use_legacy_sql=false \
  "EXPORT DATA OPTIONS (uri='gs://<project>-migration-test/products/*.parquet', format='PARQUET')
   AS SELECT * FROM migration_test.products"
bq query --use_legacy_sql=false \
  "CREATE OR REPLACE EXTERNAL TABLE migration_test.ext_products
   OPTIONS (format='PARQUET', uris=['gs://<project>-migration-test/products/*.parquet'])"
```

## 4. Saved query

In BigQuery Studio, open a new query, paste
`saved_queries/Returned_orders_by_status.sql`, and **Save** it as
*Returned orders by status*. The inventory cannot read saved queries through an API
in 0.1, so pass the exported copy instead:
`--saved-queries-dir test-estate/saved_queries`.

## 5. Notebook

In BigQuery Studio, create a notebook named *migration test notebook*. The
inventory records notebooks as **not scanned** in 0.1 (they migrate in 0.2), and
the plan shows that at the top.

## 6. Run the inventory

```bash
pip install -e '.[gcp]'
export GOOGLE_APPLICATION_CREDENTIALS=/path/outside/the/repo/key.json
WORK=~/gcp-aidp-test      # outputs hold your project's metadata: keep them out of the repo
mkdir -p $WORK
gcp-aidp inventory --project <project> --saved-queries-dir test-estate/saved_queries -o $WORK/inv.json
gcp-aidp plan $WORK/inv.json --catalog gcp_migration_test --datasets migration_test -o $WORK/plan.json
```

Every call is a metadata `GET` with a read-only token: nothing is queried and
nothing is billed. Add `--scan-services` to also list Dataproc, Composer,
Dataform, Dataflow and Vertex AI (their APIs refuse a read-only token, so this
asks for a broader one; the calls are still GETs). In the sandbox they report
"API not enabled: none". `$WORK/inv.json` should list:

| Collection | Expected |
|---|---|
| datasets | `migration_test` |
| tables | `users`, `products`, `orders`, `order_items`, `customer_profiles`, `type_carried`, `type_blocked` (and `status_summary` / `status_summary_daily` once the procedure or the scheduled query has run) |
| views | `v_simple`, `v_rewritable`, `v_latest_order`, `v_blocked` |
| materialized_views | `mv_daily_orders` |
| routines | `net_price` (SQL), `parse_utm` (JAVASCRIPT), `refresh_status_summary` (PROCEDURE) |
| external_tables | `ext_products` (with billing) |
| scheduled_queries | *migration_test status summary* (with billing) |
| saved_queries | *Returned_orders_by_status* (from the folder) |
| access_policies | the dataset's IAM entries, one per role |
| gcs | `<project>-migration-test` (with billing) |
| not scanned | notebooks, pipelines (listed at the top of `plan.md`) |

## 7. Zero-byte semantic probes (optional)

```bash
python3 scripts/probe_bigquery_semantics.py --project <project>
```

Each probe is a query on literals. It is dry-run first and refused unless
BigQuery reports 0 bytes processed. It prints BigQuery's answer next to Spark
3.5's, which confirms or changes the translator rules marked † in
`references/dialect-translation.md`. This is the only step that runs a query.

## 8. Copy the data on AIDP

```bash
gcp-aidp migrate $WORK/plan.json -o $WORK/migrated
gcp-aidp verify $WORK/migrated
```

On AIDP, as in the README's **Setup**, step 2: create the standard catalog
`gcp_migration_test`, add the credential `gcp_bigquery_reader` (key
`credentials_b64`, the base64 service account key), and give a Spark 3.5 cluster
the `spark-bigquery-with-dependencies_2.12-<version>.jar` library. Then either:

- **publish and run** (with **Setup**, step 3, done):

  ```bash
  gcp-aidp publish $WORK/migrated --prefix <name>            # dry run: read the list
  gcp-aidp publish $WORK/migrated --prefix <name> --apply
  gcp-aidp run $WORK/migrated --prefix <name>
  ```

  Reports land in `/Workspace/<name>/reports/`.

- **or run the notebooks by hand:** import the four notebooks from
  `$WORK/migrated/notebooks/` into a workspace folder, attach them to the cluster,
  and run them in order: `00_diagnose`, `01_structure`, `02_copy_dataset` once per
  dataset (set `'dataset'` in the PARAMS cell; `'verify': 'counts+sums'` adds
  exact sums), then `03_reconcile` with `'counts': True`. Reports land in
  `/Workspace/gcp-aidp-migration/reports/`.

What each stage should report:

| Stage | Expected |
|---|---|
| `00_diagnose` | `diagnose: OK`, and for every column the BigQuery type, the type the connector returned and the target type |
| `01_structure` | `0 problem(s)`: the six copyable tables created (`type_blocked` is not: its types have no Delta equivalent), and `v_simple`, `v_rewritable`, `v_latest_order` created |
| `02_copy_dataset` | `verified` on every table, `0 problem(s)` |
| `03_reconcile` | `MIGRATED_VERIFIED` for every copied table; `VIEW_CREATED` for the three views; `BLOCKED` for `type_blocked` and `v_blocked`; `mv_daily_orders` `SNAPSHOT_BUILT` through `run` (its snapshot task is in the migration job), or `DEFERRED` when the notebooks are run by hand |

A second run copies nothing: tables that already hold rows show as
`PRESENT_NOT_REVERIFIED`, with their counts checked against BigQuery.

In the sandbox, `orders` and `order_items` lose partitions older than 60 days
every day. A copy that straddles that moment shows `count_mismatch`; re-run it
with `'mode': 'overwrite'`.

## Teardown

```bash
bq query --use_legacy_sql=false < test-estate/teardown.sql
bq rm --transfer_config <transfer-config-resource-name>   # from: bq ls --transfer_config --transfer_location=US
gcloud storage rm --recursive gs://<project>-migration-test
```

On AIDP, drop the catalog `gcp_migration_test` and delete the published folder and
jobs if you no longer need them.
