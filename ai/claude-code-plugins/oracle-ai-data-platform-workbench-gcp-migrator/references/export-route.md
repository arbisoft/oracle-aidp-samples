# Large tables: the export route

Source: `gcp_aidp/plan/planner.py` (`_export_route`), `gcp_aidp/export/` (the `export`
verb), `gcp_aidp/dataplane/stage_02_copy_dataset.py`. Tests: `tests/test_plan.py`
(`ExportRoute`), `tests/test_export.py`, `tests/test_dataplane_runtime.py` (tests 5–7).

By default the copy notebook reads every table through the Spark BigQuery connector. For
large tables the plan can send them another way: BigQuery writes the table as Parquet
files to a Cloud Storage bucket, `gcp-aidp export` copies the files to an OCI bucket, and
the copy notebook loads them into the Delta table. The checks, casts and modes are the
same as on the connector path, and the row count is still checked against BigQuery.

If no table reaches the threshold, nothing changes: no table takes the route and
`export` has nothing to do.

## The steps

```bash
gcp-aidp inventory --project <project> -o inv.json
gcp-aidp plan inv.json --catalog <catalog> --export-over-gb 50 --export-bucket <bucket> -o plan.json
gcp-aidp migrate plan.json -o migrated
gcp-aidp verify migrated
gcp-aidp export migrated              # dry run: the tables, folders, and which are billed
gcp-aidp export migrated --apply      # BigQuery → gs://<bucket>/gcp-aidp-export/<plan id>/ → OCI
gcp-aidp publish migrated --apply
gcp-aidp run migrated
```

| Step | Writes |
|---|---|
| `plan` | `plan.md` lists which tables take the route and which columns go as text |
| `migrate` | `report.json` records what `export` runs; the copy notebook reads each table's `oci://` folder |
| `export --apply` | the Parquet files in `gs://<bucket>/gcp-aidp-export/<plan id>/<dataset>/<table>/`, then the same folders in OCI, then `migrated/exports/export_report.json` |
| `run` | the Delta tables, and the copy and reconcile reports |

In Claude Code the `gcp-aidp-migrator` skill runs these steps, and shows the dry run of
`export` before it applies it.

`export` must finish before `run`: a table whose files are not on OCI fails its copy with a
missing-path error and nothing is written. The folder carries the plan id, so a new plan
never mixes with an earlier export, and a new plan needs a new export.

## What you choose

| Flag or setting | Meaning |
|---|---|
| `plan --export-over-gb N` | tables of at least N GB (BigQuery's `numBytes`) take the route; smaller ones stay on the connector. `0` sends every table, useful for testing |
| `plan --export-bucket B` | the GCS bucket the files are staged in |
| `plan --bucket-mapping <json>` | optional: the OCI bucket each GCS bucket lands in; without it the OCI bucket has the same name as `B` |
| `.env` `GCP_EXPORT_CREDENTIALS` | the export service account's key file |
| `.env` `OCI_CLI_PROFILE`, `OCI_REGION` | the `~/.oci/config` profile that writes to the OCI bucket, and its region |

```json
{"buckets": {"<gcs-bucket>": {"oci_bucket": "<oci-bucket>"}}, "default_namespace": "<ns>"}
```

Pick a threshold in the tens of GB. A small, day-partitioned table exports as about one
file per partition, thousands of tiny files, which take longer to copy than their size
suggests: in our test, two 7 MB tables made 5,575 files.

## Before you run it

**Google Cloud.** Two service accounts:

- the migration's **read-only** account (`GOOGLE_APPLICATION_CREDENTIALS`) also needs
  **Storage Object Viewer** on the staging bucket: `export` reads the files with it;
- an **export** account (`GCP_EXPORT_CREDENTIALS`) with **BigQuery Job User** and
  **BigQuery Data Viewer** on the project, and **Storage Object Creator** on the staging
  bucket. It is the only identity that writes on Google Cloud, and only `export --apply`
  uses it.

The staging bucket must be in the tables' location, or a multi-region holding it.

**OCI.**

- the OCI bucket, in the namespace the plan names;
- your `~/.oci/config` user can write objects to it;
- a policy so AIDP can read it. AIDP's own policy covers only buckets AIDP manages:

  ```
  allow any-user to manage objects in compartment id <compartment-ocid> where all { request.principal.type='aidataplatform', target.bucket.name='<bucket>' }
  allow any-user to read buckets in compartment id <compartment-ocid> where all { request.principal.type='aidataplatform', target.bucket.name='<bucket>' }
  ```

  For a bucket in the root compartment, `in tenancy` replaces `in compartment id …`.

**This machine.** `pip install -e '.[gcp,oci]'` (google-auth and the OCI Python SDK). No
`bq`, `gcloud` or rclone.

## What `export --apply` does

1. For each table, unless its GCS folder already holds files, it starts a BigQuery job
   as the export account, then waits for all of them.
2. For each exported table, it lists the files and copies to OCI those not already there
   with the same MD5, 32 at a time. OCI checks each file's MD5 on upload.
3. It writes `migrated/exports/export_report.json` and exits 1 if any table failed.

It never deletes anything. A re-run resumes: exported tables are not exported again,
copied files are not copied again. To export a table again, delete its GCS folder.

## How each column is exported

`bq extract` (a free extract job) writes most types as Parquet that Spark reads. Where it
cannot, `export` uses an `EXPORT DATA` query (billed for the bytes it scans) that turns
those columns into text; the copy notebook casts the text to the planned type.

| BigQuery type | Exported as | Lands as | Why |
|---|---|---|---|
| `JSON` | `TO_JSON_STRING(col)` | `STRING` | BigQuery refuses JSON in a Parquet export |
| `TIME` | `FORMAT_TIME('%H:%M:%E6S', col)` | `STRING` `HH:MM:SS.ffffff`, as on the connector path | Spark cannot read a Parquet TIME column |
| `BIGNUMERIC` | `CAST(col AS STRING)` | the planned `DECIMAL(p,s)`, or text with `--bignumeric string` | Spark cannot read a 76-digit decimal |
| `GEOGRAPHY` (with `--geography wkt`) | `ST_ASTEXT(col)` | `STRING`, WKT | no spatial type in Delta |
| `TIMESTAMP`, `DATETIME` | as is | `TIMESTAMP` | the file holds them without a zone (UTC clock time); the notebook reads them in UTC |
| everything else, `ARRAY` and `STRUCT` included | as is | the planned type | read natively |

The same holds at any depth inside a `STRUCT` or `ARRAY`: the query rebuilds the column
with only those fields turned into text, keeping every field's name and order and each
array's element order, and a `NULL` struct stays `NULL`. For example
`items ARRAY<STRUCT<sku STRING, attrs JSON>>` is exported as

```sql
ARRAY(SELECT IF(e IS NULL, NULL, STRUCT(e.`sku` AS `sku`, TO_JSON_STRING(e.`attrs`) AS `attrs`))
      FROM UNNEST(`items`) AS e WITH OFFSET AS o ORDER BY o) AS `items`
```

and the notebook's `CAST` to the planned `ARRAY<STRUCT<…>>` turns the text back field by
field.

## What is checked

Per table, `02_copy_dataset` records `files`, `export_rows` (rows in the files),
`source_rows` (BigQuery, now) and `target_rows`. It passes as `verified` only when all
three counts agree. If BigQuery changed after the export, the table is a
`count_mismatch` whose reason says so; export it again, or accept the snapshot.

## Measured

- 2026-10-12, a billed project: seven tables (20 M rows of 1.5 GB, `JSON`, `TIME`,
  `BIGNUMERIC`, `DATETIME`, arrays, structs, two day-partitioned tables) exported with the
  same jobs `export` starts (`bq extract` and `EXPORT DATA`) and copied to OCI; 5,629
  files. `export`'s listing and MD5 comparison then found all 5,629 in OCI, identical.
- The generated notebooks ran on local Spark 3.5 with Delta and the real BigQuery
  connector, over those files: all seven `verified` against BigQuery, values equal to the
  microsecond.
- Not yet: `export --apply` end to end with an export account, and the load on an AIDP
  cluster. The export probe of 2026-10-09 read and wrote Parquet on `oci://` from AIDP;
  `CONVERT TO DELTA` failed there, which is why the files are loaded rather than
  converted in place.
