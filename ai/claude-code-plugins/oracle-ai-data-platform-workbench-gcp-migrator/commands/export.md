---
description: Export the plan's large BigQuery tables to Parquet and copy them to OCI (dry run by default)
argument-hint: "<migrated/> [--apply]"
allowed-tools: Bash(gcp-aidp export:*), Bash(python3 -m gcp_aidp.cli export:*)
---

```bash
gcp-aidp export ./migrated
```

Only for a migration planned with `--export-over-gb`. `migrate` records the tables in
`report.json`; with none, the command says so and there is nothing to do.

Without `--apply` this is a dry run: it lists each table, its GCS and OCI folders, and
whether it goes by `bq extract` (free) or an `EXPORT DATA` query (billed for the bytes it
scans; it turns `JSON`, `TIME`, `BIGNUMERIC` and `GEOGRAPHY` into text). **Always run it
that way first and show the user the list, including the billed tables.** Only after they
have read it:

```bash
gcp-aidp export ./migrated --apply
```

`--apply` needs:

- `GCP_EXPORT_CREDENTIALS`: the key file of the **export** service account (BigQuery Job
  User and Data Viewer on the project, Storage Object Creator on the staging bucket). The
  migration's read-only key is never used to write; the command refuses without this one.
- `GOOGLE_APPLICATION_CREDENTIALS` (the read-only key) with Storage Object Viewer on the
  bucket: the copy reads the files with it.
- `~/.oci/config` (`OCI_CLI_PROFILE`, `OCI_REGION`) that can write to the OCI bucket, and
  `pip install -e '.[gcp,oci]'`.

Never print or copy a key. What to tell the user:

- **It never overwrites.** A table whose GCS folder already holds files is not exported
  again; a file already in OCI with the same MD5 is not copied again. A re-run resumes.
- **OCI checks every file's MD5 on upload.**
- A failed table is listed with BigQuery's reason and the command exits 1. Report it; do
  not re-run with other settings to make it green.

When every table is on OCI, suggest `/oracle-ai-data-platform-workbench-gcp-migrator:publish`
and `run`: the copy job loads these files and checks the counts against BigQuery.
Steps and permissions: `references/export-route.md`.
