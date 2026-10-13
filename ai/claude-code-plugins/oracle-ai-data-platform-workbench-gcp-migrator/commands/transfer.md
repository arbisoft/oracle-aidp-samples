---
description: Copy the migration's Cloud Storage buckets to OCI Object Storage (dry run by default)
argument-hint: "<migrated/> [--apply] [--bucket <gcs-bucket>]"
allowed-tools: Bash(gcp-aidp transfer:*), Bash(python3 -m gcp_aidp.cli transfer:*)
---

```bash
gcp-aidp transfer ./migrated
```

`migrate` writes one `transfers/<bucket>.transfer.json` per Cloud Storage bucket in the
plan (`GS03_TRANSFER`): which OCI bucket it lands in. With none, the command says so.

Without `--apply` this is a dry run: per bucket, how many files and MB are still to copy
and how many are already in OCI. It reads both sides and sends nothing. **Always run it
that way first and show the user the list.** Skip buckets they do not need (Dataproc's
`dataproc-staging-*` and `dataproc-temp-*` scratch buckets, say) with `--bucket`, once
per bucket to keep. Only after they have read it:

```bash
gcp-aidp transfer ./migrated --apply [--bucket <gcs-bucket> ...]
```

It needs:

- `GOOGLE_APPLICATION_CREDENTIALS` (the read-only key) with Storage Object Viewer on the
  buckets. Nothing is written on Google Cloud.
- `~/.oci/config` (`OCI_CLI_PROFILE`, `OCI_REGION`) that can write objects to the OCI
  buckets, which must exist: the command never creates one. `pip install -e '.[gcp,oci]'`.

Never print or copy a key. What to tell the user:

- **It never deletes or overwrites.** A file already in OCI with the same MD5 is skipped,
  so a re-run resumes; OCI checks each file's MD5 on upload. A composite object (no MD5 in
  Cloud Storage) is compared by size, and the summary says how many.
- It shows its progress every 10 seconds. The speed is this machine's network: every byte
  is downloaded and uploaded again.
- A bucket that fails (an OCI bucket that does not exist, a refused read) is listed with
  the reason and the command exits 1; the other buckets still copy. The record is in
  `transfers/transfer_report.json`.

External tables read their files from OCI: run this before `run` creates them
(`01_structure` with `external-tables=True`). Then suggest
`/oracle-ai-data-platform-workbench-gcp-migrator:publish`.
