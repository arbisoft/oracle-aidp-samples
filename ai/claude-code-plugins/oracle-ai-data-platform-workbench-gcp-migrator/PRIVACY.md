# Privacy

The **Oracle AIDP migrator for Google Cloud** runs on your machine and talks only to your
own Google Cloud project, your own OCI tenancy and your own AIDP workspace. There is no
telemetry in this tool, on any code path, and nothing is sent to Oracle, Anthropic or any
other third party by the tool itself.

## What each verb contacts

| Verb | Contacts | Sends |
|---|---|---|
| `inventory` | Google Cloud APIs (BigQuery, Cloud Storage, and with `--scan-services` Dataproc, Dataform, Cloud Scheduler, Dataflow, Vertex AI) | `GET` requests only, signed with your key. It reads metadata, and the code files of Dataproc jobs and the content of BigQuery Studio notebooks. No query runs. |
| `plan`, `migrate`, `verify` | nothing | nothing: they read and write local files only |
| `export` (dry run) | nothing | nothing |
| `export --apply` | BigQuery, Cloud Storage, OCI Object Storage | BigQuery jobs that export the planned tables into your staging bucket; then those files, from your bucket to your OCI bucket. Table data moves only between your own buckets |
| `transfer` (dry run) | Cloud Storage, OCI Object Storage (object lists) | nothing |
| `transfer --apply` | Cloud Storage, OCI Object Storage | the planned buckets' files, from your Cloud Storage buckets to your OCI buckets, through this machine |
| `publish` (dry run) | nothing | nothing |
| `publish --apply` | AIDP, through the `aidp` CLI | first read-only checks (the workspace folder, existing jobs, the cluster); then the notebooks, Python files and job definitions `report.json` lists, and nothing else |
| `run` | AIDP, through the `aidp` CLI | a request to start the named job, then status reads |

The optional MCP server offers `inventory`, `plan`, `migrate` and `verify` to a local MCP
client by running the local CLI; `export`, `transfer`, `publish` and `run` are left out of it.

## Data on the AIDP cluster

The generated copy notebooks run in your AIDP workspace. They read your BigQuery tables
through the Spark BigQuery connector, or the export route's files from your OCI bucket,
and write Delta tables in your catalog. The Google key they use comes from the AIDP
credential store and is never printed.

## What it reads on your machine

- The files you name: a manifest, a plan, the `migrate` output directory, and
  `--saved-queries-dir`, `--notebooks-dir`, `--bucket-mapping` if you pass them.
- **A `.env` file**, in the working directory and then in the plugin folder. Only keys
  starting `GCP_`, `GOOGLE_APPLICATION_CREDENTIALS`, `AIDP_` or `OCI_` are taken, and only
  when the variable is not already set. The filter is deliberate: an arbitrary key there
  could set `PATH` and run code.
- The Google key files named by `GOOGLE_APPLICATION_CREDENTIALS` (read-only) and, for
  `export --apply`, `GCP_EXPORT_CREDENTIALS`. They are used to sign requests; their
  contents are never printed, logged or written anywhere.
- For `export --apply`, `transfer`, `publish` and `run`, your `~/.oci/config` profile, through the
  OCI SDK or the `aidp` CLI.

## What it writes

Files under the output paths you give: the manifest, the plan (`.json` and `.md`), the
`migrate` directory and its reports, `exports/export_report.json` and
`transfers/transfer_report.json`. Atomic writes use
a `.<name>.<pid>.<uuid>.tmp` scratch file beside the target, and `migrate` keeps a
`.gcp-aidp-migration-in-progress` marker until `report.json` is written. Nothing is
written elsewhere on your machine.

## Your responsibility

- The tool acts with the credentials you give it. Give the migration a read-only Google
  service account, a separate export account only if you use the export route, and a
  dedicated OCI identity (`docs/RUNBOOK.md`, *Setup*).
- An AI assistant driving the tool, Claude Code included, sees the command output and the
  files it reads, under that assistant's own data terms.

Questions or concerns: open a GitHub issue on the repository.
