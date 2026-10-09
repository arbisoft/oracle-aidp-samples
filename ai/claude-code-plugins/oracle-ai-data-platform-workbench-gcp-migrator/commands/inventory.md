---
description: Scan a Google Cloud project (read-only, metadata only) and write an AIDP migration manifest
argument-hint: "[--project <id> | --fixture demo] [--sources bigquery,gcs] [--saved-queries-dir <dir>]"
allowed-tools: Bash(gcp-aidp inventory:*), Bash(python3 -m gcp_aidp.cli inventory:*)
---

Run a read-only Google Cloud inventory and write a manifest. Pass through the user's
arguments:

`gcp-aidp inventory $ARGUMENTS`

If no output path is given, add `-o inv.json`. If the user has no service account key
(`GOOGLE_APPLICATION_CREDENTIALS`), use `--fixture demo` instead of `--project`. Never
read or print the key.

After it runs, summarize the counts per source and **everything recorded as not
scanned** (saved queries without `--saved-queries-dir`, refused API calls, services
skipped without `--scan-services`). A gap is unknown, not zero. Then point the user to
`/oracle-ai-data-platform-workbench-gcp-migrator:plan`.
