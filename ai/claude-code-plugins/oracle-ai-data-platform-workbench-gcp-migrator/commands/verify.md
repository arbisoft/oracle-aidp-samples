---
description: Classify a Google Cloud migration's output as PASS / REVIEW / SKIP / FAIL
argument-hint: "<migrated/ or report.json>"
allowed-tools: Bash(gcp-aidp verify:*), Bash(python3 -m gcp_aidp.cli verify:*)
---

Classify the outcome of a migration:

`gcp-aidp verify $ARGUMENTS`

Report the PASS / REVIEW / SKIP / FAIL counts and the per-asset list. Explain each:
PASS = translated with no known issue detected; REVIEW = a caveat or flag needs a
human, or the asset is blocked; SKIP = reported only, or planned for a later version;
FAIL = the migrator failed or the report contradicts itself. A non-zero FAIL count is
the only hard failure.

Say plainly that PASS is **not execution-verified**: nothing parses or runs the
artifacts, so a construct no rule covers is reported clean. Do not describe a PASS
asset as "runnable" or "ready". If the user wants it on AIDP, suggest
`/oracle-ai-data-platform-workbench-gcp-migrator:publish`.
