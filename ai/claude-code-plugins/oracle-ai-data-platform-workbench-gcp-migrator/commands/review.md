---
description: Go through every REVIEW asset of a migration and say, per finding, accept, check or change
argument-hint: "[migrated/]"
---

Review the REVIEW assets in `$ARGUMENTS` (default `./migrated`).

Use the `gcp-migration-reviewer` agent on that directory. It writes `REVIEW.md` there,
with each caveat, flag and block sorted into Change (an edit to make), Check (one command
for the user to run) and Accepted (with the evidence).

Then give the user the counts and the Change and Check items. Do not run the Check
commands yourself: they run against Google Cloud or AIDP, and the user runs them. Do not
apply the edits unless the user asks; a re-run of `migrate` overwrites edited files.
