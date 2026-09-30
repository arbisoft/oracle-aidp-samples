# Jira Cloud connector: live AIDP test results

## Row 1 — 2026-09-30 — PASS

Ran end to end on a live AIDP workspace, following `LIVE_TEST_GUIDE.md`.

**Checks (`LIVE_TEST_GUIDE.md` Step 4):**
- Row count matched the Jira UI's issue count for the project. **PASS.**
- No-duplicates check (`count(*)` vs `count(DISTINCT key)` after a small
  `PAGE_SIZE` forcing many pages): equal. **PASS.**
- Incremental re-run (edited one issue in the Jira UI, reran the notebook):
  only that issue's row updated; target row count unchanged. **PASS.**
- No errors, no credential or hostname printed. **PASS.**

**Not captured this run** (reported as "works end to end" without these
specifics — record as unverified rather than guessed):
- AIDP cluster runtime versions (Spark/Python/Java) — not recorded.
- Credential method (plain environment variables vs. OCI Vault) — not
  recorded, so the OCI Vault credential path (`aidp_secrets.py`) remains
  unverified regardless of this PASS.

See `row1.json` for the structured record.

**Definition of done:** this row satisfies the "executed against a real
endpoint from a live AIDP workspace" and "recorded with a dated PASS"
criteria in `docs/specs/2026-09-28-oracle-aidp-connectors-design.md`. The
Jira Cloud connector can now be listed as supported in the root README.
