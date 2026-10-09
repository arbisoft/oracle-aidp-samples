---
description: Upload a finished Google Cloud migration into an AIDP workspace (dry run by default)
argument-hint: "<migrated/> [--prefix <name>] [--apply]"
allowed-tools: Bash(gcp-aidp publish:*), Bash(python3 -m gcp_aidp.cli publish:*)
---

```bash
gcp-aidp publish ./migrated --prefix <name>
```

`publish` and `run` are the only verbs that reach AIDP. Everything before them is
local.

Before the first `--apply`, run the `gcp-aidp-migrator-bootstrap` skill's checks.

Without `--apply` this is a dry run: it prints the folder, the notebooks it would
upload and the jobs it would create, sends nothing, and exits 0. **Always run it that
way first and show the user the list.** Only after they have read it:

```bash
gcp-aidp publish ./migrated --prefix <name> --apply
```

`--apply` needs `aidp-cli` on PATH (`pip install aidp-cli`) and an OCI profile that can
reach the instance. Settings come from flags or from `AIDP_INSTANCE_ID`,
`AIDP_WORKSPACE_KEY`, `AIDP_CLUSTER_KEY`, `AIDP_PREFIX`, `OCI_CLI_PROFILE` and
`AIDP_AUTH` in the environment or `.env`. `aidp-cli` defaults to `security_token`
auth; with an API key profile set `AIDP_AUTH=api_key` or pass `--auth api_key`. Never
write a credential into a repository file, and never echo one back.

What publish guarantees, and what to tell the user:

- **It never overwrites.** An existing notebook path or job name is skipped, so
  re-running is safe. Use a fresh `--prefix` for a second copy.
- **`--prefix` namespaces the folder and job names**, so two people publishing into
  one workspace do not collide. It must be a letter followed by letters, digits or
  underscores.
- **Jobs are created unscheduled.** The refresh job for a materialized view, and the
  job for a scheduled query, run only when someone runs or schedules them.
- **A refusal makes `--apply` exit 1**: a job with no cluster key, a cluster that is
  not the workspace's or is its Default Master, a job whose notebooks this run did not
  upload. Report a non-zero exit; do not re-run with different flags to make it green.

Publishing does not run anything. Say so, and suggest
`/oracle-ai-data-platform-workbench-gcp-migrator:run` once the user has checked the
cluster has the BigQuery connector JAR and the credential.
