---
name: format-samples-connector
description: Use when adding a data-source connector sample to this repo (oracle-aidp-samples), or bringing an existing connector PR into line with the repo's conventions — "format X for the samples repo", "make the PR match the existing connectors", "convert to a single notebook". Gives the folder layout, notebook template, catalog row, dependency and credential conventions, and a checker script.
---

# format-samples-connector

## Overview

This repo has two kinds of ingestion sample:

- **Built-in connector notebooks** — `Read_Only_Ingestion_Connectors/`,
  `Read_Write_*_Connectors/`: one notebook per AIDP built-in type
  (`format("aidataplatform").option("type", ...)`), shipped by Oracle with
  product releases ("Add 4.0/4.1 connector samples").
- **Pattern samples** — `Connect_Using_Custom_JDBC_Driver.ipynb`,
  `Ingest_from_Multi_Cloud.ipynb`, `Read_excel_data/`: integrations that need
  something installed on the cluster (jars, Python packages).

A connector that is not a built-in AIDP type (REST code, a third-party
Spark connector) is contributed **as a pattern sample, written in the
built-in connector notebooks' cell style.** Putting it among Oracle's
built-in connectors would imply Oracle supports it.

**Core principle:** match the samples already on `main`, not other open PRs.
One notebook holds the connector's logic in cells, plus at most a short
README and a `requirements.txt`. A helper module with tests, or a package
driven by a YAML config, diverges from this repo even if it works.

## Reference samples (read before writing)

In `data-engineering/ingestion/` on `main`:

| For | Read |
|---|---|
| Cell style and the options table | `Read_Only_Ingestion_Connectors/Fusion_BICC.ipynb` (smallest), `Kafka.ipynb`, `REST.ipynb`; `Read_Write_External_Ecosystem_Connectors/PostgreSQL.ipynb` |
| Folder with README + requirements | `Read_excel_data/` |
| Jars installed on the cluster | `Connect_Using_Custom_JDBC_Driver.ipynb`, `Ingest_from_Multi_Cloud.ipynb` |
| Python package on the cluster | `Ingest_from_Multi_Cloud.ipynb` (boto3 section), `data-engineering/adw-iceberg-external-table-sync/` |
| Contribution rules | `CONTRIBUTING.md` (catalog row, PR process, sign-off) |

## Layout

```
data-engineering/ingestion/<Source>/
  <Source>.ipynb       required
  README.md            optional, short: what it does, prerequisites, limits
  requirements.txt     only if Prerequisites install it as a cluster library
```

`<Source>` in `Title_Snake_Case`, the same for folder and notebook
(`Jira_Cloud/Jira_Cloud.ipynb`, `MongoDB_Atlas/MongoDB_Atlas.ipynb`). This
exact layout is **proposed here**, not taken from `main`: no merged sample
uses it yet. The closest is `Read_excel_data/` (its own folder, a notebook
plus README and `requirements.txt`, but `read_excel.ipynb` named
differently); `Connect_Using_Custom_JDBC_Driver.ipynb` and
`Ingest_from_Multi_Cloud.ipynb` are single files with no folder. The same
name for folder and notebook keeps the catalog link and the checker simple.
No helper modules, `tests/`, `pytest.ini` or `requirements-dev.txt`.

## Notebook template

Cells, in order:

1. **Code** cell (not markdown, not raw), exactly:
   ```
   Oracle AI Data Platform v1.0

   Copyright © 2025, Oracle and/or its affiliates.

   Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
   ```
2. **Markdown** title and one or two sentences:
   ```
   # <Source> Connector Samples

   Read-only ingestion samples for <Source> using <the library or Spark format>. Replace all placeholders before running a sample.
   ```
3. **`## Prerequisites`** markdown, numbered: cluster libraries, the
   Credential Store entry, network access (e.g. an IP allow-list).
4. One section per scenario: a **`## <Scenario>`** markdown cell with one
   explanatory sentence, then **one code cell**. Typical: `## Configuration`,
   `## Ingestion Sample`, `## Write to Delta`, `## Incremental Load`. `###`
   sub-headings are fine under a `##` section (REST does this).
5. Last: **`## Connector Options`** markdown table, exactly these columns:
   ```
   | Parameter name | Valid values | Mandatory | Description |
   | --- | --- | --- | --- |
   ```
   For a third-party Spark connector: its real option names
   (`connection.uri`, `database`, `collection`, …). For a Python-based
   connector: the notebook's configuration variables.

**Code style** (from the built-in notebooks):
- Backslash continuation, one `.option(...)` per line, end a read with `.show()`.
- DataFrame names `<source>_df`, `<source>_df_pushdown`.
- Placeholders are `<UPPER_SNAKE_CASE>`. Never a real host, user, catalog,
  OCID, IP or token — not in a comment, not in a saved output.
- In markdown cells, no parentheses inside inline code and no inline code
  inside parentheses. The AIDP notebook renderer turned both into broken
  links with URL-encoded text: `decimal(38, >=10)` showed as
  `decimal(38,%20%3E=10)`, and a parenthesised code span showed its
  backticks as `%60` (seen 2026-10-01). Reword without the parentheses, or
  move code to a fenced block. The checker warns about it.
- Keep everything the source needs to work correctly (paging, 429 retry,
  timezone handling, a watermark): it moves into cells, compacted, not
  dropped. Drop only scaffolding the notebook has no use for (CLI wrappers,
  config-file loaders, abstraction layers). A function defined in one cell
  and used in a later one is fine when the logic needs it; say what it does
  in the markdown above.

**Metadata:** `nbformat` 4 / `nbformat_minor` 5; kernelspec
`{"display_name": "Python 3 (ipykernel)", "language": "python", "name": "python3"}`.
Every code cell `"execution_count": null, "outputs": []`.

## Catalog row

Per `CONTRIBUTING.md`: one row in the root `README.md` table **Data
Engineering — Ingestion**, linking the notebook:
```
| [<Display Name>](data-engineering/ingestion/<Source>/<Source>.ipynb) | <One sentence starting with an action verb.> |
```
Keep the table's alphabetical order.

## Dependencies

- **Jars:** Prerequisites list each jar with its Maven Central link and
  SHA-256, then: put them in a workspace folder (uploaded through the UI,
  or downloaded there by a notebook cell that checks the SHA-256) → cluster
  **Library** tab → **Install Library** → **Workspace** → select →
  **Install**, one at a time → **Actions → Restart**.
- **Python packages:** a `requirements.txt` in the sample folder, installed
  as a cluster library from the same tab, then restart. Not `%pip install` in
  the notebook — it does not survive a scheduled job
  (`adw-iceberg-external-table-sync/README.md`).
- Packages already on the cluster need no prerequisite (`requests`).
- **No runtime jar loading** (`SparkContext.addJar`, class-loader tricks).

Verified on AIDP 2026-10-01 (MongoDB live run):
- `addJar` is not enough for a Spark DataSource: the driver loaded it, every
  executor task failed with `UnknownReason`.
- Only one library change runs at a time per cluster ("ongoing operation").

## Credentials

Read secrets from the **AIDP Credential Store**, with a placeholder name:
```python
secret = aidputils.secrets.get(name="<CREDENTIAL_NAME>", key="<KEY>")
```
Prerequisites: "Create a **Secret Token** credential in the Credential Store
with key `<KEY>`." Verified on AIDP 2026-10-01: `aidputils.secrets.get(name,
key=None)` returns the whole map without `key`; notebooks cannot prompt for
input (`getpass` raises `StdinNotImplementedError`); the cluster UI had no
environment-variable setting. No OCI Vault / `oci` SDK path.

## Procedure

1. Read the reference samples.
2. Pick the scenarios from what the connector does.
3. Write the notebook from the template; move the connector's logic into the
   cells (keep what the source needs, see *Code style*).
4. Verified gotchas become one-line notes in the section that needs them or
   in Prerequisites; the longer reasoning, if any, goes in the README.
5. Optional `README.md` (short) and `requirements.txt`; delete everything
   else from the sample folder.
6. Catalog row.
7. Run the checker until it passes.
8. **Check the cell logic offline** before using a cluster: `exec` the code
   cells that hold real logic (URI building, schema handling, date
   conversion) against small fakes for `spark`/`aidputils`, outside the repo.
   A sample folder has no `tests/`, so this is the only offline check.
9. **Run the notebook on AIDP.** A live result for an earlier layout does not
   cover the rewritten notebook; never claim a PASS that did not run. Put the
   dated result (row counts, Spark/Python versions, what was not covered) in
   the PR description, and AIDP findings in the README's notes.
10. PR per `CONTRIBUTING.md`: reference an issue, explain how to validate.
    Internal PRs against `arbisoft:main` are not signed off; a PR upstream to
    Oracle needs `Signed-off-by` (OCA) on every commit.

## Checker

```bash
python3 .claude/skills/format-samples-connector/check_sample_notebook.py \
  data-engineering/ingestion/<Source>/<Source>.ipynb
```
Checks the folder layout, the UPL code cell, title, section headings, a
`## Prerequisites` section for pattern samples, the options table, metadata,
empty outputs on every code cell and placeholder-only credentials, and
warns about parentheses next to inline code in markdown. It
also accepts Oracle's built-in layout, and passes 17 of the 19 built-in
connector notebooks on `main` (2026-10-01); the other two are Oracle's own
slips — `DB2.ipynb` (markdown UPL cell) and `Autonomous_AI_Lakehouse.ipynb`
(`"PASSWORD"` without angle brackets) — don't copy them.

## Common mistakes

- Placing a non-built-in connector in `Read_Only_Ingestion_Connectors/`.
- Dropping source-required logic (retry, paging) to make cells shorter.
- Markdown or raw UPL cell instead of a **code** cell; © 2026 instead of 2025.
- Keeping `tests/` or a helper module in the sample folder.
- Literal secrets, real catalog/host names, or saved outputs left from a live run.
- Claiming the rewritten notebook is verified because an earlier layout passed.
