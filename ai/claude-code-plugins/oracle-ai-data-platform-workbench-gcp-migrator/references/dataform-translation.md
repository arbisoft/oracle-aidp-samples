# Dataform → AIDP job

Source: `gcp_aidp/inventory/dataform.py` (reads), `gcp_aidp/translate/dataform.py`
(translates), `gcp_aidp/migrate/runner.py` (writes). Every rule has a test in
`tests/test_dataform.py`.

Each Dataform repository becomes one AIDP job, created unscheduled. Each compiled
action that runs becomes one task, and Dataform's dependencies become the tasks'
`dependsOn`. A task runs the translated SQL in its own notebook under
`notebooks/jobs/`. The SQL itself goes through the GoogleSQL translator, so its
rules (`G01`, `G03`, ...) apply unchanged; see `dialect-translation.md`.

The governing rule is the same: **never approximate, and never create part of a
job.** If any action of a repository has a finding of severity flag or block, the
job is not created. The repository is reported with the reasons, and its `.sql`
file under `dataform/` keeps what was translated for review. Caveats and info
findings do not stop the job.

## What is read

Read-only `GET` calls on the Dataform v1 API, per region: the repositories, each
one's release configs and workflow configs, and the compilation result to use. That
is the first enabled release config's `releaseCompilationResult`, otherwise the
newest compilation result by `createTime`. Its actions come from
`compilationResults.query`, following `nextPageToken`. The migrator never creates a
compilation result and never starts a workflow.

Per action it keeps the target, the file path, and the fields of the relation,
operation or assertion that the rules below use. Schedules (name, cron, time zone,
disabled) are kept as text and written into the job's description. They are never
applied.

A repository whose actions cannot be read is still listed, with no actions, and
`inventory` records `actions of <repository>` under *not scanned* with the reason:
no compilation result, compilation errors, or a refused call. The plan shows it at
the top, and `migrate` reports the repository as blocked (`DF14_NOT_SCANNED`)
rather than as an empty success.

## Rules

| Rule | Dataform construct | AIDP result | Severity | Why |
|---|---|---|---|---|
| `DF01_TABLE` | `relationType: TABLE` | `CREATE TABLE IF NOT EXISTS ... USING DELTA AS <query>`, then `INSERT OVERWRITE TABLE ... <query>` | rewrite | The same pair the materialized view refresh uses. Dataform rebuilds the table on every run; this does too, without `DROP` or `CREATE OR REPLACE`. The first run computes the query twice. An existing table keeps its columns, so a query that changes them fails the `INSERT OVERWRITE` instead of replacing the table |
| `DF02_VIEW` | `relationType: VIEW` | `CREATE VIEW IF NOT EXISTS ... AS <query>` | rewrite | As for BigQuery views. An existing view is left alone, so a changed query does not replace it |
| `DF03_ASSERTION` | an assertion action | a task that runs the translated query and raises if it returns any row | rewrite | Dataform assertions pass when their query returns no rows. The task fails the same way; nothing is created |
| `DF04_INCREMENTAL` | `relationType: INCREMENTAL_TABLE` | no task; the job is not created | flag | First-run behaviour and the merge on `uniqueKeyParts` are not translated yet. A guessed `MERGE` could duplicate or drop rows |
| `DF05_RELATION_TYPE` | `MATERIALIZED_VIEW`, `EXTERNAL`, `SNAPSHOT`, or a type this version does not know | no task; the job is not created | flag | No rule says what each means on AIDP |
| `DF06_OPERATIONS` | custom operations, or `preOperations` / `postOperations` on a relation | no task; the job is not created | flag | They are SQL scripts (`DECLARE`, several statements, DML), which are not translated (`G17_SCRIPTING`). The original is kept as written |
| `DF07_DECLARATION` | a declaration | no task | info | An external source. Queries read it as an existing table, and `G01_REFERENCE` flags the name if the plan does not hold it |
| `DF08_DISABLED` | an action with `disabled: true` | no task | info | It does not run in Dataform either. A table it would have created is not a name the SQL can resolve |
| `DF09_NOT_TRANSLATED_ACTION` | a notebook, a data preparation, or an action of an unknown type | no task; the job is not created | flag | Neither is translated in this version |
| `DF10_CYCLE` | actions that depend on each other in a loop | the job is not created | block | There is no order to run them in. The detail names the cycle |
| `DF11_CROSS_PROJECT` | an action whose target database is not the source project | the job is not created | flag | It would land in the plan's catalog, which is not where it was. Decide where it belongs |
| `DF12_DEPENDENCY_OUTSIDE` | a dependency that is not an action of the repository and not a declaration | no `dependsOn` entry | info | Read as an existing table. `G01_REFERENCE` flags it if the plan does not hold it |
| `DF13_LAYOUT` | `partitionExpression`, `clusterExpressions` | the table is created unpartitioned | info | Queries are unaffected; only the physical layout differs. Add `PARTITIONED BY` or `CLUSTER BY` by hand if it matters |
| `DF14_NOT_SCANNED` | a repository whose actions could not be read | the repository is blocked; no job | block | Reported with the reason rather than as an empty success |
| `DF15_NO_TASKS` | a repository where nothing runs (empty, only declarations, or all disabled) | no job | info | A job needs at least one task |

## Names and order

- A target is `<plan catalog>.<Dataform schema>.<Dataform name>`. The schema must
  exist on AIDP: it does when its BigQuery dataset is migrated.
- A task key is the job-name form of `<schema>_<name>`, made unique inside the job
  (`_2`, `_3`, ...). Tasks are written dependencies first.
- A `dependencyTarget` resolves to a task through its schema and name inside the
  repository. A dependency on an action that has no task (a declaration or a
  disabled action) adds no `dependsOn`.
- The names a query references resolve through the plan: the tables and views that
  the repository's non-disabled actions create are registered like any other
  relation, so a view selecting from another action's table is rewritten to its
  AIDP name (`G01_REFERENCE`).

## Not carried

- Schedules (release and workflow configs): recorded as text, never applied. The
  job is created unscheduled.
- Tags and descriptions.
- The incremental table behaviour (`DF04`) and the other cases flagged above.

## Checked offline only

The reads are tested against canned API responses, and the generated statements
against Spark 3.5 with Delta when `pyspark` is installed. Nothing here has run
against a real Dataform repository or on AIDP.
