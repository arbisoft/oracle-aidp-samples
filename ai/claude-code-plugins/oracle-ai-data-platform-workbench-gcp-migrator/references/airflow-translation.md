# Composer DAG → AIDP job

Source: `gcp_aidp/inventory/composer.py` (reads), `gcp_aidp/translate/airflow_dag.py`
(translates), `gcp_aidp/migrate/runner.py` (writes). Every rule has a test in
`tests/test_airflow_dag.py`.

Each DAG file becomes one AIDP job, created unscheduled. Each Airflow task becomes
one task of the job, and its upstream tasks become the task's `dependsOn`. A task
runs in its own notebook under `notebooks/jobs/`. The SQL of a BigQuery task goes
through the GoogleSQL translator, so its rules (`G01`, `G03`, ...) apply unchanged;
see `dialect-translation.md`.

The governing rule is the same as for Dataform: **never approximate, and never
create part of a job.** If any task or the DAG itself has a finding of severity flag
or block, the job is not created. The DAG is reported with the reasons, and its
`.sql` file under `composer/` keeps what was translated for review. Caveats and info
findings do not stop the job.

## What is read and what never runs

`inventory` lists the objects under each environment's DAG folder
(`config.dagGcsPrefix`) and reads every `.py` file as text: a Cloud Storage object
read (`GET .../b/<bucket>/o/<object>?alt=media`, the object name percent-encoded).
`airflow_monitoring.py` and anything under `__pycache__` are skipped. A file over
1 MB is not read. A file that cannot be read stays in the inventory with empty code,
and `inventory` records `code of <environment>/<object>` under *not scanned* with the
reason. `migrate` then reports that DAG as blocked (`C99_NOT_SCANNED`), not as an
empty success.

The file is parsed with Python's `ast` module and is **never imported, executed or
evaluated**. Airflow does not need to be installed. Operators are recognised by
their class name and by the module they were imported from, which is resolved from
the file's own `import` statements. Nothing connects to Airflow, Composer or Google
Cloud.

The manifest and the plan hold the DAG source, because `migrate` runs offline from
the plan. Treat `inv.json` and `plan.json` as sensitive and do not commit them.
`plan.md` does not repeat the code. `report.json` and `report.md` repeat only the SQL of
translated tasks, not the DAG file.

## What is translated

- `BigQueryInsertJobOperator` whose `configuration` is a literal
  `{"query": {"query": "<literal string>", "useLegacySql": False}}` (`useLegacySql`
  may be left out), with a literal `task_id` and optionally a literal `location` and
  `project_id`. The task runs the translated SQL.
- `EmptyOperator` and `DummyOperator`: a task that does nothing, so the graph keeps
  its shape.
- Dependencies written as `a >> b`, `b << a`, `a >> [b, c]`, `[a, b] >> c`,
  `a >> b >> c`, `set_upstream`, `set_downstream`, `chain(...)`,
  `cross_downstream(...)`, and tasks inside a literal `with TaskGroup("g"):` (the
  task key gets the group id as a prefix).
- A DAG declared as `with DAG(...) as dag:` or `dag = DAG(...)` with `dag=dag` on its
  operators, with a literal `dag_id`.

Operators are accepted only from these module paths, each confirmed in the Apache
Airflow source:

| Name | Module |
|---|---|
| `BigQueryInsertJobOperator` | `airflow.providers.google.cloud.operators.bigquery` |
| `EmptyOperator` | `airflow.operators.empty` |
| `DummyOperator` | `airflow.operators.dummy` |
| `DAG` | `airflow`, `airflow.models`, `airflow.models.dag` |
| `TaskGroup` | `airflow.utils.task_group` |
| `chain`, `cross_downstream` | `airflow.models.baseoperator` |

A known class name imported from any other path (for example the old
`airflow.operators.dummy_operator`) is flagged with the path, not guessed at.

## Rules

| Rule | Construct | AIDP result | Severity | Why |
|---|---|---|---|---|
| `C01_BIGQUERY_QUERY` | `BigQueryInsertJobOperator` with a literal query job | a task that runs the translated Spark SQL | rewrite | The query text is what BigQuery would run. Findings of the SQL translator are kept as they are, so a name the plan does not hold (`G01_REFERENCE`) stops the job |
| `C02_NO_OP` | `EmptyOperator`, `DummyOperator` | a task whose notebook prints that it does nothing | rewrite | Keeps the fan-out and fan-in points of the graph |
| `C03_DEPENDENCIES` | dependencies between tasks | `dependsOn`, tasks written dependencies first | info | Edges are only taken between tasks assigned to variables or written inline |
| `C04_SCHEDULE` | `schedule`, `schedule_interval`, `start_date`, `catchup`, `max_active_runs` | recorded as text in the job description, never applied | info | A cron string is recorded as `cron ...`, a preset as `preset @daily`, a timedelta as `timedelta ...`, anything else as `expression ...`. The job is created unscheduled |
| `C05_DEFAULT_ARGS` | `default_args` (a literal dict) or a task argument among `owner`, `email`, `retries`, `retry_delay`, `email_on_failure`, `email_on_retry` | recorded, not carried | info | None of them changes what a task computes |
| `C06_RETRIES` | `retries` other than 0 | not carried: the AIDP task runs once | info | Add task retries in AIDP by hand if you need them |
| `C89_NO_DAG` | a file with no DAG (a helper module, or a DAG built by a function), or a DAG with no task | no job | info | Nothing here runs by itself. A helper that creates operators in a function is flagged `C92` instead |
| `C90_UNSUPPORTED_OPERATOR` | any other operator or sensor (Dataproc, Dataform, Cloud Storage and transfer operators, BigQuery check and table operators, Bash, Python, Branch, TriggerDagRun, ...), or a known class from a module path not listed above | no job | flag | No rule says what it means on AIDP. The finding names the operator and its import path |
| `C91_NON_LITERAL` | a task id, DAG id, SQL, configuration, location or project that is an f-string, variable, `.format`, concatenation or `Variable.get`; `**kwargs`; a dependency on a name that is not a task; a Dataset schedule | no job | flag | The value is known only when Airflow runs. The migrator does not run it |
| `C92_DYNAMIC_GRAPH` | `@task` and `@dag` functions, TaskFlow chaining, XCom and `.output`, `.expand()` and `.partial()`, operators or dependencies inside a loop, comprehension, function, conditional or `try`, `TaskGroup` used in a dependency | no job | flag | The graph is not static, so no fixed job can stand for it |
| `C93_PARSE_ERROR` | a file that does not parse | no job | block | The detail gives the line. The original is kept, commented out |
| `C94_JINJA` | `{{`, `{%` or `{#` in a translated field | no job | flag | Airflow fills the template at run time, with values that do not exist in AIDP |
| `C95_TASK_ARGUMENT` | a task or DAG argument that is not translated (`trigger_rule` other than the default, `pool`, `queue`, `depends_on_past`, `gcp_conn_id`, callbacks, ...), a `default_args` key outside the list in `C05`, positional operator arguments, a duplicate `task_id`, a task not attached to the DAG | no job | flag | Each changes how a task runs or whether it runs |
| `C96_MULTIPLE_DAGS` | more than one DAG in a file | no job | flag | One file is one job |
| `C97_CYCLE` | tasks that depend on each other in a loop | no job | block | There is no order to run them in. The detail lists each task followed by the task it waits for |
| `C98_BIGQUERY_CONFIG` | a configuration that is not a plain query job: `destinationTable`, `writeDisposition`, `createDisposition`, query parameters, labels, load, extract or copy jobs, `useLegacySql: True`, a missing query | no job | flag | The result goes somewhere, or the job is not a query. A guessed `INSERT` could duplicate or drop rows |
| `C99_NOT_SCANNED` | a file that could not be read | no job | block | Reported with the reason rather than as an empty success |

## Names and order

- The job is named `composer_<environment>_<dag file name>`, so two environments
  with a DAG of the same name get two jobs. Two assets that would land on one job
  name halt the plan.
- A task key is the job-name form of the Airflow task id, with a group id as prefix
  (`group_task`), made unique inside the job (`_2`, `_3`, ...).
- The DAG's id is the file name without `.py`, which is what `--dags` matches. Two
  files with the same name in one environment, in different folders, give the same
  asset id and halt the plan.

## Not carried

- Schedules, start dates, catchup and `max_active_runs`: recorded as text, never
  applied.
- Retries, owners, email settings and `retry_delay`.
- Tags, descriptions and documentation of the DAG.
- Everything flagged above.

## Checked offline only

The reads are tested against canned API responses, and the SQL of the demo DAG
against Spark 3.5 with Delta when `pyspark` is installed. Nothing here has run
against a real Composer environment or on AIDP.
