# Dataproc → AIDP translation

Source: `gcp_aidp/translate/pyspark_to_aidp.py`. Used for Dataproc PySpark and Spark SQL
job definitions, serverless batches and workflow template steps. Every rule has a test in
`tests/test_pyspark.py`.

The code is Spark already; only what it reaches outside Spark changes. Python's `ast`
finds each construct with its exact position and only that span of text is replaced, so
the rest of the script, formatting and comments included, stays byte for byte. A result
that no longer parses is never returned: the script is blocked instead. Docstrings and
comments are never edited.

## Rewrites

| Rule | Dataproc | AIDP | Type | Notes |
|---|---|---|---|---|
| `P01_GCS_PATH` | `"gs://bucket/path"`, `f"gs://bucket/{x}/"` | `oci://<mapped bucket>@<namespace>/path` | rewrite | Through the plan's bucket map; an unmapped bucket is flagged and left |
| `P02_BQ_READ` | `spark.read.format("bigquery").load("project.dataset.table")`, or `.option("table", ...)`, `.option("dataset", ...)`, `project:dataset.table` | `spark.table("`catalog`.`schema`.`table`")` | rewrite | Only for a table in the plan, in the plan's project, named by a string literal, with no option beyond a plain table read. Anything else is flagged and left |
| `P05_CONNECTOR_SETTING` | `spark.conf.set("temporaryGcsBucket", ...)` and the other connector keys; `.config("spark.jars.packages", "...spark-bigquery...")` on the session builder | removed (a statement becomes `pass` with a comment) | rewrite | The connector has no use on AIDP |
| `P06_HIVE_NAME` | `dataset.table` in `spark.sql("...")` and in Spark SQL jobs | `` `catalog`.`schema`.`table` `` from the plan | **caveat** | On Dataproc the name resolved through the Hive metastore, which may not be BigQuery's table of that name |

## Flagged — left as written

| Rule | Dataproc | Why |
|---|---|---|
| `P02_BQ_READ` | a connector read of a table not in the plan, in another project, not a literal, or with options such as `filter` | Not provably a plain read of a migrated table |
| `P03_BQ_QUERY_READ` | `.option("query", sql)`, `.load("SELECT ...")` | A query result; run it as `spark.sql` on the migrated tables (the GoogleSQL translator can convert it) |
| `P04_BQ_WRITE` | `df.write.format("bigquery")...save()` | Write the migrated Delta table instead; the connector's `overwrite` keeps the table's schema, Delta's does not |
| `P06_HIVE_NAME` | a name not in the plan; `spark.sql(f"...")` | A Hive table to migrate separately; a name built at run time is not checked |
| `P07_GOOGLE_LIBRARY` | `google.cloud`, `google.auth`, `google.oauth2`, `gcsfs`, `pandas_gbq`, `bigframes` | AIDP has no access to Google Cloud |
| `P08_SQL_VARIABLE` | `${var}` in a Spark SQL job, or script variables | Pass them as job parameters, or `SET` them before the statements |
| `P09_LOCAL_FILES` | `__file__`; a job's `pythonFileUris`, `fileUris`, `archiveUris`, or JARs other than the BigQuery connector | An AIDP Python task defines no `__file__` and runs in a container home directory, without the files Dataproc staged beside the job |
| `P10_TEMPLATE_PARAMETER` | a workflow template with parameters | Dataproc substitutes `${PARAM}` when the template runs; on AIDP it would reach the steps as literal text |

## Blocked — not translated

| Rule | Dataproc | Why |
|---|---|---|
| `P97_NOT_TRANSLATED_TYPE` | a workflow template step of a type 0.2 does not translate (JAR, Hive, Pig, ...) | The template's job would run with a step missing, so no job is created |
| `P98_UNPARSABLE` | code that is not valid Python 3 (Python 2, for example), or a translation that would not parse | Cannot be read or changed safely |
| `P99_CODE_NOT_READ` | no code in the inventory (too large, not on Cloud Storage, refused) | Nothing to translate; the reason is in the finding |

Spark SQL jobs also use `G21_GCS_PATH` (paths in string literals) from the GoogleSQL
translator; none of its dialect rules apply, since the SQL is Spark's already.

## How the jobs run on AIDP

A PySpark definition becomes a job with one `PYTHON_TASK` running the translated script,
uploaded as a workspace file. A Spark SQL definition becomes a notebook task that runs its
statements in order. A workflow template becomes one job: each step a task, its
prerequisites as `dependsOn`. Jobs are created unscheduled; a blocked definition, or a
template with a blocked step, gets no job.

Measured on AIDP (Spark 3.5.0, 2026-10-09): a Python task's `commandLineArguments` is
pasted as `sys.argv = <value>`, so it is written as a Python list, and AIDP adds no
script name, so the list starts with the script's own path, as on Dataproc. The script
runs as `__main__`, `spark` is already defined (a script's own `getOrCreate()` returns
it), and `__file__` is not defined.

## Known gaps
- A connector read built across variables (`reader = spark.read.format("bigquery")`, then
  `reader.load(...)`) is not rewritten: only a read written as one chain is. It is never
  reported clean: any `format("bigquery")` no recognised read or write accounts for is
  flagged (`P02_BQ_READ`) with its line.
- `pythonFileUris` (extra modules) are recorded, not read or translated.
