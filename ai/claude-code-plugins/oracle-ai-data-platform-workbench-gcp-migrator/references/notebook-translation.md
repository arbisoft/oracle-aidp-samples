# BigQuery Studio notebooks → AIDP notebooks

Source: `gcp_aidp/translate/notebook_to_aidp.py`. Every rule has a test in
`tests/test_bq_notebooks.py`.

BigQuery Studio notebooks have no read API this tool uses: export them as `.ipynb` and
pass `inventory --notebooks-dir <folder>`. The inventory keeps each cell's source and
drops its outputs, which can hold query results. A notebook listed without content stays
in the plan as `SKIP`, with the reason.

Each cell is translated on its own. A `%%bigquery` cell goes through the GoogleSQL
translator (`references/dialect-translation.md`) and carries its findings; a Python cell
goes through the Dataproc translator (`references/dataproc-translation.md`): `gs://`
paths, BigQuery connector reads, and Google client libraries such as `bigframes` and
`google.cloud` flagged. Markdown cells are kept as they are.

## Rules

| Rule | BigQuery Studio | AIDP | Type |
|---|---|---|---|
| `N01_BIGQUERY_MAGIC` | `%%bigquery [var]` and a GoogleSQL query | `var = spark.sql("""<translated>""").toPandas()`; without a variable, the expression alone, so the result is shown | rewrite, with the query's own findings |
| `N01_BIGQUERY_MAGIC` | `%%bigquery [var]` and a table id | `spark.table("<AIDP name>").toPandas()`; a table not in the plan is flagged | rewrite |
| `N02_MAGIC_OPTION` | `--params`, `--destination_table`, `--graph`, `--engine`, `--project` of another project | left; Spark 3.5 takes named parameters as `:name` with `spark.sql(query, args={...})` | flag |
| `N02_MAGIC_OPTION` | `--use_legacy_sql`, an unreadable magic line | not translated | **block** |
| `N03_NOTEBOOK_COMMAND` | `%load_ext bigquery_magics`, `%load_ext google.cloud.bigquery` | removed | rewrite |
| `N03_NOTEBOOK_COMMAND` | other `%` and `!` lines (`!pip install ...`) | commented out: install libraries on the AIDP cluster | flag |
| `N03_NOTEBOOK_COMMAND` | another cell magic (`%%bash`, `%%writefile`) | not translated | **block** |

`.toPandas()` keeps what the magic returns: a pandas DataFrame holding the whole result.
`--verbose`, `--no_query_cache`, `--use_rest_api`, `--use_bqstorage_api`, `--progress_bar_type`,
`--location` and `--max_results` change only how a result is fetched or shown, and are
dropped.

## A blocked cell

The cell keeps its original text, commented out, and the whole notebook is blocked: it is
written to `migrated/bq_notebooks/` for a person to finish, and `publish` does not
upload it. A notebook without a blocked cell is uploaded beside the other notebooks; no
job is created for it.
