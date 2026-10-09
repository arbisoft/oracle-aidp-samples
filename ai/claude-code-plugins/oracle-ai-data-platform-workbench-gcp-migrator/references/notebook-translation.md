# BigQuery Studio notebooks → AIDP notebooks

Source: `gcp_aidp/translate/notebook_to_aidp.py`. Every rule has a test in
`tests/test_bq_notebooks.py`.

`inventory --scan-services` reads BigQuery Studio notebooks through the Dataform API (GET
calls; the service account needs Dataform Viewer). A notebook is a Dataform repository
labelled `single-file-asset-type: notebook` holding `content.ipynb`; it lands in whatever
region BigQuery Studio chose, so every Dataform location is listed. Any other single-file
asset is named in a warning. Without `--scan-services`, export notebooks as `.ipynb` and pass
`--notebooks-dir <folder>`. Either way the inventory keeps each cell's source and drops
its outputs, which can hold query results. A notebook listed without content stays in the
plan as `SKIP`, with the reason.

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
| `N04_SQL_CELL` | a BigQuery Studio SQL cell: generated code with `# sql_engine: bigquery`, the query in `_sql = """..."""` and `google.colab.sql` running it | `<output_variable> = spark.sql("""<translated>""").toPandas()`, then the variable, as the cell showed it | rewrite, with the query's own findings |
| `N04_SQL_CELL` | a SQL cell on another engine, or one whose query cannot be read | left as written | flag |

Any other code importing `google.colab` is flagged (`P07`), so a Colab-generated cell never
passes silently.

`.toPandas()` keeps what the magic returns: a pandas DataFrame holding the whole result.
`--verbose`, `--no_query_cache`, `--use_rest_api`, `--use_bqstorage_api`, `--progress_bar_type`,
`--location` and `--max_results` change only how a result is fetched or shown, and are
dropped.

## A blocked cell

The cell keeps its original text, commented out, and the whole notebook is blocked: it is
written to `migrated/bq_notebooks/` for a person to finish, and `publish` does not
upload it. A notebook without a blocked cell is uploaded beside the other notebooks; no
job is created for it.
