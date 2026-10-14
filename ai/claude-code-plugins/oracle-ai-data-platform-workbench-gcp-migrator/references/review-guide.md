# Reviewing a REVIEW asset

`verify` grades an asset **REVIEW** when its report row carries a caveat, a flag or a
block. Each finding names a rule; this page says, per rule, what to check and when the
asset is done. The `gcp-migration-reviewer` agent (`/review`) works through the same
checks and writes `migrated/REVIEW.md`.

| Severity | Meaning | You |
|---|---|---|
| **caveat** | Migrated and runs; it can differ from BigQuery in a known, narrow case | Check whether the case applies to your data. Usually it does not: accept it |
| **flag** | Migrated, with something the migrator could not do or confirm | Do that step by hand |
| **block** | Not migrated; the file holds the original, commented out | Rewrite it, drop it, or re-plan with the option the finding names |
| info, rewrite | What was changed | Nothing |

In a notebook finding, `cell N` counts the cells of the BigQuery Studio notebook. The migrated
notebook adds a status cell at the top and drops `%load_ext` cells, so find the cell by its code.

Edits go in the file the report names under `migrated/`, before `publish`. A re-run of
`migrate` overwrites them, so keep a copy of anything you change. Jobs are created with
their schedules PAUSED: an asset can be published while its review is open, and turned on
once it is done.

## Types (tables)

| Rule | Check | Done when |
|---|---|---|
| `TY09_BIGNUMERIC` (caveat) | Carried as exact decimal text. Do queries do arithmetic, `SUM`, or sort on the column? | No: accept. Yes: `CAST(col AS DECIMAL(38, s))` where it is used, if the values fit 38 digits |
| `TY10_DATETIME` | Carried as TIMESTAMP, read through the session time zone: anyone querying the table sees it shifted unless the session is UTC | The AIDP cluster's `spark.sql.session.timeZone` is `UTC`. The copy job sets UTC for itself only, so this is never settled by the files |
| `TY11_TIME` (flag) | Carried as `'HH:MM:SS[.ffffff]'` text | Comparing two such strings orders correctly; time arithmetic on it needs a rewrite. Accept if the column is only read or compared |
| `TY14_JSON` | Carried as JSON text | Queries that read fields use `get_json_object` / `from_json`; views doing it are flagged by the SQL rules |
| `TY15_GEOGRAPHY` | Carried as WKT text | Nothing spatial runs on AIDP: accept if the column is only displayed or passed on |
| `TY16_INTERVAL` | Carried as BigQuery's interval text | Date arithmetic with it needs a rewrite; accept if it is only read |
| `TY17_RANGE` | Carried as `STRUCT<start, end>`; `end` NULL for unbounded | Range functions (`RANGE_CONTAINS`, ...) become comparisons on the two fields |
| `X01_EXPORT_ROUTE` (flag) | The table goes through Parquet files | `gcp-aidp export --apply` ran before the copy job, and `MIGRATION_REPORT.md` shows the table `MIGRATED_VERIFIED`. Before the run, it is a check on that report |
| `D03_INGESTION_TIME` (flag) | `_PARTITIONTIME` / `_PARTITIONDATE` are ordinary columns now | Nothing writes new rows on AIDP, or what does sets them |

## SQL (views, scheduled queries, routines)

| Rule | Check | Done when |
|---|---|---|
| `G01_REFERENCE` (flag) | A table outside the plan, or in another project | It is migrated by another plan, or you point the reference at a table that exists on AIDP |
| `G03_SAFE_DIVIDE` (caveat) | Are both operands INT64 or FLOAT64? (column types: `plan.json`) | Yes: exact, accept. NUMERIC: Spark keeps 6 decimals, not 9 |
| `G06_TIMESTAMP_TRUNC` | caveat: truncates in the session time zone | Cluster session time zone is `UTC` |
| | flag: a time zone or `WEEK(<day>)` part | Rewrite: BigQuery weeks start Sunday, Spark's Monday |
| `G07_DATE_DIFF` (flag) | A part other than DAY | Rewrite with `months_between` or arithmetic on `datediff` |
| `G08_FORMAT_DATE` | caveat: years before 1000 format differently | Accept unless the data has such dates |
| | flag: pattern outside the proven set | Rewrite with a `date_format` pattern and compare a few rows |
| `G14_STAR_MODIFIER` (flag) | `SELECT * EXCEPT/REPLACE` | List the columns |
| `G15_QUALIFY` (caveat) | Its `ORDER BY` now sorts the subquery's output | The `ORDER BY` names only selected columns |
| `G16_PSEUDO_COLUMN` (flag) | `_PARTITIONTIME` read as a column | As `D03` |
| `G19_CAST_TYPE` (flag) | A cast to a type with no Spark equal | Rewrite the cast for the carried type (see Types) |
| `G21_GCS_PATH` (flag) | `gs://` bucket not in the bucket map | Add it to `--bucket-mapping` and re-plan, or edit the path |
| `G22_SAME_NAME` (flag) | Spark has the name, not the meaning (`SPLIT` is a regex, `DATE_TRUNC` returns TIMESTAMP) | Run it on a few rows on AIDP and compare with BigQuery |
| `G23_QUERY_PARAMETER` (flag) | `@param` in the query | Pass it as an AIDP job parameter |
| `G24_LITERAL` (flag) | `b'..'` or `'''..'''` | Rewrite as `X'..'` or a single-quoted string |
| `G02_SAFE_CAST` (flag) | `try_cast` parses strings differently (`'yes'`, `'1'` → TRUE; `'0x1A'` → NULL) | The input never holds such strings, or rewrite with a `CASE` |
| `G05_GENERATE_ARRAY` (flag) | `sequence(5, 1)` counts down; `GENERATE_ARRAY(5, 1)` is empty | Start is never above end, or guard it |
| `G08_PARSE_DATE` (flag) | `to_date` is strict about digit counts | The strings always have the full digit counts the pattern names |
| `G09_ARRAY_LENGTH` (flag) | `size(NULL)` is -1, `ARRAY_LENGTH(NULL)` is NULL | The array is never NULL, or `IF(a IS NULL, NULL, size(a))` |
| `G10_REGEXP` (flag) | RE2 vs Java regex; `\1` vs `$1`; `regexp_extract` defaults to group 1 | Run the pattern on a few rows on both sides and compare |
| `G11_JSON` (flag) | `get_json_object` path syntax and result types | Rewrite the path (`$.a.b`), compare a few rows |
| `G12_STRING_AGG` (flag) | No ordered string aggregate | `array_join(array_sort(collect_list(..)), sep)` when order matters, else `concat_ws(sep, collect_list(..))` |
| `G13_UNNEST` (flag) | `UNNEST` becomes `explode` / `LATERAL VIEW`; the shape depends on the query | Rewrite it and compare the row count |
| `G90_NOT_SPARK_BUILTIN` (flag) | A function Spark 3.5 does not have | Rewrite it, or a UDF |
| `D06_SQL_FUNCTION` (flag) | A SQL function that is not arithmetic | Register a Python UDF, or inline the expression in its callers |
| `D08_PROCEDURE` (flag) | Argument modes missing | Run `inventory` again |
| `J02_DESTINATION_TABLE` (flag) | The scheduled query wrote into a table: no job made | Add the `INSERT INTO` / `INSERT OVERWRITE` and create the job |
| `J01_UNSCHEDULED` (flag) | The schedule did not translate | Set a schedule on the AIDP job |
| `J03_SCHEDULE` (caveat) | Placed by the next run time | The first run on AIDP lands when the source's would |

## External tables and buckets

| Rule | Check | Done when |
|---|---|---|
| `GS01_BUCKET_MAP` (flag) | The bucket is not in the bucket map | Add it and re-plan |
| `GS02_LOCATION` (caveat) | Spark reads every file in the folder, not only BigQuery's pattern | `gcloud storage ls <folder>` lists data files only |
| `GS03_TRANSFER` (flag) | The files must be on OCI | `gcp-aidp transfer --apply` ran; `01_structure` then creates the table |
| `D05_EXTERNAL_FORMAT` | caveat: no column list | Compare `DESCRIBE` on AIDP with BigQuery's schema |
| | flag: CSV header or options | Set `header` / the named options in the `CREATE TABLE` and compare a count |

## Dataproc jobs

| Rule | Check | Done when |
|---|---|---|
| `P06_HIVE_NAME` (caveat) | On Dataproc the name resolved through the Hive metastore | `DESCRIBE FORMATTED <name>` on the cluster says not found, or names the BigQuery table: the mapping is right. A Hive table with a `Location`: migrate its files and point the job at it |
| `P06_HIVE_NAME` (flag) | A Hive table not in the plan, or an f-string name | Migrate it separately; check the built names by hand |
| `P01_GCS_PATH` (flag) | `gs://` bucket not in the bucket map | As `G21` |
| `P02_BQ_READ`, `P03_BQ_QUERY_READ` (flag) | A connector read not rewritten | `spark.table(...)` or `spark.sql(...)` on the migrated tables |
| `P04_BQ_WRITE` (flag) | A write to BigQuery | `saveAsTable` / `insertInto` the migrated Delta table; mind overwrite replacing the schema |
| `P07_GOOGLE_LIBRARY` (flag) | Imports a Google Cloud library | Replace it with Spark or the OCI SDK |
| `P08_SQL_VARIABLE` (flag) | `${variables}` | Pass them as job parameters or `SET` them first |
| `P09_LOCAL_FILES` (flag) | Files Dataproc staged beside the job | Upload them to the workspace or add them as cluster libraries |
| `P10_TEMPLATE_PARAMETER` (flag) | Template parameters | Pass the values as task arguments |
| `P11_SPARK_PROPERTY` (flag) | A property a job cannot set | Set it in the AIDP cluster's Spark configuration if needed |

## Notebooks

| Rule | Check | Done when |
|---|---|---|
| `N01_BIGQUERY_MAGIC` (flag) | `%%bigquery` on a table not in the plan | Point it at a migrated table |
| `N02_MAGIC_OPTION` (flag) | A magic option with no AIDP meaning (`--params`, `--project`) | Rewrite with `spark.sql(sql, args={...})` |
| `N03_NOTEBOOK_COMMAND` (flag) | `!pip`, `%load_ext`: commented out | Install the library on the AIDP cluster, if still needed |
| `N04_SQL_CELL` (flag) | A SQL cell on an engine not read | Rewrite the cell |
