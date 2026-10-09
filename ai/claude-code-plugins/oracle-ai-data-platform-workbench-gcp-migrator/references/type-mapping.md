# BigQuery → Delta type mapping

Source: `gcp_aidp/translate/types.py`. Every planned column records the rule IDs
applied to it (`target.columns[].rules` in `plan.json`). Every rule has a test in
`tests/test_types.py`.

Severities: **map** (exact), **caveat** (carried, exact under the stated
condition; makes the table REVIEW), **flag** (carried, needs review), **block**
(not carried: the table is not created).

| Rule | BigQuery | Delta | Severity | Note |
|---|---|---|---|---|
| `TY01_INT64` | `INT64` / `INTEGER` | `BIGINT` | map | |
| `TY02_FLOAT64` | `FLOAT64` / `FLOAT` | `DOUBLE` | map | |
| `TY03_BOOL` | `BOOL` / `BOOLEAN` | `BOOLEAN` | map | |
| `TY04_STRING` | `STRING` | `STRING` | map | A declared `STRING(L)` length is not enforced by Delta |
| `TY05_BYTES` | `BYTES` | `BINARY` | map | |
| `TY06_DATE` | `DATE` | `DATE` | map | |
| `TY07_TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP` | map | Both are an instant in UTC |
| `TY08_NUMERIC` | `NUMERIC`, `NUMERIC(P,S)` | `DECIMAL(38,9)`, `DECIMAL(P,S)` | map | |
| `TY09_BIGNUMERIC` | `BIGNUMERIC` | none | **block** | 76 digits exceed Spark's 38. `--bignumeric string` carries exact decimal text (caveat). A parameterized `BIGNUMERIC(P,S)` with P ≤ 38 maps to `DECIMAL(P,S)` |
| `TY10_DATETIME` | `DATETIME` | `TIMESTAMP` | caveat | AIDP refuses `TIMESTAMP_NTZ` in `CREATE TABLE`, so the wall-clock value is stored as `TIMESTAMP` and read through the session time zone. Keep `spark.sql.session.timeZone=UTC` |
| `TY11_TIME` | `TIME` | `STRING` | flag | No Spark TIME type; comparison, ordering and arithmetic become string operations |
| `TY12_STRUCT` | `STRUCT` / `RECORD` | `STRUCT<...>` | map | Recursive; the column takes its worst field's severity, and a blocked field blocks the column |
| `TY13_ARRAY` | `ARRAY` / mode `REPEATED` | `ARRAY<...>` | map | |
| `TY14_JSON` | `JSON` | `STRING` | caveat | JSON text on every affected column; read with `get_json_object` / `from_json` |
| `TY15_GEOGRAPHY` | `GEOGRAPHY` | none | **block** | `--geography wkt` carries WKT text (caveat): no spatial type, index or predicate |
| `TY16_INTERVAL` | `INTERVAL` | none | **block** | |
| `TY17_RANGE` | `RANGE<T>` | none | **block** | Carry the bounds as two columns by hand |
| `TY99_UNKNOWN` | anything else | none | **block** | Never approximated |

`REQUIRED` columns are created `NOT NULL`. Column and table descriptions become
`COMMENT`s.

## What the connector delivers (confirmed on AIDP)

`00_diagnose` on an AIDP cluster (Spark 3.5.0, `spark-bigquery-with-dependencies_2.12-0.45.0.jar`)
printed the connector's Spark type for every column of the test estate. The
copy converts each to the target type above:

| BigQuery | Connector delivers | Copy converts with |
|---|---|---|
| `INT64`, `FLOAT64`, `BOOL`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP` | `bigint`, `double`, `boolean`, `string`, `binary`, `date`, `timestamp` | nothing to convert |
| `STRUCT` / `ARRAY` | `struct<...>` / `array<...>`, fields intact | nothing to convert |
| `NUMERIC(P,S)`, `BIGNUMERIC(P,S)` with P ≤ 38 | `decimal(P,S)` | nothing to convert |
| `JSON` | `string` | nothing to convert |
| `TIME` | `bigint` (microseconds since midnight) | `date_format(timestamp_micros(c), 'HH:mm:ss.SSSSSS')` |
| `DATETIME` | `string` (ISO 8601) | `CAST(c AS TIMESTAMP)` with the session time zone at UTC |

Every conversion runs with ANSI casts, so a value that does not fit or parse
fails the table instead of becoming NULL. Not yet observed: unparameterized
`BIGNUMERIC`, `GEOGRAPHY`, `INTERVAL` and `RANGE` (blocked by default, so
never read).

## Table layout (`gcp_aidp/translate/ddl.py`)

| Rule | BigQuery | AIDP |
|---|---|---|
| `D01_PARTITION` | DAY partitioning on a `DATE` column, no clustering | `PARTITIONED BY (col)`: exact |
| `D02_CLUSTER` | clustering, or any other partitioning (MONTH/YEAR/HOUR, a `TIMESTAMP`/`DATETIME` column, integer RANGE) | liquid `CLUSTER BY`, partition column first, at most 4 keys, only on `BIGINT`/`DOUBLE`/`STRING`/`DATE`/`TIMESTAMP`/`DECIMAL` columns. Delta cannot combine `PARTITIONED BY` with `CLUSTER BY`, and partitions by exact value, not by a granularity |
| `D03_INGESTION_TIME` | ingestion-time partitioning | not partitioned; flagged: no column carries `_PARTITIONTIME` |
| `D04_METADATA` | table expiration, labels | reported, not carried |
| `D05_EXTERNAL_FORMAT` | external table format | `USING PARQUET/AVRO/ORC/CSV/JSON`; schema inferred (caveat); CSV options flagged; other formats blocked |
| `D06_SQL_FUNCTION` | SQL scalar UDF | flagged: Apache Spark 3.5 cannot parse `CREATE FUNCTION ... RETURN` |
| `M01_MATERIALIZED_VIEW` | materialized view | a snapshot computed on AIDP from the migrated base tables (`CREATE TABLE ... AS`), plus an unscheduled refresh job (`INSERT OVERWRITE`). Not copied through the connector: reading a materialized view makes it write a temporary table in BigQuery |
| `J01_UNSCHEDULED` | scheduled query | an AIDP job created unscheduled; the source schedule is recorded |

Every statement is `CREATE ... IF NOT EXISTS`: nothing replaces an existing object.
