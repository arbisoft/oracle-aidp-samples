# GoogleSQL → Spark SQL translation

Source: `gcp_aidp/translate/googlesql_to_spark.py`. Used for views, materialized
view queries, saved queries, scheduled queries and SQL function bodies. Every
rule has a test in `tests/test_googlesql.py`.

The governing rule: **never approximate.** A rewrite is applied only where Spark
returns the same result. Anything else is flagged and left as written, or blocks
the whole statement. A statement with any blocked construct is never partially
translated: the artifact keeps the original, commented out.

A lexer splits each statement into code, identifiers, string literals and
comments, so no rule edits text inside a literal or a comment.

## How the rules were checked

Every Spark-side claim below was run on **Apache Spark 3.5.9 with Java 17**, the
AIDP runtime (session time zone set as noted). The BigQuery-side behaviour comes
from the GoogleSQL reference, and was confirmed on BigQuery by
`scripts/probe_bigquery_semantics.py`, a set of zero-byte queries on literals
(see `test-estate/MANUAL_STEPS.md`), run on 2026-10-08. Rows marked † are not yet
confirmed on BigQuery.

`tests/test_spark_runtime.py` runs every generated artifact of the demo on
Spark 3.5 + Delta 3.2: each one whose findings are only rewrites and caveats
must run.

## Rewrites

| Rule | GoogleSQL | Spark SQL | Severity | Why |
|---|---|---|---|---|
| `G01_REFERENCE` | `` `project.dataset.table` ``, `dataset.table` after FROM/JOIN, `dataset.fn(` | `` `catalog`.`schema`.`table` `` from the plan | rewrite | A name the plan does not hold, or another project's, is flagged. Unquoted `a.b.c` outside FROM/JOIN is a column path and is left alone |
| `G03_SAFE_DIVIDE` | `SAFE_DIVIDE(a, b)` | `try_divide(a, b)` | **caveat** | NULL on division by zero in both. On `NUMERIC` operands Spark returns `DECIMAL(38,6)`, dropping 3 of 9 decimal places (verified) |
| `G04_COUNTIF` | `COUNTIF(c)` | `count_if(c)` | rewrite | Both count TRUE and ignore NULL (verified) |
| `G06_TIMESTAMP_TRUNC` | `TIMESTAMP_TRUNC(ts, part)` | `date_trunc('PART', ts)` | **caveat** | BigQuery truncates in UTC, Spark in the session time zone (verified: New York vs UTC differ). Exact with `spark.sql.session.timeZone=UTC`. `ISOWEEK` → `'WEEK'` (both Monday). `WEEK`/`WEEK(<day>)` (Sunday start), and any time-zone argument, are flagged |
| `G07_DATE_DIFF` | `DATE_DIFF(a, b, DAY)` | `datediff(a, b)` | rewrite | Both return a − b in days (verified). Any other part is flagged |
| `G08_FORMAT_DATE` | `FORMAT_DATE('%Y-%m-%d', d)` with only `%Y %m %d %F` and `- / : . , _ space` | `date_format(d, 'yyyy-MM-dd')` | **caveat** | Exact for years 1000–9999. Before that Spark prints `0005` and BigQuery `5` (both confirmed). Any other specifier is flagged |
| `G15_QUALIFY` | `... QUALIFY <cond>` | a subquery filtered on `<cond>`: with a window function, the row travels as `struct(<select list>)` beside `(<cond>) AS _qualify_keep` and comes out as `_qualify_row.*`; without one (`QUALIFY rn = 1`), `SELECT * FROM (<query>) WHERE <cond>` | rewrite | Spark 3.5 cannot parse `QUALIFY` (verified). The subquery evaluates the condition after `GROUP BY`/`HAVING`, as `QUALIFY` does, and keeps the same rows (`test_spark_runtime`). `DISTINCT` moves outside; a following `ORDER BY` is a **caveat**: it now sees only selected columns |
| `G19_CAST_TYPE` | `CAST(x AS INT64)` (also `FLOAT64`, `BOOL`, `BYTES`, `NUMERIC`, nested `ARRAY<STRUCT<...>>`) | `CAST(x AS BIGINT)` ... | rewrite | Spark cannot parse `INT64` (verified). `DATETIME` is a caveat; `TIME`, `JSON`, `GEOGRAPHY`, `BIGNUMERIC`, `INTERVAL` are flagged |
| `G20_HASH_COMMENT` | `# note` | `-- note` | rewrite | Spark has no `#` comment (verified) |
| `G21_GCS_PATH` | `'gs://bucket/path'` in a literal | `'oci://bucket@namespace/path'` | rewrite | Through the bucket map; an unmapped bucket is flagged |

## Flagged — left as written

| Rule | GoogleSQL | Why no rewrite |
|---|---|---|
| `G02_SAFE_CAST` | `SAFE_CAST` | `try_cast` parses strings differently: Spark reads `'yes'`/`'1'` as TRUE and `'0x1A'` as NULL (verified); BigQuery gives NULL, NULL and 26 †. Both accept `' 12 '` and refuse `'1.5'` (confirmed) |
| `G05_GENERATE_ARRAY` | `GENERATE_ARRAY(a, b)` | `sequence(5, 1)` is `[5,4,3,2,1]`; `GENERATE_ARRAY(5, 1)` is empty (both confirmed) |
| `G08_PARSE_DATE` | `PARSE_DATE` | `to_date('2026-1-5', 'yyyy-MM-dd')` raises on Spark 3.5 (verified); `PARSE_DATE` accepts single-digit months (confirmed) |
| `G09_ARRAY_LENGTH` | `ARRAY_LENGTH(a)` | `size(NULL)` is `-1` on Spark 3.5; `ARRAY_LENGTH(NULL)` is NULL (verified) |
| `G10_REGEXP` | `REGEXP_CONTAINS`, `REGEXP_EXTRACT[_ALL]`, `REGEXP_REPLACE` | RE2 vs Java regex; Spark's `regexp_extract` defaults to group 1 and fails without one (verified); `\1` vs `$1` |
| `G11_JSON` | `JSON_VALUE`, `JSON_QUERY`, `JSON_EXTRACT[_SCALAR]` | `get_json_object`'s path syntax and result types differ |
| `G12_STRING_AGG` | `STRING_AGG` | No ordered string aggregate in Spark 3.5 |
| `G13_UNNEST` | `UNNEST(...)` | Becomes `explode` / `LATERAL VIEW`; the shape depends on the query (Spark cannot resolve `UNNEST`, verified) |
| `G14_STAR_MODIFIER` | `SELECT * EXCEPT (...)` / `* REPLACE (...)` | Spark 3.5 cannot parse it (verified) |
| `G22_SAME_NAME` | `SPLIT`, `DATE_TRUNC`, `DATE_ADD`, `DATE_SUB` | Spark has the name, not the meaning: `split('a.b', '.')` returns four empty strings (regex); `date_sub(d, INTERVAL 1 DAY)` fails (verified) |
| `G23_QUERY_PARAMETER` | `@run_date`, `@param` | Pass it as an AIDP job parameter |
| `G24_LITERAL` | `b'...'`, `'''...'''` | Spark uses `X'..'`; reads a triple-quoted string as three adjacent literals |
| `G90_NOT_SPARK_BUILTIN` | any other call | Not among Spark 3.5.9's 418 built-ins (`spark_builtins.py`) |

## Blocked — the statement is not translated

| Rule | GoogleSQL | Why |
|---|---|---|
| `G15_QUALIFY` | `QUALIFY` in a shape the rewrite does not cover: a window condition that uses a select alias, `SELECT * EXCEPT/REPLACE`, an unnamed select item, `WINDOW`, a set operation, `SELECT AS STRUCT` | Each one would need the output columns or a guess |
| `G16_PSEUDO_COLUMN` | `_PARTITIONTIME`, `_PARTITIONDATE`, `_TABLE_SUFFIX`, wildcard tables | No Delta equivalent |
| `G17_SCRIPTING` | `DECLARE`, `BEGIN…END`, `EXECUTE IMMEDIATE`, `SET`, `IF`, `LOOP`, `CALL`, ... and any multi-statement script | Not translated in 0.1 |
| `G18_ML_AI_GEO` | `ML.*`, `AI.*`, `ST_*` | No equivalent |
| `G97_LEGACY_SQL` | a view written in legacy SQL (checked in `migrate`) | Rewrite it in GoogleSQL first |
| `G98_UNBALANCED` | unterminated quote, unbalanced parentheses | Cannot be read safely |

## Known gaps

- An unquoted two-part name in a comma join (`FROM a, dataset.t`) is not
  rewritten. It still resolves when the target catalog is the session's current catalog.
- Function argument types are not inferred, so `G03` carries its caveat even
  when both operands are `FLOAT64`.
- PASS means "no rule found a problem". It is not execution-verified on the
  customer's data.
