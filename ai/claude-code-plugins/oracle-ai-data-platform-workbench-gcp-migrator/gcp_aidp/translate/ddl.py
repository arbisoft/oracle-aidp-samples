"""Spark SQL DDL for the planned AIDP objects.

Every statement is `IF NOT EXISTS`: nothing here replaces an existing object.
Layout decisions are named rules (D0x), recorded as findings:

  D01_PARTITION         DAY partitioning on a DATE column → PARTITIONED BY (exact)
  D02_CLUSTER           clustering, and any other partitioning → liquid CLUSTER BY
  D03_INGESTION_TIME    ingestion-time partitioning: _PARTITIONTIME (and _PARTITIONDATE) carried
                        as columns, CLUSTER BY _PARTITIONTIME (flag: new rows must set them)
  D04_METADATA          table expiration and labels: reported, not carried
  D05_EXTERNAL_FORMAT   external table format and options
  D06_SQL_FUNCTION      SQL UDFs: Spark 3.5 has no CREATE FUNCTION ... RETURN (flag);
                        arithmetic ones become a Python UDF (python_udf)
"""
from __future__ import annotations

import re

from gcp_aidp.translate.types import map_type_name, quote_ident

MAX_CLUSTER_KEYS = 4
# Delta clusters only on columns it keeps min/max statistics for.
_CLUSTERABLE = ("BIGINT", "DOUBLE", "STRING", "DATE", "TIMESTAMP", "DECIMAL")
# AIDP's Delta keeps the backticks of a quoted CLUSTER BY column as part of its
# name (UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED), so
# clustering keys are written bare and only plain identifiers qualify.
_PLAIN_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_EXTERNAL_FORMATS = {"PARQUET": "PARQUET", "AVRO": "AVRO", "ORC": "ORC",
                     "CSV": "CSV", "NEWLINE_DELIMITED_JSON": "JSON"}


def sql_string(value: str) -> str:
    """A Spark SQL string literal (backslash escapes, Spark's default parser)."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def fqn(target: dict) -> str:
    return ".".join(quote_ident(target[k]) for k in ("catalog", "schema", "name") if target.get(k))


def table_layout(table: dict, columns: list[dict]) -> dict:
    """PARTITIONED BY / CLUSTER BY for a BigQuery table, with the rule behind each."""
    findings: list[tuple[str, str, str]] = []
    part = table.get("partitioning") or {}
    field = part.get("field")
    types = {c["name"]: c.get("target_type") or "" for c in columns}
    source_types = {c["name"]: c.get("source_type") for c in columns}
    clustering = list(table.get("clustering") or [])
    partitioned_by: list[str] = []
    keys: list[str] = []

    if part and not field:
        carried = [c for c in ("_PARTITIONTIME", "_PARTITIONDATE") if c in types]
        if "_PARTITIONTIME" in types:
            keys.append("_PARTITIONTIME")  # the data skipping the partitions gave
        findings.append(("D03_INGESTION_TIME", "flag",
                         (f"{part.get('type', 'DAY')} ingestion-time partitioning: "
                          + (f"{' and '.join(carried)} carried as columns of the same names, so queries "
                             "filtering on them keep working; rows written on AIDP must set them, as "
                             "nothing fills them in" if carried else
                             "no column carries _PARTITIONTIME, so queries filtering on it need rewriting"))))
    elif field and not clustering and part.get("type") == "DAY" and source_types.get(field) == "DATE":
        partitioned_by = [field]
        findings.append(("D01_PARTITION", "rewrite", f"DAY partitioning on DATE {field} → PARTITIONED BY ({field})"))
    elif field:
        keys.append(field)
        why = ("Delta cannot combine PARTITIONED BY with CLUSTER BY" if clustering else
               f"Delta partitions by exact value, not by {part.get('type')} of a {source_types.get(field)}")
        findings.append(("D02_CLUSTER", "info", f"{part.get('type')} partitioning on {field} → CLUSTER BY: {why}"))
    keys += clustering

    cluster_by: list[str] = []
    for k in dict.fromkeys(keys):
        if not types.get(k, "").startswith(_CLUSTERABLE):
            findings.append(("D02_CLUSTER", "info", f"{k} not clustered: Delta keeps no statistics for {types.get(k) or 'it'}"))
        elif not _PLAIN_IDENT.fullmatch(k):
            findings.append(("D02_CLUSTER", "info", f"{k} not clustered: AIDP takes only plain names as CLUSTER BY keys"))
        elif len(cluster_by) == MAX_CLUSTER_KEYS:
            findings.append(("D02_CLUSTER", "info", f"{k} not clustered: at most {MAX_CLUSTER_KEYS} keys"))
        else:
            cluster_by.append(k)
    if clustering and cluster_by:
        findings.append(("D02_CLUSTER", "rewrite", f"clustering → CLUSTER BY ({', '.join(cluster_by)})"))

    if table.get("expiration_ms"):
        findings.append(("D04_METADATA", "info", f"table expiration ({table['expiration_ms']} ms) is not carried"))
    if table.get("labels"):
        findings.append(("D04_METADATA", "info", f"labels {sorted(table['labels'])} are not carried"))
    return {"partitioned_by": partitioned_by, "cluster_by": cluster_by, "findings": findings}


def create_schema(catalog: str, schema: str, comment: str = "") -> str:
    sql = f"CREATE SCHEMA IF NOT EXISTS {quote_ident(catalog)}.{quote_ident(schema)}"
    return sql + (f"\nCOMMENT {sql_string(comment)}" if comment else "")


def create_table(target: dict, columns: list[dict], *, partitioned_by=(), cluster_by=(), comment="") -> str:
    lines = []
    for c in columns:
        line = f"  {quote_ident(c['name'])} {c['target_type']}"
        if not c.get("nullable", True):
            line += " NOT NULL"
        if c.get("comment"):
            line += f" COMMENT {sql_string(c['comment'])}"
        lines.append(line)
    sql = f"CREATE TABLE IF NOT EXISTS {fqn(target)} (\n" + ",\n".join(lines) + "\n) USING DELTA"
    if partitioned_by:
        sql += "\nPARTITIONED BY (" + ", ".join(quote_ident(c) for c in partitioned_by) + ")"
    if cluster_by:
        sql += "\nCLUSTER BY (" + ", ".join(cluster_by) + ")"  # bare: see _PLAIN_IDENT
    if comment:
        sql += f"\nCOMMENT {sql_string(comment)}"
    return sql


def create_view(target: dict, query: str) -> str:
    return f"CREATE VIEW IF NOT EXISTS {fqn(target)} AS\n{query.strip()}"


def materialized_view(target: dict, query: str) -> tuple[str, str]:
    """(snapshot, refresh). The snapshot is computed on AIDP from the migrated base
    tables: reading a materialized view through the BigQuery connector would make
    it write a temporary table in BigQuery."""
    q = query.strip()
    return (f"CREATE TABLE IF NOT EXISTS {fqn(target)} USING DELTA AS\n{q}",
            f"INSERT OVERWRITE TABLE {fqn(target)}\n{q}")


_CSV_OPTIONS = {"fieldDelimiter": "sep", "quote": "quote", "encoding": "encoding", "nullMarker": "nullValue"}
# Spark's PERMISSIVE mode already reads a short row as NULLs, as allowJaggedRows does.
_CSV_SAME = {"skipLeadingRows", "allowQuotedNewlines", "allowJaggedRows", *_CSV_OPTIONS}


def _csv_options(csv: dict, autodetect: bool | None) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """BigQuery's csvOptions as Spark CSV options: the same reading of the same files."""
    skip = int(csv.get("skipLeadingRows") or 0)
    options = {"header": "true" if skip else "false"}
    findings = []
    if skip > 1:
        findings.append(("D05_EXTERNAL_FORMAT", "flag",
                         f"BigQuery skips {skip} leading rows; Spark skips one header row at most"))
    if "skipLeadingRows" not in csv and autodetect:
        findings.append(("D05_EXTERNAL_FORMAT", "flag",
                         ("BigQuery detects a header row itself (autodetect, no skipLeadingRows): read as "
                          "header 'false'; if the files start with a header row, set header 'true'")))
    options |= {spark: str(csv[bq]) for bq, spark in _CSV_OPTIONS.items() if csv.get(bq) is not None}
    if csv.get("allowQuotedNewlines"):
        options["multiLine"] = "true"
    other = sorted(k for k, v in csv.items() if k not in _CSV_SAME and v not in (None, False, "", []))
    if other:
        findings.append(("D05_EXTERNAL_FORMAT", "flag", f"CSV options not carried: {', '.join(other)}"))
    return options, findings


def create_external_table(target: dict, fmt: str | None, location: str, columns: list[dict] | None = None,
                          csv: dict | None = None, autodetect: bool | None = None,
                          ) -> tuple[str | None, list[tuple[str, str, str]]]:
    spark_fmt = _EXTERNAL_FORMATS.get(str(fmt or "").upper())
    if spark_fmt is None:
        return None, [("D05_EXTERNAL_FORMAT", "block", f"external format {fmt!r} has no Spark data source")]
    findings = [("D05_EXTERNAL_FORMAT", "rewrite", f"{fmt} → USING {spark_fmt}")]
    column_list = ""
    if columns:
        findings += [("D05_EXTERNAL_FORMAT", c["severity"], f"column {c['name']}: {'; '.join(c['details'])}")
                     for c in columns if c["severity"] != "map"]
        if any(c["severity"] == "block" for c in columns):
            return None, findings
        column_list = " (\n" + ",\n".join(f"  {quote_ident(c['name'])} {c['target_type']}" for c in columns) + "\n)"
    else:
        findings.append(("D05_EXTERNAL_FORMAT", "caveat", ("no column list in the manifest: Spark infers the "
                                                           "schema from the files; compare it with BigQuery's")))
    options = {}
    if spark_fmt == "CSV" and csv is None:
        findings.append(("D05_EXTERNAL_FORMAT", "flag", ("CSV options (header rows, delimiter, quote) are not in "
                                                         "the manifest; Spark's defaults apply: run inventory again")))
    elif spark_fmt == "CSV":  # with a column list, Spark reads CSV fields by position, as BigQuery does
        options, more = _csv_options(csv, autodetect)
        findings += more
    sql = f"CREATE TABLE IF NOT EXISTS {fqn(target)}{column_list}\nUSING {spark_fmt}"
    if options:
        sql += "\nOPTIONS (" + ", ".join(f"{k} {sql_string(v)}" for k, v in options.items()) + ")"
    return sql + f"\nLOCATION {sql_string(location)}", findings


def create_function(target: dict, arguments: list[dict], return_type: str | None, body: str) -> tuple[str, list[tuple[str, str, str]]]:
    findings = [("D06_SQL_FUNCTION", "flag", ("Apache Spark 3.5 cannot parse CREATE FUNCTION ... RETURN; "
                                              "inline the expression in its callers or register a Python UDF"))]
    args = []
    for a in arguments:
        t, _, sev, detail = map_type_name(a.get("type", ""))
        if t is None or sev == "block":
            findings.append(("D06_SQL_FUNCTION", "flag", f"argument {a.get('name')}: {detail}"))
        args.append(f"{a.get('name')} {t or a.get('type')}")
    ret = map_type_name(return_type)[0] if return_type else None
    sql = (f"CREATE FUNCTION IF NOT EXISTS {fqn(target)}({', '.join(args)})"
           + (f"\nRETURNS {ret or return_type}" if return_type else "")
           + f"\nRETURN {body.strip()}")
    return sql, findings


_UDF_TYPES = {"INT64": "bigint", "INTEGER": "bigint", "FLOAT64": "double", "FLOAT": "double"}
_UDF_TOKEN = re.compile(r"\s*(?:(?P<num>(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)|(?P<word>[A-Za-z_]\w*)|(?P<op>[-+*/(),]))")


def python_udf(name: str, arguments: list[dict], return_type: str | None, body: str) -> str | None:
    """A SQL function over numbers as one line registering a Python UDF, or None when its body is
    more than arithmetic (+ - * /, parentheses, number literals, its arguments) and
    IFNULL/COALESCE(argument, number). NULL in, NULL out, as in SQL: an argument read outside
    IFNULL returns None when it is None."""
    import keyword
    types = [_UDF_TYPES.get(str(a.get("type")).upper()) for a in arguments]
    ret = _UDF_TYPES.get(str(return_type).upper())
    names = {str(a.get("name")).lower(): a.get("name") for a in arguments}
    if None in types or ret is None or any(not str(n).isidentifier() or keyword.iskeyword(n) for n in names.values()):
        return None
    toks, pos = [], 0
    while pos < len(body.rstrip()):
        m = _UDF_TOKEN.match(body, pos)
        if not m:
            return None
        toks.append((m.lastgroup, m.group(m.lastgroup)))
        pos = m.end()
    guarded: list[str] = []

    def peek(*ops):
        return toks and toks[0][0] == "op" and toks[0][1] in ops

    def take(op):
        if not peek(op):
            raise ValueError(op)
        toks.pop(0)

    def number():
        sign = "-" if peek("-") and toks.pop(0) else ""
        if not toks or toks[0][0] != "num":
            raise ValueError("number")
        return sign + toks.pop(0)[1]

    def atom():
        if peek("("):
            take("(")
            inner = expr()
            take(")")
            return f"({inner})"
        if not toks:
            raise ValueError("end")
        kind, text = toks.pop(0)
        if kind == "num":
            return text
        if kind == "word" and text.lower() in names:
            guarded.append(names[text.lower()])
            return names[text.lower()]
        if kind == "word" and text.upper() in ("IFNULL", "COALESCE"):
            take("(")
            kind, arg = toks.pop(0) if toks else ("", "")
            if kind != "word" or arg.lower() not in names:
                raise ValueError("argument")
            take(",")
            default = number()
            take(")")
            return f"({default} if {names[arg.lower()]} is None else {names[arg.lower()]})"
        raise ValueError(text)

    def unary():
        if peek("-"):
            toks.pop(0)
            return f"-{unary()}"
        return atom()

    def chain(part, ops):
        out = part()
        while peek(*ops):
            out += f" {toks.pop(0)[1]} {part()}"
        return out

    def expr():
        return chain(lambda: chain(unary, "*/"), "+-")

    try:
        python = expr()
    except (ValueError, IndexError):
        return None
    if toks:
        return None
    if ret == "double":  # a Python int is NULL in a double column
        python = f"float({python})"
    guards = " or ".join(f"{n} is None" for n in dict.fromkeys(guarded))
    if guards:
        python = f"None if {guards} else {python}"
    params = ", ".join(a["name"] for a in arguments)
    return f"spark.udf.register({name!r}, lambda {params}: {python}, {ret!r})"
