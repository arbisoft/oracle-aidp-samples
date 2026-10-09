"""Spark SQL DDL for the planned AIDP objects.

Every statement is `IF NOT EXISTS`: nothing here replaces an existing object.
Layout decisions are named rules (D0x), recorded as findings:

  D01_PARTITION         DAY partitioning on a DATE column → PARTITIONED BY (exact)
  D02_CLUSTER           clustering, and any other partitioning → liquid CLUSTER BY
  D03_INGESTION_TIME    ingestion-time partitioning: no column to carry (flag)
  D04_METADATA          table expiration and labels: reported, not carried
  D05_EXTERNAL_FORMAT   external table format and options
  D06_SQL_FUNCTION      SQL UDFs: Spark 3.5 has no CREATE FUNCTION ... RETURN (flag)
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
        findings.append(("D03_INGESTION_TIME", "flag",
                         f"{part.get('type', 'DAY')} ingestion-time partitioning: no column carries "
                         "_PARTITIONTIME, so the target is not partitioned and queries filtering on it "
                         "need rewriting"))
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


def create_external_table(target: dict, fmt: str | None, location: str) -> tuple[str | None, list[tuple[str, str, str]]]:
    spark_fmt = _EXTERNAL_FORMATS.get(str(fmt or "").upper())
    if spark_fmt is None:
        return None, [("D05_EXTERNAL_FORMAT", "block", f"external format {fmt!r} has no Spark data source")]
    findings = [("D05_EXTERNAL_FORMAT", "rewrite", f"{fmt} → USING {spark_fmt}"),
                ("D05_EXTERNAL_FORMAT", "caveat", "no column list in the manifest: Spark infers the schema "
                 "from the files; compare it with BigQuery's")]
    if spark_fmt == "CSV":
        findings.append(("D05_EXTERNAL_FORMAT", "flag", "CSV options (header rows, delimiter, quote) are not "
                         "in the manifest; Spark's defaults apply"))
    sql = f"CREATE TABLE IF NOT EXISTS {fqn(target)}\nUSING {spark_fmt}\nLOCATION {sql_string(location)}"
    return sql, findings


def create_function(target: dict, arguments: list[dict], return_type: str | None, body: str) -> tuple[str, list[tuple[str, str, str]]]:
    findings = [("D06_SQL_FUNCTION", "flag", "Apache Spark 3.5 cannot parse CREATE FUNCTION ... RETURN; "
                 "inline the expression in its callers or register a Python UDF")]
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
