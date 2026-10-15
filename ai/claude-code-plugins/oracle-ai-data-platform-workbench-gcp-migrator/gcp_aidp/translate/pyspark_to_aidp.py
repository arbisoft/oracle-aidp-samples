"""Dataproc PySpark scripts and Spark SQL jobs → AIDP, deterministic, one named rule per construct.

The code is Spark already; what changes is what it reaches outside Spark: the
BigQuery connector, Cloud Storage paths, connector settings and Google client
libraries. `ast` finds each construct with its exact position and only that span
of text is replaced, so the rest of the script (formatting, comments) is kept
byte for byte. The result must parse again, or the script is blocked.

  P01_GCS_PATH           gs:// in a string or f-string → oci:// (bucket map)
  P02_BQ_READ            connector read of a planned table → spark.table(<AIDP name>)
  P03_BQ_QUERY_READ      connector query read: flagged
  P04_BQ_WRITE           connector write: flagged
  P05_CONNECTOR_SETTING  connector settings and packages: removed
  P06_HIVE_NAME          table names in spark.sql(...) and Spark SQL jobs (caveat)
  P07_GOOGLE_LIBRARY     Google client libraries: flagged
  P08_SQL_VARIABLE       ${var} in a Spark SQL job: flagged
  P09_LOCAL_FILES        __file__, or files Dataproc staged beside the job: flagged
  P11_SPARK_PROPERTY     a job's Spark properties: runtime spark.sql.* set at the top; the rest noted
  P98_UNPARSABLE         code that does not parse, before or after: blocked
  P99_CODE_NOT_READ      no code in the inventory: blocked
"""
from __future__ import annotations

import ast
import re

from gcp_aidp.translate.gcs_to_oci import rewrite_text
from gcp_aidp.translate.googlesql_to_spark import (
    Context,
    Finding,
    Result,
    names_and_paths,
)
from gcp_aidp.translate.types import quote_ident

# Options a plain table read may carry and still be exactly a table read on AIDP.
_READ_OPTIONS = {"table", "dataset", "project", "parentProject", "credentials", "credentialsFile",
                 "maxParallelism", "preferredMinParallelism", "readDataFormat", "viewsEnabled"}
# Connector settings, global (spark.conf.set) or on the session builder.
_CONNECTOR_KEYS = {"temporaryGcsBucket", "persistentGcsBucket", "persistentGcsPath", "viewsEnabled",
                   "materializationProject", "materializationDataset", "parentProject", "credentials",
                   "credentialsFile", "gcpAccessToken", "writeMethod", "intermediateFormat"}
_GOOGLE_LIBRARIES = ("google.cloud", "google.auth", "google.oauth2", "google.colab", "gcsfs", "pandas_gbq",
                     "bigframes")
_SQL_VARIABLE = re.compile(r"\$\{[^}]+\}")
# spark.sql.* keys fixed when the session starts (Spark 3.5 StaticSQLConf, and catalog plugins).
_STATIC_SQL = ("spark.sql.warehouse.dir", "spark.sql.catalogImplementation", "spark.sql.globalTempDatabase",
               "spark.sql.extensions", "spark.sql.hive.", "spark.sql.catalog.", "spark.sql.queryExecutionListeners",
               "spark.sql.streaming.streamingQueryListeners", "spark.sql.streaming.ui.", "spark.sql.ui.",
               "spark.sql.cache.serializer", "spark.sql.event.truncate.length", "spark.sql.metadataCacheTTLSeconds",
               "spark.sql.sources.disabledJdbcConnProviderList")
_DATAPROC_ONLY = ("spark.dataproc.", "dataproc:", "yarn:", "spark.yarn.", "spark.datasource.bigquery.")
_SIZING = ("spark.executor.", "spark.driver.", "spark.dynamicAllocation.", "spark.cores.max",
           "spark.default.parallelism")


class _Source:
    """Character offsets for ast positions (ast columns are UTF-8 byte offsets)."""

    def __init__(self, text: str):
        self.text = text
        self.lines = text.splitlines(keepends=True)
        self.starts = [0]
        for line in self.lines:
            self.starts.append(self.starts[-1] + len(line))

    def offset(self, line: int, col: int) -> int:
        return self.starts[line - 1] + len(self.lines[line - 1].encode()[:col].decode(errors="ignore"))

    def span(self, node: ast.AST) -> tuple[int, int]:
        return self.offset(node.lineno, node.col_offset), self.offset(node.end_lineno, node.end_col_offset)

    def segment(self, node: ast.AST) -> str:
        a, b = self.span(node)
        return self.text[a:b]


def _chain(call: ast.Call) -> tuple[list[tuple[str, ast.Call]], ast.AST | None]:
    """`x.read.format(...).option(...).load()` → ([("format", c), ("option", c), ("load", c)], `x.read`)."""
    links: list[tuple[str, ast.Call]] = []
    node: ast.AST = call
    while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        links.append((node.func.attr, node))
        node = node.func.value
    return links[::-1], node


def _literal(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _connector(links: list[tuple[str, ast.Call]]) -> bool:
    return any(m == "format" and c.args and _literal(c.args[0]) == "bigquery" for m, c in links)


def _options(links: list[tuple[str, ast.Call]]) -> dict[str, ast.AST]:
    out: dict[str, ast.AST] = {}
    for method, call in links:
        if method == "option" and len(call.args) == 2 and _literal(call.args[0]) is not None:
            out[_literal(call.args[0])] = call.args[1]
    return out


def _table_target(table: str, opts: dict, ctx: Context) -> tuple[tuple[str, str, str] | None, str]:
    """(AIDP name, why not) for a connector table id: project.dataset.table, project:dataset.table, dataset.table."""
    parts = table.replace(":", ".").split(".")
    if len(parts) == 1 and _literal(opts.get("dataset")):
        parts = [_literal(opts["dataset"]), parts[0]]
    if len(parts) == 2:
        parts = [_literal(opts.get("project")) or ctx.project, *parts]
    if len(parts) != 3:
        return None, f"table id {table!r} is not project.dataset.table"
    if parts[0] != ctx.project:
        return None, f"table {table} is in another project"
    target = ctx.relations.get((parts[1], parts[2]))
    return (target, "") if target else (None, f"table {table} is not in the migration plan")


def translate_pyspark(code: str | None, ctx: Context | None = None) -> Result:
    ctx = ctx or Context()
    if not code:
        return Result("", "", [Finding("P99_CODE_NOT_READ", "block", "the inventory holds no code for this job")])
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return Result(code, code, [Finding("P98_UNPARSABLE", "block",
                                           f"not valid Python 3 (line {exc.lineno}: {exc.msg})")])
    src, edits, out = _Source(code), [], []
    handled: set[int] = set()  # the format("bigquery") calls a recognised read or write accounts for

    def edit(node_or_span, new: str) -> None:
        edits.append((*(src.span(node_or_span) if isinstance(node_or_span, ast.AST) else node_or_span), new))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [node.module or ""] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names]
            for name in names:
                if name.startswith(_GOOGLE_LIBRARIES):
                    out.append(Finding("P07_GOOGLE_LIBRARY", "flag", f"imports {name}: AIDP has no access to "
                                       "Google Cloud; use Spark or the OCI SDK instead"))
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):  # spark.conf.set("<connector key>", ...)
            f = node.value.func
            key = _literal(node.value.args[0]) if node.value.args else None
            if isinstance(f, ast.Attribute) and f.attr == "set" and isinstance(f.value, ast.Attribute) \
                    and f.value.attr == "conf" and key in _CONNECTOR_KEYS:
                edit(node, f"pass  # gcp-aidp: removed the BigQuery connector setting {key}")
                out.append(Finding("P05_CONNECTOR_SETTING", "rewrite", f"spark.conf.set({key!r}, ...) removed"))
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method = node.func.attr
        if method == "config" and node.args:  # SparkSession.builder.config(...)
            key = _literal(node.args[0]) or ""
            value = _literal(node.args[1]) if len(node.args) > 1 else ""
            if key in _CONNECTOR_KEYS or (key.startswith("spark.jars") and "bigquery" in (value or "")):
                edit((src.span(node.func.value)[1], src.span(node)[1]), "")
                out.append(Finding("P05_CONNECTOR_SETTING", "rewrite", f".config({key!r}, ...) removed"))
            continue
        if method not in ("load", "save"):
            continue
        links, root = _chain(node)
        if not _connector(links):
            continue
        handled.update(id(c) for m, c in links if m == "format")
        opts = _options(links)
        if method == "save":
            table = _literal(node.args[0]) if node.args else _literal(opts.get("table"))
            out.append(Finding("P04_BQ_WRITE", "flag", f"writes to BigQuery table {table or '(not a literal)'}: "
                               "write the migrated Delta table instead (saveAsTable / insertInto); the "
                               "connector's overwrite keeps the table's schema, Delta's does not"))
            continue
        query = opts.get("query") or (node.args[0] if node.args and " " in (_literal(node.args[0]) or "") else None)
        if query is not None:
            out.append(Finding("P03_BQ_QUERY_READ", "flag", "reads a query result through the connector: run it "
                               "as spark.sql on the migrated tables (the GoogleSQL translator can convert it)"))
            continue
        extra = sorted(set(opts) - _READ_OPTIONS)
        table = _literal(node.args[0]) if node.args else _literal(opts.get("table"))
        if extra:
            out.append(Finding("P02_BQ_READ", "flag", f"connector read with option(s) {extra}: not a plain "
                               "table read; rewrite it by hand"))
            continue
        if table is None:
            out.append(Finding("P02_BQ_READ", "flag", "connector read whose table is not a string literal"))
            continue
        target, why = _table_target(table, opts, ctx)
        reader = root.value if isinstance(root, ast.Attribute) and root.attr == "read" else None
        if target is None or reader is None:
            out.append(Finding("P02_BQ_READ", "flag", why or "connector read not on <session>.read"))
            continue
        name = ".".join(quote_ident(p) for p in target)
        edit(node, f'{src.segment(reader)}.table("{name}")')
        out.append(Finding("P02_BQ_READ", "rewrite", f"BigQuery table {table} → {name}"))

    if any(isinstance(n, ast.Name) and n.id == "__file__" for n in ast.walk(tree)):
        out.append(Finding("P09_LOCAL_FILES", "flag", "uses __file__, which an AIDP Python task does not define; "
                           "files beside the script are not there either"))
    for node in ast.walk(tree):  # the net: a connector use no read or write above accounted for
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format" \
                and node.args and _literal(node.args[0]) == "bigquery" and id(node) not in handled:
            out.append(Finding("P02_BQ_READ", "flag", f"BigQuery connector use on line {node.lineno} that is not one "
                               "read or write chain (built across variables?): rewrite it by hand"))

    taken = sorted((a, b) for a, b, _ in edits)

    def free(a: int, b: int) -> bool:
        return not any(x < b and a < y for x, y in taken)

    docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                  if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                  and n.body and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
    inner = {id(v) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr) for v in n.values}
    sql_args = {id(n.args[0]) for n in ast.walk(tree) if isinstance(n, ast.Call) and n.args
                and isinstance(n.func, ast.Attribute) and n.func.attr == "sql"}
    for node in ast.walk(tree):
        is_text = isinstance(node, ast.JoinedStr) or (isinstance(node, ast.Constant) and isinstance(node.value, str))
        if not is_text or id(node) in docstrings or id(node) in inner:
            continue
        a, b = src.span(node)
        if not free(a, b):
            continue
        segment = code[a:b]
        if isinstance(node, ast.Constant) and id(node) in sql_args:  # spark.sql("..."): a Spark SQL statement
            r = names_and_paths(node.value, ctx)
            out += [_hive(f) if f.rule == "G01_REFERENCE" else _path(f) for f in r.findings]
            if r.sql != node.value:
                edits.append((a, b, _requote(segment, node.value, r.sql)))
            continue
        if isinstance(node, ast.JoinedStr) and id(node) in sql_args:
            out.append(Finding("P06_HIVE_NAME", "flag", "spark.sql on an f-string: its table names are built at "
                               "run time and are not checked"))
        if "gs://" in segment:
            new, done, missing = rewrite_text(segment, ctx.buckets, ctx.namespace)
            out += [Finding("P01_GCS_PATH", "rewrite", f"{p} → oci://") for p in done]
            out += [Finding("P01_GCS_PATH", "flag", f"{p}: bucket not in the bucket map") for p in missing]
            if new != segment:
                edits.append((a, b, new))

    result = code
    for a, b, new in sorted(edits, key=lambda e: e[0], reverse=True):
        result = result[:a] + new + result[b:]
    try:
        ast.parse(result)
    except SyntaxError as exc:  # never hand back code that no longer parses
        return Result(code, code, [Finding("P98_UNPARSABLE", "block", f"the translation would not parse: {exc.msg}")])
    return Result(code, result, list(dict.fromkeys(out)))


def _requote(segment: str, value: str, new_value: str) -> str:
    """The literal with its own quotes kept when it holds no escapes; repr() otherwise."""
    for q in ('"""', "'''", '"', "'"):
        if segment.startswith(q) and segment.endswith(q) and len(segment) >= 2 * len(q) \
                and segment[len(q):-len(q)] == value and q not in new_value and "\\" not in new_value:
            return q + new_value + q
    return repr(new_value)


def _path(f: Finding) -> Finding:
    """G21 inside a spark.sql string reads as the script's own path rule."""
    return Finding("P01_GCS_PATH", f.severity, f.detail) if f.rule == "G21_GCS_PATH" else f


def _hive(f: Finding) -> Finding:
    """G01 on a Dataproc name: the name resolved through the Hive metastore there, not BigQuery."""
    if f.severity == "rewrite":
        return Finding("P06_HIVE_NAME", "caveat", f"{f.detail}: on Dataproc this name resolved through the Hive "
                       "metastore; check it is the same table as BigQuery's")
    return Finding("P06_HIVE_NAME", "flag", f"{f.detail.replace('relation ', 'Hive table ')}: migrate it separately")


def spark_properties(code: str, properties: dict | None, python: bool) -> tuple[str, list[Finding]]:
    """A job's Spark properties on AIDP: runtime spark.sql.* keys are set at the top of the code
    (spark.conf.set, or SET in Spark SQL); sizing keys are for the cluster; Dataproc's own are dropped;
    anything else cannot be set from a job and is flagged."""
    keep, out = [], []
    for key, value in sorted((properties or {}).items()):
        value = str(value)
        if key.startswith(_DATAPROC_ONLY) or key in _CONNECTOR_KEYS \
                or (key.startswith("spark.jars") and "bigquery" in value):
            out.append(Finding("P11_SPARK_PROPERTY", "info", f"{key}: Dataproc or connector only, dropped"))
        elif key.startswith("spark.sql.") and not key.startswith(_STATIC_SQL) and (python or ";" not in value):
            keep.append((key, value))
            out.append(Finding("P11_SPARK_PROPERTY", "rewrite", f"{key}={value} set at the start of the job"))
        elif key.startswith(_SIZING):
            out.append(Finding("P11_SPARK_PROPERTY", "info", f"{key}={value}: size the AIDP cluster from it"))
        else:
            out.append(Finding("P11_SPARK_PROPERTY", "flag", f"{key}={value}: cannot be set from a job; set it "
                               "in the AIDP cluster's Spark configuration if the job needs it"))
    if not keep:
        return code, out
    if not python:
        return "".join(f"SET {k}={v};\n" for k, v in keep) + code, out
    body = ast.parse(code).body  # after the docstring and __future__ imports, which must stay first
    at = 0
    for i, node in enumerate(body):
        if (i == 0 and isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)) \
                or (isinstance(node, ast.ImportFrom) and node.module == "__future__"):
            at = node.end_lineno
        else:
            break
    lines = code.splitlines(keepends=True)
    if at and not lines[at - 1].endswith("\n"):
        lines[at - 1] += "\n"
    block = "".join(f"spark.conf.set({k!r}, {v!r})  # gcp-aidp: Dataproc job property\n" for k, v in keep)
    return "".join(lines[:at]) + block + "".join(lines[at:]), out


def translate_spark_sql(sql: str | None, ctx: Context | None = None, variables: dict | None = None) -> Result:
    """A Spark SQL job's script or query list: names (P06) and gs:// paths; ${var} flagged."""
    if not sql:
        return Result("", "", [Finding("P99_CODE_NOT_READ", "block", "the inventory holds no SQL for this job")])
    r = names_and_paths(sql, ctx)
    findings = [_hive(f) if f.rule == "G01_REFERENCE" else f for f in r.findings]
    if _SQL_VARIABLE.search(sql) or variables:
        findings.append(Finding("P08_SQL_VARIABLE", "flag", "uses ${variables} "
                                f"({', '.join(sorted(variables or {})) or 'in the script'}): pass them as job "
                                "parameters or set them with SET before the statements"))
    return Result(sql, r.sql, list(dict.fromkeys(findings)))
