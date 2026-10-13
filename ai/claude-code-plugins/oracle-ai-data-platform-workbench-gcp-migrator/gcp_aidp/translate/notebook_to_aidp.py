"""BigQuery Studio notebooks → AIDP notebooks, cell by cell, one named rule per construct.

  N01_BIGQUERY_MAGIC      %%bigquery [var]: GoogleSQL → spark.sql(<translated>).toPandas()
  N02_MAGIC_OPTION        magic options with no AIDP meaning: flagged; --use_legacy_sql blocks
  N03_NOTEBOOK_COMMAND    %load_ext of the BigQuery magic removed; other % and ! lines commented out
  N04_SQL_CELL            a BigQuery Studio SQL cell (generated Colab code): its query → spark.sql(...).toPandas()

A %%bigquery cell carries the GoogleSQL translator's findings; a Python cell goes
through the Dataproc translator (paths, connector reads, Google libraries).
Markdown cells are kept as they are.
"""
from __future__ import annotations

import re
import shlex

from gcp_aidp.translate.googlesql_to_spark import Context, Finding, translate
from gcp_aidp.translate.pyspark_to_aidp import translate_pyspark
from gcp_aidp.translate.types import quote_ident

_LOAD_MAGIC = re.compile(r"^\s*%load_ext\s+(bigquery_magics|google\.cloud\.bigquery)\s*$")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Options that change only how the result is fetched or shown.
_HARMLESS = {"--verbose", "--no_query_cache", "--use_rest_api", "--use_bqstorage_api", "--progress_bar_type",
             "--location", "--max_results"}
_TAKES_VALUE = {"--project", "--destination_table", "--progress_bar_type", "--location", "--max_results",
                "--maximum_bytes_billed", "--engine"}


def _magic(header: str, body: str, ctx: Context) -> tuple[str, list[Finding]]:
    """One %%bigquery cell → Python running the translated query on AIDP."""
    out = []
    if "--params" in header:  # the magic takes the rest of the line as the parameters (JSON may hold spaces)
        header, _, params = header.partition("--params")
        out.append(Finding("N02_MAGIC_OPTION", "flag", f"--params {params.strip()}: Spark 3.5 takes named "
                           "parameters as :name with spark.sql(query, args={...}); rewrite each @name and pass the "
                           "values"))
    try:
        words = shlex.split(header)[1:]
    except ValueError:
        return "", [Finding("N02_MAGIC_OPTION", "block", f"cannot read the magic line {header!r}")]
    var, i = None, 0
    while i < len(words):
        w = words[i]
        value = words[i + 1] if w in _TAKES_VALUE and i + 1 < len(words) else None
        i += 2 if value is not None else 1
        if not w.startswith("--"):
            var = w
        elif w == "--use_legacy_sql":
            return "", [Finding("N02_MAGIC_OPTION", "block", "legacy SQL: rewrite it in GoogleSQL first")]
        elif w == "--project" and value != ctx.project:
            out.append(Finding("N02_MAGIC_OPTION", "flag", f"runs in project {value}, outside the migration"))
        elif w not in _HARMLESS | {"--project"}:
            out.append(Finding("N02_MAGIC_OPTION", "flag", f"{w}{' ' + value if value else ''}: no AIDP meaning; "
                               "handle it by hand"))
    if var is not None and not _NAME.fullmatch(var):
        return "", [Finding("N02_MAGIC_OPTION", "block", f"destination {var!r} is not a Python name")]
    query = body.strip()
    parts = query.strip("`").replace(":", ".").split(".")
    if query and "\n" not in query and " " not in query and len(parts) == 3:  # a table id, read whole
        target = ctx.relations.get((parts[1], parts[2])) if parts[0] == ctx.project else None
        if target is None:
            return "", [Finding("N01_BIGQUERY_MAGIC", "flag", f"reads table {query}, which is not in the plan")]
        expr = f'spark.table("{".".join(quote_ident(p) for p in target)}").toPandas()'
        out.append(Finding("N01_BIGQUERY_MAGIC", "rewrite", f"%%bigquery table {query} → spark.table"))
    else:
        r = translate(query, ctx)
        out += r.findings
        if r.status == "blocked":
            return "", out
        expr = f"spark.sql({_triple(r.sql)}).toPandas()"
        out.append(Finding("N01_BIGQUERY_MAGIC", "rewrite", "%%bigquery → spark.sql(...).toPandas()"))
    return (f"{var} = {expr}" if var else expr), out


_SQL_CELL = re.compile(r'_sql = """(?P<sql>.*?)"""', re.DOTALL)


def _sql_cell(source: str, ctx: Context) -> tuple[str, list[Finding]]:
    """A BigQuery Studio SQL cell: Colab writes `# key: value` headers, the query in `_sql = \"\"\"...\"\"\"`,
    then runs it with google.colab.sql, which AIDP does not have (as the Dataform API returned it, 2026-10-09)."""
    headers = dict(line[2:].split(": ", 1) for line in source.splitlines()
                   if line.startswith("# ") and ": " in line)
    m = _SQL_CELL.search(source)
    if headers.get("sql_engine") != "bigquery" or m is None:
        return source, [Finding("N04_SQL_CELL", "flag", f"a SQL cell on engine {headers.get('sql_engine')!r} that "
                                "is not read: rewrite it by hand")]
    var = headers.get("output_variable", "df")
    if not _NAME.fullmatch(var):
        return "", [Finding("N04_SQL_CELL", "block", f"output variable {var!r} is not a Python name")]
    r = translate(m.group("sql").strip(), ctx)
    if r.status == "blocked":
        return "", r.findings
    code = f"{var} = spark.sql({_triple(r.sql)}).toPandas()"
    if headers.get("output_mode", "table") != "none":
        code += f"\n{var}"  # the cell showed its result
    return code, r.findings + [Finding("N04_SQL_CELL", "rewrite", "BigQuery Studio SQL cell → spark.sql(...).toPandas()")]


def _triple(sql: str) -> str:
    """A Python literal for the SQL, as a readable triple-quoted string when it allows one."""
    if '"""' not in sql and "\\" not in sql and not sql.endswith('"'):
        return f'"""{sql}"""'
    return repr(sql)


def _python(source: str, ctx: Context) -> tuple[str, list[Finding]]:
    """A code cell: notebook commands handled line by line, then the Python through the Dataproc rules."""
    lines, out = [], []
    for line in source.splitlines():
        if _LOAD_MAGIC.match(line):
            out.append(Finding("N03_NOTEBOOK_COMMAND", "rewrite", f"{line.strip()} removed: no BigQuery magic on AIDP"))
            continue
        if line.lstrip().startswith(("%", "!")):
            lines.append(f"# gcp-aidp: {line.strip()}")
            out.append(Finding("N03_NOTEBOOK_COMMAND", "flag", f"{line.strip()}: a notebook or shell command, "
                               "commented out; install libraries on the AIDP cluster instead"))
            continue
        lines.append(line)
    code = "\n".join(lines)
    if not code.strip():
        return code, out
    r = translate_pyspark(code + "\n", ctx)
    return r.sql.rstrip("\n"), out + r.findings


def translate_notebook(cells: list[dict], ctx: Context | None = None) -> tuple[list[dict], list[Finding]]:
    """(AIDP cells, findings). A blocked cell keeps its original text, commented out, and blocks the notebook."""
    ctx = ctx or Context()
    out_cells, findings = [], []
    for n, cell in enumerate(cells, 1):
        source = cell.get("source", "")
        if cell.get("cell_type") != "code":
            out_cells.append({"cell_type": "markdown", "source": source})
            continue
        first, _, rest = source.lstrip().partition("\n")
        if first.startswith("# sql_engine:") or "_bqsqlcell.run(" in source:
            code, f = _sql_cell(source, ctx)
        elif first.startswith("%%bigquery"):
            code, f = _magic(first, rest, ctx)
        elif first.startswith("%%"):
            code, f = "", [Finding("N03_NOTEBOOK_COMMAND", "block", f"cell magic {first.split()[0]}: not Python; "
                                   "rewrite the cell by hand")]
        else:
            code, f = _python(source, ctx)
            if source.strip() and not code.strip() and not any(x.severity == "block" for x in f):
                findings += [Finding(x.rule, x.severity, f"cell {n}: {x.detail}") for x in f]
                continue  # nothing left but what was removed (%load_ext): no empty cell
        if any(x.severity == "block" for x in f):
            code = "# gcp-aidp: NOT TRANSLATED. The original cell, commented out:\n" + "\n".join(
                f"# {line}" for line in source.splitlines())
        findings += [Finding(x.rule, x.severity, f"cell {n}: {x.detail}") for x in f]
        out_cells.append({"cell_type": "code", "source": code})
    return out_cells, list(dict.fromkeys(findings))
