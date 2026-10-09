"""GoogleSQL → Spark SQL 3.5, deterministic, one named rule per construct.

A lexer splits the statement into code, identifiers, string literals and
comments, so no rule ever edits text inside a literal or a comment. Each
finding has a severity:

  rewrite  exact: Spark returns the same result (checked on Spark 3.5)
  caveat   rewritten, exact only under the stated condition
  flag     left as written; a human must review it
  block    the statement is not translated at all (no partial translation)

A statement with any `block` is returned unchanged. `caveat` and `flag` make
the asset REVIEW. The authoritative rule table is references/dialect-translation.md.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from gcp_aidp.translate.gcs_to_oci import rewrite_text
from gcp_aidp.translate.spark_builtins import SPARK_BUILTINS, SPARK_KEYWORDS
from gcp_aidp.translate.types import map_type_name, quote_ident

# ── lexer ─────────────────────────────────────────────────────────────────────

_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*|\#[^\n]*|/\*.*?(?:\*/|\Z))
  | (?P<string>(?:[rRbB]{1,2})?(?:'''.*?'''|\"\"\".*?\"\"\"|'(?:\\.|[^'\\\n])*'|"(?:\\.|[^"\\\n])*"))
  | (?P<ident>`(?:\\.|[^`\\])*`)
  | (?P<param>@@?[A-Za-z_][A-Za-z0-9_]*)
  | (?P<number>(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
  | (?P<punct>.)
""", re.VERBOSE | re.DOTALL)


@dataclass
class Tok:
    kind: str
    text: str


def lex(sql: str) -> list[Tok]:
    return [Tok(m.lastgroup, m.group()) for m in _TOKEN.finditer(sql)]


# ── results ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Finding:
    rule: str
    severity: str  # rewrite | caveat | flag | block | info
    detail: str


@dataclass
class Result:
    source_sql: str
    sql: str
    findings: list[Finding]

    @property
    def status(self) -> str:
        sev = {f.severity for f in self.findings}
        if "block" in sev:
            return "blocked"
        if sev & {"caveat", "flag"}:
            return "needs_manual_review"
        return "ok"

    @property
    def changes(self) -> int:
        return sum(f.severity in ("rewrite", "caveat") for f in self.findings)

    @property
    def flags(self) -> int:
        return sum(f.severity in ("caveat", "flag", "block") for f in self.findings)


@dataclass
class Context:
    """What a reference can resolve to: the plan's targets."""
    project: str = ""
    relations: dict[tuple[str, str], tuple[str, str, str]] = field(default_factory=dict)
    functions: dict[tuple[str, str], tuple[str, str, str]] = field(default_factory=dict)
    buckets: dict[str, str] = field(default_factory=dict)
    namespace: str = "<your-oci-namespace>"


# ── rule tables ───────────────────────────────────────────────────────────────

_SCRIPTING = {"DECLARE", "BEGIN", "EXECUTE", "SET", "IF", "LOOP", "WHILE", "REPEAT", "FOR",
              "CALL", "RAISE", "RETURN", "LEAVE", "CONTINUE", "BREAK", "ITERATE", "ASSERT",
              "EXPORT", "LOAD"}
_PSEUDO_COLUMNS = {"_PARTITIONTIME", "_PARTITIONDATE", "_TABLE_SUFFIX"}
_TRUNC_PARTS = {"MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY",
                "MONTH", "QUARTER", "YEAR", "ISOWEEK"}
# FORMAT_DATE specifiers proven to match Spark's datetime patterns, and the
# literal characters allowed between them without Java-pattern quoting.
_DATE_SPECIFIERS = {"%Y": "yyyy", "%m": "MM", "%d": "dd", "%F": "yyyy-MM-dd"}
_DATE_LITERALS = set("-/:., _")

# Left in place: Spark has the name or a near equivalent, but not the meaning.
_FLAGGED_CALLS = {
    "SAFE_CAST": ("G02_SAFE_CAST", "try_cast parses strings differently: Spark reads 'yes' and '1' as TRUE "
                  "and '0x1A' as NULL, where BigQuery gives NULL, NULL and 26"),
    "GENERATE_ARRAY": ("G05_GENERATE_ARRAY", "sequence(5, 1) counts down; GENERATE_ARRAY(5, 1) is empty"),
    "PARSE_DATE": ("G08_PARSE_DATE", "to_date is strict about digit counts where PARSE_DATE is lenient"),
    "ARRAY_LENGTH": ("G09_ARRAY_LENGTH", "size(NULL) is -1 on Spark 3.5; ARRAY_LENGTH(NULL) is NULL"),
    "REGEXP_CONTAINS": ("G10_REGEXP", "RE2 vs Java regex dialects differ; rlike is the nearest"),
    "REGEXP_EXTRACT": ("G10_REGEXP", "Spark's regexp_extract defaults to group 1 and fails without one"),
    "REGEXP_EXTRACT_ALL": ("G10_REGEXP", "RE2 vs Java regex dialects differ"),
    "REGEXP_REPLACE": ("G10_REGEXP", "RE2 vs Java regex, and \\1 vs $1 in the replacement"),
    "JSON_VALUE": ("G11_JSON", "get_json_object's path syntax and result types differ"),
    "JSON_QUERY": ("G11_JSON", "get_json_object's path syntax and result types differ"),
    "JSON_EXTRACT": ("G11_JSON", "get_json_object's path syntax and result types differ"),
    "JSON_EXTRACT_SCALAR": ("G11_JSON", "get_json_object's path syntax and result types differ"),
    "STRING_AGG": ("G12_STRING_AGG", "no ordered string aggregate in Spark 3.5; collect_list does not keep order"),
    "UNNEST": ("G13_UNNEST", "becomes explode / LATERAL VIEW; the shape depends on the query"),
    "SPLIT": ("G22_SAME_NAME", "Spark's split takes a regex, BigQuery's a literal: split('a.b', '.') differs"),
    "DATE_TRUNC": ("G22_SAME_NAME", "Spark's date_trunc takes (unit, timestamp) and returns a TIMESTAMP"),
    "DATE_ADD": ("G22_SAME_NAME", "Spark's date_add takes a day count, not an INTERVAL"),
    "DATE_SUB": ("G22_SAME_NAME", "Spark's date_sub takes a day count, not an INTERVAL"),
}


# ── translator ────────────────────────────────────────────────────────────────

class _Sql:
    """Tokens plus the indices of the significant ones (not whitespace or comment)."""

    def __init__(self, sql: str):
        self.toks = lex(sql)
        self.sig = [i for i, t in enumerate(self.toks) if t.kind not in ("ws", "comment")]

    def t(self, j: int) -> Tok | None:
        return self.toks[self.sig[j]] if 0 <= j < len(self.sig) else None

    def word(self, j: int) -> str:
        t = self.t(j)
        return t.text.upper() if t is not None and t.kind == "word" else ""

    def is_punct(self, j: int, ch: str) -> bool:
        t = self.t(j)
        return t is not None and t.kind == "punct" and t.text == ch

    def close(self, j: int) -> int | None:
        """Index of the ')' matching the '(' at significant index j."""
        depth = 0
        for k in range(j, len(self.sig)):
            if self.is_punct(k, "("):
                depth += 1
            elif self.is_punct(k, ")"):
                depth -= 1
                if depth == 0:
                    return k
        return None

    def args(self, open_j: int, close_j: int) -> list[tuple[int, int]]:
        """Top-level argument spans [start, end) between a '(' and its ')'."""
        spans, start, depth = [], open_j + 1, 0
        for k in range(open_j + 1, close_j):
            if self.is_punct(k, "(") or self.is_punct(k, "["):
                depth += 1
            elif self.is_punct(k, ")") or self.is_punct(k, "]"):
                depth -= 1
            elif depth == 0 and self.is_punct(k, ","):
                spans.append((start, k))
                start = k + 1
        if start < close_j:
            spans.append((start, close_j))
        return spans

    def text(self, a: int, b: int) -> str:
        """Source text of significant tokens [a, b), with what lies between them."""
        if a >= b:
            return ""
        return "".join(t.text for t in self.toks[self.sig[a]:self.sig[b - 1] + 1])

    def replace(self, a: int, b: int, new: str) -> None:
        """Replace significant tokens [a, b) and everything between them by `new`."""
        lo, hi = self.sig[a], self.sig[b - 1]
        self.toks[lo].text = new
        for i in range(lo + 1, hi + 1):
            self.toks[i].text = ""

    def calls(self):
        """(index of name, name upper) for every unqualified `name(`."""
        for j in range(len(self.sig)):
            if self.word(j) and self.is_punct(j + 1, "(") and not self.is_punct(j - 1, "."):
                yield j, self.word(j)

    def render(self) -> str:
        return "".join(t.text for t in self.toks)


def _blockers(s: _Sql) -> list[Finding]:
    out = []
    for t in s.toks:
        if t.kind == "punct" and t.text in "'\"`":
            out.append(Finding("G98_UNBALANCED", "block", "unterminated quote"))
            return out
    statements = [[]]
    depth = 0
    for j in range(len(s.sig)):
        if s.is_punct(j, "("):
            depth += 1
        elif s.is_punct(j, ")"):
            depth -= 1
        if depth == 0 and s.is_punct(j, ";"):
            statements.append([])
        else:
            statements[-1].append(j)
    if depth != 0:
        out.append(Finding("G98_UNBALANCED", "block", "unbalanced parentheses"))
    statements = [st for st in statements if st]
    if len(statements) > 1:
        out.append(Finding("G17_SCRIPTING", "block", f"{len(statements)} statements; a script is not translated"))
    if statements and s.word(statements[0][0]) in _SCRIPTING:
        out.append(Finding("G17_SCRIPTING", "block", f"scripting statement {s.word(statements[0][0])}"))
    for j in range(len(s.sig)):
        w, t = s.word(j), s.t(j)
        if w in _PSEUDO_COLUMNS:
            out.append(Finding("G16_PSEUDO_COLUMN", "block", f"{w} has no Delta equivalent"))
        elif t.kind == "ident" and "*" in t.text:
            out.append(Finding("G16_PSEUDO_COLUMN", "block", f"wildcard table {t.text}"))
        elif w in ("ML", "AI") and s.is_punct(j + 1, ".") and s.is_punct(j + 3, "("):
            out.append(Finding("G18_ML_AI_GEO", "block", f"{w}.{s.word(j + 2)}: BigQuery ML / AI function"))
        elif w.startswith("ST_") and s.is_punct(j + 1, "("):
            out.append(Finding("G18_ML_AI_GEO", "block", f"{w}: geography function"))
    return list(dict.fromkeys(out))


# A dotted name after one of these is a table. The DML/DDL targets matter most:
# a two-part `INSERT INTO d.t` left as written would land in whatever catalog
# the job's session defaults to.
_RELATION_KEYWORDS = {"FROM", "JOIN", "INTO", "UPDATE", "TABLE", "MERGE", "USING", "INSERT", "DELETE"}
_QUALIFY_STOPS = {"ORDER", "LIMIT", "WINDOW", "UNION", "INTERSECT", "EXCEPT"}


def _qualify(sql: str) -> tuple[str, list[Finding]]:
    """G15: Spark 3.5 has no QUALIFY, so each one becomes a subquery filtered on its condition.

    A condition with no window function filters the query's own output:
        SELECT * FROM (<query>) AS _qualified WHERE <cond>
    A condition with one is computed beside the row, which travels as a struct
    so the output columns need not be known (`SELECT *` included):
        SELECT _qualify_row.* FROM (SELECT struct(<items>) AS _qualify_row,
            (<cond>) AS _qualify_keep FROM ...) AS _qualified WHERE _qualify_keep
    Both are exact. A shape outside these is blocked, never guessed.
    """
    found = []
    while True:
        s = _Sql(sql)
        q = next((j for j in range(len(s.sig)) if s.word(j) == "QUALIFY"), None)
        if q is None:
            return sql, found
        new, finding = _qualify_one(s, q)
        if new is None:
            return sql, [finding]
        sql = new
        found.append(finding)


def _qualify_one(s: _Sql, q: int) -> tuple[str | None, Finding]:
    def blocked(why: str):
        return None, Finding("G15_QUALIFY", "block", f"QUALIFY: {why}; rewrite it as a subquery by hand")

    depth, d = [], 0
    for j in range(len(s.sig)):
        d -= s.is_punct(j, ")")
        depth.append(d)
        d += s.is_punct(j, "(")
    dq = depth[q]
    level = [j for j in range(len(s.sig)) if depth[j] == dq]  # this query block's tokens and its parents'
    sel = next((j for j in range(q - 1, -1, -1) if depth[j] < dq or (depth[j] == dq and s.word(j) == "SELECT")), None)
    if sel is None or depth[sel] < dq:
        return blocked("no SELECT before it")
    end = next((j for j in range(q + 1, len(s.sig)) if depth[j] < dq or (
        depth[j] == dq and (s.is_punct(j, ";") or s.word(j) in _QUALIFY_STOPS))), len(s.sig))
    stop = s.word(end) if end < len(s.sig) and depth[end] == dq else ""
    if stop in ("WINDOW", "UNION", "INTERSECT", "EXCEPT"):
        return blocked(f"followed by {stop}")
    cond = s.text(q + 1, end)
    if not cond:
        return blocked("no condition")
    lst = sel + 1
    distinct = s.word(lst) == "DISTINCT"
    lst += s.word(lst) in ("DISTINCT", "ALL")
    if s.word(lst) == "AS":
        return blocked(f"SELECT AS {s.word(lst + 1)}")
    frm = next((j for j in level if lst <= j < q and s.word(j) == "FROM"), None)
    if frm is None:
        return blocked("no FROM")
    order = (Finding("G15_QUALIFY", "caveat", "QUALIFY → subquery; its ORDER BY now sorts the subquery's "
                     "output, so it can name only selected columns") if stop == "ORDER" else None)

    if not any(s.word(k) == "OVER" for k in range(q + 1, end)):
        new = f"SELECT * FROM ({s.text(sel, q)}) AS _qualified WHERE {cond}"
        s.replace(sel, end, new)
        return s.render(), order or Finding("G15_QUALIFY", "rewrite", "QUALIFY on the output → outer WHERE")

    def plain(a: int, b: int) -> bool:  # col, t.col, `t`.`col`
        return (b - a) % 2 == 1 and all(
            (s.t(k).kind in ("word", "ident")) if (k - a) % 2 == 0 else s.is_punct(k, ".") for k in range(a, b))

    def norm(tok: Tok) -> str:
        return tok.text.strip("`").lower()

    items, aliases = [], set()
    for a, b in s.args(lst - 1, frm):
        if any(s.is_punct(k, "*") and s.word(k + 1) in ("EXCEPT", "REPLACE") for k in range(a, b)):
            return blocked(f"SELECT * {s.word(next(k for k in range(a, b) if s.is_punct(k, '*')) + 1)}")
        if s.is_punct(b - 1, "*"):
            items.append(s.text(a, b))
        elif b - a >= 3 and s.word(b - 2) == "AS" and s.t(b - 1).kind in ("word", "ident"):
            items.append(s.text(a, b))
            if not (plain(a, b - 2) and norm(s.t(b - 3)) == norm(s.t(b - 1))):
                aliases.add(norm(s.t(b - 1)))
        elif plain(a, b):
            items.append(f"{s.text(a, b)} AS {s.t(b - 1).text}")
        else:
            return blocked(f"select item {s.text(a, b)!r} has no name; give it an alias")
    refs = {norm(s.t(k)) for k in range(q + 1, end) if s.t(k).kind in ("word", "ident")
            and not s.is_punct(k - 1, ".") and not s.is_punct(k + 1, "(")}
    if refs & aliases:
        return blocked(f"its window condition uses the select alias {sorted(refs & aliases)[0]}")
    new = (f"SELECT {'DISTINCT ' if distinct else ''}_qualify_row.* FROM (SELECT struct({', '.join(items)}) "
           f"AS _qualify_row, ({cond}) AS _qualify_keep {s.text(frm, q)}) AS _qualified WHERE _qualify_keep")
    s.replace(sel, end, new)
    return s.render(), order or Finding("G15_QUALIFY", "rewrite", "QUALIFY → subquery filtered on its condition")


def _references(s: _Sql, ctx: Context, out: list[Finding]) -> set[int]:
    """G01: rewrite table and function references to their planned targets.

    A dotted name is a relation when it is a backtick name containing a dot, or
    follows a keyword that names a table (FROM, JOIN, and the DML/DDL targets
    INTO, UPDATE, TABLE, MERGE, USING, INSERT, DELETE). Elsewhere `a.b.c` is a
    column path and is left alone.
    Returns the indices of rewritten function names (exempt from the gate).
    """
    exempt: set[int] = set()
    j = 0
    while j < len(s.sig):
        t = s.t(j)
        if t.kind not in ("word", "ident") or s.is_punct(j - 1, "."):
            j += 1
            continue
        k, parts, quoted_dots = j, [], False
        while True:
            tok = s.t(k)
            if tok.kind == "ident":
                inner = tok.text[1:-1].replace("``", "`")
                quoted_dots |= "." in inner
                parts += inner.split(".")
            else:
                parts.append(tok.text)
            if s.is_punct(k + 1, ".") and s.t(k + 2) is not None and s.t(k + 2).kind in ("word", "ident"):
                k += 2
            else:
                break
        end = k + 1
        in_from = s.word(j - 1) in _RELATION_KEYWORDS
        is_call = s.is_punct(end, "(") and not in_from  # `INSERT INTO d.t (a, b)` names a table
        if len(parts) >= 2 and (quoted_dots or in_from or is_call) and len(parts) <= 3:
            project = parts[0] if len(parts) == 3 else ctx.project
            key = (parts[-2], parts[-1])
            table = ctx.functions if is_call else ctx.relations
            name = ".".join(parts)
            if project != ctx.project:
                out.append(Finding("G01_REFERENCE", "flag", f"cross-project reference {name}"))
            elif key in table:
                new = ".".join(quote_ident(p) for p in table[key])
                s.replace(j, end, new)
                out.append(Finding("G01_REFERENCE", "rewrite", f"{name} → {new}"))
                if is_call:
                    exempt.add(j)
            else:
                what = "function" if is_call else "relation"
                out.append(Finding("G01_REFERENCE", "flag", f"{what} {name} is not in the migration plan"))
        j = end
    return exempt


def _cast_types(s: _Sql, out: list[Finding]) -> None:
    """G19: GoogleSQL type names inside CAST(x AS T) → Spark type names."""
    for j, name in list(s.calls()):
        if name not in ("CAST", "SAFE_CAST"):
            continue
        close = s.close(j + 1)
        if close is None:
            continue
        # The CAST's own AS, at its top level: an AS inside a nested call or CAST
        # (`CAST(COALESCE(CAST(x AS STRING), bytes) AS STRING)`) is not this one's,
        # and the words after it are column names, not a type.
        as_j, depth = None, 0
        for k in range(j + 2, close):
            if s.is_punct(k, "("):
                depth += 1
            elif s.is_punct(k, ")"):
                depth -= 1
            elif depth == 0 and s.word(k) == "AS":
                as_j = k
                break
        if as_j is None:
            continue
        for k in range(as_j + 1, close):
            w = s.word(k)
            # A word followed by a word is a STRUCT field name (`STRUCT<x INT64>`), not a type.
            if not w or w in ("ARRAY", "STRUCT") or s.word(k + 1):
                continue
            target, rule, severity, detail = map_type_name(w)
            if s.is_punct(k + 1, "(") and w in ("NUMERIC", "BIGNUMERIC"):
                target, severity = "DECIMAL", "map" if w == "NUMERIC" else "flag"
            if severity == "block" or target is None:
                out.append(Finding("G19_CAST_TYPE", "flag", f"CAST to {w}: {detail}"))
            elif target != w:
                s.t(k).text = target
                sev = "rewrite" if severity == "map" else "caveat"
                out.append(Finding("G19_CAST_TYPE", sev, f"CAST AS {w} → {target}" + (f": {detail}" if detail else "")))


def _functions(s: _Sql, out: list[Finding]) -> set[int]:
    """Function rewrites and flags. Returns indices already reported (exempt from the gate)."""
    seen: set[int] = set()
    # Innermost first: an outer rewrite copies its arguments' text, so an inner
    # call must already be rewritten when that happens.
    for j, name in reversed(list(s.calls())):
        close = s.close(j + 1)
        if close is None:
            continue
        args = s.args(j + 1, close)
        if name == "COUNTIF":
            s.t(j).text = "count_if"
            out.append(Finding("G04_COUNTIF", "rewrite", "COUNTIF → count_if"))
        elif name == "SAFE_DIVIDE":
            s.t(j).text = "try_divide"
            out.append(Finding("G03_SAFE_DIVIDE", "caveat",
                               "SAFE_DIVIDE → try_divide: exact for INT64/FLOAT64; on NUMERIC operands Spark "
                               "returns DECIMAL(38,6), dropping 3 of NUMERIC's 9 decimal places"))
        elif name == "TIMESTAMP_TRUNC":
            part = s.word(args[1][0]) if len(args) == 2 and args[1][1] - args[1][0] == 1 else ""
            if part in _TRUNC_PARTS:
                unit = "WEEK" if part == "ISOWEEK" else part
                s.replace(j, close + 1, f"date_trunc('{unit}', {s.text(*args[0])})")
                out.append(Finding("G06_TIMESTAMP_TRUNC", "caveat",
                                   f"TIMESTAMP_TRUNC(.., {part}) → date_trunc('{unit}', ..): BigQuery truncates "
                                   "in UTC, Spark in the session time zone; exact with spark.sql.session.timeZone=UTC"))
            else:
                out.append(Finding("G06_TIMESTAMP_TRUNC", "flag",
                                   "TIMESTAMP_TRUNC with a time zone or WEEK(<day>) part: BigQuery weeks start "
                                   "on Sunday, Spark's on Monday"))
        elif name == "DATE_DIFF":
            part = s.word(args[2][0]) if len(args) == 3 and args[2][1] - args[2][0] == 1 else ""
            if part == "DAY":
                s.replace(j, close + 1, f"datediff({s.text(*args[0])}, {s.text(*args[1])})")
                out.append(Finding("G07_DATE_DIFF", "rewrite", "DATE_DIFF(a, b, DAY) → datediff(a, b)"))
            else:
                out.append(Finding("G07_DATE_DIFF", "flag", f"DATE_DIFF with part {part or '?'}: only DAY is exact"))
        elif name == "FORMAT_DATE":
            fmt = s.t(args[0][0]) if args and args[0][1] - args[0][0] == 1 else None
            java = _java_date_pattern(fmt.text) if fmt is not None and fmt.kind == "string" else None
            if java is not None and len(args) == 2:
                s.replace(j, close + 1, f"date_format({s.text(*args[1])}, '{java}')")
                out.append(Finding("G08_FORMAT_DATE", "caveat",
                                   f"FORMAT_DATE({fmt.text}) → date_format(.., '{java}'): exact for years "
                                   "1000-9999; before that Spark zero-pads ('0005') and BigQuery does not ('5')"))
            else:
                out.append(Finding("G08_FORMAT_DATE", "flag", "FORMAT_DATE pattern outside the proven set"))
        elif name in _FLAGGED_CALLS:
            rule, why = _FLAGGED_CALLS[name]
            out.append(Finding(rule, "flag", f"{name}: {why}"))
        else:
            continue
        seen.add(j)
    for j in range(len(s.sig)):  # SELECT * EXCEPT (...) / * REPLACE (...)
        if s.is_punct(j, "*") and s.word(j + 1) in ("EXCEPT", "REPLACE") and s.is_punct(j + 2, "("):
            out.append(Finding("G14_STAR_MODIFIER", "flag",
                               f"SELECT * {s.word(j + 1)} (...): Spark 3.5 cannot parse it; list the columns"))
    return seen


def _java_date_pattern(literal: str) -> str | None:
    if not literal or literal[0] not in "'\"" or literal.startswith(("'''", '"""')):
        return None
    body, out, i = literal[1:-1], [], 0
    while i < len(body):
        if body[i] == "%":
            spec = body[i:i + 2]
            if spec not in _DATE_SPECIFIERS:
                return None
            out.append(_DATE_SPECIFIERS[spec])
            i += 2
        elif body[i] in _DATE_LITERALS:
            out.append(body[i])
            i += 1
        else:
            return None
    return "".join(out)


def _literals_and_comments(s: _Sql, ctx: Context, out: list[Finding]) -> None:
    for t in s.toks:
        if t.kind == "comment" and t.text.startswith("#"):
            t.text = "--" + t.text[1:]
            out.append(Finding("G20_HASH_COMMENT", "rewrite", "# comment → -- comment"))
        elif t.kind == "string":
            prefix = t.text[:len(t.text) - len(t.text.lstrip("rRbB"))]  # r, b, rb, br before the quote
            if "b" in prefix.lower():
                out.append(Finding("G24_LITERAL", "flag", f"bytes literal {t.text[:20]}: Spark uses X'..'"))
            elif "'''" in t.text[:4] or '"""' in t.text[:4]:
                out.append(Finding("G24_LITERAL", "flag", "triple-quoted string: Spark reads it as three "
                                   "adjacent literals, which breaks on an embedded quote"))
            if "gs://" in t.text:
                t.text, done, missing = rewrite_text(t.text, ctx.buckets, ctx.namespace)
                out += [Finding("G21_GCS_PATH", "rewrite", f"{p} → oci://") for p in done]
                out += [Finding("G21_GCS_PATH", "flag", f"{p}: bucket not in the bucket map") for p in missing]
        elif t.kind == "param":
            out.append(Finding("G23_QUERY_PARAMETER", "flag",
                               f"query parameter {t.text}: pass it as an AIDP job parameter"))


def _gate(s: _Sql, exempt: set[int], out: list[Finding]) -> None:
    """G90: any remaining call Spark 3.5 does not have is flagged, never assumed to work."""
    for j, name in s.calls():
        low = name.lower()
        if j in exempt or low in SPARK_BUILTINS or low in SPARK_KEYWORDS:
            continue
        out.append(Finding("G90_NOT_SPARK_BUILTIN", "flag", f"{name}() is not a Spark 3.5 built-in"))


def translate(sql: str, ctx: Context | None = None) -> Result:
    ctx = ctx or Context()
    blocks = _blockers(_Sql(sql))
    rewritten, out = _qualify(sql) if not blocks else (sql, [])
    blocks += [f for f in out if f.severity == "block"]
    if blocks:
        return Result(sql, sql, blocks)
    s = _Sql(rewritten)
    _literals_and_comments(s, ctx, out)
    exempt = _references(s, ctx, out)
    _cast_types(s, out)
    exempt |= _functions(s, out)
    _gate(s, exempt, out)
    return Result(sql, s.render(), list(dict.fromkeys(out)))
