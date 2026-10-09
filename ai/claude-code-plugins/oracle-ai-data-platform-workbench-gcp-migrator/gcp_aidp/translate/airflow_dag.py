"""A Composer DAG file → one AIDP job, one task per Airflow task.

The file is parsed with `ast` and never imported, executed or evaluated. Each rule is
recorded as a finding (references/airflow-translation.md):

  C01_BIGQUERY_QUERY      BigQueryInsertJobOperator with a literal query → a task running the translated SQL
  C02_NO_OP               EmptyOperator / DummyOperator → a task that does nothing
  C03_DEPENDENCIES        task dependencies → dependsOn (info)
  C04_SCHEDULE            schedule, start date, catchup, max_active_runs recorded as text (info)
  C05_DEFAULT_ARGS        default_args and per-task arguments that are recorded only (info)
  C06_RETRIES             retries are not carried: the AIDP task runs once (info)
  C89_NO_DAG              no DAG in the file, or no task: no job (info)
  C90_UNSUPPORTED_OPERATOR  an operator or sensor that is not translated, or a module path not recognised (flag)
  C91_NON_LITERAL         a value that is not a literal, or a Dataset schedule (flag)
  C92_DYNAMIC_GRAPH       loops, functions, decorators, expand/partial, XCom, TaskFlow (flag)
  C93_PARSE_ERROR         the file does not parse (block)
  C94_JINJA               a Jinja template in a translated field (flag)
  C95_TASK_ARGUMENT       an argument or default_args key that is not translated (flag)
  C96_MULTIPLE_DAGS       more than one DAG in a file (flag)
  C97_CYCLE               dependency cycle (block)
  C98_BIGQUERY_CONFIG     a BigQuery job configuration that is not a plain literal query (flag)
  C99_NOT_SCANNED         the file could not be read (block)

A job is created only if no finding is a flag or a block: there are no partial jobs.
"""
from __future__ import annotations

import ast
from inspect import cleandoc

from gcp_aidp.translate.dataform import _dfs
from gcp_aidp.translate.googlesql_to_spark import Context, translate

# Class paths confirmed against the Apache Airflow source. A name with any other path is flagged.
_OPERATORS = {
    "airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator": "bigquery",
    "airflow.operators.empty.EmptyOperator": "empty",
    "airflow.operators.dummy.DummyOperator": "empty",
}
_DAG = ("airflow.DAG", "airflow.models.DAG", "airflow.models.dag.DAG")
_TASK_GROUP = "airflow.utils.task_group.TaskGroup"
_CHAIN = {"airflow.models.baseoperator.chain": "chain",
          "airflow.models.baseoperator.cross_downstream": "cross"}
_DAG_ARGS = ("dag_id", "schedule", "schedule_interval", "start_date", "catchup", "max_active_runs",
             "default_args", "description", "tags", "doc_md")
_RECORDED = ("owner", "email", "retries", "retry_delay", "email_on_failure", "email_on_retry")
_GROUP_ARGS = ("group_id", "tooltip", "ui_color", "ui_fgcolor")


def _f(rule: str, severity: str, detail: str) -> dict:
    return {"rule": rule, "severity": severity, "detail": detail}


def _jinja(text: str) -> bool:
    return any(m in text for m in ("{{", "{%", "{#"))


def _recorded(key: str, value, where: str) -> list[dict]:
    """A default_args key or per-task argument: recorded when it is one of `_RECORDED`, otherwise flagged."""
    if key not in _RECORDED:
        return [_f("C95_TASK_ARGUMENT", "flag", f"{where} key {key} is not translated")]
    out = [_f("C05_DEFAULT_ARGS", "info", f"{where} {key}={ast.unparse(value)[:60]} is recorded, not carried")]
    if key == "retries" and not (isinstance(value, ast.Constant) and value.value == 0):
        out.append(_f("C06_RETRIES", "info", "retries are not carried: the AIDP task runs once"))
    return out


def _str(node) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


class _Parser:
    def __init__(self, tree: ast.Module, ctx: Context):
        self.ctx = ctx
        self.imports: dict[str, str] = {}
        self.dicts: dict[str, ast.Dict] = {}
        self.dag_vars: set[str] = set()
        self.dags: list[dict] = []
        self.tasks: dict[str, dict] = {}
        self.var_key: dict[str, str] = {}
        self.group_vars: set[str] = set()
        self.edges: list[tuple[str, str]] = []
        self.findings: list[dict] = []
        self.n = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    self.imports[a.asname or a.name.split(".")[0]] = a.name if a.asname else a.name.split(".")[0]
            elif isinstance(node, ast.ImportFrom):
                base = "." * node.level + (node.module or "")
                for a in node.names:
                    self.imports[a.asname or a.name] = f"{base}.{a.name}"
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                if isinstance(node.value, ast.Dict):
                    self.dicts[node.targets[0].id] = node.value
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) and \
                    self.dotted(node.value.func) in _DAG:
                self.dag_vars |= {t.id for t in node.targets if isinstance(t, ast.Name)}
            elif isinstance(node, ast.withitem) and isinstance(node.context_expr, ast.Call) and \
                    self.dotted(node.context_expr.func) in _DAG and isinstance(node.optional_vars, ast.Name):
                self.dag_vars.add(node.optional_vars.id)
        self.visit(tree.body, [], False)
        self.xcom(tree)

    # ── names ────────────────────────────────────────────────────────────────

    def dotted(self, node) -> str | None:
        if isinstance(node, ast.Name):
            return self.imports.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            base = self.dotted(node.value)
            return f"{base}.{node.attr}" if base else None
        return None

    def operator(self, call: ast.Call):
        """(class name, its path, True if the path goes on past the class) or None."""
        segs = (self.dotted(call.func) or "").split(".")
        for i, s in enumerate(segs):
            if s.endswith(("Operator", "Sensor")):
                return s, ".".join(segs[:i + 1]), i != len(segs) - 1
        return None

    def airflow_call(self, call: ast.Call) -> bool:
        return bool(self.operator(call)) or self.dotted(call.func) in _DAG + (_TASK_GROUP,)

    def f(self, rule, severity, detail):
        self.findings.append(_f(rule, severity, detail))

    # ── statements ───────────────────────────────────────────────────────────

    def visit(self, stmts, group, in_dag):
        for s in stmts:
            if isinstance(s, (ast.Import, ast.ImportFrom, ast.Pass)):
                continue
            if isinstance(s, ast.Expr):
                self.expr(s.value, group, in_dag)
            elif isinstance(s, (ast.Assign, ast.AnnAssign)) and s.value is not None:
                self.assign(s.targets if isinstance(s, ast.Assign) else [s.target], s.value, group, in_dag)
            elif isinstance(s, ast.With):
                self.with_(s, group, in_dag)
            elif isinstance(s, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for d in s.decorator_list:
                    path = self.dotted(d.func if isinstance(d, ast.Call) else d) or ""
                    if path.startswith("airflow.decorators") or path.split(".")[0] in ("task", "dag"):
                        self.f("C92_DYNAMIC_GRAPH", "flag", f"function {s.name} is decorated with @{path}: "
                                                            "TaskFlow tasks are not translated")
                self.dynamic(s, f"function {s.name}")
            else:
                self.dynamic(s, type(s).__name__.lower())

    def dynamic(self, node, where: str):
        """Anything that builds the graph some other way than a plain statement is flagged by name."""
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and self.airflow_call(n):
                op = self.operator(n)
                what = f"{op[0]} ({op[1]})" if op else (self.dotted(n.func) or "?")
                self.f("C92_DYNAMIC_GRAPH", "flag", f"{what} is created inside {where} or with expand/partial: "
                                                    "the task graph is not static")
            elif isinstance(n, ast.BinOp) and isinstance(n.op, (ast.RShift, ast.LShift)) or \
                    isinstance(n, ast.Attribute) and n.attr in ("set_upstream", "set_downstream"):
                self.f("C92_DYNAMIC_GRAPH", "flag", f"dependencies are set inside {where}: not translated")

    def assign(self, targets, value, group, in_dag):
        name = targets[0].id if len(targets) == 1 and isinstance(targets[0], ast.Name) else None
        if isinstance(value, ast.Dict):
            return
        if isinstance(value, ast.Call):
            d = self.dotted(value.func)
            if d in _DAG:
                return self.dag(value)
            op = self.operator(value)
            if op and not op[2]:
                key = self.task(value, group, in_dag)
                if name:
                    self.var_key[name] = key
                return
        self.dynamic(value, "an assignment")

    def expr(self, value, group, in_dag):
        if isinstance(value, ast.BinOp) and isinstance(value.op, (ast.RShift, ast.LShift)):
            self.ref(value, group, in_dag)
        elif isinstance(value, ast.Call):
            d = self.dotted(value.func) or ""
            if isinstance(value.func, ast.Attribute) and value.func.attr in ("set_upstream", "set_downstream"):
                recv, arg = self.ref(value.func.value, group, in_dag), []
                if len(value.args) != 1 or value.keywords:
                    self.f("C92_DYNAMIC_GRAPH", "flag", f"{value.func.attr} with this form of arguments is not translated")
                else:
                    arg = self.ref(value.args[0], group, in_dag)
                up, down = (recv, arg) if value.func.attr == "set_downstream" else (arg, recv)
                self.edges += [(u, v) for u in up for v in down]
            elif _CHAIN.get(d) == "chain":
                self.chain(value, group, in_dag)
            elif _CHAIN.get(d) == "cross":
                args = list(value.args) + [k.value for k in value.keywords]
                if len(args) != 2:
                    return self.f("C92_DYNAMIC_GRAPH", "flag", "cross_downstream with this form of arguments is not translated")
                up, down = self.ref(args[0], group, in_dag), self.ref(args[1], group, in_dag)
                self.edges += [(u, v) for u in up for v in down]
            elif d.startswith("airflow.") and d.rsplit(".", 1)[-1] in ("chain", "cross_downstream"):
                self.f("C90_UNSUPPORTED_OPERATOR", "flag", f"{d} is not a module path this version recognises "
                                                           "(airflow.models.baseoperator is)")
            elif self.operator(value) and not self.operator(value)[2]:
                self.task(value, group, in_dag)
            else:
                self.dynamic(value, "an expression")

    def chain(self, call, group, in_dag):
        seq = []
        for a in call.args:
            if isinstance(a, ast.Starred):
                return self.f("C92_DYNAMIC_GRAPH", "flag", "chain(*...) is not translated")
            seq.append((self.ref(a, group, in_dag), isinstance(a, (ast.List, ast.Tuple))))
        for (a, al), (b, bl) in zip(seq, seq[1:]):
            if al and bl and len(a) == len(b) and len(a) > 1:
                self.edges += list(zip(a, b))
            elif al and bl and len(a) != len(b) and 1 not in (len(a), len(b)):
                self.f("C92_DYNAMIC_GRAPH", "flag", "chain of two lists of different length: not translated")
            else:
                self.edges += [(u, v) for u in a for v in b]

    def ref(self, node, group, in_dag) -> list[str]:
        """The task ids an expression stands for, adding the edges of any `>>` / `<<` inside it."""
        if isinstance(node, ast.Name):
            if node.id in self.var_key:
                return [self.var_key[node.id]]
            if node.id in self.group_vars:
                self.f("C92_DYNAMIC_GRAPH", "flag", f"TaskGroup {node.id} used in a dependency: not translated")
            else:
                self.f("C91_NON_LITERAL", "flag", f"dependency on {node.id}, which is not a task assigned in this file")
            return []
        if isinstance(node, (ast.List, ast.Tuple)):
            return [k for e in node.elts for k in self.ref(e, group, in_dag)]
        if isinstance(node, ast.Call) and self.operator(node) and not self.operator(node)[2]:
            return [self.task(node, group, in_dag)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            left, right = self.ref(node.left, group, in_dag), self.ref(node.right, group, in_dag)
            self.edges += [(l, r) if isinstance(node.op, ast.RShift) else (r, l) for l in left for r in right]
            return right
        self.f("C92_DYNAMIC_GRAPH", "flag", f"dependency on {ast.unparse(node)[:60]} is not translated")
        return []

    def with_(self, s, group, in_dag):
        for item in s.items:
            call, var = item.context_expr, item.optional_vars
            d = self.dotted(call.func) if isinstance(call, ast.Call) else None
            if d in _DAG:
                self.dag(call)
                in_dag = True
            elif d == _TASK_GROUP:
                gid = _str(call.args[0]) if call.args else next((_str(k.value) for k in call.keywords
                                                                  if k.arg == "group_id"), None)
                if gid is None:
                    self.f("C91_NON_LITERAL", "flag", "TaskGroup id is not a literal string")
                bad = [k.arg or "**" for k in call.keywords if k.arg not in _GROUP_ARGS]
                if bad:
                    self.f("C95_TASK_ARGUMENT", "flag", f"TaskGroup argument(s) {', '.join(bad)} are not translated")
                group = group + [gid or "group"]
                if isinstance(var, ast.Name):
                    self.group_vars.add(var.id)
        self.visit(s.body, group, in_dag)

    # ── the DAG ──────────────────────────────────────────────────────────────

    def dag(self, call: ast.Call):
        info = {"dag_id": None, "schedule": "", "extras": []}
        self.dags.append(info)
        if len(call.args) > 1:
            self.f("C95_TASK_ARGUMENT", "flag", "DAG positional arguments after dag_id are not translated")
        for k in call.keywords:
            if k.arg is None:
                self.f("C91_NON_LITERAL", "flag", "DAG(**...) is not translated")
            elif k.arg not in _DAG_ARGS:
                self.f("C95_TASK_ARGUMENT", "flag", f"DAG argument {k.arg} is not translated")
            elif k.arg in ("schedule", "schedule_interval"):
                info["schedule"] = self.schedule(k.value)
            elif k.arg in ("start_date", "catchup", "max_active_runs"):
                info["extras"].append(f"{k.arg} {ast.unparse(k.value)}")
            elif k.arg == "default_args":
                self.default_args(k.value)
        idn = call.args[0] if call.args else next((k.value for k in call.keywords if k.arg == "dag_id"), None)
        info["dag_id"] = _str(idn)
        if info["dag_id"] is None:
            self.f("C91_NON_LITERAL", "flag", "dag_id is not a literal string")
        elif _jinja(info["dag_id"]):
            self.f("C94_JINJA", "flag", "dag_id contains a Jinja template")
        info["schedule"] = info["schedule"] or "none given"
        self.f("C04_SCHEDULE", "info", "schedule recorded as text, not applied: " + "; ".join(
            [info["schedule"]] + info["extras"]))

    def schedule(self, node) -> str:
        text = ast.unparse(node)
        if any(isinstance(n, (ast.Name, ast.Attribute, ast.Call)) and
               (self.dotted(n.func if isinstance(n, ast.Call) else n) or "").rsplit(".", 1)[-1].startswith("Dataset")
               for n in ast.walk(node)):
            self.f("C91_NON_LITERAL", "flag", f"schedule {text} depends on Datasets: not translated")
            return f"dataset {text}"
        if isinstance(node, ast.Constant) and node.value is None:
            return "none"
        v = _str(node)
        if v is not None:
            return f"preset {v}" if v.startswith("@") else f"cron {v}"
        if isinstance(node, ast.Call) and (self.dotted(node.func) or "").endswith("timedelta"):
            return f"timedelta {text}"
        return f"expression {text}"

    def default_args(self, node):
        if isinstance(node, ast.Name) and node.id in self.dicts:
            node = self.dicts[node.id]
        if not isinstance(node, ast.Dict) or any(_str(k) is None for k in node.keys):
            return self.f("C91_NON_LITERAL", "flag", "default_args is not a literal dict")
        for k, v in zip(node.keys, node.values):
            self.findings += _recorded(_str(k), v, "default_args")

    # ── tasks ────────────────────────────────────────────────────────────────

    def task(self, call: ast.Call, group, in_dag) -> str:
        name, path, _ = self.operator(call)
        self.n += 1
        rec = {"cls": name, "path": path, "findings": [], "statements": [], "original": [], "noop": False,
               "id": f"?{self.n}"}
        fs = rec["findings"]
        kinds = _OPERATORS.get(path)
        tid_node = next((k.value for k in call.keywords if k.arg == "task_id"), None)
        tid = _str(tid_node)
        if tid is not None and not _jinja(tid):
            rec["id"] = ".".join(group + [tid])
        if rec["id"] in self.tasks:
            fs.append(_f("C95_TASK_ARGUMENT", "flag", f"task_id {rec['id']} is used more than once"))
            rec["id"] = f"{rec['id']}#{self.n}"
        self.tasks[rec["id"]] = rec
        if kinds is None:
            where = path if "." in path else "no import found for it"
            known = [p for p in _OPERATORS if p.endswith("." + name)]
            why = (f"the module path {path} is not one this version recognises ({known[0].rsplit('.', 1)[0]} is)"
                   if known else "it is not translated")
            fs.append(_f("C90_UNSUPPORTED_OPERATOR", "flag", f"{name} ({where}): {why}"))
            return rec["id"]
        if tid is None:
            fs.append(_f("C91_NON_LITERAL", "flag", f"task_id of a {name} is not a literal string"))
        elif _jinja(tid):
            fs.append(_f("C94_JINJA", "flag", f"task_id {tid} contains a Jinja template"))
        label = rec["id"]
        if call.args:
            fs.append(_f("C95_TASK_ARGUMENT", "flag", f"{label}: positional arguments are not translated"))
        if not in_dag and not any(k.arg == "dag" for k in call.keywords):
            fs.append(_f("C95_TASK_ARGUMENT", "flag", f"{label} is not attached to a DAG (no dag= and not inside "
                                                      "`with DAG`)"))
        config = None
        for k in call.keywords:
            if k.arg is None:
                fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: **{ast.unparse(k.value)} is not translated"))
            elif k.arg == "task_id":
                continue
            elif k.arg == "dag":
                if not (isinstance(k.value, ast.Name) and k.value.id in self.dag_vars):
                    fs.append(_f("C95_TASK_ARGUMENT", "flag", f"{label}: dag= is not the DAG declared in this file"))
            elif k.arg == "trigger_rule":
                if not (_str(k.value) == "all_success" or ast.unparse(k.value).endswith("TriggerRule.ALL_SUCCESS")):
                    fs.append(_f("C95_TASK_ARGUMENT", "flag", f"{label}: trigger_rule {ast.unparse(k.value)} "
                                                              "is not translated"))
            elif k.arg in _RECORDED:
                fs += _recorded(k.arg, k.value, f"{label} argument")
            elif kinds == "bigquery" and k.arg == "configuration":
                config = k.value
            elif kinds == "bigquery" and k.arg in ("location", "project_id"):
                s = _str(k.value)
                if s is None:
                    fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: {k.arg} is not a literal string"))
                elif _jinja(s):
                    fs.append(_f("C94_JINJA", "flag", f"{label}: {k.arg} contains a Jinja template"))
            else:
                fs.append(_f("C95_TASK_ARGUMENT", "flag", f"{label}: argument {k.arg} of {name} is not translated"))
        if kinds == "empty":
            rec["noop"] = True
            fs.append(_f("C02_NO_OP", "rewrite", f"{label}: {name} becomes a task that does nothing"))
        else:
            self.bigquery(rec, config)
        return rec["id"]

    def bigquery(self, rec, config):
        fs, label = rec["findings"], rec["id"]
        if config is None:
            return fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: no configuration"))
        if not isinstance(config, ast.Dict) or any(_str(k) is None for k in config.keys):
            return fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: configuration is not a literal dict"))
        keys = [_str(k) for k in config.keys]
        other = [k for k in keys if k != "query"]
        if other:
            fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: configuration key(s) {', '.join(other)} are not "
                                                        "translated; only a query job is"))
        if "query" not in keys:
            return fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: the configuration is not a query job"))
        inner = config.values[keys.index("query")]
        if not isinstance(inner, ast.Dict) or any(_str(k) is None for k in inner.keys):
            return fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: configuration.query is not a literal dict"))
        ik = [_str(k) for k in inner.keys]
        extra = [k for k in ik if k not in ("query", "useLegacySql")]
        if extra:
            fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: query option(s) {', '.join(extra)} are not translated"))
        if "useLegacySql" in ik:
            legacy = inner.values[ik.index("useLegacySql")]
            if not isinstance(legacy, ast.Constant) or not isinstance(legacy.value, bool):
                fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: useLegacySql is not a literal"))
            elif legacy.value:
                fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: useLegacySql is true: rewrite it in GoogleSQL first"))
        if "query" not in ik:
            return fs.append(_f("C98_BIGQUERY_CONFIG", "flag", f"{label}: no query text"))
        sql = _str(inner.values[ik.index("query")])
        if sql is None:
            return fs.append(_f("C91_NON_LITERAL", "flag", f"{label}: the SQL is not a literal string "
                                                           "(f-string, variable, .format, concatenation or a call)"))
        sql = cleandoc(sql)
        rec["original"] = [sql]
        if _jinja(sql):
            return fs.append(_f("C94_JINJA", "flag", f"{label}: the SQL contains a Jinja template"))
        if any(f["severity"] in ("flag", "block") for f in fs):
            return
        r = translate(sql, self.ctx)
        fs.extend(_f(x.rule, x.severity, x.detail) for x in r.findings)
        if r.status != "blocked":
            rec["statements"] = [r.sql]
            fs.append(_f("C01_BIGQUERY_QUERY", "rewrite", f"{label}: the query runs as translated Spark SQL"))

    def xcom(self, tree):
        for n in ast.walk(tree):
            name = n.attr if isinstance(n, ast.Attribute) else n.id if isinstance(n, ast.Name) else ""
            output = (isinstance(n, ast.Attribute) and n.attr == "output" and isinstance(n.value, ast.Name)
                      and n.value.id in self.var_key)
            if name in ("xcom_pull", "xcom_push") or output:
                return self.f("C92_DYNAMIC_GRAPH", "flag", "XCom or .output is used: data passed between tasks "
                                                           "is not translated")


def translate_dag(src: dict, ctx: Context, job: str) -> tuple[dict, list[dict], bool]:
    """(job, findings, creatable). `job` is {name, dag_id, schedule, tasks, edges, actions}; a task is
    {taskKey, title, statements, assertion, noop, dependsOn}. `creatable` is False when any finding is a
    flag or a block, or when nothing runs: the job is then not created at all."""
    from gcp_aidp.plan.planner import job_name

    empty = {"name": job, "dag_id": src.get("dag_id"), "schedule": "", "tasks": [], "edges": [], "actions": []}
    code = src.get("code") or ""
    if src.get("code_not_scanned") and not code:
        return empty, [_f("C99_NOT_SCANNED", "block", f"the file {src.get('file')} was not scanned: "
                                                      f"{src['code_not_scanned']}")], False
    try:
        p = _Parser(ast.parse(code), ctx)
    except (SyntaxError, ValueError, RecursionError, MemoryError) as exc:
        where = f"line {exc.lineno}: {exc.msg}" if isinstance(exc, SyntaxError) else type(exc).__name__
        return empty, [_f("C93_PARSE_ERROR", "block", f"{src.get('file')} does not parse ({where}); it is not run "
                                                      "and not translated")], False
    if not p.dags:  # what was flagged is reported; otherwise this is a helper module
        if any(f["severity"] in ("flag", "block") for f in p.findings):
            return empty, p.findings, False
        return empty, [_f("C89_NO_DAG", "info", "no DAG is declared in this file (a helper module, or a DAG built "
                                                 "by a function): no job")], False
    extra = list(p.findings)
    if len(p.dags) > 1:
        extra.append(_f("C96_MULTIPLE_DAGS", "flag", f"{len(p.dags)} DAGs in one file: one job per file only"))

    graph: dict[str, list[str]] = {i: [] for i in p.tasks}
    for up, down in p.edges:
        if up in graph and down in graph and up not in graph[down]:
            graph[down].append(up)
    order, cycle = _dfs(graph)
    if cycle:
        extra.append(_f("C97_CYCLE", "block", "dependency cycle: " + " → ".join(cycle)))
    elif p.edges:
        extra.append(_f("C03_DEPENDENCIES", "info", f"{len(p.edges)} dependency edge(s) become dependsOn"))

    keys: dict[str, str] = {}
    taken: set[str] = set()
    for tid in (order if not cycle else list(graph)):
        key = base = job_name(tid.replace(".", "_"))
        n = 1
        while key.casefold() in taken:
            n += 1
            key = f"{base}_{n}"
        taken.add(key.casefold())
        keys[tid] = key
    tasks, actions = [], []
    for tid, key in keys.items():
        r = p.tasks[tid]
        deps = list(dict.fromkeys(keys[d] for d in graph[tid]))
        actions.append({"label": key, "what": r["cls"], "statements": r["statements"], "original": r["original"],
                        "findings": r["findings"], "taskKey": key, "dependsOn": deps})
        tasks.append({"taskKey": key, "title": f"Composer task {tid}", "statements": r["statements"],
                      "assertion": False, "noop": r["noop"], "dependsOn": deps})
    d = p.dags[0]
    if not tasks:
        extra.append(_f("C89_NO_DAG", "info", "the DAG has no task: no job"))
    findings = extra + [f for r in actions for f in r["findings"]]
    findings = [dict(x) for x in dict.fromkeys(tuple(f.items()) for f in findings)]
    edges = [(keys[u], keys[v]) for v in graph for u in graph[v] if u in keys and v in keys]
    schedule = "; ".join([d["schedule"]] + d["extras"])
    out = {"name": job, "dag_id": d["dag_id"] or src.get("dag_id"), "schedule": schedule, "tasks": tasks,
           "edges": edges, "actions": actions}
    creatable = bool(tasks) and not any(f["severity"] in ("flag", "block") for f in findings)
    return out, findings, creatable
