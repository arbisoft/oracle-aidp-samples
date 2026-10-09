"""A Dataform repository's compiled actions → one AIDP job, one task per action.

Each rule is recorded as a finding (references/dataform-translation.md):

  DF01_TABLE              table → CREATE TABLE IF NOT EXISTS ... AS, then INSERT OVERWRITE
  DF02_VIEW               view → CREATE VIEW IF NOT EXISTS
  DF03_ASSERTION          assertion → a task that fails when its query returns a row
  DF04_INCREMENTAL        incremental table: first run and merge not translated (flag)
  DF05_RELATION_TYPE      materialized view, external, snapshot or unknown type (flag)
  DF06_OPERATIONS         custom operations, pre/post operations (flag)
  DF07_DECLARATION        external source: no task (info)
  DF08_DISABLED           disabled action: no task (info)
  DF09_NOT_TRANSLATED_ACTION  notebook, data preparation or unknown action (flag)
  DF10_CYCLE              dependency cycle (block)
  DF11_CROSS_PROJECT      target in another project (flag)
  DF12_DEPENDENCY_OUTSIDE dependency that is not an action of the repository (info)
  DF13_LAYOUT             partitionExpression / clusterExpressions not carried (info)
  DF14_NOT_SCANNED        the actions could not be read (block)
  DF15_NO_TASKS           nothing in the repository runs, so no job (info)

A job is created only if no finding is a flag or a block: there are no partial jobs.
"""
from __future__ import annotations

from gcp_aidp.translate import ddl
from gcp_aidp.translate.googlesql_to_spark import Context, translate

_KINDS = ("declaration", "relation", "operations", "assertion", "notebook", "dataPreparation")
_OUTPUTS = ("TABLE", "VIEW")


def _f(rule: str, severity: str, detail: str) -> dict:
    return {"rule": rule, "severity": severity, "detail": detail}


def _dfs(graph: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    """(post-order, first cycle found) of a depth-first walk in the graph's key order.
    Iterative, so a long chain cannot hit the recursion limit."""
    state: dict[str, int] = {}  # 1 on the stack, 2 done
    order: list[str] = []
    cycle: list[str] = []
    for root in graph:
        if root in state:
            continue
        state[root] = 1
        path, stack = [root], [(root, iter(graph[root]))]
        while stack:
            node, edges = stack[-1]
            for nxt in edges:
                if nxt not in graph:
                    continue
                if state.get(nxt) == 1:
                    cycle = cycle or path[path.index(nxt):] + [nxt]
                elif nxt not in state:
                    state[nxt] = 1
                    path.append(nxt)
                    stack.append((nxt, iter(graph[nxt])))
                    break
            else:
                stack.pop()
                path.pop()
                state[node] = 2
                order.append(node)
    return order, cycle


def find_cycle(graph: dict[str, list[str]]) -> list[str]:
    """A dependency cycle as [a, b, ..., a] over {key: [keys it depends on]}, or [] if there is none."""
    return _dfs(graph)[1]


def _label(target: dict) -> str:
    return ".".join(x for x in (target.get("schema"), target.get("name")) if x)


def _kind(action: dict) -> str:
    return next((k for k in _KINDS if k in action), "")


def _deps(action: dict) -> list[str]:
    return [_label(t) for t in (action.get(_kind(action)) or {}).get("dependencyTargets") or []]


def register_outputs(repo: dict, ctx: Context, catalog: str) -> None:
    """Names a query can reference: the tables and views this repository creates."""
    for a in repo.get("actions") or []:
        rel = a.get("relation") or {}
        if rel.get("relationType") in _OUTPUTS and not rel.get("disabled"):
            t = a["target"]
            ctx.relations[(t["schema"], t["name"])] = (catalog, t["schema"], t["name"])


def schedule_text(repo: dict) -> str:
    """The source schedules, for a job description and plan notes. They are never applied."""
    parts = [f"{s['kind'].replace('_', ' ')} {s['name']} {s['cronSchedule']} {s['timeZone']}".strip()
             + (" (disabled)" if s.get("disabled") == "true" else "")
             for s in repo.get("schedules") or [] if s.get("cronSchedule")]
    return "; ".join(parts) or "none"


def _translate_action(a: dict, ctx: Context, catalog: str) -> dict:
    """One action's record: what it is, its statements (none: no task), and its findings."""
    kind, t = _kind(a), a.get("target") or {}
    body = a.get(kind) or {}
    label = _label(t) or a.get("filePath") or "?"
    what = (body.get("relationType") or "relation").lower() if kind == "relation" else kind or "unknown"
    rec = {"label": label, "what": what, "statements": [], "original": [], "assertion": False,
           "findings": [], "deps": _deps(a)}
    fs = rec["findings"]
    if kind == "declaration":
        fs.append(_f("DF07_DECLARATION", "info", f"{label} is an external source declared in Dataform: no task, "
                                                  "read as an existing table"))
        return rec
    if body.get("disabled"):
        fs.append(_f("DF08_DISABLED", "info", f"{label} is disabled: no task, it does not run in Dataform either"))
        return rec
    if t.get("database") and ctx.project and t["database"] != ctx.project:
        fs.append(_f("DF11_CROSS_PROJECT", "flag", f"{label} is in project {t['database']}, not {ctx.project}: "
                                                    "it would land in the plan's catalog; decide where it belongs"))
    target = {"catalog": catalog, "schema": t.get("schema", ""), "name": t.get("name", "")}

    def run(query: str):
        rec["original"] = [query]
        r = translate(query, ctx)
        fs.extend(_f(x.rule, x.severity, x.detail) for x in r.findings)
        return None if r.status == "blocked" else r.sql

    if kind == "assertion":
        sql = run(body.get("selectQuery", ""))
        if sql is not None:
            rec.update(what="assertion", statements=[sql], assertion=True)
            fs.append(_f("DF03_ASSERTION", "rewrite", f"{label}: the task fails if the query returns any row"))
    elif kind == "operations":
        rec["original"] = list(body.get("queries") or [])
        fs.append(_f("DF06_OPERATIONS", "flag", f"{label}: custom operations are SQL scripts and are not "
                                                 "translated; they are left as written"))
    elif kind == "relation":
        rtype = body.get("relationType") or ""
        flagged = False
        if body.get("preOperations") or body.get("postOperations"):
            flagged = True
            fs.append(_f("DF06_OPERATIONS", "flag", f"{label}: pre and post operations are SQL scripts and are "
                                                     "not translated; they are left as written"))
        if rtype == "INCREMENTAL_TABLE":
            flagged = True
            fs.append(_f("DF04_INCREMENTAL", "flag", f"{label}: first-run and merge semantics of an incremental "
                                                      "table are not translated yet"))
        elif rtype not in _OUTPUTS:
            flagged = True
            fs.append(_f("DF05_RELATION_TYPE", "flag", f"{label}: relation type {rtype or 'unknown'} "
                                                        "is not translated"))
        if not flagged:
            sql = run(body.get("selectQuery", ""))
            if sql is not None and rtype == "TABLE":
                rec.update(what="table", statements=list(ddl.materialized_view(target, sql)))
                fs.append(_f("DF01_TABLE", "rewrite", f"{label}: CREATE TABLE IF NOT EXISTS ... AS, then "
                                                       "INSERT OVERWRITE"))
            elif sql is not None:
                rec.update(what="view", statements=[ddl.create_view(target, sql)])
                fs.append(_f("DF02_VIEW", "rewrite", f"{label}: CREATE VIEW IF NOT EXISTS"))
            if sql is not None and (body.get("partitionExpression") or body.get("clusterExpressions")):
                fs.append(_f("DF13_LAYOUT", "info", f"{label}: partitionExpression and clusterExpressions are "
                                                     "not carried; the table is created unpartitioned"))
    else:
        fs.append(_f("DF09_NOT_TRANSLATED_ACTION", "flag", f"{label}: a {kind or 'unknown'} action is not "
                                                           "translated"))
    return rec


def translate_repository(repo: dict, ctx: Context, catalog: str, job: str) -> tuple[dict, list[dict], bool]:
    """(job, findings, creatable). `job` is {name, tasks, actions}; a task is {taskKey, title,
    statements, assertion, dependsOn}. `creatable` is False when any finding is a flag or a block,
    or when nothing runs: the job is then not created at all."""
    from gcp_aidp.plan.planner import job_name

    actions = repo.get("actions") or []
    recs = [_translate_action(a, ctx, catalog) for a in actions]
    by_label = {r["label"]: r for r in recs}
    extra: list[dict] = []
    if not actions and repo.get("actions_not_scanned"):
        extra.append(_f("DF14_NOT_SCANNED", "block", f"the actions of {repo.get('name')} were not scanned: "
                                                      f"{repo['actions_not_scanned']}"))

    graph = {r["label"]: [d for d in r["deps"] if d in by_label] for r in recs}
    order, cycle = _dfs(graph)
    if cycle:
        extra.append(_f("DF10_CYCLE", "block", "dependency cycle: " + " → ".join(cycle)))
    for r in recs:
        for d in r["deps"]:
            if d not in by_label:
                r["findings"].append(_f("DF12_DEPENDENCY_OUTSIDE", "info", f"{r['label']} depends on {d}, which "
                                        "is not an action of this repository: read as an existing table"))

    keys: dict[str, str] = {}  # label → task key, dependencies first
    taken: set[str] = set()
    for label in (order if not cycle else list(by_label)):
        if by_label[label]["statements"]:
            key = base = job_name(label.replace(".", "_"))
            n = 1
            while key.casefold() in taken:
                n += 1
                key = f"{base}_{n}"
            taken.add(key.casefold())
            keys[label] = key
    tasks = []
    for label, key in keys.items():
        r = by_label[label]
        r["taskKey"] = key
        r["dependsOn"] = list(dict.fromkeys(keys[d] for d in r["deps"] if d in keys))
        tasks.append({"taskKey": key, "title": f"Dataform {r['what']} {label}", "statements": r["statements"],
                      "assertion": r["assertion"], "dependsOn": r["dependsOn"]})
    if not tasks and not extra:
        extra.append(_f("DF15_NO_TASKS", "info", "no action of this repository runs (declarations, disabled "
                                                  "or none): no job is created"))
    findings = list(dict.fromkeys(tuple(f.items()) for r in recs for f in r["findings"]))
    findings = [dict(f) for f in findings] + extra
    creatable = bool(tasks) and not any(f["severity"] in ("flag", "block") for f in findings)
    return {"name": job, "tasks": tasks, "actions": recs}, findings, creatable
