"""Dataform: repositories per region, each with the actions of its compilation. Migrated in 0.3.

GET only. A compilation result is read, never created, and no workflow is invoked.
The actions come from the first enabled release config's compilation result,
otherwise from the newest compilation result. Schedules are recorded as text.
"""
from __future__ import annotations

from gcp_aidp.gcp_client import GcpClient, GcpError
from gcp_aidp.inventory._common import _per_region, _scan

API = "https://dataform.googleapis.com/v1"
_RELATION = ("relationType", "selectQuery", "disabled", "dependencyTargets", "tags", "partitionExpression",
             "clusterExpressions", "preOperations", "postOperations")
_OPERATIONS = ("queries", "hasOutput", "disabled", "dependencyTargets")
_ASSERTION = ("selectQuery", "dependencyTargets", "disabled")
_PRESENCE = ("declaration", "notebook", "dataPreparation")  # only the kind matters


def _target(t: dict) -> dict:
    return {k: t.get(k, "") for k in ("database", "schema", "name")}


def _pick(body: dict, keys) -> dict:
    out = {k: body[k] for k in keys if k in body}
    if "dependencyTargets" in out:
        out["dependencyTargets"] = [_target(t) for t in out["dependencyTargets"] or []]
    return out


def _action(a: dict) -> dict:
    """The fields the translator uses; nothing else is kept."""
    out = {"target": _target(a.get("target") or {}), "filePath": a.get("filePath", "")}
    if "relation" in a:
        out["relation"] = _pick(a["relation"], _RELATION)
        inc = a["relation"].get("incrementalTableConfig")
        if inc:
            out["relation"]["incrementalTableConfig"] = _pick(inc, ("incrementalSelectQuery", "uniqueKeyParts"))
    if "operations" in a:
        out["operations"] = _pick(a["operations"], _OPERATIONS)
    if "assertion" in a:
        out["assertion"] = _pick(a["assertion"], _ASSERTION)
    out.update({k: {} for k in _PRESENCE if k in a})
    return out


def _schedule(kind: str, c: dict) -> dict:
    return {"kind": kind, "name": c["name"].rsplit("/", 1)[-1], "cronSchedule": c.get("cronSchedule", ""),
            "timeZone": c.get("timeZone", ""), "disabled": str(bool(c.get("disabled"))).lower()}


def _repository(client: GcpClient, resource: str, region: str, not_scanned: dict[str, str]) -> dict:
    name = resource.rsplit("/", 1)[-1]
    item = {"name": name, "region": region, "schedules": [], "actions": []}

    def configs(collection: str) -> list[dict]:
        try:
            return client.pages(f"{API}/{resource}/{collection}", collection)
        except GcpError as exc:
            not_scanned[f"{collection} of {name}"] = str(exc)
            return []

    release, workflow = configs("releaseConfigs"), configs("workflowConfigs")
    item["schedules"] = ([_schedule("release_config", c) for c in release]
                         + [_schedule("workflow_config", c) for c in workflow])
    try:
        chosen = next((c["releaseCompilationResult"] for c in release
                       if not c.get("disabled") and c.get("releaseCompilationResult")), None)
        item["compiled_from"] = "an enabled release config"
        if chosen is None:
            # ponytail: lists every compilation result to find the newest; fine until a repository holds thousands.
            results = client.pages(f"{API}/{resource}/compilationResults", "compilationResults")
            chosen = max(results, key=lambda r: r.get("createTime", ""), default={}).get("name")
            item["compiled_from"] = "the newest compilation result"
        if chosen is None:
            raise GcpError("the repository has no enabled release config with a compilation result "
                           "and no compilation results")
        item["compiled_from"] += f" ({chosen.rsplit('/', 1)[-1]})"
        errors = client.get(f"{API}/{chosen}").get("compilationErrors") or []
        if errors:
            first = str(errors[0].get("message", "")).splitlines()[:1] or [""]
            raise GcpError(f"compilation result {chosen.rsplit('/', 1)[-1]} has {len(errors)} compilation "
                           f"error(s); first: {first[0][:200]}")
        item["actions"] = [_action(a) for a in
                           client.pages(f"{API}/{chosen}:query", "compilationResultActions")]
    except GcpError as exc:
        item["actions"] = []
        item["actions_not_scanned"] = str(exc)
        not_scanned[f"actions of {name}"] = str(exc)
    return item


def scan(client: GcpClient, *, regions=("us-central1",), **_) -> dict:
    not_scanned: dict[str, str] = {}

    def repositories():
        return _per_region(regions, lambda r: [
            _repository(client, x["name"], r, not_scanned)
            for x in client.pages(f"{API}/projects/{client.project}/locations/{r}/repositories", "repositories")])
    out = _scan({"repositories": repositories})
    if not_scanned:
        out["summary"].setdefault("not_scanned", {}).update(not_scanned)
    return out
