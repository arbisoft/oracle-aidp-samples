"""Push a migration into an AIDP workspace (`publish`) and start its copy job (`run`).

The rules:

  * Dry run by default; `--apply` is the only thing that sends anything.
  * Never overwrite. A notebook path or job name that exists is skipped, and a
    job whose notebooks this run did not upload is refused (unless
    `--reuse-existing-notebooks`): it could otherwise run someone else's.
  * A prefix per person, required with `--apply`: it names the workspace
    folder and the front of every job name.
  * Read the workspace, and check the cluster key, before sending anything.
  * Notebooks first, jobs second. Jobs are created unscheduled.

What is published is what `migrate` recorded in report.json (`notebooks`,
`jobs`), never a glob of the directory.
"""
from __future__ import annotations

import json
import re
import time
from collections import Counter
from pathlib import Path

from gcp_aidp.dataplane import MIGRATION_JOB
from gcp_aidp.migrate.runner import IN_PROGRESS_MARKER
from gcp_aidp.publish.aidp_client import AidpClient, AidpError, AidpUnavailable

DEFAULT_WORKSPACE_ROOT = "/Workspace"
JOB_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
TASK_TIMEOUT_SECONDS = 12 * 3600  # per task; one dataset's copy can run for hours
FINAL = {"SUCCESS", "FAILED", "UPSTREAM_FAILED", "CANCELED", "SKIPPED", "TIMEDOUT", "INTERNAL_ERROR"}


class PublishError(Exception):
    """The migration cannot be published (or run) as it stands; nothing was sent."""


def _client(workspace_key, instance_id, profile, auth) -> AidpClient:
    if not workspace_key:
        raise PublishError("--workspace-key (or AIDP_WORKSPACE_KEY) is required")
    if not AidpClient.available():
        raise PublishError("the `aidp` CLI is not installed; `pip install aidp-cli`")
    return AidpClient(workspace_key=workspace_key, instance_id=instance_id, profile=profile, auth=auth)


def _report(out_dir: Path) -> dict:
    if not (out_dir / "report.json").is_file():
        raise PublishError(f"{out_dir} has no report.json; run `migrate` first")
    if (out_dir / IN_PROGRESS_MARKER).exists():
        raise PublishError(f"{out_dir} holds an interrupted migration ({IN_PROGRESS_MARKER}); re-run `migrate`")
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    if not isinstance(report.get("notebooks"), list) or not isinstance(report.get("jobs"), list):
        raise PublishError(f"{out_dir}/report.json lists no notebooks or jobs; re-run `migrate` with this version")
    return report


def plan_publish(out_dir, *, prefix="", workspace_root=DEFAULT_WORKSPACE_ROOT, cluster_key=None) -> dict:
    """What publishing would do. Reads the migration only; contacts nothing."""
    out_dir = Path(out_dir)
    if prefix and not JOB_NAME.fullmatch(prefix):
        raise PublishError(f"--prefix {prefix!r} must be a letter followed by letters, digits or underscores: "
                           "it starts every job name, and AIDP refuses any other")
    report = _report(out_dir)
    folder = "/".join([workspace_root.rstrip("/")] + ([prefix] if prefix else []))

    notebooks, remote_of = [], {}
    for rel in report["notebooks"]:
        local = (out_dir / rel).resolve()
        if out_dir.resolve() not in local.parents or not local.is_file():
            raise PublishError(f"report.json lists notebook {rel!r}, which is not a file inside {out_dir}; "
                               "re-run `migrate`")
        remote = f"{folder}/{local.name}"  # flat: one folder per prefix
        if remote in remote_of.values():
            raise PublishError(f"two notebooks would land on {remote}")
        remote_of[rel] = remote
        notebooks.append({"local": str(local), "remote": remote})

    jobs, blocked = [], []
    for job in report["jobs"]:
        name = f"{prefix}_{job['name']}" if prefix else job["name"]
        if not JOB_NAME.fullmatch(name):
            blocked.append({"job": name, "reason": "not a name AIDP accepts; re-run `migrate`"})
            continue
        if not cluster_key:
            blocked.append({"job": name, "reason": "no cluster key; an AIDP task cannot run without one "
                                                   "-- pass --cluster-key (or AIDP_CLUSTER_KEY)"})
            continue
        tasks = []
        for t in job["tasks"]:
            parameters = list(t.get("parameters") or [])
            if not t["notebook"].startswith("notebooks/jobs/"):  # the data-plane notebooks keep their reports here
                parameters.append({"name": "reports_dir", "value": f"{folder}/reports"})
            task = {"type": "NOTEBOOK_TASK", "taskKey": t["taskKey"], "runIf": "ALL_SUCCESS", "maxRetries": 0,
                    "notebookPath": remote_of[t["notebook"]], "source": "WORKSPACE",
                    "timeoutSeconds": TASK_TIMEOUT_SECONDS, "cluster": {"clusterKey": cluster_key}}
            if parameters:
                task["parameters"] = parameters
            if t.get("dependsOn"):
                task["dependsOn"] = [{"taskKey": k} for k in t["dependsOn"]]
            tasks.append(task)
        jobs.append({"name": name, "definition": {
            "name": name, "description": f"{job.get('description', '')} Created by gcp-aidp publish, "
                                         "unscheduled. Not execution-verified.".strip(),
            "maxConcurrentRuns": 1, "tasks": tasks}})

    names = Counter(j["name"] for j in jobs)
    for dup in sorted(n for n, c in names.items() if c > 1):  # never pick a winner
        blocked.append({"job": dup, "reason": f"{names[dup]} jobs map to this name"})
    jobs = [j for j in jobs if names[j["name"]] == 1]
    return {"out_dir": str(out_dir), "folder": folder, "prefix": prefix,
            "notebooks": notebooks, "jobs": jobs, "blocked": blocked}


def publish(out_dir, *, workspace_key=None, cluster_key=None, prefix="", workspace_root=DEFAULT_WORKSPACE_ROOT,
            instance_id=None, profile=None, auth=None, apply=False, reuse_existing=False,
            log=print, client=None) -> dict:
    """Dry run unless `apply`. Returns the plan, with a status per notebook and job when applied."""
    if apply and not prefix:
        raise PublishError("--prefix is required with --apply: it keeps your notebooks and jobs apart from "
                           "anyone else's in the workspace; pass --prefix <yourname>")
    planned = plan_publish(out_dir, prefix=prefix, workspace_root=workspace_root, cluster_key=cluster_key)
    planned["applied"] = bool(apply)
    if not apply:
        return planned

    client = client or _client(workspace_key, instance_id, profile, auth)
    log(f"publishing to {planned['folder']} in workspace {workspace_key}")
    try:  # read before writing: if a read fails, nothing has been sent
        present = client.folder_names(planned["folder"])
        existing = {j.get("name") for j in client.list_jobs()} if planned["jobs"] else set()
        problem = client.cluster_problem(cluster_key) if planned["jobs"] else ""
    except (AidpError, AidpUnavailable) as exc:
        raise PublishError(f"cannot read the workspace, so cannot prove nothing would be overwritten; "
                           f"nothing was sent: {exc}") from exc
    if problem:
        raise PublishError(f"{problem}; nothing was sent")

    for upload in planned["notebooks"]:
        name = upload["remote"].rsplit("/", 1)[1]
        if name in present:
            upload.update(status="skipped", reason="already exists (publish never overwrites)")
        else:
            try:
                client.put_notebook(upload["remote"], Path(upload["local"]).read_text(encoding="utf-8"))
                upload["status"] = "uploaded"
            except (AidpError, AidpUnavailable, OSError) as exc:
                upload.update(status="error", reason=str(exc))
        log(f"  {upload['status']:<9}{upload['remote']}" + (f": {upload['reason']}" if upload.get("reason") else ""))

    usable = {"uploaded", "skipped"} if reuse_existing else {"uploaded"}
    unusable = {u["remote"] for u in planned["notebooks"] if u["status"] not in usable}
    for job in planned["jobs"]:
        missing = sorted({t["notebookPath"] for t in job["definition"]["tasks"]} & unusable)
        if job["name"] in existing:
            job.update(status="skipped", reason="a job of this name already exists (publish never overwrites)")
        elif missing:
            job.update(status="refused", reason=f"notebooks not uploaded by this run: {missing} (if they are this "
                                                "migration's, re-run with --reuse-existing-notebooks)")
        else:
            try:
                job.update(status="created", key=client.create_job(job["definition"]))
            except (AidpError, AidpUnavailable) as exc:
                job.update(status="error", reason=str(exc))
        log(f"  {job['status']:<9}job {job['name']}" + (f" ({job['key']})" if job.get("key") else "")
            + (f": {job['reason']}" if job.get("reason") else ""))
    return planned


def readable_failure(output_text: str) -> str:
    """The error text in a task's output (errorTrace, or a cell's ename/evalue), "" if none."""
    try:
        payload = json.loads(output_text)
    except (TypeError, ValueError):
        return ""
    found = []

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("errorTrace", "ename", "evalue") and isinstance(value, str) and value.strip():
                    found.append(value.strip())
                elif key == "traceback" and isinstance(value, list):
                    found.append("\n".join(str(v) for v in value))
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, str) and node.lstrip().startswith(("{", "[")):
            try:
                walk(json.loads(node))
            except ValueError:
                pass

    walk(payload)
    return "\n".join(found)


def run(out_dir, *, workspace_key=None, prefix="", job=MIGRATION_JOB, instance_id=None, profile=None, auth=None,
        wait=6 * 3600, poll=15, log=print, client=None, sleep=time.sleep) -> dict:
    """Start one run of a published job and follow it to the end.

    The copy notebooks skip what is already there, so a failed or stopped run
    is resumed by running it again.
    """
    report = _report(Path(out_dir))
    local = next((j for j in report["jobs"] if j["name"] == job), None)
    if local is None:
        raise PublishError(f"this migration has no job {job!r}; its jobs: {[j['name'] for j in report['jobs']]}")
    name = f"{prefix}_{job}" if prefix else job
    client = client or _client(workspace_key, instance_id, profile, auth)
    try:
        key = next((j.get("key") for j in client.list_jobs() if j.get("name") == name and j.get("key")), None)
        if key is None:
            raise PublishError(f"no job {name!r} in the workspace; run `publish --apply` first")
        run_key = client.run_job(key)
        log(f"started {name} ({key}): run {run_key}")
        seen, tasks = {}, []
        for _ in range(max(1, int(wait / poll)) + 1):
            tasks = client.task_runs(run_key)
            for task_key, status, _message, _ in tasks:
                if seen.get(task_key) != status:
                    seen[task_key] = status
                    log(f"  {time.strftime('%H:%M:%S')}  {task_key:<40} {status}")
            failed = any(s in FINAL and s != "SUCCESS" for _, s, _, _ in tasks)
            if tasks and all(s in FINAL for _, s, _, _ in tasks) and (failed or len(tasks) >= len(local["tasks"])):
                break
            sleep(poll)
        else:
            log(f"still running after {wait}s; follow run {run_key} in the AIDP console")
            return {"run": run_key, "tasks": tasks, "finished": False, "ok": False}
        for task_key, status, message, task_run in tasks:
            if status not in ("SUCCESS", "UPSTREAM_FAILED", "SKIPPED"):
                detail = readable_failure(client.task_output(task_run)) if task_run else ""
                first = ((message or "").strip().splitlines() or [""])[0]
                log(f"\n{task_key} {status}: {first}\n{detail[-2000:]}".rstrip())
    except (AidpError, AidpUnavailable) as exc:
        raise PublishError(str(exc)) from exc
    ok = all(s == "SUCCESS" for _, s, _, _ in tasks)
    return {"run": run_key, "tasks": tasks, "finished": True, "ok": ok}
