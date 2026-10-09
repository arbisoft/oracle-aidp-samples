"""`gcp-aidp <verb>`: inventory, plan, migrate, verify, publish, run."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from importlib.resources import files
from pathlib import Path

from gcp_aidp import __version__
from gcp_aidp._env import load_dotenv
from gcp_aidp.inventory.manifest import ALL_SOURCES

_FIXTURE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


def _load_json_object(path, label: str) -> dict:
    """Load a command input and fail closed when its JSON root is not an object."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object: {path}")
    return value


def _fixture_path(name: str):
    """Resolve a bundled fixture name without allowing filesystem traversal."""
    if not _FIXTURE_NAME_RE.fullmatch(name):
        raise ValueError("invalid fixture name; use 1-64 letters, digits, underscores, or hyphens")
    resource = files("gcp_aidp.fixtures").joinpath(f"{name}-manifest.json")
    if not resource.is_file():
        raise ValueError(f"bundled fixture not found: {name}")
    return resource


def _parse_sources(raw: str | None) -> tuple[str, ...]:
    if raw is None:
        return ALL_SOURCES
    sources = tuple(dict.fromkeys(p.strip().lower() for p in raw.split(",") if p.strip()))
    bad = [s for s in sources if s not in ALL_SOURCES]
    if not sources or bad:
        detail = f"unknown source(s): {bad}" if bad else "source list is empty"
        raise ValueError(f"{detail}; valid: {ALL_SOURCES}")
    return sources


def cmd_inventory(args: argparse.Namespace) -> int:
    from gcp_aidp.inventory.manifest import summarize, write_manifest

    sources = _parse_sources(args.sources)
    if args.fixture is None:
        from gcp_aidp.gcp_client import GcpClient
        from gcp_aidp.inventory.manifest import build_manifest

        client = GcpClient(args.project or os.environ.get("GCP_PROJECT", ""))
        try:
            client.session  # fail fast on missing google-auth or credentials, before any scan
        except Exception as exc:  # noqa: BLE001 - reported, never echoing a credential
            print(f"error: cannot authenticate to Google Cloud: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 2
        regions = tuple(r.strip() for r in args.regions.split(",") if r.strip())
        print(f"# project={client.project}  regions={','.join(regions)}  (read-only metadata calls)")
        log = lambda line: print(f"[{time.strftime('%H:%M:%S')}] {line}", flush=True)
        if args.scan_services:
            print("# --scan-services: Dataproc, Composer, Dataform, Dataflow and Vertex AI are listed with a "
                  "cloud-platform token (GET calls only)")
        manifest = build_manifest(client, sources, regions=regions, saved_queries_dir=args.saved_queries_dir,
                                  scan_services=args.scan_services, log=log)
    else:
        path = _fixture_path(args.fixture)
        manifest = _load_json_object(path, "fixture manifest")
        fixture_sources = manifest.get("sources")
        if not isinstance(fixture_sources, dict):
            raise ValueError("fixture manifest field 'sources' must be a JSON object")
        missing = [s for s in sources if s not in fixture_sources]
        if missing:
            raise ValueError("fixture does not contain requested source(s): " + ", ".join(missing))
        manifest["sources"] = {s: fixture_sources[s] for s in sources}
        manifest["sources_scanned"] = list(sources)
        print(f"[fixture] using {path}")

    project = manifest.get("project_id") or "unknown"
    out = Path(args.output or f"inventory-{project}-{time.strftime('%Y%m%dT%H%M%S')}.json")
    write_manifest(manifest, out)
    print(f"\n# wrote {out}\n")
    print(summarize(manifest))
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    from gcp_aidp.plan import build_plan, summarize_plan, write_plan, write_plan_markdown

    manifest = _load_json_object(Path(args.manifest), "manifest")
    ns = args.namespace or os.environ.get("OCI_NAMESPACE") or "<your-oci-namespace>"
    datasets = None
    if args.datasets is not None:
        datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
        if not datasets:
            raise ValueError("--datasets is empty; leave it out to plan every dataset")
    plan = build_plan(manifest, oci_namespace=ns, catalog=args.catalog,
                      bignumeric=args.bignumeric, geography=args.geography, datasets=datasets)
    out = Path(args.output) if args.output else Path(args.manifest).with_suffix(".plan.json")
    write_plan(plan, out)
    md = write_plan_markdown(plan, out.with_suffix(".md"))
    print(f"# wrote {out} and {md} (approval document)\n")
    print(summarize_plan(plan))
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    from gcp_aidp.migrate import migrate

    plan = _load_json_object(Path(args.plan), "plan")
    out_dir = Path(args.out_dir)
    print(f"# plan={plan.get('plan_id')}  out={out_dir}  (offline: nothing is sent to AIDP)\n")
    report = migrate(plan, out_dir=out_dir, log=lambda line: print(line, flush=True))
    c = report["counts"]
    print(f"\n# done. ok={c['ok']}  needs_review={c['needs_manual_review']}  blocked={c['blocked']}  "
          f"reported={c['reported']}  skipped={c['skipped']}  error={c['error']}")
    print(f"# wrote {out_dir}/report.json and {out_dir}/report.md")
    return 0 if c["error"] == 0 else 1


def cmd_verify(args: argparse.Namespace) -> int:
    from gcp_aidp.verify import format_verify, verify

    path = Path(args.report_or_dir)
    if path.is_dir():
        path = path / "report.json"
    if not path.exists():
        print(f"error: {path} not found", file=sys.stderr)
        return 2
    result = verify(path)
    print(format_verify(result))
    return 0 if result["summary"]["FAIL"] == 0 else 1


def _aidp(args: argparse.Namespace) -> dict:
    return {"workspace_key": args.workspace_key or os.environ.get("AIDP_WORKSPACE_KEY"),
            "instance_id": args.instance_id or os.environ.get("AIDP_INSTANCE_ID"),
            "profile": args.profile or os.environ.get("OCI_CLI_PROFILE"), "auth": args.auth or os.environ.get("AIDP_AUTH"),
            "prefix": args.prefix if args.prefix is not None else os.environ.get("AIDP_PREFIX", "")}


def cmd_publish(args: argparse.Namespace) -> int:
    from gcp_aidp.publish import PublishError, publish

    try:
        result = publish(args.out_dir, cluster_key=args.cluster_key or os.environ.get("AIDP_CLUSTER_KEY"),
                         apply=args.apply, reuse_existing=args.reuse_existing_notebooks, **_aidp(args))
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    for b in result["blocked"]:
        print(f"  REFUSED  job {b['job']}: {b['reason']}")
    if not args.apply:
        print(f"dry run: nothing was sent. Would upload to {result['folder']}:")
        for n in result["notebooks"]:
            print(f"    {n['remote']}")
        print("and create, unscheduled:")
        for j in result["jobs"]:
            print(f"    job {j['name']}: " + " → ".join(t["taskKey"] for t in j["definition"]["tasks"]))
        if not result["prefix"]:
            print("warning: no --prefix; --apply refuses to run without one.")
        print("re-run with --apply to publish.")
        return 0
    failed = [x for x in result["notebooks"] + result["jobs"] if x.get("status") in ("error", "refused")]
    failed += result["blocked"]
    print(f"\nuploaded {sum(n['status'] == 'uploaded' for n in result['notebooks'])}/{len(result['notebooks'])} "
          f"notebook(s); created {sum(j.get('status') == 'created' for j in result['jobs'])}/"
          f"{len(result['jobs']) + len(result['blocked'])} job(s); {len(failed)} failed or refused.")
    return 1 if failed else 0


def cmd_run(args: argparse.Namespace) -> int:
    from gcp_aidp.publish import PublishError, run

    try:
        result = run(args.out_dir, job=args.job, wait=args.wait, **_aidp(args))
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if result["ok"]:
        print("run finished: every task succeeded. The verdicts are in MIGRATION_REPORT.md in the reports folder.")
    elif result["finished"]:
        print("run finished with failures (above). Fix the cause, then run again: completed copies are skipped.")
    return 0 if result["ok"] else 1


def _aidp_args(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("out_dir", help="the directory `migrate` wrote")
    sp.add_argument("--prefix", default=None,
                    help="your workspace folder and job-name prefix, so two people never collide "
                         "(default: $AIDP_PREFIX; required with publish --apply)")
    sp.add_argument("--workspace-key", help="AIDP workspace key (default: $AIDP_WORKSPACE_KEY)")
    sp.add_argument("--instance-id", help="AIDP instance OCID (default: $AIDP_INSTANCE_ID)")
    sp.add_argument("--profile", help="~/.oci/config profile (default: $OCI_CLI_PROFILE, else DEFAULT)")
    sp.add_argument("--auth", help="OCI auth mode, e.g. api_key or security_token (default: $AIDP_AUTH, else the aidp CLI's own default, security_token)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gcp-aidp", description="Google Cloud data stack → Oracle AIDP migrator")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    inv = sub.add_parser("inventory", help="scan Google Cloud (read-only, metadata only) and emit a manifest")
    inv.add_argument("--project", help="GCP project id (default: $GCP_PROJECT)")
    inv.add_argument("--sources", help=f"comma-separated subset of {','.join(ALL_SOURCES)} (default: all)")
    inv.add_argument("--regions", default="us-central1",
                     help="comma-separated regions for Dataproc, Composer, Dataform and Vertex AI (default: us-central1)")
    inv.add_argument("--saved-queries-dir", help="a folder of saved queries exported as .sql files")
    inv.add_argument("--scan-services", action="store_true",
                     help="also list Dataproc, Composer, Dataform, Dataflow and Vertex AI; their APIs refuse a "
                          "read-only token, so this asks for a cloud-platform token (still GET calls only)")
    inv.add_argument("--fixture", help="load a bundled fixture manifest instead of scanning (e.g. 'demo')")
    inv.add_argument("-o", "--output", help="manifest output path (default: ./inventory-<project>-<ts>.json)")
    inv.set_defaults(func=cmd_inventory)

    pl = sub.add_parser("plan", help="produce a migration plan and approval document from a manifest")
    pl.add_argument("manifest")
    pl.add_argument("-o", "--output", help="plan path (default: <manifest>.plan.json); the .md sits beside it")
    pl.add_argument("--namespace", help="OCI namespace for target buckets (default: $OCI_NAMESPACE)")
    pl.add_argument("--catalog", help="target INTERNAL catalog (default: the project id, made a valid name)")
    pl.add_argument("--datasets",
                    help="comma-separated BigQuery datasets to migrate (default: all); the others stay in "
                         "the plan as SKIP")
    pl.add_argument("--bignumeric", choices=("block", "string"), default="block",
                    help="BIGNUMERIC columns: block the table (default) or carry exact decimal text")
    pl.add_argument("--geography", choices=("block", "wkt"), default="block",
                    help="GEOGRAPHY columns: block the table (default) or carry WKT text")
    pl.set_defaults(func=cmd_plan)

    mg = sub.add_parser("migrate", help="write translated artifacts and a report locally; contacts nothing")
    mg.add_argument("plan")
    mg.add_argument("-o", "--out-dir", default="./migrated", help="output directory (default: ./migrated)")
    mg.set_defaults(func=cmd_migrate)

    vf = sub.add_parser("verify", help="classify a migrate report into PASS / REVIEW / SKIP / FAIL")
    vf.add_argument("report_or_dir", help="report.json, or the directory migrate wrote")
    vf.set_defaults(func=cmd_verify)

    pb = sub.add_parser("publish", help="upload the notebooks and create the jobs in AIDP; a dry run until --apply")
    _aidp_args(pb)
    pb.add_argument("--apply", action="store_true", help="actually publish; without it nothing is sent")
    pb.add_argument("--cluster-key", help="AIDP cluster every task runs on (default: $AIDP_CLUSTER_KEY)")
    pb.add_argument("--reuse-existing-notebooks", action="store_true",
                    help="let jobs use notebooks already at their paths (finishing a publish whose job "
                         "creation failed); they are still never overwritten")
    pb.set_defaults(func=cmd_publish)

    rn = sub.add_parser("run", help="start a published job on AIDP and follow it to the end")
    _aidp_args(rn)
    rn.add_argument("--job", default="gcp_aidp_migration",
                    help="which of the migration's jobs (default: gcp_aidp_migration, the full copy)")
    rn.add_argument("--wait", type=int, default=6 * 3600, help="seconds to follow the run (default: 6 hours)")
    rn.set_defaults(func=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
