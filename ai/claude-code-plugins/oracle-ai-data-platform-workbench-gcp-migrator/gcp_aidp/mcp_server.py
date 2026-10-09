"""MCP server for gcp-aidp-migrator.

Exposes the four offline migration verbs as MCP tools so any MCP-compatible
client (OpenAI Codex, Cursor, Claude Desktop, etc.) gets the same workflow the
Claude Code plugin does. Each tool shells out to the already-tested
`gcp_aidp.cli` pipeline rather than reimplementing logic. `publish` and `run`
change an AIDP workspace, so they stay CLI-only.

Run:   gcp-aidp-mcp                 (after `pip install -e '.[mcp]'`)
   or:  python3 -m gcp_aidp.mcp_server
"""
from __future__ import annotations

import subprocess
import sys
from typing import Optional

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "The compatible MCP SDK (1.x) is required. Install it with: "
        "pip install -e '.[mcp]'"
    ) from e

mcp = FastMCP("gcp-aidp-migrator")


def _run(args: list[str], *, timeout: float = 300.0) -> str:
    """Run `python -m gcp_aidp.cli <args>` and return combined output."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "gcp_aidp.cli", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            # Detach the child's stdin from the MCP stdio transport pipe, or the
            # spawned CLI blocks forever at interpreter start on Windows.
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        output = "".join(part for part in (e.stdout, e.stderr) if isinstance(part, str))
        return f"[exit timeout after {timeout:g}s]\n{output}"
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        return f"[exit {proc.returncode}]\n{out}"
    return out or "[ok, no output]"


@mcp.tool()
def inventory(
    project: Optional[str] = None,
    fixture: Optional[str] = None,
    sources: Optional[str] = None,
    regions: Optional[str] = None,
    saved_queries_dir: Optional[str] = None,
    scan_services: bool = False,
    output: str = "inv.json",
) -> str:
    """Read-only, metadata-only scan of a Google Cloud data stack → migration manifest.

    Args:
        project: GCP project id (default: $GCP_PROJECT; ignored if fixture is set).
        fixture: use a bundled fixture (e.g. "demo") instead of live Google Cloud.
        sources: comma-separated subset of the sources to scan (default: all).
        regions: comma-separated regions for Dataproc, Composer, Dataform and Vertex AI.
        saved_queries_dir: a folder of saved queries exported as .sql files.
        scan_services: also list Dataproc, Composer, Dataform, Dataflow and Vertex AI
            (asks for a cloud-platform token; still GET calls only).
        output: manifest output path.
    """
    args = ["inventory", "-o", output]
    if fixture:
        args += ["--fixture", fixture]
    elif project:
        args += ["--project", project]
    if sources:
        args += ["--sources", sources]
    if regions:
        args += ["--regions", regions]
    if saved_queries_dir:
        args += ["--saved-queries-dir", saved_queries_dir]
    if scan_services:
        args.append("--scan-services")
    return _run(args)


@mcp.tool()
def plan(
    manifest: str,
    output: str = "plan.json",
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    datasets: Optional[str] = None,
    dataform_repos: Optional[str] = None,
    dags: Optional[str] = None,
    bignumeric: str = "block",
    geography: str = "block",
) -> str:
    """Turn an inventory manifest into an AIDP migration plan and approval document.

    Args:
        manifest: path to the inventory manifest (from `inventory`).
        output: plan output path; the approval .md is written beside it.
        namespace: OCI namespace for target buckets (default: $OCI_NAMESPACE).
        catalog: target AIDP catalog (default: the project id, made a valid name).
        datasets: comma-separated BigQuery datasets to migrate (default: all); the
            others stay in the plan as SKIP.
        dataform_repos: comma-separated Dataform repositories to migrate (default: all);
            the others stay in the plan as SKIP.
        dags: comma-separated Composer DAG ids to migrate (default: all); the others
            stay in the plan as SKIP.
        bignumeric: BIGNUMERIC columns: "block" the table or carry exact decimal "string".
        geography: GEOGRAPHY columns: "block" the table or carry "wkt" text.
    """
    args = ["plan", manifest, "-o", output, "--bignumeric", bignumeric, "--geography", geography]
    if namespace:
        args += ["--namespace", namespace]
    if catalog:
        args += ["--catalog", catalog]
    if datasets:
        args += ["--datasets", datasets]
    if dataform_repos:
        args += ["--dataform-repos", dataform_repos]
    if dags:
        args += ["--dags", dags]
    return _run(args)


@mcp.tool()
def migrate(plan_path: str, out_dir: str = "migrated") -> str:
    """Translate a migration plan (GoogleSQL→Spark SQL, DDL, data-copy notebooks) locally.

    Contacts nothing: artifacts and a report are written to out_dir.

    Args:
        plan_path: path to the plan (from `plan`).
        out_dir: output directory for translated artifacts + report.
    """
    return _run(["migrate", plan_path, "-o", out_dir])


@mcp.tool()
def verify(report_or_dir: str = "migrated") -> str:
    """Classify a migration's output as PASS / REVIEW / SKIP / FAIL.

    Args:
        report_or_dir: path to a migrate report.json or the directory containing it.
    """
    return _run(["verify", report_or_dir])


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
