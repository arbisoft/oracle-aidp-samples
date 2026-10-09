"""Verify a complete migration report and classify each result.

PASS means "translated, and no known
issue was detected", not "this runs on Spark": nothing here parses or executes
the artifact, so a construct no rule covers is reported clean.

  ok                   → PASS
  needs_manual_review  → REVIEW
  blocked              → REVIEW  (not translated; a human must rewrite it)
  reported / skipped   → SKIP
  error, or a report that contradicts itself → FAIL
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from gcp_aidp.migrate.runner import IN_PROGRESS_MARKER, STATUSES

_VERDICT = {"ok": "PASS", "needs_manual_review": "REVIEW", "blocked": "REVIEW",
            "reported": "SKIP", "skipped": "SKIP", "error": "FAIL"}
_WITH_ARTIFACT = {"ok", "needs_manual_review", "blocked"}


def _artifact_problem(output_path: object, report_path: Path) -> str | None:
    if not isinstance(output_path, str) or not output_path.strip():
        return "result is missing output_path"
    report_dir = report_path.resolve().parent
    raw = Path(output_path)
    if raw.is_absolute() or ".." in raw.parts:
        return "output_path must stay inside the migration directory"
    resolved = (report_dir / raw).resolve()
    if os.path.commonpath((str(report_dir), str(resolved))) != str(report_dir):
        return "output_path escapes the migration directory"
    if not resolved.is_file():
        return "output artifact is missing"
    return None


def verify(report_path: Path) -> dict:
    report_path = Path(report_path)
    if (report_path.resolve().parent / IN_PROGRESS_MARKER).exists():
        raise ValueError("migration is incomplete: the in-progress marker exists; re-run migrate")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or report.get("complete") is not True:
        raise ValueError("migration report is incomplete")
    results = report.get("results")
    if not isinstance(results, list):
        raise ValueError("migration report field 'results' must be a JSON array")
    counts = report.get("counts") or {}
    for status in STATUSES:
        actual = sum(isinstance(r, dict) and r.get("status") == status for r in results)
        if counts.get(status) != actual:
            raise ValueError(f"report count mismatch for {status!r}: says {counts.get(status)}, results hold {actual}")

    for nb in report.get("notebooks", []):
        problem = _artifact_problem(nb, report_path)
        if problem:
            raise ValueError(f"data-plane notebook {nb}: {problem}")

    summary = {"PASS": 0, "REVIEW": 0, "SKIP": 0, "FAIL": 0}
    rows = []
    for i, r in enumerate(results):
        if not isinstance(r, dict) or not isinstance(r.get("asset_id"), str) or not r["asset_id"].strip():
            verdict, asset_id, note, r = "FAIL", f"<invalid-result-{i}>", "result needs an object with asset_id", {}
        else:
            status = r.get("status")
            asset_id = r["asset_id"]
            verdict = _VERDICT.get(status, "FAIL")
            note = r.get("note") or ("" if status in _VERDICT else f"unknown status {status!r}")
            flags = r.get("flags", 0)
            if isinstance(flags, bool) or not isinstance(flags, int) or flags < 0:
                verdict, note = "FAIL", "flags must be a non-negative integer"
            elif status == "ok" and flags:
                verdict, note = "FAIL", "status is 'ok' but flags are present"
            elif status in _WITH_ARTIFACT:
                problem = _artifact_problem(r.get("output_path"), report_path)
                if problem:
                    verdict, note = "FAIL", problem
                elif status == "blocked":
                    note = "blocked: " + "; ".join(f["detail"] for f in r.get("findings", [])
                                                  if f.get("severity") == "block")
        summary[verdict] += 1
        rows.append({"asset_id": asset_id, "verdict": verdict, "changes": r.get("changes", 0),
                     "flags": r.get("flags", 0) if verdict != "FAIL" else 0, "note": note})
    return {"summary": summary, "rows": rows}


def format_verify(result: dict) -> str:
    s = result["summary"]
    lines = [
        "verify summary:",
        f"  PASS:   {s['PASS']}",
        f"  REVIEW: {s['REVIEW']}",
        f"  SKIP:   {s['SKIP']}",
        f"  FAIL:   {s['FAIL']}",
        "",
        "  PASS = translated, no known issue detected -- not execution-verified.",
        "         Nothing parses or runs the artifact, so a construct no rule",
        "         covers is reported clean. Review before running in production.",
        "",
        "per-asset:",
    ]
    for r in result["rows"]:
        suffix = f"  changes={r['changes']}" if r["changes"] else ""
        if r["flags"]:
            suffix += f" flags={r['flags']}"
        if r["note"]:
            suffix += f"  ({r['note']})"
        lines.append(f"  {r['verdict']:<6s} {r['asset_id']}{suffix}")
    return "\n".join(lines)
