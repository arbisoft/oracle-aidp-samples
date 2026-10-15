"""Copy Cloud Storage buckets to OCI Object Storage (`transfer`).

The rules, as for `export` and `publish`:

  * Dry run by default: it lists, per bucket, what would be copied, and sends nothing.
    `--apply` copies.
  * Read-only on Google Cloud: the migration's read-only key reads the files. OCI writes
    use ~/.oci/config.
  * Never delete, never overwrite: a file already in OCI with the same MD5 (or size, for a
    composite object) is skipped, so a rerun resumes. The OCI bucket must exist; this
    never creates one.

What is copied is what `migrate` wrote to transfers/<bucket>.transfer.json, one file per
bucket in the plan. The copy is `_copy`, shared with `export`.
"""
from __future__ import annotations

import json
from pathlib import Path

from gcp_aidp import _copy
from gcp_aidp._atomic import write_text_atomic
from gcp_aidp._copy import CopyError
from gcp_aidp.gcp_client import READ_ONLY_SCOPE
from gcp_aidp.migrate.runner import IN_PROGRESS_MARKER

SPEC_SUFFIX = ".transfer.json"


class TransferError(CopyError):
    """The transfer cannot run as it stands; nothing was sent."""


def load(out_dir: str | Path, buckets: list[str] | None = None) -> list[dict]:
    out = Path(out_dir)
    if (out / IN_PROGRESS_MARKER).exists():
        raise TransferError(f"{out}: migrate did not finish; run it again first")
    specs = []
    for p in sorted((out / "transfers").glob(f"*{SPEC_SUFFIX}")):
        try:
            specs.append(json.loads(p.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            raise TransferError(f"{p}: {exc}") from exc
    if not specs:
        raise TransferError("no Cloud Storage bucket in this migration (inventory --sources gcs)")
    if buckets:
        unknown = sorted(set(buckets) - {s["gcs_bucket"] for s in specs})
        if unknown:
            raise TransferError(f"not in this migration: {', '.join(unknown)} "
                                f"(it has {', '.join(s['gcs_bucket'] for s in specs)})")
        specs = [s for s in specs if s["gcs_bucket"] in buckets]
    missing = [s["gcs_bucket"] for s in specs if not s.get("namespace")]
    if missing:
        raise TransferError(f"no OCI namespace for {', '.join(missing)}: set OCI_NAMESPACE (or plan --namespace), "
                            "then plan and migrate again")
    return specs


def transfer(out_dir: str | Path, *, apply: bool = False, buckets: list[str] | None = None, reader=None,
             oci_client=None, log=print) -> dict:
    """Dry run: per bucket, the files and bytes still to copy. With apply: copy them."""
    specs = load(out_dir, buckets)
    reader = reader or _copy.session(READ_ONLY_SCOPE)
    oci_client = oci_client or _copy.oci_client()
    result = {"applied": apply, "buckets": {}}
    for s in specs:
        label = f"gs://{s['gcs_bucket']} → oci://{s['oci_bucket']}@{s['namespace']}"
        try:
            if apply:
                rec = _copy.copy(reader, oci_client, s["namespace"], s["gcs"], s["oci"], s["gcs_bucket"], log)
                log(_copy.copied_line(label, rec))
            else:
                objects, todo = _copy.plan_copy(reader, oci_client, s["namespace"], s["gcs"], s["oci"])
                rec = {"files": len(objects), "to_copy": len(todo), "bytes_to_copy": sum(int(o["size"]) for o, _ in todo),
                       "already_in_oci": len(objects) - len(todo)}
                log(f"  would copy        {label}  {rec['to_copy']} of {rec['files']} file(s), "
                    f"{rec['bytes_to_copy'] / 1e6:.1f} MB; {rec['already_in_oci']} already in OCI")
        except Exception as exc:  # noqa: BLE001 - one bucket's failure is recorded; the others go on
            rec = {"error": f"{type(exc).__name__}: {exc}"}
            log(f"  FAILED            {label}  {rec['error']}")
        result["buckets"][s["gcs_bucket"]] = rec
    if apply:
        path = write_text_atomic(Path(out_dir) / "transfers" / "transfer_report.json",
                                 json.dumps(result["buckets"], indent=2))
        result["report"] = str(path)
    return result
