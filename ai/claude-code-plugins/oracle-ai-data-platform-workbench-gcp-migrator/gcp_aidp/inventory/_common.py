"""Shared by the per-service scans: metadata GETs only.

A service whose API is not enabled cannot hold resources, so it is recorded as
`api_disabled` with nothing found, not as an error. A call that is refused for
any other reason is recorded under `not_scanned`.
"""
from __future__ import annotations

from typing import Callable

from gcp_aidp.gcp_client import GcpError

GCS = "https://storage.googleapis.com/storage/v1"


def _scan(collections: dict[str, Callable[[], list[dict]]], depends: dict[str, str] | None = None) -> dict:
    """Run each collection's fetch. A collection listed in `depends` is only as
    scanned as its parent: Composer DAGs are found through the environments."""
    items: dict[str, list[dict]] = {}
    not_scanned: dict[str, str] = {}
    disabled = False
    for name, fetch in collections.items():
        parent = (depends or {}).get(name)
        if parent in not_scanned:
            items[name] = []
            not_scanned[name] = f"{parent} were not scanned"
            continue
        try:
            items[name] = fetch()
        except GcpError as exc:
            items[name] = []
            if exc.api_disabled:
                disabled = True
            else:
                not_scanned[name] = str(exc)
    summary: dict = {k: len(v) for k, v in items.items() if k not in not_scanned}
    if disabled:
        summary["api_disabled"] = True
    if not_scanned:
        summary["not_scanned"] = not_scanned
    return {"summary": summary, "items": items}


def _per_region(regions, fetch) -> list[dict]:
    out: list[dict] = []
    for region in regions:
        out += fetch(region)
    return out
