"""`gs://` → `oci://` paths.

Paths are rewritten through a bucket map (GCS bucket → OCI bucket) taken from
the plan; a bucket the map does not name is flagged and left as written. The
files themselves are copied by `gcp-aidp transfer` (gcp_aidp/transfer).
"""
from __future__ import annotations

import re

_GS = re.compile(r"gs://([a-z0-9][a-z0-9._-]{1,221}[a-z0-9])(/[^\s'\"]*)?", re.IGNORECASE)


def rewrite_uri(uri: str, buckets: dict[str, str], namespace: str) -> str | None:
    """`gs://b/k` → `oci://<mapped b>@ns/k`, or None when `b` is not in the bucket map."""
    m = _GS.fullmatch(uri)
    if not m or m.group(1) not in buckets:
        return None
    return f"oci://{buckets[m.group(1)]}@{namespace}{m.group(2) or ''}"


def rewrite_text(text: str, buckets: dict[str, str], namespace: str) -> tuple[str, list[str], list[str]]:
    """Rewrite every `gs://` path in `text`. Returns (text, rewritten, unmapped)."""
    rewritten, unmapped = [], []

    def sub(m: re.Match) -> str:
        new = rewrite_uri(m.group(0), buckets, namespace)
        if new is None:
            unmapped.append(m.group(0))
            return m.group(0)
        rewritten.append(m.group(0))
        return new

    return _GS.sub(sub, text), rewritten, unmapped


def location_for(uris: list[str]) -> tuple[str | None, str]:
    """A Spark table LOCATION for BigQuery external-table source URIs: (gs:// dir, note).

    BigQuery takes a wildcard per URI; a Spark LOCATION is a directory. One URI
    whose wildcard is only in the last path segment maps to that directory.
    """
    if len(uris) != 1:
        return None, f"{len(uris)} source URIs; a Spark table has one LOCATION"
    uri = uris[0]
    head, _, last = uri.rpartition("/")
    if "*" in head:
        return None, f"wildcard in a directory of {uri!r}"
    if "*" in last:
        return head + "/", (f"LOCATION is the directory {head}/; every file in it is read, "
                            f"not only those matching {last!r}")
    return uri, ""
