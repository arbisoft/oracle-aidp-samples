"""Cloud Storage → OCI Object Storage, for `export` and `transfer`.

Read from Cloud Storage with a Google session, written to OCI with ~/.oci/config.
A file is never deleted or overwritten with other content: one already in OCI with the
same MD5 (or, for a composite object, which has no MD5, the same size) is skipped, so a
rerun resumes. OCI checks each file's MD5 on upload when Cloud Storage gives one.
"""
from __future__ import annotations

import concurrent.futures
import os
import threading
import time
from urllib.parse import quote

from gcp_aidp.gcp_client import _error

GCS = "https://storage.googleapis.com/storage/v1"
COPY_WORKERS = 32
PROGRESS_SECONDS = 10  # a copy shows its progress this often, so a long one does not look stuck


def rows_file(folder_url: str) -> str:
    """Beside a table's export folder (…/orders/ → …/orders.rows.json): the rows BigQuery's EXPORT
    DATA job wrote, the count the copy checks for a table the connector cannot read. Not inside the
    folder: Spark would read it as Parquet, and skips a "_" name even when asked for it."""
    return folder_url.rstrip("/") + ".rows.json"


class CopyError(Exception):
    """The copy cannot start as it stands; nothing was sent."""


def split(url: str, scheme: str) -> tuple[str, str]:
    """gs://b/p/ → (b, p/); oci://b@ns/p/ → (b, p/)."""
    bucket, _, path = url[len(scheme):].partition("/")
    return bucket.split("@")[0], path


def session(scope: str, key_file: str | None = None):
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account
    except ImportError as exc:
        raise CopyError("this needs google-auth: pip install -e '.[gcp,oci]'") from exc
    if key_file:
        credentials = service_account.Credentials.from_service_account_file(key_file, scopes=[scope])
    else:
        credentials, _ = google.auth.default(scopes=[scope])
    return AuthorizedSession(credentials)


def oci_client():
    try:
        import oci
    except ImportError as exc:
        raise CopyError("this needs the OCI SDK: pip install -e '.[gcp,oci]'") from exc
    config = oci.config.from_file(profile_name=os.environ.get("OCI_CLI_PROFILE") or "DEFAULT")
    if os.environ.get("OCI_REGION"):
        config["region"] = os.environ["OCI_REGION"]
    # The SDK's 60 s read timeout is also the socket's write timeout: with many files sharing
    # a slow uplink, one upload waited longer and failed (seen live, 2026-10-13).
    return oci.object_storage.ObjectStorageClient(config, retry_strategy=oci.retry.NoneRetryStrategy(),
                                                  timeout=(10, 600))


def check(response) -> dict:
    if response.status_code >= 400:
        raise _error(response)
    return response.json() if response.content else {}


def gcs_objects(reader, gcs_url: str) -> list[dict]:
    """Every object under the prefix, but not a console "folder" (a zero-byte name ending in /)."""
    bucket, prefix = split(gcs_url, "gs://")
    items, params = [], {"prefix": prefix, "fields": "items(name,size,md5Hash),nextPageToken"}
    while True:
        body = check(reader.get(f"{GCS}/b/{bucket}/o", params=params, timeout=60))
        items += [o for o in body.get("items", []) if not (o["name"].endswith("/") and int(o["size"]) == 0)]
        if not body.get("nextPageToken"):
            return items
        params["pageToken"] = body["nextPageToken"]


def oci_objects(client, namespace: str, oci_url: str) -> dict[str, tuple[str | None, int]]:
    """name → (MD5, size) of every object under the prefix."""
    bucket, prefix = split(oci_url, "oci://")
    out, start = {}, None
    while True:
        page = client.list_objects(namespace, bucket, prefix=prefix, fields="md5,size", start=start, limit=1000).data
        out.update((o.name, (o.md5, o.size)) for o in page.objects)
        start = page.next_start_with
        if not start:
            return out


class _Body:
    """A download as an upload body: `read` and a length, nothing else. Passed as it is, a
    urllib3 response has a `fileno()`, so the OCI SDK takes it for a file and fails reading
    its `.mode` (seen live, 2026-10-13)."""

    def __init__(self, raw, size: int, counted=None):
        self._raw, self._size, self._counted = raw, size, counted

    def read(self, n: int = -1) -> bytes:
        chunk = self._raw.read(n)
        if self._counted:
            self._counted(len(chunk))
        return chunk

    def __len__(self) -> int:
        return self._size


def _copy_file(reader, client, namespace: str, gcs_bucket: str, oci_bucket: str, obj: dict, oci_name: str,
               counted=None):
    url = f"{GCS}/b/{gcs_bucket}/o/{quote(obj['name'], safe='')}"
    last: Exception = RuntimeError("not attempted")
    for _ in range(3):  # each attempt streams the file again from the start
        response = reader.get(url, params={"alt": "media"}, stream=True, timeout=600)
        try:
            if response.status_code >= 400:
                raise _error(response)
            kwargs = {"content_md5": obj["md5Hash"]} if obj.get("md5Hash") else {}
            client.put_object(namespace, oci_bucket, oci_name, _Body(response.raw, int(obj["size"]), counted),
                              content_length=int(obj["size"]), **kwargs)
            return
        except Exception as exc:  # noqa: BLE001 - retried, then reported
            last = exc
        finally:
            response.close()
    raise last


class _Progress:
    """Bytes sent so far, added from every copying thread."""

    def __init__(self):
        self.sent = 0
        self._lock = threading.Lock()

    def add(self, n: int) -> None:
        with self._lock:
            self.sent += n


def progress_line(label: str, done: int, files: int, sent: int, total: int, seconds: float) -> str:
    rate = sent / seconds if seconds else 0
    left = f", about {(total - sent) / rate / 60:.0f} min left" if rate and sent < total else ""
    return (f"  copying           {label}  {done}/{files} file(s), {sent / 1e6:.0f}/{total / 1e6:.0f} MB, "
            f"{rate / 1e6:.1f} MB/s{left}")


def _same(obj: dict, have: tuple[str | None, int] | None) -> bool:
    if have is None:
        return False
    if obj.get("md5Hash"):
        return have[0] == obj["md5Hash"]
    return have[1] == int(obj["size"])  # a composite object has no MD5: the size is the check


def plan_copy(reader, client, namespace: str, gcs_url: str, oci_url: str) -> tuple[list, list[tuple[dict, str]]]:
    """(every object, [(object, OCI name)] still to copy)."""
    _, gcs_prefix = split(gcs_url, "gs://")
    _, oci_prefix = split(oci_url, "oci://")
    objects = gcs_objects(reader, gcs_url)
    have = oci_objects(client, namespace, oci_url)
    named = [(o, oci_prefix + o["name"][len(gcs_prefix):]) for o in objects]
    return objects, [(o, name) for o, name in named if not _same(o, have.get(name))]


def copy(reader, client, namespace: str, gcs_url: str, oci_url: str, label: str, log=print) -> dict:
    """Copy what is not yet in OCI; the counts for the report."""
    gcs_bucket, _ = split(gcs_url, "gs://")
    oci_bucket, _ = split(oci_url, "oci://")
    objects, todo = plan_copy(reader, client, namespace, gcs_url, oci_url)
    failed, progress = [], _Progress()
    total, start = sum(int(o["size"]) for o, _ in todo), time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(COPY_WORKERS) as pool:
        futures = {pool.submit(_copy_file, reader, client, namespace, gcs_bucket, oci_bucket, o, name,
                               progress.add): name for o, name in todo}
        pending, done = set(futures), 0
        while pending:
            finished, pending = concurrent.futures.wait(pending, timeout=PROGRESS_SECONDS)
            for f in finished:
                done += 1
                if f.exception():
                    failed.append(f"{futures[f]}: {f.exception()}")
            if pending:  # a retried file counts twice, so the figures are an estimate
                log(progress_line(label, done, len(todo), min(progress.sent, total), total,
                                  time.monotonic() - start))
    return {"files": len(objects), "copied": len(todo) - len(failed), "already_in_oci": len(objects) - len(todo),
            "bytes": sum(int(o["size"]) for o in objects),
            "checked_by_size": sum(1 for o in objects if not o.get("md5Hash")),
            "copy_failed": failed[:5], "copy_failures": len(failed)}


def copied_line(label: str, rec: dict) -> str:
    return (f"  {'copied' if not rec['copy_failures'] else 'copy FAILED':<17} {label}  {rec['files']} file(s), "
            f"{rec['bytes'] / 1e6:.1f} MB; {rec['copied']} copied, {rec['already_in_oci']} already in OCI"
            + (f"; {rec['checked_by_size']} composite object(s) checked by size, not MD5"
               if rec.get("checked_by_size") else "")
            + (f", {rec['copy_failures']} failed: {rec['copy_failed'][0]}" if rec["copy_failures"] else ""))
