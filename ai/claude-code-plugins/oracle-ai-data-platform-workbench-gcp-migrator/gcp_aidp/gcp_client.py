"""Read-only Google Cloud REST client. GET only, and a read-only token.

Two independent guarantees that inventory never writes:
  * this class has no method that sends anything but GET;
  * by default the OAuth token carries only the `cloud-platform.read-only`
    scope, so Google itself refuses a write even if one were attempted.
    (`--scan-services` is the one exception; see CLOUD_PLATFORM_SCOPE.)

Credentials come from Application Default Credentials: the key file named by
GOOGLE_APPLICATION_CREDENTIALS, or `gcloud auth application-default login`.
The key is never read, copied or printed here.

`google-auth` is imported lazily, so the offline verbs need no dependencies.
"""
from __future__ import annotations

from typing import Any

READ_ONLY_SCOPE = "https://www.googleapis.com/auth/cloud-platform.read-only"
# Dataproc, Composer, Dataform, Dataflow and Vertex AI refuse a read-only token
# (ACCESS_TOKEN_SCOPE_INSUFFICIENT). Only `--scan-services` asks for this scope,
# and only for those services; read-only then rests on this class sending GETs
# only and on the service account's viewer roles.
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
MAX_PAGES = 1000


class GcpError(Exception):
    def __init__(self, message: str, *, status: int | None = None, reason: str = ""):
        super().__init__(message)
        self.status = status
        self.reason = reason

    @property
    def api_disabled(self) -> bool:
        """The service's API is not enabled, so the project cannot hold any of its resources."""
        return self.reason in ("SERVICE_DISABLED", "accessNotConfigured")


def _error(response) -> GcpError:
    try:
        err = response.json().get("error", {})
    except ValueError:
        err = {}
    reasons = [d.get("reason") for d in err.get("details", []) if isinstance(d, dict)]
    reasons += [e.get("reason") for e in err.get("errors", []) if isinstance(e, dict)]
    reason = next((r for r in reasons if r), err.get("status", ""))
    message = str(err.get("message") or response.text[:300]).splitlines()[0]
    return GcpError(f"HTTP {response.status_code} {reason}: {message}", status=response.status_code, reason=reason)


class GcpClient:
    def __init__(self, project: str, *, scope: str = READ_ONLY_SCOPE, timeout: float = 60.0):
        if not project:
            raise ValueError("a GCP project id is required (--project or GCP_PROJECT)")
        self.project = project
        self.scope = scope
        self.timeout = timeout
        self._session: Any = None

    @property
    def session(self):
        if self._session is None:
            try:
                import google.auth
                from google.auth.transport.requests import AuthorizedSession
            except ImportError as exc:
                raise RuntimeError("live inventory needs google-auth: pip install -e '.[gcp]'") from exc
            credentials, _ = google.auth.default(scopes=[self.scope])
            self._session = AuthorizedSession(credentials)
        return self._session

    def get(self, url: str, params: dict | None = None) -> dict:
        response = self.session.get(url, params=params or {}, timeout=self.timeout)
        if response.status_code >= 400:
            raise _error(response)
        return response.json() if response.content else {}

    def get_text(self, url: str, params: dict | None = None) -> str:
        """The raw body of a GET, for a file read with `alt=media` (not JSON)."""
        response = self.session.get(url, params=params or {}, timeout=self.timeout)
        if response.status_code >= 400:
            raise _error(response)
        return response.text

    def pages(self, url: str, key: str, params: dict | None = None) -> list[dict]:
        """Every item of a list call, following nextPageToken."""
        items: list[dict] = []
        params = dict(params or {})
        for _ in range(MAX_PAGES):
            body = self.get(url, params)
            items += body.get(key, []) or []
            token = body.get("nextPageToken")
            if not token:
                return items
            params["pageToken"] = token
        raise GcpError(f"{url} still paging after {MAX_PAGES} pages")
