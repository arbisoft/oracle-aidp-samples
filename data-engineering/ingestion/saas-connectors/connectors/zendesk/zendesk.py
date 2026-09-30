"""Zendesk Cloud REST API v2 reader for AIDP notebooks.

Read-only. Uses only ``requests`` plus ``connectors/_shared/``. Credentials
come from environment variables (or OCI Vault) and are never logged. See
connectors/zendesk/REQUIREMENTS.md.
"""

from __future__ import annotations

import re
from typing import Tuple

import aidp_http
import aidp_secrets

ENV_SUBDOMAIN = "ZENDESK_SUBDOMAIN"
ENV_EMAIL = "ZENDESK_EMAIL"
ENV_API_TOKEN = "ZENDESK_API_TOKEN"

# Same classes as connectors/_shared/aidp_http.py, aliased for a Zendesk-shaped
# public API — not subclasses, so callers can catch either name.
ZendeskError = aidp_http.ConnectorError
ZendeskAuthError = aidp_http.ConnectorAuthError
ZendeskRateLimitError = aidp_http.ConnectorRateLimitError

_SUBDOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$", re.I)


def normalize_subdomain(subdomain: str) -> str:
    """Return a bare Zendesk subdomain (e.g. ``example``) or raise ValueError."""
    value = re.sub(r"^https?://", "", (subdomain or "").strip(), flags=re.I)
    value = value.rstrip("/")
    value = re.sub(r"\.zendesk\.com$", "", value, flags=re.I)
    if not _SUBDOMAIN_RE.match(value or ""):
        raise ValueError(
            "subdomain must be a bare Zendesk subdomain such as 'example'"
        )
    return value


def credentials_from_env() -> Tuple[str, str, str]:
    """Return ``(subdomain, email, api_token)`` from Vault/environment."""
    try:
        subdomain = aidp_secrets.get_secret(ENV_SUBDOMAIN)
        email = aidp_secrets.get_secret(ENV_EMAIL)
        api_token = aidp_secrets.get_secret(ENV_API_TOKEN)
    except KeyError as exc:
        raise ZendeskError(str(exc)) from None
    return (normalize_subdomain(subdomain), email, api_token)


def zendesk_session(email: str, api_token: str):
    """A requests.Session with HTTP Basic auth (email/token:api_token) and JSON Accept."""
    import requests

    session = requests.Session()
    session.auth = ("{}/token".format(email), api_token)
    session.headers.update({"Accept": "application/json"})
    return session
