"""Secret loading with a fallback chain: OCI Vault, then environment, then default.

Pattern adapted from Oracle's own
`oracle-ai-data-platform-workbench-spark-connectors` plugin
(`scripts/oracle_ai_data_platform_connectors/auth/secrets.py`, MIT licence,
Copyright (c) 2026 Ahmed Awan) — rewritten for this repo's naming and test
style, not copied verbatim. Named ``aidp_secrets`` rather than ``secrets`` to
avoid shadowing the Python standard library module of that name once this
package is on ``sys.path``.

Order of resolution:
    1. OCI Vault secret (if ``OCI_VAULT_ID`` is set, or ``vault_id`` is passed,
       and the helper can resolve a secret named ``name`` inside it)
    2. Environment variable ``name`` (uppercased)
    3. ``default`` parameter (or raise ``KeyError`` if not provided)

The Vault path is best-effort: any failure there (missing ``oci`` package, no
network, wrong permissions) falls through silently to the environment-variable
lookup, so the same code works in unit tests, a laptop spike, and a live AIDP
notebook without branching on which is which.
"""

from __future__ import annotations

import os
from typing import Any, Optional

_MISSING = object()


def get_secret(name: str, default: Any = _MISSING, vault_id: Optional[str] = None) -> str:
    """Resolve ``name`` from OCI Vault, then an environment variable, then ``default``.

    Args:
        name: The secret name. Used as the Vault secret name (case-sensitive)
            and, uppercased, as the environment-variable fallback.
        default: Returned if the secret can't be resolved anywhere. Raises
            ``KeyError`` instead if left unspecified.
        vault_id: OCI Vault OCID. Defaults to the ``OCI_VAULT_ID`` environment
            variable; if neither is set, the Vault step is skipped entirely.

    Returns:
        The resolved secret value.

    Raises:
        KeyError: If no value can be resolved and ``default`` is not provided.
    """
    vault_id = vault_id or os.environ.get("OCI_VAULT_ID")
    if vault_id:
        try:
            return _get_from_vault(name, vault_id)
        except Exception:
            pass  # Vault is best-effort; fall through to the environment.

    env_value = os.environ.get(name.upper())
    if env_value is not None:
        return env_value

    if default is _MISSING:
        raise KeyError("secret {!r} not found in OCI Vault, environment, or default".format(name))
    return default


def _get_from_vault(name: str, vault_id: str) -> str:
    """Look up ``name`` as a secret in the given OCI Vault.

    Imported lazily so unit tests, and any connector spike, never need the
    ``oci`` package installed unless they actually reach this path.
    """
    import base64

    import oci

    config = oci.config.from_file()
    vaults_client = oci.vault.VaultsClient(config)
    secrets_client = oci.secrets.SecretsClient(config)

    secrets = oci.pagination.list_call_get_all_results(
        vaults_client.list_secrets,
        compartment_id=config.get("tenancy"),
        vault_id=vault_id,
    ).data
    match = next((s for s in secrets if s.secret_name == name), None)
    if match is None:
        raise KeyError("secret {!r} not in vault {!r}".format(name, vault_id))

    bundle = secrets_client.get_secret_bundle(secret_id=match.id).data
    content = bundle.secret_bundle_content.content  # base64-encoded
    return base64.b64decode(content).decode("utf-8")
