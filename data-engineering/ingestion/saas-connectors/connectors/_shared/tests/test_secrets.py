import pytest

import aidp_secrets


def test_returns_env_var_when_no_vault_configured(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("JIRA_SITE", "example.atlassian.net")
    assert aidp_secrets.get_secret("JIRA_SITE") == "example.atlassian.net"


def test_returns_default_when_nothing_resolves(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("SOME_MISSING_NAME", raising=False)
    assert aidp_secrets.get_secret("SOME_MISSING_NAME", default="fallback") == "fallback"


def test_raises_key_error_when_nothing_resolves_and_no_default(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("SOME_MISSING_NAME", raising=False)
    with pytest.raises(KeyError):
        aidp_secrets.get_secret("SOME_MISSING_NAME")


def test_env_var_name_is_uppercased(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("MY_TOKEN", "abc123")
    assert aidp_secrets.get_secret("my_token") == "abc123"


def test_vault_path_is_tried_first_when_configured(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.setenv("JIRA_SITE", "env-value-should-not-be-used")
    monkeypatch.setattr(
        aidp_secrets, "_get_from_vault", lambda name, vault_id: "vault-value"
    )
    assert aidp_secrets.get_secret("JIRA_SITE") == "vault-value"


def test_falls_through_to_env_when_vault_lookup_fails(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.setenv("JIRA_SITE", "env-fallback-value")

    def _boom(name, vault_id):
        raise RuntimeError("vault unreachable")

    monkeypatch.setattr(aidp_secrets, "_get_from_vault", _boom)
    assert aidp_secrets.get_secret("JIRA_SITE") == "env-fallback-value"


def test_explicit_vault_id_overrides_env_var(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    seen = {}

    def _fake_vault(name, vault_id):
        seen["vault_id"] = vault_id
        return "vault-value"

    monkeypatch.setattr(aidp_secrets, "_get_from_vault", _fake_vault)
    result = aidp_secrets.get_secret("X", vault_id="ocid1.vault.oc1..explicit")
    assert result == "vault-value"
    assert seen["vault_id"] == "ocid1.vault.oc1..explicit"
