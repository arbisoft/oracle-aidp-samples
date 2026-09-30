import pytest

import jira_client as j


def test_returns_env_var_when_no_vault_configured(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("JIRA_SITE", "example.atlassian.net")
    assert j.get_secret("JIRA_SITE") == "example.atlassian.net"


def test_returns_default_when_nothing_resolves(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("SOME_MISSING_NAME", raising=False)
    assert j.get_secret("SOME_MISSING_NAME", default="fallback") == "fallback"


def test_raises_key_error_when_nothing_resolves_and_no_default(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("SOME_MISSING_NAME", raising=False)
    with pytest.raises(KeyError):
        j.get_secret("SOME_MISSING_NAME")


def test_env_var_name_is_uppercased(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("MY_TOKEN", "abc123")
    assert j.get_secret("my_token") == "abc123"


def test_vault_path_is_tried_first_when_configured(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.setenv("JIRA_SITE", "env-value-should-not-be-used")
    monkeypatch.setattr(
        j, "_get_from_vault", lambda name, vault_id: "vault-value"
    )
    assert j.get_secret("JIRA_SITE") == "vault-value"


def test_falls_through_to_env_when_vault_lookup_fails(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.setenv("JIRA_SITE", "env-fallback-value")

    def _boom(name, vault_id):
        raise RuntimeError("vault unreachable")

    monkeypatch.setattr(j, "_get_from_vault", _boom)
    assert j.get_secret("JIRA_SITE") == "env-fallback-value"


def test_explicit_vault_id_overrides_env_var(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    seen = {}

    def _fake_vault(name, vault_id):
        seen["vault_id"] = vault_id
        return "vault-value"

    monkeypatch.setattr(j, "_get_from_vault", _fake_vault)
    result = j.get_secret("X", vault_id="ocid1.vault.oc1..explicit")
    assert result == "vault-value"
    assert seen["vault_id"] == "ocid1.vault.oc1..explicit"


def test_missing_everywhere_mentions_the_vault_failure(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    monkeypatch.delenv("NOWHERE_SECRET", raising=False)

    def _boom(name, vault_id):
        raise RuntimeError("not authorized")

    monkeypatch.setattr(j, "_get_from_vault", _boom)
    with pytest.raises(KeyError) as exc:
        j.get_secret("NOWHERE_SECRET")
    assert "not authorized" in str(exc.value)


def test_vault_lookup_uses_compartment_env_var_when_set(monkeypatch):
    import sys
    from types import SimpleNamespace

    seen = {}

    def list_all(fn, **kwargs):
        seen.update(kwargs)
        return SimpleNamespace(data=[])

    fake_oci = SimpleNamespace(
        config=SimpleNamespace(from_file=lambda: {"tenancy": "ocid1.tenancy"}),
        vault=SimpleNamespace(VaultsClient=lambda c: SimpleNamespace(list_secrets=None)),
        secrets=SimpleNamespace(SecretsClient=lambda c: None),
        pagination=SimpleNamespace(list_call_get_all_results=list_all),
    )
    monkeypatch.setitem(sys.modules, "oci", fake_oci)
    monkeypatch.setenv("OCI_COMPARTMENT_ID", "ocid1.compartment.child")
    with pytest.raises(KeyError):
        j._get_from_vault("X", "ocid1.vault.oc1..fake")
    assert seen["compartment_id"] == "ocid1.compartment.child"

    monkeypatch.delenv("OCI_COMPARTMENT_ID")
    with pytest.raises(KeyError):
        j._get_from_vault("X", "ocid1.vault.oc1..fake")
    assert seen["compartment_id"] == "ocid1.tenancy"


def test_vault_lookup_ignores_secrets_that_are_not_active(monkeypatch):
    import sys, base64
    from types import SimpleNamespace

    def secret(sid, state):
        return SimpleNamespace(secret_name="X", id=sid, lifecycle_state=state)

    fetched = []

    def get_bundle(secret_id):
        fetched.append(secret_id)
        content = base64.b64encode(b"active-value").decode()
        return SimpleNamespace(data=SimpleNamespace(
            secret_bundle_content=SimpleNamespace(content=content)))

    fake_oci = SimpleNamespace(
        config=SimpleNamespace(from_file=lambda: {"tenancy": "t"}),
        vault=SimpleNamespace(VaultsClient=lambda c: SimpleNamespace(list_secrets=None)),
        secrets=SimpleNamespace(SecretsClient=lambda c: SimpleNamespace(get_secret_bundle=get_bundle)),
        pagination=SimpleNamespace(list_call_get_all_results=lambda fn, **kw: SimpleNamespace(
            data=[secret("old", "PENDING_DELETION"), secret("live", "ACTIVE")])),
    )
    monkeypatch.setitem(sys.modules, "oci", fake_oci)
    monkeypatch.delenv("OCI_COMPARTMENT_ID", raising=False)
    assert j._get_from_vault("X", "ocid1.vault.oc1..fake") == "active-value"
    assert fetched == ["live"]
