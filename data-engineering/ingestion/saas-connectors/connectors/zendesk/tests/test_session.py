import pytest

import zendesk as z


def test_normalize_subdomain_accepts_bare_name():
    assert z.normalize_subdomain("example") == "example"


def test_normalize_subdomain_strips_scheme_and_full_host():
    assert z.normalize_subdomain("https://example.zendesk.com/") == "example"
    assert z.normalize_subdomain("example.zendesk.com") == "example"
    assert z.normalize_subdomain("  example  ") == "example"


@pytest.mark.parametrize("bad", ["", "   ", "https://", "example.com", "a/b", "a b"])
def test_normalize_subdomain_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        z.normalize_subdomain(bad)


def test_credentials_from_env_returns_subdomain_email_token(monkeypatch):
    monkeypatch.setenv("ZENDESK_SUBDOMAIN", "https://example.zendesk.com/")
    monkeypatch.setenv("ZENDESK_EMAIL", "me@example.com")
    monkeypatch.setenv("ZENDESK_API_TOKEN", "tok3n")
    assert z.credentials_from_env() == ("example", "me@example.com", "tok3n")


def test_credentials_from_env_names_the_missing_variable_only(monkeypatch):
    monkeypatch.setenv("ZENDESK_SUBDOMAIN", "example")
    monkeypatch.setenv("ZENDESK_EMAIL", "me@example.com")
    monkeypatch.delenv("ZENDESK_API_TOKEN", raising=False)
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    with pytest.raises(z.ZendeskError) as exc:
        z.credentials_from_env()
    assert "ZENDESK_API_TOKEN" in str(exc.value)
    assert "me@example.com" not in str(exc.value)


def test_session_uses_email_token_basic_auth_and_json_accept_header():
    session = z.zendesk_session("me@example.com", "tok3n")
    assert session.auth == ("me@example.com/token", "tok3n")
    assert session.headers["Accept"] == "application/json"
