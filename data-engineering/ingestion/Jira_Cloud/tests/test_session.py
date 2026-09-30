import pytest

import jira_client as j


def test_normalize_site_strips_scheme_and_trailing_slash():
    assert j.normalize_site("https://example.atlassian.net/") == "example.atlassian.net"
    assert j.normalize_site("HTTP://example.atlassian.net") == "example.atlassian.net"
    assert j.normalize_site("  example.atlassian.net  ") == "example.atlassian.net"


@pytest.mark.parametrize("bad", ["", "   ", "https://", "example.atlassian.net/jira", "a b"])
def test_normalize_site_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        j.normalize_site(bad)


def test_credentials_from_env_returns_site_email_token(monkeypatch):
    monkeypatch.setenv("JIRA_SITE", "https://example.atlassian.net/")
    monkeypatch.setenv("JIRA_EMAIL", "me@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok3n")
    assert j.credentials_from_env() == ("example.atlassian.net", "me@example.com", "tok3n")


def test_credentials_from_env_names_the_missing_variable_only(monkeypatch):
    monkeypatch.setenv("JIRA_SITE", "example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "me@example.com")
    monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
    with pytest.raises(j.JiraError) as exc:
        j.credentials_from_env()
    assert "JIRA_API_TOKEN" in str(exc.value)
    assert "me@example.com" not in str(exc.value)


def test_session_uses_basic_auth_and_json_accept_header():
    session = j.jira_session("me@example.com", "tok3n")
    assert session.auth == ("me@example.com", "tok3n")
    assert session.headers["Accept"] == "application/json"


def test_redact_replaces_every_secret_and_ignores_empty_ones():
    text = "failed for hunter2 and tok3n"
    assert j.redact(text, "hunter2", "tok3n", "") == "failed for *** and ***"


def test_credentials_error_shows_the_vault_message_verbatim(monkeypatch):
    monkeypatch.setenv("OCI_VAULT_ID", "ocid1.vault.oc1..fake")
    for name in ("JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    def boom(name, vault_id):
        raise RuntimeError('policy says "no"')

    monkeypatch.setattr(j, "_get_from_vault", boom)
    with pytest.raises(j.JiraError) as exc:
        j.credentials_from_env()
    assert 'policy says "no")' in str(exc.value)


def test_credentials_are_stripped_of_stray_whitespace(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("JIRA_SITE", "example.atlassian.net\n")
    monkeypatch.setenv("JIRA_EMAIL", "  me@example.com\n")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok123\n")
    assert j.credentials_from_env() == ("example.atlassian.net", "me@example.com", "tok123")


@pytest.mark.parametrize("name", ["JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN"])
def test_empty_credential_values_are_reported_as_missing(monkeypatch, name):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("JIRA_SITE", "example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "me@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv(name, "  ")
    with pytest.raises(j.JiraError) as exc:
        j.credentials_from_env()
    assert name in str(exc.value)


@pytest.mark.parametrize("bad", [
    "attacker.example",
    "example.atlassian.net.evil.com",
    "evil.com#.atlassian.net",
    "me@example.atlassian.net",
    "example.atlassian.net:8443",
    "atlassian.net",
    "a.b.atlassian.net",
    "-x.atlassian.net",
])
def test_normalize_site_rejects_hosts_that_are_not_jira_cloud_sites(bad):
    # The Basic-auth header carries the API token, so it must only ever go to
    # a <site>.atlassian.net host.
    with pytest.raises(ValueError):
        j.normalize_site(bad)


def test_normalize_site_lowercases_the_host():
    assert j.normalize_site("HTTPS://Example.Atlassian.NET/") == "example.atlassian.net"
