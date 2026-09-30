import pytest

import jira as j


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
