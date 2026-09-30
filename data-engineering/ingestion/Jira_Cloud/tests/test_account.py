import pytest

import jira_client as j
from fakes import FakeResponse, FakeSession

URL = "https://example.atlassian.net/rest/api/3/myself"


def test_account_timezone_returns_the_time_zone_field():
    session = FakeSession([FakeResponse(200, {"timeZone": "Asia/Karachi"})])
    assert j.account_timezone(session, "example.atlassian.net") == "Asia/Karachi"
    assert session.calls[0]["url"] == URL


def test_account_timezone_raises_when_field_is_missing():
    session = FakeSession([FakeResponse(200, {"accountId": "x"})])
    with pytest.raises(j.JiraError) as exc:
        j.account_timezone(session, "example.atlassian.net")
    assert "timeZone" in str(exc.value)


def test_account_timezone_raises_auth_error_on_401():
    session = FakeSession([FakeResponse(401, {})])
    with pytest.raises(j.JiraAuthError):
        j.account_timezone(session, "example.atlassian.net")


def test_account_timezone_retries_on_429_then_succeeds():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "1"}),
        FakeResponse(200, {"timeZone": "UTC"}),
    ])
    assert j.account_timezone(session, "example.atlassian.net", sleep=sleeps.append) == "UTC"
    assert sleeps == [1.0]
