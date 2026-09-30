import pytest

import jira_client as j
from fakes import FakeResponse, FakeSession

URL = "https://example.atlassian.net/rest/api/3/search/jql"


def test_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"issues": []})])
    assert j.post_json(session, URL, {"jql": "x"}, timeout=7) == {"issues": []}
    assert session.calls[0]["timeout"] == 7
    assert session.calls[0]["json"] == {"jql": "x"}


def test_429_sleeps_retry_after_then_succeeds():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "2"}),
        FakeResponse(200, {"issues": [1]}),
    ])
    assert j.post_json(session, URL, {}, sleep=sleeps.append) == {"issues": [1]}
    assert sleeps == [2.0]


def test_429_without_usable_retry_after_uses_capped_exponential_backoff():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "not-a-number"}),
        FakeResponse(429, {}),
        FakeResponse(200, {"issues": []}),
    ])
    j.post_json(session, URL, {}, sleep=sleeps.append)
    assert sleeps == [1, 2]


def test_retry_after_is_capped():
    sleeps = []
    session = FakeSession([FakeResponse(429, {}, {"Retry-After": "99999"}), FakeResponse(200, {})])
    j.post_json(session, URL, {}, sleep=sleeps.append)
    assert sleeps == [j.MAX_BACKOFF_SECONDS]


def test_429_forever_raises_after_bounded_retries():
    session = FakeSession([FakeResponse(429, {}) for _ in range(4)])
    with pytest.raises(j.JiraRateLimitError):
        j.post_json(session, URL, {}, max_retries=3, sleep=lambda s: None)
    assert len(session.calls) == 4


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_raise_auth_error(status):
    with pytest.raises(j.JiraAuthError):
        j.post_json(FakeSession([FakeResponse(status, {})]), URL, {})


def test_invalid_jql_400_includes_body_text_in_error():
    resp = FakeResponse(400, {"errorMessages": ["Error in the JQL Query"]})
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([resp]), URL, {})
    assert "400" in str(exc.value)


def test_other_http_errors_raise_generic_error_with_status():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(500, {})]), URL, {})
    assert "500" in str(exc.value)


def test_non_json_200_raises_clear_error():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(200, None)]), URL, {})
    assert "not json" in str(exc.value).lower()
