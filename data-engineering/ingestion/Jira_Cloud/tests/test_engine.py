import pytest

import jira_client as j
from fakes import FakeResponse, FakeSession

URL = "https://example.atlassian.net/rest/api/3/search/jql"


def test_post_json_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"issues": []})])
    assert j.post_json(session, URL, {"jql": "x"}, timeout=7) == {"issues": []}
    assert session.calls[0]["timeout"] == 7


def test_get_json_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"timeZone": "UTC"})])
    assert j.get_json(session, URL, timeout=9) == {"timeZone": "UTC"}
    assert session.calls[0]["timeout"] == 9


def test_429_sleeps_retry_after_then_succeeds():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "2"}),
        FakeResponse(200, {"ok": True}),
    ])
    assert j.post_json(session, URL, {}, sleep=sleeps.append) == {"ok": True}
    assert sleeps == [2.0]


def test_429_forever_raises_after_bounded_retries():
    session = FakeSession([FakeResponse(429, {}) for _ in range(4)])
    with pytest.raises(j.JiraRateLimitError):
        j.post_json(session, URL, {}, max_retries=3, sleep=lambda s: None)
    assert len(session.calls) == 4


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_raise_auth_error(status):
    with pytest.raises(j.JiraAuthError):
        j.post_json(FakeSession([FakeResponse(status, {})]), URL, {})


def test_other_http_errors_raise_generic_error_with_status():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(500, {})]), URL, {})
    assert "500" in str(exc.value)


def test_non_json_200_raises_clear_error():
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(200, None)]), URL, {})
    assert "not json" in str(exc.value).lower()


def test_redact_replaces_every_secret_and_ignores_empty_ones():
    assert j.redact("failed for hunter2", "hunter2", "") == "failed for ***"


def test_a_valid_json_array_response_raises_clear_error_not_a_later_crash():
    # A 200 with valid JSON that isn't an object (e.g. a bare array, or a
    # redirect page that happens to return "null") would otherwise crash later
    # with an unhelpful AttributeError the first time a caller does payload.get(...).
    with pytest.raises(j.JiraError) as exc:
        j.post_json(FakeSession([FakeResponse(200, ["not", "a", "dict"])]), URL, {})
    assert "object" in str(exc.value).lower()


@pytest.mark.parametrize("status", [502, 503, 504])
def test_transient_server_errors_are_retried_then_succeed(status):
    sleeps = []
    session = FakeSession([FakeResponse(status, {}), FakeResponse(200, {"ok": True})])
    assert j.post_json(session, URL, {}, sleep=sleeps.append) == {"ok": True}
    assert sleeps == [1]


def test_persistent_server_error_raises_generic_error_not_rate_limit():
    session = FakeSession([FakeResponse(503, {}) for _ in range(3)])
    with pytest.raises(j.JiraError) as exc:
        j.post_json(session, URL, {}, max_retries=2, sleep=lambda s: None)
    assert not isinstance(exc.value, j.JiraRateLimitError)
    assert "503" in str(exc.value)
    assert len(session.calls) == 3


def test_network_errors_are_retried_then_succeed():
    session = FakeSession([ConnectionError("reset"), FakeResponse(200, {"ok": True})])
    assert j.get_json(session, URL, sleep=lambda s: None) == {"ok": True}


def test_persistent_network_error_is_wrapped_as_jira_error():
    session = FakeSession([TimeoutError("slow") for _ in range(3)])
    with pytest.raises(j.JiraError) as exc:
        j.get_json(session, URL, max_retries=2, sleep=lambda s: None)
    assert "TimeoutError" in str(exc.value)


def test_permanent_request_errors_fail_fast_without_backoff():
    import requests

    sleeps = []
    session = FakeSession([requests.exceptions.InvalidURL("bad site")])
    with pytest.raises(j.JiraError):
        j.get_json(session, URL, sleep=sleeps.append)
    assert sleeps == [] and len(session.calls) == 1


def test_requests_connection_errors_and_timeouts_are_retried():
    import requests

    session = FakeSession([
        requests.exceptions.ConnectionError("reset"),
        requests.exceptions.Timeout("slow"),
        FakeResponse(200, {"ok": True}),
    ])
    assert j.get_json(session, URL, sleep=lambda s: None) == {"ok": True}


@pytest.mark.parametrize("name", ["ChunkedEncodingError", "ContentDecodingError"])
def test_transient_body_errors_are_retried_not_treated_as_permanent(name):
    import requests

    exc = getattr(requests.exceptions, name)("dropped mid-body")
    session = FakeSession([exc, FakeResponse(200, {"ok": True})])
    assert j.get_json(session, URL, sleep=lambda s: None) == {"ok": True}


@pytest.mark.parametrize("name", ["InvalidURL", "MissingSchema", "InvalidSchema"])
def test_each_permanent_url_error_fails_fast(name):
    import requests

    session = FakeSession([getattr(requests.exceptions, name)("bad")])
    with pytest.raises(j.JiraError):
        j.get_json(session, URL, sleep=lambda s: pytest.fail("must not back off"))


@pytest.mark.parametrize("value", ["NaN", "nan", "inf", "-inf", "soon", ""])
def test_unusable_retry_after_falls_back_to_backoff(value):
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": value}),
        FakeResponse(200, {"ok": True}),
    ])
    assert j.post_json(session, URL, {}, sleep=sleeps.append) == {"ok": True}
    assert sleeps == [1]
