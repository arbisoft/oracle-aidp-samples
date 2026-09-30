import pytest

import aidp_http as h
from shared_fakes import FakeResponse, FakeSession

URL = "https://example.atlassian.net/rest/api/3/search/jql"


def test_post_json_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"issues": []})])
    assert h.post_json(session, URL, {"jql": "x"}, timeout=7) == {"issues": []}
    assert session.calls[0]["timeout"] == 7


def test_get_json_returns_json_and_passes_timeout():
    session = FakeSession([FakeResponse(200, {"timeZone": "UTC"})])
    assert h.get_json(session, URL, timeout=9) == {"timeZone": "UTC"}
    assert session.calls[0]["timeout"] == 9


def test_429_sleeps_retry_after_then_succeeds():
    sleeps = []
    session = FakeSession([
        FakeResponse(429, {}, {"Retry-After": "2"}),
        FakeResponse(200, {"ok": True}),
    ])
    assert h.post_json(session, URL, {}, sleep=sleeps.append) == {"ok": True}
    assert sleeps == [2.0]


def test_429_forever_raises_after_bounded_retries():
    session = FakeSession([FakeResponse(429, {}) for _ in range(4)])
    with pytest.raises(h.ConnectorRateLimitError):
        h.post_json(session, URL, {}, max_retries=3, sleep=lambda s: None)
    assert len(session.calls) == 4


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_raise_auth_error(status):
    with pytest.raises(h.ConnectorAuthError):
        h.post_json(FakeSession([FakeResponse(status, {})]), URL, {})


def test_other_http_errors_raise_generic_error_with_status():
    with pytest.raises(h.ConnectorError) as exc:
        h.post_json(FakeSession([FakeResponse(500, {})]), URL, {})
    assert "500" in str(exc.value)


def test_non_json_200_raises_clear_error():
    with pytest.raises(h.ConnectorError) as exc:
        h.post_json(FakeSession([FakeResponse(200, None)]), URL, {})
    assert "not json" in str(exc.value).lower()


def test_redact_replaces_every_secret_and_ignores_empty_ones():
    assert h.redact("failed for hunter2", "hunter2", "") == "failed for ***"


def test_a_valid_json_array_response_raises_clear_error_not_a_later_crash():
    # A 200 with valid JSON that isn't an object (e.g. a bare array, or a
    # redirect page that happens to return "null") would otherwise crash later
    # with an unhelpful AttributeError the first time a caller does payload.get(...).
    with pytest.raises(h.ConnectorError) as exc:
        h.post_json(FakeSession([FakeResponse(200, ["not", "a", "dict"])]), URL, {})
    assert "object" in str(exc.value).lower()
