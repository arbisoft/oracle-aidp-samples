# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import pytest

from fakes import FakeResponse, FakeSession, FakeTime, error_response
from notion_connector.client import NotionApiError, NotionClient


def make(responses, **kwargs):
    session = FakeSession(responses)
    fake_time = FakeTime()
    client = NotionClient(
        "secret_token", session=session, sleep=fake_time.sleep, clock=fake_time.clock, **kwargs
    )
    return client, session, fake_time


def listing(results, next_cursor=None):
    return FakeResponse(
        200,
        {"object": "list", "results": results, "has_more": next_cursor is not None, "next_cursor": next_cursor},
    )


def test_token_is_required():
    with pytest.raises(ValueError):
        NotionClient("", session=FakeSession([]))


def test_request_sends_auth_and_version_headers():
    client, session, _ = make([FakeResponse(200, {"ok": True})], api_version="2026-03-11")
    assert client.request("GET", "/v1/users/me") == {"ok": True}
    call = session.calls[0]
    assert call["url"] == "https://api.notion.com/v1/users/me"
    assert call["headers"]["Authorization"] == "Bearer secret_token"
    assert call["headers"]["Notion-Version"] == "2026-03-11"


def test_get_pagination_uses_query_params():
    client, session, _ = make([listing([{"id": "a"}], "c1"), listing([{"id": "b"}])])
    assert [u["id"] for u in client.paginate("GET", "/v1/users")] == ["a", "b"]
    assert session.calls[0]["params"] == {"page_size": 100}
    assert session.calls[1]["params"] == {"page_size": 100, "start_cursor": "c1"}
    assert session.calls[0]["json"] is None


def test_post_pagination_uses_body_and_does_not_mutate_it():
    body = {"filter": {"property": "object", "value": "page"}}
    client, session, _ = make([listing([{"id": "a"}], "c1"), listing([{"id": "b"}])])
    assert [p["id"] for p in client.paginate("POST", "/v1/search", body=body)] == ["a", "b"]
    assert session.calls[1]["json"] == {
        "filter": {"property": "object", "value": "page"},
        "page_size": 100,
        "start_cursor": "c1",
    }
    assert body == {"filter": {"property": "object", "value": "page"}}


def test_pagination_stops_when_has_more_but_no_cursor():
    broken = FakeResponse(200, {"results": [{"id": "a"}], "has_more": True, "next_cursor": None})
    client, session, _ = make([broken])
    assert [x["id"] for x in client.paginate("GET", "/v1/users")] == ["a"]
    assert len(session.calls) == 1


def test_pagination_stops_on_repeated_cursor():
    client, session, _ = make([listing([{"id": "a"}], "c1"), listing([{"id": "b"}], "c1")])
    assert [x["id"] for x in client.paginate("GET", "/v1/users")] == ["a", "b"]
    assert len(session.calls) == 2


def test_rate_limit_waits_for_retry_after():
    client, session, fake_time = make(
        [error_response(429, "rate_limited", headers={"Retry-After": "7"}), FakeResponse(200, {"ok": True})]
    )
    assert client.request("GET", "/v1/users/me") == {"ok": True}
    assert 7.0 in fake_time.sleeps
    assert len(session.calls) == 2


def test_rate_limit_without_header_waits_one_second():
    client, _, fake_time = make([error_response(429, "rate_limited"), FakeResponse(200, {"ok": True})])
    client.request("GET", "/v1/users/me")
    assert 1.0 in fake_time.sleeps


def test_rate_limit_gives_up_eventually():
    client, session, _ = make([error_response(429, "rate_limited")] * 3, max_rate_limit_waits=2)
    with pytest.raises(NotionApiError) as caught:
        client.request("GET", "/v1/users/me")
    assert caught.value.status == 429
    assert len(session.calls) == 3


def test_server_error_is_retried_with_backoff():
    client, session, fake_time = make(
        [error_response(503, "service_unavailable"), error_response(502, "bad_gateway"), FakeResponse(200, {"ok": 1})]
    )
    assert client.request("GET", "/v1/users/me") == {"ok": 1}
    assert len(session.calls) == 3
    assert 1 in fake_time.sleeps and 2 in fake_time.sleeps


def test_server_error_exhausts_attempts():
    client, session, _ = make([error_response(500, "internal_server_error")] * 3, max_attempts=3)
    with pytest.raises(NotionApiError) as caught:
        client.request("GET", "/v1/users/me")
    assert caught.value.status == 500
    assert len(session.calls) == 3


def test_connection_error_is_retried():
    client, session, _ = make([ConnectionError("reset"), FakeResponse(200, {"ok": 1})])
    assert client.request("GET", "/v1/users/me") == {"ok": 1}
    assert len(session.calls) == 2


def test_connection_error_exhausts_attempts():
    client, _, _ = make([ConnectionError("reset")] * 2, max_attempts=2)
    with pytest.raises(ConnectionError):
        client.request("GET", "/v1/users/me")


def test_client_error_raises_immediately_with_details():
    client, session, _ = make([error_response(404, "object_not_found", "Could not find block")])
    with pytest.raises(NotionApiError) as caught:
        client.request("GET", "/v1/blocks/x/children")
    assert (caught.value.status, caught.value.code) == (404, "object_not_found")
    assert "Could not find block" in str(caught.value)
    assert "secret_token" not in str(caught.value)
    assert len(session.calls) == 1


def test_error_with_non_json_body():
    client, _, _ = make([FakeResponse(400, None, text="<html>Bad Request</html>")])
    with pytest.raises(NotionApiError) as caught:
        client.request("GET", "/v1/users/me")
    assert caught.value.code == "unknown"
    assert "Bad Request" in caught.value.message


def test_requests_are_spaced_by_the_rate_limit():
    client, _, fake_time = make([FakeResponse(200, {})] * 3, requests_per_second=2)
    for _ in range(3):
        client.request("GET", "/v1/users/me")
    assert fake_time.sleeps == [0.5, 0.5]
