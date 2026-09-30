from datetime import datetime

import pytest

import jira_client as j
from fakes import FakeResponse, FakeSession, FakeSearch

UNTIL = datetime(2026, 9, 28, 12, 0, 0)


def make_issue(n):
    return {"key": "KAN-%d" % n, "fields": {"updated": "2026-09-28T10:00:%02d.000+0000" % n}}


def keys(rows):
    return [r["key"] for r in rows]


def run(session, **kw):
    kw.setdefault("until", UNTIL)
    kw.setdefault("sleep", lambda s: None)
    kw.setdefault("tz_name", "UTC")  # explicit, so scripted sessions never see a /myself call
    return list(j.search_issues(session, "example.atlassian.net", **kw))


def test_pages_through_every_issue_exactly_once():
    pages = [
        {"issues": [make_issue(1), make_issue(2)], "isLast": False, "nextPageToken": "t1"},
        {"issues": [make_issue(3), make_issue(4)], "isLast": False, "nextPageToken": "t2"},
        {"issues": [make_issue(5)], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    assert keys(run(session, page_size=2)) == ["KAN-1", "KAN-2", "KAN-3", "KAN-4", "KAN-5"]
    assert len(session.calls) == 3


def test_is_last_true_stops_even_if_a_token_is_present():
    pages = [{"issues": [make_issue(1)], "isLast": True, "nextPageToken": "stale-token"}]
    session = FakeSearch(pages)
    assert len(run(session, page_size=10)) == 1
    assert len(session.calls) == 1


def test_empty_first_page_yields_nothing():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    assert run(session, page_size=10) == []
    assert len(session.calls) == 1


def test_missing_next_page_token_with_is_last_false_raises_instead_of_looping():
    session = FakeSearch([{"issues": [make_issue(1)], "isLast": False, "nextPageToken": None}])
    with pytest.raises(j.JiraError):
        run(session, page_size=10)


def test_repeated_token_raises_instead_of_looping_forever():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "same"},
        {"issues": [make_issue(2)], "isLast": False, "nextPageToken": "same"},
        {"issues": [make_issue(3)], "isLast": False, "nextPageToken": "same"},
    ]
    session = FakeSearch(pages)
    with pytest.raises(j.JiraError):
        run(session, page_size=10)


def test_upper_bound_is_fixed_for_every_page():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "t1"},
        {"issues": [make_issue(2)], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    run(session, query="project = KAN", page_size=1)
    for call in session.calls:
        assert 'updated <= "2026-09-28 12:00"' in call["json"]["jql"]


def test_since_minus_overlap_is_the_lower_bound():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    # 10:01:01 minus 2s crosses a minute boundary, so the assertion fails if the
    # overlap subtraction is dropped (10:01) rather than applied (10:00).
    run(session, since=datetime(2026, 9, 28, 10, 1, 1), overlap_seconds=2, page_size=10)
    assert 'updated >= "2026-09-28 10:00"' in session.calls[0]["json"]["jql"]


def test_default_upper_bound_comes_from_injected_clock_once():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    list(j.search_issues(
        session, "example.atlassian.net", page_size=10,
        now=lambda: datetime(2026, 9, 28, 12, 0, 0),
    ))
    assert 'updated <= "2026-09-28 12:00"' in session.calls[0]["json"]["jql"]


def test_request_body_shape_and_field_list_always_includes_key_and_updated():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, fields=["summary", "status"], page_size=25, timeout=9)
    call = session.calls[0]
    assert call["url"] == "https://example.atlassian.net/rest/api/3/search/jql"
    assert call["json"]["maxResults"] == 25
    assert call["json"]["fields"] == ["key", "updated", "summary", "status"]
    assert "nextPageToken" not in call["json"]
    assert call["timeout"] == 9


def test_tz_name_shifts_the_watermark_bounds_into_that_timezone():
    # Jira interprets JQL date-time literals in the account's own timezone, not
    # UTC. since/until are given as UTC instants; with tz_name="Asia/Karachi"
    # (+05:00, no DST) the JQL literals must be shifted forward five hours so
    # Jira reads them as meaning the same UTC instants.
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(
        session,
        since=datetime(2026, 9, 28, 5, 0, 0, tzinfo=None),
        until=datetime(2026, 9, 28, 12, 0, 0, tzinfo=None),
        overlap_seconds=0,
        tz_name="Asia/Karachi",
    )
    jql = session.calls[0]["json"]["jql"]
    assert 'updated >= "2026-09-28 10:00"' in jql
    assert 'updated <= "2026-09-28 17:00"' in jql


def test_explicit_utc_tz_name_gives_utc_bounds_without_a_profile_lookup():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, since=datetime(2026, 9, 28, 10, 0, 0), overlap_seconds=0, tz_name="UTC")
    jql = session.calls[0]["json"]["jql"]
    assert 'updated >= "2026-09-28 10:00"' in jql
    assert 'updated <= "2026-09-28 12:00"' in jql
    assert session.get_calls == []


def test_omitted_tz_name_fetches_the_account_timezone_once():
    # The safe behaviour is the default: a caller who forgets tz_name must not
    # silently get UTC bounds for a non-UTC account.
    session = FakeSearch(
        [{"issues": [], "isLast": True, "nextPageToken": None}], account_tz="Asia/Karachi"
    )
    run(
        session,
        since=datetime(2026, 9, 28, 5, 0, 0),
        overlap_seconds=0,
        tz_name=None,
    )
    assert session.get_calls == ["https://example.atlassian.net/rest/api/3/myself"]
    jql = session.calls[0]["json"]["jql"]
    assert 'updated >= "2026-09-28 10:00"' in jql
    assert 'updated <= "2026-09-28 17:00"' in jql


def test_explicit_tz_name_makes_no_profile_lookup():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, tz_name="Asia/Karachi")
    assert session.get_calls == []


def test_profile_lookup_failure_surfaces_instead_of_falling_back_to_utc():
    session = FakeSession([FakeResponse(401, {})])
    with pytest.raises(j.JiraAuthError):
        list(j.search_issues(session, "example.atlassian.net", until=UNTIL))
    assert session.calls[0]["url"].endswith("/rest/api/3/myself")


def test_default_fields_request_every_typed_column_not_just_key_and_updated():
    # A caller who passes no fields= must still get summary/status/etc. populated
    # in to_dataframe; requesting only key+updated silently nulls every other
    # typed column (the bug the notebook shipped with).
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    run(session, page_size=10)
    requested = session.calls[0]["json"]["fields"]
    for name, _ in j.TYPED_FIELDS:
        assert name in requested, "TYPED_FIELDS column %r not requested by default" % name


def test_second_page_includes_next_page_token():
    pages = [
        {"issues": [make_issue(1)], "isLast": False, "nextPageToken": "abc"},
        {"issues": [], "isLast": True, "nextPageToken": None},
    ]
    session = FakeSearch(pages)
    run(session, page_size=1)
    assert session.calls[1]["json"]["nextPageToken"] == "abc"


def test_site_with_scheme_is_normalised():
    session = FakeSearch([{"issues": [], "isLast": True, "nextPageToken": None}])
    list(j.search_issues(session, "https://example.atlassian.net/", until=UNTIL))
    assert session.calls[0]["url"].startswith("https://example.atlassian.net/rest/api/3/")


@pytest.mark.parametrize("kwargs", [
    {"page_size": 0},
    {"page_size": 5001},
    {"query": "project = KAN order by created"},
])
def test_invalid_arguments_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        run(FakeSession([]), **kwargs)


def test_unknown_timezone_name_raises_jira_error():
    with pytest.raises(j.JiraError):
        list(j.search_issues(FakeSession([]), "example.atlassian.net", tz_name="Not/AZone"))
