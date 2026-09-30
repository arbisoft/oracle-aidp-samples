from datetime import datetime, timedelta, timezone

import pytest

import jira_client as j


def test_bare_query_only_orders_by_updated_then_key():
    assert j.build_jql() == "ORDER BY updated ASC, key ASC"


def test_all_conditions_combine_in_fixed_order():
    q = j.build_jql(
        query='project = KAN',
        since=datetime(2026, 9, 28, 10, 0, 0),
        until=datetime(2026, 9, 28, 12, 0, 0),
    )
    assert q == (
        '(project = KAN) AND updated >= "2026-09-28 10:00"'
        ' AND updated <= "2026-09-28 12:00" ORDER BY updated ASC, key ASC'
    )


def test_query_without_since_or_until():
    assert j.build_jql(query="project = KAN") == "(project = KAN) ORDER BY updated ASC, key ASC"


def test_aware_datetimes_are_converted_to_utc():
    tz = timezone(timedelta(hours=5))
    assert j.format_jql_timestamp(datetime(2026, 9, 28, 12, 0, 0, tzinfo=tz)) == "2026-09-28 07:00"


def test_naive_datetimes_are_treated_as_utc():
    assert j.format_jql_timestamp(datetime(2026, 9, 28, 7, 0, 0)) == "2026-09-28 07:00"


def test_format_jql_timestamp_converts_to_a_given_timezone_not_just_utc():
    # Jira interprets JQL date-time literals in the searching account's own
    # timezone, not UTC — an aware UTC instant must be re-expressed in tz=.
    tz = timezone(timedelta(hours=5))
    utc_instant = datetime(2026, 9, 28, 7, 0, 0, tzinfo=timezone.utc)
    assert j.format_jql_timestamp(utc_instant, tz=tz) == "2026-09-28 12:00"


def test_build_jql_uses_the_given_timezone_for_both_bounds():
    tz = timezone(timedelta(hours=5))
    q = j.build_jql(
        since=datetime(2026, 9, 28, 10, 0, 0, tzinfo=timezone.utc),
        until=datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc),
        tz=tz,
    )
    assert 'updated >= "2026-09-28 15:00"' in q
    assert 'updated <= "2026-09-28 17:00"' in q


def test_build_jql_defaults_to_utc_when_no_tz_given():
    q = j.build_jql(since=datetime(2026, 9, 28, 10, 0, 0))
    assert 'updated >= "2026-09-28 10:00"' in q


@pytest.mark.parametrize("bad", [
    "project = KAN order by created",
    "project = KAN ORDER BY created",
    "ORDER BY key",
])
def test_caller_query_with_order_by_is_rejected(bad):
    with pytest.raises(ValueError):
        j.build_jql(query=bad)
