import json
from datetime import datetime, timedelta, timezone

import pytest

import mongodb_client as m

UTC = timezone.utc
SINCE = datetime(2015, 1, 1, 0, 5, tzinfo=UTC)
UNTIL = datetime(2016, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)


def test_no_watermark_means_no_pipeline():
    assert m.build_pipeline() is None


def test_watermark_field_without_bounds_means_no_pipeline():
    assert m.build_pipeline(watermark_field="date") is None


def test_bounds_without_a_watermark_field_raise():
    with pytest.raises(ValueError, match="watermark_field"):
        m.build_pipeline(since=SINCE)


def test_since_minus_overlap_and_until_become_a_date_match():
    pipeline = json.loads(m.build_pipeline(
        watermark_field="date", since=SINCE, until=UNTIL, overlap_seconds=300))
    assert pipeline == [{"$match": {"date": {
        "$gte": {"$date": "2015-01-01T00:00:00.000Z"},  # 00:05 minus 300 s
        "$lte": {"$date": "2016-01-01T12:00:00.123Z"},  # milliseconds, truncated
    }}}]


def test_first_run_has_only_the_upper_bound():
    pipeline = json.loads(m.build_pipeline(watermark_field="date", until=UNTIL))
    assert pipeline == [{"$match": {"date": {"$lte": {"$date": "2016-01-01T12:00:00.123Z"}}}}]


def test_non_utc_bounds_are_converted_to_utc():
    plus5 = timezone(timedelta(hours=5))
    pipeline = json.loads(m.build_pipeline(
        watermark_field="date", until=datetime(2016, 1, 1, 5, 0, tzinfo=plus5)))
    assert pipeline[0]["$match"]["date"]["$lte"] == {"$date": "2016-01-01T00:00:00.000Z"}


def test_naive_datetimes_are_rejected():
    # The class of bug Jira hit: an ambiguous timezone silently shifts the watermark.
    with pytest.raises(ValueError, match="timezone-aware"):
        m.build_pipeline(watermark_field="date", since=datetime(2015, 1, 1))
