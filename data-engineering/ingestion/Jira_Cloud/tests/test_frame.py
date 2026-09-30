import json
from datetime import datetime, timezone

import pytest

import jira_client as j


class FakeSpark:
    def __init__(self):
        self.calls = []

    def createDataFrame(self, data, schema=None):
        self.calls.append((data, schema))
        return "df"


def as_dict(tup):
    names = [name for name, _ in j.TYPED_FIELDS] + ["raw_fields"]
    return dict(zip(names, tup))


def test_normalize_issue_follows_field_order_and_unwraps_nested_objects():
    issue = {
        "key": "KAN-1",
        "fields": {
            "summary": "Fix the thing",
            "status": {"name": "In Progress"},
            "priority": {"name": "High"},
            "assignee": {"displayName": "Ann"},
            "reporter": None,
            "issuetype": {"name": "Bug"},
            "project": {"key": "KAN"},
            "created": "2026-09-01T08:00:00.000+0000",
            "updated": "2026-09-28T10:00:05.000+0000",
        },
    }
    out = as_dict(j.normalize_issue(issue))
    assert out["key"] == "KAN-1"
    assert out["status"] == "In Progress"
    assert out["assignee"] == "Ann"
    assert out["reporter"] is None
    assert out["project"] == "KAN"
    assert out["updated"] == datetime(2026, 9, 28, 10, 0, 5, tzinfo=timezone.utc)


def test_missing_fields_become_none():
    out = as_dict(j.normalize_issue({"key": "KAN-2", "fields": {}}))
    assert out["summary"] is None
    assert out["assignee"] is None
    assert out["created"] is None


def test_unparseable_timestamp_names_the_field():
    issue = {"key": "KAN-3", "fields": {"updated": "not-a-date"}}
    with pytest.raises(j.JiraError) as exc:
        j.normalize_issue(issue)
    assert "updated" in str(exc.value)


def test_to_dataframe_uses_ddl_from_typed_fields():
    spark = FakeSpark()
    issue = {"key": "KAN-1", "fields": {}}
    assert j.to_dataframe(spark, [issue]) == "df"
    data, ddl = spark.calls[0]
    assert len(data) == 1
    assert ddl.startswith("key STRING")
    assert "updated TIMESTAMP" in ddl


def test_to_dataframe_with_no_issues_still_passes_the_schema():
    spark = FakeSpark()
    j.to_dataframe(spark, [])
    data, ddl = spark.calls[0]
    assert data == []
    assert "summary STRING" in ddl


def test_extra_requested_fields_not_in_typed_fields_are_preserved_as_raw_fields_json():
    # A custom field (or any field beyond TYPED_FIELDS) must not be silently
    # dropped — it is preserved as JSON so a downstream query can still reach it.
    issue = {
        "key": "KAN-1",
        "fields": {
            "summary": "Fix the thing",
            "customfield_10019": "Sprint 4",
            "customfield_10021": {"value": "High"},
        },
    }
    out = as_dict(j.normalize_issue(issue))
    assert out["summary"] == "Fix the thing"
    raw = json.loads(out["raw_fields"])
    assert raw == {"customfield_10019": "Sprint 4", "customfield_10021": {"value": "High"}}


def test_raw_fields_is_none_when_nothing_extra_was_requested():
    issue = {"key": "KAN-2", "fields": {"summary": "x"}}
    out = as_dict(j.normalize_issue(issue))
    assert out["raw_fields"] is None


def test_to_dataframe_ddl_includes_raw_fields_column():
    spark = FakeSpark()
    j.to_dataframe(spark, [{"key": "KAN-1", "fields": {}}])
    data, ddl = spark.calls[0]
    assert "raw_fields STRING" in ddl
    assert len(data[0]) == len(j.TYPED_FIELDS) + 1


def test_to_dataframe_keeps_only_the_latest_row_per_key():
    def issue(key, updated, summary):
        return {"key": key, "fields": {"summary": summary, "updated": updated}}

    spark = FakeSpark()
    j.to_dataframe(spark, [
        issue("KAN-1", "2026-01-01T10:00:00.000+0000", "old"),
        issue("KAN-2", "2026-01-01T09:00:00.000+0000", "other"),
        issue("KAN-1", "2026-01-01T11:00:00.000+0000", "new"),
    ])
    rows = [as_dict(r) for r in spark.calls[0][0]]
    assert [(r["key"], r["summary"]) for r in rows] == [("KAN-1", "new"), ("KAN-2", "other")]


def test_parse_watermark_reads_a_utc_string_as_aware_utc():
    assert j.parse_watermark("2026-09-29 11:14:22.180") == datetime(
        2026, 9, 29, 11, 14, 22, 180000, tzinfo=timezone.utc
    )
    assert j.parse_watermark(None) is None
