# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import json
from datetime import datetime, timezone

import pytest

from notion_connector.records import (
    BLOCK_COLUMNS,
    DATA_SOURCE_COLUMNS,
    PAGE_COLUMNS,
    USER_COLUMNS,
    block_row,
    data_source_row,
    is_trashed,
    page_row,
    parse_time,
    user_row,
)

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def page(**overrides):
    obj = {
        "object": "page",
        "id": "p1",
        "created_time": "2026-09-01T08:00:00.000Z",
        "last_edited_time": "2026-09-30T10:12:00.000Z",
        "created_by": {"object": "user", "id": "u1"},
        "last_edited_by": {"object": "user", "id": "u2"},
        "in_trash": False,
        "parent": {"type": "workspace", "workspace": True},
        "url": "https://www.notion.so/p1",
        "public_url": None,
        "properties": {
            "Status": {"type": "select", "select": {"name": "Done"}},
            "Name": {
                "type": "title",
                "title": [{"plain_text": "Road"}, {"plain_text": "map"}],
            },
        },
    }
    obj.update(overrides)
    return obj


def names(columns):
    return [name for name, _ in columns]


def test_parse_time_handles_z_offset_and_empty():
    assert parse_time("2026-09-30T10:12:00.000Z") == datetime(2026, 9, 30, 10, 12, tzinfo=timezone.utc)
    assert parse_time("2026-09-30T12:12:00+02:00") == datetime(2026, 9, 30, 10, 12, tzinfo=timezone.utc)
    assert parse_time(None) is None
    assert parse_time("") is None


def test_page_row():
    row = page_row(page(), NOW)
    assert list(row) == names(PAGE_COLUMNS)
    assert row["id"] == "p1"
    assert row["created_time"] == datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    assert row["last_edited_time"] == datetime(2026, 9, 30, 10, 12, tzinfo=timezone.utc)
    assert row["created_by_id"] == "u1"
    assert row["last_edited_by_id"] == "u2"
    assert row["in_trash"] is False
    assert row["parent_type"] == "workspace"
    assert row["parent_id"] is None
    assert row["url"] == "https://www.notion.so/p1"
    assert row["title"] == "Roadmap"
    assert row["_ingested_at"] == NOW
    assert json.loads(row["raw_json"])["id"] == "p1"


@pytest.mark.parametrize(
    "parent, expected_type, expected_id",
    [
        ({"type": "page_id", "page_id": "pp"}, "page_id", "pp"),
        ({"type": "database_id", "database_id": "db"}, "database_id", "db"),
        ({"type": "data_source_id", "data_source_id": "ds", "database_id": "db"}, "data_source_id", "ds"),
        ({"type": "block_id", "block_id": "bb"}, "block_id", "bb"),
        ({"type": "workspace", "workspace": True}, "workspace", None),
        (None, None, None),
    ],
)
def test_parent_types(parent, expected_type, expected_id):
    row = page_row(page(parent=parent), NOW)
    assert row["parent_type"] == expected_type
    assert row["parent_id"] == expected_id


def test_page_without_title_or_authors():
    row = page_row(page(properties={}, created_by=None, last_edited_by=None), NOW)
    assert row["title"] is None
    assert row["created_by_id"] is None
    assert row["last_edited_by_id"] is None


def test_is_trashed_falls_back_to_archived():
    assert is_trashed({"in_trash": True}) is True
    assert is_trashed({"archived": True}) is True
    assert is_trashed({}) is False


def test_raw_json_keeps_non_ascii():
    row = page_row(page(url="https://www.notion.so/café"), NOW)
    assert "café" in row["raw_json"]


def test_data_source_row():
    source = {
        "object": "data_source",
        "id": "ds1",
        "created_time": "2026-09-01T08:00:00.000Z",
        "last_edited_time": "2026-09-02T08:00:00.000Z",
        "in_trash": False,
        "parent": {"type": "database_id", "database_id": "db1"},
        "url": "https://www.notion.so/ds1",
        "title": [{"plain_text": "Tasks"}],
    }
    row = data_source_row(source, NOW)
    assert list(row) == names(DATA_SOURCE_COLUMNS)
    assert row["title"] == "Tasks"
    assert row["parent_id"] == "db1"
    assert data_source_row({**source, "title": None}, NOW)["title"] is None


def test_block_row():
    block = {
        "object": "block",
        "id": "b1",
        "type": "paragraph",
        "has_children": True,
        "created_time": "2026-09-01T08:00:00.000Z",
        "last_edited_time": "2026-09-02T08:00:00.000Z",
        "in_trash": False,
        "parent": {"type": "page_id", "page_id": "p1"},
        "paragraph": {"rich_text": []},
    }
    row = block_row(block, "p1", 2, 5, NOW)
    assert list(row) == names(BLOCK_COLUMNS)
    assert (row["type"], row["has_children"]) == ("paragraph", True)
    assert (row["page_id"], row["depth"], row["position"]) == ("p1", 2, 5)


def test_user_rows():
    person = {
        "object": "user",
        "id": "u1",
        "type": "person",
        "name": "Ada",
        "avatar_url": None,
        "person": {"email": "ada@example.com"},
    }
    bot = {"object": "user", "id": "u2", "type": "bot", "name": "Sync", "bot": {}}
    row = user_row(person, NOW)
    assert list(row) == names(USER_COLUMNS)
    assert row["email"] == "ada@example.com"
    assert user_row(bot, NOW)["email"] is None
    assert user_row(bot, NOW)["type"] == "bot"
