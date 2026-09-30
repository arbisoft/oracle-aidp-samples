# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
from datetime import datetime, timezone

import pytest

from fakes import FakeNotion, client_for, error_response, make_block, make_data_source, make_page, make_user, ts
from notion_connector.client import NotionApiError
from notion_connector.extract import iter_page_blocks, iter_search, iter_users


def at(minute, hour=10):
    return datetime(2026, 9, 30, hour, minute, tzinfo=timezone.utc)


def ids(items):
    return [item["id"] for item in items]


def test_search_returns_newest_first_across_pages():
    fake = FakeNotion(pages=[make_page("a", ts(1)), make_page("b", ts(3)), make_page("c", ts(2))], page_size=2)
    assert ids(iter_search(client_for(fake), "page")) == ["b", "c", "a"]
    body = fake.search_calls()[0]
    assert body["filter"] == {"property": "object", "value": "page"}
    assert body["sort"] == {"timestamp": "last_edited_time", "direction": "descending"}
    assert len(fake.search_calls()) == 2


def test_search_stops_at_since():
    pages = [make_page(f"p{m}", ts(m)) for m in range(1, 7)]
    fake = FakeNotion(pages=pages, page_size=2)
    # since is inclusive: a page edited exactly at `since` is returned.
    assert ids(iter_search(client_for(fake), "page", since=at(4))) == ["p6", "p5", "p4"]
    # p3 on the second page stopped the scan, so the third page was never requested.
    assert len(fake.search_calls()) == 2


def test_search_trashed_pass_sets_filter():
    fake = FakeNotion(pages=[make_page("live", ts(1)), make_page("gone", ts(2), in_trash=True)])
    assert ids(iter_search(client_for(fake), "page", in_trash=True)) == ["gone"]
    assert fake.search_calls()[0]["filter"] == {"property": "object", "value": "page", "in_trash": True}
    assert ids(iter_search(client_for(fake), "page")) == ["live"]


def test_search_data_sources():
    fake = FakeNotion(pages=[make_page("p", ts(1))], data_sources=[make_data_source("d", ts(2))])
    assert ids(iter_search(client_for(fake), "data_source")) == ["d"]


def test_search_skips_partial_objects():
    partial = {"object": "page", "id": "partial"}
    fake = FakeNotion(pages=[make_page("full", ts(5)), partial])
    assert ids(iter_search(client_for(fake), "page", since=at(1))) == ["full"]


def test_block_walk_is_depth_first_with_depth_and_position():
    fake = FakeNotion(
        blocks={
            "page": [make_block("b1", has_children=True), make_block("b2")],
            "b1": [make_block("b1a"), make_block("b1b", has_children=True)],
            "b1b": [make_block("b1b1")],
        }
    )
    walked = [(b["id"], depth, pos) for b, depth, pos in iter_page_blocks(client_for(fake), "page")]
    assert walked == [("b1", 0, 0), ("b1a", 1, 0), ("b1b", 1, 1), ("b1b1", 2, 0), ("b2", 0, 1)]


def test_block_walk_pages_through_children():
    fake = FakeNotion(blocks={"page": [make_block(f"b{i}") for i in range(5)]}, page_size=2)
    walked = [(b["id"], pos) for b, _, pos in iter_page_blocks(client_for(fake), "page")]
    assert walked == [("b0", 0), ("b1", 1), ("b2", 2), ("b3", 3), ("b4", 4)]


def test_block_walk_records_but_does_not_descend_child_pages_and_databases():
    fake = FakeNotion(
        blocks={
            "page": [
                make_block("sub", type="child_page", has_children=True),
                make_block("db", type="child_database", has_children=True),
            ],
            "sub": [make_block("inside-sub")],
            "db": [make_block("inside-db")],
        }
    )
    assert ids(b for b, _, _ in iter_page_blocks(client_for(fake), "page")) == ["sub", "db"]
    assert fake.block_calls() == ["page"]


def test_block_walk_descends_original_synced_block_but_not_a_reference():
    original = make_block("orig", type="synced_block", has_children=True, synced_block={"synced_from": None})
    reference = make_block(
        "ref", type="synced_block", has_children=True, synced_block={"synced_from": {"block_id": "orig"}}
    )
    fake = FakeNotion(blocks={"page": [original, reference], "orig": [make_block("shared")], "ref": [make_block("shared")]})
    assert ids(b for b, _, _ in iter_page_blocks(client_for(fake), "page")) == ["orig", "shared", "ref"]


def test_block_walk_ignores_a_block_seen_twice():
    looping = make_block("loop", has_children=True)
    fake = FakeNotion(blocks={"page": [looping], "loop": [looping]})
    assert ids(b for b, _, _ in iter_page_blocks(client_for(fake), "page")) == ["loop"]


def test_unreadable_nested_block_skips_only_its_subtree():
    fake = FakeNotion(
        blocks={
            "page": [make_block("b1", has_children=True), make_block("b2", has_children=True)],
            "b2": [make_block("b2a")],
        }
    )
    fake.errors["/v1/blocks/b1/children"] = error_response(404, "object_not_found")
    assert ids(b for b, _, _ in iter_page_blocks(client_for(fake), "page")) == ["b1", "b2", "b2a"]


def test_unreadable_page_root_raises():
    fake = FakeNotion()
    with pytest.raises(NotionApiError) as caught:
        list(iter_page_blocks(client_for(fake), "missing-page"))
    assert caught.value.status == 404


def test_iter_users():
    fake = FakeNotion(users=[make_user("u1", "Ada", "ada@example.com"), make_user("u2", "Bot")], page_size=1)
    assert ids(iter_users(client_for(fake))) == ["u1", "u2"]
