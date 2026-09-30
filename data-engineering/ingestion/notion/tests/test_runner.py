# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
from datetime import datetime, timezone

import pytest

from fakes import (
    FakeNotion,
    InMemoryState,
    InMemoryWriter,
    client_for,
    error_response,
    make_block,
    make_data_source,
    make_page,
    make_user,
    ts,
)
from notion_connector.config import ConfigError, parse_config
from notion_connector.runner import NotionAuthError, format_summary, raise_on_failure, run

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def at(minute, hour=10):
    return datetime(2026, 9, 30, hour, minute, tzinfo=timezone.utc)


def config(objects, mode=None):
    sync = {"objects": list(objects)}
    if mode:
        sync["mode"] = mode
    return parse_config({"target": {"catalog": "lake", "schema": "notion_raw"}, "sync": sync})


class Harness:
    def __init__(self, fake):
        self.fake = fake
        self.writer = InMemoryWriter()
        self.state = InMemoryState()

    def run(self, objects, mode=None, mode_override=None):
        self.fake.calls.clear()
        self.writer.operations.clear()
        summaries = run(
            None,
            config(objects, mode),
            "secret_token",
            mode_override,
            client=client_for(self.fake),
            writer=self.writer,
            state=self.state,
            now=lambda: NOW,
            log=lambda message: None,
        )
        return {summary["object"]: summary for summary in summaries}

    def row(self, table, row_id):
        return next(row for row in self.writer.tables[table] if row["id"] == row_id)


def test_first_cdc_run_loads_everything_and_sets_watermarks():
    fake = FakeNotion(
        pages=[make_page("p1", ts(5)), make_page("p2", ts(1))],
        data_sources=[make_data_source("d1", ts(3))],
        blocks={"p1": [make_block("b1")], "p2": [make_block("b2")]},
        users=[make_user("u1", "Ada", "ada@example.com")],
    )
    harness = Harness(fake)
    result = harness.run(["pages", "blocks", "users", "data_sources"])

    assert list(result) == ["users", "data_sources", "pages", "blocks"]  # fixed run order
    assert all(summary["status"] == "SUCCESS" for summary in result.values())
    assert result["pages"]["mode"] == "full" and result["pages"]["note"] == "first run"
    assert result["users"]["mode"] == "full" and result["users"]["note"] == "no change tracking"
    assert harness.writer.ids("pages") == ["p1", "p2"]
    assert harness.writer.ids("blocks") == ["b1", "b2"]
    assert harness.writer.ids("users") == ["u1"]
    assert harness.writer.ids("data_sources") == ["d1"]
    assert harness.state.get("pages") == at(5)
    assert harness.state.get("blocks") == at(5)
    assert harness.state.get("data_sources") == at(3)
    assert harness.state.get("users") is None
    assert result["pages"]["watermark"] == "2026-09-30T10:05:00+00:00"
    assert harness.writer.schema_ensured and harness.state.ensured
    assert harness.row("pages", "p1")["_ingested_at"] == NOW


def test_second_cdc_run_only_reads_changes_inside_the_overlap():
    fake = FakeNotion(pages=[make_page("old", ts(0, hour=9)), make_page("p1", ts(5)), make_page("p2", ts(1))])
    harness = Harness(fake)
    harness.run(["pages"])
    fake.pages[2] = make_page("p2", ts(30), title="Renamed")

    result = harness.run(["pages"])

    # since = 10:05 - 120s = 10:03, so p2 (10:30) and p1 (10:05) are re-read; "old" is not.
    assert result["pages"]["mode"] == "cdc"
    assert result["pages"]["rows"] == 2
    assert harness.writer.operations == [("merge", "pages", 2)]
    assert harness.writer.ids("pages") == ["old", "p1", "p2"]
    assert harness.row("pages", "p2")["title"] == "Renamed"
    assert harness.state.get("pages") == at(30)


def test_cdc_run_with_no_changes_keeps_the_watermark():
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    harness = Harness(fake)
    harness.run(["pages"])
    fake.pages.clear()  # nothing visible any more
    result = harness.run(["pages"])
    assert result["pages"]["rows"] == 0
    assert harness.state.get("pages") == at(5)
    assert harness.writer.ids("pages") == ["p1"]  # CDC never purges


def test_users_are_always_fully_refreshed():
    fake = FakeNotion(users=[make_user("u1"), make_user("u2")])
    harness = Harness(fake)
    harness.run(["users"])
    fake.users.pop()
    result = harness.run(["users"])
    assert result["users"]["mode"] == "full"
    assert harness.writer.operations == [("overwrite", "users", 1)]
    assert harness.writer.ids("users") == ["u1"]


def test_blocks_work_without_pages_selected():
    fake = FakeNotion(pages=[make_page("p1", ts(5))], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    result = harness.run(["blocks"])
    assert list(result) == ["blocks"]
    assert harness.writer.ids("blocks") == ["b1"]
    assert "pages" not in harness.writer.tables
    assert harness.state.get("blocks") == at(5)


def test_cdc_replaces_blocks_of_changed_pages_only():
    fake = FakeNotion(
        pages=[make_page("p1", ts(5)), make_page("p2", ts(0, hour=9))],
        blocks={"p1": [make_block("b1"), make_block("b2")], "p2": [make_block("c1")]},
    )
    harness = Harness(fake)
    harness.run(["pages", "blocks"])
    fake.pages[0] = make_page("p1", ts(40))
    fake.blocks["p1"] = [make_block("b2"), make_block("b3")]  # b1 deleted, b3 added

    result = harness.run(["pages", "blocks"])

    assert result["blocks"]["mode"] == "cdc"
    assert harness.writer.ids("blocks") == ["b2", "b3", "c1"]
    assert fake.block_calls() == ["p1"]  # p2 was not walked again
    assert harness.state.get("blocks") == at(40)


def test_cdc_page_with_no_blocks_left_clears_its_old_rows():
    fake = FakeNotion(pages=[make_page("p1", ts(5))], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    harness.run(["blocks"])
    fake.pages[0] = make_page("p1", ts(40))
    fake.blocks["p1"] = []
    harness.run(["blocks"])
    assert harness.writer.ids("blocks") == []


def test_cdc_trashed_page_is_flagged_and_its_blocks_removed():
    fake = FakeNotion(pages=[make_page("p1", ts(5))], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    harness.run(["pages", "blocks"])
    fake.pages[0] = make_page("p1", ts(40), in_trash=True)

    harness.run(["pages", "blocks"])

    assert harness.row("pages", "p1")["in_trash"] is True
    assert harness.writer.ids("blocks") == []
    assert fake.block_calls() == []  # trashed pages are not walked


def test_duplicate_page_in_results_lands_once():
    duplicate = make_page("p1", ts(5))
    fake = FakeNotion(pages=[duplicate, dict(duplicate)], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    harness.run(["pages", "blocks"])
    assert harness.writer.ids("pages") == ["p1"]
    assert harness.writer.ids("blocks") == ["b1"]
    assert fake.block_calls() == ["p1"]  # walked once


def test_full_mode_overwrites_and_purges():
    fake = FakeNotion(pages=[make_page("p1", ts(5)), make_page("p2", ts(1))], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    harness.run(["pages", "blocks"])
    del fake.pages[0]  # p1 permanently deleted

    result = harness.run(["pages", "blocks"], mode="full")

    assert result["pages"]["mode"] == "full" and result["pages"]["note"] is None
    assert harness.writer.ids("pages") == ["p2"]
    assert harness.writer.ids("blocks") == []
    assert harness.state.get("pages") == at(1)


@pytest.mark.parametrize("override", ["FULL", " full "])
def test_mode_override_is_case_insensitive(override):
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    harness = Harness(fake)
    harness.run(["pages"])
    result = harness.run(["pages"], mode_override=override)
    assert harness.writer.operations == [("overwrite", "pages", 1)]
    assert result["pages"]["note"] is None


@pytest.mark.parametrize("override", [None, "", "  "])
def test_empty_mode_override_uses_the_config(override):
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    harness = Harness(fake)
    harness.run(["pages"])
    harness.run(["pages"], mode_override=override)
    assert harness.writer.operations == [("merge", "pages", 1)]


def test_bad_mode_override_is_rejected():
    with pytest.raises(ConfigError):
        Harness(FakeNotion()).run(["pages"], mode_override="incremental")


def test_failed_object_keeps_its_watermark_and_others_continue():
    fake = FakeNotion(pages=[make_page("p1", ts(5))], users=[make_user("u1")])
    harness = Harness(fake)
    harness.run(["pages", "users"])
    fake.pages[0] = make_page("p1", ts(40))
    harness.writer.fail_on = {"pages"}

    result = harness.run(["pages", "users"])

    assert result["pages"]["status"] == "FAILED"
    assert "write to pages failed" in result["pages"]["error"]
    assert result["users"]["status"] == "SUCCESS"
    assert harness.state.get("pages") == at(5)
    assert harness.state.rows["pages"]["last_status"] == "FAILED"


def test_blocks_run_their_own_discovery_when_the_pages_step_breaks_midway():
    fake = FakeNotion(pages=[make_page("p1", ts(5))], blocks={"p1": [make_block("b1")]})
    harness = Harness(fake)
    fake.errors["/v1/search"] = error_response(400, "validation_error")
    result = harness.run(["pages", "blocks"])
    assert result["pages"]["status"] == "FAILED"
    assert result["blocks"]["status"] == "FAILED"
    assert result["blocks"]["error"].startswith("page discovery failed")
    assert harness.state.get("blocks") is None

    del fake.errors["/v1/search"]
    harness.writer.fail_on = {"pages"}
    result = harness.run(["pages", "blocks"])
    assert result["pages"]["status"] == "FAILED"
    assert result["blocks"]["status"] == "SUCCESS"
    assert harness.writer.ids("blocks") == ["b1"]


def test_page_that_vanishes_before_its_walk_is_skipped():
    fake = FakeNotion(
        pages=[make_page("p1", ts(5)), make_page("p2", ts(4))],
        blocks={"p1": [make_block("b1")]},
    )
    fake.errors["/v1/blocks/p2/children"] = error_response(404, "object_not_found")
    harness = Harness(fake)
    result = harness.run(["blocks"])
    assert result["blocks"]["status"] == "SUCCESS"
    assert result["blocks"]["note"] == "first run; 1 page(s) skipped (deleted or no longer shared)"
    assert harness.writer.ids("blocks") == ["b1"]


def test_block_walk_server_error_fails_the_object():
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    fake.errors["/v1/blocks/p1/children"] = error_response(400, "validation_error")
    result = Harness(fake).run(["blocks"])
    assert result["blocks"]["status"] == "FAILED"


def test_full_run_with_no_visible_pages_adds_a_hint():
    result = Harness(FakeNotion()).run(["pages"])
    assert result["pages"]["status"] == "SUCCESS"
    assert "shared with" in result["pages"]["note"]


def test_invalid_token_aborts_before_anything_is_written():
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    fake.errors["/v1/users/me"] = error_response(401, "unauthorized", "API token is invalid.")
    harness = Harness(fake)
    with pytest.raises(NotionAuthError, match="401"):
        harness.run(["pages"])
    assert harness.writer.tables == {} and not harness.state.ensured


def test_users_403_explains_the_missing_capability():
    fake = FakeNotion(pages=[make_page("p1", ts(5))])
    fake.errors["/v1/users"] = error_response(403, "restricted_resource")
    result = Harness(fake).run(["users", "pages"])
    assert result["users"]["status"] == "FAILED"
    assert "user information" in result["users"]["error"]
    assert result["pages"]["status"] == "SUCCESS"


def test_errors_never_contain_the_token():
    fake = FakeNotion()
    fake.errors["/v1/search"] = error_response(400, "validation_error", "bad filter")
    result = Harness(fake).run(["pages", "data_sources"])
    assert all("secret_token" not in summary["error"] for summary in result.values())


def test_raise_on_failure_and_format_summary():
    summaries = [
        {"object": "pages", "mode": "cdc", "status": "SUCCESS", "rows": 3,
         "watermark": "2026-09-30T10:05:00+00:00", "note": None, "error": None},
        {"object": "blocks", "mode": "cdc", "status": "FAILED", "rows": 0,
         "watermark": None, "note": None, "error": "boom"},
    ]
    text = format_summary(summaries)
    assert "pages" in text and "FAILED" in text and "boom" in text
    with pytest.raises(RuntimeError, match="blocks: boom"):
        raise_on_failure(summaries)
    raise_on_failure(summaries[:1])  # no failure, no exception
