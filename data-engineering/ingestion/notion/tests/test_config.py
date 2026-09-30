# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import copy
import datetime

import pytest

from notion_connector.config import (
    ConfigError,
    load_config,
    normalize_mode,
    parse_config,
)


def base():
    return {
        "target": {"catalog": "lake", "schema": "notion_raw"},
        "sync": {"objects": ["pages", "users"]},
    }


def with_change(section, key, value):
    data = copy.deepcopy(base())
    data.setdefault(section, {})[key] = value
    return data


def test_defaults():
    cfg = parse_config(base())
    assert cfg.sync.mode == "cdc"
    assert cfg.sync.overlap_seconds == 120
    assert cfg.sync.objects == ("pages", "users")
    assert cfg.notion.api_version == "2026-03-11"
    assert cfg.notion.requests_per_second == 3.0
    assert cfg.notion.token_env == "NOTION_TOKEN"
    assert cfg.notion.credential_name is None
    assert cfg.notion.credential_key == "secret"
    assert cfg.target.table_prefix == ""


def test_table_names():
    cfg = parse_config(with_change("target", "table_prefix", "notion_"))
    assert cfg.target.table("pages") == "lake.notion_raw.notion_pages"
    assert cfg.target.qualified_schema == "lake.notion_raw"
    assert parse_config(base()).target.table("pages") == "lake.notion_raw.pages"


def test_mode_is_case_insensitive():
    assert parse_config(with_change("sync", "mode", " FULL ")).sync.mode == "full"
    assert parse_config(with_change("sync", "mode", None)).sync.mode == "cdc"


def test_normalize_mode():
    assert normalize_mode("CDC") == "cdc"
    with pytest.raises(ConfigError, match="cdc, full"):
        normalize_mode("incremental")


def test_unquoted_api_version_is_kept_as_string():
    # PyYAML turns an unquoted 2026-03-11 into a date.
    cfg = parse_config(with_change("notion", "api_version", datetime.date(2026, 3, 11)))
    assert cfg.notion.api_version == "2026-03-11"


@pytest.mark.parametrize(
    "data, fragment",
    [
        ({"sync": {"objects": ["pages"]}}, "missing section 'target'"),
        ({"target": {"catalog": "lake", "schema": "s"}}, "missing section 'sync'"),
        (with_change("target", "catalog", None), "target.catalog"),
        (with_change("target", "schema", "bad-name"), "target.schema"),
        (with_change("target", "catalog", "a.b"), "target.catalog"),
        (with_change("target", "table_prefix", "1x"), "target.table_prefix"),
        (with_change("sync", "objects", []), "sync.objects"),
        (with_change("sync", "objects", "pages"), "sync.objects"),
        (with_change("sync", "objects", ["pages", "comments"]), "comments"),
        (with_change("sync", "objects", ["pages", "pages"]), "duplicate"),
        (with_change("sync", "mode", "incremental"), "cdc, full"),
        (with_change("sync", "overlap_seconds", -1), "overlap_seconds"),
        (with_change("sync", "overlap_seconds", True), "overlap_seconds"),
        (with_change("notion", "requests_per_second", 0), "requests_per_second"),
        (with_change("notion", "token", "secret_abc"), "unknown key"),
        (with_change("sync", "modee", "full"), "unknown key"),
        ({**base(), "extra": {}}, "unknown section"),
        (["not", "a", "mapping"], "must be a mapping"),
    ],
)
def test_invalid_config_is_rejected(data, fragment):
    with pytest.raises(ConfigError, match=fragment):
        parse_config(data)


def test_load_config_reads_yaml(tmp_path):
    path = tmp_path / "notion_ingest.yaml"
    path.write_text(
        "notion:\n"
        "  api_version: 2026-03-11\n"
        "target:\n"
        "  catalog: lake\n"
        "  schema: notion_raw\n"
        "sync:\n"
        "  objects: [pages, blocks]\n",
        encoding="utf-8",
    )
    cfg = load_config(str(path))
    assert cfg.notion.api_version == "2026-03-11"
    assert cfg.sync.objects == ("pages", "blocks")
    assert cfg.sync.mode == "cdc"


def test_load_config_empty_file(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ConfigError, match="must be a mapping"):
        load_config(str(path))
