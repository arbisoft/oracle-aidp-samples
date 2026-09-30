# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
from datetime import datetime, timezone

import pytest

from fakes import FakeSpark
from notion_connector.state import StateStore

TABLE = "lake.notion_raw.notion_sync_state"
RUN_AT = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
WATERMARK = datetime(2026, 9, 30, 10, 12, tzinfo=timezone.utc)


def test_ensure_creates_the_table():
    spark = FakeSpark()
    StateStore(spark, TABLE).ensure()
    assert spark.statements == [
        f"CREATE TABLE IF NOT EXISTS {TABLE} (object_name STRING, watermark TIMESTAMP, "
        "last_mode STRING, last_status STRING, last_rows BIGINT, last_run_at TIMESTAMP, "
        "last_error STRING) USING DELTA"
    ]


def test_get_returns_none_when_missing_or_null():
    assert StateStore(FakeSpark(), TABLE).get("pages") is None
    assert StateStore(FakeSpark([("unix_micros", [(None,)])]), TABLE).get("pages") is None


def test_get_converts_microseconds_to_utc():
    micros = int(WATERMARK.timestamp()) * 1_000_000
    spark = FakeSpark([("unix_micros", [(micros,)])])
    assert StateStore(spark, TABLE).get("pages") == WATERMARK
    assert spark.statements == [
        f"SELECT unix_micros(watermark) AS watermark_micros FROM {TABLE} WHERE object_name = 'pages'"
    ]


def test_get_rejects_unknown_object_names():
    with pytest.raises(ValueError):
        StateStore(FakeSpark(), TABLE).get("pages' OR '1'='1")


def test_record_success_sets_the_watermark():
    spark = FakeSpark()
    StateStore(spark, TABLE).record_success("pages", "cdc", 7, WATERMARK, RUN_AT)
    assert spark.views["_notion_sync_state_update"] == [("pages", WATERMARK, "cdc", "SUCCESS", 7, RUN_AT, None)]
    merge = spark.statements[-1]
    assert merge.startswith(f"MERGE INTO {TABLE} t USING _notion_sync_state_update s ON t.object_name = s.object_name")
    assert "t.watermark = s.watermark" in merge
    assert "WHEN NOT MATCHED THEN INSERT (object_name, watermark, last_mode, last_status, last_rows, last_run_at, last_error)" in merge


def test_record_failure_never_touches_the_watermark():
    spark = FakeSpark()
    StateStore(spark, TABLE).record_failure("blocks", "cdc", "boom", RUN_AT)
    assert spark.views["_notion_sync_state_update"] == [("blocks", None, "cdc", "FAILED", 0, RUN_AT, "boom")]
    merge = spark.statements[-1]
    assert "t.watermark = s.watermark" not in merge
    assert "t.last_status = s.last_status" in merge
    assert "t.last_error = s.last_error" in merge
