# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import pytest

from fakes import FakeSpark
from notion_connector.config import TargetSettings
from notion_connector.load import Writer, ddl

COLUMNS = (("id", "STRING"), ("last_edited_time", "TIMESTAMP"), ("_ingested_at", "TIMESTAMP"))
BLOCK_COLUMNS = (("id", "STRING"), ("page_id", "STRING"), ("last_edited_time", "TIMESTAMP"))
TARGET = TargetSettings(catalog="lake", schema="notion_raw", table_prefix="n_")


def rows(count):
    return [{"id": f"r{i}", "last_edited_time": i, "_ingested_at": 0} for i in range(count)]


def test_ddl():
    assert ddl(COLUMNS) == "id STRING, last_edited_time TIMESTAMP, _ingested_at TIMESTAMP"


def test_ensure_schema():
    spark = FakeSpark()
    Writer(spark, TARGET).ensure_schema()
    assert spark.statements == ["CREATE SCHEMA IF NOT EXISTS lake.notion_raw"]


def test_overwrite_stages_in_batches_then_swaps():
    spark = FakeSpark()
    count = Writer(spark, TARGET, batch_size=2).overwrite("pages", iter(rows(5)), COLUMNS)
    assert count == 5
    assert [len(batch) for _, batch, _ in spark.saved] == [2, 2, 1]
    assert {table for table, _, _ in spark.saved} == {"lake.notion_raw.n_pages__staging"}
    assert {mode for _, _, mode in spark.saved} == {"append"}
    assert spark.saved[0][1][0] == ("r0", 0, 0)  # tuples in column order
    assert spark.schemas[0] == ddl(COLUMNS)
    assert spark.statements[0].startswith("CREATE TABLE IF NOT EXISTS lake.notion_raw.n_pages (")
    assert "USING DELTA" in spark.statements[0]
    final = [s for s in spark.statements if s.startswith("INSERT OVERWRITE TABLE lake.notion_raw.n_pages ")]
    assert len(final) == 1
    assert "PARTITION BY id ORDER BY _ingested_at DESC" in final[0]
    assert spark.statements[-1] == "DROP TABLE IF EXISTS lake.notion_raw.n_pages__staging"


def test_overwrite_with_no_rows_still_empties_the_table():
    spark = FakeSpark()
    assert Writer(spark, TARGET).overwrite("pages", [], COLUMNS) == 0
    assert spark.saved == []
    assert any(s.startswith("INSERT OVERWRITE TABLE lake.notion_raw.n_pages ") for s in spark.statements)


def test_merge_deduplicates_the_source():
    spark = FakeSpark()
    assert Writer(spark, TARGET).merge("pages", rows(3), COLUMNS) == 3
    merge = [s for s in spark.statements if s.startswith("MERGE INTO")]
    assert len(merge) == 1
    assert merge[0].startswith("MERGE INTO lake.notion_raw.n_pages t USING (SELECT id, last_edited_time, _ingested_at FROM (")
    assert "ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_edited_time DESC) AS _rn" in merge[0]
    assert "WHERE _rn = 1" in merge[0]
    assert "ON t.id = s.id" in merge[0]
    assert "WHEN MATCHED THEN UPDATE SET *" in merge[0]
    assert "WHEN NOT MATCHED THEN INSERT *" in merge[0]


def test_replace_pages_deletes_then_inserts():
    spark = FakeSpark()
    block_rows = [{"id": "b1", "page_id": "p1", "last_edited_time": 1}]
    count = Writer(spark, TARGET).replace_pages("blocks", block_rows, BLOCK_COLUMNS, ["p1", "p2"])
    assert count == 1
    ids_table = "lake.notion_raw.n_blocks__pages_staging"
    assert (ids_table, [("p1",), ("p2",)], "append") in spark.saved
    delete = f"DELETE FROM lake.notion_raw.n_blocks WHERE page_id IN (SELECT page_id FROM {ids_table})"
    insert = [s for s in spark.statements if s.startswith("INSERT INTO lake.notion_raw.n_blocks ")]
    assert delete in spark.statements
    assert len(insert) == 1
    assert "PARTITION BY page_id, id ORDER BY last_edited_time DESC" in insert[0]
    assert spark.statements.index(delete) < spark.statements.index(insert[0])
    assert spark.statements[-2:] == [
        "DROP TABLE IF EXISTS lake.notion_raw.n_blocks__staging",
        f"DROP TABLE IF EXISTS {ids_table}",
    ]


def test_replace_pages_with_no_rows_still_deletes():
    spark = FakeSpark()
    Writer(spark, TARGET).replace_pages("blocks", [], BLOCK_COLUMNS, ["p1"])
    assert any(s.startswith("DELETE FROM lake.notion_raw.n_blocks ") for s in spark.statements)


def test_staging_is_dropped_when_the_final_statement_fails():
    spark = FakeSpark()
    spark.fail_on = "MERGE INTO"
    with pytest.raises(RuntimeError, match="simulated failure"):
        Writer(spark, TARGET).merge("pages", rows(1), COLUMNS)
    assert spark.statements[-1] == "DROP TABLE IF EXISTS lake.notion_raw.n_pages__staging"


def test_staging_is_dropped_when_the_row_source_fails():
    def broken():
        yield rows(1)[0]
        raise RuntimeError("notion went away")

    spark = FakeSpark()
    with pytest.raises(RuntimeError, match="notion went away"):
        Writer(spark, TARGET).overwrite("pages", broken(), COLUMNS)
    assert not any(s.startswith("INSERT OVERWRITE") for s in spark.statements)
    assert spark.statements[-1] == "DROP TABLE IF EXISTS lake.notion_raw.n_pages__staging"
