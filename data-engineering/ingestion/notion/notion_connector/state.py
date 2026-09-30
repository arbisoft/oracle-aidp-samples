# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""One row per Notion object: its watermark and the outcome of its last run."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from .config import SUPPORTED_OBJECTS
from .load import ddl

STATE_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("object_name", "STRING"),
    ("watermark", "TIMESTAMP"),
    ("last_mode", "STRING"),
    ("last_status", "STRING"),
    ("last_rows", "BIGINT"),
    ("last_run_at", "TIMESTAMP"),
    ("last_error", "STRING"),
)
_VIEW = "_notion_sync_state_update"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class StateStore:
    def __init__(self, spark: Any, table: str):
        self._spark = spark
        self._table = table

    def ensure(self) -> None:
        self._spark.sql(f"CREATE TABLE IF NOT EXISTS {self._table} ({ddl(STATE_COLUMNS)}) USING DELTA")

    def get(self, object_name: str) -> Optional[datetime]:
        """Return the stored watermark as an aware UTC datetime, or None."""
        _check(object_name)
        # Read microseconds rather than a timestamp so the result does not
        # depend on the Spark session or driver time zone.
        rows = self._spark.sql(
            f"SELECT unix_micros(watermark) AS watermark_micros FROM {self._table} "
            f"WHERE object_name = '{object_name}'"
        ).collect()
        if not rows or rows[0][0] is None:
            return None
        return _EPOCH + timedelta(microseconds=int(rows[0][0]))

    def record_success(
        self, object_name: str, mode: str, rows: int, watermark: Optional[datetime], run_at: datetime
    ) -> None:
        self._upsert((object_name, watermark, mode, "SUCCESS", int(rows), run_at, None), keep_watermark=False)

    def record_failure(self, object_name: str, mode: str, error: str, run_at: datetime) -> None:
        self._upsert((object_name, None, mode, "FAILED", 0, run_at, error), keep_watermark=True)

    def _upsert(self, row: tuple, keep_watermark: bool) -> None:
        _check(row[0])
        names = [name for name, _ in STATE_COLUMNS]
        self._spark.createDataFrame([row], ddl(STATE_COLUMNS)).createOrReplaceTempView(_VIEW)
        updated = [
            name for name in names if name != "object_name" and not (keep_watermark and name == "watermark")
        ]
        assignments = ", ".join(f"t.{name} = s.{name}" for name in updated)
        values = ", ".join(f"s.{name}" for name in names)
        self._spark.sql(
            f"MERGE INTO {self._table} t USING {_VIEW} s ON t.object_name = s.object_name "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}) VALUES ({values})"
        )


def _check(object_name: str) -> None:
    # The name is interpolated into SQL, so only the fixed set is accepted.
    if object_name not in SUPPORTED_OBJECTS:
        raise ValueError(f"unknown object name: {object_name!r}")
