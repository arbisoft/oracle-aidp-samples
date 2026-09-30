# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Write rows to Delta tables in the target catalog and schema.

Rows are streamed in batches into a staging table, then moved into the target
by one final statement. That keeps driver memory bounded on large loads and
means a failure while reading from Notion never leaves the target half-written.

Table and column names come from validated configuration and from constants in
this package, never from Notion data, so building SQL with them is safe. Data
values always travel through DataFrames.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Sequence, Tuple

Column = Tuple[str, str]


def ddl(columns: Sequence[Column]) -> str:
    return ", ".join(f"{name} {sql_type}" for name, sql_type in columns)


class Writer:
    def __init__(self, spark: Any, target: Any, batch_size: int = 5000):
        self._spark = spark
        self._target = target
        self._batch_size = batch_size

    def ensure_schema(self) -> None:
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._target.qualified_schema}")

    def overwrite(
        self,
        name: str,
        rows: Iterable[Dict[str, Any]],
        columns: Sequence[Column],
        key: Sequence[str] = ("id",),
        order_by: str = "_ingested_at",
    ) -> int:
        """Replace the whole table with ``rows``."""
        table = self._target.table(name)
        staging = table + "__staging"
        self._create(table, columns)
        try:
            count = self._stage(staging, rows, columns)
            source = self._deduplicated(staging, columns, key, order_by)
            self._spark.sql(f"INSERT OVERWRITE TABLE {table} {source}")
        finally:
            self._drop(staging)
        return count

    def merge(
        self,
        name: str,
        rows: Iterable[Dict[str, Any]],
        columns: Sequence[Column],
        key: Sequence[str] = ("id",),
        order_by: str = "last_edited_time",
    ) -> int:
        """Upsert ``rows`` into the table on ``key``."""
        table = self._target.table(name)
        staging = table + "__staging"
        self._create(table, columns)
        try:
            count = self._stage(staging, rows, columns)
            # Delta rejects a MERGE whose source has two rows for one key.
            source = self._deduplicated(staging, columns, key, order_by)
            condition = " AND ".join(f"t.{column} = s.{column}" for column in key)
            self._spark.sql(
                f"MERGE INTO {table} t USING ({source}) s ON {condition} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        finally:
            self._drop(staging)
        return count

    def replace_pages(
        self,
        name: str,
        rows: Iterable[Dict[str, Any]],
        columns: Sequence[Column],
        page_ids: Iterable[str],
        key: Sequence[str] = ("page_id", "id"),
        order_by: str = "last_edited_time",
    ) -> int:
        """Delete every row belonging to ``page_ids``, then insert ``rows``.

        Two statements, not one transaction. If the run dies between them the
        caller does not advance its watermark, so the next run repeats this.
        """
        table = self._target.table(name)
        staging = table + "__staging"
        ids_staging = table + "__pages_staging"
        self._create(table, columns)
        try:
            count = self._stage(staging, rows, columns)
            self._stage(ids_staging, ({"page_id": page_id} for page_id in page_ids), (("page_id", "STRING"),))
            self._spark.sql(f"DELETE FROM {table} WHERE page_id IN (SELECT page_id FROM {ids_staging})")
            self._spark.sql(f"INSERT INTO {table} {self._deduplicated(staging, columns, key, order_by)}")
        finally:
            self._drop(staging)
            self._drop(ids_staging)
        return count

    def _create(self, table: str, columns: Sequence[Column]) -> None:
        self._spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl(columns)}) USING DELTA")

    def _drop(self, table: str) -> None:
        self._spark.sql(f"DROP TABLE IF EXISTS {table}")

    def _stage(self, staging: str, rows: Iterable[Dict[str, Any]], columns: Sequence[Column]) -> int:
        self._drop(staging)
        self._spark.sql(f"CREATE TABLE {staging} ({ddl(columns)}) USING DELTA")
        names = [name for name, _ in columns]
        schema = ddl(columns)
        total = 0
        batch = []
        for row in rows:
            batch.append(tuple(row[name] for name in names))
            if len(batch) >= self._batch_size:
                self._append(staging, batch, schema)
                total += len(batch)
                batch = []
        if batch:
            self._append(staging, batch, schema)
            total += len(batch)
        return total

    def _append(self, table: str, batch: list, schema: str) -> None:
        self._spark.createDataFrame(batch, schema).write.format("delta").mode("append").saveAsTable(table)

    @staticmethod
    def _deduplicated(staging: str, columns: Sequence[Column], key: Sequence[str], order_by: str) -> str:
        names = ", ".join(name for name, _ in columns)
        partition = ", ".join(key)
        return (
            f"SELECT {names} FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by} DESC) AS _rn "
            f"FROM {staging}) WHERE _rn = 1"
        )
