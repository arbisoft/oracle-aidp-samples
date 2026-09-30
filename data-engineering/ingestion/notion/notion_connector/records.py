# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Turn Notion API objects into flat rows, and declare the target columns.

Each table keeps a few typed columns for filtering and joining, plus the whole
API object in ``raw_json`` so nothing is lost. Column specs and row mappers
live together because they must change together.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

Column = Tuple[str, str]

_COMMON: Tuple[Column, ...] = (
    ("id", "STRING"),
    ("created_time", "TIMESTAMP"),
    ("last_edited_time", "TIMESTAMP"),
    ("created_by_id", "STRING"),
    ("last_edited_by_id", "STRING"),
    ("in_trash", "BOOLEAN"),
    ("parent_type", "STRING"),
    ("parent_id", "STRING"),
)
_TAIL: Tuple[Column, ...] = (("raw_json", "STRING"), ("_ingested_at", "TIMESTAMP"))

PAGE_COLUMNS: Tuple[Column, ...] = (
    _COMMON + (("url", "STRING"), ("public_url", "STRING"), ("title", "STRING")) + _TAIL
)
DATA_SOURCE_COLUMNS: Tuple[Column, ...] = _COMMON + (("url", "STRING"), ("title", "STRING")) + _TAIL
BLOCK_COLUMNS: Tuple[Column, ...] = (
    _COMMON
    + (
        ("type", "STRING"),
        ("has_children", "BOOLEAN"),
        ("page_id", "STRING"),
        ("depth", "INT"),
        ("position", "INT"),
    )
    + _TAIL
)
USER_COLUMNS: Tuple[Column, ...] = (
    ("id", "STRING"),
    ("type", "STRING"),
    ("name", "STRING"),
    ("avatar_url", "STRING"),
    ("email", "STRING"),
) + _TAIL


def parse_time(value: Optional[str]) -> Optional[datetime]:
    """Parse a Notion ISO 8601 timestamp into an aware UTC datetime."""
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_trashed(obj: Dict[str, Any]) -> bool:
    # `archived` is the deprecated alias of `in_trash`.
    return bool(obj.get("in_trash", obj.get("archived", False)))


def page_row(page: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    row = _common(page)
    row["url"] = page.get("url")
    row["public_url"] = page.get("public_url")
    row["title"] = _page_title(page)
    return _finish(row, page, ingested_at)


def data_source_row(data_source: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    row = _common(data_source)
    row["url"] = data_source.get("url")
    title = data_source.get("title")
    row["title"] = _plain_text(title) if title is not None else None
    return _finish(row, data_source, ingested_at)


def block_row(
    block: Dict[str, Any], page_id: str, depth: int, position: int, ingested_at: datetime
) -> Dict[str, Any]:
    row = _common(block)
    row["type"] = block.get("type")
    row["has_children"] = bool(block.get("has_children", False))
    row["page_id"] = page_id
    row["depth"] = depth
    row["position"] = position
    return _finish(row, block, ingested_at)


def user_row(user: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    row = {
        "id": user.get("id"),
        "type": user.get("type"),
        "name": user.get("name"),
        "avatar_url": user.get("avatar_url"),
        "email": (user.get("person") or {}).get("email"),
    }
    return _finish(row, user, ingested_at)


def _common(obj: Dict[str, Any]) -> Dict[str, Any]:
    parent_type, parent_id = _parent(obj)
    return {
        "id": obj.get("id"),
        "created_time": parse_time(obj.get("created_time")),
        "last_edited_time": parse_time(obj.get("last_edited_time")),
        "created_by_id": (obj.get("created_by") or {}).get("id"),
        "last_edited_by_id": (obj.get("last_edited_by") or {}).get("id"),
        "in_trash": is_trashed(obj),
        "parent_type": parent_type,
        "parent_id": parent_id,
    }


def _finish(row: Dict[str, Any], obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    row["raw_json"] = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    row["_ingested_at"] = ingested_at
    return row


def _parent(obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    parent = obj.get("parent") or {}
    parent_type = parent.get("type")
    value = parent.get(parent_type) if parent_type else None
    # A workspace parent carries `true`, not an id.
    return parent_type, value if isinstance(value, str) else None


def _plain_text(rich_text: Any) -> str:
    return "".join(part.get("plain_text", "") for part in (rich_text or []))


def _page_title(page: Dict[str, Any]) -> Optional[str]:
    for prop in (page.get("properties") or {}).values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            return _plain_text(prop.get("title"))
    return None
