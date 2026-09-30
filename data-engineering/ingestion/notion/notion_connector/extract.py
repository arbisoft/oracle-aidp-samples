# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Generators over the Notion endpoints the connector reads. No Spark, no state."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Tuple

from .records import parse_time

SEARCH_PATH = "/v1/search"
USERS_PATH = "/v1/users"
# Their content belongs to another page or data source, which search returns on its own.
_NEVER_DESCEND = ("child_page", "child_database")


def iter_search(
    client: Any,
    object_type: str,
    since: Optional[datetime] = None,
    in_trash: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Yield pages or data sources shared with the integration, newest edit first.

    Notion search cannot filter by edit time, so incremental reads sort
    newest-first and stop at the first object edited before ``since``.
    """
    search_filter: Dict[str, Any] = {"property": "object", "value": object_type}
    if in_trash:
        search_filter["in_trash"] = True
    body = {
        "filter": search_filter,
        "sort": {"timestamp": "last_edited_time", "direction": "descending"},
    }
    for obj in client.paginate("POST", SEARCH_PATH, body=body):
        if obj.get("object") != object_type:
            continue
        edited = parse_time(obj.get("last_edited_time"))
        if edited is None:
            # A partial object (id only): the integration cannot read its content.
            continue
        if since is not None and edited < since:
            return
        yield obj


def iter_page_blocks(client: Any, page_id: str) -> Iterator[Tuple[Dict[str, Any], int, int]]:
    """Yield ``(block, depth, position)`` for every block under a page, depth-first."""
    seen = set()

    def walk(parent_id: str, depth: int) -> Iterator[Tuple[Dict[str, Any], int, int]]:
        children = client.paginate("GET", f"/v1/blocks/{parent_id}/children")
        for position, block in enumerate(children):
            block_id = block.get("id")
            if block_id in seen:
                continue
            seen.add(block_id)
            yield block, depth, position
            if block.get("has_children") and _should_descend(block):
                yield from walk(block_id, depth + 1)

    yield from walk(page_id, 0)


def iter_users(client: Any) -> Iterator[Dict[str, Any]]:
    return client.paginate("GET", USERS_PATH)


def _should_descend(block: Dict[str, Any]) -> bool:
    block_type = block.get("type")
    if block_type in _NEVER_DESCEND:
        return False
    if block_type == "synced_block" and (block.get("synced_block") or {}).get("synced_from"):
        # A reference to a synced block; the original carries the content.
        return False
    return True
