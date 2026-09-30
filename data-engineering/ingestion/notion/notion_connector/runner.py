# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Run one sync: decide full or CDC per object, extract, write, record state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from .client import NotionApiError, NotionClient
from .config import SUPPORTED_OBJECTS, Config, normalize_mode
from .extract import iter_page_blocks, iter_search, iter_users
from .load import Writer
from .records import (
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
from .state import StateStore

STATE_TABLE = "notion_sync_state"
# A page deleted or unshared between discovery and its block walk.
_SKIP_STATUSES = (403, 404)
_MAX_ERROR_LENGTH = 1000

StepResult = Tuple[int, Optional[datetime], Optional[str]]


class NotionAuthError(RuntimeError):
    """Notion rejected the token before any work started."""


@dataclass(frozen=True)
class PageRef:
    id: str
    last_edited_time: datetime
    in_trash: bool


def run(
    spark: Any,
    config: Config,
    token: str,
    mode_override: Optional[str] = None,
    *,
    client: Any = None,
    writer: Any = None,
    state: Any = None,
    now: Optional[Callable[[], datetime]] = None,
    log: Callable[[str], None] = print,
) -> List[Dict[str, Any]]:
    """Sync the configured objects and return one summary dict per object.

    ``client``, ``writer``, ``state`` and ``now`` exist for tests; leave them
    unset on a cluster.
    """
    mode = config.sync.mode
    if mode_override is not None and str(mode_override).strip():
        mode = normalize_mode(mode_override)
    run_at = now() if now else datetime.now(timezone.utc)
    if client is None:
        client = NotionClient(token, config.notion.api_version, config.notion.requests_per_second)
    if writer is None:
        writer = Writer(spark, config.target)
    if state is None:
        state = StateStore(spark, config.target.table(STATE_TABLE))

    _preflight(client)
    writer.ensure_schema()
    state.ensure()
    return _Sync(client, writer, state, config, mode, run_at, log).execute()


def format_summary(summaries: List[Dict[str, Any]]) -> str:
    lines = [f"{'object':<14}{'mode':<6}{'status':<9}{'rows':>8}  watermark / note / error"]
    for item in summaries:
        detail = " | ".join(str(part) for part in (item["watermark"], item["note"], item["error"]) if part)
        lines.append(f"{item['object']:<14}{item['mode']:<6}{item['status']:<9}{item['rows']:>8}  {detail}")
    return "\n".join(lines)


def raise_on_failure(summaries: List[Dict[str, Any]]) -> None:
    failed = [f"{item['object']}: {item['error']}" for item in summaries if item["status"] != "SUCCESS"]
    if failed:
        raise RuntimeError("Notion sync failed for " + "; ".join(failed))


def _preflight(client: Any) -> None:
    try:
        client.request("GET", "/v1/users/me")
    except NotionApiError as exc:
        if exc.status in (401, 403):
            raise NotionAuthError(
                f"Notion rejected the token ({exc.status} {exc.code}). Check that the credential holds "
                "the integration secret and that the integration is still installed in the workspace."
            ) from exc
        raise


def _describe(exc: BaseException) -> str:
    text = str(exc) or type(exc).__name__
    return text[:_MAX_ERROR_LENGTH]


class _NewestEdit:
    """Tracks the greatest last_edited_time seen while rows stream past."""

    def __init__(self) -> None:
        self.value: Optional[datetime] = None

    def see(self, obj: Dict[str, Any]) -> None:
        edited = parse_time(obj.get("last_edited_time"))
        if edited is not None and (self.value is None or edited > self.value):
            self.value = edited


class _Sync:
    def __init__(self, client, writer, state, config: Config, mode: str, run_at: datetime, log):
        self.client = client
        self.writer = writer
        self.state = state
        self.mode = mode
        self.run_at = run_at
        self.log = log
        self.overlap = timedelta(seconds=config.sync.overlap_seconds)
        self.selected = [name for name in SUPPORTED_OBJECTS if name in config.sync.objects]
        self.watermarks = {name: state.get(name) for name in self.selected}
        # Pages found by discovery, keyed by id. None until discovery has run to completion.
        self.page_refs: Optional[Dict[str, PageRef]] = None

    def execute(self) -> List[Dict[str, Any]]:
        steps = {
            "users": self._users,
            "data_sources": self._data_sources,
            "pages": self._pages,
            "blocks": self._blocks,
        }
        return [self._step(name, steps[name]) for name in self.selected]

    # ---- mode and thresholds -------------------------------------------------

    def _effective_mode(self, name: str) -> Tuple[str, Optional[str]]:
        if name == "users":
            return "full", "no change tracking"
        if self.mode == "full":
            return "full", None
        if self.watermarks[name] is None:
            return "full", "first run"
        return "cdc", None

    def _since(self, name: str) -> Optional[datetime]:
        mode, _ = self._effective_mode(name)
        return None if mode == "full" else self.watermarks[name] - self.overlap

    def _discovery_since(self) -> Optional[datetime]:
        thresholds = [self._since(name) for name in ("pages", "blocks") if name in self.selected]
        if any(threshold is None for threshold in thresholds):
            return None
        return min(thresholds)

    # ---- one object ----------------------------------------------------------

    def _step(self, name: str, step: Callable[[str], StepResult]) -> Dict[str, Any]:
        mode, note = self._effective_mode(name)
        self.log(f"[notion] {name}: starting ({mode})")
        try:
            rows, newest, extra_note = step(mode)
        except Exception as exc:  # one object failing must not stop the others
            error = _describe(exc)
            self.state.record_failure(name, mode, error, self.run_at)
            self.log(f"[notion] {name}: FAILED - {error}")
            return self._summary(name, mode, "FAILED", 0, self.watermarks[name], note, error)
        watermark = newest or self.watermarks[name]
        self.state.record_success(name, mode, rows, watermark, self.run_at)
        self.log(f"[notion] {name}: {rows} row(s)")
        notes = "; ".join(part for part in (note, extra_note) if part) or None
        return self._summary(name, mode, "SUCCESS", rows, watermark, notes, None)

    @staticmethod
    def _summary(name, mode, status, rows, watermark, note, error) -> Dict[str, Any]:
        return {
            "object": name,
            "mode": mode,
            "status": status,
            "rows": rows,
            "watermark": watermark.isoformat() if watermark else None,
            "note": note,
            "error": error,
        }

    # ---- extract + write per object -----------------------------------------

    def _users(self, mode: str) -> StepResult:
        def rows() -> Iterator[Dict[str, Any]]:
            try:
                for user in iter_users(self.client):
                    yield user_row(user, self.run_at)
            except NotionApiError as exc:
                if exc.status == 403:
                    raise RuntimeError(
                        "Notion returned 403 for the user list: give the integration the "
                        "'Read user information' capability, or remove 'users' from sync.objects."
                    ) from exc
                raise

        count = self.writer.overwrite("users", rows(), USER_COLUMNS, key=("id",), order_by="_ingested_at")
        return count, None, None

    def _data_sources(self, mode: str) -> StepResult:
        since = self._since("data_sources")
        newest = _NewestEdit()

        def rows() -> Iterator[Dict[str, Any]]:
            for in_trash in (False, True):
                for source in iter_search(self.client, "data_source", since=since, in_trash=in_trash):
                    newest.see(source)
                    yield data_source_row(source, self.run_at)

        write = self.writer.overwrite if mode == "full" else self.writer.merge
        count = write("data_sources", rows(), DATA_SOURCE_COLUMNS, key=("id",), order_by="last_edited_time")
        return count, newest.value, None

    def _discover_pages(self) -> Iterator[Dict[str, Any]]:
        """Yield changed pages (live, then trashed); set ``page_refs`` once exhausted."""
        refs: Dict[str, PageRef] = {}
        since = self._discovery_since()
        for in_trash in (False, True):
            for page in iter_search(self.client, "page", since=since, in_trash=in_trash):
                edited = parse_time(page["last_edited_time"])
                known = refs.get(page["id"])
                if known is None or edited >= known.last_edited_time:
                    refs[page["id"]] = PageRef(page["id"], edited, is_trashed(page))
                yield page
        self.page_refs = refs

    def _pages(self, mode: str) -> StepResult:
        since = self._since("pages")
        newest = _NewestEdit()

        def rows() -> Iterator[Dict[str, Any]]:
            for page in self._discover_pages():
                # Discovery may reach further back than this object needs (for blocks).
                if since is not None and parse_time(page["last_edited_time"]) < since:
                    continue
                newest.see(page)
                yield page_row(page, self.run_at)

        write = self.writer.overwrite if mode == "full" else self.writer.merge
        count = write("pages", rows(), PAGE_COLUMNS, key=("id",), order_by="last_edited_time")
        note = None
        if mode == "full" and count == 0:
            note = "no pages visible - is the integration shared with any pages?"
        return count, newest.value, note

    def _blocks(self, mode: str) -> StepResult:
        if self.page_refs is None:
            try:
                for _ in self._discover_pages():
                    pass
            except Exception as exc:
                raise RuntimeError(f"page discovery failed: {_describe(exc)}") from exc

        since = self._since("blocks")
        changed = [
            ref for ref in self.page_refs.values() if since is None or ref.last_edited_time >= since
        ]
        skipped = [0]

        def rows() -> Iterator[Dict[str, Any]]:
            for ref in changed:
                if ref.in_trash:
                    continue
                try:
                    # Read the whole page before yielding so a page is never half-written.
                    blocks = list(iter_page_blocks(self.client, ref.id))
                except NotionApiError as exc:
                    if exc.status in _SKIP_STATUSES:
                        skipped[0] += 1
                        continue
                    raise
                for block, depth, position in blocks:
                    yield block_row(block, ref.id, depth, position, self.run_at)

        key, order_by = ("page_id", "id"), "last_edited_time"
        if mode == "full":
            count = self.writer.overwrite("blocks", rows(), BLOCK_COLUMNS, key=key, order_by=order_by)
        else:
            page_ids = [ref.id for ref in changed]
            count = self.writer.replace_pages(
                "blocks", rows(), BLOCK_COLUMNS, page_ids, key=key, order_by=order_by
            )
        newest = max((ref.last_edited_time for ref in changed), default=None)
        note = f"{skipped[0]} page(s) skipped (deleted or no longer shared)" if skipped[0] else None
        return count, newest, note
