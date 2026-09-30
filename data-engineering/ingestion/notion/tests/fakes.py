# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Test doubles: no network, no Spark."""

from __future__ import annotations

import json as _json


class FakeTime:
    """A clock that only moves when something sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text is not None else _json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON body")
        return self._payload


def error_response(status, code, message="boom", headers=None):
    return FakeResponse(
        status,
        {"object": "error", "status": status, "code": code, "message": message},
        headers=headers,
    )


class FakeSession:
    """Replays queued responses (or raises queued exceptions) and records calls."""

    def __init__(self, responses):
        self._queue = list(responses)
        self.calls = []

    def request(self, method, url, headers=None, json=None, params=None, timeout=None):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "json": json, "params": params}
        )
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ts(minute, hour=10):
    """A Notion-style timestamp on 2026-09-30."""
    return f"2026-09-30T{hour:02d}:{minute:02d}:00.000Z"


def make_page(page_id, edited, in_trash=False, title="Title"):
    return {
        "object": "page",
        "id": page_id,
        "created_time": ts(0, hour=8),
        "last_edited_time": edited,
        "created_by": {"object": "user", "id": "u1"},
        "last_edited_by": {"object": "user", "id": "u1"},
        "in_trash": in_trash,
        "parent": {"type": "workspace", "workspace": True},
        "url": f"https://www.notion.so/{page_id}",
        "public_url": None,
        "properties": {"Name": {"type": "title", "title": [{"plain_text": title}]}},
    }


def make_data_source(source_id, edited, in_trash=False, title="Source"):
    return {
        "object": "data_source",
        "id": source_id,
        "created_time": ts(0, hour=8),
        "last_edited_time": edited,
        "in_trash": in_trash,
        "parent": {"type": "database_id", "database_id": f"db-{source_id}"},
        "url": f"https://www.notion.so/{source_id}",
        "title": [{"plain_text": title}],
    }


def make_block(block_id, type="paragraph", has_children=False, edited=None, **extra):
    block = {
        "object": "block",
        "id": block_id,
        "type": type,
        "has_children": has_children,
        "created_time": ts(0, hour=8),
        "last_edited_time": edited or ts(0, hour=9),
        "in_trash": False,
        "parent": {"type": "page_id", "page_id": "unknown"},
        type: {},
    }
    block.update(extra)
    return block


def make_user(user_id, name="User", email=None):
    if email:
        return {"object": "user", "id": user_id, "type": "person", "name": name, "person": {"email": email}}
    return {"object": "user", "id": user_id, "type": "bot", "name": name, "bot": {}}


class FakeNotion:
    """An in-memory Notion workspace that answers the endpoints the connector calls.

    Use it as the ``session`` of a NotionClient. ``blocks`` maps a parent id
    (page or block) to its list of child blocks. Set ``errors[path]`` to a
    FakeResponse to make that path fail.
    """

    def __init__(self, pages=(), data_sources=(), blocks=None, users=(), page_size=100):
        self.pages = list(pages)
        self.data_sources = list(data_sources)
        self.blocks = dict(blocks or {})
        self.users = list(users)
        self.page_size = page_size
        self.calls = []
        self.errors = {}

    def request(self, method, url, headers=None, json=None, params=None, timeout=None):
        path = url.replace("https://api.notion.com", "")
        self.calls.append((method, path, json, params))
        if path in self.errors:
            return self.errors[path]
        if path == "/v1/users/me":
            return FakeResponse(200, {"object": "user", "id": "bot", "type": "bot"})
        if path == "/v1/users":
            return self._listing(self.users, params or {})
        if path == "/v1/search":
            wanted = json["filter"]
            pool = self.pages if wanted["value"] == "page" else self.data_sources
            trashed = bool(wanted.get("in_trash", False))
            # Objects without last_edited_time model partial responses; they sort last.
            items = [o for o in pool if bool(o.get("in_trash", False)) == trashed]
            items.sort(key=lambda o: o.get("last_edited_time", ""), reverse=True)
            return self._listing(items, json)
        if path.startswith("/v1/blocks/") and path.endswith("/children"):
            parent_id = path.split("/")[3]
            if parent_id in self.blocks:
                return self._listing(self.blocks[parent_id], params or {})
            if any(p["id"] == parent_id for p in self.pages):
                return self._listing([], params or {})
            return error_response(404, "object_not_found", "Could not find block")
        return error_response(404, "object_not_found", f"no route for {path}")

    def _listing(self, items, args):
        size = min(int(args.get("page_size", 100)), self.page_size)
        start = int(args.get("start_cursor") or 0)
        chunk = items[start : start + size]
        more = start + size < len(items)
        return FakeResponse(
            200,
            {
                "object": "list",
                "results": chunk,
                "has_more": more,
                "next_cursor": str(start + size) if more else None,
            },
        )

    def search_calls(self, value=None):
        calls = [body for method, path, body, _ in self.calls if path == "/v1/search"]
        if value is None:
            return calls
        return [body for body in calls if body["filter"]["value"] == value]

    def block_calls(self):
        return [path.split("/")[3] for _, path, _, _ in self.calls if path.endswith("/children")]


def client_for(fake):
    from notion_connector.client import NotionClient

    fake_time = FakeTime()
    return NotionClient("secret_token", session=fake, sleep=fake_time.sleep, clock=fake_time.clock)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return list(self._rows)


class _FakeWriter:
    def __init__(self, spark, rows):
        self._spark = spark
        self._rows = rows
        self._mode = None

    def format(self, _name):
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def saveAsTable(self, table):
        self._spark.saved.append((table, self._rows, self._mode))


class _FakeFrame:
    def __init__(self, spark, rows, schema):
        self._spark = spark
        self.rows = rows
        self.schema = schema

    @property
    def write(self):
        return _FakeWriter(self._spark, self.rows)

    def createOrReplaceTempView(self, name):
        self._spark.views[name] = self.rows


class FakeSpark:
    """Records SQL and DataFrame writes. ``results`` is a list of
    (substring, rows): the first entry whose substring occurs in a statement
    supplies what ``.collect()`` returns for it."""

    def __init__(self, results=()):
        self.statements = []
        self.saved = []
        self.views = {}
        self.schemas = []
        self.fail_on = None
        self._results = list(results)

    def sql(self, statement):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError(f"simulated failure: {self.fail_on}")
        for fragment, rows in self._results:
            if fragment in statement:
                return _FakeResult(rows)
        return _FakeResult([])

    def createDataFrame(self, data, schema=None):
        self.schemas.append(schema)
        return _FakeFrame(self, list(data), schema)
