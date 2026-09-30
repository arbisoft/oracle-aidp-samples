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
