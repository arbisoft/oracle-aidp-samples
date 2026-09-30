"""Test doubles for the shared HTTP engine. No network, no Spark."""


class FakeResponse:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("response body is not JSON")
        return self._payload


class FakeSession:
    """Returns scripted responses in order and records every call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return self._responses.pop(0)

    def get(self, url, timeout=None):
        self.calls.append({"url": url, "timeout": timeout})
        return self._responses.pop(0)
