"""Test doubles for the Jira helper. No network, no Spark."""


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

    def _next(self):
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return self._next()

    def get(self, url, timeout=None):
        self.calls.append({"url": url, "timeout": timeout})
        return self._next()


class FakeSearch:
    """Serves pre-built /search/jql response pages in order, by call count.

    Each page is a dict like {"issues": [...], "isLast": bool, "nextPageToken": str|None}.
    Ignores the request body — paging correctness is tested here, JQL construction
    is tested separately in test_query.py.
    """

    def __init__(self, pages, on_call=None, account_tz="UTC"):
        self.pages = list(pages)
        self.on_call = on_call
        self.account_tz = account_tz
        self.calls = []
        self.get_calls = []

    def get(self, url, timeout=None):
        self.get_calls.append(url)
        return FakeResponse(200, {"timeZone": self.account_tz})

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        n = len(self.calls)
        if self.on_call:
            self.on_call(n)
        page = self.pages[n - 1] if n <= len(self.pages) else self.pages[-1]
        return FakeResponse(200, page)
