"""GcpClient.get_text against a fake session (no network)."""
from __future__ import annotations

import unittest

from gcp_aidp.gcp_client import GcpClient, GcpError


class _Response:
    def __init__(self, status, text, body=None):
        self.status_code, self.text, self.content, self._body = status, text, text.encode(), body or {}

    def json(self):
        if not self._body:
            raise ValueError("not JSON")
        return self._body


class _Session:
    def __init__(self, response):
        self.response, self.calls = response, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return self.response


def _client(response) -> tuple[GcpClient, _Session]:
    client = GcpClient("p", timeout=7)
    client._session = _Session(response)
    return client, client._session


class GetText(unittest.TestCase):
    def test_returns_the_raw_body_and_sends_a_get_with_the_params(self):
        client, session = _client(_Response(200, "import airflow\nwith DAG('x'):\n    pass\n"))
        text = client.get_text("https://storage.googleapis.com/storage/v1/b/b/o/dag.py", {"alt": "media"})
        self.assertEqual(text, "import airflow\nwith DAG('x'):\n    pass\n")
        self.assertEqual(session.calls, [("https://storage.googleapis.com/storage/v1/b/b/o/dag.py",
                                          {"alt": "media"}, 7)])

    def test_an_error_status_raises_like_get(self):
        body = {"error": {"message": "no such object", "status": "NOT_FOUND"}}
        client, _ = _client(_Response(404, "{}", body))
        with self.assertRaisesRegex(GcpError, "HTTP 404 NOT_FOUND: no such object") as ctx:
            client.get_text("https://example.invalid/x")
        self.assertEqual(ctx.exception.status, 404)

    def test_there_is_still_no_write_method(self):
        self.assertFalse(any(hasattr(GcpClient, m) for m in ("post", "put", "patch", "delete")))


if __name__ == "__main__":
    unittest.main()
