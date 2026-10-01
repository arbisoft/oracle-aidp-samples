import pytest

import mongodb_client as m

URI = "mongodb+srv://spikeuser:S3cr%40t@cluster0.abcde.mongodb.net/?retryWrites=true"


def test_credentials_from_env_returns_the_uri(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("MONGODB_URI", URI)
    assert m.credentials_from_env() == URI


def test_credentials_from_env_missing_raises_without_leaking(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.delenv("MONGODB_URI", raising=False)
    with pytest.raises(m.MongoError, match="MONGODB_URI"):
        m.credentials_from_env()


def test_credentials_from_env_rejects_a_non_mongodb_uri_without_echoing_it(monkeypatch):
    monkeypatch.delenv("OCI_VAULT_ID", raising=False)
    monkeypatch.setenv("MONGODB_URI", "https://user:pw@example.com")
    with pytest.raises(m.MongoError) as exc:
        m.credentials_from_env()
    assert "pw" not in str(exc.value) and "example.com" not in str(exc.value)


def test_redact_uri_removes_user_password_encoded_password_and_hosts():
    text = ("auth failed for spikeuser with S3cr%40t / S3cr@t on cluster0.abcde.mongodb.net "
            "and shard ac-xyz-shard-00-01.abcde.mongodb.net:27017 at 10.1.2.3")
    out = m.redact_uri(text, URI)
    for leak in ("spikeuser", "S3cr%40t", "S3cr@t", "cluster0", "ac-xyz", "10.1.2.3"):
        assert leak not in out, leak


def test_redact_uri_handles_a_multi_host_uri_without_credentials():
    uri = "mongodb://db1.internal:27017,db2.internal:27018/"
    out = m.redact_uri("cannot reach db1.internal:27017 or db2.internal", uri)
    assert "db1.internal" not in out and "db2.internal" not in out


@pytest.mark.parametrize("uri, expected", [
    ("mongodb+srv://u:p@h", "mongodb+srv://u:p@h/?serverSelectionTimeoutMS=5000"),
    ("mongodb+srv://u:p@h/", "mongodb+srv://u:p@h/?serverSelectionTimeoutMS=5000"),
    ("mongodb+srv://u:p@h/?w=majority", "mongodb+srv://u:p@h/?w=majority&serverSelectionTimeoutMS=5000"),
    ("mongodb://h/db", "mongodb://h/db?serverSelectionTimeoutMS=5000"),
])
def test_with_timeout_appends_server_selection_timeout(uri, expected):
    assert m.with_timeout(uri, 5000) == expected


def test_with_timeout_keeps_a_callers_own_timeout():
    uri = "mongodb://h/?serverSelectionTimeoutMS=1234"
    assert m.with_timeout(uri, 5000) == uri
