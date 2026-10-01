"""SRV resolution on the driver. DNS is faked: no network, no JVM."""

import pytest

import mongodb_client as m

SRV = ["0 0 27017 shard-00-00.abcde.example.net.",
       "0 0 27017 shard-00-01.abcde.example.net.",
       "0 0 27017 shard-00-02.abcde.example.net."]
TXT = ['"authSource=admin&replicaSet=rs-shard-0"']
HOSTS = ("shard-00-00.abcde.example.net:27017,shard-00-01.abcde.example.net:27017,"
         "shard-00-02.abcde.example.net:27017")


def test_hosts_come_from_srv_options_from_txt_and_tls_is_switched_on():
    got = m.build_standard_uri("mongodb+srv://reader@cluster0.abcde.example.net/", SRV, TXT)
    assert got == ("mongodb://reader@" + HOSTS + "/?authSource=admin&replicaSet=rs-shard-0&tls=true")


def test_uri_options_and_path_win_over_txt():
    got = m.build_standard_uri(
        "mongodb+srv://cluster0.abcde.example.net/mydb?authSource=other&tls=false&w=majority", SRV, TXT)
    assert got == ("mongodb://" + HOSTS + "/mydb?authSource=other&replicaSet=rs-shard-0"
                   "&tls=false&w=majority")


def test_txt_options_outside_the_spec_are_ignored():
    got = m.build_standard_uri("mongodb+srv://cluster0.abcde.example.net",
                               SRV, ['"replicaSet=rs&connectTimeoutMS=1"'])
    assert "connectTimeoutMS" not in got and "replicaSet=rs" in got


@pytest.mark.parametrize("record", ["0 0 27017 evil.attacker.example.", "0 0 27017 abcde.example.net."])
def test_a_target_outside_the_srv_hosts_domain_is_rejected(record):
    with pytest.raises(m.MongoError, match="domain"):
        m.build_standard_uri("mongodb+srv://u@cluster0.abcde.example.net/", [record], TXT)


def test_no_srv_records_is_an_error():
    with pytest.raises(m.MongoError, match="no hosts"):
        m.build_standard_uri("mongodb+srv://cluster0.abcde.example.net/", [], TXT)


def test_resolve_srv_looks_up_srv_and_txt_for_the_host(monkeypatch):
    lookups = []

    def fake(spark, name, kind):
        lookups.append((name, kind))
        return SRV if kind == "SRV" else TXT

    monkeypatch.setattr(m, "_dns_records", fake)
    got = m.resolve_srv("spark", "mongodb+srv://reader@cluster0.abcde.example.net/?w=1")
    assert lookups == [("_mongodb._tcp.cluster0.abcde.example.net", "SRV"),
                       ("cluster0.abcde.example.net", "TXT")]
    assert got.startswith("mongodb://reader@" + HOSTS + "/?")


def test_resolve_srv_leaves_a_standard_uri_alone(monkeypatch):
    monkeypatch.setattr(m, "_dns_records", lambda *a: pytest.fail("no lookup expected"))
    assert m.resolve_srv("spark", "mongodb://h1:27017/") == "mongodb://h1:27017/"


def test_a_failed_lookup_raises_a_redacted_mongo_error(monkeypatch):
    def boom(spark, name, kind):
        raise RuntimeError("lookup failed for " + name)

    monkeypatch.setattr(m, "_dns_records", boom)
    with pytest.raises(m.MongoError) as exc:
        m.resolve_srv("spark", "mongodb+srv://reader@cluster0.abcde.example.net/")
    assert "cluster0.abcde.example.net" not in str(exc.value)


def test_redact_uri_removes_resolved_hosts_and_oci_storage_paths():
    uri = m.build_standard_uri("mongodb+srv://reader@cluster0.abcde.example.net/", SRV, TXT)
    text = ("Task failed while writing rows to oci://bucket@namespace/x.cat/db.db/t. "
            "cannot reach shard-00-01.abcde.example.net:27017 as reader")
    out = m.redact_uri(text, uri)
    assert "namespace" not in out and "shard-00-01" not in out and "reader" not in out
