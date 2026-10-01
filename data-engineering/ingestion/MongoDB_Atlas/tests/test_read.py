import json
from datetime import datetime, timezone

import pytest

import mongodb_client as m
from fakes import FakeJavaException, FakePy4JError, FakeSchema, FakeSpark, field

URI = "mongodb+srv://spikeuser:S3cret@cluster0.abcde.mongodb.net/"
INFERRED = {"type": "struct", "fields": [
    field("_id", "string"),
    field("price", "decimal(5,2)"),
    field("year", "integer"),
    field("released", "timestamp"),
    field("awards", {"type": "struct", "fields": [field("wins", "integer")]}),
    field("scores", {"type": "array", "elementType": "decimal(3,1)", "containsNull": False}),
    field("attrs", {"type": "map", "keyType": "string", "valueType": "integer",
                    "valueContainsNull": False}),
]}


def types(schema_json):
    return {f["name"]: f["type"] for f in schema_json["fields"]}


# --- widen_schema ------------------------------------------------------------

def test_widen_schema_widens_decimals_and_ints_everywhere_and_makes_all_nullable():
    wide = m.widen_schema(INFERRED)
    t = types(wide)
    assert t["price"] == "decimal(38,10)"
    assert t["year"] == "long"
    assert t["_id"] == "string" and t["released"] == "timestamp"
    assert types(t["awards"])["wins"] == "long"
    assert t["scores"]["elementType"] == "decimal(38,10)" and t["scores"]["containsNull"]
    assert t["attrs"]["valueType"] == "long" and t["attrs"]["valueContainsNull"]
    assert all(f["nullable"] for f in wide["fields"])


def test_widen_schema_keeps_a_larger_decimal_scale():
    assert types(m.widen_schema({"type": "struct", "fields": [field("x", "decimal(20,12)")]})) == {
        "x": "decimal(38,12)"}


def test_widen_schema_forces_string_fields():
    wide = m.widen_schema(INFERRED, string_fields=["year", "awards"])
    assert types(wide)["year"] == "string" and types(wide)["awards"] == "string"


def test_widen_schema_rejects_an_unknown_string_field():
    with pytest.raises(ValueError, match="nope"):
        m.widen_schema(INFERRED, string_fields=["nope"])


def test_widen_schema_does_not_mutate_its_input():
    before = json.dumps(INFERRED, sort_keys=True)
    m.widen_schema(INFERRED, string_fields=["year"])
    assert json.dumps(INFERRED, sort_keys=True) == before


# --- read_collection ---------------------------------------------------------

def test_read_collection_infers_then_rereads_with_the_widened_schema():
    spark = FakeSpark(inferred=INFERRED)
    df = m.read_collection(spark, URI, "sample_mflix", "movies", sample_size=5000)
    infer, read = spark.loads
    assert infer.fmt == read.fmt == "mongodb"
    assert infer.options["sampleSize"] == "5000"
    assert infer.explicit_schema is None
    assert types(read.explicit_schema.jsonValue())["price"] == "decimal(38,10)"
    assert df.schema is read.explicit_schema


def test_read_collection_with_an_explicit_schema_reads_once_and_uses_it_as_given():
    spark = FakeSpark()
    schema = FakeSchema({"type": "struct", "fields": [field("price", "decimal(5,2)")]})
    m.read_collection(spark, URI, "db", "coll", schema=schema)
    (read,) = spark.loads
    assert read.explicit_schema is schema


def test_read_collection_sets_uri_database_collection_and_timeout():
    spark = FakeSpark()
    m.read_collection(spark, URI, "db", "coll", schema=FakeSchema({}), server_selection_timeout_ms=7000)
    opts = spark.loads[0].options
    assert opts["connection.uri"] == URI + "?serverSelectionTimeoutMS=7000"
    assert (opts["database"], opts["collection"]) == ("db", "coll")
    assert "aggregation.pipeline" not in opts


def test_read_collection_pushes_the_watermark_pipeline_to_both_reads():
    spark = FakeSpark(inferred=INFERRED)
    until = datetime(2016, 1, 1, tzinfo=timezone.utc)
    m.read_collection(spark, URI, "db", "coll", watermark_field="released", until=until)
    for r in spark.loads:
        assert json.loads(r.options["aggregation.pipeline"]) == json.loads(
            m.build_pipeline(watermark_field="released", until=until))


# --- errors ------------------------------------------------------------------

def _raising(text):
    return FakeSpark(error=FakePy4JError(FakeJavaException(
        "org.apache.spark.SparkException: wrapper", FakeJavaException(text))))


@pytest.mark.parametrize("root, cls, hint", [
    ("java.lang.ClassNotFoundException: mongodb.DefaultSource", m.MongoError, "load_mongo_connector"),
    ("com.mongodb.MongoCommandException: Command failed with error 8000 (AtlasError): "
     "'bad auth : authentication failed'", m.MongoAuthError, "authentication failed"),
    ("com.mongodb.MongoTimeoutException: Timed out ... {javax.net.ssl.SSLException: (internal_error) "
     "Received fatal alert: internal_error}", m.MongoError, "IP access list"),
    ("com.mongodb.MongoTimeoutException: Timed out ... srvResolutionException="
     "com.mongodb.MongoConfigurationException: Failed looking up SRV record", m.MongoError, "SRV"),
    ("com.mongodb.MongoTimeoutException: Timed out while waiting for a server", m.MongoError,
     "could not reach"),
])
def test_known_failures_get_a_hint_and_the_whole_java_chain(root, cls, hint):
    with pytest.raises(cls) as exc:
        m.read_collection(_raising(root), URI, "db", "coll", schema=FakeSchema({}))
    msg = str(exc.value)
    assert hint in msg
    assert "wrapper" in msg and root.split(":")[0] in msg  # whole chain, not Py4J's "calling o78"
    assert "o78" not in msg


def test_errors_are_redacted():
    leaky = "auth failed for spikeuser:S3cret on ac-xyz-shard-00-01.abcde.mongodb.net"
    with pytest.raises(m.MongoError) as exc:
        m.read_collection(_raising(leaky), URI, "db", "coll", schema=FakeSchema({}))
    msg = str(exc.value)
    assert "S3cret" not in msg and "spikeuser" not in msg and "abcde" not in msg


def test_explain_error_passes_a_non_java_exception_through_as_text():
    err = m.explain_error(RuntimeError("plain failure"), URI)
    assert isinstance(err, m.MongoError) and "plain failure" in str(err)


def test_check_connection_samples_one_document():
    spark = FakeSpark()
    m.check_connection(spark, URI, "db", "coll")
    (r,) = spark.loads
    assert r.options["sampleSize"] == "1" and r.explicit_schema is None


def test_check_connection_raises_the_explained_error():
    with pytest.raises(m.MongoAuthError):
        m.check_connection(_raising("bad auth : authentication failed"), URI, "db", "coll")


# --- latest_watermark ----------------------------------------------------------

class _Row(dict):
    pass


class _SqlSpark:
    def __init__(self, micros):
        self.micros, self.queries = micros, []

    def sql(self, query):
        self.queries.append(query)
        spark = self

        class _Result:
            def first(self):
                return _Row(m=spark.micros)
        return _Result()


def test_latest_watermark_reads_epoch_micros_so_no_local_timezone_can_shift_it():
    # 2017-09-13T00:37:11.5Z. Collecting a TIMESTAMP instead would give a naive
    # datetime in the Python process's local zone (+5 h on a UTC+5 machine),
    # whatever spark.sql.session.timeZone says (verified 2026-09-30).
    spark = _SqlSpark(1505263031500000)
    got = m.latest_watermark(spark, "cat.sch.t", "date")
    assert got == datetime(2017, 9, 13, 0, 37, 11, 500000, tzinfo=timezone.utc)
    assert "unix_micros(max(`date`))" in spark.queries[0] and "cat.sch.t" in spark.queries[0]


def test_latest_watermark_of_an_empty_table_is_none():
    assert m.latest_watermark(_SqlSpark(None), "t", "date") is None
