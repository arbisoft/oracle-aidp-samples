"""MongoDB Atlas reader for AIDP notebooks, via the MongoDB Spark Connector.

Read-only. The MongoDB Spark Connector and driver jars are installed on the
cluster as libraries (see README.md). The connection URI comes from the AIDP Credential Store (read in the notebook with
``aidputils.secrets.get``) or, for local runs, an environment variable, and is
never logged. Upload this single file to a workspace folder, put that folder
on ``sys.path`` and ``import mongodb_client``; the example notebook shows the
steps.

Sections: connection URI, and the MongoDB read (SRV resolution, pipeline,
schema widening, error explanation).
"""

from __future__ import annotations

import copy
import json
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

ENV_URI = "MONGODB_URI"


class MongoError(Exception):
    """Any failure talking to MongoDB. Messages never contain the URI's secrets."""


class MongoAuthError(MongoError):
    """MongoDB rejected the user or password."""


# --------------------------------------------------------------------------
# Connection URI
# --------------------------------------------------------------------------

def validate_uri(uri) -> str:
    """Return ``uri`` stripped, or raise MongoError without echoing it."""
    uri = str(uri or "").strip()
    if not uri.startswith(("mongodb://", "mongodb+srv://")):
        raise MongoError("the connection URI must start with mongodb:// or mongodb+srv://")
    return uri


def credentials_from_env() -> str:
    """Return ``MONGODB_URI`` from the environment, for runs outside AIDP.
    On AIDP, read the URI from the Credential Store instead (see the notebook)."""
    if not os.environ.get(ENV_URI, "").strip():
        raise MongoError("missing environment variable: " + ENV_URI)
    return validate_uri(os.environ[ENV_URI])


# --------------------------------------------------------------------------
# MongoDB read
# --------------------------------------------------------------------------

# Checked in order against the whole Java cause chain; first match wins.
_HINTS = (
    ("ClassNotFoundException: mongodb.DefaultSource", MongoError,
     "MongoDB Spark connector is not installed: install the five jars listed in README.md "
     "as cluster libraries and restart the cluster."),
    ("bad auth", MongoAuthError,
     "authentication failed: check the user and password in MONGODB_URI."),
    ("internal_error", MongoError,
     "TLS handshake aborted: on Atlas this usually means this cluster's IP is not on "
     "the project's IP access list (changes take a minute or two to apply)."),
    ("Failed looking up TXT record", MongoError,
     "DNS TXT lookup failed. On AIDP, executors cannot resolve mongodb+srv:// URIs: pass "
     "the URI through resolve_srv(spark, uri) first."),
    ("Failed looking up SRV record", MongoError,
     "SRV DNS lookup failed: check the host in MONGODB_URI and that DNS works from here."),
    ("MongoTimeoutException", MongoError,
     "could not reach MongoDB within serverSelectionTimeoutMS."),
    ("UnknownReason", MongoError,
     "a Spark task failed and Spark could not report why. If even df.limit(1).collect() "
     "fails, the executors cannot use the connector: install the jars as cluster libraries "
     "and restart. Otherwise it is often a field whose type differs from the sampled one - "
     "pass it in string_fields=, or pass a schema."),
)
_ATLAS_HOST = re.compile(r"[\w.-]+\.mongodb\.net")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_OCI_PATH = re.compile(r"oci://[^\s'\"]+")
_DECIMAL = re.compile(r"decimal\((\d+),\s*(\d+)\)")


def redact_uri(text: str, uri: str) -> str:
    """Remove the URI's user, password (raw and decoded) and hosts from ``text``,
    plus any Atlas host or IPv4 address - Atlas errors name shard hosts that
    never appear in an SRV URI."""
    authority = uri.split("://", 1)[-1].split("/", 1)[0].split("?", 1)[0]
    userinfo, _, hosts = authority.rpartition("@")
    user, _, password = userinfo.partition(":")
    secrets = {user, password, unquote(user), unquote(password)}
    secrets.update(h.rsplit(":", 1)[0] for h in hosts.split(","))
    # Longest first, so a secret that contains another is replaced whole.
    for secret in sorted(secrets, key=len, reverse=True):
        if secret:
            text = text.replace(secret, "***")
    text = _OCI_PATH.sub("<oci-path>", text)  # storage paths name the tenancy's bucket
    return _IPV4.sub("<ip>", _ATLAS_HOST.sub("<host>", text))


_TXT_OPTIONS = ("authSource", "replicaSet", "loadBalanced")  # the only ones the SRV spec allows


def build_standard_uri(uri: str, srv_records, txt_records) -> str:
    """Turn a ``mongodb+srv://`` URI plus its DNS records into the equivalent
    ``mongodb://`` URI: hosts from the SRV records, options from the TXT
    record, ``tls=true`` unless the URI sets TLS itself. Options in the URI
    win over TXT options. ``srvMaxHosts`` and ``srvServiceName`` are not
    supported.

    ``srv_records`` are ``"priority weight port target."`` strings. Every
    target must share the SRV host's parent domain, as the driver itself
    requires, so a spoofed record cannot redirect the credentials elsewhere.
    """
    rest = uri[len("mongodb+srv://"):]
    rest, _, query = rest.partition("?")
    authority, _, path = rest.partition("/")
    userinfo, _, host = authority.rpartition("@")
    parent = host.split(".", 1)[-1].lower()
    hosts = []
    for record in srv_records:
        _, _, port, target = record.split()
        target = target.rstrip(".")
        if not target.lower().endswith("." + parent):
            raise MongoError("SRV record target is not in the SRV host's domain")
        hosts.append("{}:{}".format(target, port))
    if not hosts:
        raise MongoError("SRV lookup returned no hosts")
    options = {}
    for record in txt_records:
        for pair in record.strip('"').split("&"):
            key, _, value = pair.partition("=")
            if key in _TXT_OPTIONS:
                options[key] = value
    for pair in filter(None, query.split("&")):
        key, _, value = pair.partition("=")
        options[key] = value
    if not any(k.lower() in ("tls", "ssl") for k in options):
        options["tls"] = "true"
    return "mongodb://{}{}/{}?{}".format(
        userinfo + "@" if userinfo else "", ",".join(hosts), path,
        "&".join("{}={}".format(k, v) for k, v in options.items()))


def _dns_records(spark, name: str, kind: str):
    """``kind`` records for ``name``, looked up by the driver JVM's own DNS
    client (the one the MongoDB driver uses)."""
    jvm = spark._jvm
    env = jvm.java.util.Hashtable()
    env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory")
    kinds = spark.sparkContext._gateway.new_array(jvm.java.lang.String, 1)
    kinds[0] = kind
    attr = jvm.javax.naming.directory.InitialDirContext(env).getAttributes(name, kinds).get(kind)
    return [str(attr.get(i)) for i in range(attr.size())] if attr is not None else []


def resolve_srv(spark, uri: str) -> str:
    """Resolve a ``mongodb+srv://`` URI on the driver into a standard
    ``mongodb://`` URI; any other URI is returned unchanged.

    On AIDP the driver can look up SRV and TXT records but executors cannot:
    reads failed on every executor with ``Failed looking up TXT record``
    (verified 2026-10-01). Resolving on each run keeps up with Atlas host
    changes, which a hard-coded host list would not.
    """
    if not uri.startswith("mongodb+srv://"):
        return uri
    host = uri[len("mongodb+srv://"):].split("/", 1)[0].split("?", 1)[0].rpartition("@")[2]
    try:
        srv = _dns_records(spark, "_mongodb._tcp." + host, "SRV")
        txt = _dns_records(spark, host, "TXT")
    except Exception as exc:
        raise explain_error(exc, uri) from None
    return build_standard_uri(uri, srv, txt)


def with_timeout(uri: str, timeout_ms: int) -> str:
    """Append ``serverSelectionTimeoutMS`` unless the URI already sets it.
    The driver default is 30 s, which is how long an Atlas IP-access-list miss
    hangs before failing."""
    if "serverselectiontimeoutms=" in uri.lower():
        return uri
    if "?" in uri:
        return "{}&serverSelectionTimeoutMS={}".format(uri, timeout_ms)
    if "/" not in uri.split("://", 1)[-1]:
        uri += "/"
    return "{}?serverSelectionTimeoutMS={}".format(uri, timeout_ms)


def _date(value) -> dict:
    if value.tzinfo is None:
        raise ValueError("since/until must be timezone-aware datetimes")
    v = value.astimezone(timezone.utc)
    return {"$date": v.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (v.microsecond // 1000)}


def build_pipeline(*, watermark_field=None, since=None, until=None, overlap_seconds=0):
    """Return the aggregation pipeline JSON for a watermark window, or None.

    ``since - overlap_seconds <= watermark_field <= until``, pushed down to
    MongoDB as a ``$match``. The field must hold BSON Dates: MongoDB never
    matches a Date bound against a string.
    """
    if watermark_field is None:
        if since is not None or until is not None:
            raise ValueError("since/until need a watermark_field")
        return None
    bounds = {}
    if since is not None:
        bounds["$gte"] = _date(since - timedelta(seconds=overlap_seconds))
    if until is not None:
        bounds["$lte"] = _date(until)
    if not bounds:
        return None
    return json.dumps([{"$match": {watermark_field: bounds}}])


def latest_watermark(spark, table, watermark_field):
    """``max(watermark_field)`` of ``table`` as an aware UTC datetime, or None.

    Read as epoch microseconds: collecting a TIMESTAMP gives a naive datetime
    in the Python process's local timezone regardless of
    ``spark.sql.session.timeZone``, which would shift the watermark.
    """
    micros = spark.sql("SELECT unix_micros(max(`{}`)) AS m FROM {}".format(
        watermark_field, table)).first()["m"]
    if micros is None:
        return None
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)


def _widen_type(t):
    if isinstance(t, str):
        if t == "integer":
            return "long"
        match = _DECIMAL.fullmatch(t)
        return "decimal(38,{})".format(max(int(match.group(2)), 10)) if match else t
    kind = t.get("type")
    if kind == "struct":
        return {**t, "fields": [{**f, "type": _widen_type(f["type"]), "nullable": True}
                                for f in t["fields"]]}
    if kind == "array":
        return {**t, "elementType": _widen_type(t["elementType"]), "containsNull": True}
    if kind == "map":
        return {**t, "valueType": _widen_type(t["valueType"]), "valueContainsNull": True}
    return t


def widen_schema(schema_json: dict, string_fields=()) -> dict:
    """Widen an inferred schema (Spark's ``schema.jsonValue()`` form) so values
    the sample didn't see still fit.

    Decimals become ``decimal(38, max(scale, 10))`` and ints become longs,
    everywhere, all nullable: a sampled ``decimal(5,2)`` otherwise silently
    nulls wider values. A field holding *different types* (int in some
    documents, string in others) can't be fixed by widening - name it in
    ``string_fields`` to read it as text.
    """
    wide = _widen_type(copy.deepcopy(schema_json))
    names = {f["name"] for f in wide.get("fields", [])}
    unknown = set(string_fields) - names
    if unknown:
        raise ValueError("string_fields not in the inferred schema: " + ", ".join(sorted(unknown)))
    for f in wide.get("fields", []):
        if f["name"] in string_fields:
            f["type"] = "string"
    return wide


def explain_error(exc: BaseException, uri: str) -> MongoError:
    """Turn a Spark/Py4J failure into a MongoError carrying the whole Java cause
    chain (Py4J's own message is only "An error occurred while calling oNN"),
    a hint for known failures, and nothing from the URI.

    Use it around a notebook action too (a write, ``count()``): with a given
    schema, the connection is only made there, not at ``load()``.
    """
    java, chain = getattr(exc, "java_exception", None), []
    while java is not None:
        chain.append(java.toString())
        java = java.getCause()
    text = " <- caused by: ".join(chain) if chain else str(exc)
    cls, hint = MongoError, ""
    for pattern, error_cls, message in _HINTS:
        if pattern in text:
            cls, hint = error_cls, message + " "
            break
    return cls(redact_uri(hint + text[:2000], uri))


def _reader(spark, uri, database, collection, timeout_ms, pipeline=None, sample_size=None):
    reader = (spark.read.format("mongodb")
              .option("connection.uri", with_timeout(uri, timeout_ms))
              .option("database", database)
              .option("collection", collection))
    if pipeline:
        reader = reader.option("aggregation.pipeline", pipeline)
    if sample_size:
        reader = reader.option("sampleSize", str(sample_size))
    return reader


def check_connection(spark, uri, database, collection, *, server_selection_timeout_ms=10000):
    """Cheapest real round trip: infer a schema from one sampled document.
    Proves network, auth and read access before the real read."""
    try:
        _reader(spark, uri, database, collection, server_selection_timeout_ms, sample_size=1).load()
    except Exception as exc:
        raise explain_error(exc, uri) from None


def read_collection(
    spark, uri, database, collection, *,
    schema=None, string_fields=(), sample_size=None,
    watermark_field=None, since=None, until=None, overlap_seconds=300,
    server_selection_timeout_ms=10000,
):
    """Read one collection into a DataFrame.

    Without ``schema``, the schema is inferred from a sample (``sample_size``
    documents, connector default 1000), widened by ``widen_schema``, and the
    collection is read with that. With ``schema``, it's used exactly as given.
    ``watermark_field``/``since``/``until`` narrow the read server-side (see
    ``build_pipeline``); de-duplicate re-read rows on ``_id``.
    """
    pipeline = build_pipeline(watermark_field=watermark_field, since=since, until=until,
                              overlap_seconds=overlap_seconds)
    try:
        if schema is None:
            inferred = _reader(spark, uri, database, collection, server_selection_timeout_ms,
                               pipeline, sample_size).load().schema
            schema = type(inferred).fromJson(widen_schema(inferred.jsonValue(), string_fields))
        return (_reader(spark, uri, database, collection, server_selection_timeout_ms, pipeline)
                .schema(schema).load())
    except Exception as exc:
        raise explain_error(exc, uri) from None
