"""MongoDB Atlas reader for AIDP notebooks, via the MongoDB Spark Connector.

Read-only. Needs no PyPI package: the connector and driver jars are fetched
from Maven Central at runtime, SHA-256-pinned, and loaded into the running
SparkSession. The connection URI comes from OCI Vault or an environment
variable and is never logged. Upload this single file to a workspace folder,
put that folder on ``sys.path`` and ``import mongodb_client``; the example
notebook shows the steps.

Sections: credential lookup, runtime jar loading, and the MongoDB read
(pipeline, schema widening, error explanation).
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import unquote

ENV_URI = "MONGODB_URI"


class MongoError(Exception):
    """Any failure talking to MongoDB. Messages never contain the URI's secrets."""


class MongoAuthError(MongoError):
    """MongoDB rejected the user or password."""


# --------------------------------------------------------------------------
# Credential lookup: OCI Vault, then environment, then default
# --------------------------------------------------------------------------

_MISSING = object()


class SecretNotFoundError(KeyError):
    """No value found anywhere; ``vault_error`` holds the Vault failure, if any."""

    def __init__(self, message, vault_error=None):
        super().__init__(message)
        self.vault_error = vault_error


def get_secret(name: str, default: Any = _MISSING, vault_id: Optional[str] = None) -> str:
    """Resolve ``name`` from OCI Vault, then an environment variable, then ``default``.

    Args:
        name: The secret name. Used as the Vault secret name (case-sensitive)
            and, uppercased, as the environment-variable fallback.
        default: Returned if the secret can't be resolved anywhere. Raises
            ``KeyError`` instead if left unspecified.
        vault_id: OCI Vault OCID. Defaults to the ``OCI_VAULT_ID`` environment
            variable; if neither is set, the Vault step is skipped entirely.

    Returns:
        The resolved secret value.

    Raises:
        KeyError: If no value can be resolved and ``default`` is not provided.
    """
    vault_id = vault_id or os.environ.get("OCI_VAULT_ID")
    vault_error = None
    if vault_id:
        try:
            return _get_from_vault(name, vault_id)
        except Exception as exc:
            vault_error = exc  # Vault is best-effort; fall through to the environment.

    env_value = os.environ.get(name.upper())
    if env_value is not None:
        return env_value

    if default is _MISSING:
        detail = " (Vault lookup failed: {})".format(vault_error) if vault_error else ""
        raise SecretNotFoundError(
            "secret {!r} not found in OCI Vault, environment, or default{}".format(name, detail),
            vault_error,
        )
    return default


def _get_from_vault(name: str, vault_id: str) -> str:
    """Look up ``name`` as a secret in the given OCI Vault.

    Imported lazily so unit tests never need the ``oci`` package installed
    unless they actually reach this path.
    """
    import oci

    config = oci.config.from_file()
    vaults_client = oci.vault.VaultsClient(config)
    secrets_client = oci.secrets.SecretsClient(config)

    secrets = oci.pagination.list_call_get_all_results(
        vaults_client.list_secrets,
        compartment_id=os.environ.get("OCI_COMPARTMENT_ID") or config.get("tenancy"),
        vault_id=vault_id,
    ).data
    match = next(
        (s for s in secrets
         if s.secret_name == name and getattr(s, "lifecycle_state", "ACTIVE") == "ACTIVE"),
        None,
    )
    if match is None:
        raise KeyError("secret {!r} not in vault {!r}".format(name, vault_id))

    bundle = secrets_client.get_secret_bundle(secret_id=match.id).data
    content = bundle.secret_bundle_content.content  # base64-encoded
    return base64.b64decode(content).decode("utf-8")


def credentials_from_env() -> str:
    """Return the ``MONGODB_URI`` connection string (Vault first, then env).

    Secrets are looked up in ``OCI_COMPARTMENT_ID`` if set, else the tenancy
    root. The value is stripped and never echoed in an error.
    """
    try:
        uri = get_secret(ENV_URI).strip()
    except KeyError as exc:
        vault_error = getattr(exc, "vault_error", None)
        hint = " (Vault lookup failed: {})".format(vault_error) if vault_error else ""
        raise MongoError("missing environment variable: " + ENV_URI + hint) from None
    if not uri.startswith(("mongodb://", "mongodb+srv://")):
        raise MongoError(ENV_URI + " must start with mongodb:// or mongodb+srv://")
    return uri


# --------------------------------------------------------------------------
# Runtime jar loading
# --------------------------------------------------------------------------

# Connector 10.7.0 accepts driver [5.1.1, 5.1.99); these five are its complete
# non-optional runtime set.
MAVEN_CENTRAL = "https://repo1.maven.org/maven2/"
JARS = (
    ("org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar",
     "1b0908775a41d72621944a43e36ed83df4dbaff5bf8581811f0b5e9eabeb7cbe"),
    ("org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar",
     "341880078296edd762756440e9d6b1d6d2bf4d6b91975c2638e3da611f28b1c2"),
    ("org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar",
     "ae3dbfd439d5afe9e0d6abbd61ef27d858ca2d38083bb7361f69e243aff31dec"),
    ("org/mongodb/bson/5.1.4/bson-5.1.4.jar",
     "bba556a8acd4e87545c1b9a1cb25c12ce9587ed5bf01685f236c5d92abf1e676"),
    ("org/mongodb/bson-record-codec/5.1.4/bson-record-codec-5.1.4.jar",
     "698b2b9a10fdd49a3ed99ad2b7bcc8e797639c8c4a5c974de7f6fc8654bda655"),
)
PROVIDER_CLASS = "com.mongodb.spark.sql.connector.MongoTableProvider"


class JarIntegrityError(Exception):
    """A downloaded jar's SHA-256 didn't match the pinned value."""


def _sha256_of(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_jar(url: str, target_path: str, expected_sha256: str) -> str:
    """Fetch ``url`` to ``target_path`` unless a file with the pinned SHA-256 is
    already there. A mismatching download is removed and raises
    ``JarIntegrityError``, so a jar is never loaded unless it matches its pin."""
    if os.path.exists(target_path) and _sha256_of(target_path) == expected_sha256:
        return target_path
    urllib.request.urlretrieve(url, target_path)
    actual = _sha256_of(target_path)
    if actual != expected_sha256:
        os.remove(target_path)
        raise JarIntegrityError(
            "downloaded jar's sha256 ({}) does not match the pinned value ({})"
            " - refusing to load it".format(actual, expected_sha256)
        )
    return target_path


def install_jars(spark, jar_paths, verify_class: str) -> None:
    """Make ``jar_paths`` loadable in the running session without a restart.

    The driver gets a ``URLClassLoader`` over the jars, set as the thread
    context class loader (which Spark's DataSource lookup uses); executors get
    the jars through ``addJar``. ``verify_class`` is loaded first, so a missing
    class fails before anything is distributed.
    """
    jar_paths = list(jar_paths)
    jvm = spark._jvm
    urls = spark.sparkContext._gateway.new_array(jvm.java.net.URL, len(jar_paths))
    for i, p in enumerate(jar_paths):
        urls[i] = jvm.java.io.File(p).toURI().toURL()
    thread = jvm.java.lang.Thread.currentThread()
    loader = jvm.java.net.URLClassLoader(urls, thread.getContextClassLoader())
    thread.setContextClassLoader(loader)
    loader.loadClass(verify_class)  # raises if missing
    for p in jar_paths:
        spark._jsc.addJar(p)


def load_mongo_connector(spark, jar_dir="/tmp/aidp_mongodb_jars"):
    """Download the pinned jar set (skipped when already present and matching)
    and install it into the running session. Safe to call again on a re-run."""
    os.makedirs(jar_dir, exist_ok=True)
    paths = [
        download_jar(MAVEN_CENTRAL + path, os.path.join(jar_dir, path.rsplit("/", 1)[1]), sha256)
        for path, sha256 in JARS
    ]
    install_jars(spark, paths, PROVIDER_CLASS)
    return paths


# --------------------------------------------------------------------------
# MongoDB read
# --------------------------------------------------------------------------

# Checked in order against the whole Java cause chain; first match wins.
_HINTS = (
    ("ClassNotFoundException: mongodb.DefaultSource", MongoError,
     "MongoDB Spark connector is not loaded: call load_mongo_connector(spark) first."),
    ("bad auth", MongoAuthError,
     "authentication failed: check the user and password in MONGODB_URI."),
    ("internal_error", MongoError,
     "TLS handshake aborted: on Atlas this usually means this cluster's IP is not on "
     "the project's IP access list (changes take a minute or two to apply)."),
    ("Failed looking up SRV record", MongoError,
     "SRV DNS lookup failed: check the host in MONGODB_URI and that DNS works from here."),
    ("MongoTimeoutException", MongoError,
     "could not reach MongoDB within serverSelectionTimeoutMS."),
    ("UnknownReason", MongoError,
     "a Spark task failed; with an inferred schema this is often a field whose type "
     "differs from the sampled one - pass it in string_fields=, or pass a schema."),
)
_ATLAS_HOST = re.compile(r"[\w.-]+\.mongodb\.net")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
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
    return _IPV4.sub("<ip>", _ATLAS_HOST.sub("<host>", text))


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
