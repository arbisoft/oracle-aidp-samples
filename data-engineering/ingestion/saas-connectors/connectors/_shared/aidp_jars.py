"""Load JDBC drivers and Spark DataSource JARs into a running AIDP Spark session
at runtime, without restarting the notebook's SparkSession.

Adapted from Oracle's own `oracle-ai-data-platform-workbench-spark-connectors`
plugin (`scripts/oracle_ai_data_platform_connectors/jdbc/runtime_load.py`, MIT
licence, Copyright (c) 2026 Ahmed Awan) — rewritten for this repo's naming and
test style, not copied verbatim. Not yet used by a shipped connector in this
repo (Jira Cloud needs no jar); kept ready for the next connector that does
(e.g. MongoDB via the mongo-spark-connector, or any JDBC-only source).

Spark's normal mechanism for adding a driver/DataSource JAR is to set
``spark.jars`` at session creation time, which means restarting the kernel —
awkward in AIDP, where the notebook itself owns the SparkSession lifecycle.
This module avoids that:

* The **driver** JVM gets a new ``URLClassLoader`` rooted at the existing
  thread context class loader, with the new JARs added, then set as the
  thread context class loader so both ``ServiceLoader`` (Spark's DataSource
  lookup) and ``Class.forName`` (JDBC) resolve the new classes.
* The **executors** receive the JARs via ``SparkContext.addJar()`` — without
  this, task code referencing the new classes fails to deserialize on the
  executor side (``ClassNotFoundException`` inside
  ``ObjectInputStream.resolveClass``).

Maven Central is reachable from AIDP clusters; PyPI is not — jars come from
Maven Central, never from a PyPI package.
"""

from __future__ import annotations

import hashlib
import os
import urllib.request
from typing import Iterable, Optional


class JarIntegrityError(Exception):
    """A downloaded jar's SHA-256 didn't match the pinned value."""


def _install_urlclassloader(spark, jar_paths: Iterable[str]):
    """Build a URLClassLoader rooted at the current thread's class loader,
    covering ``jar_paths``, set it as the thread context class loader, and
    return it."""
    jar_paths = list(jar_paths)
    jvm = spark._jvm
    gw = spark.sparkContext._gateway

    urls = gw.new_array(jvm.java.net.URL, len(jar_paths))
    for i, p in enumerate(jar_paths):
        urls[i] = jvm.java.io.File(p).toURI().toURL()

    parent = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    loader = jvm.java.net.URLClassLoader(urls, parent)
    jvm.java.lang.Thread.currentThread().setContextClassLoader(loader)
    return loader


def _distribute_to_executors(spark, jar_paths: Iterable[str]) -> None:
    """Push JARs to executors via the SparkContext's file server."""
    for p in jar_paths:
        spark._jsc.addJar(p)


def add_jdbc_jar_at_runtime(
    spark,
    *,
    jar_path: str,
    driver_class: str,
    distribute_to_executors: bool = True,
) -> None:
    """Make a JDBC driver class loadable in the current Spark session.

    Args:
        spark: The active SparkSession.
        jar_path: Filesystem path to the JDBC driver JAR, visible to the
            driver JVM (typically ``/tmp/...`` after download, or a
            ``/Volumes/...`` path).
        driver_class: The JDBC driver class name, e.g. ``org.postgresql.Driver``.
        distribute_to_executors: If True (default), also distribute the JAR to
            executors — required whenever the read partitions across more
            than one executor. Pass False only for driver-local cases.
    """
    jvm = spark._jvm
    loader = _install_urlclassloader(spark, [jar_path])

    cls = loader.loadClass(driver_class)
    driver = cls.newInstance()
    jvm.java.sql.DriverManager.registerDriver(driver)

    if distribute_to_executors:
        _distribute_to_executors(spark, [jar_path])


def add_spark_connector_at_runtime(
    spark,
    *,
    jar_paths: Iterable[str],
    verify_classes: Optional[Iterable[str]] = None,
    register_jdbc_driver_class: Optional[str] = None,
) -> None:
    """Install a Spark DataSource (and optionally a JDBC driver) at runtime.

    Use for a connector that registers a Spark format via
    ``META-INF/services/org.apache.spark.sql.sources.DataSourceRegister``
    (e.g. the MongoDB Spark Connector).

    Args:
        spark: The active SparkSession.
        jar_paths: Every JAR the connector needs — the connector JAR plus any
            driver JAR it depends on.
        verify_classes: Class names to load through the new class loader as a
            sanity check before returning; raises if any is missing.
        register_jdbc_driver_class: If the connector also needs a JDBC driver
            registered with ``DriverManager`` for some operations, its class
            name.
    """
    jvm = spark._jvm
    loader = _install_urlclassloader(spark, jar_paths)

    if verify_classes:
        for class_name in verify_classes:
            loader.loadClass(class_name)  # raises if missing

    if register_jdbc_driver_class:
        cls = loader.loadClass(register_jdbc_driver_class)
        driver = cls.newInstance()
        jvm.java.sql.DriverManager.registerDriver(driver)

    _distribute_to_executors(spark, jar_paths)


def download_jar(
    *,
    maven_url: str,
    target_path: str,
    overwrite: bool = False,
    expected_sha256: Optional[str] = None,
) -> str:
    """Fetch a JAR from Maven Central to a JVM-readable local path.

    Args:
        maven_url: Full URL to the JAR on Maven Central.
        target_path: Where to write it — a ``/tmp/...`` path is recommended.
        overwrite: If False (default) and the file already exists, skip the
            download (its hash is not re-checked in that case).
        expected_sha256: If given, the downloaded file's SHA-256 must match
            (case-insensitive hex digest) or the file is removed and
            ``JarIntegrityError`` is raised — a connector never loads a jar
            whose contents don't match what was pinned when the connector was
            written.

    Returns:
        ``target_path``, for chaining.

    Raises:
        JarIntegrityError: If ``expected_sha256`` is given and doesn't match.
    """
    if overwrite or not os.path.exists(target_path):
        urllib.request.urlretrieve(maven_url, target_path)
        if expected_sha256:
            actual = _sha256_of(target_path)
            if actual.lower() != expected_sha256.lower():
                os.remove(target_path)
                raise JarIntegrityError(
                    "downloaded jar's sha256 ({}) does not match the pinned"
                    " value ({}) — refusing to load it".format(actual, expected_sha256)
                )
    return target_path


def _sha256_of(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
