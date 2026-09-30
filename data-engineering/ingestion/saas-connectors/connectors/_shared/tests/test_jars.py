"""Structural tests using fake Spark/JVM objects — no real JVM is available
offline, so these verify call sequencing and argument plumbing only."""

import pytest

import aidp_jars as j


class FakeFile:
    def __init__(self, path):
        self.path = path

    def toURI(self):
        return self

    def toURL(self):
        return "url:" + self.path


class FakeClass:
    def __init__(self, name, registry):
        self.name = name
        self._registry = registry
        if name not in registry["known_classes"]:
            raise Exception("ClassNotFoundException: " + name)

    def newInstance(self):
        return "instance-of-" + self.name


class FakeClassLoader:
    def __init__(self, urls, parent, registry):
        self.urls = urls
        self.parent = parent
        self._registry = registry

    def loadClass(self, name):
        return FakeClass(name, self._registry)


class FakeThread:
    def __init__(self):
        self._context_cl = "original-cl"

    def currentThread(self):
        return self

    def getContextClassLoader(self):
        return self._context_cl

    def setContextClassLoader(self, loader):
        self._context_cl = loader


class FakeGateway:
    def new_array(self, cls, size):
        return [None] * size


class FakeDriverManager:
    def __init__(self):
        self.registered = []

    def registerDriver(self, driver):
        self.registered.append(driver)


class FakeJVM:
    def __init__(self, registry):
        self._registry = registry
        self.java = self

    # java.net / java.io / java.lang nesting, flattened for the fake
    class net:
        URL = "java.net.URL"

        class URLClassLoader:
            pass

    class io:
        class File:
            pass

    class lang:
        class Thread:
            pass

    class sql:
        pass

    def __getattr__(self, name):
        raise AttributeError(name)


class FakeSpark:
    def __init__(self, known_classes=()):
        registry = {"known_classes": set(known_classes)}
        self._registry = registry
        self._jvm = _FakeJvmRoot(registry)
        self.sparkContext = self
        self._gateway = FakeGateway()
        self._jsc = self
        self.added_jars = []

    def addJar(self, path):
        self.added_jars.append(path)


class _FakeJvmRoot:
    """Mimics py4j's ``spark._jvm`` attribute-chain access (``jvm.java.io.File(...)`` etc.)."""

    def __init__(self, registry):
        self._registry = registry
        self._thread = FakeThread()
        self._driver_manager = FakeDriverManager()

    @property
    def java(self):
        return self

    @property
    def io(self):
        return self

    @property
    def lang(self):
        return self

    @property
    def net(self):
        return self

    @property
    def sql(self):
        return self

    URL = "java.net.URL"

    def File(self, path):
        return FakeFile(path)

    @property
    def Thread(self):
        return self._thread

    def URLClassLoader(self, urls, parent):
        return FakeClassLoader(urls, parent, self._registry)

    @property
    def DriverManager(self):
        return self._driver_manager


def test_add_jdbc_jar_at_runtime_registers_driver_and_distributes_to_executors():
    spark = FakeSpark(known_classes={"org.sqlite.JDBC"})
    j.add_jdbc_jar_at_runtime(spark, jar_path="/tmp/sqlite.jar", driver_class="org.sqlite.JDBC")
    assert spark._jvm._driver_manager.registered == ["instance-of-org.sqlite.JDBC"]
    assert spark.added_jars == ["/tmp/sqlite.jar"]


def test_add_jdbc_jar_at_runtime_can_skip_executor_distribution():
    spark = FakeSpark(known_classes={"org.sqlite.JDBC"})
    j.add_jdbc_jar_at_runtime(
        spark, jar_path="/tmp/sqlite.jar", driver_class="org.sqlite.JDBC",
        distribute_to_executors=False,
    )
    assert spark.added_jars == []


def test_add_spark_connector_at_runtime_verifies_classes_before_distributing():
    spark = FakeSpark(known_classes={"net.snowflake.spark.snowflake.DefaultSource"})
    j.add_spark_connector_at_runtime(
        spark,
        jar_paths=["/tmp/a.jar", "/tmp/b.jar"],
        verify_classes=["net.snowflake.spark.snowflake.DefaultSource"],
    )
    assert spark.added_jars == ["/tmp/a.jar", "/tmp/b.jar"]


def test_add_spark_connector_at_runtime_raises_when_a_verify_class_is_missing():
    spark = FakeSpark(known_classes=set())
    try:
        j.add_spark_connector_at_runtime(
            spark, jar_paths=["/tmp/a.jar"], verify_classes=["not.a.real.Class"],
        )
        assert False, "expected an exception for the missing class"
    except Exception as exc:
        assert "not.a.real.Class" in str(exc)
    # A missing verify class must fail before any executor distribution happens.
    assert spark.added_jars == []


def test_add_spark_connector_at_runtime_registers_optional_jdbc_driver():
    spark = FakeSpark(known_classes={"net.snowflake.client.jdbc.SnowflakeDriver"})
    j.add_spark_connector_at_runtime(
        spark,
        jar_paths=["/tmp/a.jar"],
        register_jdbc_driver_class="net.snowflake.client.jdbc.SnowflakeDriver",
    )
    assert spark._jvm._driver_manager.registered == ["instance-of-net.snowflake.client.jdbc.SnowflakeDriver"]


def test_download_jar_verifies_sha256_when_given_and_raises_on_mismatch(tmp_path, monkeypatch):
    import hashlib
    import urllib.request

    content = b"pretend jar bytes"
    real_sha256 = hashlib.sha256(content).hexdigest()
    target = tmp_path / "driver.jar"

    def fake_urlretrieve(url, path):
        with open(path, "wb") as f:
            f.write(content)

    monkeypatch.setattr(urllib.request, "urlretrieve", fake_urlretrieve)

    # Matching hash: succeeds and returns the path.
    result = j.download_jar(
        maven_url="https://repo1.maven.org/fake.jar",
        target_path=str(target),
        expected_sha256=real_sha256,
    )
    assert result == str(target)

    # Mismatched hash: raises and does not leave a jar callers might load.
    target2 = tmp_path / "driver2.jar"
    with pytest.raises(j.JarIntegrityError):
        j.download_jar(
            maven_url="https://repo1.maven.org/fake.jar",
            target_path=str(target2),
            expected_sha256="0" * 64,
        )
    assert not target2.exists()
