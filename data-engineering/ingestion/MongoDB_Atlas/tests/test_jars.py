"""Jar pinning, download and runtime install. The JVM is faked: these check
call order and arguments only."""

import hashlib
import re
import urllib.request

import pytest

import mongodb_client as m


def test_jar_pins_are_full_maven_paths_with_sha256():
    assert len(m.JARS) == 5
    for path, sha in m.JARS:
        assert path.startswith("org/mongodb/") and path.endswith(".jar")
        assert re.fullmatch(r"[0-9a-f]{64}", sha)


def _fake_download(monkeypatch, content, calls):
    def fake_urlretrieve(url, path):
        calls.append(url)
        with open(path, "wb") as f:
            f.write(content)
    monkeypatch.setattr(urllib.request, "urlretrieve", fake_urlretrieve)


def test_download_jar_keeps_a_matching_file_and_rejects_a_mismatch(tmp_path, monkeypatch):
    content, calls = b"pretend jar bytes", []
    _fake_download(monkeypatch, content, calls)
    pin = hashlib.sha256(content).hexdigest()

    target = tmp_path / "ok.jar"
    assert m.download_jar("https://repo1.maven.org/a.jar", str(target), pin) == str(target)

    bad = tmp_path / "bad.jar"
    with pytest.raises(m.JarIntegrityError):
        m.download_jar("https://repo1.maven.org/a.jar", str(bad), "0" * 64)
    assert not bad.exists()  # never left behind for a later load


def test_download_jar_rechecks_an_existing_file_and_replaces_it_on_mismatch(tmp_path, monkeypatch):
    content, calls = b"pinned jar bytes", []
    _fake_download(monkeypatch, content, calls)
    pin = hashlib.sha256(content).hexdigest()
    target = tmp_path / "connector.jar"
    target.write_bytes(b"stale or tampered bytes")

    m.download_jar("https://repo1.maven.org/c.jar", str(target), pin)
    assert target.read_bytes() == content and len(calls) == 1

    m.download_jar("https://repo1.maven.org/c.jar", str(target), pin)  # matches now
    assert len(calls) == 1


class _Loader:
    def __init__(self, urls, parent, known):
        self.urls, self.parent, self.known = urls, parent, known

    def loadClass(self, name):
        if name not in self.known:
            raise Exception("ClassNotFoundException: " + name)


class _Jvm:
    """Just enough of py4j's ``spark._jvm`` attribute chain."""

    def __init__(self, known):
        self.known, self.context = known, "original-cl"
        self.java = self.net = self.io = self.lang = self.Thread = self
        self.URL = "java.net.URL"

    def File(self, path):
        return _Url(path)

    def currentThread(self):
        return self

    def getContextClassLoader(self):
        return self.context

    def setContextClassLoader(self, loader):
        self.context = loader

    def URLClassLoader(self, urls, parent):
        return _Loader(urls, parent, self.known)


class _Url:
    def __init__(self, path):
        self.path = path

    def toURI(self):
        return self

    def toURL(self):
        return "url:" + self.path


class _Spark:
    def __init__(self, known=()):
        self._jvm = _Jvm(set(known))
        self.sparkContext = self._gateway = self._jsc = self
        self.added = []

    def new_array(self, cls, size):
        return [None] * size

    def addJar(self, path):
        self.added.append(path)


def test_install_jars_sets_the_context_loader_then_distributes():
    spark = _Spark(known={m.PROVIDER_CLASS})
    m.install_jars(spark, ["/tmp/a.jar", "/tmp/b.jar"], m.PROVIDER_CLASS)
    loader = spark._jvm.context
    assert loader.urls == ["url:/tmp/a.jar", "url:/tmp/b.jar"] and loader.parent == "original-cl"
    assert spark.added == ["/tmp/a.jar", "/tmp/b.jar"]


def test_install_jars_fails_before_distributing_when_the_class_is_missing():
    spark = _Spark()
    with pytest.raises(Exception, match="not.a.real.Class"):
        m.install_jars(spark, ["/tmp/a.jar"], "not.a.real.Class")
    assert spark.added == []


def test_load_mongo_connector_downloads_every_pinned_jar_then_installs_them(monkeypatch, tmp_path):
    downloads, installs = [], []
    monkeypatch.setattr(m, "download_jar", lambda url, path, sha: downloads.append((url, path, sha)) or path)
    monkeypatch.setattr(m, "install_jars", lambda spark, paths, cls: installs.append((spark, paths, cls)))

    paths = m.load_mongo_connector("spark", jar_dir=str(tmp_path / "jars"))

    assert [d[0] for d in downloads] == [m.MAVEN_CENTRAL + p for p, _ in m.JARS]
    assert [d[2] for d in downloads] == [sha for _, sha in m.JARS]
    assert all(d[1].startswith(str(tmp_path / "jars")) for d in downloads)
    assert installs == [("spark", paths, m.PROVIDER_CLASS)]
