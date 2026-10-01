import json
import re
from pathlib import Path

NOTEBOOKS = sorted(Path(__file__).resolve().parents[1].glob("*.ipynb"))


def test_at_least_one_example_notebook_exists():
    assert NOTEBOOKS


def test_notebooks_have_no_outputs_real_hosts_or_inline_uris():
    for path in NOTEBOOKS:
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == [], path.name
                assert cell["execution_count"] is None, path.name
            text = "".join(cell["source"])
            for host in re.findall(r"[\w.-]+\.mongodb\.net", text):
                assert host == "<cluster>.mongodb.net", (path.name, host)
            # A URI with credentials typed into a cell, rather than read from the environment.
            assert not re.search(r"mongodb(\+srv)?://[^<\s\"']+:[^<\s\"']+@", text), path.name


def test_notebooks_read_the_uri_from_the_credential_store_and_never_load_jars_at_runtime():
    for path in NOTEBOOKS:
        code = "".join(
            "".join(c["source"])
            for c in json.loads(path.read_text())["cells"]
            if c["cell_type"] == "code"
        )
        assert "aidputils.secrets.get(" in code, path.name
        assert "resolve_srv(" in code, path.name  # executors cannot resolve mongodb+srv://
        assert "addJar" not in code and "load_mongo_connector" not in code, path.name
