# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import ast
import json
from pathlib import Path

from notion_connector.config import load_config

SAMPLE = Path(__file__).resolve().parent.parent
REPO = SAMPLE.parents[2]


def test_sample_config_is_valid_and_defaults_to_cdc():
    cfg = load_config(str(SAMPLE / "notion_ingest.sample.yaml"))
    assert cfg.sync.mode == "cdc"
    assert set(cfg.sync.objects) == {"pages", "data_sources", "blocks", "users"}
    assert cfg.notion.api_version == "2026-03-11"


def test_notebook_is_valid_and_clean():
    notebook = json.loads((SAMPLE / "notion_ingest.ipynb").read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    first = "".join(notebook["cells"][0]["source"])
    assert "Universal Permissive License" in first
    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
    assert code_cells
    for cell in code_cells:
        assert cell["outputs"] == [] and cell["execution_count"] is None
        ast.parse("".join(cell["source"]))
    text = json.dumps(notebook)
    assert "secret_" not in text and "ntn_" not in text  # no token committed


def test_root_readme_links_to_the_sample():
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    assert "(data-engineering/ingestion/notion/README.md)" in readme
    assert (SAMPLE / "README.md").exists()
