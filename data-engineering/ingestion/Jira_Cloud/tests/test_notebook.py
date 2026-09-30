import json
import re
from pathlib import Path

NOTEBOOKS = sorted(Path(__file__).resolve().parents[1].glob("*.ipynb"))


def test_at_least_one_example_notebook_exists():
    assert NOTEBOOKS


def test_notebooks_have_no_outputs_real_sites_or_inline_tokens():
    for path in NOTEBOOKS:
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4
        for cell in nb["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == [], path.name
                assert cell["execution_count"] is None, path.name
            text = "".join(cell["source"])
            for site in re.findall(r"[\w-]+\.atlassian\.net", text):
                assert site == "<site>.atlassian.net", (path.name, site)
            assert not re.search(r"api_token\s*=\s*['\"][^'\"<]", text, re.I), path.name


def test_watermark_is_read_as_a_utc_string_not_a_driver_local_datetime():
    for path in NOTEBOOKS:
        code = "".join(
            "".join(c["source"])
            for c in json.loads(path.read_text())["cells"]
            if c["cell_type"] == "code"
        )
        assert "parse_watermark" in code, path.name
        assert "date_format(max(updated)" in code, path.name
        assert "max(updated) AS m" not in code, path.name
