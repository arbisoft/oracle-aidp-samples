"""Check a connector notebook against the oracle-aidp-samples connector template.

Usage: python3 check_sample_notebook.py <path/to/Source.ipynb> [...]
Exit 0 when every notebook matches; prints one line per problem otherwise.
Standard library only. See SKILL.md in this folder for the template itself.
"""

import json
import re
import sys
from pathlib import Path

UPL = ("Oracle AI Data Platform v1.0\n\n"
       "Copyright © 2025, Oracle and/or its affiliates.\n\n"
       "Licensed under the Universal Permissive License v 1.0 as shown at "
       "https://oss.oracle.com/licenses/upl/")
CATEGORIES = ("Read_Only_Ingestion_Connectors", "Read_Write_External_Ecosystem_Connectors",
              "Read_Write_Oracle_Ecosystem_Connectors")
OPTIONS_HEADER = "| Parameter name | Valid values | Mandatory | Description |"
# A secret-looking option or assignment whose value is not a <PLACEHOLDER>.
SECRET_VALUE = re.compile(
    r"""(?ix)(?:\.option\(\s*["'][\w.]*(?:password|secret|token|key\.content|pass\.phrase|uri)["']\s*,\s*
         |(?:password|secret|token|api_key)\s*=\s*)["'](?!<)[^"']+["']""")
CREDENTIAL_URI = re.compile(r"\w+(?:\+\w+)?://[^<\s\"'/@]+:[^<\s\"'@]+@")


def _text(cell):
    return "".join(cell["source"]).strip()


CODE_WITH_PARENS = re.compile(r"`[^`\n]*\([^`\n]*`")
PARENS_WITH_CODE = re.compile(r"\([^()\n]*`[^`\n]*`[^()\n]*\)")


def markdown_warnings(cells):
    """Parentheses inside or around inline code: the AIDP notebook renderer
    turned these into broken links with URL-encoded text (seen 2026-10-01)."""
    found = []
    for i, cell in enumerate(cells, start=1):
        if cell["cell_type"] != "markdown":
            continue
        prose = re.sub(r"```.*?```", "", "".join(cell["source"]), flags=re.S)
        for match in CODE_WITH_PARENS.findall(prose) + PARENS_WITH_CODE.findall(prose):
            found.append("cell {}: parentheses next to inline code may render as a broken link: {}"
                         .format(i, match[:60]))
    return found


def check(path):
    path = Path(path)
    problems = []
    if not re.fullmatch(r"[A-Z][A-Za-z0-9]*(?:_[A-Za-z0-9]+)*\.ipynb", path.name):
        problems.append("file name must be Title_Snake_Case, e.g. Jira_Cloud.ipynb")
    siblings = sorted(p.name for p in path.parent.iterdir()
                      if p != path and not p.name.startswith("."))
    pattern_sample = path.parent.name not in CATEGORIES
    if not pattern_sample:  # Oracle's built-in connector layout
        extra = [n for n in siblings if not n.endswith(".ipynb")]
        if extra:
            problems.append("built-in connector folders hold only notebooks; found " + ", ".join(extra))
    elif path.parent.name == path.stem:  # pattern-sample layout: <Source>/<Source>.ipynb
        extra = [n for n in siblings if n not in ("README.md", "requirements.txt")]
        if extra:
            problems.append("sample folder may add only README.md and requirements.txt; found "
                            + ", ".join(extra))
    else:
        problems.append("notebook must be <Source>/<Source>.ipynb (or in a built-in connector folder)")

    nb = json.loads(path.read_text(encoding="utf-8"))
    cells = nb.get("cells", [])
    if (nb.get("nbformat"), nb.get("nbformat_minor")) != (4, 5):
        problems.append("nbformat must be 4.5")
    if nb.get("metadata", {}).get("kernelspec", {}).get("name") != "python3":
        problems.append("kernelspec name must be python3")
    if len(cells) < 4:
        return problems + ["too few cells for the template"]

    if cells[0]["cell_type"] != "code" or _text(cells[0]) != UPL:
        problems.append("cell 1 must be a code cell holding exactly the UPL header (© 2025)")
    title = _text(cells[1]).splitlines()
    if cells[1]["cell_type"] != "markdown" or not re.fullmatch(r"# .+ Connector Samples", title[0] if title else ""):
        problems.append('cell 2 must be markdown starting "# <Source> Connector Samples"')

    if pattern_sample and not any(c["cell_type"] == "markdown" and _text(c).startswith("## Prerequisites")
                                  for c in cells):
        problems.append('a pattern sample needs a "## Prerequisites" markdown section')

    last = _text(cells[-1])
    if cells[-1]["cell_type"] != "markdown" or not last.startswith("## Connector Options") \
            or OPTIONS_HEADER not in last:
        problems.append('last cell must be "## Connector Options" with the four-column options table')

    for i, cell in enumerate(cells, start=1):
        if cell["cell_type"] == "code" and (cell.get("outputs") or cell.get("execution_count") is not None):
            problems.append("cell {}: clear outputs and execution_count".format(i))
    for i, cell in enumerate(cells[2:], start=3):
        text = _text(cell)
        if cell["cell_type"] == "markdown" and not text.startswith(("## ", "### ")):
            problems.append("cell {}: markdown sections must start with a '## ' or '### ' heading".format(i))
        if cell["cell_type"] == "code":
            if SECRET_VALUE.search(text) or CREDENTIAL_URI.search(text):
                problems.append("cell {}: a credential is not a <PLACEHOLDER>".format(i))
        if cell["cell_type"] == "raw":
            problems.append("cell {}: raw cells are not used by the template".format(i))
    return problems


def main(paths):
    failed = False
    for p in paths:
        problems = check(p)
        failed |= bool(problems)
        print("{}: {}".format(p, "OK" if not problems else "{} problem(s)".format(len(problems))))
        for problem in problems:
            print("  - " + problem)
        for warning in markdown_warnings(json.loads(Path(p).read_text(encoding="utf-8")).get("cells", [])):
            print("  warning: " + warning)
    return 1 if failed or not paths else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
