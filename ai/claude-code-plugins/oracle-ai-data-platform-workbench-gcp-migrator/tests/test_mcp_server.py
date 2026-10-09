"""The MCP tools give the same results as the CLI. Skipped unless the mcp extra is installed."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HAS_MCP = importlib.util.find_spec("mcp") is not None


def run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "gcp_aidp.cli", *args],
                          capture_output=True, text=True, stdin=subprocess.DEVNULL)


@unittest.skipUnless(HAS_MCP, "install the mcp extra to enable the MCP parity tests")
class McpParityTests(unittest.TestCase):
    def test_offline_workflow_matches_cli(self):
        from gcp_aidp.mcp_server import inventory, migrate, plan, verify

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inv, pln = root / "inv.json", root / "plan.json"
            self.assertNotIn("[exit", inventory(fixture="demo", output=str(inv)))
            self.assertNotIn("[exit", plan(str(inv), output=str(pln), namespace="ns"))

            cli = run_cli("migrate", str(pln), "-o", str(root / "cli"))
            self.assertEqual(cli.returncode, 0, cli.stderr)
            self.assertNotIn("[exit", migrate(str(pln), out_dir=str(root / "mcp")))

            cli_report = json.loads((root / "cli" / "report.json").read_text())
            mcp_report = json.loads((root / "mcp" / "report.json").read_text())
            self.assertEqual(cli_report["counts"], mcp_report["counts"])
            self.assertEqual(run_cli("verify", str(root / "cli")).stdout,
                             verify(str(root / "mcp")).replace(str(root / "mcp"), str(root / "cli")))

    def test_cli_failure_is_reported_with_exit_code(self):
        from gcp_aidp.mcp_server import verify

        with tempfile.TemporaryDirectory() as tmp:
            direct = run_cli("verify", str(Path(tmp) / "missing"))
            self.assertNotEqual(direct.returncode, 0)
            self.assertTrue(verify(str(Path(tmp) / "missing")).startswith(f"[exit {direct.returncode}]"))

    def test_timeout_is_reported_cleanly(self):
        from gcp_aidp.mcp_server import _run

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=["gcp-aidp"], timeout=1)):
            self.assertIn("[exit timeout after 1s]", _run(["inventory"], timeout=1))


if __name__ == "__main__":
    unittest.main()
