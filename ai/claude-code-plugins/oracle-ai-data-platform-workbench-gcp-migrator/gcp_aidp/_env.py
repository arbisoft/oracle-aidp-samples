"""Tiny .env loader + required-var check. Stdlib only."""
from __future__ import annotations

import os
from pathlib import Path


# Only these keys are taken from a .env: `publish`
# and `run` start the `aidp` CLI, and a stray PATH= must not choose which one.
ALLOWED_PREFIXES = ("GCP_", "GOOGLE_APPLICATION_CREDENTIALS", "AIDP_", "OCI_")
# The plugin folder's .env, read after the working directory's, so the CLI
# finds it when run from a migration folder elsewhere.
PLUGIN_ENV = Path(__file__).resolve().parents[1] / ".env"


def load_dotenv(path: str | Path | None = None) -> None:
    """Load allowed KEY=VALUE pairs from .env into os.environ if not already set.

    With no path: the working directory's .env, then the plugin's. The shell
    wins over both, and the working directory over the plugin.
    """
    if path is None:
        load_dotenv(".env")
        load_dotenv(PLUGIN_ENV)
        return
    p = Path(path)
    if not p.exists():
        return
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # `KEY=` (as .env.example ships it) means unset: exported as "", the OCI SDK would
        # look for a profile named ''.
        if k.startswith(ALLOWED_PREFIXES) and v and k not in os.environ:
            os.environ[k] = v


def require(*names: str) -> dict[str, str]:
    """Return dict of required env vars, raising with a clean message if any missing."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError(
            "missing required env var(s): " + ", ".join(missing)
            + "  (set in .env or shell; see .env.example)"
        )
    return {n: os.environ[n] for n in names}
