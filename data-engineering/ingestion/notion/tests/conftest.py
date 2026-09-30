# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Put the sample root on sys.path so tests can import notion_connector and fakes."""

import sys
from pathlib import Path

SAMPLE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_ROOT))
sys.path.insert(0, str(SAMPLE_ROOT / "tests"))
