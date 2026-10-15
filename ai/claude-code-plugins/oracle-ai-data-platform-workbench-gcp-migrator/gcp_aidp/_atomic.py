"""Write a file so a crash leaves the old version or the new one, never half of each."""
from __future__ import annotations

import os
import uuid
from pathlib import Path


def write_text_atomic(path: str | Path, text: str) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_name(f".{p.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, p)
    finally:
        temp.unlink(missing_ok=True)
    return p
