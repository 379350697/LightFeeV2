"""Atomic snapshot store for restart recovery."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class SnapshotStore:
    """Atomic JSON snapshot store. Writes to temp file then replaces."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def write(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = Path(tempfile.mktemp(dir=str(self.path.parent), prefix=".snapshot-"))
        try:
            content = json.dumps(data, ensure_ascii=False, indent=2)
            with open(tmp_path, "w") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            tmp_path.replace(self.path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def read(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            with open(self.path) as f:
                data = json.loads(f.read())
            if not isinstance(data, dict):
                return None
            return data
        except (json.JSONDecodeError, OSError):
            return None

    def exists(self) -> bool:
        return self.path.exists()
