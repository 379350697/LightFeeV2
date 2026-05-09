"""JSONL journal: append-only event log matching Rust reference behavior."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


class Journal:
    """Append-only JSONL journal for event persistence."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._seq = 0
        self._run_id = str(int(time.time() * 1000))
        self._file = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a")

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def append(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        flush: bool = False,
        ts_ms: int | None = None,
    ) -> int:
        if self._file is None:
            raise RuntimeError("journal not open")
        self._seq += 1
        record = {
            "seq": self._seq,
            "run_id": self._run_id,
            "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
            "kind": kind,
            "payload": payload,
        }
        self._file.write(json.dumps(record, ensure_ascii=False) + "\n")
        if flush:
            self._file.flush()
            os.fsync(self._file.fileno())
        return self._seq

    def read_all(self) -> list[dict[str, Any]]:
        """Read all journal records. Returns list of parsed dicts."""
        if not self.path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def run_id(self) -> str:
        return self._run_id
