"""Lightweight seq→byte_offset index for JSONL journal files.

Enables sub-linear seek for stream_from() without read_all() pressure.
The index is a rebuildable sidecar file — never required for correctness,
only for speed.
"""

from __future__ import annotations

import json
from pathlib import Path


class JournalIndex:
    """Maps journal seq → byte offset for fast seek."""

    def __init__(self, journal_path: str | Path) -> None:
        self._journal_path = Path(journal_path)
        self._index_path = Path(str(journal_path) + ".idx")
        self._offsets: dict[int, int] = {}

    # ------------------------------------------------------------------
    # Build / load
    # ------------------------------------------------------------------

    def build(self) -> int:
        """Scan the journal and rebuild the byte-offset index.

        Returns the number of records indexed.
        """
        self._offsets.clear()
        path = self._journal_path
        if not path.exists():
            self._persist()
            return 0

        count = 0
        with open(path, "rb") as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    rec = json.loads(stripped)
                    seq = rec.get("seq")
                    if isinstance(seq, int) and seq > 0:
                        self._offsets[seq] = offset
                        count += 1
                except json.JSONDecodeError:
                    pass

        self._persist()
        return count

    def load(self) -> bool:
        """Load the index from its sidecar file. Returns True on success."""
        try:
            if self._index_path.exists():
                with open(self._index_path) as f:
                    raw = json.load(f)
                # Convert JSON string keys back to int
                self._offsets = {int(k): v for k, v in raw.items()}
                return True
        except (OSError, json.JSONDecodeError, ValueError):
            pass
        return False

    def _persist(self) -> None:
        """Write the index to its sidecar file."""
        with open(self._index_path, "w") as f:
            json.dump(self._offsets, f)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def offset_for(self, seq: int) -> int | None:
        """Byte offset where the record for `seq` starts, or None."""
        return self._offsets.get(seq)

    @property
    def max_seq(self) -> int:
        """Highest seq in the index (0 if empty)."""
        return max(self._offsets) if self._offsets else 0

    @property
    def record_count(self) -> int:
        """Number of indexed records."""
        return len(self._offsets)

    @property
    def index_path(self) -> Path:
        return self._index_path
