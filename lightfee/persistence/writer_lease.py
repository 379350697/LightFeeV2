"""Single-host exclusion for mutations of a live persistence pair."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TextIO

try:  # Production runs on Linux; keep the failure explicit on unsupported hosts.
    import fcntl
except ImportError:  # pragma: no cover - exercised only on non-Unix hosts.
    fcntl = None  # type: ignore[assignment]


class PersistenceWriterLeaseError(RuntimeError):
    """Raised when another process owns a persistence writer lease."""


class PersistenceWriterLease:
    """Fail closed when another local process owns the same event-log pair.

    The lock is advisory and intentionally local to one host.  It is enough for
    the single-server deployment: ``lightfee-live`` and every mutating
    ``lightfee-ops`` command acquire the same lease before reading the snapshot
    or appending a journal event. Kernel lock release on process exit makes
    stale lock *files* harmless.
    """

    def __init__(self, event_log_path: str | Path) -> None:
        event_log = Path(event_log_path)
        self.path = event_log.with_name(f".{event_log.name}.writer.lock")
        self._file: TextIO | None = None

    def acquire(self) -> None:
        """Acquire the non-blocking lease or raise without changing state."""
        if self._file is not None:
            return
        if fcntl is None:
            raise PersistenceWriterLeaseError(
                "single-writer protection requires a Unix fcntl host"
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = open(self.path, "a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise PersistenceWriterLeaseError(
                "persistence writer is active; stop lightfee-live before mutating persisted state"
            ) from exc

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()}\n")
        lock_file.flush()
        self._file = lock_file

    def release(self) -> None:
        """Release the lease.  The marker file is retained for diagnostics only."""
        lock_file = self._file
        self._file = None
        if lock_file is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()

    def __enter__(self) -> "PersistenceWriterLease":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except Exception:
            pass
