"""Durable entry open-interest evidence cache.

The store is a last-good evidence cache, not a trading input by itself.  Every
read is revalidated against the same targeted OI proof contract before it can
be promoted back into the runtime fallback path.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from lightfee.marketdata.open_interest import (
    ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
    normalize_open_interest_status,
    observed_open_interest_proof_reason,
    open_interest_timestamps_are_fresh,
    open_interest_uses_cache_fallback,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS entry_open_interest_evidence (
    venue TEXT NOT NULL,
    canonical_symbol TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    open_interest_observed_at_ms INTEGER NOT NULL,
    open_interest_event_at_ms INTEGER NOT NULL DEFAULT 0,
    open_interest_received_at_ms INTEGER NOT NULL,
    open_interest_sample_id TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (venue, canonical_symbol)
);
CREATE INDEX IF NOT EXISTS idx_entry_open_interest_evidence_observed
    ON entry_open_interest_evidence(open_interest_observed_at_ms);
"""

_DURABLE_PAYLOAD_FIELDS = (
    "open_interest_quote",
    "open_interest_evidence_status",
    "open_interest_evidence_reason",
    "open_interest_observed_at_ms",
    "open_interest_event_at_ms",
    "open_interest_received_at_ms",
    "open_interest_source",
    "open_interest_sample_id",
    "open_interest_venue_symbol",
    "raw_open_interest",
    "raw_open_interest_unit",
    "open_interest_contract_multiplier",
    "open_interest_conversion_mark_price",
)


def _normal_key(venue: str, symbol: str) -> tuple[str, str]:
    return str(venue or "").strip().lower(), str(symbol or "").strip().upper()


def _clock(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _observed_payload_is_valid(
    *,
    venue: str,
    symbol: str,
    payload: dict[str, Any],
    now_ms: int | None = None,
    max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
) -> bool:
    if (
        normalize_open_interest_status(
            payload.get("open_interest_evidence_status", "unavailable")
        )
        != "observed"
        or open_interest_uses_cache_fallback(payload)
    ):
        return False
    proof_reason = observed_open_interest_proof_reason(
        venue=venue,
        canonical_symbol=symbol,
        venue_symbol=str(payload.get("open_interest_venue_symbol") or ""),
        value_quote=payload.get("open_interest_quote"),
        raw_value=payload.get("raw_open_interest"),
        raw_unit=str(payload.get("raw_open_interest_unit") or ""),
        contract_multiplier=payload.get("open_interest_contract_multiplier"),
        conversion_mark_price=payload.get("open_interest_conversion_mark_price"),
        observed_at_ms=payload.get("open_interest_observed_at_ms"),
        event_at_ms=payload.get("open_interest_event_at_ms"),
        received_at_ms=payload.get("open_interest_received_at_ms"),
        source=str(payload.get("open_interest_source") or ""),
        sample_id=str(payload.get("open_interest_sample_id") or ""),
    )
    if proof_reason:
        return False
    if now_ms is None:
        return True
    return open_interest_timestamps_are_fresh(
        observed_at_ms=_clock(payload.get("open_interest_observed_at_ms")),
        received_at_ms=_clock(payload.get("open_interest_received_at_ms")),
        event_at_ms=_clock(payload.get("open_interest_event_at_ms")),
        now_ms=int(now_ms),
        max_age_ms=max_age_ms,
    )


def _durable_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    result = {
        field: payload[field]
        for field in _DURABLE_PAYLOAD_FIELDS
        if field in payload
    }
    result["open_interest_evidence_status"] = normalize_open_interest_status(
        result.get("open_interest_evidence_status", "unavailable")
    )
    result.setdefault("open_interest_event_at_ms", 0)
    for field in (
        "open_interest_cache_fallback",
        "open_interest_cache_fallback_max_age_ms",
        "open_interest_cache_fallback_age_ms",
    ):
        result.pop(field, None)
    try:
        json.dumps(result, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    return result


class OpenInterestEvidenceStore:
    """SQLite-backed targeted OI last-good store with fail-closed reads."""

    def __init__(self, path: str | Path | None) -> None:
        text = str(path or "").strip()
        self.path = Path(text).expanduser() if text else None

    @property
    def enabled(self) -> bool:
        return self.path is not None

    def _connect(self, *, create: bool) -> sqlite3.Connection | None:
        if self.path is None:
            return None
        if not create and not self.path.exists():
            return None
        if create:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                return None
        conn = sqlite3.connect(str(self.path), timeout=0.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=0")
            if create:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_SCHEMA)
            return conn
        except sqlite3.Error:
            conn.close()
            return None

    def store_observed(
        self,
        *,
        venue: str,
        symbol: str,
        payload: dict[str, Any],
        now_ms: int,
    ) -> bool:
        venue_key, symbol_key = _normal_key(venue, symbol)
        if (
            not venue_key
            or not symbol_key
            or not isinstance(payload, dict)
            or not _observed_payload_is_valid(
                venue=venue_key,
                symbol=symbol_key,
                payload=payload,
                now_ms=now_ms,
            )
        ):
            return False
        durable = _durable_payload(payload)
        if durable is None:
            return False
        observed_at_ms = _clock(durable.get("open_interest_observed_at_ms"))
        event_at_ms = _clock(durable.get("open_interest_event_at_ms"))
        received_at_ms = _clock(durable.get("open_interest_received_at_ms"))
        sample_id = str(durable.get("open_interest_sample_id") or "")
        payload_json = json.dumps(durable, sort_keys=True, separators=(",", ":"))
        conn: sqlite3.Connection | None = None
        try:
            conn = self._connect(create=True)
            if conn is None:
                return False
            conn.execute(
                """
                INSERT INTO entry_open_interest_evidence (
                    venue,
                    canonical_symbol,
                    payload_json,
                    open_interest_observed_at_ms,
                    open_interest_event_at_ms,
                    open_interest_received_at_ms,
                    open_interest_sample_id,
                    updated_at_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, canonical_symbol) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    open_interest_observed_at_ms=excluded.open_interest_observed_at_ms,
                    open_interest_event_at_ms=excluded.open_interest_event_at_ms,
                    open_interest_received_at_ms=excluded.open_interest_received_at_ms,
                    open_interest_sample_id=excluded.open_interest_sample_id,
                    updated_at_ms=excluded.updated_at_ms
                WHERE
                    excluded.open_interest_observed_at_ms
                        > entry_open_interest_evidence.open_interest_observed_at_ms
                """,
                (
                    venue_key,
                    symbol_key,
                    payload_json,
                    observed_at_ms,
                    event_at_ms,
                    received_at_ms,
                    sample_id,
                    int(now_ms),
                ),
            )
            conn.commit()
            return True
        except sqlite3.Error:
            return False
        finally:
            if conn is not None:
                conn.close()

    def load_observed(
        self,
        *,
        venue: str,
        symbol: str,
        now_ms: int,
        max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
    ) -> dict[str, Any] | None:
        venue_key, symbol_key = _normal_key(venue, symbol)
        if not venue_key or not symbol_key:
            return None
        conn: sqlite3.Connection | None = None
        try:
            conn = self._connect(create=False)
            if conn is None:
                return None
            row = conn.execute(
                """
                SELECT venue, canonical_symbol, payload_json
                FROM entry_open_interest_evidence
                WHERE venue = ? AND canonical_symbol = ?
                """,
                (venue_key, symbol_key),
            ).fetchone()
            if row is None:
                return None
            if row["venue"] != venue_key or row["canonical_symbol"] != symbol_key:
                return None
            payload = json.loads(str(row["payload_json"] or ""))
            if not isinstance(payload, dict):
                return None
            payload.setdefault("open_interest_event_at_ms", 0)
            if not _observed_payload_is_valid(
                venue=venue_key,
                symbol=symbol_key,
                payload=payload,
                now_ms=now_ms,
                max_age_ms=max_age_ms,
            ):
                return None
            return dict(payload)
        except (json.JSONDecodeError, sqlite3.Error, TypeError, ValueError):
            return None
        finally:
            if conn is not None:
                conn.close()

    def delete_expired(
        self,
        *,
        now_ms: int,
        max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
    ) -> int:
        conn: sqlite3.Connection | None = None
        try:
            conn = self._connect(create=True)
            if conn is None:
                if self.path is None:
                    return 0
                raise sqlite3.OperationalError(
                    "entry open-interest evidence cleanup store unavailable"
                )
            cutoff_ms = max(int(now_ms) - max(int(max_age_ms), 1), 0)
            cursor = conn.execute(
                """
                DELETE FROM entry_open_interest_evidence
                WHERE open_interest_observed_at_ms < ?
                """,
                (cutoff_ms,),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            if conn is not None:
                conn.close()
