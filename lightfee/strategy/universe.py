"""Daily universe: symbol filtering, ranking, and persistence matching Rust universe.rs."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional


@dataclass
class PersistedDailyUniverse:
    """Serializable daily universe snapshot persisted to JSON."""

    trading_date: str  # YYYY-MM-DD
    generated_at_ms: int
    selector_version: int = 1
    source_symbol_count: int = 0
    selected_symbol_count: int = 0
    selected_symbols: list[str] = field(default_factory=list)

    def validate(self) -> list[str]:
        """Return list of validation error messages. Empty = valid."""
        errors: list[str] = []
        try:
            date.fromisoformat(self.trading_date)
        except (TypeError, ValueError):
            errors.append("trading_date must use YYYY-MM-DD")
        if self.generated_at_ms < 0:
            errors.append("generated_at_ms must be >= 0")
        if self.selector_version < 1:
            errors.append("selector_version must be >= 1")
        if self.selected_symbol_count != len(self.selected_symbols):
            errors.append(
                f"selected_symbol_count {self.selected_symbol_count} != "
                f"len(selected_symbols) {len(self.selected_symbols)}"
            )
        if self.selected_symbol_count > self.source_symbol_count:
            errors.append(
                f"selected_symbol_count {self.selected_symbol_count} > "
                f"source_symbol_count {self.source_symbol_count}"
            )
        # Duplicate check
        seen = set()
        if not isinstance(self.selected_symbols, list):
            errors.append("selected_symbols must be a list")
            return errors
        for s in self.selected_symbols:
            try:
                norm = _normalize_symbol(s)
            except (AttributeError, TypeError, ValueError):
                errors.append(f"invalid symbol: {s!r}")
                continue
            if norm in seen:
                errors.append(f"duplicate symbol: {s}")
            seen.add(norm)
        return errors

    def canonical_selected_symbols(self) -> list[str]:
        """Return V1-normalized selected symbols after contract validation."""
        errors = self.validate()
        if errors:
            raise ValueError(f"invalid PersistedDailyUniverse: {errors}")
        return [_normalize_symbol(symbol) for symbol in self.selected_symbols]

    def save(self, path: str) -> None:
        """Atomically write JSON to path."""
        errors = self.validate()
        if errors:
            raise ValueError(f"invalid PersistedDailyUniverse: {errors}")
        dirname = os.path.dirname(path) or "."
        fd, tmp = tempfile.mkstemp(dir=dirname, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self._asdict(), f, indent=2)
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    @classmethod
    def load(cls, path: str) -> Optional[PersistedDailyUniverse]:
        """Load the V1 persisted payload. Returns ``None`` only if absent."""
        if not os.path.exists(path):
            return None
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"invalid PersistedDailyUniverse in {path}: object required")
        required = (
            "trading_date",
            "generated_at_ms",
            "source_symbol_count",
            "selected_symbol_count",
        )
        missing = [field for field in required if field not in data]
        if missing:
            raise ValueError(
                f"invalid PersistedDailyUniverse in {path}: missing {', '.join(missing)}"
            )
        integer_fields = (
            "generated_at_ms",
            "selector_version",
            "source_symbol_count",
            "selected_symbol_count",
        )
        for field_name in integer_fields:
            if field_name in data and (
                isinstance(data[field_name], bool)
                or not isinstance(data[field_name], int)
            ):
                raise ValueError(
                    f"invalid PersistedDailyUniverse in {path}: {field_name} must be an integer"
                )
        if not isinstance(data["trading_date"], str):
            raise ValueError(
                f"invalid PersistedDailyUniverse in {path}: trading_date must be a string"
            )
        if "selected_symbols" in data and not isinstance(data["selected_symbols"], list):
            raise ValueError(
                f"invalid PersistedDailyUniverse in {path}: selected_symbols must be a list"
            )
        inst = cls(
            trading_date=data["trading_date"],
            generated_at_ms=data["generated_at_ms"],
            selector_version=data.get("selector_version", 1),
            source_symbol_count=data["source_symbol_count"],
            selected_symbol_count=data["selected_symbol_count"],
            selected_symbols=data.get("selected_symbols", []),
        )
        errors = inst.validate()
        if errors:
            raise ValueError(f"invalid PersistedDailyUniverse in {path}: {errors}")
        return inst

    def _asdict(self) -> dict:
        return {
            "trading_date": self.trading_date,
            "generated_at_ms": self.generated_at_ms,
            "selector_version": self.selector_version,
            "source_symbol_count": self.source_symbol_count,
            "selected_symbol_count": self.selected_symbol_count,
            "selected_symbols": self.selected_symbols,
        }


@dataclass
class RuntimeSymbolResolutionSummary:
    """Returned by prepare_runtime_symbols with resolution stats."""

    daily_universe_enabled: bool = False
    global_symbol_count: int = 0
    resolved_symbol_count: int = 0
    selector_adapter_count: int = 0


def _normalize_symbol(raw: str) -> str:
    """Uppercase and strip separators/dashes/spaces."""
    s = raw.upper().replace("-", "").replace("_", "").replace("/", "").replace(" ", "")
    if not s:
        raise ValueError(f"blank symbol after normalization: {raw!r}")
    return s


def today_trading_date(tz: timezone | None = None) -> str:
    """Return today's date as YYYY-MM-DD in Shanghai time (UTC+8) by default."""
    if tz is None:
        tz = timezone(timedelta(hours=8))  # UTC+8 (Shanghai)
    return datetime.now(tz).strftime("%Y-%m-%d")
