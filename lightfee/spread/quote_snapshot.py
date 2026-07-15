"""Compact, fail-closed market snapshot for the spread sidecar.

The complete opportunity snapshot intentionally contains funding, liquidity,
candidate, and audit evidence.  Spread screening only needs the already-fetched
public quotes.  Publishing this compact view immediately after the concurrent
market fetch prevents the slower funding workflow from aging every spread
observation before it can be sampled.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import json
from math import isfinite
import os
from pathlib import Path
import tempfile

from lightfee.sidecar.snapshot import QuoteSnapshot, _quote_field_contract_errors


SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SpreadQuoteSnapshot:
    published_at_ms: int
    market_observed_at_ms: int
    batch_started_at_ms: int
    configured_venues: list[str]
    degraded_venues: list[str]
    degraded_symbols: dict[str, list[str]]
    quotes: dict[str, QuoteSnapshot]
    schema_version: int = SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION
    source_mode: str = "sidecar_market_fast_path"


def spread_quote_snapshot_path(sidecar_snapshot_path: str | Path) -> Path:
    """Derive a stable sibling path without expanding shared runtime config."""
    path = Path(sidecar_snapshot_path)
    suffix = path.suffix or ".json"
    stem = path.name[: -len(path.suffix)] if path.suffix else path.name
    return path.with_name(f"{stem}.spread-quotes.v1{suffix}")


def publish_spread_quote_snapshot(
    snapshot: SpreadQuoteSnapshot,
    path: str | Path,
) -> None:
    data = _snapshot_to_dict(snapshot)
    errors = validate_spread_quote_snapshot_contract(data)
    if errors:
        raise ValueError(
            "refusing to publish invalid spread quote snapshot: " + "; ".join(errors)
        )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".spread-quotes-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        with open(tmp, "w") as handle:
            json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_spread_quote_snapshot(path: str | Path) -> SpreadQuoteSnapshot | None:
    target = Path(path)
    if not target.exists():
        return None
    try:
        with open(target) as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError, OverflowError):
        return None
    if validate_spread_quote_snapshot_contract(data):
        return None
    try:
        quotes = {
            key: QuoteSnapshot(
                **{
                    **raw,
                    "bid_depth": tuple(tuple(level) for level in raw.get("bid_depth", [])),
                    "ask_depth": tuple(tuple(level) for level in raw.get("ask_depth", [])),
                }
            )
            for key, raw in data["quotes"].items()
        }
        return SpreadQuoteSnapshot(
            schema_version=data["schema_version"],
            published_at_ms=data["published_at_ms"],
            market_observed_at_ms=data["market_observed_at_ms"],
            batch_started_at_ms=data["batch_started_at_ms"],
            source_mode=data["source_mode"],
            configured_venues=list(data["configured_venues"]),
            degraded_venues=list(data["degraded_venues"]),
            degraded_symbols={
                venue: list(symbols)
                for venue, symbols in data["degraded_symbols"].items()
            },
            quotes=quotes,
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def validate_spread_quote_snapshot_contract(raw: object) -> list[str]:
    if not isinstance(raw, dict):
        return ["root_not_object"]
    required = {
        "schema_version",
        "published_at_ms",
        "market_observed_at_ms",
        "batch_started_at_ms",
        "source_mode",
        "configured_venues",
        "degraded_venues",
        "degraded_symbols",
        "quotes",
    }
    errors = [f"missing:{name}" for name in sorted(required - raw.keys())]
    errors.extend(f"unknown:{name}" for name in sorted(raw.keys() - required))
    if raw.get("schema_version") != SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
        errors.append("schema_version_unsupported")

    timestamps = {
        name: raw.get(name)
        for name in ("published_at_ms", "market_observed_at_ms", "batch_started_at_ms")
    }
    for name, value in timestamps.items():
        if type(value) is not int or value <= 0:
            errors.append(f"{name}_invalid")
    if all(type(value) is int and value > 0 for value in timestamps.values()):
        # Last-good quotes may predate this refresh batch. Both evidence
        # watermarks must precede publication, but their mutual order is not a
        # freshness guarantee.
        if not (
            timestamps["batch_started_at_ms"] <= timestamps["published_at_ms"]
            and timestamps["market_observed_at_ms"]
            <= timestamps["published_at_ms"]
        ):
            errors.append("watermark_order_invalid")
    if raw.get("source_mode") != "sidecar_market_fast_path":
        errors.append("source_mode_invalid")

    configured = raw.get("configured_venues")
    configured_set: set[str] = set()
    if not isinstance(configured, list) or not configured:
        errors.append("configured_venues_invalid")
    elif any(
        not isinstance(venue, str) or venue != venue.strip().lower() or not venue
        for venue in configured
    ) or len(set(configured)) != len(configured):
        errors.append("configured_venues_invalid")
    else:
        configured_set = set(configured)

    degraded_venues = raw.get("degraded_venues")
    if not isinstance(degraded_venues, list):
        errors.append("degraded_venues_invalid")
    elif any(venue not in configured_set for venue in degraded_venues) or len(
        set(degraded_venues)
    ) != len(degraded_venues):
        errors.append("degraded_venues_invalid")

    degraded_symbols = raw.get("degraded_symbols")
    if not isinstance(degraded_symbols, dict):
        errors.append("degraded_symbols_invalid")
    else:
        for venue, symbols in degraded_symbols.items():
            if (
                venue not in configured_set
                or not isinstance(symbols, list)
                or not symbols
                or any(
                    not isinstance(symbol, str)
                    or symbol != symbol.strip().upper()
                    or not symbol
                    for symbol in symbols
                )
                or len(set(symbols)) != len(symbols)
            ):
                errors.append(f"degraded_symbols_invalid:{venue}")

    quotes = raw.get("quotes")
    if not isinstance(quotes, dict) or not quotes:
        errors.append("quotes_invalid")
        return errors
    observed: list[int] = []
    published_at_ms = timestamps.get("published_at_ms")
    for key, quote in quotes.items():
        if not isinstance(key, str) or not isinstance(quote, dict):
            errors.append(f"quote_invalid:{key}")
            continue
        errors.extend(_quote_field_contract_errors(quote, key=key))
        venue = quote.get("venue")
        symbol = quote.get("symbol")
        if venue not in configured_set or key != f"{venue}:{symbol}":
            errors.append(f"quote_identity_invalid:{key}")
        bid = quote.get("bid")
        ask = quote.get("ask")
        if not (
            type(bid) in (int, float)
            and type(ask) in (int, float)
            and isfinite(float(bid))
            and isfinite(float(ask))
            and 0 < float(bid) <= float(ask)
        ):
            errors.append(f"quote_bbo_invalid:{key}")
        quote_observed_at_ms = quote.get("observed_at_ms")
        if type(quote_observed_at_ms) is int and quote_observed_at_ms > 0:
            observed.append(quote_observed_at_ms)
            if type(published_at_ms) is int and quote_observed_at_ms > published_at_ms:
                errors.append(f"quote_from_future:{key}")
    if observed and timestamps.get("market_observed_at_ms") != max(observed):
        errors.append("market_observed_at_ms_mismatch")
    return errors


def _snapshot_to_dict(snapshot: SpreadQuoteSnapshot) -> dict[str, object]:
    quote_fields = fields(QuoteSnapshot)
    quotes: dict[str, dict[str, object]] = {}
    for key, quote in snapshot.quotes.items():
        payload: dict[str, object] = {}
        for quote_field in quote_fields:
            value = getattr(quote, quote_field.name)
            if quote_field.name in {"bid_depth", "ask_depth"}:
                value = [list(level) for level in value]
            payload[quote_field.name] = value
        quotes[key] = payload
    return {
        "schema_version": snapshot.schema_version,
        "published_at_ms": snapshot.published_at_ms,
        "market_observed_at_ms": snapshot.market_observed_at_ms,
        "batch_started_at_ms": snapshot.batch_started_at_ms,
        "source_mode": snapshot.source_mode,
        "configured_venues": list(snapshot.configured_venues),
        "degraded_venues": list(snapshot.degraded_venues),
        "degraded_symbols": {
            venue: list(symbols)
            for venue, symbols in snapshot.degraded_symbols.items()
        },
        "quotes": quotes,
    }
