"""Compact, fail-closed market snapshot for the spread sidecar.

The complete opportunity snapshot intentionally contains funding, liquidity,
candidate, and audit evidence.  Spread screening only needs the already-fetched
public quotes.  Publishing this compact view immediately after the concurrent
market fetch prevents the slower funding workflow from aging every spread
observation before it can be sampled.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from math import isfinite
import os
from pathlib import Path
import tempfile

import orjson

from lightfee.sidecar.snapshot import QuoteSnapshot, _quote_field_contract_errors


SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION = 2
LEGACY_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS = frozenset({1})
_QUOTE_FIELD_NAMES = tuple(field.name for field in fields(QuoteSnapshot))


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
    return path.with_name(f"{stem}.spread-quotes.v2{suffix}")


def publish_spread_quote_snapshot(
    snapshot: SpreadQuoteSnapshot,
    path: str | Path,
    *,
    validate_contract: bool = True,
) -> None:
    """Atomically publish JSON; only a fully checked producer may skip validation.

    The independent BBO producer validates identity, receipt timestamps and
    BBO numerics, then overlays them only on the main producer's typed,
    contract-eligible last-good cache.  Avoiding a second 51-field Python
    validation pass keeps that trusted hot path below its freshness budget.
    All other callers retain the strict default, and every consumer validates
    the file again.
    """

    data = _snapshot_to_dict(snapshot)
    if validate_contract:
        errors = validate_spread_quote_snapshot_contract(data)
        if errors:
            raise ValueError(
                "refusing to publish invalid spread quote snapshot: "
                + "; ".join(errors)
            )
    else:
        errors = _hot_path_snapshot_errors(snapshot)
        if errors:
            raise ValueError(
                "refusing to publish invalid prevalidated spread quote snapshot: "
                + "; ".join(errors)
            )
    payload = orjson.dumps(data)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".spread-quotes-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        with open(tmp, "wb") as handle:
            handle.write(payload)
            handle.flush()
        # This is an ephemeral, fail-closed market-data view which is replaced
        # several times per second.  Atomic rename protects readers from torn
        # JSON; forcing every generation to stable storage only adds latency
        # and write pressure.  A host crash may lose the latest generation,
        # which is safe because the consumer rejects stale/missing snapshots.
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
        data = orjson.loads(target.read_bytes())
    except (OSError, orjson.JSONDecodeError, TypeError, ValueError, OverflowError):
        return None
    if validate_spread_quote_snapshot_contract(data):
        return None
    try:
        quote_payloads = _quote_payloads(data)
        if quote_payloads is None:
            return None
        quotes = {
            key: QuoteSnapshot(
                **{
                    **raw,
                    "bid_depth": tuple(tuple(level) for level in raw.get("bid_depth", [])),
                    "ask_depth": tuple(tuple(level) for level in raw.get("ask_depth", [])),
                }
            )
            for key, raw in quote_payloads.items()
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
    schema_version = raw.get("schema_version")
    if schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
        required.add("quote_fields")
    errors = [f"missing:{name}" for name in sorted(required - raw.keys())]
    errors.extend(f"unknown:{name}" for name in sorted(raw.keys() - required))
    if schema_version not in (
        LEGACY_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS
        | {SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION}
    ):
        errors.append("schema_version_unsupported")
    if schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION and raw.get(
        "quote_fields"
    ) != list(_QUOTE_FIELD_NAMES):
        errors.append("quote_fields_invalid")

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
    quote_payloads = _quote_payloads(raw)
    if quote_payloads is None:
        errors.append("quote_rows_invalid")
        return errors
    observed: list[int] = []
    published_at_ms = timestamps.get("published_at_ms")
    for key, quote in quote_payloads.items():
        if not isinstance(key, str):
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


def _hot_path_snapshot_errors(snapshot: SpreadQuoteSnapshot) -> list[str]:
    """Check volatile trust-boundary invariants without revalidating metadata."""

    errors: list[str] = []
    published_at_ms = int(snapshot.published_at_ms or 0)
    market_observed_at_ms = int(snapshot.market_observed_at_ms or 0)
    batch_started_at_ms = int(snapshot.batch_started_at_ms or 0)
    if not (
        0 < batch_started_at_ms <= published_at_ms
        and 0 < market_observed_at_ms <= published_at_ms
    ):
        errors.append("watermark_order_invalid")
    configured = set(snapshot.configured_venues)
    if not configured or len(configured) != len(snapshot.configured_venues):
        errors.append("configured_venues_invalid")
    if any(venue not in configured for venue in snapshot.degraded_venues):
        errors.append("degraded_venues_invalid")

    observed: list[int] = []
    for key, quote in snapshot.quotes.items():
        venue = str(quote.venue or "")
        symbol = str(quote.symbol or "")
        quote_observed_at_ms = int(quote.observed_at_ms or 0)
        numeric = (
            quote.bid,
            quote.ask,
            quote.bid_size,
            quote.ask_size,
        )
        if key != f"{venue}:{symbol}" or venue not in configured:
            errors.append(f"quote_identity_invalid:{key}")
        if (
            venue != venue.strip().lower()
            or symbol != symbol.strip().upper()
            or not venue
            or not symbol
        ):
            errors.append(f"quote_identity_not_canonical:{key}")
        if not all(type(value) in (int, float) and isfinite(float(value)) for value in numeric):
            errors.append(f"quote_bbo_type_invalid:{key}")
        elif not (
            float(quote.bid) > 0.0
            and float(quote.ask) >= float(quote.bid)
            and float(quote.bid_size) >= 0.0
            and float(quote.ask_size) >= 0.0
        ):
            errors.append(f"quote_bbo_invalid:{key}")
        if quote_observed_at_ms <= 0 or quote_observed_at_ms > published_at_ms:
            errors.append(f"quote_from_future:{key}")
        else:
            observed.append(quote_observed_at_ms)
    if not observed:
        errors.append("quotes_invalid")
    elif market_observed_at_ms != max(observed):
        errors.append("market_observed_at_ms_mismatch")
    return errors


def _snapshot_to_dict(snapshot: SpreadQuoteSnapshot) -> dict[str, object]:
    quote_fields = fields(QuoteSnapshot)
    quotes: dict[str, object] = {}
    for key, quote in snapshot.quotes.items():
        payload: dict[str, object] = {}
        for quote_field in quote_fields:
            value = getattr(quote, quote_field.name)
            if quote_field.name in {"bid_depth", "ask_depth"}:
                value = [list(level) for level in value]
            payload[quote_field.name] = value
        if snapshot.schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
            quotes[key] = [payload[name] for name in _QUOTE_FIELD_NAMES]
        else:
            quotes[key] = payload
    result: dict[str, object] = {
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
    if snapshot.schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
        # Field names are emitted once for the whole file instead of once per
        # quote.  This preserves every QuoteSnapshot value while removing the
        # dominant repeated-key overhead from thousands of symbols.
        result["quote_fields"] = list(_QUOTE_FIELD_NAMES)
    return result


def _quote_payloads(raw: dict[str, object]) -> dict[str, dict[str, object]] | None:
    """Decode v1 object rows or v2 positional rows without coercing values."""
    quotes = raw.get("quotes")
    if not isinstance(quotes, dict):
        return None
    schema_version = raw.get("schema_version")
    if schema_version in LEGACY_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS:
        if any(not isinstance(value, dict) for value in quotes.values()):
            return None
        return {str(key): dict(value) for key, value in quotes.items()}
    if schema_version != SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
        return None
    quote_fields = raw.get("quote_fields")
    if quote_fields != list(_QUOTE_FIELD_NAMES):
        return None
    decoded: dict[str, dict[str, object]] = {}
    for key, row in quotes.items():
        if not isinstance(key, str) or not isinstance(row, list):
            return None
        if len(row) != len(_QUOTE_FIELD_NAMES):
            return None
        decoded[key] = dict(zip(_QUOTE_FIELD_NAMES, row, strict=True))
    return decoded
