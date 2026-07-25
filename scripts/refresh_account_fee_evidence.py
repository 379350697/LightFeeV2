#!/usr/bin/env python3
"""Refresh local account-fee schedules from seven read-only account APIs.

The snapshot is atomically written as a service-user-owned 0600 file.  A failed
venue refresh reuses its still-fresh last-good row, which is appropriate for
slow-changing account fee tiers; otherwise that venue naturally falls back to
the separately capped conservative funding tier.  This command never creates,
cancels, or amends an order.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Callable

from lightfee.config.loader import load_config
from lightfee.config.paths import resolve_config_artifact_path
from lightfee.core.domain import Venue
from lightfee.strategy.fee_evidence import (
    LOCAL_FEE_EVIDENCE_SCHEMA_VERSION,
    load_fee_evidence,
)
from lightfee.venues.registry import build_adapter_map


ACCOUNT_FEE_API_VENUES = frozenset(
    {
        Venue.ASTER,
        Venue.BINANCE,
        Venue.BITGET,
        Venue.BYBIT,
        Venue.GATE,
        Venue.HYPERLIQUID,
        Venue.OKX,
    }
)


def _rate(value: object, *, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"invalid {field}")
    return parsed


def _identity_hash(value: object, *, venue: str) -> str:
    import hashlib

    identity = str(value or "").strip()
    if not identity:
        raise ValueError(f"missing {venue} account identity")
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _observed_at_ms(value: object, *, now_ms: int, venue: str) -> int:
    try:
        observed_at_ms = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid {venue} observation timestamp") from exc
    if observed_at_ms <= 0 or observed_at_ms > now_ms + 5_000:
        raise ValueError(f"invalid {venue} observation timestamp")
    return min(observed_at_ms, now_ms)


def _canonical_contract_symbol(value: object) -> str:
    normalized = str(value or "").strip().upper().replace("-SWAP", "")
    return normalized.replace("-", "").replace("_", "")


def _validate_response_symbol(
    row: dict[str, Any],
    *,
    expected: str,
    venue: str,
    fields: tuple[str, ...],
    required: bool,
) -> None:
    returned = next(
        (str(row.get(field) or "").strip() for field in fields if row.get(field)),
        "",
    )
    if required and not returned:
        raise ValueError(f"missing {venue} response symbol")
    if returned and _canonical_contract_symbol(returned) != _canonical_contract_symbol(
        expected
    ):
        raise ValueError(f"mismatched {venue} response symbol")


def parse_bybit_evidence(
    fee_response: dict[str, Any],
    identity_response: dict[str, Any] | None = None,
    *,
    now_ms: int,
    symbol: str = "BTCUSDT",
) -> dict[str, object]:
    if fee_response.get("retCode") != 0 or (
        identity_response is not None and identity_response.get("retCode") != 0
    ):
        raise ValueError("bybit private API rejected evidence request")
    fee_rows = (fee_response.get("result") or {}).get("list") or []
    identity = (identity_response or {}).get("result") or {}
    if not isinstance(fee_rows, list) or len(fee_rows) != 1 or not isinstance(identity, dict):
        raise ValueError("invalid bybit fee evidence shape")
    row = fee_rows[0]
    if not isinstance(row, dict):
        raise ValueError("invalid bybit fee evidence row")
    _validate_response_symbol(
        row,
        expected=symbol,
        venue="bybit",
        fields=("symbol",),
        required=True,
    )
    taker = _rate(row.get("takerFeeRate"), field="bybit takerFeeRate")
    maker = _rate(row.get("makerFeeRate"), field="bybit makerFeeRate")
    if taker < 0.0:
        raise ValueError("invalid bybit takerFeeRate")
    observed_at_ms = _observed_at_ms(
        fee_response.get("time"), now_ms=now_ms, venue="bybit"
    )
    result = {
        "taker_fee_bps": taker * 10_000.0,
        "maker_fee_bps": maker * 10_000.0,
        "observed_at_ms": observed_at_ms,
        "source": "account_fee_api",
        "evidence_ref": (
            f"bybit:/v5/account/fee-rate:{row.get('symbol') or 'linear'}:{observed_at_ms}"
        ),
    }
    if identity_response is not None:
        result["account_identity_hash"] = _identity_hash(
            identity.get("userID"), venue="bybit"
        )
    return result


def parse_okx_evidence(
    fee_response: dict[str, Any],
    identity_response: dict[str, Any] | None = None,
    *,
    now_ms: int,
    symbol: str = "",
    expected_group_id: str = "",
) -> dict[str, object]:
    if str(fee_response.get("code")) != "0" or (
        identity_response is not None and str(identity_response.get("code")) != "0"
    ):
        raise ValueError("okx private API rejected evidence request")
    fee_rows = fee_response.get("data") or []
    identity_rows = (identity_response or {}).get("data") or []
    if len(fee_rows) != 1 or (identity_response is not None and len(identity_rows) != 1):
        raise ValueError("invalid okx fee evidence shape")
    row = fee_rows[0]
    identity = identity_rows[0] if identity_rows else {}
    if not isinstance(row, dict) or not isinstance(identity, dict):
        raise ValueError("invalid okx fee evidence row")
    fee_groups = row.get("feeGroup")
    if not isinstance(fee_groups, list) or not expected_group_id:
        raise ValueError("invalid okx feeGroup evidence")
    matching_groups = [
        group
        for group in fee_groups
        if isinstance(group, dict)
        and str(group.get("groupId") or "").strip() == expected_group_id
    ]
    if len(matching_groups) != 1:
        raise ValueError("ambiguous okx feeGroup evidence")
    fee_group = matching_groups[0]
    # OKX uses negative values for fees charged and positive values for
    # rebates.  The strategy contract uses positive values for costs.
    taker = -_rate(fee_group.get("taker"), field="okx feeGroup taker")
    maker = -_rate(fee_group.get("maker"), field="okx feeGroup maker")
    if taker < 0.0:
        raise ValueError("invalid okx taker fee sign")
    observed_at_ms = _observed_at_ms(
        row.get("ts"), now_ms=now_ms, venue="okx"
    )
    result = {
        "taker_fee_bps": taker * 10_000.0,
        "maker_fee_bps": maker * 10_000.0,
        "observed_at_ms": observed_at_ms,
        "source": "account_fee_api",
        "evidence_ref": (
            "okx:/api/v5/account/trade-fee:"
            f"SWAP:{symbol}:{expected_group_id}:{observed_at_ms}"
        ),
    }
    if identity_response is not None:
        result["account_identity_hash"] = _identity_hash(identity.get("uid"), venue="okx")
    return result


def parse_okx_instrument_groups(
    response: dict[str, Any], symbols: list[str]
) -> dict[str, str]:
    """Bind each requested OKX SWAP instrument to its current fee group."""
    if str(response.get("code")) != "0" or not isinstance(response.get("data"), list):
        raise ValueError("okx account instruments request failed")
    expected = {_canonical_contract_symbol(symbol): symbol for symbol in symbols}
    groups: dict[str, str] = {}
    for row in response["data"]:
        if not isinstance(row, dict):
            continue
        canonical = _canonical_contract_symbol(row.get("instId"))
        group_id = str(row.get("groupId") or "").strip()
        if canonical not in expected or not group_id:
            continue
        symbol = expected[canonical]
        if symbol in groups and groups[symbol] != group_id:
            raise ValueError(f"ambiguous okx instrument fee group:{symbol}")
        groups[symbol] = group_id
    if not groups:
        raise ValueError("okx returned no requested instrument fee groups")
    return groups


def _single_data_row(response: dict[str, Any], *, venue: str) -> dict[str, Any]:
    value: Any = response.get("data", response)
    if isinstance(value, list):
        if len(value) != 1:
            raise ValueError(f"invalid {venue} fee evidence shape")
        value = value[0]
    if not isinstance(value, dict):
        raise ValueError(f"invalid {venue} fee evidence shape")
    return value


def _rate_row(
    *, venue: str, row: dict[str, Any], taker_field: str, maker_field: str,
    now_ms: int, evidence_ref: str
) -> dict[str, object]:
    taker = _rate(row.get(taker_field), field=f"{venue} {taker_field}")
    maker = _rate(row.get(maker_field), field=f"{venue} {maker_field}")
    if taker < 0.0:
        raise ValueError(f"invalid {venue} taker fee")
    return {
        "taker_fee_bps": taker * 10_000.0,
        "maker_fee_bps": maker * 10_000.0,
        "observed_at_ms": now_ms,
        "source": "account_fee_api",
        "evidence_ref": f"{evidence_ref}:{now_ms}",
    }


def parse_binance_evidence(
    response: dict[str, Any], *, now_ms: int, symbol: str = "BTCUSDT"
) -> dict[str, object]:
    row = _single_data_row(response, venue="binance")
    _validate_response_symbol(
        row,
        expected=symbol,
        venue="binance",
        fields=("symbol",),
        required=True,
    )
    return _rate_row(
        venue="binance",
        row=row,
        taker_field="takerCommissionRate",
        maker_field="makerCommissionRate",
        now_ms=now_ms,
        evidence_ref=f"binance:/fapi/v1/commissionRate:{symbol}",
    )


def parse_aster_evidence(
    response: dict[str, Any], *, now_ms: int, symbol: str = "BTCUSDT"
) -> dict[str, object]:
    if str(response.get("code", "0")) not in {"0", "200"}:
        raise ValueError("aster private API rejected evidence request")
    row = _single_data_row(response, venue="aster")
    _validate_response_symbol(
        row,
        expected=symbol,
        venue="aster",
        fields=("symbol",),
        required=True,
    )
    return _rate_row(
        venue="aster",
        row=row,
        taker_field="takerCommissionRate",
        maker_field="makerCommissionRate",
        now_ms=now_ms,
        evidence_ref=f"aster:/fapi/v3/commissionRate:{symbol}",
    )


def parse_bitget_evidence(
    response: dict[str, Any], *, now_ms: int, symbol: str = "BTCUSDT"
) -> dict[str, object]:
    if str(response.get("code")) not in {"0", "00000"}:
        raise ValueError("bitget private API rejected evidence request")
    row = _single_data_row(response, venue="bitget")
    _validate_response_symbol(
        row,
        expected=symbol,
        venue="bitget",
        fields=("symbol",),
        required=False,
    )
    return _rate_row(
        venue="bitget",
        row=row,
        taker_field="takerFeeRate",
        maker_field="makerFeeRate",
        now_ms=now_ms,
        evidence_ref=f"bitget:/api/v2/common/trade-rate:{symbol}:mix",
    )


def parse_gate_evidence(
    response: dict[str, Any], *, now_ms: int, symbol: str = "BTC_USDT"
) -> dict[str, object]:
    row: Any = response.get(symbol, response)
    if isinstance(row, dict) and "data" in row:
        row = row["data"]
    if not isinstance(row, dict):
        raise ValueError("invalid gate fee evidence shape")
    return _rate_row(
        venue="gate",
        row=row,
        taker_field="taker_fee",
        maker_field="maker_fee",
        now_ms=now_ms,
        evidence_ref=f"gate:/api/v4/futures/usdt/fee:{symbol}",
    )


def parse_hyperliquid_evidence(
    response: dict[str, Any], *, now_ms: int
) -> dict[str, object]:
    return _rate_row(
        venue="hyperliquid",
        row=_single_data_row(response, venue="hyperliquid"),
        taker_field="userCrossRate",
        maker_field="userAddRate",
        now_ms=now_ms,
        evidence_ref="hyperliquid:/info:userFees",
    )


def _aggregate_symbol_rows(
    venue: str,
    rows: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Freeze a venue-wide conservative schedule over an explicit symbol set."""
    if not rows:
        raise ValueError(f"{venue} returned no symbol-scoped fee evidence")
    covered_symbols = sorted(rows)
    taker_fee_bps = max(float(row["taker_fee_bps"]) for row in rows.values())
    maker_fee_bps = max(float(row["maker_fee_bps"]) for row in rows.values())
    observed_at_ms = min(int(row["observed_at_ms"]) for row in rows.values())
    digest = hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "taker_fee_bps": taker_fee_bps,
        "maker_fee_bps": maker_fee_bps,
        "observed_at_ms": observed_at_ms,
        "source": "account_fee_api",
        "evidence_ref": f"{venue}:worst-case-symbol-scope:{digest}",
        "covered_symbols": covered_symbols,
        "symbol_schedules": {
            symbol: {
                "taker_fee_bps": float(rows[symbol]["taker_fee_bps"]),
                "maker_fee_bps": float(rows[symbol]["maker_fee_bps"]),
                "observed_at_ms": int(rows[symbol]["observed_at_ms"]),
                "evidence_ref": str(rows[symbol]["evidence_ref"]),
            }
            for symbol in covered_symbols
        },
    }


async def _collect_symbol_scoped(
    venue: str,
    symbols: list[str],
    fetch_one: Any,
) -> dict[str, object]:
    """Collect independent symbols; failed contracts remain uncovered/fail-closed."""
    results = await asyncio.gather(
        *(fetch_one(symbol) for symbol in symbols),
        return_exceptions=True,
    )
    rows: dict[str, dict[str, object]] = {}
    for symbol, result in zip(symbols, results, strict=True):
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, Exception):
            continue
        if isinstance(result, BaseException):
            raise result
        rows[symbol] = result
    return _aggregate_symbol_rows(venue, rows)


async def collect_evidence(
    config_path: str,
    *,
    now_ms: int,
    clock_ms: Callable[[], int] | None = None,
) -> tuple[dict[str, dict[str, object]], dict[str, str], set[str]]:
    receipt_clock_ms = clock_ms or (lambda: int(time.time() * 1000))
    config = load_config(config_path)
    requested = {
        Venue.from_str(str(venue))
        for venue in config.strategy.funding_canary_allowed_venues
        if str(venue).strip()
    }
    exact_venues = requested & ACCOUNT_FEE_API_VENUES
    if not exact_venues:
        raise ValueError(
            "no configured canary venue has an account-fee API collector"
        )
    adapters = build_adapter_map(config)
    symbols = sorted(
        {
            str(symbol or "").strip().upper()
            for symbol in config.symbols
            if str(symbol or "").strip()
        }
    )
    if not symbols:
        raise ValueError("no configured funding symbol has an account-fee scope")
    try:
        async def collect_aster() -> dict[str, object]:
            private = getattr(adapters[Venue.ASTER], "_private", None)
            if private is None:
                raise ValueError("aster v3 private client unavailable")
            async def fetch_one(symbol: str) -> dict[str, object]:
                response = await private._request(
                    "GET", "/fapi/v3/commissionRate", params={"symbol": symbol}
                )
                return parse_aster_evidence(response, now_ms=now_ms, symbol=symbol)

            return await _collect_symbol_scoped("aster", symbols, fetch_one)

        async def collect_binance() -> dict[str, object]:
            async def fetch_one(symbol: str) -> dict[str, object]:
                response = await adapters[Venue.BINANCE]._transport._request(
                    "GET",
                    "/fapi/v1/commissionRate",
                    params={"symbol": symbol},
                    private=True,
                )
                return parse_binance_evidence(response, now_ms=now_ms, symbol=symbol)

            return await _collect_symbol_scoped("binance", symbols, fetch_one)

        async def collect_bitget() -> dict[str, object]:
            async def fetch_one(symbol: str) -> dict[str, object]:
                response = await adapters[Venue.BITGET]._transport._request(
                    "GET",
                    "/api/v2/common/trade-rate",
                    params={"symbol": symbol, "businessType": "mix"},
                    private=True,
                )
                return parse_bitget_evidence(response, now_ms=now_ms, symbol=symbol)

            return await _collect_symbol_scoped("bitget", symbols, fetch_one)

        async def collect_bybit() -> dict[str, object]:
            transport = adapters[Venue.BYBIT]._transport
            async def fetch_one(symbol: str) -> dict[str, object]:
                fee = await transport._request(
                    "GET",
                    "/v5/account/fee-rate",
                    params={"category": "linear", "symbol": symbol},
                    private=True,
                )
                receipt_ms = max(now_ms, int(receipt_clock_ms()))
                return parse_bybit_evidence(
                    fee, now_ms=receipt_ms, symbol=symbol
                )

            return await _collect_symbol_scoped("bybit", symbols, fetch_one)

        async def collect_gate() -> dict[str, object]:
            transport = adapters[Venue.GATE]._transport

            async def fetch_one(symbol: str) -> dict[str, object]:
                venue_symbol = transport._venue_symbol(symbol)
                response = await transport._request(
                    "GET",
                    "/api/v4/futures/usdt/fee",
                    params={"contract": venue_symbol},
                    private=True,
                )
                return parse_gate_evidence(
                    response, now_ms=now_ms, symbol=venue_symbol
                )

            return await _collect_symbol_scoped("gate", symbols, fetch_one)

        async def collect_hyperliquid() -> dict[str, object]:
            adapter = adapters[Venue.HYPERLIQUID]
            credential = getattr(adapter, "_credential", None)
            account_address = str(
                getattr(credential, "account_address", "") or ""
            ).strip()
            if not account_address:
                raise ValueError("hyperliquid account address unavailable")
            response = await adapter._transport._request(
                "POST",
                "/info",
                body={"type": "userFees", "user": account_address},
                private=False,
            )
            result = parse_hyperliquid_evidence(response, now_ms=now_ms)
            return _aggregate_symbol_rows(
                "hyperliquid",
                {symbol: dict(result) for symbol in symbols},
            )

        async def collect_okx() -> dict[str, object]:
            transport = adapters[Venue.OKX]._transport
            venue_symbols = {
                symbol: transport._venue_symbol(symbol) for symbol in symbols
            }
            instruments = await transport._request(
                "GET",
                "/api/v5/account/instruments",
                params={"instType": "SWAP"},
                private=True,
            )
            group_by_symbol = parse_okx_instrument_groups(
                instruments, list(venue_symbols.values())
            )
            fee_by_group: dict[str, dict[str, object]] = {}
            receipt_by_group: dict[str, int] = {}
            for index, group_id in enumerate(sorted(set(group_by_symbol.values()))):
                if index:
                    # Official user-ID limit is five calls per two seconds.
                    await asyncio.sleep(0.41)
                fee_by_group[group_id] = await transport._request(
                    "GET",
                    "/api/v5/account/trade-fee",
                    params={"instType": "SWAP", "groupId": group_id},
                    private=True,
                )
                receipt_by_group[group_id] = max(now_ms, int(receipt_clock_ms()))
            rows = {
                symbol: parse_okx_evidence(
                    fee_by_group[group_by_symbol[venue_symbol]],
                    now_ms=receipt_by_group[group_by_symbol[venue_symbol]],
                    symbol=venue_symbol,
                    expected_group_id=group_by_symbol[venue_symbol],
                )
                for symbol, venue_symbol in venue_symbols.items()
                if venue_symbol in group_by_symbol
            }
            return _aggregate_symbol_rows("okx", rows)

        collectors = {
            Venue.ASTER: collect_aster,
            Venue.BINANCE: collect_binance,
            Venue.BITGET: collect_bitget,
            Venue.BYBIT: collect_bybit,
            Venue.GATE: collect_gate,
            Venue.HYPERLIQUID: collect_hyperliquid,
            Venue.OKX: collect_okx,
        }
        ordered = sorted(exact_venues, key=lambda value: value.value)
        results = await asyncio.gather(
            *(collectors[venue]() for venue in ordered), return_exceptions=True
        )
        rows: dict[str, dict[str, object]] = {}
        failures: dict[str, str] = {}
        for venue, result in zip(ordered, results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                failures[venue.value] = f"{type(result).__name__}:{result}"
            elif isinstance(result, BaseException):
                raise result
            else:
                rows[venue.value] = result
        return rows, failures, {venue.value for venue in exact_venues}
    finally:
        await asyncio.gather(
            *(adapter.shutdown() for adapter in adapters.values()),
            return_exceptions=True,
        )


def merge_evidence_rows(
    fresh: dict[str, dict[str, object]],
    previous: dict[str, dict[str, object]],
    *,
    requested: set[str],
    now_ms: int,
    max_age_ms: int,
    requested_symbols: set[str] | None = None,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    """Merge fresh rows with still-fresh venue and per-symbol last-good rows."""
    merged: dict[str, dict[str, object]] = {}
    reused: list[str] = []
    for venue in sorted(requested):
        fresh_row = fresh.get(venue)
        previous_row = previous.get(venue)
        fresh_symbols = (
            dict(fresh_row.get("symbol_schedules", {}))
            if isinstance(fresh_row, dict)
            and isinstance(fresh_row.get("symbol_schedules"), dict)
            else {}
        )
        previous_symbols = (
            dict(previous_row.get("symbol_schedules", {}))
            if isinstance(previous_row, dict)
            and isinstance(previous_row.get("symbol_schedules"), dict)
            else {}
        )
        if requested_symbols is not None:
            fresh_symbols = {
                symbol: row
                for symbol, row in fresh_symbols.items()
                if symbol in requested_symbols
            }
            previous_symbols = {
                symbol: row
                for symbol, row in previous_symbols.items()
                if symbol in requested_symbols
            }
        if fresh_symbols:
            symbol_rows = dict(fresh_symbols)
            reused_symbol = False
            for symbol, row in previous_symbols.items():
                if symbol in symbol_rows or not isinstance(row, dict):
                    continue
                try:
                    observed_at_ms = int(row.get("observed_at_ms", 0))
                except (TypeError, ValueError, OverflowError):
                    continue
                if (
                    observed_at_ms <= 0
                    or observed_at_ms > now_ms
                    or now_ms - observed_at_ms > max_age_ms
                ):
                    continue
                symbol_rows[str(symbol)] = dict(row)
                reused_symbol = True
            merged[venue] = _aggregate_symbol_rows(venue, symbol_rows)
            if reused_symbol:
                reused.append(venue)
            continue
        if previous_symbols:
            valid_previous_symbols: dict[str, dict[str, object]] = {}
            for symbol, row in previous_symbols.items():
                if not isinstance(row, dict):
                    continue
                try:
                    observed_at_ms = int(row.get("observed_at_ms", 0))
                except (TypeError, ValueError, OverflowError):
                    continue
                if (
                    observed_at_ms <= 0
                    or observed_at_ms > now_ms
                    or now_ms - observed_at_ms > max_age_ms
                ):
                    continue
                valid_previous_symbols[symbol] = dict(row)
            if valid_previous_symbols:
                merged[venue] = _aggregate_symbol_rows(
                    venue, valid_previous_symbols
                )
                reused.append(venue)
            continue
        if isinstance(fresh_row, dict):
            merged[venue] = dict(fresh_row)
            continue
        row = previous_row
        if not isinstance(row, dict):
            continue
        try:
            observed_at_ms = int(row.get("observed_at_ms", 0))
        except (TypeError, ValueError, OverflowError):
            continue
        if observed_at_ms <= 0 or observed_at_ms > now_ms or now_ms - observed_at_ms > max_age_ms:
            continue
        merged[venue] = dict(row)
        reused.append(venue)
    return merged, reused


def _previous_rows(path: Path, *, now_ms: int, max_age_ms: int) -> dict[str, dict[str, object]]:
    evidence = load_fee_evidence(path, now_ms=now_ms, max_age_ms=max_age_ms)
    if not evidence.loaded:
        return {}
    return {
        venue: {
            "taker_fee_bps": schedule.taker_fee_bps,
            "maker_fee_bps": schedule.maker_fee_bps,
            "observed_at_ms": schedule.observed_at_ms,
            "source": schedule.source,
            "evidence_ref": schedule.evidence_ref,
            "covered_symbols": list(schedule.covered_symbols),
            "symbol_schedules": {
                row.symbol: {
                    "taker_fee_bps": row.taker_fee_bps,
                    "maker_fee_bps": row.maker_fee_bps,
                    "observed_at_ms": row.observed_at_ms,
                    "evidence_ref": row.evidence_ref,
                }
                for row in schedule.symbol_schedules
            },
        }
        for venue, schedule in evidence.schedules.items()
    }


def _atomic_write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, 0o600)
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/live.toml")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    config = load_config(args.config)
    output = resolve_config_artifact_path(
        config,
        args.output or config.runtime.funding_fee_evidence_path,
    )
    now_ms = int(time.time() * 1000)
    fresh, failures, requested = asyncio.run(
        collect_evidence(args.config, now_ms=now_ms)
    )
    previous = _previous_rows(
        output,
        now_ms=now_ms,
        max_age_ms=int(config.runtime.funding_fee_evidence_max_age_ms),
    )
    venues, reused = merge_evidence_rows(
        fresh,
        previous,
        requested=requested,
        now_ms=now_ms,
        max_age_ms=int(config.runtime.funding_fee_evidence_max_age_ms),
        requested_symbols={
            str(symbol or "").strip().upper()
            for symbol in config.symbols
            if str(symbol or "").strip()
        },
    )
    if not venues:
        parser.error("all account-fee collectors failed and no fresh last-good row exists")
    payload = {
        "schema_version": LOCAL_FEE_EVIDENCE_SCHEMA_VERSION,
        "generated_at_ms": now_ms,
        "venues": venues,
    }
    _atomic_write(output, payload)
    print(
        json.dumps(
            {
                "output": str(output),
                "venues": sorted(venues),
                "refreshed": sorted(fresh),
                "reused": reused,
                "failed": failures,
                "observed_at_ms": {
                    venue: row["observed_at_ms"] for venue, row in venues.items()
                },
                "requested_symbols": sorted(
                    {
                        str(symbol or "").strip().upper()
                        for symbol in config.symbols
                        if str(symbol or "").strip()
                    }
                ),
                "covered_symbols_by_venue": {
                    venue: list(row.get("covered_symbols", []))
                    for venue, row in sorted(venues.items())
                },
                "missing_symbols_by_venue": {
                    venue: sorted(
                        {
                            str(symbol or "").strip().upper()
                            for symbol in config.symbols
                            if str(symbol or "").strip()
                        }
                        - set(row.get("covered_symbols", []))
                    )
                    for venue, row in sorted(venues.items())
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
