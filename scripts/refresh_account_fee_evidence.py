#!/usr/bin/env python3
"""Refresh signed account-fee evidence from available read-only private APIs.

Venues without an implemented account-fee endpoint remain eligible only for
the separately capped conservative canary tier.  This command never creates,
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
from typing import Any

from lightfee.config.loader import load_config
from lightfee.core.domain import Venue
from lightfee.strategy.fee_evidence import (
    FEE_EVIDENCE_SCHEMA_VERSION,
    TRUSTED_FEE_EVIDENCE_HMAC_ENV,
    TRUSTED_FEE_EVIDENCE_KEY_ID,
    sign_fee_evidence_payload,
)
from lightfee.venues.registry import build_adapter_map


ACCOUNT_FEE_API_VENUES = frozenset({Venue.BYBIT, Venue.OKX})


def _rate(value: object, *, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"invalid {field}")
    return parsed


def _identity_hash(value: object, *, venue: str) -> str:
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


def parse_bybit_evidence(
    fee_response: dict[str, Any],
    identity_response: dict[str, Any],
    *,
    now_ms: int,
) -> dict[str, object]:
    if fee_response.get("retCode") != 0 or identity_response.get("retCode") != 0:
        raise ValueError("bybit private API rejected evidence request")
    fee_rows = (fee_response.get("result") or {}).get("list") or []
    identity = identity_response.get("result") or {}
    if not isinstance(fee_rows, list) or len(fee_rows) != 1 or not isinstance(identity, dict):
        raise ValueError("invalid bybit fee evidence shape")
    row = fee_rows[0]
    if not isinstance(row, dict):
        raise ValueError("invalid bybit fee evidence row")
    taker = _rate(row.get("takerFeeRate"), field="bybit takerFeeRate")
    maker = _rate(row.get("makerFeeRate"), field="bybit makerFeeRate")
    if taker < 0.0:
        raise ValueError("invalid bybit takerFeeRate")
    observed_at_ms = _observed_at_ms(
        fee_response.get("time"), now_ms=now_ms, venue="bybit"
    )
    return {
        "taker_fee_bps": taker * 10_000.0,
        "maker_fee_bps": maker * 10_000.0,
        "observed_at_ms": observed_at_ms,
        "source": "account_fee_api",
        "evidence_ref": (
            f"bybit:/v5/account/fee-rate:{row.get('symbol') or 'linear'}:{observed_at_ms}"
        ),
        "account_identity_hash": _identity_hash(
            identity.get("userID"), venue="bybit"
        ),
    }


def parse_okx_evidence(
    fee_response: dict[str, Any],
    identity_response: dict[str, Any],
    *,
    now_ms: int,
) -> dict[str, object]:
    if str(fee_response.get("code")) != "0" or str(identity_response.get("code")) != "0":
        raise ValueError("okx private API rejected evidence request")
    fee_rows = fee_response.get("data") or []
    identity_rows = identity_response.get("data") or []
    if len(fee_rows) != 1 or len(identity_rows) != 1:
        raise ValueError("invalid okx fee evidence shape")
    row, identity = fee_rows[0], identity_rows[0]
    if not isinstance(row, dict) or not isinstance(identity, dict):
        raise ValueError("invalid okx fee evidence row")
    # OKX uses negative values for fees charged and positive values for
    # rebates.  The strategy contract uses positive values for costs.
    taker = -_rate(row.get("taker"), field="okx taker")
    maker = -_rate(row.get("maker"), field="okx maker")
    if taker < 0.0:
        raise ValueError("invalid okx taker fee sign")
    observed_at_ms = _observed_at_ms(
        row.get("ts"), now_ms=now_ms, venue="okx"
    )
    return {
        "taker_fee_bps": taker * 10_000.0,
        "maker_fee_bps": maker * 10_000.0,
        "observed_at_ms": observed_at_ms,
        "source": "account_fee_api",
        "evidence_ref": f"okx:/api/v5/account/trade-fee:SWAP:{observed_at_ms}",
        "account_identity_hash": _identity_hash(identity.get("uid"), venue="okx"),
    }


async def collect_evidence(config_path: str, *, now_ms: int) -> dict[str, object]:
    config = load_config(config_path)
    requested = {
        Venue.from_str(str(venue))
        for venue in config.strategy.funding_canary_allowed_venues
        if str(venue).strip()
    }
    exact_venues = requested & ACCOUNT_FEE_API_VENUES
    if not exact_venues:
        raise ValueError(
            "no configured canary venue has an account-fee API collector; "
            "conservative fee tiers do not produce signed account evidence"
        )
    adapters = build_adapter_map(config)
    missing = [venue.value for venue in exact_venues if venue not in adapters]
    if missing:
        raise ValueError("missing configured account-fee venues: " + ", ".join(missing))
    try:
        async def collect_bybit() -> dict[str, object]:
            transport = adapters[Venue.BYBIT]._transport
            fee, identity = await asyncio.gather(
                transport._request(
                    "GET",
                    "/v5/account/fee-rate",
                    params={"category": "linear", "symbol": "BTCUSDT"},
                    private=True,
                ),
                transport._request("GET", "/v5/user/query-api", private=True),
            )
            return parse_bybit_evidence(fee, identity, now_ms=now_ms)

        async def collect_okx() -> dict[str, object]:
            transport = adapters[Venue.OKX]._transport
            fee, identity = await asyncio.gather(
                transport._request(
                    "GET",
                    "/api/v5/account/trade-fee",
                    params={"instType": "SWAP"},
                    private=True,
                ),
                transport._request("GET", "/api/v5/account/config", private=True),
            )
            return parse_okx_evidence(fee, identity, now_ms=now_ms)

        collectors = {
            Venue.BYBIT: collect_bybit,
            Venue.OKX: collect_okx,
        }
        rows = await asyncio.gather(
            *(collectors[venue]() for venue in sorted(exact_venues, key=lambda v: v.value))
        )
        return {
            venue.value: row
            for venue, row in zip(
                sorted(exact_venues, key=lambda value: value.value), rows, strict=True
            )
        }
    finally:
        transports = {
            id(adapter._transport): adapter._transport for adapter in adapters.values()
        }
        await asyncio.gather(
            *(transport.close() for transport in transports.values()),
            return_exceptions=True,
        )


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
    output = Path(args.output or config.runtime.fee_evidence_path)
    secret = os.environ.get(TRUSTED_FEE_EVIDENCE_HMAC_ENV, "")
    if not secret:
        parser.error(f"{TRUSTED_FEE_EVIDENCE_HMAC_ENV} must be non-empty")
    now_ms = int(time.time() * 1000)
    venues = asyncio.run(collect_evidence(args.config, now_ms=now_ms))
    unsigned = {
        "schema_version": FEE_EVIDENCE_SCHEMA_VERSION,
        "generated_at_ms": now_ms,
        "venues": venues,
        "integrity": {
            "algorithm": "hmac-sha256",
            "key_id": TRUSTED_FEE_EVIDENCE_KEY_ID,
            "signature": "",
        },
    }
    signed = sign_fee_evidence_payload(unsigned, secret)
    _atomic_write(output, signed)
    print(
        json.dumps(
            {
                "output": str(output),
                "venues": sorted(venues),
                "observed_at_ms": {
                    venue: row["observed_at_ms"] for venue, row in venues.items()
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
