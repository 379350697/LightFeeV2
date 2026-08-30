"""OKX V5 adapter (OkxV5 unified account)."""

from __future__ import annotations

import math
import time
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.domain import (
    AccountFeeSnapshot,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.account_fees import fee_rate_from_mapping, first_mapping
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import okx_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class OkxAdapter(VenueAdapter):
    """OKX V5 unified account adapter."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = okx_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)

    @property
    def venue(self) -> Venue:
        return Venue.OKX

    @property
    def supports_risk_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_entry_leverage_preparation(self) -> bool:
        return True

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        venue_symbol = self._transport._venue_symbol(reference_symbol) if reference_symbol else ""
        if not venue_symbol:
            return None
        raw = await self._transport._request(
            "GET",
            "/api/v5/account/trade-fee",
            params={"instType": "SWAP", "instFamily": venue_symbol.removesuffix("-SWAP")},
            private=True,
        )
        if not isinstance(raw, dict) or str(raw.get("code", "0")) != "0":
            raise ValueError("OKX trade-fee request failed")
        row = first_mapping(raw.get("data"), "OKX trade-fee row")
        # OKX signs account fee rates as cash flow: negative is commission and
        # positive is rebate.  Final-L2 scoring uses the inverse convention:
        # positive cost and negative rebate.
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=-fee_rate_from_mapping(
                row, "maker fee", "maker", "makerU", "makerUSDC"
            ),
            taker_fee_bps=-fee_rate_from_mapping(
                row, "taker fee", "taker", "takerU", "takerUSDC"
            ),
            observed_at_ms=int(time.time() * 1000),
            source=f"okx_trade_fee:{venue_symbol.removesuffix('-SWAP')}",
        )

    def l2_book_quantity_to_base_scale(self, symbol: str) -> float | None:
        """Convert OKX local-book contract counts with cached ``ctVal``."""
        metadata_by_symbol = getattr(self._transport, "_symbol_metadata", {}) or {}
        keys = [self._transport._venue_symbol(symbol), symbol]
        for key in keys:
            metadata = metadata_by_symbol.get(key)
            if not isinstance(metadata, dict):
                continue
            ct_type = str(
                metadata.get(
                    "ctType",
                    metadata.get("ct_type", metadata.get("contractType", "")),
                )
                or ""
            )
            if not ct_type:
                return None
            for field in ("ct_val", "ctVal", "contract_size", "contractSize"):
                try:
                    value = float(metadata.get(field, 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if value > 0.0 and math.isfinite(value):
                    return value
            return None
        return None

    def supported_symbols(self) -> list[str]:
        """Return loaded OKX SWAP symbols, if the instrument catalog is available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        symbols: set[str] = set()
        for symbol in metadata:
            symbol_text = str(symbol)
            if not symbol_text:
                continue
            if "-SWAP" in symbol_text:
                symbols.add(okx_spec().symbol_from_venue(symbol_text))
            elif symbol_text.endswith("USDT"):
                symbols.add(symbol_text)
        return sorted(symbols)

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate OKX SWAP instrument metadata for recovery catalog gating."""
        if self._transport._symbol_metadata:
            return
        await self._transport._ensure_okx_swap_instrument_metadata_loaded()

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Require the exact OKX SWAP instrument to be currently ``live``."""
        venue_symbol = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "GET",
            "/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": venue_symbol},
            private=False,
        )
        if (
            not isinstance(raw, dict)
            or str(raw.get("code", "0")) != "0"
            or not isinstance(raw.get("data"), list)
        ):
            raise entry_tradability_unavailable(
                Venue.OKX.value,
                venue_symbol,
                "instruments_response_missing_or_unsuccessful",
            )
        row = next(
            (
                item
                for item in raw["data"]
                if isinstance(item, dict)
                and str(item.get("instId", "")).upper() == venue_symbol.upper()
            ),
            None,
        )
        if row is None:
            raise entry_tradability_blocked(
                Venue.OKX.value,
                venue_symbol,
                state="MISSING",
                inst_type="MISSING",
            )
        state = str(row.get("state", "")).lower()
        inst_type = str(row.get("instType", "")).upper()
        if state != "live" or inst_type != "SWAP":
            raise entry_tradability_blocked(
                Venue.OKX.value,
                venue_symbol,
                state=state or "MISSING",
                inst_type=inst_type or "MISSING",
            )
        return {
            "venue": Venue.OKX.value,
            "symbol": venue_symbol,
            "status": "ok",
            "instrument_state": state,
            "instrument_type": inst_type,
        }

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return await self._transport.fetch_position(symbol)

    async def fetch_account_risk_snapshot(self):
        return await self._transport.fetch_account_risk_snapshot()

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    @staticmethod
    def _okx_entry_position_sides(raw: Any) -> tuple[str, ...]:
        if (
            not isinstance(raw, dict)
            or str(raw.get("code", "")) != "0"
            or not isinstance(raw.get("data"), list)
            or not raw["data"]
            or not isinstance(raw["data"][0], dict)
        ):
            raise ValueError("OKX account config response is malformed")
        mode = str(raw["data"][0].get("posMode", "") or "").lower()
        if "long_short" in mode:
            return ("long", "short")
        if mode == "net_mode":
            return ("net",)
        raise ValueError(f"OKX account config position mode is unsupported: {mode or 'missing'}")

    @staticmethod
    def _okx_entry_leverages(
        raw: Any, venue_symbol: str, expected_sides: tuple[str, ...]
    ) -> dict[str, int]:
        if (
            not isinstance(raw, dict)
            or str(raw.get("code", "")) != "0"
            or not isinstance(raw.get("data"), list)
        ):
            raise ValueError("OKX leverage-info response is malformed")

        leverages: dict[str, int] = {}
        for row in raw["data"]:
            if not isinstance(row, dict):
                continue
            if str(row.get("instId", "")).upper() != venue_symbol.upper():
                continue
            if str(row.get("mgnMode", "")).lower() != "cross":
                continue
            side = str(row.get("posSide", "") or "").lower()
            if side not in expected_sides:
                continue
            try:
                leverage = float(row.get("lever"))
            except (TypeError, ValueError, OverflowError):
                raise ValueError("OKX leverage-info leverage is missing or invalid")
            if not math.isfinite(leverage) or leverage <= 0 or not leverage.is_integer():
                raise ValueError("OKX leverage-info leverage is missing or invalid")
            leverages[side] = int(leverage)

        if set(leverages) != set(expected_sides):
            raise ValueError(
                "OKX leverage-info is incomplete for position mode "
                f"expected={list(expected_sides)} actual={sorted(leverages)}"
            )
        return leverages

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        """Set and read back cross leverage for the account's exact OKX position mode."""
        target = int(leverage or 0)
        if target <= 0 or self._transport.mode != "live":
            return

        venue_symbol = self._transport._venue_symbol(symbol)
        payload: dict[str, Any] = {
            "venue": Venue.OKX.value,
            "symbol": venue_symbol,
            "requested_leverage": target,
            "requested_notional_quote": float(notional_quote or 0.0),
            "account_config_endpoint": "/api/v5/account/config",
            "leverage_info_endpoint": "/api/v5/account/leverage-info",
            "set_leverage_endpoint": "/api/v5/account/set-leverage",
        }
        try:
            config_raw = await self._transport._request(
                "GET", "/api/v5/account/config", private=True
            )
            sides = self._okx_entry_position_sides(config_raw)
            payload["position_sides"] = list(sides)

            async def readback() -> dict[str, int]:
                raw = await self._transport._request(
                    "GET",
                    "/api/v5/account/leverage-info",
                    params={"instId": venue_symbol, "mgnMode": "cross"},
                    private=True,
                )
                return self._okx_entry_leverages(raw, venue_symbol, sides)

            before = await readback()
            payload["before_leverages"] = dict(before)
            if all(value == target for value in before.values()):
                payload["outcome"] = "already_verified"
                self._transport._record_order_diagnostic("order.entry_leverage_ready", payload)
                return

            for side in sides:
                if before[side] == target:
                    continue
                body: dict[str, str] = {
                    "instId": venue_symbol,
                    "lever": str(target),
                    "mgnMode": "cross",
                }
                if side != "net":
                    body["posSide"] = side
                response = await self._transport._request(
                    "POST", "/api/v5/account/set-leverage", body=body, private=True
                )
                response_code = str(response.get("code", "") if isinstance(response, dict) else "")
                if response_code != "0":
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "OKX entry leverage set rejected "
                        f"symbol={venue_symbol} posSide={side} code={response_code or 'missing'}",
                    )

            after = await readback()
            payload["after_leverages"] = dict(after)
            if any(value != target for value in after.values()):
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "OKX entry leverage readback mismatch "
                    f"symbol={venue_symbol} expected={target} actual={after}",
                )
            payload["outcome"] = "set_and_verified"
            self._transport._record_order_diagnostic("order.entry_leverage_ready", payload)
        except OrderSubmitError:
            payload["outcome"] = "rejected"
            self._transport._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise
        except Exception as exc:
            payload["outcome"] = "error"
            payload["error"] = str(exc)[:300]
            self._transport._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                f"OKX entry leverage prepare failed: {exc}",
            ) from exc

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        return await self._transport.fetch_order_status(
            symbol,
            order_id=order_id,
            client_order_id=client_order_id or "",
        )

    async def shutdown(self) -> None:
        await self._transport.close()
