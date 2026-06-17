"""Entry dispatch runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change entry dispatch, order admission, or journal payload semantics while extracting it.
"""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from typing import Any

from lightfee.core.domain import OrderRequest, Side, TimeInForce, Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.entry import EntryContext, EntryType, normalize_opportunity_type
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    plan_incremental_entry_execution,
)
from lightfee.engine.recovery import (
    has_pending_entry_for_symbol,
    is_client_order_id_duplicate,
)
from lightfee.engine.runtime_context import EntryDispatchRuntimeContext
from lightfee.engine.v1_lifecycle import V1TradingLifecycle
from lightfee.venues.cid import generate_exchange_cid


class EntryDispatchRuntime:
    def __init__(self, ctx: EntryDispatchRuntimeContext) -> None:
        self.ctx = ctx

    def get_venue_adapter(self, *args: Any, **kwargs: Any):
        return self.ctx.get_venue_adapter(*args, **kwargs)

    def _safe_positive_float(self, *args: Any, **kwargs: Any):
        return self.ctx._safe_positive_float(*args, **kwargs)

    def _candidate_pair_id(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_pair_id(*args, **kwargs)

    def _entry_admission_reject_metadata(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_admission_reject_metadata(*args, **kwargs)

    def _record_symbol_admission_block(self, *args: Any, **kwargs: Any):
        return self.ctx._record_symbol_admission_block(*args, **kwargs)

    def _candidate_admission_block(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_admission_block(*args, **kwargs)

    def _candidate_is_tradeable_for_selection(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_is_tradeable_for_selection(*args, **kwargs)

    def _gate_reduce_only(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_reduce_only(*args, **kwargs)

    def _gate_pending_close_reconciliation(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_pending_close_reconciliation(*args, **kwargs)

    def _gate_passive_close_pending(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_passive_close_pending(*args, **kwargs)

    def _gate_recovery_ledger(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_recovery_ledger(*args, **kwargs)

    def _gate_pending_entry_dedup(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_pending_entry_dedup(*args, **kwargs)

    def _gate_entry_sizing(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_entry_sizing(*args, **kwargs)

    def _gate_venue_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_venue_cooldown(*args, **kwargs)

    def _gate_zero_fill_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._gate_zero_fill_cooldown(*args, **kwargs)

    def _entry_quote_lease_max_age_ms(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_quote_lease_max_age_ms(*args, **kwargs)

    def _quote_lease_blocker_family(self, *args: Any, **kwargs: Any):
        return self.ctx._quote_lease_blocker_family(*args, **kwargs)

    def _entry_readiness_provider_name(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_readiness_provider_name(*args, **kwargs)

    def _entry_readiness_provider_uses_quote_lease(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_readiness_provider_uses_quote_lease(*args, **kwargs)

    def _local_l2_effective_enabled(self, *args: Any, **kwargs: Any):
        return self.ctx._local_l2_effective_enabled(*args, **kwargs)

    def _post_only_maker_bbo_guard(self, *args: Any, **kwargs: Any):
        return self.ctx._post_only_maker_bbo_guard(*args, **kwargs)

    def _entry_reject_is_post_only_would_take(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_reject_is_post_only_would_take(*args, **kwargs)

    def _record_post_only_reject_cooldown(self, *args: Any, **kwargs: Any):
        return self.ctx._record_post_only_reject_cooldown(*args, **kwargs)

    def _record_entry_result_admission_blocks(self, candidate, reject_reason: str, now_ms: int) -> None:
        symbol = str(getattr(candidate, "symbol", "") or "")
        candidate_pair_id = self._candidate_pair_id(candidate)
        for raw_venue in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        ):
            try:
                venue = Venue.from_str(str(raw_venue))
            except ValueError:
                continue
            metadata = self._entry_admission_reject_metadata(venue, reject_reason)
            if metadata:
                reason = str(metadata["reason"])
                self._record_symbol_admission_block(
                    venue=venue,
                    symbol=symbol,
                    reason=reason,
                    raw_error=reject_reason,
                    now_ms=now_ms,
                    evidence=metadata,
                    source="initial_entry",
                    candidate_pair_id=candidate_pair_id,
                )

    async def _prepare_live_entry_leverage_for_candidate(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
    ) -> bool:
        mode = str(getattr(self.ctx.config.runtime, "mode", "") or "").lower()
        if mode != "live":
            return True
        try:
            target_leverage = int(
                getattr(self.ctx.config.strategy, "live_target_leverage", 0) or 0
            )
        except (TypeError, ValueError):
            target_leverage = 0
        if target_leverage <= 0:
            return True

        symbol = str(getattr(candidate, "symbol", "") or "")
        notional_quote = float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0)
        pair_id = self._candidate_pair_id(candidate)
        tasks: list[tuple[Venue, Any]] = []
        for venue in (long_venue, short_venue):
            if venue not in (Venue.BINANCE, Venue.ASTER):
                continue
            adapter = self.ctx.venue_adapters.get(venue)
            ensure = getattr(adapter, "ensure_entry_leverage", None) if adapter else None
            if callable(ensure):
                tasks.append((venue, ensure))
        if not tasks:
            return True

        async def _call(venue: Venue, ensure: Any) -> tuple[Venue, BaseException | None]:
            try:
                try:
                    await ensure(symbol, target_leverage, notional_quote=notional_quote)
                except TypeError as exc:
                    if "notional_quote" not in str(exc):
                        raise
                    await ensure(symbol, target_leverage)
                return venue, None
            except Exception as exc:
                return venue, exc

        results = await asyncio.gather(*[_call(venue, ensure) for venue, ensure in tasks])
        ok = True
        for venue, exc in results:
            if exc is None:
                self.ctx.journal.append(
                    "execution.entry_leverage_ready",
                    {
                        "venue": venue.value,
                        "symbol": symbol,
                        "target_leverage": target_leverage,
                        "entry_notional_quote": notional_quote,
                        "candidate_pair_id": pair_id,
                        "pair_id": pair_id,
                        "ts_ms": now_ms,
                    },
                )
                continue

            ok = False
            error_text = str(exc)
            metadata = self._entry_admission_reject_metadata(venue, error_text)
            if metadata:
                reason = str(metadata["reason"])
            else:
                reason = "entry_leverage_unavailable"
                metadata = {
                    "reason": reason,
                    "official_doc_url": "",
                    "evidence_gap": True,
                }
            self._record_symbol_admission_block(
                venue=venue,
                symbol=symbol,
                reason=reason,
                raw_error=error_text,
                now_ms=now_ms,
                evidence=metadata,
                source="entry_leverage_prepare",
                candidate_pair_id=pair_id,
            )
            self.ctx.journal.append(
                "execution.entry_leverage_unavailable",
                {
                    "venue": venue.value,
                    "symbol": symbol,
                    "target_leverage": target_leverage,
                    "entry_notional_quote": notional_quote,
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "reason": reason,
                    "raw_error": error_text[:500],
                    "official_doc_url": metadata.get("official_doc_url", ""),
                    "evidence_gap": bool(metadata.get("evidence_gap", True)),
                    "ts_ms": now_ms,
                },
            )
        if not ok:
            self.ctx.journal.append(
                "runtime.entry_blocked_gate",
                {
                    "symbol": symbol,
                    "gate": "entry_leverage_prepare",
                    "reason": "entry_leverage_unavailable",
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "ts_ms": now_ms,
                },
            )
        return ok

    async def _precheck_bybit_entry_admission(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        quantity: float,
        long_order_price_hint: float,
        short_order_price_hint: float,
        maker_venue: Venue,
        entry_type,
        maker_client_order_id: str,
        hedge_client_order_id: str,
    ) -> bool:
        if Venue.BYBIT not in (long_venue, short_venue):
            return True
        adapter = self.ctx.venue_adapters.get(Venue.BYBIT)
        precheck = getattr(adapter, "precheck_order_admission", None)
        if adapter is None or not callable(precheck):
            return True

        symbol = str(getattr(candidate, "symbol", "") or "")
        pair_id = self._candidate_pair_id(candidate)
        entry_type_value = str(getattr(entry_type, "value", entry_type) or "")
        bybit_is_maker = maker_venue == Venue.BYBIT
        passive = bybit_is_maker and "passive" in entry_type_value
        side = Side.BUY if long_venue == Venue.BYBIT else Side.SELL
        price_hint = (
            long_order_price_hint if long_venue == Venue.BYBIT else short_order_price_hint
        )
        client_order_id = (
            maker_client_order_id if bybit_is_maker else hedge_client_order_id
        )
        request = OrderRequest(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price_hint if passive and price_hint > 0 else None,
            reduce_only=False,
            client_order_id=client_order_id,
            post_only=passive,
            time_in_force=TimeInForce.POST_ONLY if passive else TimeInForce.IOC,
            price_hint=price_hint if price_hint > 0 else None,
            observed_at_ms=now_ms,
        )

        try:
            await precheck(request)
            return True
        except OrderSubmitError as exc:
            error_text = str(exc)
            metadata = self._entry_admission_reject_metadata(Venue.BYBIT, error_text)
            if metadata:
                reason = str(metadata["reason"])
                self._record_symbol_admission_block(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    reason=reason,
                    raw_error=error_text,
                    now_ms=now_ms,
                    evidence=metadata,
                    source="pre_entry_bybit_precheck",
                    candidate_pair_id=pair_id,
                )
                return False
            self.ctx.journal.append(
                "runtime.entry_admission_precheck_rejected",
                {
                    "venue": Venue.BYBIT.value,
                    "symbol": symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "raw_error": error_text[:500],
                    "ts_ms": now_ms,
                },
            )
            return False
        except Exception as exc:
            self.ctx.journal.append(
                "runtime.entry_admission_precheck_uncertain",
                {
                    "venue": Venue.BYBIT.value,
                    "symbol": symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "candidate_pair_id": pair_id,
                    "pair_id": pair_id,
                    "raw_error": str(exc)[:500],
                    "ts_ms": now_ms,
                },
            )
            return False

    async def _okx_entry_base_quantity_step(
        self, venue: Venue, symbol: str,
    ) -> float | None:
        if venue != Venue.OKX:
            return 0.0
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        explicit_step = self._safe_positive_float(
            getattr(adapter, "okx_base_quantity_step", 0.0)
        )
        if explicit_step > 0:
            return explicit_step

        transport = getattr(adapter, "_transport", None)
        if transport is None:
            return 0.0

        transport_step = self._safe_positive_float(
            getattr(transport, "okx_base_quantity_step", 0.0)
        )
        if transport_step > 0:
            return transport_step

        venue_symbol = symbol
        venue_symbol_fn = getattr(transport, "_venue_symbol", None)
        if callable(venue_symbol_fn):
            try:
                venue_symbol = venue_symbol_fn(symbol)
            except Exception:
                venue_symbol = symbol

        metadata = getattr(transport, "_symbol_metadata", {}) or {}
        for key in (symbol, venue_symbol):
            meta = metadata.get(key) or {}
            if not isinstance(meta, dict):
                continue
            ct_val = self._safe_positive_float(
                meta.get("ct_val") or meta.get("ctVal") or meta.get("contract_size")
            )
            lot_sz = self._safe_positive_float(
                meta.get("lot_sz") or meta.get("lotSz") or meta.get("qty_step")
            )
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz

        try:
            from lightfee.venues.symbol_rules import get_symbol_rules_cache

            rule = await get_symbol_rules_cache().get(transport, Venue.OKX, venue_symbol)
            ct_val = self._safe_positive_float(getattr(rule, "ct_val", 0.0))
            lot_sz = self._safe_positive_float(getattr(rule, "qty_step", 0.0))
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz
        except Exception:
            pass

        mode = str(getattr(transport, "mode", "") or "").lower()
        if mode == "live":
            return None
        return 0.0

    async def _okx_aligned_entry_quantity(
        self,
        *,
        long_venue: Venue,
        short_venue: Venue,
        symbol: str,
        quantity: float,
        now_ms: int,
    ) -> tuple[float, float | None]:
        okx_steps: list[float] = []
        missing = False
        for venue in (long_venue, short_venue):
            step = await self._okx_entry_base_quantity_step(venue, symbol)
            if step is None:
                missing = True
            elif step > 0:
                okx_steps.append(step)
        if missing:
            return 0.0, None
        if not okx_steps:
            return quantity, 0.0
        step = max(okx_steps)
        aligned = math.floor((quantity / step) + 1e-12) * step
        if aligned <= 0:
            return 0.0, step
        return aligned, step

    async def _entry_venue_quantity_step(
        self,
        venue: Venue,
        symbol: str,
    ) -> float | None:
        quantity_step, missing_fields = await self._entry_venue_quantity_metadata(
            venue,
            symbol,
        )
        if missing_fields:
            return None
        return quantity_step

    async def _entry_venue_quantity_metadata(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[float | None, list[str]]:
        okx_step = await self._okx_entry_base_quantity_step(venue, symbol)
        if okx_step is None:
            return None, ["okx_contract_step"]
        if okx_step > 0:
            return okx_step, []

        adapter = self.get_venue_adapter(venue)
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if callable(passive_metadata):
            try:
                metadata = passive_metadata(symbol) or {}
                if not metadata:
                    return None, ["metadata"]
                quantity_step = self._safe_positive_float(
                    metadata.get("quantity_step")
                    or metadata.get("step_size")
                    or metadata.get("qtyStep")
                )
                missing_fields: list[str] = []
                if quantity_step > 0:
                    for field_name, aliases in {
                        "min_quantity": ("min_quantity", "min_qty", "minOrderQty"),
                        "min_notional": (
                            "min_notional",
                            "min_notional_quote",
                            "minNotionalValue",
                        ),
                    }.items():
                        values = [
                            metadata.get(alias)
                            for alias in aliases
                            if alias in metadata
                        ]
                        if not values:
                            missing_fields.append(field_name)
                            continue
                        if field_name == "min_quantity":
                            min_quantity = self._safe_positive_float(values[0])
                            if min_quantity <= 0:
                                missing_fields.append(field_name)
                        else:
                            try:
                                min_notional = float(values[0] or 0.0)
                            except (TypeError, ValueError):
                                min_notional = -1.0
                            if not math.isfinite(min_notional) or min_notional < 0:
                                missing_fields.append(field_name)
                    return quantity_step, missing_fields
                missing_fields.append("quantity_step")
                return None, missing_fields
            except Exception:
                return None, ["metadata"]
        return None, ["metadata"]

    async def _entry_venue_quantity_metadata_evidence(
        self,
        venue: Venue,
        symbol: str,
        quantity_step: float | None,
        missing_fields: list[str] | None = None,
    ) -> dict:
        evidence = {
            "quantity_step": float(quantity_step or 0.0),
            "min_quantity": 0.0,
            "min_notional": 0.0,
            "missing_fields": list(missing_fields or []),
            "source": "entry_venue_quantity_metadata",
        }
        okx_step = await self._okx_entry_base_quantity_step(venue, symbol)
        if okx_step and okx_step > 0:
            evidence["quantity_step"] = float(okx_step)
            evidence["source"] = "okx_contract_step"
            return evidence
        adapter = self.get_venue_adapter(venue)
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if not callable(passive_metadata):
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        try:
            metadata = passive_metadata(symbol) or {}
        except Exception:
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        if not isinstance(metadata, dict) or not metadata:
            if "metadata" not in evidence["missing_fields"]:
                evidence["missing_fields"].append("metadata")
            return evidence
        evidence["quantity_step"] = float(
            self._safe_positive_float(
                metadata.get("quantity_step")
                or metadata.get("step_size")
                or metadata.get("qtyStep")
            )
            or evidence["quantity_step"]
        )
        evidence["min_quantity"] = float(
            self._safe_positive_float(
                metadata.get("min_quantity")
                or metadata.get("min_qty")
                or metadata.get("minOrderQty")
            )
        )
        try:
            min_notional = float(
                metadata.get("min_notional")
                or metadata.get("min_notional_quote")
                or metadata.get("minNotionalValue")
                or 0.0
            )
        except (TypeError, ValueError):
            min_notional = 0.0
        evidence["min_notional"] = min_notional if math.isfinite(min_notional) else 0.0
        return evidence

    @staticmethod
    def _entry_quantity_plan_reason(
        *,
        raw_quantity: float,
        common_quantity: float,
        full_target_quantity: float,
        initial_maker_target_quantity: float,
    ) -> str:
        if abs(common_quantity - raw_quantity) > 1e-9:
            return "exchange_step_rounding"
        if abs(common_quantity - full_target_quantity) > 1e-9:
            return "planner_quantity_adjustment"
        if abs(initial_maker_target_quantity - full_target_quantity) > 1e-9:
            return "passive_initial_slice"
        return "full_target_quantity"

    def _entry_quote_lease_execution_check(
        self,
        candidate,
        now_ms: int,
    ) -> tuple[str, object | None, dict]:
        provider_name = self._entry_readiness_provider_name()
        evidence = {
            "provider": provider_name,
            "source": provider_name,
            "domain": "quote_lease_execution_gate",
            "pair_id": self._candidate_pair_id(candidate),
            "symbol": str(getattr(candidate, "symbol", "")),
            "long_venue": str(getattr(candidate, "long_venue", "")),
            "short_venue": str(getattr(candidate, "short_venue", "")),
            "max_age_ms": self._entry_quote_lease_max_age_ms(),
        }
        def blocked(reason: str, lease: object | None):
            evidence["blocker_family"] = self._quote_lease_blocker_family(reason)
            return reason, lease, evidence

        if (
            self.ctx.config.runtime.mode != "live"
            or not self._entry_readiness_provider_uses_quote_lease()
        ):
            return "", None, evidence

        get_lease = getattr(self.ctx.entry_readiness_provider, "get_lease", None)
        if not callable(get_lease):
            return blocked("missing_quote_lease_provider", None)
        lease = get_lease(evidence["pair_id"])
        if lease is None:
            return blocked("missing_quote_lease", None)

        evidence.update(
            {
                "lease_provider": str(getattr(lease, "provider", "")),
                "created_at_ms": int(getattr(lease, "created_at_ms", 0) or 0),
                "expires_at_ms": int(getattr(lease, "expires_at_ms", 0) or 0),
                "long_observed_at_ms": int(
                    getattr(lease, "long_observed_at_ms", 0) or 0
                ),
                "short_observed_at_ms": int(
                    getattr(lease, "short_observed_at_ms", 0) or 0
                ),
                "long_bid": float(getattr(lease, "long_bid", 0.0) or 0.0),
                "long_ask": float(getattr(lease, "long_ask", 0.0) or 0.0),
                "short_bid": float(getattr(lease, "short_bid", 0.0) or 0.0),
                "short_ask": float(getattr(lease, "short_ask", 0.0) or 0.0),
            }
        )
        if evidence["lease_provider"] != provider_name:
            return blocked("quote_lease_provider_mismatch", lease)
        if str(getattr(lease, "symbol", "")) != evidence["symbol"]:
            return blocked("quote_lease_symbol_mismatch", lease)
        if str(getattr(lease, "long_venue", "")) != evidence["long_venue"]:
            return blocked("quote_lease_long_venue_mismatch", lease)
        if str(getattr(lease, "short_venue", "")) != evidence["short_venue"]:
            return blocked("quote_lease_short_venue_mismatch", lease)

        expires_at_ms = evidence["expires_at_ms"]
        if expires_at_ms <= 0 or now_ms >= expires_at_ms:
            return blocked("expired_quote_lease", lease)

        max_age_ms = int(evidence["max_age_ms"] or 0)
        quote_age_ms: dict[str, int | None] = {}
        for leg in ("long", "short"):
            observed_at_ms = int(evidence[f"{leg}_observed_at_ms"] or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            evidence[f"{leg}_age_ms"] = age_ms
            quote_age_ms[leg] = age_ms
        evidence["quote_age_ms"] = quote_age_ms
        for leg in ("long", "short"):
            observed_at_ms = int(evidence[f"{leg}_observed_at_ms"] or 0)
            age_ms = evidence[f"{leg}_age_ms"]
            if (
                observed_at_ms <= 0
                or max_age_ms <= 0
                or age_ms is None
                or age_ms > max_age_ms
            ):
                return blocked("stale_quote_lease", lease)

        if (
            evidence["long_bid"] <= 0.0
            or evidence["long_ask"] <= evidence["long_bid"]
            or evidence["short_bid"] <= 0.0
            or evidence["short_ask"] <= evidence["short_bid"]
        ):
            return blocked("invalid_quote_lease", lease)
        return "", lease, evidence

    def _refresh_entry_quote_lease_for_execution(
        self,
        candidate,
        now_ms: int,
        quote_lease_reason: str,
        quote_lease: object | None,
        quote_lease_evidence: dict,
    ) -> tuple[str, object | None, dict]:
        if quote_lease_reason not in {"expired_quote_lease", "stale_quote_lease"}:
            return quote_lease_reason, quote_lease, quote_lease_evidence
        if self._entry_readiness_provider_name() != "ws_bbo_quote_lease":
            return quote_lease_reason, quote_lease, quote_lease_evidence

        decide = getattr(self.ctx.entry_readiness_provider, "decide", None)
        if not callable(decide):
            return quote_lease_reason, quote_lease, quote_lease_evidence

        refresh_evidence = dict(quote_lease_evidence)
        refresh_evidence["execution_refresh_attempted"] = True
        refresh_evidence["execution_refresh_reason"] = quote_lease_reason
        try:
            decision = decide(candidate, now_ms)
        except Exception as exc:
            refresh_evidence["execution_refresh_error"] = (
                f"{type(exc).__name__}: {str(exc)[:300]}"
            )
            return quote_lease_reason, quote_lease, refresh_evidence

        if not getattr(decision, "allowed", False):
            refresh_evidence["execution_refresh_block_reason"] = str(
                getattr(decision, "reason", "")
            )
            refresh_evidence["execution_refresh_evidence"] = dict(
                getattr(decision, "evidence", {}) or {}
            )
            return quote_lease_reason, quote_lease, refresh_evidence

        new_reason, new_lease, new_evidence = self._entry_quote_lease_execution_check(
            candidate,
            now_ms,
        )
        new_evidence = dict(new_evidence)
        new_evidence["execution_refresh_attempted"] = True
        new_evidence["execution_refresh_reason"] = quote_lease_reason
        return new_reason, new_lease, new_evidence

    @staticmethod
    def _quote_lease_reference_price(lease) -> float:
        long_ask = float(getattr(lease, "long_ask", 0.0) or 0.0)
        short_bid = float(getattr(lease, "short_bid", 0.0) or 0.0)
        if long_ask > 0.0 and short_bid > 0.0:
            return (long_ask + short_bid) / 2.0
        return max(long_ask, short_bid, 0.0)

    def _entry_final_gate_skew_blocker(
        self,
        candidate,
        *,
        long_venue,
        short_venue,
        now_ms: int,
    ) -> dict | None:
        if (
            self.ctx.config.runtime.mode != "live"
            or not self._local_l2_effective_enabled()
        ):
            return None
        long_book = self.ctx.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue.value, candidate.symbol)
        if long_book is None or short_book is None:
            return None
        long_observed_at_ms = int(getattr(long_book, "observed_at_ms", 0) or 0)
        short_observed_at_ms = int(getattr(short_book, "observed_at_ms", 0) or 0)
        if long_observed_at_ms <= 0 or short_observed_at_ms <= 0:
            return None
        max_skew_ms = max(
            int(getattr(self.ctx.config.strategy, "entry_final_gate_max_skew_ms", 0) or 0),
            0,
        )
        skew_ms = abs(long_observed_at_ms - short_observed_at_ms)
        if skew_ms <= max_skew_ms:
            return None
        return {
            "pair_id": self._candidate_pair_id(candidate),
            "symbol": candidate.symbol,
            "long_venue": long_venue.value,
            "short_venue": short_venue.value,
            "reason": "execution_skew",
            "skew_ms": skew_ms,
            "max_skew_ms": max_skew_ms,
            "left_venue": long_venue.value,
            "left_observed_at_ms": long_observed_at_ms,
            "right_venue": short_venue.value,
            "right_observed_at_ms": short_observed_at_ms,
            "ts_ms": now_ms,
        }

    def _entry_initial_gate_blocked(self, candidate, now_ms: int) -> bool:
        admission_block = self._candidate_admission_block(candidate, now_ms)
        if admission_block:
            pair_id = self._candidate_pair_id(candidate)
            payload = {
                **admission_block,
                "long_venue": getattr(candidate, "long_venue", ""),
                "short_venue": getattr(candidate, "short_venue", ""),
                "candidate_pair_id": pair_id,
                "pair_id": pair_id,
                "ts_ms": now_ms,
            }
            if payload.get("source"):
                payload["cooldown_source"] = payload["source"]
            payload["source"] = "initial_entry"
            self.ctx.journal.append(
                "runtime.entry_admission_blocked",
                payload,
            )
            return True

        if not self._candidate_is_tradeable_for_selection(candidate):
            self.ctx.journal.append(
                "runtime.entry_blocked_trading_capability",
                {
                    "symbol": getattr(candidate, "symbol", ""),
                    "long_venue": getattr(candidate, "long_venue", ""),
                    "short_venue": getattr(candidate, "short_venue", ""),
                    "reason": "candidate_not_tradeable_for_selection",
                    "ts_ms": now_ms,
                },
            )
            return True

        decision = V1TradingLifecycle.entry_admissibility(
            candidate,
            now_ms=now_ms,
            strategy=self.ctx.config.strategy,
            recovery_ledger=getattr(self, "recovery_ledger", None),
            source="dispatch",
        )
        if not decision.allowed:
            self.ctx.journal.append(
                "runtime.entry_blocked_lifecycle",
                {
                    "symbol": getattr(candidate, "symbol", ""),
                    "long_venue": getattr(candidate, "long_venue", ""),
                    "short_venue": getattr(candidate, "short_venue", ""),
                    "reason": decision.reason,
                    **dict(getattr(decision, "evidence", {}) or {}),
                    "ts_ms": now_ms,
                },
            )
            return True

        # V1: apply_runtime_entry_guards — 8+ gate checks before entry
        gates = [
            ("reduce_only", self._gate_reduce_only, ()),
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation, ()),
            ("passive_close_in_flight", self._gate_passive_close_pending, ()),
            ("recovery_ledger", self._gate_recovery_ledger, ()),
            ("pending_entry_duplicate", self._gate_pending_entry_dedup, ()),
            ("entry_sizing", self._gate_entry_sizing, ()),
            ("venue_cooldown", self._gate_venue_cooldown, (now_ms,)),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown, (now_ms,)),
        ]
        for gate_name, gate_fn, gate_args in gates:
            allowed, reason = gate_fn(candidate, *gate_args)
            if not allowed:
                self.ctx.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                # V1: review.candidate_rejected — per-candidate rejection logging
                self.ctx.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": candidate.long_venue,
                        "short_venue": candidate.short_venue,
                        "rejected_stage": "runtime_entry_gate",
                        "rejected_reason": f"{gate_name}: {reason}",
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
                    },
                )
                return True
        return False

    def _entry_price_resolution(
        self,
        candidate,
        now_ms: int,
        price_hint: float,
    ) -> tuple[float, float, float, Any] | None:
        quote_lease = None
        quote_lease_evidence: dict = {}
        quote_lease_reason, quote_lease, quote_lease_evidence = (
            self._entry_quote_lease_execution_check(candidate, now_ms)
        )
        if quote_lease_reason:
            quote_lease_reason, quote_lease, quote_lease_evidence = (
                self._refresh_entry_quote_lease_for_execution(
                    candidate,
                    now_ms,
                    quote_lease_reason,
                    quote_lease,
                    quote_lease_evidence,
                )
            )
        if quote_lease_reason:
            payload = {
                **quote_lease_evidence,
                "reason": quote_lease_reason,
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_blocked_quote_lease", payload)
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "rejected_stage": "quote_lease_execution_gate",
                    "rejected_reason": quote_lease_reason,
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return None
        long_order_price_hint = price_hint
        short_order_price_hint = price_hint
        if quote_lease is not None:
            price_hint = self._quote_lease_reference_price(quote_lease)
            long_order_price_hint = float(
                getattr(quote_lease, "long_ask", 0.0) or 0.0
            )
            short_order_price_hint = float(
                getattr(quote_lease, "short_bid", 0.0) or 0.0
            )

        # V1 price gate: require valid quote before constructing entry context
        if price_hint <= 0 or candidate.entry_notional_quote <= 0:
            self.ctx.journal.append(
                "runtime.entry_skipped_no_quote",
                {
                    "symbol": candidate.symbol,
                    "price_hint": price_hint,
                    "notional": candidate.entry_notional_quote,
                    "reason": "no valid quote to construct entry — V1 rejects",
                },
            )
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "rejected_stage": "price_gate",
                    "rejected_reason": "no valid quote",
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return None
        return price_hint, long_order_price_hint, short_order_price_hint, quote_lease

    async def _resolve_entry_quantity_steps(
        self,
        *,
        candidate,
        long_venue: Venue,
        short_venue: Venue,
        price_hint: float,
        now_ms: int,
    ) -> tuple[float, float, float, float | None, float | None] | None:
        raw_quantity = candidate.entry_notional_quote / price_hint
        quantity = raw_quantity
        quantity, okx_base_step = await self._okx_aligned_entry_quantity(
            long_venue=long_venue,
            short_venue=short_venue,
            symbol=candidate.symbol,
            quantity=quantity,
            now_ms=now_ms,
        )
        if okx_base_step is None:
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_metadata_missing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "raw_quantity": raw_quantity,
                    "reason": "okx_ct_val_lot_sz_unconfirmed",
                    "ts_ms": now_ms,
                },
            )
            return None
        if quantity <= 0:
            self.ctx.journal.append(
                "runtime.entry_skipped_okx_contract_step",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "okx_base_quantity_step": okx_base_step,
                    "raw_quantity": raw_quantity,
                    "reason": "quantity_below_okx_contract_step",
                    "ts_ms": now_ms,
                },
            )
            return None

        long_quantity_step, long_missing_quantity_fields = await self._entry_venue_quantity_metadata(
            long_venue,
            candidate.symbol,
        )
        short_quantity_step, short_missing_quantity_fields = await self._entry_venue_quantity_metadata(
            short_venue,
            candidate.symbol,
        )
        if (
            long_quantity_step is None
            or short_quantity_step is None
            or long_missing_quantity_fields
            or short_missing_quantity_fields
        ):
            missing_venues = []
            missing_fields = {}
            if long_quantity_step is None:
                missing_venues.append(long_venue.value)
            elif long_missing_quantity_fields:
                missing_venues.append(long_venue.value)
            if short_quantity_step is None:
                missing_venues.append(short_venue.value)
            elif short_missing_quantity_fields:
                missing_venues.append(short_venue.value)
            if long_quantity_step is None or long_missing_quantity_fields:
                missing_fields[long_venue.value] = (
                    long_missing_quantity_fields or ["quantity_step"]
                )
            if short_quantity_step is None or short_missing_quantity_fields:
                missing_fields[short_venue.value] = (
                    short_missing_quantity_fields or ["quantity_step"]
                )
            self.ctx.journal.append(
                "runtime.entry_skipped_quantity_metadata_missing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "missing_venues": missing_venues,
                    "missing_fields": missing_fields,
                    "raw_quantity": raw_quantity,
                    "common_quantity": quantity,
                    "reason": "quantity_metadata_missing",
                    "ts_ms": now_ms,
                },
            )
            return None
        return raw_quantity, quantity, okx_base_step, long_quantity_step, short_quantity_step

    def _entry_local_l2_gate_blocked(
        self,
        *,
        candidate,
        long_venue: Venue,
        short_venue: Venue,
        now_ms: int,
    ) -> bool:
        # V1 local-L2 entry readiness gate: block entry when local-L2 enabled
        # but either leg's book is not ready (stale, degraded, cold, etc.)
        if not self._local_l2_effective_enabled():
            return False

        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2

        long_book = self.ctx.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
        short_book = self.ctx.local_l2_runtime.get_book(short_venue.value, candidate.symbol)

        not_ready_reasons: list[str] = []
        l2_stale_decisions: list[dict] = []
        max_age_ms = self.ctx.config.strategy.max_liquidity_snapshot_age_ms
        if long_book is None:
            not_ready_reasons.append(
                f"long book missing: {long_venue.value}:{candidate.symbol} "
                f"max_age_ms={max_age_ms}"
            )
            l2_stale_decisions.append({
                "venue": long_venue.value,
                "symbol": candidate.symbol,
                "domain": "execution_l2",
                "source": "local_l2",
                "observed_at_ms": 0,
                "age_ms": 0,
                "budget_ms": max_age_ms,
                "decision": "skip_entry",
                "fallback_source": "none",
                "reason": "execution_l2_stale",
                "l2_reason": "missing_book",
                "blocking": True,
            })
        else:
            liq = execution_liquidity_from_local_l2(
                long_book, max_age_ms=max_age_ms,
                now_ms=now_ms, require_ready=True,
            )
            long_age_ms = long_book.age_ms(now_ms)
            if not liq.book_ready:
                not_ready_reasons.append(
                    f"long leg not ready: {long_venue.value}:{candidate.symbol} "
                    f"status={long_book.status.value} pool={long_book.pool.value if hasattr(long_book, 'pool') else 'unknown'} "
                    f"age={long_age_ms}ms max_age_ms={max_age_ms}"
                )
                l2_stale_decisions.append({
                    "venue": long_venue.value,
                    "symbol": candidate.symbol,
                    "domain": "execution_l2",
                    "source": "local_l2",
                    "observed_at_ms": int(getattr(long_book, "observed_at_ms", 0) or 0),
                    "age_ms": int(long_age_ms),
                    "budget_ms": max_age_ms,
                    "decision": "skip_entry",
                    "fallback_source": "none",
                    "reason": "execution_l2_stale",
                    "l2_reason": liq.fallback_reason or "book_not_ready",
                    "book_status": long_book.status.value,
                    "blocking": True,
                })

        if short_book is None:
            not_ready_reasons.append(
                f"short book missing: {short_venue.value}:{candidate.symbol} "
                f"max_age_ms={max_age_ms}"
            )
            l2_stale_decisions.append({
                "venue": short_venue.value,
                "symbol": candidate.symbol,
                "domain": "execution_l2",
                "source": "local_l2",
                "observed_at_ms": 0,
                "age_ms": 0,
                "budget_ms": max_age_ms,
                "decision": "skip_entry",
                "fallback_source": "none",
                "reason": "execution_l2_stale",
                "l2_reason": "missing_book",
                "blocking": True,
            })
        else:
            liq = execution_liquidity_from_local_l2(
                short_book, max_age_ms=max_age_ms,
                now_ms=now_ms, require_ready=True,
            )
            short_age_ms = short_book.age_ms(now_ms)
            if not liq.book_ready:
                not_ready_reasons.append(
                    f"short leg not ready: {short_venue.value}:{candidate.symbol} "
                    f"status={short_book.status.value} pool={short_book.pool.value if hasattr(short_book, 'pool') else 'unknown'} "
                    f"age={short_age_ms}ms max_age_ms={max_age_ms}"
                )
                l2_stale_decisions.append({
                    "venue": short_venue.value,
                    "symbol": candidate.symbol,
                    "domain": "execution_l2",
                    "source": "local_l2",
                    "observed_at_ms": int(getattr(short_book, "observed_at_ms", 0) or 0),
                    "age_ms": int(short_age_ms),
                    "budget_ms": max_age_ms,
                    "decision": "skip_entry",
                    "fallback_source": "none",
                    "reason": "execution_l2_stale",
                    "l2_reason": liq.fallback_reason or "book_not_ready",
                    "book_status": short_book.status.value,
                    "blocking": True,
                })

        if not_ready_reasons:
            pair_id = self._candidate_pair_id(candidate)
            for payload in l2_stale_decisions:
                payload = dict(payload)
                payload["pair_id"] = pair_id
                payload["ts_ms"] = now_ms
                self.ctx.journal.append("runtime.snapshot_freshness_decision", payload)
                self.ctx.journal.append("runtime.execution_l2_stale", payload)
            self.ctx.journal.append(
                "runtime.entry_blocked_local_l2_not_ready",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reasons": not_ready_reasons,
                    "ts_ms": now_ms,
                },
            )
            return True

        skew_blocker = self._entry_final_gate_skew_blocker(
            candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            now_ms=now_ms,
        )
        if skew_blocker is not None:
            self.ctx.journal.append("runtime.entry_blocked_final_gate", skew_blocker)
            self.ctx.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "rejected_stage": "entry_final_gate",
                    "rejected_reason": skew_blocker["reason"],
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "expected_edge_bps": candidate.expected_edge_bps,
                    "funding_edge_bps": candidate.funding_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return True
        return False

    def _build_entry_context(
        self,
        *,
        candidate,
        entry_id: str,
        long_venue: Venue,
        short_venue: Venue,
        effective_quantity: float,
        long_order_price_hint: float,
        short_order_price_hint: float,
        maker_leg: Side,
        entry_type: EntryType,
        route: ExecutionRoute,
        now_ms: int,
    ) -> EntryContext:
        def _positive_ms(value) -> int:
            try:
                parsed = int(value or 0)
            except (TypeError, ValueError):
                return 0
            return parsed if parsed > 0 else 0

        def _float_attr(name: str, default: float = 0.0) -> float:
            try:
                return float(getattr(candidate, name, default) or default)
            except (TypeError, ValueError):
                return default

        def _str_attr(name: str, default: str = "") -> str:
            return str(getattr(candidate, name, default) or default)

        opportunity_type = normalize_opportunity_type(
            str(getattr(candidate, "opportunity_type", "aligned") or "aligned")
        )
        long_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "long_funding_timestamp_ms", 0)
        )
        short_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "short_funding_timestamp_ms", 0)
        )
        funding_timestamp_ms = _positive_ms(getattr(candidate, "funding_timestamp_ms", 0))
        first_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "first_funding_timestamp_ms", 0)
        )
        if first_funding_timestamp_ms <= 0 and (long_funding_timestamp_ms > 0 or short_funding_timestamp_ms > 0):
            first_funding_timestamp_ms = min(
                ts for ts in (long_funding_timestamp_ms, short_funding_timestamp_ms)
                if ts > 0
            )
        if funding_timestamp_ms <= 0:
            funding_timestamp_ms = first_funding_timestamp_ms
        second_funding_timestamp_ms = _positive_ms(
            getattr(candidate, "second_funding_timestamp_ms", 0)
        )
        if (
            second_funding_timestamp_ms <= 0
            and opportunity_type == "staggered"
            and long_funding_timestamp_ms > 0
            and short_funding_timestamp_ms > 0
        ):
            later_funding_ms = max(long_funding_timestamp_ms, short_funding_timestamp_ms)
            if later_funding_ms > first_funding_timestamp_ms:
                second_funding_timestamp_ms = later_funding_ms
        funding_edge_bps_entry = float(getattr(candidate, "funding_edge_bps", 0.0) or 0.0)
        total_funding_edge_bps_entry = float(
            getattr(candidate, "total_funding_edge_bps", 0.0) or funding_edge_bps_entry
        )
        expected_edge_bps_entry = float(getattr(candidate, "expected_edge_bps", 0.0) or 0.0)
        worst_case_edge_bps_entry = _float_attr("worst_case_edge_bps")
        first_funding_leg = str(getattr(candidate, "first_funding_leg", "") or "")
        entry_maker_leg = _str_attr(
            "entry_maker_leg",
            "long" if maker_leg == Side.BUY else "short",
        )
        entry_liquidity_source_at_entry = (
            getattr(candidate, "entry_liquidity_source_at_entry", None)
            or _str_attr("sizing_liquidity_source")
            or None
        )
        exit_after_first_stage = (
            opportunity_type == "staggered"
            and str(getattr(self.ctx.config.strategy, "staggered_exit_mode", "") or "").lower()
            == "after_first_stage"
        )

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=candidate.symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_quantity=effective_quantity,
            short_quantity=effective_quantity,
            long_price_hint=long_order_price_hint,
            short_price_hint=short_order_price_hint,
            maker_leg=maker_leg,
            entry_type=entry_type,
            created_at_ms=now_ms,
            opportunity_type=opportunity_type,
            funding_timestamp_ms=funding_timestamp_ms,
            first_funding_timestamp_ms=first_funding_timestamp_ms,
            long_funding_timestamp_ms=long_funding_timestamp_ms,
            short_funding_timestamp_ms=short_funding_timestamp_ms,
            second_funding_timestamp_ms=second_funding_timestamp_ms,
            first_funding_leg=first_funding_leg,
            funding_edge_bps_entry=funding_edge_bps_entry,
            total_funding_edge_bps_entry=total_funding_edge_bps_entry,
            expected_edge_bps_entry=expected_edge_bps_entry,
            worst_case_edge_bps_entry=worst_case_edge_bps_entry,
            entry_maker_leg=entry_maker_leg,
            exit_maker_leg=_str_attr("exit_maker_leg"),
            entry_cross_bps_entry=_float_attr("entry_cross_bps"),
            fee_bps_entry=_float_attr("fee_bps"),
            entry_slippage_bps_entry=_float_attr("entry_slippage_bps"),
            transfer_bias_bps_entry=_float_attr("transfer_bias_bps"),
            transfer_state_at_entry=getattr(candidate, "transfer_state_at_entry", None),
            entry_liquidity_source_at_entry=entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=_float_attr("long_volume_24h_quote"),
            short_volume_24h_quote_at_entry=_float_attr("short_volume_24h_quote"),
            long_open_interest_quote_at_entry=_float_attr("long_open_interest_quote_at_entry"),
            short_open_interest_quote_at_entry=_float_attr("short_open_interest_quote_at_entry"),
            long_entry_vwap=getattr(candidate, "long_entry_vwap", None),
            short_entry_vwap=getattr(candidate, "short_entry_vwap", None),
            entry_capacity_constrained=bool(
                getattr(candidate, "entry_capacity_constrained", False)
            ),
            entry_target_quantity=_float_attr("entry_target_quantity"),
            long_max_executable_quantity=_float_attr("long_max_executable_quantity"),
            short_max_executable_quantity=_float_attr("short_max_executable_quantity"),
            entry_max_executable_quantity=_float_attr("entry_max_executable_quantity"),
            entry_depth_shortfall_quantity=_float_attr("entry_depth_shortfall_quantity"),
            entry_max_executable_notional_quote=_float_attr(
                "entry_max_executable_notional_quote"
            ),
            entry_depth_capped_at_entry=bool(
                getattr(candidate, "entry_depth_capped_at_entry", False)
            ),
            advisories=list(getattr(candidate, "advisories", []) or []),
            blocked_reasons=list(getattr(candidate, "blocked_reasons", []) or []),
            exit_after_first_stage=exit_after_first_stage,
        )
        candidate_pair_id = self._candidate_pair_id(candidate)

        # V1: review.candidate_shortlisted — candidate passed all gates, entered shortlist
        self.ctx.journal.append(
            "review.candidate_shortlisted",
            {
                "entry_id": entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "ranking_edge_bps": candidate.ranking_edge_bps,
                "expected_edge_bps": candidate.expected_edge_bps,
                "funding_edge_bps": candidate.funding_edge_bps,
                "worst_case_edge_bps": candidate.worst_case_edge_bps,
                "opportunity_type": opportunity_type,
                "funding_timestamp_ms": funding_timestamp_ms,
                "first_funding_timestamp_ms": first_funding_timestamp_ms,
                "long_funding_timestamp_ms": long_funding_timestamp_ms,
                "short_funding_timestamp_ms": short_funding_timestamp_ms,
                "second_funding_timestamp_ms": second_funding_timestamp_ms,
                "first_funding_leg": first_funding_leg,
                "exit_after_first_stage": exit_after_first_stage,
                "entry_notional_quote": candidate.entry_notional_quote,
                "route": route.value,
                "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                "ts_ms": now_ms,
            },
        )
        return ctx

    def _selected_submit_deadline_ms(self) -> int:
        try:
            value = int(
                getattr(self.ctx.config.strategy, "selected_submit_deadline_ms", 0)
                or 0
            )
        except (TypeError, ValueError):
            return 0
        return value if value > 0 else 0

    @staticmethod
    def _payload_matches_entry(payload: dict[str, Any], entry_id: str) -> bool:
        for key in ("entry_id", "position_id", "internal_entry_id", "pending_id"):
            if str(payload.get(key) or "") == entry_id:
                return True
        return False

    def _entry_submit_or_terminal_evidence_seen(
        self,
        entry_id: str,
        *,
        after_seq: int,
    ) -> tuple[bool, str]:
        evidence_kinds = {
            "order.submitted",
            "execution.entry_order_submitted",
            "runtime.pending_entry_registered",
            "runtime.entry_dispatched",
            "entry.opened",
            "entry.aborted",
            "entry.passive_unfilled",
            "runtime.position_opened",
        }
        for record in self.ctx.journal.read_all():
            try:
                seq = int(record.get("seq") or 0)
            except (TypeError, ValueError):
                seq = 0
            if seq <= after_seq:
                continue
            kind = str(record.get("kind") or "")
            if kind not in evidence_kinds:
                continue
            payload = record.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if self._payload_matches_entry(payload, entry_id):
                return True, kind
        return False, ""

    async def _execute_entry_with_selected_deadline(
        self,
        *,
        ctx: EntryContext,
        candidate,
        selected_seq: int,
        selected_at_ms: int,
    ):
        deadline_ms = self._selected_submit_deadline_ms()
        if deadline_ms <= 0:
            return await self.ctx.entry_executor.execute(ctx)

        task = asyncio.create_task(self.ctx.entry_executor.execute(ctx))
        done, _pending = await asyncio.wait(
            {task},
            timeout=deadline_ms / 1000.0,
        )
        if done:
            return task.result()

        has_evidence, evidence_kind = self._entry_submit_or_terminal_evidence_seen(
            ctx.entry_id,
            after_seq=selected_seq,
        )
        if has_evidence:
            self.ctx.journal.append(
                "runtime.entry_selected_submit_deadline_waiting_on_order_truth",
                {
                    "entry_id": ctx.entry_id,
                    "symbol": candidate.symbol,
                    "deadline_ms": deadline_ms,
                    "evidence_kind": evidence_kind,
                    "reason": "submit_or_terminal_evidence_seen",
                    "ts_ms": selected_at_ms + deadline_ms,
                },
            )
            return await task

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        candidate_pair_id = self._candidate_pair_id(candidate)
        self.ctx.journal.append(
            "runtime.entry_selected_submit_deadline_exceeded",
            {
                "entry_id": ctx.entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "deadline_ms": deadline_ms,
                "reason": "no_submit_or_order_evidence",
                "ts_ms": selected_at_ms + deadline_ms,
            },
        )
        self.ctx.journal.append(
            "review.candidate_rejected",
            {
                "entry_id": ctx.entry_id,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "symbol": candidate.symbol,
                "long_venue": ctx.long_venue.value,
                "short_venue": ctx.short_venue.value,
                "rejected_stage": "selected_pre_submit_deadline",
                "rejected_reason": "no_submit_or_order_evidence",
                "ts_ms": selected_at_ms + deadline_ms,
            },
        )
        return None

    async def _execute_entry_context(
        self,
        *,
        ctx: EntryContext,
        candidate,
        route: ExecutionRoute,
        effective_quantity: float,
        price_hint: float,
        maker_venue: Venue,
        maker_leg: Side,
        maker_bbo_evidence: dict,
        now_ms: int,
    ) -> bool:
        try:
            # V1: execution.entry_selected — engine decided to open this candidate
            candidate_pair_id = self._candidate_pair_id(candidate)
            selected_seq = self.ctx.journal.append(
                "execution.entry_selected",
                {
                    "symbol": candidate.symbol,
                    "entry_id": ctx.entry_id,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                    "long_venue": ctx.long_venue.value,
                    "short_venue": ctx.short_venue.value,
                    "quantity": effective_quantity,
                    "route": route.value,
                    "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                    "price_hint": price_hint,
                    "opportunity_type": ctx.opportunity_type,
                    "funding_timestamp_ms": ctx.funding_timestamp_ms,
                    "first_funding_timestamp_ms": ctx.first_funding_timestamp_ms,
                    "long_funding_timestamp_ms": ctx.long_funding_timestamp_ms,
                    "short_funding_timestamp_ms": ctx.short_funding_timestamp_ms,
                    "second_funding_timestamp_ms": ctx.second_funding_timestamp_ms,
                    "first_funding_leg": ctx.first_funding_leg,
                    "exit_after_first_stage": ctx.exit_after_first_stage,
                    "funding_edge_bps_entry": ctx.funding_edge_bps_entry,
                    "total_funding_edge_bps_entry": ctx.total_funding_edge_bps_entry,
                    "expected_edge_bps_entry": ctx.expected_edge_bps_entry,
                    "ts_ms": now_ms,
                },
            )
            result = await self._execute_entry_with_selected_deadline(
                ctx=ctx,
                candidate=candidate,
                selected_seq=selected_seq,
                selected_at_ms=now_ms,
            )
            if result is None:
                return False
            self.ctx.journal.append(
                "runtime.entry_dispatched",
                {
                    "entry_id": ctx.entry_id,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                    "symbol": candidate.symbol,
                    "route": result.route.value,
                    "state": result.state.value,
                    "has_uncertainty": result.has_uncertainty,
                },
            )
            if (
                result.route == ExecutionRoute.REJECTED
                and self._entry_reject_is_post_only_would_take(
                    getattr(result, "reject_reason", "")
                )
            ):
                self._record_post_only_reject_cooldown(
                    candidate,
                    now_ms,
                    getattr(result, "reject_reason", ""),
                    venue=maker_venue.value,
                    side=maker_leg.value,
                    price=price_hint,
                    bbo=maker_bbo_evidence,
                )
                return True
            if result.open_position is not None:
                self.ctx.state.open_positions[result.open_position.position_id] = result.open_position
                self.ctx.journal.append(
                    "runtime.position_opened",
                    {"position_id": result.open_position.position_id},
                )
            if result.route == ExecutionRoute.REJECTED and getattr(result, "reject_reason", ""):
                self._record_entry_result_admission_blocks(
                    candidate,
                    str(result.reject_reason),
                    now_ms,
                )
            if result.pending_entry is not None:
                if getattr(result.pending_entry, "outcome", "") == "rejected":
                    self.ctx.journal.append(
                        "runtime.rejected_pending_suppressed",
                        {
                            "pending_id": result.pending_entry.pending_id,
                            "symbol": result.pending_entry.symbol,
                            "route": result.route.value,
                            "state": result.state.value,
                            "reason": "maker rejected is terminal in V1",
                        },
                    )
                    return True
                # Track pending entry for reconciliation
                if getattr(result.pending_entry, "created_cycle", 0) == 0:
                    result.pending_entry.created_cycle = int(
                        getattr(self.ctx.state, "tick_count", 0) or 0
                    )
                if getattr(result.pending_entry, "frozen_candidate", None) is None:
                    from dataclasses import asdict, is_dataclass

                    if is_dataclass(candidate):
                        result.pending_entry.frozen_candidate = asdict(candidate)
                    else:
                        result.pending_entry.frozen_candidate = dict(
                            getattr(candidate, "__dict__", {}) or {}
                        )
                self.ctx.state.pending_entries[result.pending_entry.pending_id] = result.pending_entry
                self.ctx._recovery_dedup_index[result.pending_entry.maker_client_order_id] = result.pending_entry.pending_id
                self.ctx._recovery_dedup_index[result.pending_entry.hedge_client_order_id] = result.pending_entry.pending_id
                self.ctx.journal.append(
                    "runtime.pending_entry_registered",
                    {
                        "pending_id": result.pending_entry.pending_id,
                        "symbol": result.pending_entry.symbol,
                        "outcome": result.pending_entry.outcome,
                        "maker_client_order_id": result.pending_entry.maker_client_order_id,
                        "hedge_client_order_id": result.pending_entry.hedge_client_order_id,
                        "opportunity_type": result.pending_entry.opportunity_type,
                        "funding_timestamp_ms": result.pending_entry.funding_timestamp_ms,
                        "first_funding_timestamp_ms": result.pending_entry.first_funding_timestamp_ms,
                        "long_funding_timestamp_ms": result.pending_entry.long_funding_timestamp_ms,
                        "short_funding_timestamp_ms": result.pending_entry.short_funding_timestamp_ms,
                        "second_funding_timestamp_ms": result.pending_entry.second_funding_timestamp_ms,
                        "first_funding_leg": result.pending_entry.first_funding_leg,
                    },
                )
        except Exception as e:
            error_text = str(e)
            if self._entry_reject_is_post_only_would_take(error_text):
                self._record_post_only_reject_cooldown(
                    candidate,
                    now_ms,
                    error_text,
                    venue=maker_venue.value,
                    side=maker_leg.value,
                    price=price_hint,
                    bbo=maker_bbo_evidence,
                )
            else:
                self._record_entry_result_admission_blocks(
                    candidate,
                    error_text,
                    now_ms,
                )
            self.ctx.journal.append(
                "runtime.entry_dispatch_error",
                {"entry_id": ctx.entry_id, "error": error_text},
            )
            return False

        return True

    async def _dispatch_entry(self, candidate, now_ms: int, price_hint: float = 0.0) -> bool:
        """Transform a tradeable candidate into an entry context and execute via entry_executor.

        V1: entry route/maker-leg/price gate from config and execution planner.
        Fix 5: no 1.0 pseudo-price — reject entries without valid quote.
        Fix EN-001: route and maker leg driven by planner, not hardcoded in runtime.
        """
        if self._entry_initial_gate_blocked(candidate, now_ms):
            return False

        price_resolution = self._entry_price_resolution(candidate, now_ms, price_hint)
        if price_resolution is None:
            return False
        price_hint, long_order_price_hint, short_order_price_hint, quote_lease = price_resolution

        # Resolve venue enums from candidate string fields
        long_venue = Venue.from_str(candidate.long_venue)
        short_venue = Venue.from_str(candidate.short_venue)
        quantity_resolution = await self._resolve_entry_quantity_steps(
            candidate=candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            price_hint=price_hint,
            now_ms=now_ms,
        )
        if quantity_resolution is None:
            return False
        raw_quantity, quantity, okx_base_step, long_quantity_step, short_quantity_step = quantity_resolution

        # V1 runtime entry guards (apply_runtime_entry_guards)
        gate_checks = [
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation),
            ("passive_close_pending", self._gate_passive_close_pending),
            ("reduce_only", self._gate_reduce_only),
            ("venue_cooldown", self._gate_venue_cooldown),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown),
        ]
        for gate_name, gate_fn in gate_checks:
            allowed, reason = gate_fn(candidate, now_ms) if gate_name in ("venue_cooldown", "zero_fill_cooldown") else gate_fn(candidate)
            if not allowed:
                self.ctx.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                return False

        if self._entry_local_l2_gate_blocked(
            candidate=candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            now_ms=now_ms,
        ):
            return False

        # V1 entry route planning: derive route and maker leg from execution planner.
        # Strategy config provides min-notional; venue-specific chunk/min-notional
        # are resolved from the adapter or spec when available.
        strategy = self.ctx.config.strategy
        min_notional = strategy.min_entry_leg_notional_quote
        # V1: maker leg from strategy config (funding arb: long side is typically maker)
        maker_leg = Side.BUY if strategy.maker_leg_default == "buy" else Side.SELL
        if quote_lease is not None:
            if maker_leg == Side.BUY:
                long_order_price_hint = float(
                    getattr(quote_lease, "long_bid", 0.0) or 0.0
                )
                short_order_price_hint = float(
                    getattr(quote_lease, "short_bid", 0.0) or 0.0
                )
            else:
                long_order_price_hint = float(
                    getattr(quote_lease, "long_ask", 0.0) or 0.0
                )
                short_order_price_hint = float(
                    getattr(quote_lease, "short_ask", 0.0) or 0.0
                )
        maker_planner_price = (
            long_order_price_hint if maker_leg == Side.BUY else short_order_price_hint
        )
        hedge_planner_price = (
            short_order_price_hint if maker_leg == Side.BUY else long_order_price_hint
        )

        # V1: min_hedgeable_chunk aligns to venue step and notional floor
        min_hedgeable_chunk = min_notional / price_hint if price_hint > 0 else 0.0
        if okx_base_step and okx_base_step > 0:
            min_hedgeable_chunk = max(min_hedgeable_chunk, okx_base_step)

        route, plan = plan_incremental_entry_execution(
            target_quantity=quantity,
            slice_ratio=strategy.maker_initial_slice_ratio,
            min_hedgeable_chunk=min_hedgeable_chunk,
            maker_min_notional_quote=min_notional,
            maker_price_hint=maker_planner_price if maker_planner_price > 0 else None,
            max_initial_clip_ratio=strategy.entry_max_initial_clip_ratio,
            hedge_min_notional_quote=min_notional,
            hedge_price_hint=hedge_planner_price if hedge_planner_price > 0 else None,
        )

        if route == ExecutionRoute.REJECTED:
            self.ctx.journal.append(
                "runtime.entry_skipped_planner_rejected",
                {
                    "symbol": candidate.symbol,
                    "target_quantity": quantity,
                    "reason": plan.reason or "planner rejected entry",
                },
            )
            return False

        if (
            okx_base_step is not None
            and okx_base_step > 0
            and route == ExecutionRoute.PASSIVE_INCREMENTAL
            and plan.full_target_quantity > 0
        ):
            plan.initial_maker_target_quantity = plan.full_target_quantity

        # Map planner route to EntryType
        if route == ExecutionRoute.PASSIVE_INCREMENTAL:
            entry_type = EntryType.PASSIVE_INCREMENTAL
            effective_quantity = plan.initial_maker_target_quantity
        elif route == ExecutionRoute.FALLBACK_TO_STANDARD:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = plan.full_target_quantity
        else:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = plan.full_target_quantity

        if quote_lease is not None and entry_type == EntryType.STANDARD_DUAL_TAKER:
            long_order_price_hint = float(
                getattr(quote_lease, "long_ask", 0.0) or 0.0
            )
            short_order_price_hint = float(
                getattr(quote_lease, "short_bid", 0.0) or 0.0
            )

        entry_id = f"entry-{now_ms}-{candidate.symbol}"

        # --- V1 recovery dedup: check for duplicate entries after restart ---
        # Must use the same CID generation as build_entry_orders so the
        # dedup index keys match the actual on-wire clientOrderId.
        maker_venue = long_venue if maker_leg == Side.BUY else short_venue
        hedge_venue = short_venue if maker_leg == Side.BUY else long_venue
        maker_cid = generate_exchange_cid(entry_id, "m", maker_venue)
        hedge_cid = generate_exchange_cid(entry_id, "h", hedge_venue)
        venue_quantity_metadata = {
            long_venue.value: await self._entry_venue_quantity_metadata_evidence(
                long_venue,
                candidate.symbol,
                long_quantity_step,
            ),
            short_venue.value: await self._entry_venue_quantity_metadata_evidence(
                short_venue,
                candidate.symbol,
                short_quantity_step,
            ),
        }
        quantity_plan_reason = self._entry_quantity_plan_reason(
            raw_quantity=raw_quantity,
            common_quantity=quantity,
            full_target_quantity=plan.full_target_quantity,
            initial_maker_target_quantity=plan.initial_maker_target_quantity,
        )
        self.ctx.journal.append(
            "execution.entry_quantity_plan",
            {
                "entry_id": entry_id,
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "raw_quantity": raw_quantity,
                "common_quantity": quantity,
                "full_target_quantity": plan.full_target_quantity,
                "initial_maker_target_quantity": plan.initial_maker_target_quantity,
                "effective_quantity": effective_quantity,
                "quantity_plan_reason": quantity_plan_reason,
                "route": route.value,
                "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                "min_hedgeable_chunk": min_hedgeable_chunk,
                "okx_base_quantity_step": okx_base_step,
                "venue_quantity_steps": {
                    long_venue.value: long_quantity_step or 0.0,
                    short_venue.value: short_quantity_step or 0.0,
                },
                "venue_quantity_metadata": venue_quantity_metadata,
                "ts_ms": now_ms,
            },
        )

        if is_client_order_id_duplicate(maker_cid, self.ctx._recovery_dedup_index):
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": maker_cid,
                    "reason": "duplicate maker clientOrderId in recovery dedup index",
                },
            )
            return False

        if is_client_order_id_duplicate(hedge_cid, self.ctx._recovery_dedup_index):
            self.ctx.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": hedge_cid,
                    "reason": "duplicate hedge clientOrderId in recovery dedup index",
                },
            )
            return False

        # Check for existing pending entry on same symbol pair
        if has_pending_entry_for_symbol(
            self.ctx.state, candidate.symbol,
            long_venue.value, short_venue.value,
        ):
            self.ctx.journal.append(
                "runtime.entry_skipped_existing_pending",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": "pending entry already exists for this symbol pair",
                },
            )
            return False

        if not await self._precheck_bybit_entry_admission(
            candidate=candidate,
            now_ms=now_ms,
            long_venue=long_venue,
            short_venue=short_venue,
            quantity=effective_quantity,
            long_order_price_hint=long_order_price_hint,
            short_order_price_hint=short_order_price_hint,
            maker_venue=maker_venue,
            entry_type=entry_type,
            maker_client_order_id=maker_cid,
            hedge_client_order_id=hedge_cid,
        ):
            return False

        if not await self._prepare_live_entry_leverage_for_candidate(
            candidate=candidate,
            now_ms=now_ms,
            long_venue=long_venue,
            short_venue=short_venue,
        ):
            return False

        maker_bbo_evidence: dict = {}
        if entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK):
            maker_order_price_hint = (
                long_order_price_hint if maker_leg == Side.BUY else short_order_price_hint
            )
            bbo_ok, bbo_reason, maker_bbo_evidence = self._post_only_maker_bbo_guard(
                venue=maker_venue,
                symbol=candidate.symbol,
                side=maker_leg,
                price=maker_order_price_hint,
                now_ms=now_ms,
            )
            if not bbo_ok:
                payload = {
                    **maker_bbo_evidence,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": bbo_reason,
                    "ts_ms": now_ms,
                }
                self.ctx.journal.append("runtime.entry_blocked_post_only_bbo", payload)
                self.ctx.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "rejected_stage": "post_only_bbo_gate",
                        "rejected_reason": bbo_reason,
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
                    },
                )
                return False
            repriced_price = float(
                maker_bbo_evidence.get("repriced_price", 0.0) or 0.0
            )
            if repriced_price > 0.0:
                maker_order_price_hint = repriced_price
                if maker_leg == Side.BUY:
                    long_order_price_hint = repriced_price
                else:
                    short_order_price_hint = repriced_price
                self.ctx.journal.append(
                    "runtime.entry_post_only_bbo_repriced",
                    {
                        **maker_bbo_evidence,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "reason": "post_only_would_cross_repriced",
                        "ts_ms": now_ms,
                    },
                )

        ctx = self._build_entry_context(
            candidate=candidate,
            entry_id=entry_id,
            long_venue=long_venue,
            short_venue=short_venue,
            effective_quantity=effective_quantity,
            long_order_price_hint=long_order_price_hint,
            short_order_price_hint=short_order_price_hint,
            maker_leg=maker_leg,
            entry_type=entry_type,
            route=route,
            now_ms=now_ms,
        )

        return await self._execute_entry_context(
            ctx=ctx,
            candidate=candidate,
            route=route,
            effective_quantity=effective_quantity,
            price_hint=price_hint,
            maker_venue=maker_venue,
            maker_leg=maker_leg,
            maker_bbo_evidence=maker_bbo_evidence,
            now_ms=now_ms,
        )
