"""Close runtime delegate.

This module owns close reconciliation and normal-exit helpers mechanically moved
from LiveRuntime. Keep journal events, payload keys, and close semantics stable.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from typing import Any

from lightfee.core.domain import OrderFillProbeStatus, Side, Venue
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.business_contract import (
    classify_close_reconciliation_state,
    close_reconciliation_exchange_truth,
    close_reconciliation_exchange_truth_clean,
    close_reconciliation_evidence_fields,
)
from lightfee.engine.exit_shadow import (
    ExitShadowConfig,
    ExitShadowMarket,
    ExitShadowQuote,
    ExitShadowSnapshot,
    ExitShadowTracker,
)
from lightfee.engine.lifecycle import set_lifecycle
from lightfee.engine.reconciliation import _recon_fill_price
from lightfee.engine.runtime_context import CloseRuntimeContext
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class CloseRuntime:
    # V1 reconciliation retry constants (Rust V1 recovery.rs)
    _RECONCILE_RETRY_BASE_MS = 30_000
    _RECONCILE_RETRY_MAX_MS = 300_000
    _RECONCILE_HARD_DEADLINE_MS = 600_000  # 10 min hard deadline

    def __init__(self, ctx: CloseRuntimeContext) -> None:
        self.ctx = ctx
        self._close_reconciliation_archive_emitted_keys: set[str] = set()
        self._close_reconciliation_exchange_truth: dict[str, Any] | None = None
        self._close_reconciliation_truth_refresh_next_ms = 0
        self._exit_shadow_tracker: ExitShadowTracker | None = None

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        flush = getattr(self.ctx, "_flush_adapter_order_diagnostics", None)
        if callable(flush):
            return flush(adapter)
        return None

    def _entry_readiness_provider_uses_local_l2(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_local_l2()

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_ws_bbo()

    def _entry_quote_lease_max_age_ms(self) -> int:
        return self.ctx._entry_quote_lease_max_age_ms()

    def _runtime_method_override(self, method_name: str):
        method = getattr(self.ctx, method_name, None)
        class_method = getattr(type(self.ctx), method_name, None)
        if getattr(method, "__func__", None) is class_method:
            return None
        return method if callable(method) else None

    async def _call_fetch_close_leg_reconciliations(self, **kwargs):
        override = self._runtime_method_override("_fetch_close_leg_reconciliations")
        if override is not None:
            return await override(**kwargs)
        return await self._fetch_close_leg_reconciliations(**kwargs)

    async def _call_fetch_pending_close_terminal_live_sizes(self, **kwargs):
        override = self._runtime_method_override("_fetch_pending_close_terminal_live_sizes")
        if override is not None:
            return await override(**kwargs)
        return await self._fetch_pending_close_terminal_live_sizes(**kwargs)

    async def _call_try_abandon_stale_pending_close_reconciliation(self, *args, **kwargs):
        override = self._runtime_method_override(
            "_try_abandon_stale_pending_close_reconciliation"
        )
        if override is not None:
            return await override(*args, **kwargs)
        return await self._try_abandon_stale_pending_close_reconciliation(*args, **kwargs)

    def _call_venue_private_position_confirmed(self, *args, **kwargs) -> bool:
        override = self._runtime_method_override("_venue_private_position_confirmed")
        if override is not None:
            return override(*args, **kwargs)
        return self._venue_private_position_confirmed(*args, **kwargs)

    def _call_open_positions_private_confirmation_ready(self) -> bool:
        override = self._runtime_method_override("_open_positions_private_confirmation_ready")
        if override is not None:
            return override()
        return self._open_positions_private_confirmation_ready()

    def _call_apply_pending_close_reconciliation_backoff(self, *args, **kwargs) -> None:
        override = self._runtime_method_override("_apply_pending_close_reconciliation_backoff")
        if override is not None:
            return override(*args, **kwargs)
        return self._apply_pending_close_reconciliation_backoff(*args, **kwargs)

    def _call_resolve_ws_bbo_close_mid(self, *args, **kwargs) -> float:
        override = self._runtime_method_override("_resolve_ws_bbo_close_mid")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_ws_bbo_close_mid(*args, **kwargs)

    def _call_resolve_local_l2_mid(self, *args, **kwargs) -> float:
        override = self._runtime_method_override("_resolve_local_l2_mid")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_local_l2_mid(*args, **kwargs)

    def _call_resolve_ws_bbo_close_quote(self, *args, **kwargs):
        override = self._runtime_method_override("_resolve_ws_bbo_close_quote")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_ws_bbo_close_quote(*args, **kwargs)

    def _call_resolve_local_l2_quote(self, *args, **kwargs):
        override = self._runtime_method_override("_resolve_local_l2_quote")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_local_l2_quote(*args, **kwargs)

    async def _rewarm_close_price_evidence(
        self,
        keys: list[tuple[str, str]],
        *,
        now_ms: int,
    ) -> dict[tuple[str, str], Any]:
        """Refresh active close WS BBO quotes before passive-close price hints."""
        overlay: dict[tuple[str, str], Any] = {}
        if (
            not keys
            or not self._entry_readiness_provider_uses_ws_bbo()
            or self.ctx.config.runtime.mode == "paper"
        ):
            return overlay

        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            return overlay

        unique_targets: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for venue, symbol in keys:
            key = (str(venue or "").lower(), str(symbol or "").upper())
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            unique_targets.append(key)

        unresolved = set(unique_targets)

        def collect_fresh_from_cache(stage: str) -> None:
            cache = self.ctx.ws_bbo_cache
            if cache is None or not hasattr(cache, "fresh_quote"):
                return
            for key in list(unresolved):
                quote = cache.fresh_quote(
                    key[0],
                    key[1],
                    now_ms=now_ms,
                    max_age_ms=budget_ms,
                )
                if quote is None:
                    continue
                overlay[key] = quote
                unresolved.discard(key)
                if stage != "initial":
                    self.ctx.journal.append(
                        "runtime.close_price_evidence_ws_rewarm_succeeded",
                        {
                            "venue": key[0],
                            "symbol": key[1],
                            "source": str(getattr(quote, "source", "") or "ws_bbo_cache"),
                            "observed_at_ms": int(getattr(quote, "observed_at_ms", 0) or 0),
                            "age_ms": max(
                                now_ms - int(getattr(quote, "observed_at_ms", 0) or 0),
                                0,
                            ),
                            "budget_ms": budget_ms,
                            "outcome": "ws_bbo_rewarm_succeeded",
                            "ts_ms": now_ms,
                        },
                    )

        def cached_quote_evidence(key: tuple[str, str]) -> dict[str, Any]:
            cache = self.ctx.ws_bbo_cache
            quote = None
            if cache is not None and hasattr(cache, "get_quote"):
                try:
                    quote = cache.get_quote(key[0], key[1])
                except Exception:  # pragma: no cover - defensive telemetry
                    quote = None
            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            ws_budget_excluded = (
                observed_at_ms <= 0
                or age_ms is None
                or age_ms > budget_ms
                or bid <= 0.0
                or ask <= bid
            )
            return {
                "observed_at_ms": observed_at_ms,
                "age_ms": age_ms,
                "stale_quote_source": str(getattr(quote, "source", "") or ""),
                "ws_budget_excluded": ws_budget_excluded,
            }

        collect_fresh_from_cache("initial")
        wait_budget_ms = min(budget_ms, 750)
        elapsed_ms = 0
        while unresolved and elapsed_ms < wait_budget_ms:
            await asyncio.sleep(0.05)
            elapsed_ms += 50
            collect_fresh_from_cache("wait")

        refresher_factory = getattr(self.ctx, "_entry_quote_truth_refresher", None)
        refresher = refresher_factory() if callable(refresher_factory) else None
        refresh_quote = getattr(refresher, "refresh_quote", None)
        accept_quote = getattr(self.ctx, "_entry_quote_truth_accept_quote", None)
        for key in list(unresolved):
            rest_error = ""
            refreshed = None
            if callable(refresh_quote):
                try:
                    refreshed = refresh_quote(key[0], key[1], now_ms=now_ms)
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    rest_error = f"{type(exc).__name__}: {exc}"[:240]
            accepted = (
                bool(accept_quote(refreshed, now_ms=now_ms))
                if callable(accept_quote)
                else False
            )
            if accepted:
                cache = self.ctx.ws_bbo_cache
                if cache is not None and hasattr(cache, "update_quote"):
                    cache.update_quote(refreshed)
                overlay[key] = refreshed
                unresolved.discard(key)
                self.ctx.journal.append(
                    "runtime.close_price_evidence_rest_rewarm_succeeded",
                    {
                        "venue": key[0],
                        "symbol": key[1],
                        "source": str(getattr(refreshed, "source", "") or "rest_topbook"),
                        "observed_at_ms": int(getattr(refreshed, "observed_at_ms", 0) or 0),
                        "age_ms": max(
                            now_ms - int(getattr(refreshed, "observed_at_ms", 0) or 0),
                            0,
                        ),
                        "budget_ms": budget_ms,
                        "wait_budget_ms": wait_budget_ms,
                        "endpoint": "rest_topbook",
                        "outcome": "rest_topbook_rewarm_succeeded",
                        "ts_ms": now_ms,
                    },
                )
                continue
            self.ctx.journal.append(
                "runtime.close_price_evidence_rewarm_failed",
                {
                    "venue": key[0],
                    "symbol": key[1],
                    "source": "ws_bbo_quote_lease",
                    **cached_quote_evidence(key),
                    "budget_ms": budget_ms,
                    "wait_budget_ms": wait_budget_ms,
                    "endpoint": "rest_topbook",
                    "rest_error": rest_error,
                    "outcome": (
                        "rest_topbook_unavailable"
                        if callable(refresh_quote)
                        else "rest_topbook_capability_unavailable"
                    ),
                    "ts_ms": now_ms,
                },
            )
        return overlay

    @staticmethod
    def _venue_from_close_reconciliation(value: Any) -> Venue | None:
        if isinstance(value, Venue):
            return value
        if isinstance(value, str) and value:
            try:
                return Venue.from_str(value)
            except ValueError:
                return None
        return None

    @classmethod
    def _venue_from_close_reconciliation_legs(cls, legs: Any) -> Venue | None:
        if not isinstance(legs, list):
            return None
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            venue = cls._venue_from_close_reconciliation(leg.get("venue"))
            if venue is not None:
                return venue
        return None

    @staticmethod
    def _close_reconciliation_leg_identity(leg: Any) -> tuple[str, str]:
        if not isinstance(leg, dict):
            return "", ""
        return str(leg.get("order_id") or ""), str(leg.get("client_order_id") or "")
    @classmethod
    def _has_close_reconciliation_leg_identity(cls, legs: Any) -> bool:
        if not isinstance(legs, list):
            return False
        for leg in legs:
            order_id, client_order_id = cls._close_reconciliation_leg_identity(leg)
            if order_id or client_order_id:
                return True
        return False

    @staticmethod
    def _close_reconciliation_leg_quantity_hint(leg: Any) -> float:
        if not isinstance(leg, dict):
            return 0.0
        raw = leg.get("quantity_hint", leg.get("quantity", 0.0))
        try:
            qty = float(raw or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return qty if math.isfinite(qty) else 0.0

    @classmethod
    def _close_reconciliation_leg_statement_probe_candidate(cls, leg: Any) -> bool:
        if not isinstance(leg, dict):
            return False
        if bool(
            leg.get("statement_probe_candidate")
            or leg.get("truth_gap_candidate")
            or leg.get("accepted_order_truth_gap")
            or leg.get("accounting_only_backfill")
        ):
            return True
        order_id, client_order_id = cls._close_reconciliation_leg_identity(leg)
        return bool(order_id or client_order_id) and (
            cls._close_reconciliation_leg_quantity_hint(leg) <= 1e-12
        )

    @classmethod
    def _statement_probe_candidates_from_reconciliation(
        cls,
        reconciliation: dict[str, Any],
        *,
        missing_leg: str = "",
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        allowed_legs = {"long", "short"}
        if missing_leg in allowed_legs:
            allowed_legs = {missing_leg}

        def _int_or_zero(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        def _append_candidate(row: dict[str, Any], *, leg_name: str) -> None:
            if leg_name not in allowed_legs:
                return
            order_id, client_order_id = cls._close_reconciliation_leg_identity(row)
            if not order_id and not client_order_id:
                return
            venue = str(row.get("venue") or "")
            key = (leg_name, venue.lower(), order_id, client_order_id)
            if key in seen:
                return
            seen.add(key)
            candidates.append(
                {
                    "leg": leg_name,
                    "venue": venue,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "source": str(row.get("source") or ""),
                    "quantity_hint": cls._close_reconciliation_leg_quantity_hint(row),
                    "submitted_at_ms": _int_or_zero(row.get("submitted_at_ms")),
                }
            )

        top_level_candidates = reconciliation.get("statement_probe_candidates")
        if isinstance(top_level_candidates, list):
            for candidate in top_level_candidates:
                if not isinstance(candidate, dict):
                    continue
                leg_name = str(candidate.get("leg") or "")
                _append_candidate(candidate, leg_name=leg_name)

        for leg_name, key in (("long", "long_legs"), ("short", "short_legs")):
            if leg_name not in allowed_legs:
                continue
            legs = reconciliation.get(key)
            if not isinstance(legs, list):
                continue
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                if not cls._close_reconciliation_leg_statement_probe_candidate(leg):
                    continue
                _append_candidate(leg, leg_name=leg_name)
        return candidates

    @classmethod
    def _has_statement_probe_candidates(cls, reconciliation: dict[str, Any]) -> bool:
        return bool(cls._statement_probe_candidates_from_reconciliation(reconciliation))

    @staticmethod
    def _append_journal(ctx: Any, event: str, payload: dict[str, Any]) -> None:
        journal = getattr(ctx, "journal", None)
        append = getattr(journal, "append", None)
        if callable(append):
            append(event, payload)

    @staticmethod
    def _close_reconciliation_fill_qty(fill: Any) -> float:
        qty = getattr(fill, "quantity", 0.0) if fill is not None else 0.0
        return float(qty) if isinstance(qty, (int, float)) and math.isfinite(float(qty)) else 0.0
    async def _fetch_close_leg_reconciliations(
        self,
        *,
        symbol: str,
        venue: Venue,
        legs: Any,
    ) -> list[Any] | None:
        if not isinstance(legs, list):
            return []
        adapter = self.ctx.venue_adapters.get(venue)
        if adapter is None:
            return None
        fetch_probe = getattr(adapter, "fetch_order_fill_probe", None)
        fetch_reconciliation = getattr(adapter, "fetch_order_fill_reconciliation", None)
        if not callable(fetch_probe) and not callable(fetch_reconciliation):
            return None

        fills: list[Any] = []
        unresolved_statement_candidates: list[dict[str, str]] = []
        for leg in legs:
            order_id, client_order_id = self._close_reconciliation_leg_identity(leg)
            if not order_id and not client_order_id:
                return None
            statement_candidate = (
                self._close_reconciliation_leg_statement_probe_candidate(leg)
            )
            if callable(fetch_probe):
                probe = await fetch_probe(symbol, order_id, client_order_id)
                self._flush_adapter_order_diagnostics(adapter)
                status = getattr(probe, "status", None)
                if status == OrderFillProbeStatus.CONFIRMED_NO_FILL:
                    continue
                if status in {
                    OrderFillProbeStatus.UNAVAILABLE,
                    OrderFillProbeStatus.ERROR,
                }:
                    if statement_candidate:
                        unresolved_statement_candidates.append(
                            {
                                "order_id": order_id,
                                "client_order_id": client_order_id,
                            }
                        )
                        continue
                    return None
                if status != OrderFillProbeStatus.CONFIRMED_FILL:
                    if statement_candidate:
                        unresolved_statement_candidates.append(
                            {
                                "order_id": order_id,
                                "client_order_id": client_order_id,
                            }
                        )
                        continue
                    return None
                fill = getattr(probe, "reconciliation", None)
            else:
                fill = await fetch_reconciliation(symbol, order_id, client_order_id)
                self._flush_adapter_order_diagnostics(adapter)
                if fill is None:
                    if statement_candidate:
                        unresolved_statement_candidates.append(
                            {
                                "order_id": order_id,
                                "client_order_id": client_order_id,
                            }
                        )
                        continue
                    return None
                if self._close_reconciliation_fill_qty(fill) <= 1e-12:
                    continue
            if fill is None:
                if statement_candidate:
                    unresolved_statement_candidates.append(
                        {
                            "order_id": order_id,
                            "client_order_id": client_order_id,
                        }
                    )
                    continue
                return None
            fills.append(fill)
        if fills:
            return fills
        if unresolved_statement_candidates:
            return None
        return fills
    @staticmethod
    def _close_reconciliation_live_size(position: Any) -> float:
        if position is None:
            return 0.0
        raw = getattr(position, "quantity", getattr(position, "size", 0.0))
        try:
            size = float(raw)
        except (TypeError, ValueError):
            return 0.0
        return size if math.isfinite(size) else 0.0
    async def _fetch_pending_close_terminal_live_sizes(
        self,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
    ) -> tuple[float, float] | None:
        long_adapter = self.ctx.venue_adapters.get(long_venue)
        short_adapter = self.ctx.venue_adapters.get(short_venue)
        if long_adapter is None or short_adapter is None:
            return None
        try:
            long_position = await long_adapter.fetch_position(symbol)
            self._flush_adapter_order_diagnostics(long_adapter)
            short_position = await short_adapter.fetch_position(symbol)
            self._flush_adapter_order_diagnostics(short_adapter)
        except Exception:
            return None
        return (
            self._close_reconciliation_live_size(long_position),
            self._close_reconciliation_live_size(short_position),
        )
    async def _try_abandon_stale_pending_close_reconciliation(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
        error: str,
    ) -> bool:
        if str(reconciliation.get("kind") or "final") != "final":
            return False
        if (
            bool(reconciliation.get("accounting_only_backfill"))
            or str(reconciliation.get("close_reconciliation_state") or "")
            == "terminal_flat_accounting_gap"
            or self._has_statement_probe_candidates(reconciliation)
        ):
            return False
        position_id = str(reconciliation.get("position_id") or "")
        if any(
            str(getattr(position, "position_id", "")) == position_id
            for position in self.ctx.state.open_positions.values()
        ):
            return False

        next_attempt_count = int(reconciliation.get("attempt_count") or 0) + 1
        terminal_sizes = await self._call_fetch_pending_close_terminal_live_sizes(
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
        )
        if terminal_sizes is None:
            return False
        long_live_size, short_live_size = terminal_sizes
        if abs(long_live_size) > 1e-9 or abs(short_live_size) > 1e-9:
            return False

        self.ctx.journal.append_critical(
            now_ms,
            "exit.reconciliation_abandoned",
            {
                "position_id": position_id,
                "symbol": symbol,
                "kind": "final",
                "reason": reconciliation.get("reason", ""),
                "closed_at_ms": int(reconciliation.get("closed_at_ms") or 0),
                "attempt_count": next_attempt_count,
                "terminal_reason": "fill_reconciliation_unavailable_after_terminal_budget",
                "error": error,
                "lifetime_ms": max(
                    0,
                    now_ms - max(0, int(reconciliation.get("closed_at_ms") or 0)),
                ),
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "long_live_size": long_live_size,
                "short_live_size": short_live_size,
            },
        )
        return True
    def _venue_private_position_confirmed(self, venue: Venue, symbol: str) -> bool:
        if str(getattr(self.ctx.config.runtime, "mode", "") or "").lower() != "live":
            return True
        adapter = self.ctx.venue_adapters.get(venue)
        if adapter is None:
            return False
        if not bool(getattr(adapter, "supports_private_health", False)):
            return True

        worker_count = getattr(adapter, "private_ws_worker_count", None)
        transport = getattr(adapter, "_transport", None)
        if not callable(worker_count) and transport is not None:
            worker_count = getattr(transport, "private_ws_worker_count", None)
        if callable(worker_count):
            try:
                if int(worker_count() or 0) == 0:
                    return True
            except (TypeError, ValueError):
                return True

        health_fn = getattr(adapter, "cached_private_connection_health", None)
        if not callable(health_fn):
            return False
        health = health_fn()
        if health is None:
            return False
        is_unhealthy = getattr(health, "is_unhealthy", None)
        if callable(is_unhealthy) and is_unhealthy():
            return False
        cached_position = getattr(adapter, "cached_position", None)
        if not callable(cached_position):
            return False
        return cached_position(symbol) is not None
    def _open_positions_private_confirmation_ready(self) -> bool:
        return all(
            self._call_venue_private_position_confirmed(position.long_venue, position.symbol)
            and self._call_venue_private_position_confirmed(position.short_venue, position.symbol)
            for position in self.ctx.state.open_positions.values()
        )
    @staticmethod
    def _aggregate_close_reconciliation_fills(fills: list[Any]) -> dict[str, Any]:
        qty = 0.0
        notional = 0.0
        fee_quote = 0.0
        leg_payloads: list[dict[str, Any]] = []
        for fill in fills:
            leg_qty = CloseRuntime._close_reconciliation_fill_qty(fill)
            price = _recon_fill_price(fill)
            fee = float(getattr(fill, "fee_quote", None) or 0.0)
            qty += leg_qty
            notional += leg_qty * price
            fee_quote += fee
            metadata = getattr(fill, "metadata", None)
            if not isinstance(metadata, dict):
                metadata = {}
            trade_id = str(
                getattr(fill, "trade_id", "")
                or metadata.get("trade_id")
                or metadata.get("tradeId")
                or ""
            )
            exec_id = str(
                getattr(fill, "exec_id", "")
                or metadata.get("exec_id")
                or metadata.get("execId")
                or ""
            )
            fill_event_anchor_id = str(
                getattr(fill, "fill_event_anchor_id", "")
                or metadata.get("fill_event_anchor_id")
                or metadata.get("fillEventAnchorId")
                or ""
            )
            leg_payload = {
                "venue": getattr(getattr(fill, "venue", ""), "value", getattr(fill, "venue", "")),
                "order_id": getattr(fill, "order_id", "") or "",
                "client_order_id": getattr(fill, "client_order_id", None) or "",
                "quantity": leg_qty,
                "average_price": price,
                "fee_quote": fee,
                "filled_at_ms": int(getattr(fill, "filled_at_ms", 0) or 0),
            }
            if trade_id:
                leg_payload["trade_id"] = trade_id
            if exec_id:
                leg_payload["exec_id"] = exec_id
            if fill_event_anchor_id:
                leg_payload["fill_event_anchor_id"] = fill_event_anchor_id
            leg_payloads.append(leg_payload)
        average_price = notional / qty if qty > 1e-12 else 0.0
        first = fills[0] if fills else None
        return {
            "quantity": qty,
            "average_price": average_price,
            "fee_quote": fee_quote,
            "order_id": getattr(first, "order_id", "") if first is not None else "",
            "client_order_id": (
                getattr(first, "client_order_id", None) if first is not None else ""
            ) or "",
            "legs": leg_payloads,
        }

    @staticmethod
    def _close_leg_identity(position_id: str, leg: str, fill: Any) -> tuple[str, ...]:
        quantity = CloseRuntime._close_reconciliation_fill_qty(fill)
        price = _recon_fill_price(fill)
        venue = getattr(getattr(fill, "venue", ""), "value", getattr(fill, "venue", ""))
        return (
            str(position_id or ""),
            str(leg or ""),
            str(venue or "").lower(),
            str(getattr(fill, "order_id", "") or ""),
            str(getattr(fill, "client_order_id", None) or ""),
            f"{quantity:.12g}",
            f"{price:.12g}",
            str(int(getattr(fill, "filled_at_ms", 0) or 0)),
        )

    @staticmethod
    def _close_leg_duplicate_sample(leg: str, fill: Any) -> dict[str, Any]:
        venue = getattr(getattr(fill, "venue", ""), "value", getattr(fill, "venue", ""))
        return {
            "leg": str(leg or ""),
            "venue": str(venue or "").lower(),
            "order_id": str(getattr(fill, "order_id", "") or ""),
            "client_order_id": str(getattr(fill, "client_order_id", None) or ""),
            "quantity": CloseRuntime._close_reconciliation_fill_qty(fill),
            "average_price": _recon_fill_price(fill),
            "filled_at_ms": int(getattr(fill, "filled_at_ms", 0) or 0),
        }

    @staticmethod
    def _deduplicate_close_leg_fills(
        position_id: str,
        leg: str,
        fills: list[Any],
    ) -> tuple[list[Any], list[dict[str, Any]]]:
        seen: set[tuple[str, ...]] = set()
        retained: list[Any] = []
        duplicates: list[dict[str, Any]] = []
        for fill in fills:
            order_id = str(getattr(fill, "order_id", "") or "")
            client_order_id = str(getattr(fill, "client_order_id", None) or "")
            if not order_id and not client_order_id:
                retained.append(fill)
                continue
            key = CloseRuntime._close_leg_identity(position_id, leg, fill)
            if key in seen:
                if len(duplicates) < 12:
                    duplicates.append(CloseRuntime._close_leg_duplicate_sample(leg, fill))
                continue
            seen.add(key)
            retained.append(fill)
        return retained, duplicates

    def _apply_pending_close_reconciliation_backoff(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
    ) -> None:
        attempt = int(reconciliation.get("attempt_count") or 0) + 1
        reconciliation["attempt_count"] = attempt
        delay = min(
            self._RECONCILE_RETRY_BASE_MS * (2 ** max(attempt - 1, 0)),
            self._RECONCILE_RETRY_MAX_MS,
        )
        reconciliation["next_attempt_ms"] = now_ms + delay

    @staticmethod
    def _missing_leg_from_reconciliation(reconciliation: dict[str, Any]) -> str:
        missing_leg = str(reconciliation.get("missing_leg") or "")
        if missing_leg in {"long", "short", "both"}:
            return missing_leg
        if (
            reconciliation.get("accepted_order_truth_gap") is True
            or str(reconciliation.get("kind") or "") == "accepted_order_truth_gap"
        ):
            leg = str(reconciliation.get("leg") or "")
            if leg in {"long", "short"}:
                return leg
        reason = str(
            reconciliation.get("last_evidence_gap_reason")
            or reconciliation.get("evidence_gap_reason")
            or reconciliation.get("reason")
            or ""
        )
        if "missing_both_close_trade_statement" in reason:
            return "both"
        if "missing_long_close_trade_statement" in reason:
            return "long"
        if "missing_short_close_trade_statement" in reason:
            return "short"
        return ""

    def _retain_terminal_flat_accounting_backfill(
        self,
        reconciliation: dict[str, Any],
        *,
        now_ms: int,
        exchange_truth: dict[str, Any],
        state: str,
        symbol: str,
        missing_leg: str,
        evidence_gap_reason: str,
        truth_hash: str,
        source: str,
        emit_event: bool = True,
    ) -> None:
        candidates = self._statement_probe_candidates_from_reconciliation(
            reconciliation,
            missing_leg=missing_leg,
        )
        if not candidates:
            candidates = self._statement_probe_candidates_from_reconciliation(
                reconciliation
            )

        reconciliation["pending_backfill"] = True
        reconciliation["accounting_only_backfill"] = True
        reconciliation["blocking_trading"] = False
        reconciliation["close_reconciliation_state"] = state
        reconciliation["archive_reconciliation"] = False
        reconciliation["business_contract_action"] = state
        reconciliation["exchange_truth"] = dict(exchange_truth)
        reconciliation["last_partial_reconciled_at_ms"] = now_ms
        reconciliation["unresolved_statement_probe_candidates"] = candidates
        if missing_leg:
            reconciliation["missing_leg"] = missing_leg
        if evidence_gap_reason:
            reconciliation["last_evidence_gap_reason"] = evidence_gap_reason
        if truth_hash:
            reconciliation["exchange_truth_hash"] = truth_hash
        original_payload = reconciliation.get("original_payload")
        if not isinstance(original_payload, dict):
            original_payload = {}
        original_payload["exchange_truth"] = dict(exchange_truth)
        reconciliation["original_payload"] = original_payload

        if emit_event:
            self._append_journal(
                self.ctx,
                "reconciliation.pending_close_backfill_retained",
                {
                    "position_id": reconciliation.get("position_id", ""),
                    "symbol": symbol,
                    "candidate_owner_id": reconciliation.get(
                        "candidate_owner_id",
                        reconciliation.get("position_id", ""),
                    ),
                    "missing_leg": missing_leg,
                    "evidence_gap_reason": evidence_gap_reason,
                    "next_attempt_ms": reconciliation.get("next_attempt_ms", 0),
                    "source": source,
                    "close_reconciliation_state": state,
                    "archive_reconciliation": False,
                    "accounting_only_backfill": True,
                    "blocking_trading": False,
                    "exchange_truth_hash": truth_hash,
                    "unresolved_statement_probe_candidates": candidates,
                },
            )

    def _archive_terminal_flat_pending_close_reconciliation(
        self,
        reconciliation: dict[str, Any],
        *,
        now_ms: int,
    ) -> bool:
        exchange_truth = self._current_exchange_truth_for_close_reconciliation(
            reconciliation
        )
        if not isinstance(exchange_truth, dict):
            return False
        contract = classify_close_reconciliation_state(
            reconciliation,
            current_exchange_truth_clean=True,
        )
        if contract.get("archive_reconciliation") is not True:
            return False
        state = str(contract.get("state") or "")
        if state != "terminal_flat_accounting_gap":
            return False

        snapshot = reconciliation.get("position_snapshot")
        if not isinstance(snapshot, dict):
            snapshot = {}
        symbol = str(
            reconciliation.get("symbol") or snapshot.get("symbol") or ""
        ).upper()
        evidence_gap_reason = str(
            reconciliation.get("last_evidence_gap_reason")
            or reconciliation.get("evidence_gap_reason")
            or contract.get("reason")
            or "terminal_flat_accounting_gap"
        )
        missing_leg = self._missing_leg_from_reconciliation(reconciliation)
        truth_hash = self._close_reconciliation_truth_hash(exchange_truth)
        candidates = self._statement_probe_candidates_from_reconciliation(
            reconciliation,
            missing_leg=missing_leg,
        )
        if candidates:
            self._retain_terminal_flat_accounting_backfill(
                reconciliation,
                now_ms=now_ms,
                exchange_truth=exchange_truth,
                state=state,
                symbol=symbol,
                missing_leg=missing_leg,
                evidence_gap_reason=evidence_gap_reason,
                truth_hash=truth_hash,
                source=str(
                    reconciliation.get("source")
                    or "pending_close_reconciliation"
                ),
                emit_event=reconciliation.get("accounting_only_backfill") is not True,
            )
            return False
        payload = {
            "position_id": reconciliation.get("position_id", ""),
            "symbol": symbol,
            "candidate_owner_id": reconciliation.get(
                "candidate_owner_id",
                reconciliation.get("position_id", ""),
            ),
            "missing_leg": missing_leg,
            "evidence_gap_reason": evidence_gap_reason,
            "next_attempt_ms": reconciliation.get("next_attempt_ms", 0),
            "source": reconciliation.get(
                "source",
                "pending_close_reconciliation",
            ),
            "close_reconciliation_state": state,
            "archive_reconciliation": True,
            "exchange_truth_hash": truth_hash,
        }
        archive_key = (
            self._close_reconciliation_archive_key(
                payload,
                truth_hash=truth_hash,
            )
            if truth_hash
            else ""
        )
        archive_already_emitted = bool(
            archive_key
            and archive_key in self._close_reconciliation_archive_emitted_keys
        )

        reconciliation["close_reconciliation_state"] = state
        reconciliation["archived"] = True
        reconciliation["archive_reason"] = state
        reconciliation["business_contract_action"] = state
        reconciliation["exchange_truth"] = dict(exchange_truth)
        reconciliation["last_partial_reconciled_at_ms"] = now_ms
        if missing_leg:
            reconciliation["missing_leg"] = missing_leg
        if evidence_gap_reason:
            reconciliation["last_evidence_gap_reason"] = evidence_gap_reason
        if truth_hash:
            reconciliation["exchange_truth_hash"] = truth_hash
        if archive_key:
            reconciliation["archive_key"] = archive_key
        original_payload = reconciliation.get("original_payload")
        if not isinstance(original_payload, dict):
            original_payload = {}
        original_payload["exchange_truth"] = dict(exchange_truth)
        reconciliation["original_payload"] = original_payload

        if not archive_already_emitted:
            self.ctx.journal.append(
                "reconciliation.pending_close_backfill_archived",
                payload,
            )
            if archive_key:
                self._close_reconciliation_archive_emitted_keys.add(archive_key)
        return True

    def _current_exchange_truth_for_close_reconciliation(
        self,
        reconciliation: dict[str, Any],
    ) -> dict[str, Any] | None:
        for raw_truth in (
            getattr(self.ctx, "_last_recovery_exchange_truth", None),
            self._close_reconciliation_exchange_truth,
        ):
            current_truth = raw_truth if isinstance(raw_truth, dict) else None
            exchange_truth = close_reconciliation_exchange_truth(
                reconciliation,
                current_exchange_truth=current_truth,
            )
            if exchange_truth is not None:
                return exchange_truth
        return close_reconciliation_exchange_truth(reconciliation)

    async def _refresh_close_reconciliation_account_truth(
        self,
        reconciliations: list[dict[str, Any]],
        *,
        now_ms: int,
    ) -> None:
        if not reconciliations:
            return
        if all(
            self._current_exchange_truth_for_close_reconciliation(reconciliation)
            is not None
            for reconciliation in reconciliations
        ):
            return
        if self._close_reconciliation_truth_refresh_next_ms > now_ms:
            return
        collector = getattr(self.ctx, "_collect_recovery_ledger_account_truth", None)
        if not callable(collector):
            return

        self._close_reconciliation_truth_refresh_next_ms = (
            now_ms + self._RECONCILE_RETRY_BASE_MS
        )
        try:
            exchange_truth = await collector(now_ms)
        except Exception as exc:
            self.ctx.journal.append(
                "reconciliation.pending_close_exchange_truth_refresh_failed",
                {
                    "reason": str(exc),
                    "next_attempt_ms": self._close_reconciliation_truth_refresh_next_ms,
                },
            )
            return
        if not isinstance(exchange_truth, dict):
            return
        if exchange_truth.get("truth_supported") is False:
            return
        exchange_truth = dict(exchange_truth)
        exchange_truth.setdefault(
            "source",
            "pending_close_reconciliation_account_truth_refresh",
        )
        self._close_reconciliation_exchange_truth = exchange_truth
        self.ctx.journal.append(
            "reconciliation.pending_close_exchange_truth_refreshed",
            {
                "truth_supported": exchange_truth.get("truth_supported"),
                "truth_available": exchange_truth.get("truth_available"),
                "position_row_count": len(exchange_truth.get("positions") or []),
                "open_order_count": len(exchange_truth.get("open_orders") or []),
                "probe_evidence_count": len(exchange_truth.get("probe_evidence") or []),
                "error_count": len(exchange_truth.get("errors") or []),
                "next_attempt_ms": self._close_reconciliation_truth_refresh_next_ms,
            },
        )

    @staticmethod
    def _close_reconciliation_truth_hash(exchange_truth: dict[str, Any] | None) -> str:
        if not isinstance(exchange_truth, dict):
            return ""
        raw = json.dumps(exchange_truth, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _close_reconciliation_archive_key(
        payload: dict[str, Any],
        *,
        truth_hash: str,
    ) -> str:
        parts = [
            str(payload.get("position_id") or ""),
            str(payload.get("symbol") or "").upper(),
            str(payload.get("missing_leg") or ""),
            str(payload.get("evidence_gap_reason") or ""),
            truth_hash,
        ]
        return "|".join(parts)

    @staticmethod
    def _close_reconciliation_retained_evidence_key(
        payload: dict[str, Any],
        *,
        truth_hash: str,
    ) -> str:
        def _safe_float(value: Any) -> float:
            try:
                result = float(value or 0.0)
            except (TypeError, ValueError):
                return 0.0
            return result if math.isfinite(result) else 0.0

        def _leg_fingerprint(leg: str) -> list[dict[str, Any]]:
            raw_legs = payload.get(f"{leg}_legs")
            if not isinstance(raw_legs, list):
                return []
            normalized: list[dict[str, Any]] = []
            for raw in raw_legs:
                if not isinstance(raw, dict):
                    continue
                normalized.append({
                    "leg": leg,
                    "venue": str(raw.get("venue") or "").lower(),
                    "order_id": str(raw.get("order_id") or ""),
                    "client_order_id": str(raw.get("client_order_id") or ""),
                    "trade_id": str(raw.get("trade_id") or ""),
                    "exec_id": str(raw.get("exec_id") or ""),
                    "quantity": f"{_safe_float(raw.get('quantity')):.12g}",
                    "average_price": f"{_safe_float(raw.get('average_price') or raw.get('price')):.12g}",
                    "fee_quote": f"{_safe_float(raw.get('fee_quote')):.12g}",
                    "filled_at_ms": str(raw.get("filled_at_ms") or ""),
                })
            return sorted(
                normalized,
                key=lambda item: (
                    item["leg"],
                    item["venue"],
                    item["order_id"],
                    item["client_order_id"],
                    item["trade_id"],
                    item["exec_id"],
                    item["quantity"],
                    item["filled_at_ms"],
                ),
            )

        fingerprint = {
            "position_id": str(payload.get("position_id") or ""),
            "symbol": str(payload.get("symbol") or "").upper(),
            "missing_leg": str(payload.get("missing_leg") or ""),
            "evidence_gap_reason": str(payload.get("evidence_gap_reason") or ""),
            "truth_hash": truth_hash,
            "long": _leg_fingerprint("long"),
            "short": _leg_fingerprint("short"),
        }
        raw = json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _annotate_terminal_flat_accounting_gap_payload(
        payload: dict[str, Any],
        *,
        contract: dict[str, Any],
        exchange_truth: dict[str, Any] | None,
        truth_hash: str,
    ) -> None:
        state = str(contract.get("state") or "")
        payload["close_reconciliation_state"] = state
        payload["archive_reconciliation"] = bool(
            contract.get("archive_reconciliation") is True
        )
        payload["business_contract_action"] = state
        if truth_hash:
            payload["exchange_truth_hash"] = truth_hash
        if isinstance(exchange_truth, dict):
            payload["exchange_truth"] = dict(exchange_truth)
        if state != "terminal_flat_accounting_gap":
            return
        missing_leg = str(payload.get("missing_leg") or "")
        if missing_leg not in {"long", "short", "both"}:
            return
        payload["missing_leg_terminality"] = (
            "flat_by_position_truth_no_trade_statement"
        )
        status = payload.get("trade_probe_status")
        if not isinstance(status, dict):
            status = {}
        status = dict(status)
        if missing_leg == "both":
            status["long"] = "flat_by_position_truth"
            status["short"] = "flat_by_position_truth"
        else:
            status[missing_leg] = "flat_by_position_truth"
        payload["trade_probe_status"] = status

    def _exit_reconciled_payload_from_leg_fills(
        self,
        reconciliation: dict[str, Any],
        long_fills: list[Any],
        short_fills: list[Any],
        now_ms: int,
    ) -> dict[str, Any]:
        snapshot = reconciliation.get("position_snapshot") or {}
        if not isinstance(snapshot, dict):
            snapshot = {}

        position_id = str(reconciliation.get("position_id") or "")
        long_fills, long_duplicates = self._deduplicate_close_leg_fills(
            position_id,
            "long",
            long_fills,
        )
        short_fills, short_duplicates = self._deduplicate_close_leg_fills(
            position_id,
            "short",
            short_fills,
        )
        duplicate_samples = long_duplicates + short_duplicates
        long = self._aggregate_close_reconciliation_fills(long_fills)
        short = self._aggregate_close_reconciliation_fills(short_fills)
        long_qty = float(long["quantity"])
        short_qty = float(short["quantity"])
        duplicate_count = len(long_duplicates) + len(short_duplicates)
        evidence_gap_fields = close_reconciliation_evidence_fields(
            long_quantity=long_qty,
            short_quantity=short_qty,
            duplicate_close_leg_suppressed_count=duplicate_count,
        )
        long_entry = float(snapshot.get("long_entry_price") or 0.0)
        short_entry = float(snapshot.get("short_entry_price") or 0.0)
        funding_quote = float(snapshot.get("captured_funding_quote") or 0.0)
        entry_fee = float(snapshot.get("total_entry_fee_quote") or 0.0)
        price_pnl = ((float(long["average_price"]) - long_entry) * long_qty) + (
            (short_entry - float(short["average_price"])) * short_qty
        )
        exit_fee = float(long["fee_quote"]) + float(short["fee_quote"])
        complete = long_qty > 1e-12 and short_qty > 1e-12
        missing_legs = [
            leg
            for leg, qty in (("long", long_qty), ("short", short_qty))
            if qty <= 1e-12
        ]
        if not missing_legs:
            missing_leg = "none"
        elif len(missing_legs) == 2:
            missing_leg = "both"
        else:
            missing_leg = missing_legs[0]
        return {
            "position_id": reconciliation.get("position_id", ""),
            "symbol": reconciliation.get("symbol", snapshot.get("symbol", "")),
            "kind": reconciliation.get("kind", "final"),
            "reason": reconciliation.get("reason", ""),
            "closed_at_ms": int(reconciliation.get("closed_at_ms") or now_ms),
            "reconciled_at_ms": now_ms,
            "long_closed_qty": long_qty,
            "short_closed_qty": short_qty,
            "long_average_price": float(long["average_price"]),
            "short_average_price": float(short["average_price"]),
            "long_order_id": long["order_id"],
            "short_order_id": short["order_id"],
            "long_client_order_id": long["client_order_id"],
            "short_client_order_id": short["client_order_id"],
            "long_legs": long["legs"],
            "short_legs": short["legs"],
            "price_pnl": price_pnl,
            "funding_pnl_quote": funding_quote,
            "entry_fee_quote": entry_fee,
            "exit_fee_quote": exit_fee,
            "net_quote": price_pnl + funding_quote - entry_fee - exit_fee,
            "venue_statement_reconciled": complete,
            "evidence_gap": not complete,
            "candidate_owner_id": position_id,
            "missing_leg": missing_leg,
            "pending_backfill": not complete,
            "accounting_status": "complete" if complete else "pending_backfill",
            "clean_accounting_ready": complete,
            **evidence_gap_fields,
            "duplicate_close_leg_suppressed_count": duplicate_count,
            "duplicate_close_leg_suppressed_samples": duplicate_samples[:12],
            "source": reconciliation.get("source", "pending_close_reconciliation"),
        }

    @staticmethod
    def _order_filled_events_from_close_reconciliation_payload(
        payload: dict[str, Any],
        *,
        source: str,
    ) -> list[dict[str, Any]]:
        position_id = str(payload.get("position_id") or "")
        symbol = str(payload.get("symbol") or "")
        events: list[dict[str, Any]] = []
        for leg, side in (("long", Side.SELL), ("short", Side.BUY)):
            raw_legs = payload.get(f"{leg}_legs") or []
            if not isinstance(raw_legs, list):
                continue
            for leg_payload in raw_legs:
                if not isinstance(leg_payload, dict):
                    continue
                try:
                    quantity = float(leg_payload.get("quantity") or 0.0)
                except (TypeError, ValueError):
                    quantity = 0.0
                if quantity <= 1e-12:
                    continue
                try:
                    price = float(
                        leg_payload.get("average_price")
                        or leg_payload.get("price")
                        or 0.0
                    )
                except (TypeError, ValueError):
                    price = 0.0
                try:
                    fee_quote = float(leg_payload.get("fee_quote") or 0.0)
                except (TypeError, ValueError):
                    fee_quote = 0.0
                try:
                    filled_at_ms = int(leg_payload.get("filled_at_ms") or 0)
                except (TypeError, ValueError):
                    filled_at_ms = 0
                venue = str(leg_payload.get("venue") or "").lower()
                order_id = str(leg_payload.get("order_id") or "")
                client_order_id = str(leg_payload.get("client_order_id") or "")
                trade_id = str(leg_payload.get("trade_id") or "")
                exec_id = str(leg_payload.get("exec_id") or "")
                fill_event_anchor_id = str(
                    leg_payload.get("fill_event_anchor_id")
                    or CloseRuntime._deterministic_fill_event_anchor_id(
                        position_id=position_id,
                        leg=leg,
                        venue=venue,
                        order_id=order_id,
                        client_order_id=client_order_id,
                        trade_id=trade_id,
                        exec_id=exec_id,
                    )
                )
                events.append({
                    "fill_event_anchor_id": fill_event_anchor_id,
                    "position_id": position_id,
                    "leg": leg,
                    "venue": venue,
                    "symbol": symbol,
                    "side": side.value,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "trade_id": trade_id,
                    "exec_id": exec_id,
                    "quantity": quantity,
                    "cumulative_quantity": quantity,
                    "price": price,
                    "fee_quote": fee_quote,
                    "filled_at_ms": filled_at_ms,
                    "source": source,
                })
        return events

    @staticmethod
    def _deterministic_fill_event_anchor_id(
        *,
        position_id: str,
        leg: str,
        venue: str,
        order_id: str,
        client_order_id: str,
        trade_id: str,
        exec_id: str,
    ) -> str:
        raw = "|".join(
            [
                position_id,
                leg,
                venue,
                order_id,
                client_order_id,
                trade_id,
                exec_id,
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _deterministic_fill_event_id(
        *,
        fill_event_anchor_id: str | None = None,
        position_id: str = "",
        leg: str = "",
        venue: str = "",
        order_id: str = "",
        client_order_id: str = "",
        trade_id: str = "",
        exec_id: str = "",
        quantity: float = 0.0,
        cumulative_quantity: float | None = None,
    ) -> str:
        anchor = fill_event_anchor_id or CloseRuntime._deterministic_fill_event_anchor_id(
            position_id=position_id,
            leg=leg,
            venue=venue,
            order_id=order_id,
            client_order_id=client_order_id,
            trade_id=trade_id,
            exec_id=exec_id,
        )
        cumulative = quantity if cumulative_quantity is None else cumulative_quantity
        raw = "|".join([anchor, f"{quantity:.12g}", f"{cumulative:.12g}"])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def _append_close_reconciliation_order_filled_events(
        self,
        payload: dict[str, Any],
        *,
        now_ms: int,
    ) -> None:
        source = str(payload.get("source") or "pending_close_reconciliation")
        emitted = payload.setdefault("emitted_fill_event_ids", [])
        emitted_set = {
            str(item)
            for item in emitted
            if isinstance(item, (str, int, float))
        }
        emitted_quantities = payload.setdefault("emitted_fill_event_quantities", {})
        if not isinstance(emitted_quantities, dict):
            emitted_quantities = {}
            payload["emitted_fill_event_quantities"] = emitted_quantities
        for event in self._order_filled_events_from_close_reconciliation_payload(
            payload,
            source=source,
        ):
            anchor_id = str(event.get("fill_event_anchor_id") or "")
            try:
                cumulative_quantity = float(
                    event.get("cumulative_quantity")
                    or event.get("quantity")
                    or 0.0
                )
            except (TypeError, ValueError):
                cumulative_quantity = 0.0
            try:
                emitted_quantity = float(emitted_quantities.get(anchor_id) or 0.0)
            except (TypeError, ValueError):
                emitted_quantity = 0.0
            delta_quantity = max(0.0, cumulative_quantity - emitted_quantity)
            if delta_quantity <= 1e-12:
                continue
            original_fee_quote = float(event.get("fee_quote") or 0.0)
            if cumulative_quantity > 1e-12 and delta_quantity < cumulative_quantity:
                event["fee_quote"] = original_fee_quote * delta_quantity / cumulative_quantity
            event["quantity"] = delta_quantity
            event["emitted_cumulative_quantity"] = cumulative_quantity
            fill_event_id = self._deterministic_fill_event_id(
                fill_event_anchor_id=anchor_id,
                quantity=delta_quantity,
                cumulative_quantity=cumulative_quantity,
            )
            event["fill_event_id"] = fill_event_id
            if fill_event_id and fill_event_id in emitted_set:
                continue
            self.ctx.journal.append_critical(now_ms, "order.filled", event)
            if fill_event_id:
                emitted.append(fill_event_id)
                emitted_set.add(fill_event_id)
            if anchor_id:
                emitted_quantities[anchor_id] = cumulative_quantity

    async def _process_pending_close_reconciliations(self, now_ms: int) -> None:
        self.ctx.state.set_pending_close_reconciliations(
            getattr(self.ctx.state, "pending_close_reconciliations", [])
        )
        pending_reconciliations = self.ctx.state.pending_close_reconciliations
        if not pending_reconciliations:
            return
        if str(getattr(self.ctx.config.runtime, "mode", "") or "").lower() != "live":
            return

        current_cycle = int(getattr(self.ctx.state, "tick_count", 0) or 0)
        refresh_candidates = [
            reconciliation
            for reconciliation in pending_reconciliations
            if isinstance(reconciliation, dict)
            and not (
                reconciliation.get("archived") is True
                and str(reconciliation.get("archive_reason") or "")
                == "terminal_flat_accounting_gap"
            )
            and not (
                current_cycle != 0
                and int(reconciliation.get("created_cycle") or 0) >= current_cycle
            )
        ]
        await self._refresh_close_reconciliation_account_truth(
            refresh_candidates,
            now_ms=now_ms,
        )

        retained: list[Any] = []
        eligible: list[dict[str, Any]] = []
        changed = False
        for reconciliation in list(pending_reconciliations):
            if not isinstance(reconciliation, dict):
                retained.append(reconciliation)
                continue
            if (
                reconciliation.get("archived") is True
                and str(reconciliation.get("archive_reason") or "")
                == "terminal_flat_accounting_gap"
            ):
                changed = True
                continue
            created_cycle = int(reconciliation.get("created_cycle") or 0)
            if current_cycle != 0 and created_cycle >= current_cycle:
                retained.append(reconciliation)
                continue
            if self._archive_terminal_flat_pending_close_reconciliation(
                reconciliation,
                now_ms=now_ms,
            ):
                changed = True
                continue
            if int(reconciliation.get("next_attempt_ms") or 0) > now_ms:
                retained.append(reconciliation)
                continue
            eligible.append(reconciliation)

        for reconciliation in sorted(
            eligible,
            key=lambda item: (
                int(item.get("closed_at_ms") or 0),
                0 if str(item.get("kind") or "final") == "partial" else 1,
                str(item.get("position_id") or ""),
            ),
        ):
            snapshot = reconciliation.get("position_snapshot") or {}
            if not isinstance(snapshot, dict):
                snapshot = {}
            long_legs = reconciliation.get("long_legs")
            short_legs = reconciliation.get("short_legs")
            long_has_identity = self._has_close_reconciliation_leg_identity(long_legs)
            short_has_identity = self._has_close_reconciliation_leg_identity(short_legs)
            long_venue = self._venue_from_close_reconciliation(
                reconciliation.get("long_venue") or snapshot.get("long_venue")
            )
            short_venue = self._venue_from_close_reconciliation(
                reconciliation.get("short_venue") or snapshot.get("short_venue")
            )
            if long_venue is None and long_has_identity:
                long_venue = self._venue_from_close_reconciliation_legs(long_legs)
            if short_venue is None and short_has_identity:
                short_venue = self._venue_from_close_reconciliation_legs(short_legs)
            if long_venue is None and not long_has_identity:
                long_venue = short_venue
            if short_venue is None and not short_has_identity:
                short_venue = long_venue
            if (
                long_venue is None
                or short_venue is None
                or (long_has_identity and long_venue is None)
                or (short_has_identity and short_venue is None)
            ):
                self.ctx.journal.append(
                    "reconciliation.pending_close_reconciliation_invalid",
                    {
                        "position_id": reconciliation.get("position_id", ""),
                        "symbol": reconciliation.get("symbol", ""),
                        "reason": "missing_position_snapshot_venues",
                    },
                )
                self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
                retained.append(reconciliation)
                changed = True
                continue

            if not (long_has_identity or short_has_identity):
                self.ctx.journal.append(
                    "reconciliation.pending_close_reconciliation_invalid",
                    {
                        "position_id": reconciliation.get("position_id", ""),
                        "symbol": reconciliation.get("symbol", ""),
                        "reason": "missing_order_identity",
                    },
                )
                self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
                retained.append(reconciliation)
                changed = True
                continue

            symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "")
            long_fills = await self._call_fetch_close_leg_reconciliations(
                symbol=symbol,
                venue=long_venue,
                legs=long_legs,
            )
            short_fills = await self._call_fetch_close_leg_reconciliations(
                symbol=symbol,
                venue=short_venue,
                legs=short_legs,
            )
            if long_fills is not None and short_fills is not None and (long_fills or short_fills):
                payload = self._exit_reconciled_payload_from_leg_fills(
                    reconciliation,
                    long_fills,
                    short_fills,
                    now_ms,
                )
                contract: dict[str, Any] | None = None
                archive_reconciliation = False
                exchange_truth: dict[str, Any] | None = None
                truth_hash = ""
                archive_key = ""
                archive_already_emitted = False
                retained_evidence_key = ""
                retained_already_emitted = False
                if bool(payload.get("pending_backfill")):
                    exchange_truth = self._current_exchange_truth_for_close_reconciliation(
                        reconciliation
                    )
                    truth_hash = self._close_reconciliation_truth_hash(exchange_truth)
                    contract = classify_close_reconciliation_state(
                        payload,
                        current_exchange_truth_clean=exchange_truth is not None,
                    )
                    self._annotate_terminal_flat_accounting_gap_payload(
                        payload,
                        contract=contract,
                        exchange_truth=exchange_truth,
                        truth_hash=truth_hash,
                    )
                    archive_reconciliation = bool(
                        contract.get("archive_reconciliation") is True
                    )
                    if archive_reconciliation and self._has_statement_probe_candidates(
                        reconciliation
                    ):
                        archive_reconciliation = False
                        payload["accounting_only_backfill"] = True
                        payload["blocking_trading"] = False
                        payload["archive_reconciliation"] = False
                    if archive_reconciliation and truth_hash:
                        archive_key = self._close_reconciliation_archive_key(
                            payload,
                            truth_hash=truth_hash,
                        )
                        archive_already_emitted = (
                            archive_key
                            in self._close_reconciliation_archive_emitted_keys
                        )
                    if not archive_reconciliation:
                        retained_evidence_key = (
                            self._close_reconciliation_retained_evidence_key(
                                payload,
                                truth_hash=truth_hash,
                            )
                        )
                        retained_keys = reconciliation.get(
                            "retained_reconciliation_evidence_keys"
                        )
                        retained_already_emitted = (
                            isinstance(retained_keys, list)
                            and retained_evidence_key in {
                                str(item)
                                for item in retained_keys
                                if isinstance(item, (str, int, float))
                            }
                        )
                emitted_fill_event_ids = reconciliation.get("emitted_fill_event_ids")
                if isinstance(emitted_fill_event_ids, list):
                    payload["emitted_fill_event_ids"] = list(emitted_fill_event_ids)
                emitted_fill_event_quantities = reconciliation.get(
                    "emitted_fill_event_quantities"
                )
                if isinstance(emitted_fill_event_quantities, dict):
                    payload["emitted_fill_event_quantities"] = dict(
                        emitted_fill_event_quantities
                    )
                emit_reconciled = not archive_already_emitted and not retained_already_emitted
                if emit_reconciled:
                    self.ctx.journal.append_critical(
                        now_ms,
                        "exit.reconciled",
                        payload,
                    )
                    self._append_close_reconciliation_order_filled_events(
                        payload,
                        now_ms=now_ms,
                    )
                reconciliation["emitted_fill_event_ids"] = list(
                    payload.get("emitted_fill_event_ids") or []
                )
                reconciliation["emitted_fill_event_quantities"] = dict(
                    payload.get("emitted_fill_event_quantities") or {}
                )
                if retained_evidence_key and emit_reconciled:
                    retained_keys = reconciliation.get(
                        "retained_reconciliation_evidence_keys"
                    )
                    if not isinstance(retained_keys, list):
                        retained_keys = []
                    retained_keys.append(retained_evidence_key)
                    reconciliation["retained_reconciliation_evidence_keys"] = (
                        retained_keys
                    )
                if bool(payload.get("pending_backfill")):
                    reconciliation["pending_backfill"] = True
                    reconciliation["missing_leg"] = payload.get("missing_leg", "")
                    reconciliation["candidate_owner_id"] = payload.get("candidate_owner_id", "")
                    reconciliation["last_evidence_gap_reason"] = payload.get(
                        "evidence_gap_reason",
                        "",
                    )
                    reconciliation["last_partial_reconciled_at_ms"] = now_ms
                    if isinstance(exchange_truth, dict):
                        reconciliation["exchange_truth"] = dict(exchange_truth)
                        original_payload = reconciliation.get("original_payload")
                        if not isinstance(original_payload, dict):
                            original_payload = {}
                        original_payload["exchange_truth"] = dict(exchange_truth)
                        reconciliation["original_payload"] = original_payload
                    if contract is None:
                        contract = classify_close_reconciliation_state(
                            reconciliation,
                            current_exchange_truth_clean=(
                                close_reconciliation_exchange_truth_clean(
                                    reconciliation,
                                    current_exchange_truth=getattr(
                                        self.ctx,
                                        "_last_recovery_exchange_truth",
                                        None,
                                    ),
                                )
                            ),
                        )
                    reconciliation["close_reconciliation_state"] = str(
                        contract.get("state") or ""
                    )
                    if archive_reconciliation:
                        reconciliation["archived"] = True
                        reconciliation["archive_reason"] = str(
                            contract.get("state") or ""
                        )
                        reconciliation["business_contract_action"] = str(
                            contract.get("state") or ""
                        )
                        if truth_hash:
                            reconciliation["exchange_truth_hash"] = truth_hash
                        if archive_key:
                            reconciliation["archive_key"] = archive_key
                    else:
                        if self._has_statement_probe_candidates(reconciliation):
                            reconciliation["accounting_only_backfill"] = True
                            reconciliation["blocking_trading"] = False
                            reconciliation["archive_reconciliation"] = False
                            reconciliation["business_contract_action"] = str(
                                contract.get("state") or ""
                            )
                            if truth_hash:
                                reconciliation["exchange_truth_hash"] = truth_hash
                            reconciliation[
                                "unresolved_statement_probe_candidates"
                            ] = self._statement_probe_candidates_from_reconciliation(
                                reconciliation,
                                missing_leg=str(payload.get("missing_leg") or ""),
                            )
                        self._call_apply_pending_close_reconciliation_backoff(
                            reconciliation,
                            now_ms,
                        )
                        retained.append(reconciliation)
                    event_kind = (
                        "reconciliation.pending_close_backfill_archived"
                        if archive_reconciliation
                        else "reconciliation.pending_close_backfill_retained"
                    )
                    if emit_reconciled:
                        self.ctx.journal.append(
                            event_kind,
                            {
                                "position_id": reconciliation.get("position_id", ""),
                                "symbol": symbol,
                                "candidate_owner_id": payload.get(
                                    "candidate_owner_id",
                                    "",
                                ),
                                "missing_leg": payload.get("missing_leg", ""),
                                "evidence_gap_reason": payload.get(
                                    "evidence_gap_reason",
                                    "",
                                ),
                                "next_attempt_ms": reconciliation.get(
                                    "next_attempt_ms",
                                    0,
                                ),
                                "source": payload.get(
                                    "source",
                                    "pending_close_reconciliation",
                                ),
                                "close_reconciliation_state": str(
                                    contract.get("state") or ""
                                ),
                                "archive_reconciliation": archive_reconciliation,
                                "accounting_only_backfill": bool(
                                    reconciliation.get("accounting_only_backfill")
                                ),
                                "blocking_trading": bool(
                                    reconciliation.get("blocking_trading", True)
                                ),
                                "unresolved_statement_probe_candidates": list(
                                    reconciliation.get(
                                        "unresolved_statement_probe_candidates",
                                        [],
                                    )
                                    or []
                                ),
                                "exchange_truth_hash": truth_hash,
                            },
                        )
                        if archive_key:
                            self._close_reconciliation_archive_emitted_keys.add(
                                archive_key
                            )
                changed = True
                continue
            if long_fills == [] and short_fills == []:
                exchange_truth = self._current_exchange_truth_for_close_reconciliation(
                    reconciliation
                )
                if exchange_truth is not None:
                    evidence_gap_fields = close_reconciliation_evidence_fields(
                        long_quantity=0.0,
                        short_quantity=0.0,
                        duplicate_close_leg_suppressed_count=0,
                    )
                    long_legs = reconciliation.get("long_legs")
                    short_legs = reconciliation.get("short_legs")
                    payload = {
                        "position_id": reconciliation.get("position_id", ""),
                        "symbol": symbol,
                        "kind": reconciliation.get("kind", "final"),
                        "reason": reconciliation.get("reason", ""),
                        "closed_at_ms": int(
                            reconciliation.get("closed_at_ms") or now_ms
                        ),
                        "reconciled_at_ms": now_ms,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "long_closed_qty": 0.0,
                        "short_closed_qty": 0.0,
                        "long_legs": list(long_legs)
                        if isinstance(long_legs, list)
                        else [],
                        "short_legs": list(short_legs)
                        if isinstance(short_legs, list)
                        else [],
                        "venue_statement_reconciled": False,
                        "evidence_gap": True,
                        "candidate_owner_id": reconciliation.get("position_id", ""),
                        "missing_leg": "both",
                        "pending_backfill": True,
                        "accounting_status": "pending_backfill",
                        "clean_accounting_ready": False,
                        **evidence_gap_fields,
                        "duplicate_close_leg_suppressed_count": 0,
                        "duplicate_close_leg_suppressed_samples": [],
                        "source": reconciliation.get(
                            "source",
                            "pending_close_reconciliation",
                        ),
                    }
                    truth_hash = self._close_reconciliation_truth_hash(exchange_truth)
                    contract = classify_close_reconciliation_state(
                        payload,
                        current_exchange_truth_clean=True,
                    )
                    self._annotate_terminal_flat_accounting_gap_payload(
                        payload,
                        contract=contract,
                        exchange_truth=exchange_truth,
                        truth_hash=truth_hash,
                    )
                    archive_reconciliation = bool(
                        contract.get("archive_reconciliation") is True
                    )
                    if archive_reconciliation and self._has_statement_probe_candidates(
                        reconciliation
                    ):
                        archive_reconciliation = False
                    if archive_reconciliation:
                        archive_key = self._close_reconciliation_archive_key(
                            payload,
                            truth_hash=truth_hash,
                        )
                        archive_already_emitted = (
                            archive_key
                            in self._close_reconciliation_archive_emitted_keys
                        )
                        reconciliation["pending_backfill"] = True
                        reconciliation["missing_leg"] = "both"
                        reconciliation["candidate_owner_id"] = payload.get(
                            "candidate_owner_id",
                            "",
                        )
                        reconciliation["last_evidence_gap_reason"] = payload.get(
                            "evidence_gap_reason",
                            "",
                        )
                        reconciliation["last_partial_reconciled_at_ms"] = now_ms
                        reconciliation["close_reconciliation_state"] = str(
                            contract.get("state") or ""
                        )
                        reconciliation["archived"] = True
                        reconciliation["archive_reason"] = str(
                            contract.get("state") or ""
                        )
                        reconciliation["business_contract_action"] = str(
                            contract.get("state") or ""
                        )
                        reconciliation["exchange_truth"] = dict(exchange_truth)
                        if truth_hash:
                            reconciliation["exchange_truth_hash"] = truth_hash
                        if archive_key:
                            reconciliation["archive_key"] = archive_key
                        original_payload = reconciliation.get("original_payload")
                        if not isinstance(original_payload, dict):
                            original_payload = {}
                        original_payload["exchange_truth"] = dict(exchange_truth)
                        reconciliation["original_payload"] = original_payload
                        if not archive_already_emitted:
                            self.ctx.journal.append(
                                "reconciliation.pending_close_backfill_archived",
                                {
                                    "position_id": reconciliation.get(
                                        "position_id",
                                        "",
                                    ),
                                    "symbol": symbol,
                                    "candidate_owner_id": payload.get(
                                        "candidate_owner_id",
                                        "",
                                    ),
                                    "missing_leg": payload.get("missing_leg", ""),
                                    "evidence_gap_reason": payload.get(
                                        "evidence_gap_reason",
                                        "",
                                    ),
                                    "next_attempt_ms": reconciliation.get(
                                        "next_attempt_ms",
                                        0,
                                    ),
                                    "source": payload.get(
                                        "source",
                                        "pending_close_reconciliation",
                                    ),
                                    "close_reconciliation_state": str(
                                        contract.get("state") or ""
                                    ),
                                    "archive_reconciliation": True,
                                    "exchange_truth_hash": truth_hash,
                                },
                            )
                            if archive_key:
                                self._close_reconciliation_archive_emitted_keys.add(
                                    archive_key
                                )
                        changed = True
                        continue
                    if self._has_statement_probe_candidates(reconciliation):
                        self._retain_terminal_flat_accounting_backfill(
                            reconciliation,
                            now_ms=now_ms,
                            exchange_truth=exchange_truth,
                            state=str(contract.get("state") or ""),
                            symbol=symbol,
                            missing_leg=str(payload.get("missing_leg") or ""),
                            evidence_gap_reason=str(
                                payload.get("evidence_gap_reason") or ""
                            ),
                            truth_hash=truth_hash,
                            source=str(
                                payload.get(
                                    "source",
                                    "pending_close_reconciliation",
                                )
                            ),
                        )
                        self._call_apply_pending_close_reconciliation_backoff(
                            reconciliation,
                            now_ms,
                        )
                        retained.append(reconciliation)
                        changed = True
                        continue
                self.ctx.journal.append(
                    "reconciliation.pending_close_reconciliation_invalid",
                    {
                        "position_id": reconciliation.get("position_id", ""),
                        "symbol": symbol,
                        "reason": "confirmed_no_close_fill",
                    },
                )
                self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
                retained.append(reconciliation)
                changed = True
                continue

            abandoned = await self._call_try_abandon_stale_pending_close_reconciliation(
                reconciliation,
                now_ms,
                symbol=symbol,
                long_venue=long_venue,
                short_venue=short_venue,
                error="close fill reconciliation not yet available",
            )
            if abandoned:
                changed = True
                continue

            self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
            retained.append(reconciliation)
            changed = True

        self.ctx.state.pending_close_reconciliations = retained
        if changed:
            active_empty = not self.ctx.state.open_positions
            pending_entries_empty = not self.ctx.state.pending_entries
            pending_passive_empty = not self.ctx.state.pending_passive_closes
            pending_reconciliations_empty = not self.ctx.state.pending_close_reconciliations
            fail_closed = (
                self.ctx.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
                or self.ctx.state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED
            )
            if (
                active_empty
                and pending_reconciliations_empty
                and pending_entries_empty
                and pending_passive_empty
            ):
                set_lifecycle(self.ctx.state, EngineLifecycle.RUNNING)
                self.ctx.state.last_error = None
            elif fail_closed:
                set_lifecycle(self.ctx.state, EngineLifecycle.RISK_ONLY)
                self.ctx.state.last_error = "pending_close_reconciliations_fail_closed"
            elif active_empty or self._call_open_positions_private_confirmation_ready():
                set_lifecycle(self.ctx.state, EngineLifecycle.RUNNING)
                self.ctx.state.last_error = None
            elif self.ctx.state.pending_close_reconciliations:
                set_lifecycle(self.ctx.state, EngineLifecycle.RISK_ONLY)
                self.ctx.state.last_error = "pending_close_reconciliations_active"

    def _exit_shadow_config(self) -> ExitShadowConfig:
        strategy = self.ctx.config.strategy
        horizons = tuple(
            int(value)
            for value in getattr(strategy, "exit_shadow_markout_horizons_ms", []) or []
            if int(value) > 0
        )
        take_profit_bps = tuple(
            float(value)
            for value in getattr(strategy, "exit_shadow_take_profit_bps", []) or []
            if float(value) > 0.0
        )
        return ExitShadowConfig(
            enabled=bool(getattr(strategy, "exit_shadow_enabled", False)),
            markout_horizons_ms=horizons or (1000, 2000, 5000),
            take_profit_bps=take_profit_bps or (10.0, 20.0),
            adverse_stop_bps=float(
                getattr(strategy, "exit_shadow_adverse_stop_bps", 3.0) or 3.0
            ),
            max_quote_age_ms=int(
                getattr(strategy, "exit_shadow_max_quote_age_ms", 1000) or 1000
            ),
            max_l2_age_ms=int(
                getattr(strategy, "exit_shadow_max_l2_age_ms", 1000) or 1000
            ),
            cost_buffer_bps=float(
                getattr(strategy, "exit_shadow_cost_buffer_bps", 3.0) or 3.0
            ),
        )

    def _exit_shadow_tracker_for_config(
        self,
        config: ExitShadowConfig,
    ) -> ExitShadowTracker | None:
        if not config.enabled:
            return None
        if self._exit_shadow_tracker is None:
            self._exit_shadow_tracker = ExitShadowTracker(config)
        else:
            self._exit_shadow_tracker.config = config
        return self._exit_shadow_tracker

    def _append_exit_shadow_events(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            kind = str(event.get("kind", "") or "")
            payload = event.get("payload", {})
            if kind and isinstance(payload, dict):
                self.ctx.journal.append(kind, payload)

    def _exit_shadow_quote(self, venue, symbol: str, now_ms: int) -> ExitShadowQuote | None:
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        cache = getattr(self.ctx, "ws_bbo_cache", None)
        if cache is not None and hasattr(cache, "get_quote"):
            try:
                quote = cache.get_quote(venue_value, symbol)
            except Exception:
                quote = None
            if quote is not None:
                return ExitShadowQuote(
                    venue=str(getattr(quote, "venue", venue_value) or venue_value),
                    symbol=str(getattr(quote, "symbol", symbol) or symbol),
                    bid=float(getattr(quote, "bid", 0.0) or 0.0),
                    ask=float(getattr(quote, "ask", 0.0) or 0.0),
                    bid_size=float(getattr(quote, "bid_size", 0.0) or 0.0),
                    ask_size=float(getattr(quote, "ask_size", 0.0) or 0.0),
                    observed_at_ms=int(getattr(quote, "observed_at_ms", 0) or 0),
                    source=str(getattr(quote, "source", "") or "ws_bbo_cache"),
                )
        book = self._exit_shadow_book(venue, symbol)
        if book is None:
            return None
        best_bid = float(book.best_bid() or 0.0)
        best_ask = float(book.best_ask() or 0.0)
        if best_bid <= 0.0 or best_ask <= best_bid:
            return None
        bid_size = float(getattr(book.bids[0], "quantity", 0.0) or 0.0) if book.bids else 0.0
        ask_size = float(getattr(book.asks[0], "quantity", 0.0) or 0.0) if book.asks else 0.0
        return ExitShadowQuote(
            venue=venue_value,
            symbol=symbol,
            bid=best_bid,
            ask=best_ask,
            bid_size=bid_size,
            ask_size=ask_size,
            observed_at_ms=int(getattr(book, "observed_at_ms", 0) or 0),
            source=str(getattr(book, "source", "") or "local_l2"),
        )

    def _exit_shadow_book(self, venue, symbol: str):
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        local_l2_runtime = getattr(self.ctx, "local_l2_runtime", None)
        get_book = getattr(local_l2_runtime, "get_book", None)
        if not callable(get_book):
            return None
        try:
            return get_book(venue_value, symbol)
        except Exception:
            return None

    def _exit_shadow_market(self, position, now_ms: int) -> ExitShadowMarket:
        return ExitShadowMarket(
            long_quote=self._exit_shadow_quote(position.long_venue, position.symbol, now_ms),
            short_quote=self._exit_shadow_quote(position.short_venue, position.symbol, now_ms),
            long_book=self._exit_shadow_book(position.long_venue, position.symbol),
            short_book=self._exit_shadow_book(position.short_venue, position.symbol),
            now_ms=now_ms,
        )

    def _record_exit_shadow_trigger(self, position, reason: str, now_ms: int) -> str:
        config = self._exit_shadow_config()
        tracker = self._exit_shadow_tracker_for_config(config)
        if tracker is None:
            return ""
        try:
            snapshot = ExitShadowSnapshot(
                position=position,
                reason=reason,
                market=self._exit_shadow_market(position, now_ms),
            )
            events = tracker.on_close_trigger(snapshot)
            shadow_id = ""
            for event in events:
                payload = event.get("payload", {})
                if isinstance(payload, dict) and payload.get("shadow_id"):
                    shadow_id = str(payload.get("shadow_id") or "")
                    break
            if shadow_id:
                setattr(position, "exit_shadow_id", shadow_id)
            self._append_exit_shadow_events(events)
            return shadow_id
        except Exception:
            return ""

    def _emit_due_exit_shadow_markouts(self, now_ms: int) -> None:
        config = self._exit_shadow_config()
        tracker = self._exit_shadow_tracker_for_config(config)
        if tracker is None:
            return
        try:
            events: list[dict[str, Any]] = []
            for shadow_id, snapshot in tracker.pending_items():
                market = self._exit_shadow_market(snapshot.position, now_ms)
                events.extend(tracker.evaluate_markouts_for_shadow(shadow_id, market))
            self._append_exit_shadow_events(events)
        except Exception:
            return

    async def _maybe_process_normal_exits(self, now_ms: int) -> None:
        """Evaluate normal exit reasons for open positions and route to close path.

        V1: standard_close_reason() identifies which positions should close.
        normal_close_reason_uses_passive_maker_taker() determines the close path:
        - passive close: funding_capture, trailing_exit, first_stage_capture,
          second_stage_capture, settlement_half_close, settlement_force_close
        - aggressive close: hard_stop, risk_delever, protection

        This method CONSUMES the predicate that was previously only unit-tested.
        """
        from lightfee.engine.exit_decision import (
            force_close_due,
            normal_close_reason_uses_passive_maker_taker,
            standard_close_reason,
            update_position_funding_capture_state,
        )
        from lightfee.engine.exit import ExitReason

        self._emit_due_exit_shadow_markouts(now_ms)

        if not self.ctx.state.open_positions:
            return

        for position in list(self.ctx.state.open_positions.values()):
            # Skip positions already in passive close
            if position.position_id in self.ctx.state.pending_passive_closes:
                continue

            staggered_exit_mode = str(
                getattr(self.ctx.config.strategy, "staggered_exit_mode", "") or ""
            ).lower()
            if (
                position.opportunity_type == "staggered"
                and staggered_exit_mode == "after_first_stage"
                and not position.exit_after_first_stage
            ):
                position.exit_after_first_stage = True
                self.ctx.journal.append(
                    "runtime.staggered_exit_mode_backfilled",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "opportunity_type": position.opportunity_type,
                        "staggered_exit_mode": staggered_exit_mode,
                        "exit_after_first_stage": True,
                        "source": "strategy_config",
                        "ts_ms": now_ms,
                    },
                )

            funding_captured_before = position.funding_captured
            second_stage_before = position.second_stage_funding_captured
            post_funding_hold_ms = int(
                getattr(self.ctx.config.strategy, "post_funding_hold_secs", 0) or 0
            ) * 1000
            update_position_funding_capture_state(
                position,
                now_ms,
                post_funding_hold_ms,
            )
            if (
                position.funding_captured != funding_captured_before
                or position.second_stage_funding_captured != second_stage_before
            ):
                self.ctx.journal.append(
                    "runtime.funding_capture_state_updated",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "opportunity_type": position.opportunity_type,
                        "funding_timestamp_ms": position.funding_timestamp_ms,
                        "second_funding_timestamp_ms": position.second_funding_timestamp_ms,
                        "post_funding_hold_ms": post_funding_hold_ms,
                        "funding_captured_before": funding_captured_before,
                        "funding_captured_after": position.funding_captured,
                        "second_stage_funding_captured_before": second_stage_before,
                        "second_stage_funding_captured_after": (
                            position.second_stage_funding_captured
                        ),
                        "exit_after_first_stage": position.exit_after_first_stage,
                        "ts_ms": now_ms,
                    },
                )

            reason = (
                ExitReason.SETTLEMENT_FORCE_CLOSE
                if force_close_due(position, self.ctx.config.strategy, now_ms)
                else standard_close_reason(position, self.ctx.config.strategy, now_ms)
            )
            if reason is None:
                continue

            reason_str = reason.value if hasattr(reason, 'value') else str(reason)
            exit_shadow_id = self._record_exit_shadow_trigger(position, reason_str, now_ms)

            if normal_close_reason_uses_passive_maker_taker(reason_str):
                # Route to passive close
                if self.ctx.passive_close_executor is not None:
                    await self._rewarm_close_price_evidence(
                        [
                            (
                                position.long_venue.value
                                if hasattr(position.long_venue, "value")
                                else str(position.long_venue),
                                position.symbol,
                            ),
                            (
                                position.short_venue.value
                                if hasattr(position.short_venue, "value")
                                else str(position.short_venue),
                                position.symbol,
                            ),
                        ],
                        now_ms=now_ms,
                    )
                    self.ctx.journal.append(
                        "runtime.normal_close_routing_passive",
                        {
                            "position_id": position.position_id,
                            "reason": reason_str,
                            "matched_quantity": position.matched_quantity,
                            "exit_shadow_id": exit_shadow_id,
                        },
                    )
                    pending = await self.ctx.passive_close_executor.start_pending_passive_close(
                        self.ctx.state,
                        position,
                        reason_str,
                        long_price_hint=self._call_resolve_local_l2_mid(position.long_venue, position.symbol, now_ms=now_ms),
                        short_price_hint=self._call_resolve_local_l2_mid(position.short_venue, position.symbol, now_ms=now_ms),
                        short_stage="exit_short",
                        long_stage="exit_long",
                        exit_shadow_id=exit_shadow_id,
                    )
                    if pending is not None:
                        # Immediately drive one cycle
                        await self.ctx.passive_close_executor.drive_pending_passive_close(
                            self.ctx.state, position.position_id, wait_until_terminal=False,
                        )
            else:
                # Route to aggressive close (hard_stop, risk, etc.)
                if self.ctx.close_executor is not None:
                    self.ctx.journal.append(
                        "runtime.normal_close_routing_aggressive",
                        {
                            "position_id": position.position_id,
                            "reason": reason_str,
                            "matched_quantity": position.matched_quantity,
                            "exit_shadow_id": exit_shadow_id,
                        },
                    )
                    await self.ctx.close_executor.execute_close(
                        position, reason_str, now_ms,
                        long_price_hint=self._call_resolve_local_l2_mid(position.long_venue, position.symbol, now_ms=now_ms),
                        short_price_hint=self._call_resolve_local_l2_mid(position.short_venue, position.symbol, now_ms=now_ms),
                        state=self.ctx.state,
                    )
    def _resolve_ws_bbo_close_mid(self, venue_value: str, symbol: str, now_ms: int) -> float:
        """Resolve a close price hint from the active WS BBO quote provider."""
        if not self._entry_readiness_provider_uses_ws_bbo():
            return 0.0

        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "quote_lease_budget_unavailable",
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return 0.0

        try:
            cache = self.ctx.ws_bbo_cache
            if cache is None or not hasattr(cache, "get_quote"):
                self.ctx.journal.append(
                    "runtime.close_price_evidence_missing",
                    {
                        "venue": venue_value,
                        "symbol": symbol,
                        "domain": "ws_bbo_cache",
                        "reason": "cache_unavailable",
                        "budget_ms": budget_ms,
                        "decision": "reject_price_hint",
                        "fallback_source": "none",
                        "provider": "ws_bbo_quote_lease",
                        "ts_ms": now_ms,
                    },
                )
                return 0.0
            quote = cache.get_quote(venue_value, symbol)
            if quote is None:
                self.ctx.journal.append(
                    "runtime.close_price_evidence_missing",
                    {
                        "venue": venue_value,
                        "symbol": symbol,
                        "domain": "ws_bbo_cache",
                        "reason": "missing_quote",
                        "budget_ms": budget_ms,
                        "decision": "reject_price_hint",
                        "fallback_source": "none",
                        "provider": "ws_bbo_quote_lease",
                        "ts_ms": now_ms,
                    },
                )
                return 0.0

            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            if (
                observed_at_ms <= 0
                or age_ms is None
                or age_ms > budget_ms
                or bid <= 0.0
                or ask <= bid
            ):
                self.ctx.journal.append(
                    "runtime.close_price_evidence_stale",
                    {
                        "venue": venue_value,
                        "symbol": symbol,
                        "domain": "ws_bbo_cache",
                        "age_ms": age_ms,
                        "budget_ms": budget_ms,
                        "decision": "reject_price_hint",
                        "fallback_source": "none",
                        "provider": "ws_bbo_quote_lease",
                        "ts_ms": now_ms,
                    },
                )
                return 0.0

            mid = (bid + ask) / 2.0
            self.ctx.journal.append(
                "runtime.close_price_evidence_ws_bbo_used",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "source": str(getattr(quote, "source", "") or "ws_bbo_cache"),
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "observed_at_ms": observed_at_ms,
                    "age_ms": age_ms,
                    "budget_ms": budget_ms,
                    "decision": "use_price_hint",
                    "outcome": "used_fresh_ws_bbo",
                    "provider": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return mid
        except Exception as exc:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "fallback_error",
                    "error": f"{type(exc).__name__}: {exc}"[:240],
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return 0.0
    def _resolve_local_l2_mid(self, venue, symbol: str, now_ms: int | None = None) -> float:
        """Get mid price from local L2 book or active close-price fallback for venue+symbol."""
        if now_ms is None:
            now_ms = wall_clock_now_ms()
        venue_value = venue.value if hasattr(venue, 'value') else str(venue)
        if self._entry_readiness_provider_uses_ws_bbo():
            return self._call_resolve_ws_bbo_close_mid(venue_value, symbol, now_ms)
        budget_ms = int(self.ctx.config.strategy.max_liquidity_snapshot_age_ms or 0)
        try:
            book = self.ctx.local_l2_runtime.get_book(venue_value, symbol)
            if book is not None and book.status.value == "hot":
                age_ms = book.age_ms(now_ms)
                if budget_ms > 0 and book.is_stale(budget_ms, now_ms):
                    self.ctx.journal.append(
                        "runtime.close_price_evidence_stale",
                        {
                            "venue": venue_value,
                            "symbol": symbol,
                            "domain": "local_l2_book",
                            "age_ms": age_ms,
                            "budget_ms": budget_ms,
                            "decision": "reject_price_hint",
                            "fallback_source": "none",
                            "ts_ms": now_ms,
                        },
                    )
                    return 0.0
                mid = book.mid_price()
                if mid and mid > 0:
                    return mid
        except Exception:
            pass
        return 0.0
    def _resolve_local_l2_quote(self, venue, symbol: str) -> tuple[float, float] | None:
        """Get best bid/ask from the local L2 book for passive tick inference."""
        if self._entry_readiness_provider_uses_ws_bbo():
            return self._call_resolve_ws_bbo_close_quote(venue, symbol)
        if not self._entry_readiness_provider_uses_local_l2():
            return None
        try:
            book = self.ctx.local_l2_runtime.get_book(
                venue.value if hasattr(venue, "value") else str(venue),
                symbol,
            )
            if book is not None and book.status.value == "hot":
                best_bid = book.best_bid()
                best_ask = book.best_ask()
                if best_bid > 0 and best_ask > best_bid:
                    return best_bid, best_ask
        except Exception:
            pass
        return None
    def _resolve_ws_bbo_close_quote(
        self,
        venue,
        symbol: str,
        now_ms: int | None = None,
    ) -> tuple[float, float] | None:
        if not self._entry_readiness_provider_uses_ws_bbo():
            return None
        if now_ms is None:
            now_ms = wall_clock_now_ms()
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "quote_lease_budget_unavailable",
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "source": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return None
        try:
            quote = self.ctx.ws_bbo_cache.get_quote(venue_value, symbol)
        except Exception as exc:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "fallback_error",
                    "error": f"{type(exc).__name__}: {exc}"[:240],
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "source": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return None
        if quote is None:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "missing_quote",
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "source": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return None
        try:
            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
        except Exception as exc:
            self.ctx.journal.append(
                "runtime.close_price_evidence_missing",
                {
                    "venue": venue_value,
                    "symbol": symbol,
                    "domain": "ws_bbo_cache",
                    "reason": "quote_parse_error",
                    "error": f"{type(exc).__name__}: {exc}"[:240],
                    "budget_ms": budget_ms,
                    "decision": "reject_price_hint",
                    "fallback_source": "none",
                    "provider": "ws_bbo_quote_lease",
                    "source": "ws_bbo_quote_lease",
                    "ts_ms": now_ms,
                },
            )
            return None
        if (
            observed_at_ms > 0
            and age_ms is not None
            and budget_ms > 0
            and age_ms <= budget_ms
            and bid > 0.0
            and ask > bid
        ):
            return bid, ask
        self.ctx.journal.append(
            "runtime.close_price_evidence_stale",
            {
                "venue": venue_value,
                "symbol": symbol,
                "domain": "ws_bbo_cache",
                "reason": (
                    "quote_stale"
                    if age_ms is not None and age_ms > budget_ms
                    else "invalid_quote"
                ),
                "observed_at_ms": observed_at_ms,
                "age_ms": age_ms,
                "budget_ms": budget_ms,
                "decision": "reject_price_hint",
                "fallback_source": "none",
                "provider": "ws_bbo_quote_lease",
                "source": "ws_bbo_quote_lease",
                "ts_ms": now_ms,
            },
        )
        return None
    def _resolve_close_price_hint_mid_with_source(self, venue, symbol: str):
        if self._entry_readiness_provider_uses_ws_bbo():
            now_ms = wall_clock_now_ms()
            venue_value = venue.value if hasattr(venue, "value") else str(venue)
            return (
                self._call_resolve_ws_bbo_close_mid(venue_value, symbol, now_ms),
                "ws_bbo_quote_lease",
            )
        return self._call_resolve_local_l2_mid(venue, symbol), "local_l2"
    def _resolve_close_price_hint_quote_with_source(self, venue, symbol: str):
        if self._entry_readiness_provider_uses_ws_bbo():
            quote = self._call_resolve_ws_bbo_close_quote(venue, symbol)
            if quote is None:
                return None
            return quote[0], quote[1], "ws_bbo_quote_lease"
        quote = self._call_resolve_local_l2_quote(venue, symbol)
        if quote is None:
            return None
        return quote[0], quote[1], "local_l2"
