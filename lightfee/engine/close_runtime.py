"""Close runtime delegate.

This module owns close reconciliation and normal-exit helpers mechanically moved
from LiveRuntime. Keep journal events, payload keys, and close semantics stable.
"""

from __future__ import annotations

import math
from typing import Any

from lightfee.core.domain import Venue
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.lifecycle import set_lifecycle
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER
from lightfee.engine.reconciliation import _recon_fill_price
from lightfee.engine.runtime_context import CloseRuntimeContext
from lightfee.engine.state import (
    is_unattributed_recovered_live_flat_reconciliation,
    pending_close_reconciliation_evidence_debt_reason,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class CloseRuntime:
    # V1 reconciliation retry constants (Rust V1 recovery.rs)
    _RECONCILE_RETRY_BASE_MS = 30_000
    _RECONCILE_RETRY_MAX_MS = 300_000
    _RECONCILE_HARD_DEADLINE_MS = 600_000  # 10 min hard deadline

    def __init__(self, ctx: CloseRuntimeContext) -> None:
        self.ctx = ctx

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        return self.ctx._flush_adapter_order_diagnostics(adapter)

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

    def _call_resolve_local_l2_mid(self, *args, **kwargs) -> float:
        override = self._runtime_method_override("_resolve_local_l2_mid")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_local_l2_mid(*args, **kwargs)

    def _call_resolve_local_l2_quote(self, *args, **kwargs):
        override = self._runtime_method_override("_resolve_local_l2_quote")
        if override is not None:
            return override(*args, **kwargs)
        return self._resolve_local_l2_quote(*args, **kwargs)

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
    @staticmethod
    def _close_reconciliation_leg_identity(leg: Any) -> tuple[str, str]:
        if not isinstance(leg, dict):
            return "", ""
        order_id = str(leg.get("order_id") or "")
        client_order_id = str(leg.get("client_order_id") or "")
        return order_id, "" if order_id else client_order_id
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
    def _safe_reconciliation_int(raw: Any, default: int = 0) -> int:
        try:
            return int(raw)
        except (TypeError, ValueError, OverflowError):
            return default

    @staticmethod
    def _safe_reconciliation_float(raw: Any, default: float = 0.0) -> float:
        try:
            value = float(raw)
        except (TypeError, ValueError, OverflowError):
            return default
        return value if math.isfinite(value) else default
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
        fetch = getattr(adapter, "fetch_order_fill_reconciliation", None)
        if not callable(fetch):
            return None

        fills: list[Any] = []
        seen: set[tuple[str, str]] = set()
        for leg in legs:
            order_id, client_order_id = self._close_reconciliation_leg_identity(leg)
            if not order_id and not client_order_id:
                return None
            identity = (order_id, client_order_id)
            if identity in seen:
                continue
            seen.add(identity)
            fill = await fetch(symbol, order_id, client_order_id)
            self._flush_adapter_order_diagnostics(adapter)
            if fill is None:
                return None
            fills.append(fill)
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
        position_id = str(reconciliation.get("position_id") or "")
        if any(
            str(getattr(position, "position_id", "")) == position_id
            for position in self.ctx.state.open_positions.values()
        ):
            return False

        next_attempt_count = self._safe_reconciliation_int(
            reconciliation.get("attempt_count")
        ) + 1
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
                "closed_at_ms": self._safe_reconciliation_int(
                    reconciliation.get("closed_at_ms")
                ),
                "attempt_count": next_attempt_count,
                "terminal_reason": "fill_reconciliation_unavailable_after_terminal_budget",
                "error": error,
                "lifetime_ms": max(
                    0,
                    now_ms
                    - max(
                        0,
                        self._safe_reconciliation_int(
                            reconciliation.get("closed_at_ms")
                        ),
                    ),
                ),
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "long_live_size": long_live_size,
                "short_live_size": short_live_size,
            },
        )
        return True

    def _mark_pending_close_reconciliation_evidence_debt(
        self,
        reconciliation: dict[str, Any],
        *,
        reason: str,
        now_ms: int,
        symbol: str = "",
        long_venue: Venue | None = None,
        short_venue: Venue | None = None,
    ) -> bool:
        """Persist one non-retryable close-accounting evidence debt.

        A task without its typed position routing or close-order identity cannot
        gain that fact by polling execution history again.  Keep it as a
        visible close-work owner, but transition it out of the retry loop once.
        The journal includes the full task so a crash before the next snapshot
        preserves the state transition during recovery.
        """
        if reconciliation.get("reconciliation_status") == "evidence_debt":
            return False

        snapshot = reconciliation.get("position_snapshot") or {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        resolved_symbol = str(symbol or reconciliation.get("symbol") or snapshot.get("symbol") or "")
        reconciliation["reconciliation_status"] = "evidence_debt"
        reconciliation["evidence_debt_reason"] = reason
        reconciliation["evidence_debt_at_ms"] = now_ms
        reconciliation["next_attempt_ms"] = 0
        reconciliation["billing_reconciliation_required"] = True
        if reason == "missing_close_order_identity":
            reconciliation["missing_close_order_identity"] = True

        def _leg_count(name: str) -> int:
            legs = reconciliation.get(name)
            return len(legs) if isinstance(legs, list) else 0

        self.ctx.journal.append_critical(
            now_ms,
            "exit.billing_evidence_debt_registered",
            {
                "position_id": str(
                    reconciliation.get("position_id") or snapshot.get("position_id") or ""
                ),
                "symbol": resolved_symbol,
                "kind": str(reconciliation.get("kind") or "final"),
                "source": str(reconciliation.get("source") or ""),
                "closed_at_ms": self._safe_reconciliation_int(
                    reconciliation.get("closed_at_ms")
                ),
                "terminal_accounting_status": "evidence_debt_requires_operator",
                "terminal_reason": reason,
                "reconciliation_status": "evidence_debt",
                "operator_action": "supply_typed_snapshot_and_close_leg_identity",
                "billing_reconciliation_required": True,
                "long_venue": (
                    long_venue.value
                    if long_venue is not None
                    else str(reconciliation.get("long_venue") or snapshot.get("long_venue") or "")
                ),
                "short_venue": (
                    short_venue.value
                    if short_venue is not None
                    else str(reconciliation.get("short_venue") or snapshot.get("short_venue") or "")
                ),
                "long_leg_count": _leg_count("long_legs"),
                "short_leg_count": _leg_count("short_legs"),
                "reconciliation": dict(reconciliation),
            },
        )
        return True

    async def _probe_venue_open_order_flat(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[bool | None, str | None]:
        """Return (flat_or_None, evidence) for a venue's open-order truth.

        (True, None) — trusted flat; (False, evidence) — trusted non-flat;
        (None, error) — truth query failed or unsupported (untrusted).  Uses the
        shared strict probe so an unknown/empty response is never treated as
        proven flat.
        """
        adapter = self.ctx.venue_adapters.get(venue)
        if adapter is None:
            return None, "adapter_missing"
        from lightfee.engine.exchange_truth import probe_venue_open_orders_flat

        return await probe_venue_open_orders_flat(adapter, venue, symbol)

    async def _fetch_pending_close_terminal_live_flat_truth(
        self,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
    ) -> tuple[tuple[float, float] | None, str | None]:
        """Probe both venues' position + open-order truth for a pending close.

        Returns ((long_size, short_size), open_order_evidence) when both
        position probes succeed; None if any position probe fails.  Open-order
        truth is separately reported: None evidence when both venues report
        trusted flat, or a string describing the blocker otherwise.
        """
        terminal_sizes = await self._call_fetch_pending_close_terminal_live_sizes(
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
        )
        if terminal_sizes is None:
            return None, None
        long_oo_flat, long_oo_evidence = await self._probe_venue_open_order_flat(
            long_venue, symbol
        )
        short_oo_flat, short_oo_evidence = await self._probe_venue_open_order_flat(
            short_venue, symbol
        )
        if long_oo_flat is not True or short_oo_flat is not True:
            evidence = (
                long_oo_evidence
                if long_oo_flat is not True
                else short_oo_evidence
            )
            if evidence is None:
                evidence = "open_orders_flat"
            return terminal_sizes, evidence
        return terminal_sizes, None

    async def _try_terminalize_billing_evidence_gap(
        self,
        reconciliation: dict[str, Any],
        payload: dict[str, Any],
        now_ms: int,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
    ) -> bool:
        """End a physically proven close whose fee or fill evidence is unavailable.

        Retrying cannot manufacture missing historical entry- or exit-fee evidence.
        This is only terminal after both exchange legs are proved flat, including
        open-order truth; the resulting event deliberately remains financially
        provisional.  The close-quantity-incomplete branch (one leg has no close
        fill quantity) is provisional for the same reason.
        """
        if str(reconciliation.get("kind") or "final") != "final":
            return False
        position_id = str(reconciliation.get("position_id") or "")
        if any(
            str(getattr(position, "position_id", "")) == position_id
            for position in self.ctx.state.open_positions.values()
        ):
            return False
        terminal_sizes, open_order_evidence = (
            await self._fetch_pending_close_terminal_live_flat_truth(
                symbol=symbol,
                long_venue=long_venue,
                short_venue=short_venue,
            )
        )
        if terminal_sizes is None:
            return False
        long_live_size, short_live_size = terminal_sizes
        if abs(long_live_size) > 1e-9 or abs(short_live_size) > 1e-9:
            return False
        if open_order_evidence is not None:
            return False

        close_quantity_evidence_complete = (
            payload.get("close_quantity_evidence_complete") is True
        )
        if close_quantity_evidence_complete:
            entry_fee_evidence_complete = (
                payload.get("entry_fee_evidence_complete") is True
            )
            exit_fee_evidence_complete = (
                payload.get("exit_fee_evidence_complete") is True
            )
            if not entry_fee_evidence_complete and not exit_fee_evidence_complete:
                terminal_reason = (
                    "entry_and_exit_fee_evidence_unavailable_after_confirmed_flat_close"
                )
                terminal_accounting_status = (
                    "provisional_entry_and_exit_fee_evidence_unavailable"
                )
            elif not exit_fee_evidence_complete:
                terminal_reason = (
                    "exit_fee_evidence_unavailable_after_confirmed_flat_close"
                )
                terminal_accounting_status = (
                    "provisional_exit_fee_evidence_unavailable"
                )
            else:
                terminal_reason = (
                    "entry_fee_evidence_unavailable_after_confirmed_flat_close"
                )
                terminal_accounting_status = (
                    "provisional_entry_fee_evidence_unavailable"
                )
            terminal_payload = dict(payload)
        else:
            terminal_reason = (
                "terminal_live_flat_incomplete_close_quantity_evidence"
            )
            terminal_accounting_status = (
                "provisional_close_quantity_evidence_incomplete"
            )
            # One leg has no close-fill evidence, so any computed price/fee/PnL
            # would be manufactured.  Carry only known quantities and identities;
            # never emit a guessed net_quote or realized PnL.
            terminal_payload = {
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "net_quote",
                    "net_quote_status",
                    "price_pnl",
                    "funding_pnl_quote",
                    "entry_fee_quote",
                    "exit_fee_quote",
                    "long_average_price",
                    "short_average_price",
                }
            }
            terminal_payload["net_quote_status"] = "provisional"

        def recovery_targets(raw_legs: Any) -> list[dict[str, str]]:
            if not isinstance(raw_legs, list):
                return []
            return [
                {
                    "venue": str(leg.get("venue") or ""),
                    "order_id": str(leg.get("order_id") or ""),
                    "client_order_id": str(leg.get("client_order_id") or ""),
                }
                for leg in raw_legs
                if isinstance(leg, dict)
                and (leg.get("order_id") or leg.get("client_order_id"))
            ]

        long_targets = recovery_targets(terminal_payload.get("long_legs"))
        short_targets = recovery_targets(terminal_payload.get("short_legs"))
        terminal_payload["billing_reconciliation_required"] = True
        terminal_payload["billing_reconciliation_targets"] = {
            "long": long_targets,
            "short": short_targets,
        }
        terminal_payload["close_order_identity_available"] = bool(
            long_targets or short_targets
        )
        terminal_payload["close_order_identity_complete"] = bool(
            long_targets and short_targets
        )
        self.ctx.journal.append_critical(
            now_ms,
            "exit.billing_evidence_unavailable",
            {
                **terminal_payload,
                "terminal_accounting_status": terminal_accounting_status,
                "terminal_reason": terminal_reason,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "long_live_size": long_live_size,
                "short_live_size": short_live_size,
                "open_order_truth_flat": open_order_evidence is None,
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
            fee = None
            try:
                candidate_fee = float(getattr(fill, "fee_quote", None))
                if math.isfinite(candidate_fee) and candidate_fee >= 0.0:
                    fee = candidate_fee
            except (TypeError, ValueError):
                pass
            qty += leg_qty
            notional += leg_qty * price
            fee_quote += fee or 0.0
            leg_payloads.append({
                "venue": getattr(getattr(fill, "venue", ""), "value", getattr(fill, "venue", "")),
                "order_id": getattr(fill, "order_id", "") or "",
                "client_order_id": getattr(fill, "client_order_id", None) or "",
                "quantity": leg_qty,
                "average_price": price,
                "fee_quote": fee,
                "filled_at_ms": int(getattr(fill, "filled_at_ms", 0) or 0),
            })
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
        order_id = str(getattr(fill, "order_id", "") or "")
        client_order_id = str(getattr(fill, "client_order_id", None) or "")
        return (
            str(position_id or ""),
            str(leg or ""),
            str(venue or "").lower(),
            order_id,
            "" if order_id else client_order_id,
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
        attempt = self._safe_reconciliation_int(
            reconciliation.get("attempt_count")
        ) + 1
        reconciliation["attempt_count"] = attempt
        delay = min(
            self._RECONCILE_RETRY_BASE_MS * (2 ** max(attempt - 1, 0)),
            self._RECONCILE_RETRY_MAX_MS,
        )
        reconciliation["next_attempt_ms"] = now_ms + delay
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
        long_entry = float(snapshot.get("long_entry_price") or 0.0)
        short_entry = float(snapshot.get("short_entry_price") or 0.0)
        funding_quote = float(snapshot.get("captured_funding_quote") or 0.0) + float(
            snapshot.get("second_stage_funding_quote") or 0.0
        )
        entry_fee = 0.0
        entry_fee_source = "unavailable"
        entry_fee_evidence_complete = False
        entry_fee_evidence_known = snapshot.get("entry_fee_evidence_complete") is True
        if entry_fee_evidence_known and "total_entry_fee_quote" in snapshot:
            try:
                candidate_entry_fee = float(snapshot.get("total_entry_fee_quote"))
                if math.isfinite(candidate_entry_fee):
                    entry_fee = candidate_entry_fee
                    entry_fee_source = "total"
                    entry_fee_evidence_complete = True
            except (TypeError, ValueError):
                pass
        if entry_fee_evidence_known and not entry_fee_evidence_complete and {
            "long_entry_fee_quote", "short_entry_fee_quote"
        }.issubset(snapshot):
            try:
                long_entry_fee = float(snapshot.get("long_entry_fee_quote"))
                short_entry_fee = float(snapshot.get("short_entry_fee_quote"))
                if math.isfinite(long_entry_fee) and math.isfinite(short_entry_fee):
                    entry_fee = long_entry_fee + short_entry_fee
                    entry_fee_source = "legs"
                    entry_fee_evidence_complete = True
            except (TypeError, ValueError):
                pass
        entry_price_evidence_complete = (
            math.isfinite(long_entry)
            and long_entry > 0.0
            and math.isfinite(short_entry)
            and short_entry > 0.0
        )
        price_pnl = ((float(long["average_price"]) - long_entry) * long_qty) + (
            (short_entry - float(short["average_price"])) * short_qty
        )
        exit_fee = float(long["fee_quote"]) + float(short["fee_quote"])
        exit_fee_evidence_complete = True
        for fill in [*long_fills, *short_fills]:
            if self._close_reconciliation_fill_qty(fill) <= 1e-12:
                continue
            try:
                fee = float(getattr(fill, "fee_quote", None))
            except (TypeError, ValueError):
                exit_fee_evidence_complete = False
                break
            if not math.isfinite(fee) or fee < 0.0:
                exit_fee_evidence_complete = False
                break
        close_price_evidence_complete = True
        for fill in [*long_fills, *short_fills]:
            if self._close_reconciliation_fill_qty(fill) <= 1e-12:
                continue
            try:
                price = float(getattr(fill, "price", None))
            except (TypeError, ValueError):
                close_price_evidence_complete = False
                break
            if not math.isfinite(price) or price <= 0.0:
                close_price_evidence_complete = False
                break
        expected_long_qty = float(snapshot.get("long_quantity") or snapshot.get("matched_quantity") or 0.0)
        expected_short_qty = float(snapshot.get("short_quantity") or snapshot.get("matched_quantity") or 0.0)
        close_quantity_evidence_complete = (
            long_qty > 1e-12
            and short_qty > 1e-12
            and (expected_long_qty <= 1e-12 or long_qty + 1e-12 >= expected_long_qty)
            and (expected_short_qty <= 1e-12 or short_qty + 1e-12 >= expected_short_qty)
        )
        complete = (
            close_quantity_evidence_complete
            and entry_price_evidence_complete
            and close_price_evidence_complete
            and entry_fee_evidence_complete
            and exit_fee_evidence_complete
        )
        return {
            "position_id": reconciliation.get("position_id", ""),
            "symbol": reconciliation.get("symbol", snapshot.get("symbol", "")),
            "kind": reconciliation.get("kind", "final"),
            "reason": reconciliation.get("reason", ""),
            "closed_at_ms": self._safe_reconciliation_int(
                reconciliation.get("closed_at_ms"), now_ms
            ),
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
            "entry_fee_source": entry_fee_source,
            "entry_fee_evidence_complete": entry_fee_evidence_complete,
            "entry_price_evidence_complete": entry_price_evidence_complete,
            "exit_fee_quote": exit_fee,
            "exit_fee_evidence_complete": exit_fee_evidence_complete,
            "close_price_evidence_complete": close_price_evidence_complete,
            "net_quote": price_pnl + funding_quote - entry_fee - exit_fee,
            "net_quote_status": "final" if complete else "provisional",
            "expected_long_closed_qty": expected_long_qty,
            "expected_short_closed_qty": expected_short_qty,
            "close_quantity_evidence_complete": close_quantity_evidence_complete,
            "venue_statement_reconciled": complete,
            "evidence_gap": not complete,
            "duplicate_close_leg_suppressed_count": len(long_duplicates)
            + len(short_duplicates),
            "duplicate_close_leg_suppressed_samples": duplicate_samples[:12],
            "source": reconciliation.get("source", "pending_close_reconciliation"),
        }
    async def _resolve_accepted_order_truth_gap(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
    ) -> bool:
        """Resolve an accepted_order_truth_gap task through order truth or terminal-flat evidence.

        NEVER enters the billing flow and NEVER emits ``exit.billing_unreconciled``.
        Resolution paths, in order:

        1. **Terminal-flat supersede**: both venues prove zero positions AND no open
           orders → the ACK gap is moot.  Journal
           ``exit.accepted_order_truth_gap_superseded`` and remove.
        2. **Order-truth resolve**: probe each persisted leg identity through the
           shared ``ORDER_TRUTH_LEDGER``.  Only confirmed execution fill
           (``terminal_fill`` with positive reconciled quantity from a confirmed
           fill/execution source) resolves the identity.  Weak evidence (ACK-only,
           order detail, open/new status), zero-qty responses, and unavailable
           adapter results all retain.  **No-fill is never resolved by identity
           alone;** it requires the remote terminal-flat + no-open-order probe from
           path (1).
        3. **Retain with backoff**: any identity unresolved AND position not
           terminal-flat → retain (backed off, NO billing cycle).

        Returns True when the task was resolved / superseded (caller removes it);
        returns False when the task must be retained.
        """
        kind = str(reconciliation.get("kind") or "")
        if kind != "accepted_order_truth_gap":
            return False

        position_id = str(reconciliation.get("position_id") or "")
        snapshot = reconciliation.get("position_snapshot") or {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        long_venue = self._venue_from_close_reconciliation(
            reconciliation.get("long_venue") or snapshot.get("long_venue")
        )
        short_venue = self._venue_from_close_reconciliation(
            reconciliation.get("short_venue") or snapshot.get("short_venue")
        )
        symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "")

        # Always attempt strict remote terminal-flat + no-open-order evidence
        # first, regardless of local open_positions presence.  Exchange truth
        # outranks recovered/local state — local presence may never veto
        # remote terminal truth.
        if long_venue is not None and short_venue is not None and symbol:
            terminal_sizes, open_order_evidence = (
                await self._fetch_pending_close_terminal_live_flat_truth(
                    symbol=symbol,
                    long_venue=long_venue,
                    short_venue=short_venue,
                )
            )
            # Terminal-flat supersede: both positions are provably zero AND no
            # open orders are present.  The ACK truth gap is moot — the
            # position is gone from exchange.  Only applies when terminal truth
            # is available; an unavailable probe falls through to order-truth
            # resolution below.
            if terminal_sizes is not None:
                long_live_size, short_live_size = terminal_sizes
                if (
                    abs(long_live_size) <= 1e-9
                    and abs(short_live_size) <= 1e-9
                    and open_order_evidence is None
                ):
                    next_attempt_count = self._safe_reconciliation_int(
                        reconciliation.get("attempt_count")
                    ) + 1
                    self.ctx.journal.append(
                        "exit.accepted_order_truth_gap_superseded",
                        {
                            "position_id": position_id,
                            "symbol": symbol,
                            "kind": kind,
                            "long_venue": long_venue.value,
                            "short_venue": short_venue.value,
                            "long_live_size": long_live_size,
                            "short_live_size": short_live_size,
                            "attempt_count": next_attempt_count,
                            "closed_at_ms": self._safe_reconciliation_int(
                                reconciliation.get("closed_at_ms")
                            ),
                            "superseded_at_ms": now_ms,
                            "superseded_by": "terminal_live_flat_truth",
                            "lifetime_ms": max(
                                0,
                                now_ms
                                - max(
                                    0,
                                    self._safe_reconciliation_int(
                                        reconciliation.get("closed_at_ms")
                                    ),
                                ),
                            ),
                        },
                    )
                    return True

        # Not conclusively terminal-flat — probe every persisted order
        # identity through the shared ledger (fail-closed).
        resolved = await self._probe_accepted_order_truth(reconciliation, now_ms)
        if resolved:
            return True
        return False

    async def _probe_accepted_order_truth(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
    ) -> bool:
        """Probe every persisted leg identity through the shared
        ``OrderTruthLedger``.

        Each unique (order_id, client_order_id, venue) identity is probed
        **exactly once** via the venue adapter and then routed through
        ``OrderTruthLedger.resolve_order_success()`` for weak-evidence
        discrimination.

        - **CONFIRMED_FILL** (positive qty from a confirmed fill/execution
          source) → that identity is resolved.
        - **Everything else** — including ACK-only responses, zero-qty order
          detail, open/new status, unsupported venue, unavailable adapter,
          or any exception — remains unresolved.

        No-fill resolution (``terminal_no_fill``) is **not** decided here;
        it belongs exclusively to the terminal-live-flat + no-open-order
        probe in ``_resolve_accepted_order_truth_gap``.

        Returns True only when **every** probed identity has a confirmed-fill
        decision from the ledger.
        """
        snapshot = reconciliation.get("position_snapshot") or {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "")
        position_id = str(reconciliation.get("position_id") or "")
        target_qty = self._safe_reconciliation_float(
            reconciliation.get("requested_quantity")
        )

        long_venue = self._venue_from_close_reconciliation(
            reconciliation.get("long_venue") or snapshot.get("long_venue")
        )
        short_venue = self._venue_from_close_reconciliation(
            reconciliation.get("short_venue") or snapshot.get("short_venue")
        )

        probed: set[tuple[str, str, str]] = set()
        any_unavailable = False

        for side_key, leg_list, venue in [
            ("long", reconciliation.get("long_legs"), long_venue),
            ("short", reconciliation.get("short_legs"), short_venue),
        ]:
            if not isinstance(leg_list, list):
                # Malformed leg collection → fail-closed.
                any_unavailable = True
                continue
            # An empty list is the normal case for the opposite (non-ACK)
            # side — passive_close._register_accepted_order_truth_gap
            # deliberately records only the ACK-triggering leg.  Skip it
            # without marking unavailable.
            if not leg_list:
                continue
            if venue is None:
                # Legs present but no venue → cannot probe.
                any_unavailable = True
                continue
            for leg in leg_list:
                if not isinstance(leg, dict):
                    any_unavailable = True
                    continue
                order_id = str(leg.get("order_id") or "")
                client_order_id = str(leg.get("client_order_id") or "")
                if not order_id and not client_order_id:
                    any_unavailable = True
                    continue
                venue_key = venue.value if isinstance(venue, Venue) else str(venue)
                client_order_id = "" if order_id else client_order_id
                identity = (
                    order_id,
                    client_order_id,
                    venue_key,
                )
                if identity in probed:
                    continue
                probed.add(identity)

                adapter = self.ctx.venue_adapters.get(venue)
                if adapter is None:
                    any_unavailable = True
                    continue
                fetch = getattr(adapter, "fetch_order_fill_reconciliation", None)
                if not callable(fetch):
                    any_unavailable = True
                    continue

                fill = None
                try:
                    fill = await fetch(symbol, order_id, client_order_id)
                    self._flush_adapter_order_diagnostics(adapter)
                except Exception:
                    any_unavailable = True
                    continue

                if fill is None:
                    any_unavailable = True
                    continue

                # Route through the shared ledger to discriminate confirmed
                # fill/execution truth from weak evidence (ACK, order detail,
                # open/new status, zero-qty order detail, etc.).
                fill_metadata = getattr(fill, "metadata", None) or {}
                decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                    venue=venue,
                    symbol=symbol,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    target_qty=target_qty,
                    reconciliation=fill,
                    metadata=fill_metadata,
                    # No live_position — exchange truth outranks local state.
                    # No-fill resolution stays in the terminal-flat probe.
                    live_position=None,
                    open_order_present=None,
                )
                if not decision.confirmed_fill:
                    # Weak / zero / unavailable / open / unsupported → retain.
                    any_unavailable = True

        # Must have probed at least one identity — a task with no
        # identity-bearing legs at all cannot resolve.
        if not probed:
            return False

        if any_unavailable:
            return False

        # Every probed identity is a ledger-confirmed fill.
        next_attempt_count = self._safe_reconciliation_int(
            reconciliation.get("attempt_count")
        ) + 1
        self.ctx.journal.append(
            "exit.accepted_order_truth_gap_resolved",
            {
                "position_id": position_id,
                "symbol": symbol,
                "kind": "accepted_order_truth_gap",
                "attempt_count": next_attempt_count,
                "closed_at_ms": self._safe_reconciliation_int(
                    reconciliation.get("closed_at_ms")
                ),
                "resolved_at_ms": now_ms,
                "resolved_by": "order_fill_confirmed_by_ledger",
                "lifetime_ms": max(
                    0,
                    now_ms
                    - max(
                        0,
                        self._safe_reconciliation_int(
                            reconciliation.get("closed_at_ms")
                        ),
                    ),
                ),
            },
        )
        return True

    async def _process_pending_close_reconciliations(self, now_ms: int) -> None:
        self.ctx.state.set_pending_close_reconciliations(
            getattr(self.ctx.state, "pending_close_reconciliations", [])
        )
        pending_reconciliations = self.ctx.state.pending_close_reconciliations
        if not pending_reconciliations:
            return
        if str(getattr(self.ctx.config.runtime, "mode", "") or "").lower() != "live":
            return

        retained: list[Any] = []
        eligible: list[dict[str, Any]] = []
        changed = False
        current_cycle = self._safe_reconciliation_int(
            getattr(self.ctx.state, "tick_count", 0)
        )
        for reconciliation in list(pending_reconciliations):
            if not isinstance(reconciliation, dict):
                retained.append(reconciliation)
                continue
            if is_unattributed_recovered_live_flat_reconciliation(reconciliation):
                snapshot = reconciliation.get("position_snapshot") or {}
                self.ctx.journal.append_critical(
                    now_ms,
                    "recovery.external_pair_flat_reclassified",
                    {
                        "position_id": str(reconciliation.get("position_id") or ""),
                        "symbol": str(
                            reconciliation.get("symbol")
                            or snapshot.get("symbol")
                            or ""
                        ),
                        "source": str(reconciliation.get("source") or ""),
                        "kind": str(reconciliation.get("kind") or "final"),
                        "closed_at_ms": self._safe_reconciliation_int(
                            reconciliation.get("closed_at_ms")
                        ),
                        "accounting_owner": "external_unattributed",
                        "local_order_identity_present": False,
                        "reclassified_from": str(
                            reconciliation.get("reconciliation_status") or "pending"
                        ),
                    },
                )
                changed = True
                continue
            if reconciliation.get("reconciliation_status") == "evidence_debt":
                retained.append(reconciliation)
                continue
            if str(reconciliation.get("kind") or "final") != "accepted_order_truth_gap":
                evidence_debt_reason = (
                    pending_close_reconciliation_evidence_debt_reason(reconciliation)
                )
                if evidence_debt_reason is not None:
                    changed = (
                        self._mark_pending_close_reconciliation_evidence_debt(
                            reconciliation,
                            reason=evidence_debt_reason,
                            now_ms=now_ms,
                        )
                        or changed
                    )
                    retained.append(reconciliation)
                    continue
            created_cycle = self._safe_reconciliation_int(
                reconciliation.get("created_cycle")
            )
            if current_cycle != 0 and created_cycle >= current_cycle:
                retained.append(reconciliation)
                continue
            if self._safe_reconciliation_int(
                reconciliation.get("next_attempt_ms")
            ) > now_ms:
                retained.append(reconciliation)
                continue
            eligible.append(reconciliation)

        # ------------------------------------------------------------------
        # Separate accepted_order_truth_gap tasks from billing tasks.
        # ACK truth-gap tasks MUST NOT enter the billing flow; they are
        # resolved / superseded exclusively through order truth or
        # terminal-flat evidence.  Only kind "final" / "partial" tasks
        # proceed to financial (fill-fetch + bill) reconciliation.
        # ------------------------------------------------------------------
        order_truth_eligible: list[dict[str, Any]] = []
        billing_eligible: list[dict[str, Any]] = []
        for reconciliation in eligible:
            if str(reconciliation.get("kind") or "") == "accepted_order_truth_gap":
                order_truth_eligible.append(reconciliation)
            else:
                billing_eligible.append(reconciliation)

        # -- Order-truth resolution (no billing) ---------------------------
        for reconciliation in sorted(
            order_truth_eligible,
            key=lambda item: (
                self._safe_reconciliation_int(item.get("closed_at_ms")),
                str(item.get("position_id") or ""),
            ),
        ):
            resolved = await self._resolve_accepted_order_truth_gap(
                reconciliation, now_ms
            )
            if resolved:
                changed = True
                continue
            # Retain with backoff — NEVER emit billing events.
            self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
            retained.append(reconciliation)
            changed = True

        # -- Billing reconciliation (final / partial tasks only) -----------
        for reconciliation in sorted(
            billing_eligible,
            key=lambda item: (
                self._safe_reconciliation_int(item.get("closed_at_ms")),
                0 if str(item.get("kind") or "final") == "partial" else 1,
                str(item.get("position_id") or ""),
            ),
        ):
            if reconciliation.get("reconciliation_status") == "evidence_debt":
                retained.append(reconciliation)
                continue
            snapshot = reconciliation.get("position_snapshot") or {}
            if not isinstance(snapshot, dict):
                snapshot = {}
            long_venue = self._venue_from_close_reconciliation(
                reconciliation.get("long_venue") or snapshot.get("long_venue")
            )
            short_venue = self._venue_from_close_reconciliation(
                reconciliation.get("short_venue") or snapshot.get("short_venue")
            )
            if long_venue is None or short_venue is None:
                changed = (
                    self._mark_pending_close_reconciliation_evidence_debt(
                        reconciliation,
                        reason="invalid_position_snapshot_venues",
                        now_ms=now_ms,
                    )
                    or changed
                )
                retained.append(reconciliation)
                continue

            symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "")
            has_leg_identity = (
                self._has_close_reconciliation_leg_identity(
                    reconciliation.get("long_legs")
                )
                or self._has_close_reconciliation_leg_identity(
                    reconciliation.get("short_legs")
                )
            )
            if not has_leg_identity:
                changed = (
                    self._mark_pending_close_reconciliation_evidence_debt(
                        reconciliation,
                        reason="missing_close_order_identity",
                        now_ms=now_ms,
                        symbol=symbol,
                        long_venue=long_venue,
                        short_venue=short_venue,
                    )
                    or changed
                )
                retained.append(reconciliation)
                continue

            long_fills = await self._call_fetch_close_leg_reconciliations(
                symbol=symbol,
                venue=long_venue,
                legs=reconciliation.get("long_legs"),
            )
            short_fills = await self._call_fetch_close_leg_reconciliations(
                symbol=symbol,
                venue=short_venue,
                legs=reconciliation.get("short_legs"),
            )
            if long_fills is not None and short_fills is not None and (long_fills or short_fills):
                payload = self._exit_reconciled_payload_from_leg_fills(
                    reconciliation,
                    long_fills,
                    short_fills,
                    now_ms,
                )
                if not payload["venue_statement_reconciled"]:
                    self.ctx.journal.append(
                        "exit.billing_unreconciled",
                        payload,
                    )
                    terminalized = await self._try_terminalize_billing_evidence_gap(
                        reconciliation,
                        payload,
                        now_ms,
                        symbol=symbol,
                        long_venue=long_venue,
                        short_venue=short_venue,
                    )
                    if terminalized:
                        changed = True
                        continue
                    self._call_apply_pending_close_reconciliation_backoff(reconciliation, now_ms)
                    retained.append(reconciliation)
                    changed = True
                    continue
                self.ctx.journal.append_critical(
                    now_ms,
                    "exit.reconciled",
                    payload,
                )
                changed = True
                continue
            if long_fills == [] and short_fills == []:
                self.ctx.journal.append(
                    "reconciliation.pending_close_reconciliation_invalid",
                    {
                        "position_id": reconciliation.get("position_id", ""),
                        "symbol": symbol,
                        "reason": "close_order_lookup_returned_no_fill",
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

            if normal_close_reason_uses_passive_maker_taker(reason_str):
                # Route to passive close
                if self.ctx.passive_close_executor is not None:
                    self.ctx.journal.append(
                        "runtime.normal_close_routing_passive",
                        {
                            "position_id": position.position_id,
                            "reason": reason_str,
                            "matched_quantity": position.matched_quantity,
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
                        },
                    )
                    await self.ctx.close_executor.execute_close(
                        position, reason_str, now_ms,
                        long_price_hint=self._call_resolve_local_l2_mid(position.long_venue, position.symbol, now_ms=now_ms),
                        short_price_hint=self._call_resolve_local_l2_mid(position.short_venue, position.symbol, now_ms=now_ms),
                        state=self.ctx.state,
                    )
    def _fresh_local_l2_book(
        self,
        venue,
        symbol: str,
        *,
        now_ms: int,
    ):
        """Return one fresh HOT local-L2 book, or reject its price evidence.

        Close price hints and passive close repricing share this boundary.  A
        HOT status alone is lifecycle state, not proof that a price is still
        usable for an order.
        """
        if not self.ctx._local_l2_effective_enabled():
            return None
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        budget_ms = int(self.ctx.config.strategy.max_liquidity_snapshot_age_ms or 0)
        try:
            book = self.ctx.local_l2_runtime.get_book(venue_value, symbol)
            if book is None or book.status.value != "hot":
                return None
            age_ms = book.age_ms(now_ms)
            # V1 treats a non-positive execution-liquidity TTL as unavailable;
            # it must never turn a HOT status into indefinitely valid price
            # evidence.
            if budget_ms <= 0 or book.is_stale(budget_ms, now_ms):
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
                return None
            return book
        except Exception:
            return None

    def _resolve_local_l2_mid(self, venue, symbol: str, now_ms: int | None = None) -> float:
        """Get a fresh mid price from the local L2 owner for venue+symbol."""
        if now_ms is None:
            now_ms = wall_clock_now_ms()
        book = self._fresh_local_l2_book(venue, symbol, now_ms=now_ms)
        if book is None:
            return 0.0
        try:
            mid = book.mid_price()
            if mid and mid > 0:
                return mid
        except Exception:
            pass
        return 0.0
    def _resolve_local_l2_quote(
        self,
        venue,
        symbol: str,
        now_ms: int | None = None,
    ) -> tuple[float, float] | None:
        """Get a fresh best bid/ask from the local L2 owner."""
        if now_ms is None:
            now_ms = wall_clock_now_ms()
        book = self._fresh_local_l2_book(venue, symbol, now_ms=now_ms)
        if book is None:
            return None
        try:
            best_bid = book.best_bid()
            best_ask = book.best_ask()
            if best_bid > 0 and best_ask > best_bid:
                return best_bid, best_ask
        except Exception:
            pass
        return None
    def _resolve_close_price_hint_mid_with_source(self, venue, symbol: str):
        return self._call_resolve_local_l2_mid(venue, symbol), "local_l2"
    def _resolve_close_price_hint_quote_with_source(self, venue, symbol: str):
        quote = self._call_resolve_local_l2_quote(venue, symbol)
        if quote is None:
            return None
        return quote[0], quote[1], "local_l2"
