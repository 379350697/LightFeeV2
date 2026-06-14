"""Startup recovery helpers delegated from LiveRuntime."""

from __future__ import annotations
from typing import Any

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import PositionSnapshot, Venue
from lightfee.engine.exchange_truth import request_venue_operation
from lightfee.engine.lifecycle import (
    clear_risk_mode_for_recovery,
    enter_fail_closed,
    set_lifecycle,
)
from lightfee.engine.recovery_decision_core import (
    CORE_CLEARABLE_BLOCK_REASONS,
    RecoveryDecisionKind,
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex
from lightfee.engine.runtime_context import RuntimeContext
from lightfee.engine.v1_lifecycle_closure import closure_event_fields
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.venues.specs import VenueOperation


class RecoveryStartupRuntime:
    def __init__(self, ctx: RuntimeContext) -> None:
        self.ctx = ctx

    def _refresh_recovery_ledger_from_exchange_truth(
        self,
        exchange_truth: dict[str, Any],
        *,
        now_ms: int,
        lifecycle_clear_reason: str = "current_exchange_truth_core_clean",
    ) -> RecoveryLedger:
        owner_index = RecoveryOwnerIndex.from_state_and_journal(
            self.ctx.state,
            self.ctx._recovery_owner_journal_events(),
        )
        ledger = RecoveryLedger.from_local_and_exchange_truth(
            local=self.ctx.state,
            exchange_truth=exchange_truth,
            owner_index=owner_index,
        )
        self.ctx.recovery_ledger = ledger
        self.ctx._last_recovery_exchange_truth = dict(exchange_truth or {})
        core_decision = V1RecoveryDecisionCore().decide(
            RecoveryEvidenceSnapshot(
                local_open_positions=tuple(
                    self.ctx._recovery_state_collection("open_positions")
                ),
                pending_entries=tuple(
                    self.ctx._recovery_state_collection("pending_entries")
                ),
                residual_repairs=tuple(
                    self.ctx._recovery_state_collection("pending_residual_repairs")
                ),
                passive_closes=tuple(
                    self.ctx._recovery_state_collection("pending_passive_closes")
                ),
                exchange_truth=exchange_truth,
                prior_recovery_block_reason=self.ctx.state.recovery_blocked_reason,
                operator_fail_closed=(
                    self.ctx.state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED
                ),
                recovery_work_items=tuple(ledger.work_items),
            )
        )
        self.ctx.recovery_decision = core_decision
        closure = self.ctx._current_v1_lifecycle_closure(
            now_ms,
            recovery_ledger=ledger,
            recovery_decision=core_decision,
        )
        closure_summary = dict(closure.get("summary") or {})
        recovery_closure_fields = closure_event_fields(
            closure,
            phase="RECOVERY_TRUTH",
            owner_id="core",
        )
        recovery_block_policy = str(
            closure_summary.get("recovery_block_policy") or ""
        )
        recovery_block_reason = str(
            closure_summary.get("recovery_block_reason") or ""
        )

        # Block and clear are both driven by V1RecoveryDecisionCore so
        # evidence-gap states cannot oscillate between ledger block and stale
        # block cleanup.
        if recovery_block_policy == "block" and recovery_block_reason:
            self.ctx.state.recovery_blocked_reason = recovery_block_reason
            self.ctx.state.recovery_blocked_at_ms = now_ms
            set_lifecycle(self.ctx.state, EngineLifecycle.RISK_ONLY)
            self.ctx.journal.append(
                "recovery.ledger_blocked",
                {
                    "reason": self.ctx.state.recovery_blocked_reason,
                    "decision": core_decision.kind.value,
                    "management_action": core_decision.management_action.value,
                    "work_items": [
                        self.ctx._recovery_ledger_work_item_payload(item)
                        for item in ledger.work_items
                        if item.blocking
                    ],
                    "ts_ms": now_ms,
                    **recovery_closure_fields,
                },
            )
        elif recovery_block_policy in {"clear", "warn_evidence_gap"} and (
            core_decision.clear_previous_block
            and self.ctx.state.recovery_blocked_reason in CORE_CLEARABLE_BLOCK_REASONS
        ):
            clear_risk_mode_for_recovery(self.ctx.state, core_decision)
            self.ctx.journal.append(
                "recovery.ledger_clear",
                {
                    "reason": core_decision.clear_reason,
                    "decision": core_decision.kind.value,
                    "ts_ms": now_ms,
                    **recovery_closure_fields,
                },
            )
        else:
            self.ctx._clear_stale_recovery_lifecycle_if_core_clean(
                reason=lifecycle_clear_reason,
                now_ms=now_ms,
                exchange_truth=exchange_truth,
            )
        return ledger

    def _clear_stale_recovery_lifecycle_if_core_clean(
        self,
        *,
        reason: str,
        now_ms: int,
        exchange_truth: dict[str, Any] | None,
    ) -> bool:
        """Release a stale risk_only lifecycle only from current core + truth.

        This is intentionally narrower than clear_risk_mode_for_recovery: it
        handles the post-deploy/runtime latch where the recovery core is already
        clean, the exchange is provably flat, and no local recovery owner remains.
        """
        if self.ctx.state.lifecycle != EngineLifecycle.RISK_ONLY:
            return False
        if self.ctx.state.risk_mode not in {
            GlobalRiskMode.RUNNING,
            GlobalRiskMode.FAIL_CLOSED,
        }:
            return False
        if self.ctx.state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED:
            return False
        if self.ctx.state.recovery_blocked_reason:
            return False
        if self.ctx._has_local_recovery_work():
            return False
        if not isinstance(exchange_truth, dict):
            return False
        if exchange_truth.get("truth_supported", True) is False:
            return False
        if not bool(exchange_truth.get("truth_available", False)):
            return False
        if not self.ctx._recovery_exchange_truth_flat(exchange_truth):
            return False
        if not self.ctx._recovery_exchange_truth_open_orders_empty(exchange_truth):
            return False

        core_decision = getattr(self.ctx, "recovery_decision", None)
        if core_decision is None:
            return False
        if getattr(core_decision, "block_reason", None):
            return False
        if not bool(getattr(core_decision, "entry_allowed", False)):
            return False
        if getattr(core_decision, "kind", None) != RecoveryDecisionKind.RUNNING_CLEAN:
            return False

        previous_lifecycle = self.ctx.state.lifecycle.value
        previous_risk_mode = self.ctx.state.risk_mode.value
        if not clear_risk_mode_for_recovery(self.ctx.state, core_decision):
            return False
        self.ctx.journal.append(
            "recovery.lifecycle_clear",
            {
                "reason": reason,
                "decision": core_decision.kind.value,
                "clear_reason": getattr(core_decision, "clear_reason", ""),
                "previous_lifecycle": previous_lifecycle,
                "previous_risk_mode": previous_risk_mode,
                "position_row_count": len(exchange_truth.get("positions") or []),
                "open_order_count": len(exchange_truth.get("open_orders") or []),
                "ts_ms": now_ms,
            },
        )
        return True

    @staticmethod
    def _recovery_exchange_truth_flat(exchange_truth: dict[str, Any]) -> bool:
        for position in exchange_truth.get("positions") or []:
            if not isinstance(position, dict):
                continue
            qty = position.get("quantity", position.get("position_qty", 0.0))
            try:
                if abs(float(qty or 0.0)) > 1e-9:
                    return False
            except (TypeError, ValueError):
                return False
        return True

    @staticmethod
    def _recovery_exchange_truth_open_orders_empty(exchange_truth: dict[str, Any]) -> bool:
        if "open_orders" not in exchange_truth:
            return False
        for order in exchange_truth.get("open_orders") or []:
            if isinstance(order, dict) and order.get("error"):
                return False
            return False
        return True

    def _recovery_state_collection(self, name: str) -> list[Any]:
        value = getattr(self.ctx.state, name, [])
        if value is None:
            return []
        if isinstance(value, dict):
            return list(value.values())
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return [value]

    async def _refresh_recovery_ledger_for_symbols(
        self,
        symbols: list[str],
        now_ms: int,
        *,
        lifecycle_clear_reason: str = "current_exchange_truth_core_clean",
    ) -> RecoveryLedger | None:
        symbols = sorted({str(symbol or "").upper() for symbol in symbols if symbol})
        if not symbols or not self.ctx._venue_adapters:
            return None
        exchange_truth = await self.ctx._collect_recovery_ledger_exchange_truth(
            symbols,
            now_ms,
        )
        if not exchange_truth.get("truth_supported", True):
            return None
        return self.ctx._refresh_recovery_ledger_from_exchange_truth(
            exchange_truth,
            now_ms=now_ms,
            lifecycle_clear_reason=lifecycle_clear_reason,
        )

    async def _refresh_recovery_ledger_from_account_truth(
        self,
        now_ms: int,
        *,
        lifecycle_clear_reason: str = "current_account_truth_core_clean",
    ) -> RecoveryLedger | None:
        if not self.ctx._venue_adapters:
            return None
        exchange_truth = await self.ctx._collect_recovery_ledger_account_truth(now_ms)
        if not exchange_truth.get("truth_supported", True):
            return None
        return self.ctx._refresh_recovery_ledger_from_exchange_truth(
            exchange_truth,
            now_ms=now_ms,
            lifecycle_clear_reason=lifecycle_clear_reason,
        )

    async def _collect_recovery_ledger_account_truth(
        self,
        now_ms: int,
    ) -> dict[str, Any]:
        positions: list[dict[str, Any]] = []
        open_orders: list[dict[str, Any]] = []
        probe_evidence: list[dict[str, Any]] = []
        errors: list[str] = []
        truth_probe_count = 0

        for venue, adapter in self.ctx._venue_adapters.items():
            venue_name = venue.value if hasattr(venue, "value") else str(venue)
            fetch_all_positions = getattr(adapter, "fetch_all_positions", None)
            if not callable(fetch_all_positions):
                transport = getattr(adapter, "_transport", None)
                fetch_all_positions = getattr(transport, "fetch_all_positions", None)
            if not callable(fetch_all_positions):
                errors.append(f"{venue_name}:*:positions:fetch_all_positions_unavailable")
                probe_evidence.append(
                    {
                        "venue": venue_name,
                        "symbol": "*",
                        "endpoint": "fetch_all_positions",
                        "method": "fetch_all_positions",
                        "finished_at_ms": now_ms,
                        "classification": "position_probe_unfiltered_failed",
                        "error": "fetch_all_positions_unavailable",
                    }
                )
            else:
                try:
                    rows = await fetch_all_positions()
                    truth_probe_count += 1
                    if isinstance(rows, (list, tuple, set)):
                        for position in rows:
                            positions.append(
                                self.ctx._recovery_ledger_position_payload(
                                    position,
                                    venue_name=venue_name,
                                    symbol="*",
                                )
                            )
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": "*",
                            "endpoint": "fetch_all_positions",
                            "method": "fetch_all_positions",
                            "finished_at_ms": now_ms,
                            "classification": "position_probe_unfiltered_succeeded",
                            "position_count": len(rows)
                            if isinstance(rows, (list, tuple, set))
                            else 0,
                        }
                    )
                except Exception as exc:
                    truth_probe_count += 1
                    errors.append(f"{venue_name}:*:positions:{exc}")
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": "*",
                            "endpoint": "fetch_all_positions",
                            "method": "fetch_all_positions",
                            "finished_at_ms": now_ms,
                            "classification": "position_probe_unfiltered_failed",
                            "error": str(exc),
                        }
                    )

            try:
                rows, endpoint = await self.ctx._fetch_recovery_ledger_account_open_orders(
                    venue,
                    adapter,
                )
                truth_probe_count += 1
                for row in self.ctx._recovery_ledger_open_order_payloads(
                    rows,
                    venue_name=venue_name,
                    symbol="*",
                ):
                    open_orders.append(row)
                probe_evidence.append(
                    {
                        "venue": venue_name,
                        "symbol": "*",
                        "endpoint": endpoint,
                        "method": "fetch_open_orders",
                        "finished_at_ms": now_ms,
                        "classification": "open_order_probe_unfiltered_succeeded",
                        "open_order_count": len(rows)
                        if isinstance(rows, (list, tuple, set))
                        else 0,
                    }
                )
            except Exception as exc:
                truth_probe_count += 1
                errors.append(f"{venue_name}:*:open_orders:{exc}")
                probe_evidence.append(
                    {
                        "venue": venue_name,
                        "symbol": "*",
                        "endpoint": "fetch_open_orders",
                        "method": "fetch_open_orders",
                        "finished_at_ms": now_ms,
                        "classification": "open_order_probe_unfiltered_failed",
                        "error": str(exc),
                    }
                )

        return {
            "truth_supported": truth_probe_count > 0,
            "truth_available": not errors,
            "positions": positions,
            "open_orders": open_orders,
            "probe_evidence": probe_evidence,
            "errors": errors,
        }

    async def _fetch_recovery_ledger_account_open_orders(
        self,
        venue: Venue,
        adapter: VenueAdapter,
    ) -> tuple[list[Any], str]:
        transport = getattr(adapter, "_transport", None)
        request = getattr(transport, "_request", None)

        if venue == Venue.ASTER:
            fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
            if callable(fetch_open_orders):
                rows = await fetch_open_orders(None)
                if isinstance(rows, dict) and rows.get("error"):
                    raise RuntimeError(str(rows.get("error")))
                return self.ctx._recovery_ledger_order_rows(rows), "fetch_open_orders(None)"

        if callable(request):
            credential = getattr(transport, "_credential", None)
            account = str(getattr(credential, "account_address", "") or "")
            agent_wallet = str(getattr(credential, "agent_wallet_address", "") or "")
            exchange_truth_service = getattr(self.ctx, "exchange_truth", None)
            operation_requester = getattr(
                exchange_truth_service,
                "request_venue_operation",
                request_venue_operation,
            )
            raw, contract_request = await operation_requester(
                transport,
                venue,
                VenueOperation.OPEN_ORDERS,
                account_address=account,
                agent_wallet_address=agent_wallet,
            )
            return self.ctx._recovery_ledger_order_rows(raw), contract_request.label

        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        if not callable(fetch_open_orders):
            raise RuntimeError("fetch_open_orders_unavailable")
        rows = await fetch_open_orders(None)
        if isinstance(rows, dict) and rows.get("error"):
            raise RuntimeError(str(rows.get("error")))
        return self.ctx._recovery_ledger_order_rows(rows), "fetch_open_orders(None)"

    async def _collect_recovery_ledger_exchange_truth(
        self,
        symbols: list[str],
        now_ms: int,
    ) -> dict[str, Any]:
        positions: list[dict[str, Any]] = []
        open_orders: list[dict[str, Any]] = []
        probe_evidence: list[dict[str, Any]] = []
        errors: list[str] = []
        truth_probe_count = 0

        for venue, adapter in self.ctx._venue_adapters.items():
            venue_name = venue.value if hasattr(venue, "value") else str(venue)
            for symbol in symbols:
                fetch_position = getattr(adapter, "fetch_position", None)
                if not callable(fetch_position):
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": symbol,
                            "endpoint": "fetch_position",
                            "method": "fetch_position",
                            "finished_at_ms": now_ms,
                            "classification": "position_truth_unsupported",
                        }
                    )
                else:
                    try:
                        position = await fetch_position(symbol)
                        truth_probe_count += 1
                        positions.append(
                            self.ctx._recovery_ledger_position_payload(
                                position,
                                venue_name=venue_name,
                                symbol=symbol,
                            )
                        )
                        probe_evidence.append(
                            {
                                "venue": venue_name,
                                "symbol": symbol,
                                "endpoint": "fetch_position",
                                "method": "fetch_position",
                                "finished_at_ms": now_ms,
                                "classification": "position_truth",
                            }
                        )
                    except NotImplementedError:
                        probe_evidence.append(
                            {
                                "venue": venue_name,
                                "symbol": symbol,
                                "endpoint": "fetch_position",
                                "method": "fetch_position",
                                "finished_at_ms": now_ms,
                                "classification": "position_truth_unsupported",
                            }
                        )
                    except Exception as exc:
                        truth_probe_count += 1
                        errors.append(f"{venue_name}:{symbol}:position:{exc}")
                        probe_evidence.append(
                            {
                                "venue": venue_name,
                                "symbol": symbol,
                                "endpoint": "fetch_position",
                                "method": "fetch_position",
                                "finished_at_ms": now_ms,
                                "classification": "position_truth_error",
                                "error": str(exc),
                            }
                        )

                fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
                if not callable(fetch_open_orders):
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": symbol,
                            "endpoint": "fetch_open_orders",
                            "method": "fetch_open_orders",
                            "finished_at_ms": now_ms,
                            "classification": "open_order_truth_unsupported",
                        }
                    )
                    continue
                try:
                    rows = await fetch_open_orders(symbol)
                    truth_probe_count += 1
                    for row in self.ctx._recovery_ledger_open_order_payloads(
                        rows,
                        venue_name=venue_name,
                        symbol=symbol,
                    ):
                        open_orders.append(row)
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": symbol,
                            "endpoint": "fetch_open_orders",
                            "method": "fetch_open_orders",
                            "finished_at_ms": now_ms,
                            "classification": "open_order_truth",
                        }
                    )
                except NotImplementedError:
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": symbol,
                            "endpoint": "fetch_open_orders",
                            "method": "fetch_open_orders",
                            "finished_at_ms": now_ms,
                            "classification": "open_order_truth_unsupported",
                        }
                    )
                except Exception as exc:
                    truth_probe_count += 1
                    errors.append(f"{venue_name}:{symbol}:open_orders:{exc}")
                    probe_evidence.append(
                        {
                            "venue": venue_name,
                            "symbol": symbol,
                            "endpoint": "fetch_open_orders",
                            "method": "fetch_open_orders",
                            "finished_at_ms": now_ms,
                            "classification": "open_order_truth_error",
                            "error": str(exc),
                        }
                    )

        return {
            "truth_supported": truth_probe_count > 0,
            "truth_available": not errors,
            "positions": positions,
            "open_orders": open_orders,
            "probe_evidence": probe_evidence,
            "errors": errors,
        }

    @staticmethod
    def _recovery_ledger_position_payload(
        position: Any,
        *,
        venue_name: str,
        symbol: str,
    ) -> dict[str, Any]:
        side = getattr(position, "side", "")
        if hasattr(side, "value"):
            side = side.value
        return {
            "venue": str(getattr(position, "venue", venue_name) or venue_name).lower(),
            "symbol": str(getattr(position, "symbol", symbol) or symbol).upper(),
            "side": str(side or "").lower(),
            "quantity": float(getattr(position, "quantity", 0.0) or 0.0),
            "entry_price": float(getattr(position, "entry_price", 0.0) or 0.0),
            "observed_at_ms": int(getattr(position, "observed_at_ms", 0) or 0),
        }

    @staticmethod
    def _recovery_ledger_open_order_payloads(
        rows: Any,
        *,
        venue_name: str,
        symbol: str,
    ) -> list[dict[str, Any]]:
        if rows is None:
            return []
        if isinstance(rows, dict) and rows.get("error"):
            raise RuntimeError(str(rows.get("error")))
        if isinstance(rows, dict):
            iterable = list(rows.values())
        elif isinstance(rows, (list, tuple, set)):
            iterable = list(rows)
        else:
            iterable = [rows]

        result: list[dict[str, Any]] = []
        for row in iterable:
            if isinstance(row, dict):
                result.append(
                    {
                        "venue": str(row.get("venue") or venue_name).lower(),
                        "symbol": str(
                            row.get("symbol")
                            or row.get("instId")
                            or row.get("contract")
                            or row.get("coin")
                            or symbol
                        ).upper(),
                        "side": str(row.get("side") or "").lower(),
                        "quantity": float(
                            row.get(
                                "quantity",
                                row.get(
                                    "origQty",
                                    row.get(
                                        "qty",
                                        row.get(
                                            "size",
                                            row.get("sz", row.get("amount", 0.0)),
                                        ),
                                    ),
                                ),
                            )
                            or 0.0
                        ),
                        "price": float(row.get("price", row.get("px", 0.0)) or 0.0),
                        "reduce_only": RecoveryStartupRuntime._truthy_recovery_order_field(
                            row.get("reduce_only", row.get("reduceOnly", False))
                        ),
                        "order_id": str(
                            row.get("order_id")
                            or row.get("orderId")
                            or row.get("ordId")
                            or row.get("id")
                            or row.get("orderLinkId")
                            or row.get("clientOid")
                            or row.get("clOrdId")
                            or ""
                        ),
                        "client_order_id": str(
                            row.get("client_order_id")
                            or row.get("clientOrderId")
                            or row.get("order_link_id")
                            or row.get("orderLinkId")
                            or row.get("clientOid")
                            or row.get("clOrdId")
                            or ""
                        ),
                    }
                )
        return result

    @staticmethod
    def _truthy_recovery_order_field(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").lower() in {"true", "1", "yes"}

    @staticmethod
    def _recovery_ledger_order_rows(raw: Any) -> list[Any]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, dict):
            return [raw]
        if raw.get("error"):
            raise RuntimeError(str(raw.get("error")))
        result = raw.get("result")
        if isinstance(result, dict) and isinstance(result.get("list"), list):
            return result["list"]
        data = raw.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("entrustedList", "orderList", "list", "orders"):
                rows = data.get(key)
                if isinstance(rows, list):
                    return rows
        for key in ("list", "orders", "openOrders"):
            rows = raw.get(key)
            if isinstance(rows, list):
                return rows
        return []

    def _startup_recovery_ledger_symbols(self, symbol_info: object) -> list[str]:
        symbols = set(self.ctx._startup_position_probe_symbols(symbol_info))
        symbols.update(self.ctx._startup_recovery_owner_journal_symbols())
        return sorted(symbol.upper() for symbol in symbols if symbol)

    def _startup_recovery_owner_journal_symbols(self) -> list[str]:
        symbols: set[str] = set()
        for event in RecoveryOwnerIndex.active_journal_owner_events(
            self.ctx._recovery_owner_journal_events()
        ):
            if isinstance(event, dict):
                payload = event.get("payload", {})
            else:
                payload = getattr(event, "payload", {})
            if not isinstance(payload, dict):
                continue
            symbol = str(payload.get("symbol") or "").upper()
            if symbol and (
                self.ctx._has_journal_order_owner_evidence(payload)
                or self.ctx._has_journal_position_owner_evidence(event, payload)
            ):
                symbols.add(symbol)
        return sorted(symbols)

    @staticmethod
    def _has_journal_order_owner_evidence(payload: dict[str, Any]) -> bool:
        order_id = (
            payload.get("order_id")
            or payload.get("maker_order_id")
            or payload.get("exchange_order_id")
        )
        client_order_id = (
            payload.get("client_order_id")
            or payload.get("maker_client_order_id")
            or payload.get("clientOrderId")
        )
        return bool(str(order_id or "") or str(client_order_id or ""))

    @staticmethod
    def _has_journal_position_owner_evidence(
        event: Any,
        payload: dict[str, Any],
    ) -> bool:
        if isinstance(event, dict):
            kind = str(event.get("kind") or "").lower()
        else:
            kind = str(getattr(event, "kind", "") or "").lower()
        if kind not in {
            "pending_entry.positive_fill_live_truth_conflict",
            "pending_entry.terminalizer_decision",
        }:
            return False
        if (
            kind == "pending_entry.terminalizer_decision"
            and str(payload.get("outcome") or "").lower()
            != "positive_fill_live_truth_conflict"
        ):
            return False
        try:
            live_long = float(payload.get("live_long_quantity") or 0.0)
            live_short = float(payload.get("live_short_quantity") or 0.0)
        except (TypeError, ValueError):
            return False
        return live_long > 1e-9 or live_short > 1e-9

    def _recovery_owner_journal_events(self) -> list[dict[str, Any]]:
        try:
            return self.ctx.journal.read_all()
        except Exception:
            return []

    def _has_local_recovery_work(self) -> bool:
        return any(
            (
                self.ctx.state.open_positions,
                self.ctx.state.pending_entries,
                self.ctx.state.pending_closes,
                self.ctx.state.pending_passive_closes,
                getattr(self.ctx.state, "pending_residual_repairs", []) or [],
            )
        )

    @staticmethod
    def _recovery_ledger_work_item_payload(item) -> dict[str, Any]:
        return {
            "kind": item.kind,
            "symbol": item.symbol,
            "venues": sorted(item.venues),
            "blocking": item.blocking,
            "decision": item.decision.outcome,
            "owner_type": item.owner.owner_type if item.owner is not None else "",
            "owner_confidence": item.owner.confidence if item.owner is not None else "",
        }

    def _startup_position_probe_symbols(self, symbol_info: object) -> list[str]:
        """Symbols to probe for live startup position recovery.

        V1 only probes symbols tied to local recovery work or explicitly recent
        touched symbols, never the full configured/resolved trading universe.
        """
        symbols: list[str] = []

        def add_symbol(value: object) -> None:
            symbol = str(value or "")
            if symbol:
                symbols.append(symbol)

        for pos in self.ctx.state.open_positions.values():
            add_symbol(getattr(pos, "symbol", ""))
        for pending in self.ctx.state.pending_entries.values():
            add_symbol(getattr(pending, "symbol", ""))
        for pending in self.ctx.state.pending_closes.values():
            pos = self.ctx.state.open_positions.get(getattr(pending, "position_id", ""))
            add_symbol(getattr(pos, "symbol", ""))
        for pending in self.ctx.state.pending_passive_closes.values():
            snapshot = getattr(pending, "position_snapshot", None)
            add_symbol(getattr(snapshot, "symbol", ""))
        for repair in getattr(self.ctx.state, "pending_residual_repairs", []) or []:
            if isinstance(repair, dict):
                add_symbol(repair.get("symbol", ""))
        for item in getattr(self.ctx.state, "live_recovery_reduce_only_pairs", []) or []:
            if isinstance(item, dict):
                add_symbol(item.get("symbol", ""))
        if isinstance(symbol_info, dict):
            for key in (
                "recent_touched_symbols",
                "touched_symbols",
                "recovery_symbols",
            ):
                raw = symbol_info.get(key) or []
                symbols.extend(str(s) for s in raw if str(s))
        if isinstance(self.ctx.state.last_scan, dict):
            for key in ("recent_touched_symbols", "touched_symbols"):
                raw = self.ctx.state.last_scan.get(key) or []
                symbols.extend(str(s) for s in raw if str(s))

        seen: set[str] = set()
        result: list[str] = []
        for symbol in symbols:
            if symbol not in seen:
                seen.add(symbol)
                result.append(symbol)
        return result

    def _truth_required_recovery_probe_symbol_sources(
        self,
        requested_symbols: list[str],
    ) -> dict[str, list[str]]:
        sources: dict[str, list[str]] = {}

        def add_symbol(source: str, value: object) -> None:
            symbol = str(value or "").upper()
            if not symbol:
                return
            sources.setdefault(source, [])
            if symbol not in sources[source]:
                sources[source].append(symbol)

        for symbol in requested_symbols:
            add_symbol("explicit_requested_symbol", symbol)
        for pos in self.ctx.state.open_positions.values():
            add_symbol("open_position", getattr(pos, "symbol", ""))
        for pending in self.ctx.state.pending_entries.values():
            add_symbol("pending_entry", getattr(pending, "symbol", ""))
        for pending in self.ctx.state.pending_closes.values():
            position_id = getattr(pending, "position_id", "")
            pos = self.ctx.state.open_positions.get(position_id)
            add_symbol("pending_close", getattr(pos, "symbol", ""))
            add_symbol("pending_close", getattr(pending, "symbol", ""))
        for pending in self.ctx.state.pending_passive_closes.values():
            snapshot = getattr(pending, "position_snapshot", None)
            add_symbol("pending_passive_close", getattr(snapshot, "symbol", ""))
            add_symbol("pending_passive_close", getattr(pending, "symbol", ""))
        for repair in getattr(self.ctx.state, "pending_residual_repairs", []) or []:
            if isinstance(repair, dict):
                add_symbol("pending_residual_repair", repair.get("symbol", ""))
            else:
                add_symbol("pending_residual_repair", getattr(repair, "symbol", ""))
        ledger = getattr(self.ctx, "recovery_ledger", None)
        for item in getattr(ledger, "work_items", []) or []:
            add_symbol("recovery_ledger_work", getattr(item, "symbol", ""))
        for item in getattr(self.ctx.state, "live_recovery_reduce_only_pairs", []) or []:
            if isinstance(item, dict):
                add_symbol("recent_live_mismatch_cleanup", item.get("symbol", ""))
            else:
                add_symbol("recent_live_mismatch_cleanup", getattr(item, "symbol", ""))
        return sources

    async def _recover_startup_live_positions(
        self,
        symbols: list[str],
        now_ms: int,
        *,
        source: str = "startup_live_position_probe",
    ) -> str:
        """Detect balanced exchange positions that local snapshot/journal missed."""
        if str(getattr(self.ctx.config.runtime, "mode", "")).lower() != "live":
            return "skipped"
        if not self.ctx._venue_adapters:
            return "skipped"
        if (
            self.ctx.state.open_positions
            or self.ctx.state.pending_entries
            or self.ctx.state.pending_closes
            or self.ctx.state.pending_passive_closes
        ):
            return "local_recovery_work"

        snapshots = await self.ctx._fetch_startup_live_position_snapshots(symbols)
        if not snapshots:
            return "no_live_positions"

        created, recovered_indices = self.ctx._hydrate_balanced_startup_live_positions(
            snapshots, now_ms, source=source
        )
        mismatches = [
            item for idx, item in enumerate(snapshots)
            if idx not in recovered_indices
        ]
        if mismatches:
            flattened = await self.ctx._flatten_startup_live_position_mismatches(
                mismatches, now_ms, source=source
            )
            if not flattened:
                self.ctx._block_unpaired_startup_live_positions(
                    mismatches,
                    now_ms,
                    source=source,
                    recovered_open_positions=created,
                    reason="live_position_mismatch_flatten_failed",
                )
                return "mismatch_blocked"
        if created or mismatches:
            self.ctx.journal.append(
                "recovery.live_position_probe_complete",
                {
                    "detected_positions": len(snapshots),
                    "recovered_open_positions": created,
                    "mismatch_positions": len(mismatches),
                    "ts_ms": now_ms,
                },
            )
        if mismatches:
            await self.ctx._refresh_recovery_ledger_for_symbols(
                self.ctx._live_position_snapshot_symbols(mismatches),
                now_ms,
            )
            return "mismatch_flattened"
        if created:
            return "balanced_recovered"
        return "no_recovery_needed"

    @staticmethod
    def _live_position_snapshot_symbols(
        snapshots: list[tuple[str, PositionSnapshot]],
    ) -> list[str]:
        symbols: set[str] = set()
        for requested_symbol, position in snapshots:
            requested = str(requested_symbol or "").upper()
            if requested:
                symbols.add(requested)
                continue
            symbol = str(getattr(position, "symbol", "") or "").upper()
            if symbol:
                symbols.add(symbol)
        return sorted(symbols)

    def _block_unpaired_startup_live_positions(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
        recovered_open_positions: int,
        reason: str = "unpaired_live_positions_detected",
    ) -> None:
        enter_fail_closed(self.ctx.state)
        self.ctx.state.recovery_blocked_reason = reason
        self.ctx.state.recovery_blocked_at_ms = now_ms
        self.ctx.state.last_error = "live exchange position mismatch cleanup failed"
        self.ctx.journal.append(
            "recovery.blocked",
            {
                "reason": self.ctx.state.recovery_blocked_reason,
                "source": source,
                "detected_positions": len(snapshots),
                "recovered_open_positions": recovered_open_positions,
                "positions": [
                    {
                        "requested_symbol": requested_symbol,
                        "venue": pos.venue.value,
                        "symbol": pos.symbol,
                        "side": pos.side.value,
                        "quantity": pos.quantity,
                        "entry_price": pos.entry_price,
                    }
                    for requested_symbol, pos in snapshots
                ],
                "ts_ms": now_ms,
            },
        )

    async def _flatten_startup_live_position_mismatches(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
    ) -> bool:
        flattened: list[dict[str, object]] = []
        failed: list[dict[str, object]] = []
        for requested_symbol, pos in snapshots:
            if abs(pos.quantity) <= 1e-9:
                continue
            cleanup_intent_id = (
                f"live-recovery:{source}:{requested_symbol}:"
                f"{pos.venue.value}:{now_ms}"
            )
            ok = await self.ctx._cleanup_failed_leg_exposure(
                pos.venue,
                requested_symbol,
                cleanup_intent_id,
                "live_recovery_mismatch",
            )
            post_cleanup_truth = await self.ctx._post_cleanup_position_truth(
                pos.venue,
                requested_symbol,
            )
            payload = {
                "requested_symbol": requested_symbol,
                "venue": pos.venue.value,
                "symbol": pos.symbol,
                "side": pos.side.value,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
                "cleanup_intent_id": cleanup_intent_id,
                "post_cleanup_truth": post_cleanup_truth,
            }
            if ok is True:
                flattened.append(payload)
            else:
                payload["cleanup_result"] = ok
                failed.append(payload)

        if failed:
            self.ctx.journal.append(
                "recovery.live_mismatch_flatten_failed",
                {
                    "source": source,
                    "flattened_positions": flattened,
                    "failed_positions": failed,
                    "flattened_count": len(flattened),
                    "failed_count": len(failed),
                    "live_truth_venues": sorted({
                        str(item.get("venue", "")) for item in flattened + failed
                        if item.get("venue")
                    }),
                    "owner_resolution": "unowned_live_artifact",
                    "truth_required_by": "v1_recovery_decision_core",
                    "probe_family": source,
                    "post_cleanup_truth": self.ctx._combined_post_cleanup_truth(
                        flattened + failed
                    ),
                    "ts_ms": now_ms,
                },
            )
            return False

        self.ctx.journal.append(
            "recovery.live_mismatch_flattened",
            {
                "source": source,
                "positions": flattened,
                "flattened_count": len(flattened),
                "failed_count": 0,
                "live_truth_venues": sorted({
                    str(item.get("venue", "")) for item in flattened
                    if item.get("venue")
                }),
                "owner_resolution": "unowned_live_artifact",
                "truth_required_by": "v1_recovery_decision_core",
                "probe_family": source,
                "post_cleanup_truth": self.ctx._combined_post_cleanup_truth(flattened),
                "ts_ms": now_ms,
            },
        )
        return True

    async def _post_cleanup_position_truth(
        self,
        venue: Venue,
        symbol: str,
    ) -> dict[str, object]:
        """Read-only position truth after recovery cleanup for diagnostics."""
        adapter = self.ctx.get_venue_adapter(venue)
        if adapter is None:
            return {
                "truth_available": False,
                "position_qty": 0.0,
                "side": "",
                "error": "adapter_unavailable",
            }
        try:
            pos = await adapter.fetch_position(symbol)
        except Exception as exc:
            return {
                "truth_available": False,
                "position_qty": 0.0,
                "side": "",
                "error": str(exc)[:500],
            }
        if pos is None:
            return {
                "truth_available": True,
                "position_qty": 0.0,
                "side": "",
            }
        qty = abs(float(getattr(pos, "quantity", 0.0) or 0.0))
        side = str(getattr(getattr(pos, "side", ""), "value", "") or "")
        return {
            "truth_available": True,
            "position_qty": qty,
            "side": "" if qty <= 1e-9 else side,
        }

    @staticmethod
    def _combined_post_cleanup_truth(items: list[dict[str, object]]) -> dict[str, object]:
        positions: list[dict[str, object]] = []
        truth_available = True
        for item in items:
            truth = item.get("post_cleanup_truth")
            if not isinstance(truth, dict):
                truth_available = False
                continue
            if not bool(truth.get("truth_available", False)):
                truth_available = False
            qty = float(truth.get("position_qty", 0.0) or 0.0)
            if qty > 1e-9:
                positions.append({
                    "venue": item.get("venue", ""),
                    "symbol": item.get("requested_symbol") or item.get("symbol", ""),
                    "position_qty": qty,
                    "side": truth.get("side", ""),
                })
        return {
            "truth_available": truth_available,
            "positions": positions,
        }

    async def _maybe_recover_clean_live_positions(self, now_ms: int) -> None:
        """Probe private positions when the runtime would otherwise look clean."""
        if str(getattr(self.ctx.config.runtime, "mode", "")).lower() != "live":
            return
        if (
            self.ctx.state.open_positions
            or self.ctx.state.pending_entries
            or self.ctx.state.pending_closes
        ):
            return

        interval_ms = max(self.ctx.config.runtime.private_position_max_age_ms, 1)
        if (
            self.ctx._last_private_position_probe_ms > 0
            and now_ms < self.ctx._last_private_position_probe_ms + interval_ms
        ):
            return

        self.ctx._last_private_position_probe_ms = now_ms
        open_positions_before = len(self.ctx.state.open_positions)
        recovery_result = await self.ctx._recover_startup_live_positions(
            self.ctx._startup_position_probe_symbols({}),
            now_ms,
            source="runtime_live_position_probe",
        )
        if (
            recovery_result == "no_live_positions"
            and self.ctx.state.recovery_blocked_reason
            in {"unpaired_live_position", "owned_pending_entry_live_conflict"}
            and not self.ctx._has_local_recovery_work()
            and self.ctx.state.operator.requested_mode != GlobalRiskMode.FAIL_CLOSED
        ):
            await self.ctx._refresh_recovery_ledger_from_account_truth(
                now_ms,
                lifecycle_clear_reason="runtime_flat_truth_current_state_clean",
            )
        elif (
            recovery_result == "no_live_positions"
            and self.ctx.state.lifecycle == EngineLifecycle.RISK_ONLY
            and self.ctx.state.risk_mode == GlobalRiskMode.RUNNING
            and self.ctx.state.recovery_blocked_reason is None
            and not self.ctx._has_local_recovery_work()
            and self.ctx.state.operator.requested_mode != GlobalRiskMode.FAIL_CLOSED
        ):
            await self.ctx._refresh_recovery_ledger_for_symbols(
                self.ctx._startup_recovery_ledger_symbols({}),
                now_ms,
                lifecycle_clear_reason="runtime_flat_truth_current_state_clean",
            )
        if (
            self.ctx.state.recovery_blocked_reason in CORE_CLEARABLE_BLOCK_REASONS
            and len(self.ctx.state.open_positions) > open_positions_before
            and not self.ctx.state.pending_entries
            and not self.ctx.state.pending_closes
            and not self.ctx.state.pending_passive_closes
            and self.ctx.state.operator.requested_mode != GlobalRiskMode.FAIL_CLOSED
        ):
            self.ctx._finalize_startup_recovery()

    async def _position_probe_symbols_for_venue(
        self, venue: Venue, adapter: VenueAdapter, symbols: list[str],
    ) -> list[str]:
        """Filter fallback single-position probes through a venue symbol catalog."""
        return await self.ctx._filter_symbols_supported_by_venue(
            venue,
            adapter,
            symbols,
            skip_event_kind="recovery.live_position_probe_symbol_skipped",
        )
