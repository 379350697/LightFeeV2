"""Independent recovery route for live exchange positions without a local owner."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lightfee.core.domain import OrderRequest, PositionSnapshot, Side, TimeInForce, Venue
from lightfee.engine.recovery_ledger import RecoveryLedger, RecoveryWorkItem
from lightfee.engine.recovery_symbol_identity import canonical_recovery_symbol
from lightfee.venues.cid import generate_exchange_cid

if TYPE_CHECKING:
    from lightfee.engine.runtime_context import RuntimeContext

EPSILON = 1e-9
BACKOFF_MS = 30_000
MAX_ATTEMPTS = 3
POSITION_TRUTH_MAX_AGE_MS = 10_000
TERMINAL_MANUAL_REQUIRED = "manual_required"
TERMINAL_OWNER_REASSOCIATED = "owner_reassociated"


class UnpairedLivePositionRecoveryRuntime:
    """Drive unpaired live-position cleanup without owning strategy state."""

    def __init__(self, ctx: "RuntimeContext") -> None:
        self.ctx = ctx

    def register_from_ledger(self, ledger: RecoveryLedger, *, now_ms: int) -> None:
        records = self._records()
        unpaired_keys: set[tuple[str, str, str]] = set()
        open_order_keys: set[tuple[str, str]] = set()
        owned_position_keys: set[tuple[str, str, str]] = set()
        for item in ledger.work_items:
            for artifact in item.artifacts:
                if artifact.kind == "open_order":
                    open_order_keys.add(
                        (
                            str(artifact.venue or ""),
                            canonical_recovery_symbol(artifact.symbol, artifact.venue),
                        )
                    )
                if artifact.kind == "position" and item.kind == "owned_open_position":
                    owned_position_keys.add(
                        (
                            str(artifact.venue or ""),
                            canonical_recovery_symbol(artifact.symbol, artifact.venue),
                            str(artifact.side or "").lower(),
                        )
                    )
            if item.kind != "unpaired_live_position":
                continue
            artifact = self._position_artifact(item)
            if artifact is None:
                continue
            venue = str(artifact.venue or "")
            symbol = canonical_recovery_symbol(artifact.symbol, artifact.venue)
            side = str(artifact.side or "").lower()
            quantity = float(artifact.quantity or 0.0)
            if not venue or not symbol or quantity <= EPSILON:
                continue
            unpaired_keys.add((venue, symbol, side))
            notional = abs(quantity) * float(artifact.price or 0.0)
            cap = self._cap_quote()
            existing = self._find_record(records, venue, symbol, side)
            if existing is None:
                record = {
                    "venue": venue,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "notional_quote": notional,
                    "first_seen_ms": int(now_ms),
                    "attempt_count": 0,
                    "next_attempt_ms": int(now_ms),
                    "last_error": "",
                    "terminal_status": "",
                    "owner_excluded": True,
                    "open_order_truth_available": False,
                    "cap_quote": cap,
                    "cap_ok": notional > EPSILON and notional <= cap + EPSILON,
                }
                records.append(record)
                self._append(
                    "recovery.unpaired_live_position_detected",
                    record,
                    now_ms=now_ms,
                )
                self._append(
                    "recovery.unpaired_live_position_owner_excluded",
                    record,
                    now_ms=now_ms,
                )
            elif str(existing.get("terminal_status") or "") != "flat":
                existing.update(
                    {
                        "quantity": quantity,
                        "notional_quote": notional,
                        "cap_quote": cap,
                        "cap_ok": notional > EPSILON and notional <= cap + EPSILON,
                    }
                )
        if ledger.truth_available:
            for record in records:
                if not is_active_unpaired_live_position_recovery(record):
                    continue
                venue = str(record.get("venue") or "")
                symbol = canonical_recovery_symbol(
                    record.get("symbol"),
                    record.get("venue"),
                )
                side = str(record.get("side") or "").lower()
                if (venue, symbol, side) in unpaired_keys:
                    continue
                if (venue, symbol) in open_order_keys:
                    continue
                if (venue, symbol, side) in owned_position_keys:
                    self._mark_owner_reassociated(
                        record,
                        now_ms,
                        reason="ledger_position_has_runtime_owner",
                    )
                    continue
                self._mark_terminal_flat(
                    record,
                    now_ms,
                    reason="ledger_clean_flat_no_open_orders",
                )
        self.ctx.state.unpaired_live_position_recoveries = records

    async def drive(self, *, now_ms: int) -> None:
        for record in list(self._records()):
            if not is_active_unpaired_live_position_recovery(record):
                continue
            if str(record.get("terminal_status") or "").lower() == TERMINAL_MANUAL_REQUIRED:
                continue
            if int(record.get("next_attempt_ms") or 0) > now_ms:
                continue
            if int(record.get("attempt_count") or 0) >= MAX_ATTEMPTS:
                self._mark_manual_required(record, now_ms, reason="max_attempts_exceeded")
                continue
            await self._drive_one(record, now_ms)

    async def _drive_one(self, record: dict[str, Any], now_ms: int) -> None:
        if not self._auto_enabled():
            self._skip(record, "auto_disabled", now_ms)
            return
        venue = self._venue(record.get("venue"))
        if venue is None:
            self._skip(record, "invalid_venue", now_ms)
            return
        symbol = str(record.get("symbol") or "")
        if not self._symbol_configured(symbol):
            self._skip(record, "symbol_not_configured", now_ms)
            return
        if not self._venue_configured(venue):
            self._skip(record, "venue_not_configured", now_ms)
            return
        adapter = self.ctx.get_venue_adapter(venue)
        if adapter is None:
            self._skip(record, "adapter_unavailable", now_ms)
            return

        position = await self._fetch_position(adapter, symbol, record, now_ms)
        if position is None:
            return
        open_orders = await self._fetch_open_orders(adapter, symbol, record, now_ms)
        if open_orders is None:
            return
        if self._has_non_reduce_open_order(open_orders, symbol):
            self._skip(record, "non_reduce_open_order_conflict", now_ms)
            return
        record["open_order_truth_available"] = True

        if float(position.quantity or 0.0) <= EPSILON:
            self._mark_terminal_flat(record, now_ms, reason="already_flat")
            return

        self._refresh_record_from_position(record, position)
        cap = self._cap_quote()
        record["cap_quote"] = cap
        notional = float(record.get("notional_quote") or 0.0)
        if notional <= EPSILON:
            record["cap_ok"] = False
            self._skip(record, "notional_unknown", now_ms)
            return
        record["cap_ok"] = notional <= cap + EPSILON
        if not record["cap_ok"]:
            self._skip(record, "cap_exceeded", now_ms)
            return

        cleanup_qty = await self._normalize_quantity(
            adapter,
            symbol,
            float(position.quantity or 0.0),
        )
        if cleanup_qty <= EPSILON:
            self._skip(record, "quantity_normalized_to_zero", now_ms)
            return

        attempt = int(record.get("attempt_count") or 0) + 1
        record["attempt_count"] = attempt
        record["last_attempt_ms"] = int(now_ms)
        record["next_attempt_ms"] = int(now_ms) + BACKOFF_MS
        self._append(
            "recovery.unpaired_live_position_cleanup_attempt",
            record,
            now_ms=now_ms,
        )
        try:
            fill = await adapter.place_order(
                OrderRequest(
                    venue=venue,
                    symbol=symbol,
                    side=position.side.opposite(),
                    quantity=cleanup_qty,
                    price=None,
                    reduce_only=True,
                    client_order_id=generate_exchange_cid(
                        f"unpaired:{venue.value}:{symbol}:{attempt}",
                        "cleanup",
                        venue,
                    ),
                    post_only=False,
                    time_in_force=TimeInForce.IOC,
                )
            )
        except Exception as exc:
            self._fail(record, "submit_failed", now_ms, detail=str(exc))
            return

        self._append(
            "recovery.unpaired_live_position_cleanup_submitted",
            {
                **record,
                "order_id": getattr(fill, "order_id", ""),
                "filled_quantity": float(getattr(fill, "quantity", 0.0) or 0.0),
            },
            now_ms=now_ms,
        )
        fresh_position = await self._fetch_position(adapter, symbol, record, now_ms)
        fresh_open_orders = await self._fetch_open_orders(adapter, symbol, record, now_ms)
        if (
            fresh_position is not None
            and fresh_open_orders is not None
            and float(fresh_position.quantity or 0.0) <= EPSILON
            and not self._has_any_open_order(fresh_open_orders, symbol)
        ):
            self._append(
                "recovery.unpaired_live_position_cleanup_succeeded",
                record,
                now_ms=now_ms,
            )
            self._mark_terminal_flat(record, now_ms, reason="cleanup_succeeded")
        else:
            if fresh_position is not None and float(fresh_position.quantity or 0.0) <= EPSILON:
                self._fail(record, "open_orders_still_present", now_ms)
            else:
                self._fail(record, "position_still_nonzero", now_ms)

    async def _fetch_position(
        self,
        adapter: Any,
        symbol: str,
        record: dict[str, Any],
        now_ms: int,
    ) -> PositionSnapshot | None:
        try:
            position = await adapter.fetch_position(symbol)
        except Exception as exc:
            self._fail(record, "position_truth_unavailable", now_ms, detail=str(exc))
            return None
        observed_at_ms = int(getattr(position, "observed_at_ms", 0) or 0)
        age_ms = (
            max(0, int(now_ms) - observed_at_ms)
            if observed_at_ms > 0
            else POSITION_TRUTH_MAX_AGE_MS + 1
        )
        record["position_truth_observed_at_ms"] = observed_at_ms
        record["position_truth_age_ms"] = age_ms
        if age_ms > POSITION_TRUTH_MAX_AGE_MS:
            self._skip(record, "position_truth_stale", now_ms)
            return None
        return position

    async def _fetch_open_orders(
        self,
        adapter: Any,
        symbol: str,
        record: dict[str, Any],
        now_ms: int,
    ) -> list[Any] | None:
        fetch = getattr(adapter, "fetch_open_orders", None)
        if fetch is None:
            self._skip(record, "open_order_truth_unavailable", now_ms)
            return None
        try:
            orders = await fetch(symbol)
        except Exception as exc:
            self._skip(
                record,
                "open_order_truth_unavailable",
                now_ms,
                detail=str(exc),
            )
            return None
        record["open_order_truth_available"] = True
        return _order_rows(orders)

    async def _normalize_quantity(self, adapter: Any, symbol: str, quantity: float) -> float:
        normalize = getattr(adapter, "normalize_quantity", None)
        if normalize is None:
            return abs(quantity)
        return abs(float(await normalize(symbol, abs(quantity)) or 0.0))

    def _skip(
        self,
        record: dict[str, Any],
        reason: str,
        now_ms: int,
        *,
        detail: str = "",
    ) -> None:
        record["last_error"] = reason
        record["next_attempt_ms"] = int(now_ms) + BACKOFF_MS
        payload = {
            **record,
            "reason": reason,
            "auto_enabled": self._auto_enabled(),
            **self._risk_exposure_payload(record, reason),
        }
        if detail:
            payload["detail"] = detail[:240]
        self._append(
            "recovery.unpaired_live_position_cleanup_skipped",
            payload,
            now_ms=now_ms,
        )

    def _fail(
        self,
        record: dict[str, Any],
        reason: str,
        now_ms: int,
        *,
        detail: str = "",
    ) -> None:
        record["last_error"] = reason
        record["next_attempt_ms"] = int(now_ms) + BACKOFF_MS
        payload = {
            **record,
            "reason": reason,
            **self._risk_exposure_payload(record, reason),
        }
        if detail:
            payload["detail"] = detail[:240]
        self._append(
            "recovery.unpaired_live_position_cleanup_failed",
            payload,
            now_ms=now_ms,
        )

    def _mark_terminal_flat(
        self,
        record: dict[str, Any],
        now_ms: int,
        *,
        reason: str,
    ) -> None:
        record["terminal_status"] = "flat"
        record["last_error"] = ""
        record["next_attempt_ms"] = int(now_ms)
        self._append(
            "recovery.unpaired_live_position_terminal_flat",
            {**record, "reason": reason},
            now_ms=now_ms,
        )

    def _mark_owner_reassociated(
        self,
        record: dict[str, Any],
        now_ms: int,
        *,
        reason: str,
    ) -> None:
        record["terminal_status"] = TERMINAL_OWNER_REASSOCIATED
        record["last_error"] = ""
        record["next_attempt_ms"] = int(now_ms)
        self._append(
            "recovery.unpaired_live_position_owner_reassociated",
            {**record, "reason": reason},
            now_ms=now_ms,
        )

    def _mark_manual_required(
        self,
        record: dict[str, Any],
        now_ms: int,
        *,
        reason: str,
    ) -> None:
        record["terminal_status"] = TERMINAL_MANUAL_REQUIRED
        record["last_error"] = reason
        record["next_attempt_ms"] = int(now_ms)
        self._append(
            "recovery.unpaired_live_position_cleanup_failed",
            {
                **record,
                "reason": reason,
                **self._risk_exposure_payload(record, reason),
            },
            now_ms=now_ms,
        )

    def _append(self, kind: str, payload: dict[str, Any], *, now_ms: int) -> None:
        self.ctx.journal.append(kind, dict(payload), ts_ms=int(now_ms))

    def _records(self) -> list[dict[str, Any]]:
        return list(getattr(self.ctx.state, "unpaired_live_position_recoveries", []) or [])

    @staticmethod
    def _find_record(
        records: list[dict[str, Any]],
        venue: str,
        symbol: str,
        side: str,
    ) -> dict[str, Any] | None:
        for record in records:
            if (
                str(record.get("venue") or "") == venue
                and canonical_recovery_symbol(
                    record.get("symbol"),
                    record.get("venue"),
                )
                == canonical_recovery_symbol(symbol, venue)
                and str(record.get("side") or "") == side
            ):
                return record
        return None

    @staticmethod
    def _position_artifact(item: RecoveryWorkItem) -> Any | None:
        for artifact in item.artifacts:
            if artifact.kind == "position":
                return artifact
        return None

    @staticmethod
    def _venue(value: Any) -> Venue | None:
        try:
            return Venue.from_str(str(value or ""))
        except Exception:
            return None

    def _auto_enabled(self) -> bool:
        return bool(self.ctx.config.strategy.unpaired_live_position_auto_recovery_enabled)

    def _cap_quote(self) -> float:
        strategy = self.ctx.config.strategy
        explicit = float(strategy.unpaired_live_position_max_notional_quote or 0.0)
        if explicit > 0:
            return explicit
        return float(strategy.live_entry_notional_cap_quote or 0.0)

    def _symbol_configured(self, symbol: str) -> bool:
        symbols = [str(s).upper() for s in self.ctx.config.symbols]
        return bool(symbols) and symbol.upper() in symbols

    def _venue_configured(self, venue: Venue) -> bool:
        configured = [str(v.venue or "").lower() for v in self.ctx.config.venues]
        return bool(configured) and venue.value in configured

    @staticmethod
    def _risk_exposure_payload(
        record: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        current_exposure = (
            str(record.get("terminal_status") or "").lower() != "flat"
            and abs(float(record.get("quantity") or 0.0)) > EPSILON
        )
        return {
            "current_risk_exposure": current_exposure,
            "business_terminal": False,
            "diagnostic_severity": "critical" if current_exposure else "warning",
            "next_action": _unpaired_next_action(reason),
        }

    @staticmethod
    def _has_non_reduce_open_order(open_orders: list[Any], symbol: str) -> bool:
        for order in open_orders:
            order_symbol = str(_get(order, "symbol", "") or "")
            if order_symbol and order_symbol.upper() != symbol.upper():
                continue
            quantity = _order_quantity(order)
            if quantity <= EPSILON:
                continue
            if not _truthy_order_field(
                _get(order, "reduce_only", _get(order, "reduceOnly", False))
            ):
                return True
        return False

    @staticmethod
    def _has_any_open_order(open_orders: list[Any], symbol: str) -> bool:
        for order in open_orders:
            order_symbol = str(_get(order, "symbol", "") or "")
            if order_symbol and order_symbol.upper() != symbol.upper():
                continue
            if _order_quantity(order) > EPSILON:
                return True
        return False

    @staticmethod
    def _refresh_record_from_position(
        record: dict[str, Any],
        position: PositionSnapshot,
    ) -> None:
        side = position.side.value if isinstance(position.side, Side) else str(position.side)
        quantity = abs(float(position.quantity or 0.0))
        entry_price = float(position.entry_price or 0.0)
        record.update(
            {
                "side": side,
                "quantity": quantity,
                "notional_quote": quantity * entry_price,
            }
        )


def _unpaired_next_action(reason: str) -> str:
    return {
        "auto_disabled": "operator_or_config_enable_required",
        "cap_exceeded": "operator_flatten_or_raise_cap_required",
        "notional_unknown": "operator_truth_or_manual_flatten_required",
        "position_truth_stale": "retry_fresh_position_truth",
        "position_truth_unavailable": "retry_fresh_position_truth",
        "open_order_truth_unavailable": "retry_open_order_truth",
        "non_reduce_open_order_conflict": "operator_reconcile_open_order_conflict",
        "quantity_normalized_to_zero": "operator_manual_flatten_required",
        "max_attempts_exceeded": "operator_manual_flatten_required",
        "submit_failed": "operator_manual_flatten_required",
    }.get(str(reason or ""), "retry_or_operator_reconcile")


def _get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def is_active_unpaired_live_position_recovery(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    return str(record.get("terminal_status") or "").lower() not in {
        "flat",
        TERMINAL_OWNER_REASSOCIATED,
    }


def active_unpaired_live_position_recovery_records(state: Any) -> list[dict[str, Any]]:
    records = getattr(state, "unpaired_live_position_recoveries", []) or []
    return [
        record
        for record in records
        if is_active_unpaired_live_position_recovery(record)
    ]


def _order_quantity(order: Any) -> float:
    raw = _get(
        order,
        "quantity",
        _get(
            order,
            "origQty",
            _get(
                order,
                "qty",
                _get(order, "size", _get(order, "sz", _get(order, "amount", 0.0))),
            ),
        ),
    )
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _truthy_order_field(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _order_rows(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        rows: list[Any] = []
        for item in raw:
            if isinstance(item, dict):
                nested = (
                    item.get("orders")
                    or item.get("open_orders")
                    or item.get("data")
                    or item.get("result")
                )
                if isinstance(nested, list):
                    rows.extend(nested)
                    continue
            rows.append(item)
        return rows
    if isinstance(raw, dict):
        result = raw.get("result")
        if isinstance(result, dict) and isinstance(result.get("list"), list):
            return list(result["list"])
        data = raw.get("data")
        if isinstance(data, dict):
            for key in ("entrustedList", "orderList", "list", "orders"):
                nested = data.get(key)
                if isinstance(nested, list):
                    return list(nested)
        for key in ("orders", "open_orders", "openOrders", "list", "data", "result"):
            nested = raw.get(key)
            if isinstance(nested, list):
                return list(nested)
    return []
