"""Exchange-truth-first lifecycle reconstruction.

This module is intentionally read-only: it turns journal/accounting events into
one canonical lifecycle classification per position. Runtime code and offline
analysis can then consume the same truth boundary instead of re-deriving it.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


JsonDict = dict[str, Any]
QTY_TOLERANCE = Decimal("0.999")


class LifecycleClassification(str, Enum):
    EXCHANGE_LIFECYCLE_COMPLETE = "exchange_lifecycle_complete"
    PHANTOM_ZERO_QTY_OPENED = "phantom_zero_qty_opened"
    EXCHANGE_LIFECYCLE_INCOMPLETE = "exchange_lifecycle_incomplete"
    EVIDENCE_INCOMPLETE = "evidence_incomplete"


@dataclass
class _FillFact:
    phase: str
    leg: str
    venue: str
    order_id: str = ""
    client_order_id: str = ""
    qty: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    fee_quote: Decimal = Decimal("0")
    trade_side: str = ""
    raw_side: str = ""
    filled_at_ms: int = 0
    source: str = ""
    confidence: str = "exchange_statement"
    trade_id: str = ""
    exec_id: str = ""
    fill_event_id: str = ""
    fill_event_anchor_id: str = ""

    def to_dict(self) -> JsonDict:
        return {
            "phase": self.phase,
            "leg": self.leg,
            "venue": self.venue,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "quantity": _decimal_str(self.qty),
            "price": _decimal_str(self.price),
            "fee_quote": _decimal_str(self.fee_quote),
            "trade_side": self.trade_side,
            "raw_side": self.raw_side,
            "filled_at_ms": self.filled_at_ms,
            "source": self.source,
            "confidence": self.confidence,
            "trade_id": self.trade_id,
            "exec_id": self.exec_id,
            "fill_event_id": self.fill_event_id,
            "fill_event_anchor_id": self.fill_event_anchor_id,
        }


@dataclass
class _PositionFacts:
    position_id: str
    entry: JsonDict | None = None
    entry_ts_ms: int = 0
    event_kinds: Counter[str] = field(default_factory=Counter)
    exit_reconciled: list[JsonDict] = field(default_factory=list)
    exit_closed: list[JsonDict] = field(default_factory=list)
    fills: list[_FillFact] = field(default_factory=list)
    order_identities: list[JsonDict] = field(default_factory=list)
    funding_facts: list[JsonDict] = field(default_factory=list)


def build_exchange_truth_lifecycle(
    events: list[JsonDict],
    *,
    position_ids: set[str] | list[str] | tuple[str, ...] | None = None,
) -> JsonDict:
    """Build canonical lifecycle truth for positions visible in events."""

    selected_ids = {str(item) for item in position_ids or [] if str(item)}
    positions: dict[str, _PositionFacts] = {}
    for event in sorted(events, key=_event_ts_ms):
        kind = str(event.get("kind") or "")
        payload = _payload(event)
        position_id = _position_id(payload)
        if not position_id or (selected_ids and position_id not in selected_ids):
            continue
        facts = positions.setdefault(position_id, _PositionFacts(position_id=position_id))
        facts.event_kinds[kind] += 1
        if kind in {"entry.opened", "runtime.position_opened"}:
            facts.entry = _merge_entry_payload(facts.entry, payload, kind=kind)
            facts.entry_ts_ms = _event_ts_ms(event) or _first_int(
                payload.get("opened_at_ms"),
                payload.get("entered_at_ms"),
            )
            _collect_identity_from_payload(facts, payload, kind=kind)
        elif kind == "exit.reconciled":
            facts.exit_reconciled.append({"ts_ms": _event_ts_ms(event), "payload": payload})
            _collect_identity_from_payload(facts, payload, kind=kind)
            _collect_exit_reconciled_fills(facts, payload, _event_ts_ms(event))
        elif kind == "exit.closed":
            facts.exit_closed.append({"ts_ms": _event_ts_ms(event), "payload": payload})
            _collect_identity_from_payload(facts, payload, kind=kind)
        elif kind == "order.filled":
            fill = _fill_fact_from_order_filled(facts, payload, _event_ts_ms(event))
            if fill is not None:
                facts.fills.append(fill)
            _collect_identity_from_payload(facts, payload, kind=kind)
        elif kind == "accounting.close_statement_backfill_corrected":
            _collect_backfill_correction_fills(facts, payload, _event_ts_ms(event))
        elif kind == "accounting.lifecycle_truth_rebuilt":
            _collect_lifecycle_truth_rebuilt_fills(facts, payload, _event_ts_ms(event))
        elif "funding" in kind:
            facts.funding_facts.append({"ts_ms": _event_ts_ms(event), "kind": kind, "payload": payload})
        else:
            _collect_identity_from_payload(facts, payload, kind=kind)

    report_positions = {
        position_id: _finalize_position(facts)
        for position_id, facts in sorted(positions.items())
    }
    summary_counts = Counter(
        str(row.get("classification") or "") for row in report_positions.values()
    )
    project_counts = Counter(
        str(row.get("project_record_status") or "") for row in report_positions.values()
    )
    return {
        "summary": {
            "position_count": len(report_positions),
            **{key: summary_counts.get(key, 0) for key in [item.value for item in LifecycleClassification]},
            "project_record_status_counts": dict(sorted(project_counts.items())),
        },
        "positions": report_positions,
    }


def _finalize_position(facts: _PositionFacts) -> JsonDict:
    entry = facts.entry or {}
    fills = _dedupe_fills(facts.fills)
    target_qty = _target_quantity(entry, fills)
    explicit_zero_entry = _entry_explicit_zero_qty(entry)
    open_by_leg = {
        "long": [fill for fill in fills if fill.phase == "open" and fill.leg == "long"],
        "short": [fill for fill in fills if fill.phase == "open" and fill.leg == "short"],
    }
    close_by_leg = {
        "long": [fill for fill in fills if fill.phase == "close" and fill.leg == "long"],
        "short": [fill for fill in fills if fill.phase == "close" and fill.leg == "short"],
    }
    open_coverage = {
        leg: _coverage_row(open_by_leg[leg], target_qty)
        for leg in ("long", "short")
    }
    close_coverage = {
        leg: _coverage_row(close_by_leg[leg], target_qty)
        for leg in ("long", "short")
    }
    open_long_covered = bool(open_coverage["long"]["covered"])
    open_short_covered = bool(open_coverage["short"]["covered"])
    close_long_covered = bool(close_coverage["long"]["covered"])
    close_short_covered = bool(close_coverage["short"]["covered"])
    project_status = _project_record_status(facts, open_coverage, close_coverage, explicit_zero_entry)

    if explicit_zero_entry and not any(fill.qty > 0 for fill in fills):
        classification = LifecycleClassification.PHANTOM_ZERO_QTY_OPENED.value
    elif target_qty <= 0:
        classification = LifecycleClassification.EVIDENCE_INCOMPLETE.value
    elif open_long_covered and open_short_covered and close_long_covered and close_short_covered:
        classification = LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value
    elif any(fill.qty > 0 for fill in fills):
        classification = LifecycleClassification.EXCHANGE_LIFECYCLE_INCOMPLETE.value
    else:
        classification = LifecycleClassification.EVIDENCE_INCOMPLETE.value

    pnl = _pnl_from_truth(
        entry,
        open_by_leg,
        close_by_leg,
        facts,
        classification,
        target_qty,
        open_coverage,
        close_coverage,
    )
    source_gaps = _source_coverage_gaps(
        fills,
        target_qty,
        open_coverage,
        close_coverage,
        classification,
    )
    return {
        "position_id": facts.position_id,
        "symbol": str(entry.get("symbol") or _first_payload_value(facts, "symbol") or "").upper(),
        "long_venue": str(entry.get("long_venue") or entry.get("long_exchange") or ""),
        "short_venue": str(entry.get("short_venue") or entry.get("short_exchange") or ""),
        "target_quantity": _decimal_str(target_qty),
        "classification": classification,
        "project_record_status": project_status,
        "open_legs": [fill.to_dict() for fill in open_by_leg["long"] + open_by_leg["short"]],
        "close_legs": [fill.to_dict() for fill in close_by_leg["long"] + close_by_leg["short"]],
        "open_coverage": open_coverage,
        "close_coverage": close_coverage,
        "order_identity_history": facts.order_identities,
        "funding_facts": facts.funding_facts,
        "pnl": pnl,
        "source_coverage": {
            "event_kind_counts": dict(sorted(facts.event_kinds.items())),
            "gaps": source_gaps,
        },
    }


def _merge_entry_payload(existing: JsonDict | None, incoming: JsonDict, *, kind: str) -> JsonDict:
    if existing is None:
        return dict(incoming)
    merged = dict(existing)
    prefer_incoming = kind == "entry.opened"
    for key, value in incoming.items():
        if _missing_entry_value(value):
            continue
        if prefer_incoming or _missing_entry_value(merged.get(key)):
            merged[key] = value
    return merged


def _missing_entry_value(value: Any) -> bool:
    return value is None or value == ""


def _collect_exit_reconciled_fills(facts: _PositionFacts, payload: JsonDict, ts_ms: int) -> None:
    if not _exit_reconciled_complete(payload):
        return
    emitted_from_leg_rows: set[str] = set()
    for leg, key in (("long", "long_legs"), ("short", "short_legs")):
        legs = payload.get(key)
        if isinstance(legs, list):
            for row in legs:
                fill = _fill_fact_from_leg_row(
                    row,
                    phase="close",
                    leg=leg,
                    default_source="exit.reconciled",
                    ts_ms=ts_ms,
                )
                if fill is not None:
                    facts.fills.append(fill)
                    emitted_from_leg_rows.add(leg)
                    _collect_identity_from_payload(facts, row, kind="exit.reconciled")
    for leg in ("long", "short"):
        if leg in emitted_from_leg_rows:
            continue
        qty = _decimal(payload.get(f"{leg}_closed_qty"))
        if qty <= 0:
            continue
        order_id = str(payload.get(f"{leg}_order_id") or "")
        client_order_id = str(payload.get(f"{leg}_client_order_id") or "")
        if not order_id and not client_order_id:
            continue
        fill = _FillFact(
            phase="close",
            leg=leg,
            venue=str(payload.get(f"{leg}_venue") or ""),
            order_id=order_id,
            client_order_id=client_order_id,
            qty=qty,
            price=_decimal(payload.get(f"{leg}_average_price") or payload.get(f"{leg}_price")),
            fee_quote=_decimal(payload.get(f"{leg}_fee_quote")),
            filled_at_ms=ts_ms,
            source="exit.reconciled",
        )
        facts.fills.append(fill)
        _collect_identity_from_payload(facts, fill.to_dict(), kind="exit.reconciled")


def _collect_backfill_correction_fills(facts: _PositionFacts, payload: JsonDict, ts_ms: int) -> None:
    for leg, key in (("long", "long_fills"), ("short", "short_fills")):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            fill = _fill_fact_from_leg_row(
                row,
                phase="close",
                leg=leg,
                default_source="accounting.close_statement_backfill_corrected",
                ts_ms=ts_ms,
            )
            if fill is not None:
                facts.fills.append(fill)
                _collect_identity_from_payload(facts, row, kind="accounting.close_statement_backfill_corrected")


def _collect_lifecycle_truth_rebuilt_fills(facts: _PositionFacts, payload: JsonDict, ts_ms: int) -> None:
    truth = payload.get("truth")
    if not isinstance(truth, dict):
        return
    if facts.entry is None:
        facts.entry = {
            "position_id": facts.position_id,
            "symbol": truth.get("symbol"),
            "quantity": truth.get("target_quantity"),
            "long_venue": truth.get("long_venue"),
            "short_venue": truth.get("short_venue"),
        }
    for phase, key in (("open", "open_legs"), ("close", "close_legs")):
        legs = truth.get(key)
        if not isinstance(legs, list):
            continue
        for row in legs:
            if not isinstance(row, dict):
                continue
            leg = str(row.get("leg") or "").lower()
            if leg not in {"long", "short"}:
                continue
            fill = _fill_fact_from_leg_row(
                row,
                phase=phase,
                leg=leg,
                default_source="accounting.lifecycle_truth_rebuilt",
                ts_ms=ts_ms,
            )
            if fill is not None:
                facts.fills.append(fill)
                _collect_identity_from_payload(facts, row, kind="accounting.lifecycle_truth_rebuilt")


def _fill_fact_from_order_filled(facts: _PositionFacts, payload: JsonDict, ts_ms: int) -> _FillFact | None:
    phase = _fill_phase(payload)
    if phase not in {"open", "close"}:
        return None
    leg = _fill_leg(payload, facts.entry or {})
    if leg not in {"long", "short"}:
        return None
    qty = _decimal(
        payload.get("quantity")
        or payload.get("filled_qty")
        or payload.get("executed_qty")
        or payload.get("size")
    )
    if qty <= 0:
        return None
    return _FillFact(
        phase=phase,
        leg=leg,
        venue=str(payload.get("venue") or payload.get("exchange") or ""),
        order_id=str(payload.get("order_id") or payload.get("orderId") or ""),
        client_order_id=str(
            payload.get("client_order_id")
            or payload.get("clientOrderId")
            or payload.get("clientOid")
            or payload.get("orderLinkId")
            or ""
        ),
        qty=qty,
        price=_decimal(
            payload.get("average_price")
            or payload.get("avg_price")
            or payload.get("price")
            or payload.get("avgPx")
        ),
        fee_quote=_decimal(payload.get("fee_quote") or payload.get("fee")),
        trade_side=str(payload.get("tradeSide") or payload.get("trade_side") or ""),
        raw_side=str(payload.get("side") or ""),
        filled_at_ms=_first_int(payload.get("filled_at_ms"), ts_ms),
        source=str(payload.get("source") or "order.filled"),
        trade_id=str(payload.get("trade_id") or payload.get("tradeId") or ""),
        exec_id=str(payload.get("exec_id") or payload.get("execId") or ""),
        fill_event_id=str(payload.get("fill_event_id") or ""),
        fill_event_anchor_id=str(payload.get("fill_event_anchor_id") or ""),
    )


def _fill_fact_from_leg_row(
    row: Any,
    *,
    phase: str,
    leg: str,
    default_source: str,
    ts_ms: int,
) -> _FillFact | None:
    if not isinstance(row, dict):
        return None
    qty = _decimal(row.get("quantity") or row.get("filled_qty") or row.get("qty"))
    if qty <= 0:
        return None
    return _FillFact(
        phase=phase,
        leg=leg,
        venue=str(row.get("venue") or row.get("exchange") or ""),
        order_id=str(row.get("order_id") or row.get("orderId") or ""),
        client_order_id=str(
            row.get("client_order_id")
            or row.get("clientOrderId")
            or row.get("clientOid")
            or ""
        ),
        qty=qty,
        price=_decimal(row.get("average_price") or row.get("avg_price") or row.get("price")),
        fee_quote=_decimal(row.get("fee_quote") or row.get("fee")),
        trade_side=str(row.get("tradeSide") or row.get("trade_side") or ""),
        raw_side=str(row.get("side") or ""),
        filled_at_ms=_first_int(row.get("filled_at_ms"), ts_ms),
        source=str(row.get("source") or default_source),
        trade_id=str(row.get("trade_id") or row.get("tradeId") or ""),
        exec_id=str(row.get("exec_id") or row.get("execId") or ""),
        fill_event_id=str(row.get("fill_event_id") or ""),
        fill_event_anchor_id=str(row.get("fill_event_anchor_id") or ""),
    )


def _collect_identity_from_payload(facts: _PositionFacts, payload: Any, *, kind: str) -> None:
    if not isinstance(payload, dict):
        return
    rows: list[JsonDict] = []
    for key in ("long_legs", "short_legs"):
        items = payload.get(key)
        if isinstance(items, list):
            leg = "long" if key == "long_legs" else "short"
            for item in items:
                if isinstance(item, dict):
                    row = dict(item)
                    row.setdefault("leg", leg)
                    rows.append(row)
    items = payload.get("statement_probe_candidates")
    if isinstance(items, list):
        rows.extend(item for item in items if isinstance(item, dict))
    for leg in ("long", "short"):
        leg_order_id = _first_str(
            payload.get(f"{leg}_order_id"),
            payload.get(f"{leg}_entry_order_id"),
            payload.get(f"{leg}_close_order_id"),
        )
        leg_client_order_id = _first_str(
            payload.get(f"{leg}_client_order_id"),
            payload.get(f"{leg}_entry_client_order_id"),
            payload.get(f"{leg}_close_client_order_id"),
        )
        if leg_order_id or leg_client_order_id:
            rows.append(
                {
                    "phase": _identity_phase_from_kind(kind, payload),
                    "leg": leg,
                    "venue": payload.get(f"{leg}_venue") or payload.get(f"{leg}_exchange"),
                    "order_id": leg_order_id,
                    "client_order_id": leg_client_order_id,
                    "quantity_hint": (
                        payload.get(f"{leg}_quantity")
                        or payload.get(f"{leg}_closed_qty")
                        or payload.get("matched_quantity")
                        or payload.get("quantity")
                    ),
                    "source": payload.get("source") or kind,
                }
            )
    rows.append(payload)
    seen = {
        (
            str(row.get("phase") or ""),
            str(row.get("leg") or ""),
            str(row.get("venue") or row.get("exchange") or ""),
            str(row.get("order_id") or row.get("orderId") or ""),
            str(row.get("client_order_id") or row.get("clientOrderId") or row.get("clientOid") or ""),
        )
        for row in facts.order_identities
    }
    for row in rows:
        order_id = str(row.get("order_id") or row.get("orderId") or "")
        client_order_id = str(
            row.get("client_order_id")
            or row.get("clientOrderId")
            or row.get("clientOid")
            or row.get("orderLinkId")
            or ""
        )
        if not order_id and not client_order_id:
            continue
        identity = {
            "phase": str(row.get("phase") or _fill_phase(row) or _identity_phase_from_kind(kind, row) or ""),
            "leg": str(row.get("leg") or ""),
            "venue": str(row.get("venue") or row.get("exchange") or ""),
            "order_id": order_id,
            "client_order_id": client_order_id,
            "source_kind": kind,
            "source": str(row.get("source") or ""),
            "quantity_hint": _decimal_str(
                _decimal(row.get("quantity_hint") or row.get("quantity") or row.get("qty"))
            ),
            "accepted_only": bool(row.get("accepted_only") or row.get("truth_gap_candidate")),
            "statement_probe_candidate": bool(row.get("statement_probe_candidate")),
        }
        key = (
            identity["phase"],
            identity["leg"],
            identity["venue"],
            identity["order_id"],
            identity["client_order_id"],
        )
        if key in seen:
            continue
        seen.add(key)
        facts.order_identities.append(identity)


def _identity_phase_from_kind(kind: str, payload: JsonDict) -> str:
    explicit = str(payload.get("phase") or "").lower()
    if explicit in {"open", "close"}:
        return explicit
    if kind in {"entry.opened", "runtime.position_opened"}:
        return "open"
    if kind.startswith("exit.") or kind.startswith("accounting."):
        return "close"
    source = str(payload.get("source") or "").lower()
    if "open" in source or "entry" in source:
        return "open"
    if "close" in source or "exit" in source or "backfill" in source:
        return "close"
    return ""


def _target_quantity(entry: JsonDict, fills: list[_FillFact]) -> Decimal:
    for key in ("quantity", "matched_quantity", "target_quantity"):
        value = _decimal(entry.get(key))
        if value > 0:
            return value
    leg_qtys = [
        sum((fill.qty for fill in fills if fill.phase == phase and fill.leg == leg), Decimal("0"))
        for phase in ("open", "close")
        for leg in ("long", "short")
    ]
    return min(leg_qtys) if all(qty > 0 for qty in leg_qtys) else Decimal("0")


def _entry_explicit_zero_qty(entry: JsonDict) -> bool:
    for key in ("matched_quantity", "quantity"):
        if key in entry and _decimal(entry.get(key)) <= 0:
            return True
    return False


def _coverage_row(fills: list[_FillFact], target_qty: Decimal) -> JsonDict:
    filled_qty = sum((fill.qty for fill in fills), Decimal("0"))
    order_ids = _unique([fill.order_id for fill in fills if fill.order_id])
    client_order_ids = _unique([fill.client_order_id for fill in fills if fill.client_order_id])
    covered = bool(target_qty > 0 and filled_qty >= target_qty * QTY_TOLERANCE)
    avg_price = Decimal("0")
    if filled_qty > 0:
        avg_price = sum((fill.qty * fill.price for fill in fills), Decimal("0")) / filled_qty
    return {
        "filled_qty": _decimal_str(filled_qty),
        "target_qty": _decimal_str(target_qty),
        "covered": covered,
        "average_price": _decimal_str(avg_price),
        "fee_quote": _decimal_str(sum((fill.fee_quote for fill in fills), Decimal("0"))),
        "order_ids": order_ids,
        "client_order_ids": client_order_ids,
        "fill_event_ids": _unique([fill.fill_event_id for fill in fills if fill.fill_event_id]),
    }


def _project_record_status(
    facts: _PositionFacts,
    open_coverage: dict[str, JsonDict],
    close_coverage: dict[str, JsonDict],
    explicit_zero_entry: bool,
) -> str:
    if explicit_zero_entry and not facts.fills:
        return "phantom_zero_qty_project_opened_no_real_trade"
    open_complete = open_coverage["long"]["covered"] and open_coverage["short"]["covered"]
    close_complete = close_coverage["long"]["covered"] and close_coverage["short"]["covered"]
    if facts.exit_reconciled:
        latest = facts.exit_reconciled[-1]["payload"]
        if _exit_reconciled_complete(latest) and open_complete and close_complete:
            return "ok"
        return "exit_reconciled_pending_backfill_or_evidence_gap"
    if facts.exit_closed:
        if open_complete and close_complete:
            return "legacy_exit_closed_project_record_gap"
        return "legacy_exit_closed_missing_exchange_fill_evidence"
    return "missing_exit_reconciliation"


def _pnl_from_truth(
    entry: JsonDict,
    open_by_leg: dict[str, list[_FillFact]],
    close_by_leg: dict[str, list[_FillFact]],
    facts: _PositionFacts,
    classification: str,
    target_qty: Decimal,
    open_coverage: dict[str, JsonDict],
    close_coverage: dict[str, JsonDict],
) -> JsonDict:
    if classification != LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value:
        return _empty_pnl()
    long_entry = _decimal(open_coverage["long"].get("average_price"))
    short_entry = _decimal(open_coverage["short"].get("average_price"))
    long_close = _decimal(close_coverage["long"].get("average_price"))
    short_close = _decimal(close_coverage["short"].get("average_price"))
    price = Decimal("0")
    if long_entry > 0 and long_close > 0:
        price += (long_close - long_entry) * target_qty
    if short_entry > 0 and short_close > 0:
        price += (short_entry - short_close) * target_qty
    funding, funding_refs = _funding_truth(entry, facts)
    entry_fee = sum(
        (_fee_as_pnl(fill.fee_quote) for fill in open_by_leg["long"] + open_by_leg["short"]),
        Decimal("0"),
    )
    exit_fee = sum(
        (_fee_as_pnl(fill.fee_quote) for fill in close_by_leg["long"] + close_by_leg["short"]),
        Decimal("0"),
    )
    net = price + funding + entry_fee + exit_fee
    notional = Decimal("0")
    if target_qty > 0 and long_entry > 0 and short_entry > 0:
        notional = target_qty * ((long_entry + short_entry) / Decimal("2"))
    net_bps = (net / notional) * Decimal("10000") if notional > 0 else Decimal("0")
    return {
        "price_pnl_quote": _decimal_str(price),
        "funding_pnl_quote": _decimal_str(funding),
        "entry_fee_quote": _decimal_str(entry_fee),
        "exit_fee_quote": _decimal_str(exit_fee),
        "rebate_adjustment_quote": "0",
        "net_pnl_quote": _decimal_str(net),
        "notional_quote": _decimal_str(notional),
        "net_pnl_bps": _decimal_str(net_bps),
        "evidence_refs": _pnl_refs(open_by_leg, close_by_leg, funding_refs),
    }


def _empty_pnl() -> JsonDict:
    return {
        "price_pnl_quote": "0",
        "funding_pnl_quote": "0",
        "entry_fee_quote": "0",
        "exit_fee_quote": "0",
        "rebate_adjustment_quote": "0",
        "net_pnl_quote": "0",
        "notional_quote": "0",
        "net_pnl_bps": "0",
        "evidence_refs": [],
    }


def _funding_truth(entry: JsonDict, facts: _PositionFacts) -> tuple[Decimal, list[str]]:
    statement_total = Decimal("0")
    statement_refs: list[str] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for item in facts.funding_facts:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        amount = _decimal(
            payload.get("funding_pnl_quote")
            or payload.get("funding_quote")
            or payload.get("amount_quote")
            or payload.get("amount")
        )
        if amount == 0:
            continue
        venue = str(payload.get("venue") or payload.get("exchange") or "")
        statement_id = str(
            payload.get("statement_id")
            or payload.get("funding_id")
            or payload.get("bill_id")
            or payload.get("trade_id")
            or payload.get("order_id")
            or ""
        )
        ts_key = str(
            payload.get("funding_ts_ms")
            or payload.get("funding_time_ms")
            or payload.get("settled_at_ms")
            or item.get("ts_ms")
            or ""
        )
        key = (venue, statement_id, ts_key, _decimal_str(amount), str(payload.get("leg") or ""))
        if key in seen:
            continue
        seen.add(key)
        statement_total += amount
        if statement_id:
            statement_refs.append(f"funding:{venue}:statement_id:{statement_id}")
        else:
            statement_refs.append(f"funding:{venue}:ts_ms:{ts_key}:amount:{_decimal_str(amount)}")
    if statement_refs:
        return statement_total, _unique(statement_refs)
    for event in facts.exit_reconciled:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if _exit_reconciled_complete(payload):
            amount = _decimal(payload.get("funding_pnl_quote") or payload.get("funding"))
            if amount:
                return amount, [f"funding:exit.reconciled:{facts.position_id}"]
            break
    amount = _decimal(entry.get("captured_funding_quote") or entry.get("funding_pnl_quote"))
    if amount:
        return amount, [f"funding:entry.opened:{facts.position_id}"]
    return Decimal("0"), []


def _pnl_refs(
    open_by_leg: dict[str, list[_FillFact]],
    close_by_leg: dict[str, list[_FillFact]],
    funding_refs: list[str],
) -> list[str]:
    refs: list[str] = []
    for phase, by_leg in (("open", open_by_leg), ("close", close_by_leg)):
        for leg, fills in by_leg.items():
            for fill in fills:
                if fill.trade_id:
                    refs.append(f"{phase}:{leg}:{fill.venue}:trade_id:{fill.trade_id}")
                elif fill.exec_id:
                    refs.append(f"{phase}:{leg}:{fill.venue}:exec_id:{fill.exec_id}")
                elif fill.order_id:
                    refs.append(f"{phase}:{leg}:{fill.venue}:order_id:{fill.order_id}")
                elif fill.client_order_id:
                    refs.append(f"{phase}:{leg}:{fill.venue}:client_order_id:{fill.client_order_id}")
    refs.extend(funding_refs)
    return _unique(refs)


def _source_coverage_gaps(
    fills: list[_FillFact],
    target_qty: Decimal,
    open_coverage: dict[str, JsonDict],
    close_coverage: dict[str, JsonDict],
    classification: str,
) -> list[str]:
    gaps: list[str] = []
    if target_qty <= 0:
        gaps.append("missing_target_quantity")
    for phase, coverage in (("open", open_coverage), ("close", close_coverage)):
        for leg in ("long", "short"):
            if not coverage[leg]["covered"]:
                gaps.append(f"missing_{leg}_{phase}_exchange_fill_coverage")
    if classification == LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value:
        open_by_leg = {
            "long": [fill for fill in fills if fill.phase == "open" and fill.leg == "long"],
            "short": [fill for fill in fills if fill.phase == "open" and fill.leg == "short"],
        }
        close_by_leg = {
            "long": [fill for fill in fills if fill.phase == "close" and fill.leg == "long"],
            "short": [fill for fill in fills if fill.phase == "close" and fill.leg == "short"],
        }
        if not _pnl_refs(open_by_leg, close_by_leg, []):
            gaps.append("missing_pnl_evidence_refs")
    return sorted(set(gaps))


def _dedupe_fills(fills: list[_FillFact]) -> list[_FillFact]:
    out: list[_FillFact] = []
    seen: set[tuple[str, ...]] = set()
    for fill in fills:
        key = _fill_dedupe_key(fill)
        if key in seen:
            continue
        seen.add(key)
        out.append(fill)
    return out


def _fill_dedupe_key(fill: _FillFact) -> tuple[str, ...]:
    if fill.fill_event_id:
        return ("fill_event_id", fill.fill_event_id)
    if fill.trade_id:
        return ("trade_id", fill.phase, fill.leg, fill.venue, fill.trade_id)
    if fill.exec_id:
        return ("exec_id", fill.phase, fill.leg, fill.venue, fill.exec_id)
    identity = fill.order_id or fill.client_order_id
    if identity:
        return (
            "order_identity_exact",
            fill.phase,
            fill.leg,
            fill.venue,
            fill.order_id,
            fill.client_order_id,
            _decimal_str(fill.qty),
            _decimal_str(fill.price),
            _decimal_str(fill.fee_quote),
        )
    return (
        "weak_exact",
        fill.phase,
        fill.leg,
        fill.venue,
        _decimal_str(fill.qty),
        _decimal_str(fill.price),
        _decimal_str(fill.fee_quote),
        str(fill.filled_at_ms),
    )


def _fill_phase(payload: JsonDict) -> str:
    explicit = str(payload.get("phase") or "").lower()
    if explicit in {"open", "close"}:
        return explicit
    source = str(payload.get("source") or "").lower()
    trade_side = str(payload.get("tradeSide") or payload.get("trade_side") or "").lower()
    if (
        "close" in source
        or "backfill" in source
        or "exchange_trade_history" in source
        or trade_side == "close"
    ):
        return "close"
    if "open" in source or trade_side == "open":
        return "open"
    return ""


def _fill_leg(payload: JsonDict, entry: JsonDict) -> str:
    explicit = str(payload.get("leg") or "").lower()
    if explicit in {"long", "short"}:
        return explicit
    venue = str(payload.get("venue") or payload.get("exchange") or "").lower()
    long_venue = str(entry.get("long_venue") or entry.get("long_exchange") or "").lower()
    short_venue = str(entry.get("short_venue") or entry.get("short_exchange") or "").lower()
    if venue and venue == long_venue:
        return "long"
    if venue and venue == short_venue:
        return "short"
    side = str(payload.get("side") or "").lower()
    if side in {"sell", "ask"}:
        return "long"
    if side in {"buy", "bid"}:
        return "short"
    return ""


def _exit_reconciled_complete(payload: JsonDict) -> bool:
    return (
        str(payload.get("accounting_status") or "") == "complete"
        and not bool(payload.get("evidence_gap"))
        and not bool(payload.get("pending_backfill"))
    )


def _payload(event: JsonDict) -> JsonDict:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else event


def _position_id(payload: JsonDict) -> str:
    return str(payload.get("position_id") or payload.get("entry_id") or "").strip()


def _event_ts_ms(event: JsonDict) -> int:
    return _first_int(event.get("ts_ms"), event.get("timestamp_ms"), event.get("time_ms"))


def _first_int(*values: Any) -> int:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _first_payload_value(facts: _PositionFacts, key: str) -> Any:
    for fill in facts.fills:
        value = getattr(fill, key, "")
        if value:
            return value
    for event in facts.exit_reconciled + facts.exit_closed:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if payload.get(key):
            return payload.get(key)
    return ""


def _first_str(*values: Any) -> str:
    for value in values:
        if value is None or value == "":
            continue
        text = str(value)
        if text:
            return text
    return ""


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _fee_as_pnl(value: Any) -> Decimal:
    fee = _decimal(value)
    if fee > 0:
        return -fee
    return fee


def _decimal_str(value: Any) -> str:
    number = _decimal(value)
    if number == 0:
        return "0"
    text = format(number.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text
