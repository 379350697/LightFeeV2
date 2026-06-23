"""Shared live-trading business contract helpers.

The functions in this module are intentionally pure.  Runtime paths can keep
their existing control flow while sharing the same admission, quantity, terminal
truth, and diagnosis vocabulary.
"""

from __future__ import annotations

from typing import Any


DETERMINISTIC_ENTRY_ADMISSION_REASONS = frozenset({
    "bybit_trading_terms_required",
    "insufficient_balance_admission_blocked",
    "insufficient_margin_admission_blocked",
    "leverage_admission_blocked",
    "max_notional_admission_blocked",
    "route_abnormal_terminal_cooldown",
    "venue_auth_invalid",
})


def classify_business_event_kind(
    kind: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(kind or "")
    payload = payload or {}
    market_evidence = entry_market_evidence_contract(text, payload)
    if market_evidence:
        return {
            "phase": market_evidence["phase"],
            "terminality": market_evidence["terminality"],
            "action_taken": market_evidence["action_taken"],
            "action_evidence_kind": text,
            "diagnostic_severity": market_evidence["diagnostic_severity"],
            "owner_id": market_evidence["owner_id"],
            "evidence_class": market_evidence["evidence_class"],
            "blocks_entry": market_evidence["blocks_entry"],
        }
    close_reconciliation = close_reconciliation_evidence_contract(
        payload,
        current_exchange_truth_clean=passive_close_has_terminal_truth(payload),
    )
    if text == "exit.reconciled" and close_reconciliation:
        return {
            "phase": close_reconciliation["phase"],
            "terminality": close_reconciliation["terminality"],
            "action_taken": close_reconciliation["action"],
            "action_evidence_kind": text,
            "diagnostic_severity": close_reconciliation["diagnostic_severity"],
            "owner_id": close_reconciliation["owner_id"],
        }
    if text == "execution.dual_taker_armed":
        phase = (
            "PASSIVE_CLOSE"
            if str(payload.get("execution_kind") or "").lower() == "exit"
            else "PENDING_ENTRY"
        )
        return {
            "phase": phase,
            "terminality": "terminal_fallback_armed",
            "action_taken": "execute_terminal_taker_fallback",
            "action_evidence_kind": text,
            "diagnostic_severity": "info",
            "owner_id": str(
                payload.get("entry_id") or payload.get("position_id") or ""
            ),
        }
    if text in {
        "exit.passive_close_waiting_exchange_flat_truth",
        "exit.passive_close_live_one_sided_flatten",
        "exit.passive_close_live_one_sided_truth_gap",
        "exit.passive_close_open_order_ownerless_blocked",
        "exit.passive_close_live_one_sided_normalize_failed",
        "exit.passive_close_live_one_sided_error",
        "exit.passive_close_live_one_sided_force_close_problem",
    }:
        action = str(payload.get("passive_close_final_truth_action") or "")
        if not action and text == "exit.passive_close_waiting_exchange_flat_truth":
            contract = passive_close_final_truth_contract(
                payload.get("exchange_truth_attempt", {}),
                long_venue=payload.get("long_venue", ""),
                short_venue=payload.get("short_venue", ""),
            )
            action = str(contract.get("action") or "")
        if not action and text == "exit.passive_close_live_one_sided_flatten":
            action = "flatten_remaining_live_leg"
        return {
            "phase": "PASSIVE_CLOSE",
            "terminality": "active",
            "action_taken": action,
            "action_evidence_kind": text,
            "diagnostic_severity": (
                "critical"
                if action in {
                    "flatten_remaining_live_leg",
                    "adopt_or_block_existing_close_order",
                    "fail_closed_manual_block",
                }
                else "info"
            ),
            "owner_id": str(payload.get("position_id") or ""),
        }
    if text == "runtime.passive_close_recovery_result":
        return {
            "phase": "PASSIVE_CLOSE",
            "terminality": "terminal_truth_recorded",
            "action_taken": "record_passive_close_recovery_result",
            "action_evidence_kind": text,
            "diagnostic_severity": "info",
            "owner_id": str(payload.get("position_id") or ""),
        }
    if text == "exit.compensation_already_flat":
        return {
            "phase": "PASSIVE_CLOSE",
            "terminality": "terminal_flat_already_proven",
            "action_taken": "record_compensation_terminal_flat",
            "action_evidence_kind": text,
            "diagnostic_severity": "info",
            "owner_id": str(payload.get("position_id") or ""),
        }
    return {}


def entry_market_evidence_contract(
    kind: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    text = str(kind or "")
    owner_id = _venue_symbol_owner(payload)
    reason = str(
        payload.get("reason_bucket")
        or payload.get("reason")
        or payload.get("outcome")
        or payload.get("open_interest_evidence_reason")
        or ""
    )
    base = {
        "phase": "ENTRY_MARKET_EVIDENCE",
        "owner_id": owner_id,
        "reason": reason,
        "blocks_entry": False,
        "diagnostic_severity": "info",
        "terminality": "active",
        "action": "",
        "action_taken": "",
        "evidence_class": "",
    }
    if text in {
        "runtime.entry_quote_revalidate_failed",
        "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
        "runtime.quote_stale",
        "runtime.order_quote_stale_skipped",
    }:
        return {
            **base,
            "evidence_class": "quote",
            "action": "block_stale_quote",
            "action_taken": "block_stale_quote",
            "blocks_entry": True,
            "terminality": "terminal_candidate_block",
            "diagnostic_severity": "production_issue",
        }
    if text in {
        "runtime.entry_quote_revalidate_resolved",
        "runtime.entry_quote_evidence_resolved_by_ws_bbo",
        "runtime.last_good_revalidated_by_entry_quote_truth",
    }:
        action = (
            "diagnostic_recovered_overbudget"
            if _payload_over_budget(payload)
            else "allow_entry_evidence"
        )
        return {
            **base,
            "evidence_class": "quote",
            "action": action,
            "action_taken": action,
            "terminality": "terminal_evidence_resolved",
        }
    if text == "runtime.entry_quote_rewarm_scheduled_after_rest_stale":
        return {
            **base,
            "evidence_class": "quote",
            "action": "refresh_evidence",
            "action_taken": "schedule_quote_rewarm",
            "terminality": "active",
        }
    if text == "runtime.entry_quote_rewarm_terminal_stale":
        action_taken = str(
            payload.get("action_taken") or "skip_candidate_after_hard_rewarm"
        )
        return {
            **base,
            "evidence_class": "quote",
            "action": "terminal_candidate_rewarm",
            "action_taken": action_taken,
            "blocks_entry": True,
            "terminality": "terminal_candidate_block",
            "diagnostic_severity": "production_issue",
        }
    if text == "runtime.route_abnormal_cooldown_armed":
        return {
            **base,
            "evidence_class": "route",
            "action": "block_abnormal_route",
            "action_taken": "arm_route_abnormal_cooldown",
            "blocks_entry": True,
            "terminality": "terminal_candidate_block",
            "diagnostic_severity": "production_issue",
        }
    if text in {
        "execution.entry_liquidity_blocked",
        "runtime.perp_liquidity_stale_advisory",
    }:
        if text == "runtime.perp_liquidity_stale_advisory":
            return {
                **base,
                "evidence_class": "oi",
                "action": "refresh_evidence",
                "action_taken": "record_oi_liquidity_advisory",
                "terminality": "active",
            }
        action = _entry_oi_block_action(payload, reason)
        return {
            **base,
            "evidence_class": "oi",
            "action": action,
            "action_taken": action,
            "blocks_entry": True,
            "terminality": "terminal_candidate_block",
            "diagnostic_severity": "production_issue",
        }
    if text == "runtime.entry_oi_targeted_refresh_started":
        return {
            **base,
            "evidence_class": "oi",
            "action": "refresh_evidence",
            "action_taken": "targeted_oi_refresh",
            "terminality": "active",
        }
    if text == "runtime.entry_oi_targeted_refresh_resolved":
        action = (
            "diagnostic_recovered_overbudget"
            if _payload_over_budget(payload)
            else "allow_entry_evidence"
        )
        return {
            **base,
            "evidence_class": "oi",
            "action": action,
            "action_taken": action,
            "terminality": "terminal_evidence_resolved",
        }
    if text == "runtime.entry_oi_targeted_refresh_failed":
        return {
            **base,
            "evidence_class": "oi",
            "action": "block_oi_unavailable",
            "action_taken": "block_oi_unavailable",
            "blocks_entry": True,
            "terminality": "terminal_candidate_block",
            "diagnostic_severity": "production_issue",
        }
    return {}


def _entry_oi_block_action(payload: dict[str, Any], reason: str) -> str:
    evidence_status = str(
        payload.get("open_interest_evidence_status") or ""
    ).strip().lower()
    normalized_reason = str(reason or "").strip()
    if normalized_reason == "perp_open_interest_structural":
        return "block_oi_structural"
    if normalized_reason == "perp_open_interest_below_floor":
        return "block_oi_below_floor"
    try:
        current_value = float(
            payload.get("observed_open_interest_quote")
            if payload.get("observed_open_interest_quote") is not None
            else payload.get("current_value")
        )
        floor = float(
            payload.get("min_open_interest_quote")
            if payload.get("min_open_interest_quote") is not None
            else payload.get("floor")
        )
    except (TypeError, ValueError):
        current_value = 0.0
        floor = 0.0
    if evidence_status == "available" and floor > 0.0 and current_value < floor:
        return "block_oi_below_floor"
    return "block_oi_unavailable"


def close_reconciliation_evidence_contract(
    payload: dict[str, Any] | None,
    current_exchange_truth_clean: bool,
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    if payload.get("evidence_gap") is not True:
        return {}
    effective_exchange_truth_clean = bool(
        current_exchange_truth_clean
        or close_reconciliation_exchange_truth_clean(payload)
    )
    state_contract = classify_close_reconciliation_state(
        payload,
        current_exchange_truth_clean=effective_exchange_truth_clean,
    )
    clean = str(state_contract.get("state") or "") == "terminal_flat_accounting_gap"
    action = (
        "terminal_flat_accounting_gap"
        if clean
        else "unresolved_close_accounting_gap"
    )
    return {
        "phase": "CLOSE_RECONCILIATION",
        "terminality": action,
        "action": action,
        "action_taken": action,
        "blocks_business_terminal": bool(state_contract.get("blocks_entry") is True),
        "diagnostic_severity": "info" if clean else "critical",
        "owner_id": str(payload.get("position_id") or payload.get("entry_id") or ""),
        "symbol": str(payload.get("symbol") or ""),
        "reason": str(payload.get("evidence_gap_reason") or "unknown"),
        "statement_probe_status": str(payload.get("statement_probe_status") or ""),
    }


def classify_close_reconciliation_state(
    reconciliation: dict[str, Any] | None,
    *,
    current_exchange_truth_clean: bool,
) -> dict[str, Any]:
    """Classify pending-close reconciliation work with the shared risk contract."""
    item = reconciliation if isinstance(reconciliation, dict) else {}
    marker = str(
        item.get("close_reconciliation_state")
        or item.get("business_contract_action")
        or item.get("terminality")
        or item.get("action")
        or ""
    )
    explicit_gap_reason = str(
        item.get("last_evidence_gap_reason")
        or item.get("evidence_gap_reason")
        or ""
    )
    is_accepted_order_truth_gap = (
        item.get("accepted_order_truth_gap") is True
        or str(item.get("kind") or "") == "accepted_order_truth_gap"
        or str(item.get("truth_required_by") or "") == "accepted_order_truth_gap"
    )
    reason = str(
        explicit_gap_reason
        or ("accepted_order_truth_gap" if is_accepted_order_truth_gap else "")
        or item.get("reason")
        or ""
    )
    base = {
        "state": "active_close_work",
        "blocks_entry": True,
        "archive_reconciliation": False,
        "reason": reason,
        "owner_id": str(item.get("position_id") or item.get("entry_id") or ""),
        "symbol": str(item.get("symbol") or "").upper(),
    }
    has_accounting_gap = (
        bool(item.get("pending_backfill"))
        or item.get("evidence_gap") is True
        or is_accepted_order_truth_gap
    )

    if marker == "catalog_diagnostic":
        return {
            **base,
            "state": "catalog_diagnostic",
            "blocks_entry": False,
            "archive_reconciliation": True,
            "reason": reason or "catalog_diagnostic",
        }

    if has_accounting_gap and bool(current_exchange_truth_clean):
        return {
            **base,
            "state": "terminal_flat_accounting_gap",
            "blocks_entry": False,
            "archive_reconciliation": True,
            "reason": reason or "terminal_flat_accounting_gap",
        }

    if has_accounting_gap and not current_exchange_truth_clean:
        return {
            **base,
            "state": "truth_unavailable",
            "blocks_entry": True,
            "archive_reconciliation": False,
            "reason": reason or "exchange_truth_not_clean",
        }

    if marker == "terminal_flat_accounting_gap":
        return {
            **base,
            "state": "truth_unavailable",
            "blocks_entry": True,
            "archive_reconciliation": False,
            "reason": reason or "exchange_truth_not_clean",
        }

    return base


def _close_reconciliation_scope(
    reconciliation: dict[str, Any],
) -> tuple[str, set[str]]:
    snapshot = reconciliation.get("position_snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    symbol = str(reconciliation.get("symbol") or snapshot.get("symbol") or "").upper()
    venues = {
        str(value or "").lower()
        for value in (
            reconciliation.get("long_venue") or snapshot.get("long_venue"),
            reconciliation.get("short_venue") or snapshot.get("short_venue"),
        )
        if str(value or "")
    }
    return symbol, venues


def _truth_records_cover_scope(
    records: Any,
    *,
    symbol: str,
    venues: set[str],
    quantity_key: str | None = None,
    open_orders_key: str | None = None,
) -> bool:
    if not isinstance(records, list) or not records:
        return True
    scoped: list[dict[str, Any]] = []
    for raw in records:
        if not isinstance(raw, dict):
            continue
        record_symbol = str(raw.get("symbol") or "").upper()
        record_venue = str(raw.get("venue") or "").lower()
        if symbol and record_symbol and record_symbol != symbol:
            continue
        if venues and record_venue and record_venue not in venues:
            continue
        scoped.append(raw)
    if not scoped:
        return False
    scoped_venues = {
        str(item.get("venue") or "").lower()
        for item in scoped
        if str(item.get("venue") or "")
    }
    if venues and scoped_venues and not venues.issubset(scoped_venues):
        return False
    if quantity_key is not None:
        return all(
            abs(_safe_float(item.get(quantity_key))) <= 1e-9
            for item in scoped
        )
    if open_orders_key is not None:
        return all(item.get(open_orders_key) is True for item in scoped)
    return True


def _truth_venue_probe_succeeded(
    truth: dict[str, Any],
    *,
    venue: str,
    symbol: str,
    status_key: str,
    evidence_key: str,
    success_classification: str,
) -> bool:
    fetch_status = truth.get("fetch_status")
    if isinstance(fetch_status, dict):
        venue_status = fetch_status.get(venue)
        if isinstance(venue_status, dict):
            failed_key = (
                "positions_failed"
                if status_key == "positions_succeeded"
                else "orders_failed"
            )
            failed = {
                str(item).upper()
                for item in venue_status.get(failed_key, []) or []
                if str(item)
            }
            if "*" in failed or (symbol and symbol in failed):
                return False
            succeeded = {
                str(item).upper()
                for item in venue_status.get(status_key, []) or []
                if str(item)
            }
            if "*" in succeeded or (symbol and symbol in succeeded):
                return True

    evidence = truth.get(evidence_key)
    if isinstance(evidence, dict):
        venue_evidence = evidence.get(venue)
        if isinstance(venue_evidence, dict):
            for key in ("*", symbol):
                if not key:
                    continue
                item = venue_evidence.get(key)
                if not isinstance(item, dict):
                    continue
                if str(item.get("classification") or "") == success_classification:
                    return True
    return False


def _truth_probe_evidence_succeeded(
    truth: dict[str, Any],
    *,
    venue: str,
    symbol: str,
    endpoint: str | set[str],
    success_classifications: set[str],
) -> bool:
    evidence = truth.get("probe_evidence")
    if not isinstance(evidence, list):
        return False
    endpoints = {endpoint} if isinstance(endpoint, str) else set(endpoint)
    venue_l = str(venue or "").lower()
    symbol_u = str(symbol or "").upper()
    for raw in evidence:
        if not isinstance(raw, dict):
            continue
        raw_venue = str(raw.get("venue") or "").lower()
        if raw_venue and raw_venue != venue_l:
            continue
        raw_symbol = str(raw.get("symbol") or "").upper()
        if symbol_u and raw_symbol and raw_symbol not in {symbol_u, "*"}:
            continue
        raw_endpoints = {
            str(raw.get("endpoint") or ""),
            str(raw.get("method") or ""),
        }
        raw_endpoints.discard("")
        if endpoints and raw_endpoints.isdisjoint(endpoints):
            continue
        if str(raw.get("classification") or "") in success_classifications:
            return True
    return False


def _truth_probe_evidence_has_error_for_scope(
    truth: dict[str, Any],
    *,
    venues: set[str],
) -> bool:
    if not venues:
        return False
    evidence = truth.get("probe_evidence")
    if isinstance(evidence, list):
        for raw in evidence:
            if not isinstance(raw, dict):
                continue
            raw_venue = str(raw.get("venue") or "").lower()
            if raw_venue not in venues:
                continue
            classification = str(raw.get("classification") or "")
            if raw.get("error") or classification.endswith(("_failed", "_error")):
                return True
    errors = truth.get("errors")
    if isinstance(errors, list):
        for raw_error in errors:
            venue = str(raw_error or "").split(":", 1)[0].lower()
            if venue in venues:
                return True
    return False


def _recovery_ledger_flat_truth_covers_scope(
    truth: dict[str, Any],
    *,
    symbol: str,
    venues: set[str],
) -> bool:
    if truth.get("truth_supported") is False:
        return False
    if truth.get("available") is False:
        return False
    if not venues:
        return False
    positions = truth.get("positions")
    open_orders = truth.get("open_orders")
    if not isinstance(positions, list) or not isinstance(open_orders, list):
        return False
    if _truth_probe_evidence_has_error_for_scope(truth, venues=venues):
        return False

    symbol_u = str(symbol or "").upper()
    for venue in venues:
        if not _truth_probe_evidence_succeeded(
            truth,
            venue=venue,
            symbol=symbol_u,
            endpoint={"fetch_position", "fetch_all_positions"},
            success_classifications={
                "position_truth",
                "position_probe_unfiltered_succeeded",
            },
        ):
            return False
        if not _truth_probe_evidence_succeeded(
            truth,
            venue=venue,
            symbol=symbol_u,
            endpoint="fetch_open_orders",
            success_classifications={
                "open_order_truth",
                "open_order_probe_unfiltered_succeeded",
            },
        ):
            return False

    for raw in positions:
        if not isinstance(raw, dict):
            continue
        record_symbol = str(raw.get("symbol") or "").upper()
        record_venue = str(raw.get("venue") or "").lower()
        if symbol_u and record_symbol and record_symbol != symbol_u:
            continue
        if venues and record_venue and record_venue not in venues:
            continue
        if abs(_safe_float(raw.get("quantity"))) > 1e-9:
            return False

    for raw in open_orders:
        if not isinstance(raw, dict):
            continue
        record_symbol = str(raw.get("symbol") or "").upper()
        record_venue = str(raw.get("venue") or "").lower()
        if symbol_u and record_symbol and record_symbol != symbol_u:
            continue
        if venues and record_venue and record_venue not in venues:
            continue
        return False
    return True


def _account_level_flat_truth_covers_scope(
    truth: dict[str, Any],
    *,
    symbol: str,
    venues: set[str],
) -> bool:
    if truth.get("truth_available") is False or truth.get("available") is False:
        return False
    confidence = truth.get("confidence")
    if confidence is not None and str(confidence or "").lower() != "high":
        return False
    if truth.get("has_nonzero_position") is not False:
        return False
    if truth.get("has_open_order") is not False:
        return False
    if not venues:
        return False
    for venue in venues:
        if not _truth_venue_probe_succeeded(
            truth,
            venue=venue,
            symbol=symbol,
            status_key="positions_succeeded",
            evidence_key="position_probe_evidence",
            success_classification="position_probe_unfiltered_succeeded",
        ):
            return False
        if not _truth_venue_probe_succeeded(
            truth,
            venue=venue,
            symbol=symbol,
            status_key="orders_succeeded",
            evidence_key="open_order_probe_evidence",
            success_classification="open_order_probe_unfiltered_succeeded",
        ):
            return False
    return True


def _close_reconciliation_truth_covers_scope(
    truth: dict[str, Any],
    reconciliation: dict[str, Any],
) -> bool:
    symbol, venues = _close_reconciliation_scope(reconciliation)
    if passive_close_has_terminal_truth({"exchange_truth": truth}):
        positions = truth.get("positions")
        if not _truth_records_cover_scope(
            positions,
            symbol=symbol,
            venues=venues,
            quantity_key="quantity",
        ):
            return False
        open_order_truth = truth.get("open_order_truth")
        if not _truth_records_cover_scope(
            open_order_truth,
            symbol=symbol,
            venues=venues,
            open_orders_key="open_orders_empty",
        ):
            return False
        return True
    if _recovery_ledger_flat_truth_covers_scope(
        truth,
        symbol=symbol,
        venues=venues,
    ):
        return True
    return _account_level_flat_truth_covers_scope(
        truth,
        symbol=symbol,
        venues=venues,
    )


def close_reconciliation_exchange_truth(
    reconciliation: dict[str, Any] | None,
    *,
    current_exchange_truth: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    item = reconciliation if isinstance(reconciliation, dict) else {}
    original_payload = item.get("original_payload")
    if not isinstance(original_payload, dict):
        original_payload = {}
    embedded_truth = item.get("exchange_truth") or original_payload.get(
        "exchange_truth"
    )
    if isinstance(embedded_truth, dict) and _close_reconciliation_truth_covers_scope(
        embedded_truth,
        item,
    ):
        return embedded_truth

    if not isinstance(current_exchange_truth, dict):
        return None
    if not _close_reconciliation_truth_covers_scope(current_exchange_truth, item):
        return None
    return current_exchange_truth


def close_reconciliation_exchange_truth_clean(
    reconciliation: dict[str, Any] | None,
    *,
    current_exchange_truth: dict[str, Any] | None = None,
) -> bool:
    return close_reconciliation_exchange_truth(
        reconciliation,
        current_exchange_truth=current_exchange_truth,
    ) is not None


def quote_rewarm_handoff_contract(
    *,
    phase: str,
    status: str,
    configured_action: str,
    terminal_kind: str = "",
) -> dict[str, str]:
    if str(phase or "") != "quote_rewarm":
        return {}
    action = str(configured_action or "")
    if terminal_kind:
        return {
            "action_taken": action,
            "action_evidence_kind": str(terminal_kind),
            "diagnostic_severity": "info",
        }
    if str(status or "") == "hard_over_budget":
        return {
            "action_taken": action,
            "action_evidence_kind": "business_contract.quote_rewarm_hard_timeout",
            "diagnostic_severity": "production_issue",
        }
    return {}


def close_order_error_resolution_contract(
    *,
    kind: str,
    payload: dict[str, Any],
    current_exchange_truth_clean: bool,
    position_terminal_match: bool,
    order_terminal_match: bool,
    has_order_identity: bool,
    is_post_only_close_reject: bool | None = None,
) -> dict[str, Any]:
    if not current_exchange_truth_clean:
        return {"resolved": False, "resolution_bucket": ""}
    post_only = (
        bool(is_post_only_close_reject)
        if is_post_only_close_reject is not None
        else _payload_is_post_only_close_reject(payload)
    )
    reduce_only = _payload_is_reduce_only_terminal_flat_reject(payload)
    zero_fill = (
        str(kind or "") == "order.uncertain"
        and "zero fill" in _payload_reason_text(payload)
    )
    if post_only:
        return {
            "resolved": bool(position_terminal_match),
            "resolution_bucket": "post_only_boundary_reject",
        }
    if reduce_only or zero_fill:
        resolved = bool(
            order_terminal_match
            or (not has_order_identity and position_terminal_match)
        )
        return {
            "resolved": resolved,
            "resolution_bucket": (
                "reduce_only_terminal_flat"
                if reduce_only
                else "zero_fill_terminal_flat"
            ),
        }
    return {"resolved": False, "resolution_bucket": ""}


def classify_noise_visibility(
    kind: str,
    payload: dict[str, Any] | None = None,
    *,
    current_exchange_truth_clean: bool,
) -> dict[str, Any]:
    """Classify diagnostic evidence by whether it is current risk or history."""
    payload = payload if isinstance(payload, dict) else {}
    text = str(kind or "")
    base = {
        "visibility": "aggregated_diagnostic",
        "blocks_gate": False,
        "requires_operator_action": False,
        "reason": "",
        "kind": text,
        "owner_id": str(payload.get("position_id") or payload.get("entry_id") or ""),
        "symbol": str(payload.get("symbol") or "").upper(),
    }

    if (
        text
        in {
            "recovery.unpaired_live_position_cleanup_skipped",
            "recovery.unpaired_live_position_cleanup_failed",
        }
        and payload.get("current_risk_exposure") is True
    ):
        if current_exchange_truth_clean:
            return {
                **base,
                "visibility": "historical_terminal_evidence",
                "blocks_gate": False,
                "requires_operator_action": False,
                "reason": "terminal_flat_recovered_unpaired_cleanup",
            }
        return {
            **base,
            "visibility": "current_blocker",
            "blocks_gate": True,
            "requires_operator_action": True,
            "reason": "current_single_leg_or_risk_only_exposure",
        }

    if text == "recovery.live_position_probe_unsupported_symbols":
        return {
            **base,
            "visibility": "catalog_diagnostic",
            "blocks_gate": False,
            "requires_operator_action": False,
            "reason": "unsupported_symbol_probe_filtered",
            "scope": "exchange_truth_catalog_filter",
        }

    market_evidence = entry_market_evidence_contract(text, payload)
    if market_evidence and market_evidence.get("blocks_entry") is True:
        return {
            **base,
            "visibility": "current_admission_blocker",
            "blocks_gate": False,
            "requires_operator_action": False,
            "reason": "entry_market_evidence_block",
            "scope": "entry_candidate_admission",
        }

    reconciliation = close_reconciliation_evidence_contract(
        payload,
        current_exchange_truth_clean=current_exchange_truth_clean,
    )
    if text == "exit.reconciled" and reconciliation:
        if reconciliation.get("blocks_business_terminal") is True:
            return {
                **base,
                "visibility": "current_blocker",
                "blocks_gate": True,
                "requires_operator_action": True,
                "reason": "unresolved_close_accounting_gap",
            }
        return {
            **base,
            "visibility": "historical_terminal_evidence",
            "reason": "terminal_flat_accounting_gap",
        }

    is_close_artifact = (
        text in {"order.rejected", "order.uncertain"}
        or text.startswith("exit.passive_close_")
    )
    code = str(
        payload.get("exchange_code")
        or _exchange_error_dict(payload).get("exchange_code")
        or payload.get("code")
        or ""
    )
    reason_text = _payload_reason_text(payload)
    known_close_noise = (
        _payload_is_post_only_close_reject(payload)
        or _payload_is_reduce_only_terminal_flat_reject(payload)
        or ("zero fill" in reason_text)
        or code == "110072"
        or "duplicate" in reason_text
        or "ack-only" in reason_text
        or "ack only" in reason_text
    )
    if is_close_artifact and known_close_noise:
        if current_exchange_truth_clean:
            return {
                **base,
                "visibility": "historical_terminal_evidence",
                "reason": "resolved_close_artifact_after_terminal_truth",
            }
        return {
            **base,
            "visibility": "current_blocker",
            "blocks_gate": True,
            "requires_operator_action": True,
            "reason": "unresolved_close_artifact",
        }

    return base


def entry_admission_blocks_candidate(reason: str, block_scope: str) -> bool:
    if str(block_scope or "").lower() == "venue":
        return True
    return str(reason or "") in DETERMINISTIC_ENTRY_ADMISSION_REASONS


def entry_admission_aggregation_key(
    *,
    stage: str,
    venue: str,
    symbol: str,
    reason: str,
    block_scope: str,
) -> str:
    return ":".join([
        str(stage or "unknown"),
        str(venue or "multiple").lower(),
        str(symbol or "*").upper(),
        str(reason or "unknown"),
        str(block_scope or "symbol").lower(),
    ])


def entry_route_key(symbol: str, long_venue: str, short_venue: str) -> str:
    return ":".join([
        "route",
        str(symbol or "").upper(),
        f"{str(long_venue or '').lower()}->{str(short_venue or '').lower()}",
    ])


def classify_entry_quantity_contract(
    *,
    raw_quantity: float,
    common_quantity: float,
    effective_quantity: float,
    epsilon: float = 1e-9,
) -> dict[str, Any]:
    raw = _safe_float(raw_quantity)
    common = _safe_float(common_quantity)
    effective = _safe_float(effective_quantity)
    residual = max(raw - common, 0.0)
    if effective <= epsilon or common <= epsilon:
        status = "blocked_unhedgeable_quantity"
    elif residual > epsilon:
        status = "hedgeable_adjusted"
    else:
        status = "hedgeable"
    return {
        "quantity_contract_status": status,
        "unhedgeable_residual_quantity": residual,
    }


def passive_close_final_truth_contract(
    exchange_truth_attempt: dict[str, Any] | None,
    *,
    long_venue: Any,
    short_venue: Any,
    epsilon: float = 1e-9,
) -> dict[str, Any]:
    """Classify the final passive-close exchange truth into one business action."""
    truth = exchange_truth_attempt if isinstance(exchange_truth_attempt, dict) else {}
    long_key = _normalize_venue_text(long_venue)
    short_key = _normalize_venue_text(short_venue)
    base = {
        "terminal": False,
        "phase": "PASSIVE_CLOSE",
        "diagnostic_severity": "critical",
    }

    if truth.get("truth_available") is False or truth.get("position_errors"):
        return {
            **base,
            "action": "retain_untrusted_truth",
            "next_action": "retain_untrusted_truth",
            "reason": "position_truth_untrusted",
        }

    positions = _position_truth_by_venue(truth.get("positions"))
    long_qty = _quantity_for_venue(positions, long_key)
    short_qty = _quantity_for_venue(positions, short_key)
    positions_flat = truth.get("positions_flat")
    if not isinstance(positions_flat, bool):
        if long_qty is None or short_qty is None:
            positions_flat = None
        else:
            positions_flat = abs(long_qty) <= epsilon and abs(short_qty) <= epsilon

    open_orders_flat = truth.get("open_orders_flat")
    if not isinstance(open_orders_flat, bool):
        open_orders_flat = _open_orders_flat_from_truth(truth.get("open_order_truth"))

    if open_orders_flat is None:
        return {
            **base,
            "action": "retain_untrusted_truth",
            "next_action": "retain_untrusted_truth",
            "reason": "open_order_truth_untrusted",
        }

    if positions_flat is True and open_orders_flat is True:
        return {
            **base,
            "action": "clear_flat",
            "next_action": "clear_flat",
            "terminal": True,
            "diagnostic_severity": "info",
            "reason": "exchange_flat_no_open_orders",
        }

    if long_qty is None or short_qty is None:
        return {
            **base,
            "action": "retain_untrusted_truth",
            "next_action": "retain_untrusted_truth",
            "reason": "position_truth_incomplete",
        }

    long_nonzero = abs(long_qty) > epsilon
    short_nonzero = abs(short_qty) > epsilon
    if long_nonzero == short_nonzero:
        return {
            **base,
            "action": "fail_closed_manual_block",
            "next_action": "manual_reconcile_multi_leg_exposure",
            "reason": "not_single_live_leg",
            "long_quantity": abs(long_qty),
            "short_quantity": abs(short_qty),
        }

    leg_label = "long" if long_nonzero else "short"
    venue = long_key if long_nonzero else short_key
    quantity = abs(long_qty if long_nonzero else short_qty)
    if open_orders_flat is True:
        return {
            **base,
            "action": "flatten_remaining_live_leg",
            "next_action": "flatten_remaining_live_leg",
            "reason": "trusted_one_sided_live_residual",
            "leg_label": leg_label,
            "venue": venue,
            "quantity": quantity,
        }

    return {
        **base,
        "action": "adopt_or_block_existing_close_order",
        "next_action": "adopt_existing_reduce_only_close_order",
        "reason": "open_orders_present_for_one_sided_residual",
        "leg_label": leg_label,
        "venue": venue,
        "quantity": quantity,
    }


def close_reconciliation_evidence_fields(
    *,
    long_quantity: float,
    short_quantity: float,
    duplicate_close_leg_suppressed_count: int = 0,
    epsilon: float = 1e-12,
) -> dict[str, Any]:
    long_found = _safe_float(long_quantity) > epsilon
    short_found = _safe_float(short_quantity) > epsilon
    complete = long_found and short_found
    trade_probe_status = {
        "long": "found" if long_found else "missing",
        "short": "found" if short_found else "missing",
    }
    if complete:
        reason = ""
        statement_status = "complete"
    elif long_found:
        reason = "missing_short_close_trade_statement"
        statement_status = "partial"
    elif short_found:
        reason = "missing_long_close_trade_statement"
        statement_status = "partial"
    else:
        reason = "missing_both_close_trade_statements"
        statement_status = "missing"
    if not complete and duplicate_close_leg_suppressed_count > 0:
        reason = f"{reason}_after_duplicate_leg_suppression"
    return {
        "evidence_gap_reason": reason,
        "statement_probe_status": statement_status,
        "trade_probe_status": trade_probe_status,
    }


def passive_close_has_terminal_truth(payload: dict[str, Any]) -> bool:
    truth = payload.get("exchange_truth")
    if not isinstance(truth, dict):
        return False
    if truth.get("truth_available") is False:
        return False
    positions_flat = truth.get("positions_flat")
    if not isinstance(positions_flat, bool):
        positions = truth.get("positions")
        if isinstance(positions, list):
            position_items = [item for item in positions if isinstance(item, dict)]
            positions_flat = bool(position_items) and all(
                abs(_safe_float((item or {}).get("quantity"))) <= 1e-9
                for item in position_items
            )
        else:
            positions_flat = False
    open_orders_flat = truth.get("open_orders_flat")
    if not isinstance(open_orders_flat, bool):
        open_order_truth = truth.get("open_order_truth")
        if isinstance(open_order_truth, list):
            open_order_items = [
                item for item in open_order_truth if isinstance(item, dict)
            ]
            open_orders_flat = bool(open_order_items) and all(
                bool((item or {}).get("open_orders_empty"))
                for item in open_order_items
            )
        else:
            open_orders_flat = False
    return bool(positions_flat) and bool(open_orders_flat)


def diagnose_issue_counts(payload: dict[str, Any], kind: str) -> dict[str, int]:
    if kind == "execution.entry_quantity_plan":
        status = str(payload.get("quantity_contract_status") or "")
        blocked = int(status.startswith("blocked_"))
        return {"entry_quantity_contract_blocked_count": blocked}
    if kind == "exit.reconciled":
        gap = payload.get("evidence_gap") is True
        return {"close_reconciliation_evidence_gap_count": int(gap)}
    if kind == "runtime.entry_admission_venue_degraded":
        return {
            "admission_degraded_suppressed_count": _safe_int(
                payload.get("suppressed_count")
            )
        }
    return {}


def _payload_is_reduce_only_terminal_flat_reject(payload: dict[str, Any]) -> bool:
    request_context = _payload_request_context(payload)
    if not _boolish(request_context.get("reduce_only")):
        return False
    exchange_error = _exchange_error_dict(payload)
    code = str(
        payload.get("exchange_code")
        or exchange_error.get("exchange_code")
        or payload.get("code")
        or exchange_error.get("code")
        or ""
    ).strip()
    reason = _payload_reason_text(payload)
    return (
        code == "-2022"
        or "reduceonly order is rejected" in reason
        or "reduce only order is rejected" in reason
        or "reduce-only order is rejected" in reason
    )


def _payload_is_post_only_close_reject(payload: dict[str, Any]) -> bool:
    request_context = _payload_request_context(payload)
    reason = _payload_reason_text(payload)
    return (
        ("post only" in reason or "post-only" in reason)
        and _boolish(request_context.get("post_only"))
        and _boolish(request_context.get("reduce_only"))
    )


def _payload_reason_text(payload: dict[str, Any]) -> str:
    exchange_error = _exchange_error_dict(payload)
    return str(
        payload.get("reason")
        or payload.get("error")
        or payload.get("exchange_msg")
        or payload.get("msg")
        or exchange_error.get("exchange_msg")
        or exchange_error.get("raw_body")
        or exchange_error.get("msg")
        or ""
    ).lower()


def _venue_symbol_owner(payload: dict[str, Any]) -> str:
    venue = str(payload.get("venue") or "").lower()
    symbol = str(payload.get("symbol") or "").upper()
    return f"{venue}:{symbol}" if venue and symbol else ""


def _normalize_venue_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").lower()


def _position_truth_by_venue(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        venue = _normalize_venue_text(item.get("venue"))
        if venue:
            result[venue] = item
    return result


def _quantity_for_venue(
    positions: dict[str, dict[str, Any]],
    venue: str,
) -> float | None:
    item = positions.get(venue)
    if not isinstance(item, dict):
        return None
    if item.get("error"):
        return None
    quantity = item.get("quantity")
    try:
        return float(quantity)
    except (TypeError, ValueError):
        return None


def _open_orders_flat_from_truth(value: Any) -> bool | None:
    if not isinstance(value, list):
        return None
    items = [item for item in value if isinstance(item, dict)]
    if not items:
        return None
    for item in items:
        if item.get("error"):
            return None
        if item.get("open_orders_empty") is not True:
            return False
    return True


def _exchange_error_dict(payload: dict[str, Any]) -> dict[str, Any]:
    exchange_error = payload.get("exchange_error")
    return exchange_error if isinstance(exchange_error, dict) else {}


def _payload_request_context(payload: dict[str, Any]) -> dict[str, Any]:
    request_context = payload.get("request_context")
    if isinstance(request_context, dict):
        return request_context
    exchange_error = _exchange_error_dict(payload)
    request_context = exchange_error.get("request_context")
    return request_context if isinstance(request_context, dict) else {}


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _payload_over_budget(payload: dict[str, Any]) -> bool:
    for key in ("over_budget", "recovered_overbudget", "recovered_over_budget"):
        if _boolish(payload.get(key)):
            return True
    elapsed = _safe_float(payload.get("elapsed_ms"))
    budget = _safe_float(
        payload.get("budget_ms")
        or payload.get("hard_ms")
        or payload.get("hard_budget_ms")
    )
    return bool(elapsed and budget and elapsed > budget)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0
