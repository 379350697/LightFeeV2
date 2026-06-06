"""Pure V1-compatible recovery lifecycle decision core."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping


EPSILON = 1e-9

CORE_OWNED_BLOCK_REASONS = frozenset(
    {
        "exchange_truth_recovery_ledger_blocked",
        "truth_unavailable_for_required_recovery",
        "orphan_maker_order",
        "unpaired_live_position",
        "owned_recovery_work",
        "pending_residual_repair",
        "pending_passive_close",
        "owned_pending_entry",
    }
)


class RecoveryEvidenceClass(StrEnum):
    COMPLETE_FLAT = "complete_flat"
    COMPLETE_LIVE_ARTIFACT = "complete_live_artifact"
    OWNED_RECOVERY_WORK = "owned_recovery_work"
    ORPHAN_LIVE_ARTIFACT = "orphan_live_artifact"
    PARTIAL_EVIDENCE_GAP = "partial_evidence_gap"
    TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY = (
        "truth_unavailable_for_required_recovery"
    )
    OPERATOR_FAIL_CLOSED = "operator_fail_closed"


class RecoveryDecisionKind(StrEnum):
    RUNNING_CLEAN = "RUNNING_CLEAN"
    RUNNING_WITH_EVIDENCE_GAP = "RUNNING_WITH_EVIDENCE_GAP"
    RISK_ONLY_WAIT_FOR_TRUTH = "RISK_ONLY_WAIT_FOR_TRUTH"
    MANAGE_OWNED_RECOVERY_WORK = "MANAGE_OWNED_RECOVERY_WORK"
    BLOCK_OR_FLATTEN_LIVE_ARTIFACT = "BLOCK_OR_FLATTEN_LIVE_ARTIFACT"
    OPERATOR_FAIL_CLOSED_PRESERVED = "OPERATOR_FAIL_CLOSED_PRESERVED"


@dataclass(frozen=True)
class RecoveryEvidenceSnapshot:
    local_open_positions: tuple[Any, ...] = ()
    pending_entries: tuple[Any, ...] = ()
    residual_repairs: tuple[Any, ...] = ()
    passive_closes: tuple[Any, ...] = ()
    exchange_truth: Any | None = None
    prior_recovery_block_reason: str | None = None
    operator_fail_closed: bool = False
    operator_fail_closed_reason: str = ""
    recovery_work_items: tuple[Any, ...] = ()
    owner_evidence: tuple[Any, ...] = ()


@dataclass(frozen=True)
class RecoveryDecision:
    kind: RecoveryDecisionKind
    evidence_class: RecoveryEvidenceClass
    entry_allowed: bool
    block_reason: str | None = None
    clear_previous_block: bool = False
    clear_reason: str | None = None
    entry_block_reason: str | None = None
    recovery_work_items: tuple[Any, ...] = ()
    diagnostic_severity: str = "info"
    evidence_quality: str = "complete_flat"
    journal_event_name: str = "recovery.core.running_clean"
    maintenance_note: str = ""


class V1RecoveryDecisionCore:
    """Classify recovery evidence into one block/clear/entry decision."""

    def decide(self, snapshot: RecoveryEvidenceSnapshot) -> RecoveryDecision:
        if snapshot.operator_fail_closed:
            reason = (
                snapshot.operator_fail_closed_reason
                or snapshot.prior_recovery_block_reason
                or "operator_fail_closed"
            )
            return RecoveryDecision(
                kind=RecoveryDecisionKind.OPERATOR_FAIL_CLOSED_PRESERVED,
                evidence_class=RecoveryEvidenceClass.OPERATOR_FAIL_CLOSED,
                entry_allowed=False,
                block_reason=reason,
                entry_block_reason=reason,
                recovery_work_items=snapshot.recovery_work_items,
                diagnostic_severity="critical",
                evidence_quality=RecoveryEvidenceClass.OPERATOR_FAIL_CLOSED.value,
                journal_event_name="recovery.core.operator_fail_closed_preserved",
                maintenance_note="Operator fail-closed state is preserved by policy.",
            )

        live_artifact_reason = self._live_artifact_block_reason(snapshot)
        if live_artifact_reason is not None:
            return RecoveryDecision(
                kind=RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT,
                evidence_class=RecoveryEvidenceClass.ORPHAN_LIVE_ARTIFACT,
                entry_allowed=False,
                block_reason=live_artifact_reason,
                entry_block_reason=live_artifact_reason,
                recovery_work_items=snapshot.recovery_work_items,
                diagnostic_severity="critical",
                evidence_quality=RecoveryEvidenceClass.ORPHAN_LIVE_ARTIFACT.value,
                journal_event_name="recovery.core.live_artifact_blocked",
                maintenance_note=(
                    "Concrete exchange live artifacts fail safe until ownership "
                    "or flatten policy resolves them."
                ),
            )

        has_local_work = self._has_local_recovery_work(snapshot)
        truth_available = self._truth_available(snapshot.exchange_truth)
        if has_local_work and not truth_available:
            reason = RecoveryEvidenceClass.TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY.value
            return RecoveryDecision(
                kind=RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH,
                evidence_class=RecoveryEvidenceClass.TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY,
                entry_allowed=False,
                block_reason=reason,
                entry_block_reason=reason,
                recovery_work_items=snapshot.recovery_work_items,
                diagnostic_severity="warning",
                evidence_quality=reason,
                journal_event_name="recovery.core.truth_required_blocked",
                maintenance_note=(
                    "Existing recovery work requires exchange truth before "
                    "new entry risk can be admitted."
                ),
            )

        if has_local_work:
            return RecoveryDecision(
                kind=RecoveryDecisionKind.MANAGE_OWNED_RECOVERY_WORK,
                evidence_class=RecoveryEvidenceClass.OWNED_RECOVERY_WORK,
                entry_allowed=False,
                block_reason="owned_recovery_work",
                entry_block_reason="owned_recovery_work",
                recovery_work_items=snapshot.recovery_work_items,
                diagnostic_severity="warning",
                evidence_quality=RecoveryEvidenceClass.OWNED_RECOVERY_WORK.value,
                journal_event_name="recovery.core.owned_work_managed",
                maintenance_note=(
                    "Local recovery work is managed before conflicting new "
                    "entry risk is allowed."
                ),
            )

        if not truth_available or self._has_partial_evidence_gap(snapshot.exchange_truth):
            # A probe gap is not recovery work by itself. It becomes blocking only
            # when existing local work or a concrete live artifact requires truth
            # to proceed.
            return RecoveryDecision(
                kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
                evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
                entry_allowed=True,
                clear_previous_block=self._should_clear_core_block(snapshot),
                clear_reason="core_evidence_gap_no_local_work",
                recovery_work_items=(),
                diagnostic_severity="warning",
                evidence_quality=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP.value,
                journal_event_name="recovery.core.running_with_evidence_gap",
                maintenance_note=(
                    "Flat local state with only probe-quality gaps stays "
                    "runtime-available while diagnostics report incomplete evidence."
                ),
            )

        return RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_CLEAN,
            evidence_class=RecoveryEvidenceClass.COMPLETE_FLAT,
            entry_allowed=True,
            clear_previous_block=self._should_clear_core_block(snapshot),
            clear_reason="core_running_clean",
            recovery_work_items=(),
            diagnostic_severity="info",
            evidence_quality=RecoveryEvidenceClass.COMPLETE_FLAT.value,
            journal_event_name="recovery.core.running_clean",
            maintenance_note="Core evidence proves no recovery block is required.",
        )

    def _live_artifact_block_reason(
        self, snapshot: RecoveryEvidenceSnapshot
    ) -> str | None:
        local_open_positions = _as_items(snapshot.local_open_positions)
        recovery_work_items = _as_items(snapshot.recovery_work_items)
        for order in _exchange_open_orders(snapshot.exchange_truth):
            if _quantity(order) <= EPSILON or _bool(_get(order, "reduce_only", False)):
                continue
            if _order_has_owned_work(order, recovery_work_items):
                continue
            return "orphan_maker_order"
        for position in _exchange_positions(snapshot.exchange_truth):
            if _quantity(position) > EPSILON:
                if (
                    _position_matches_local_open(position, local_open_positions)
                    or _position_has_owned_work(position, recovery_work_items)
                ):
                    continue
                return "unpaired_live_position"
        return None

    def _has_local_recovery_work(self, snapshot: RecoveryEvidenceSnapshot) -> bool:
        return any(
            (
                _has_items(snapshot.pending_entries),
                _has_items(snapshot.residual_repairs),
                _has_items(snapshot.passive_closes),
                self._has_blocking_recovery_work_item(snapshot.recovery_work_items),
                _has_items(snapshot.owner_evidence),
            )
        )

    def _has_blocking_recovery_work_item(self, work_items: Any) -> bool:
        for item in _as_items(work_items):
            if bool(_get(item, "blocking", True)):
                return True
        return False

    def _truth_available(self, exchange_truth: Any | None) -> bool:
        if exchange_truth is None:
            return False
        if isinstance(exchange_truth, Mapping):
            if "truth_available" in exchange_truth:
                return bool(exchange_truth.get("truth_available"))
            if "available" in exchange_truth:
                return bool(exchange_truth.get("available"))
        return bool(_get(exchange_truth, "truth_available", _get(exchange_truth, "available", True)))

    def _has_partial_evidence_gap(self, exchange_truth: Any | None) -> bool:
        if exchange_truth is None:
            return True
        missing = _get(exchange_truth, "missing_evidence", ())
        errors = _get(exchange_truth, "errors", ())
        probe_evidence = _get(exchange_truth, "probe_evidence", ())
        if _has_items(missing) or _has_items(errors):
            return True
        return any(_probe_is_gap(item) for item in _as_items(probe_evidence))

    def _should_clear_core_block(self, snapshot: RecoveryEvidenceSnapshot) -> bool:
        reason = snapshot.prior_recovery_block_reason
        return reason is None or reason in CORE_OWNED_BLOCK_REASONS


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _as_items(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return tuple(value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return (value,)


def _has_items(value: Any) -> bool:
    return any(True for _ in _as_items(value))


def _exchange_positions(exchange_truth: Any | None) -> tuple[Any, ...]:
    return _flatten_exchange_collection(_get(exchange_truth, "positions", ()))


def _exchange_open_orders(exchange_truth: Any | None) -> tuple[Any, ...]:
    return _flatten_exchange_collection(_get(exchange_truth, "open_orders", ()))


def _flatten_exchange_collection(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        items: list[Any] = []
        for venue, venue_value in value.items():
            if isinstance(venue_value, Mapping):
                for symbol, symbol_value in venue_value.items():
                    for item in _as_items(symbol_value):
                        if isinstance(item, Mapping):
                            merged = dict(item)
                            merged.setdefault("venue", venue)
                            merged.setdefault("symbol", symbol)
                            items.append(merged)
                        else:
                            items.append(item)
            else:
                items.extend(_as_items(venue_value))
        return tuple(items)
    return _as_items(value)


def _quantity(obj: Any) -> float:
    for key in ("quantity", "qty", "size"):
        value = _get(obj, key, None)
        if value is not None:
            return _float(value)
    return 0.0


def _symbol(obj: Any) -> str:
    return str(_get(obj, "symbol", "") or "").upper()


def _venue(obj: Any) -> str:
    value = _get(obj, "venue", "")
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _venues(obj: Any) -> set[str]:
    values = (
        _get(obj, "venue", ""),
        _get(obj, "long_venue", ""),
        _get(obj, "short_venue", ""),
        _get(obj, "maker_venue", ""),
        _get(obj, "hedge_venue", ""),
    )
    result: set[str] = set()
    for value in values:
        if hasattr(value, "value"):
            value = value.value
        text = str(value or "").lower()
        if text:
            result.add(text)
    return result


def _position_matches_local_open(position: Any, local_open_positions: tuple[Any, ...]) -> bool:
    symbol = _symbol(position)
    venue = _venue(position)
    if not symbol or not venue:
        return False
    return any(
        _symbol(item) == symbol and venue in _venues(item)
        for item in local_open_positions
    )


def _position_has_owned_work(position: Any, work_items: tuple[Any, ...]) -> bool:
    symbol = _symbol(position)
    venue = _venue(position)
    for item in work_items:
        if not str(_get(item, "kind", "")).startswith("owned_"):
            continue
        if _symbol(item) != symbol:
            continue
        if venue and venue not in _venues(item):
            continue
        return True
    return False


def _order_has_owned_work(order: Any, work_items: tuple[Any, ...]) -> bool:
    order_ids = _order_ids(order)
    if not order_ids:
        return False
    for item in work_items:
        if not str(_get(item, "kind", "")).startswith("owned_"):
            continue
        for artifact in _as_items(_get(item, "artifacts", ())):
            if _order_ids(artifact) & order_ids:
                return True
    return False


def _order_ids(obj: Any) -> set[str]:
    return {
        str(value)
        for value in (
            _get(obj, "order_id", ""),
            _get(obj, "client_order_id", ""),
            _get(obj, "maker_order_id", ""),
            _get(obj, "maker_client_order_id", ""),
            _get(obj, "hedge_order_id", ""),
            _get(obj, "hedge_client_order_id", ""),
        )
        if value
    }


def _float(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if result != result:
        return 0.0
    return abs(result)


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _probe_is_gap(item: Any) -> bool:
    text = "{} {}".format(
        _get(item, "classification", ""),
        _get(item, "error", ""),
    ).lower()
    return any(
        token in text
        for token in (
            "timeout",
            "timed out",
            "unsupported",
            "partial",
            "unavailable",
            "missing",
            "error",
        )
    )
