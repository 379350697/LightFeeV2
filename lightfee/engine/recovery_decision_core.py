"""Pure V1-compatible recovery lifecycle decision core."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from typing import Any, Mapping


EPSILON = 1e-9
STALE_RISK_ONLY_ACCOUNT_TRUTH_BLOCK_REASON = (
    "stale_risk_only_requires_account_truth"
)

CORE_OWNED_BLOCK_REASONS = frozenset(
    {
        "exchange_truth_recovery_ledger_blocked",
        "truth_unavailable_for_required_recovery",
        "orphan_maker_order",
        "orphan_reduce_only_order",
        "unpaired_live_position",
        "owned_pending_entry_live_conflict",
        STALE_RISK_ONLY_ACCOUNT_TRUTH_BLOCK_REASON,
        "owned_recovery_work",
        "pending_residual_repair",
        "pending_passive_close",
        "owned_pending_entry",
    }
)

LIVE_ARTIFACT_BLOCK_REASONS = frozenset(
    {
        "orphan_maker_order",
        "orphan_reduce_only_order",
        "unpaired_live_position",
        "owned_pending_entry_live_conflict",
    }
)

# A persisted RISK_ONLY state without an owner is ambiguous at restart: it may
# be a historical recovery latch rather than a current health-line decision.
# Treat it like a live artifact for release purposes until account-wide position
# and order truth proves it is safe to resume.
ACCOUNT_TRUTH_REQUIRED_BLOCK_REASONS = (
    LIVE_ARTIFACT_BLOCK_REASONS
    | frozenset({STALE_RISK_ONLY_ACCOUNT_TRUTH_BLOCK_REASON})
)

LEGACY_MIGRATION_CLEARABLE_BLOCK_REASONS = frozenset(
    {
        "position_drift_correction_failed",
        "startup_recovery_pending_work_without_open_positions",
    }
)

CORE_CLEARABLE_BLOCK_REASONS = (
    CORE_OWNED_BLOCK_REASONS | LEGACY_MIGRATION_CLEARABLE_BLOCK_REASONS
)


class RecoveryTruthScope(StrEnum):
    """Coverage of the exchange truth supplied to a recovery decision."""

    ACCOUNT = "account"
    SYMBOLS = "symbols"


class RecoveryEvidenceClass(StrEnum):
    COMPLETE_FLAT = "complete_flat"
    COMPLETE_LIVE_ARTIFACT = "complete_live_artifact"
    OWNED_RECOVERY_WORK = "owned_recovery_work"
    ORPHAN_LIVE_ARTIFACT = "orphan_live_artifact"
    PARTIAL_EVIDENCE_GAP = "partial_evidence_gap"
    BACKGROUND_CLOSE_RECONCILIATION = "background_close_reconciliation"
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


class RecoveryManagementAction(StrEnum):
    NONE = "none"
    WAIT_FOR_TRUTH = "wait_for_truth"
    MANAGE_OWNED_RECOVERY_WORK = "manage_owned_recovery_work"
    FLATTEN_OR_BLOCK_LIVE_ARTIFACT = "flatten_or_block_live_artifact"
    PRESERVE_OPERATOR_FAIL_CLOSED = "preserve_operator_fail_closed"


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


def _state_collection_or_count(
    state: Mapping[str, Any] | Any,
    collection_key: str,
    count_key: str,
) -> tuple[Any, ...]:
    """Read an owner collection, or its compact-state count when omitted."""
    if isinstance(state, Mapping):
        collection = state.get(collection_key)
        count_value = state.get(count_key)
    else:
        collection = getattr(state, collection_key, None)
        count_value = getattr(state, count_key, None)
    if isinstance(collection, Mapping):
        return tuple(collection.values())
    if isinstance(collection, (list, tuple, set)):
        return tuple(collection)
    try:
        count = int(count_value or 0)
    except (TypeError, ValueError):
        count = 0
    return tuple({"source": count_key} for _ in range(max(count, 0)))


@dataclass(frozen=True)
class PendingCloseOwnerCounts:
    """Canonical projection of every unresolved close-work owner.

    A close remains owned until either its execution is complete or its
    accounting reconciliation is complete.  Runtime snapshots may omit the
    full collections, so every consumer must use these counts rather than
    reconstructing a partial view from whichever fields happen to be present.
    """

    pending_close_count: int = 0
    pending_passive_close_count: int = 0
    pending_close_reconciliation_count: int = 0

    @property
    def pending_close_owner_count(self) -> int:
        return (
            self.pending_close_count
            + self.pending_passive_close_count
            + self.pending_close_reconciliation_count
        )


def pending_passive_close_evidence(
    state: Mapping[str, Any] | Any,
) -> tuple[Any, ...]:
    """Return passive-close owners from one canonical state contract.

    ``pending_close_count`` belongs to the legacy close owner.  Passive closes
    have their own state collection and count, so diagnostics must never infer
    one from the other when the collection is omitted from a compact snapshot.
    """
    return _state_collection_or_count(
        state,
        "pending_passive_closes",
        "pending_passive_close_count",
    )


def pending_close_reconciliation_evidence(
    state: Mapping[str, Any] | Any,
) -> tuple[Any, ...]:
    """Return accounting-close owners from the canonical state contract.

    Older compact snapshots exported only a reconciliation summary.  Treat its
    total as the owner count when the queue and explicit count are absent, so a
    mixed-version fleet cannot report an unresolved reconciliation as clean.
    """
    if isinstance(state, Mapping):
        collection = state.get("pending_close_reconciliations")
        count_value = state.get("pending_close_reconciliation_count")
        summary = state.get("pending_close_reconciliation_summary")
    else:
        collection = getattr(state, "pending_close_reconciliations", None)
        count_value = getattr(state, "pending_close_reconciliation_count", None)
        summary = getattr(state, "pending_close_reconciliation_summary", None)

    if isinstance(collection, Mapping):
        materialized = tuple(collection.values())
    elif isinstance(collection, (list, tuple, set)):
        materialized = tuple(collection)
    else:
        materialized = ()

    candidate_counts = [len(materialized)]
    for value in (
        count_value,
        summary.get("total_count") if isinstance(summary, Mapping) else None,
    ):
        try:
            candidate_counts.append(max(int(value or 0), 0))
        except (TypeError, ValueError):
            continue
    count = max(candidate_counts)
    return materialized + tuple(
        {"source": "pending_close_reconciliation_count"}
        for _ in range(count - len(materialized))
    )


def pending_close_owner_counts(
    state: Mapping[str, Any] | Any,
) -> PendingCloseOwnerCounts:
    """Return the only valid close-work owner projection.

    This intentionally includes background close reconciliation.  V1 keeps
    that work in ``recovery_work_snapshot`` even while the runtime is allowed
    to continue operating, so reporting it as zero is a false clean state.
    """
    return PendingCloseOwnerCounts(
        pending_close_count=len(
            _state_collection_or_count(state, "pending_closes", "pending_close_count")
        ),
        pending_passive_close_count=len(pending_passive_close_evidence(state)),
        pending_close_reconciliation_count=len(
            pending_close_reconciliation_evidence(state)
        ),
    )


def is_nonblocking_background_close_reconciliation(
    *,
    open_position_count: int,
    pending_entry_count: int,
    pending_close_owners: PendingCloseOwnerCounts,
    pending_residual_repair_count: int,
    pending_close_reconciliation_unknown_count: int,
    exchange_truth: Mapping[str, Any] | Any | None,
    recovery_decision: Mapping[str, Any],
) -> bool:
    """Whether visible reconciliation is the only non-blocking recovery work.

    V1 allows this accounting work to continue after trustworthy exchange truth
    proves the account is flat. Consumers must not infer the condition from an
    owner count alone: any execution owner, incomplete exchange truth, or
    unknown reconciliation status remains fail-closed.
    """
    return (
        open_position_count == 0
        and pending_entry_count == 0
        and pending_close_owners.pending_close_count == 0
        and pending_close_owners.pending_passive_close_count == 0
        and pending_close_owners.pending_close_reconciliation_count > 0
        and pending_residual_repair_count == 0
        and pending_close_reconciliation_unknown_count == 0
        and isinstance(exchange_truth, Mapping)
        and bool(exchange_truth.get("available"))
        and str(exchange_truth.get("confidence", "")) == "high"
        and not bool(exchange_truth.get("has_nonzero_position"))
        and not bool(exchange_truth.get("has_open_order"))
        and recovery_decision.get("kind") == "RUNNING_WITH_EVIDENCE_GAP"
        and recovery_decision.get("evidence_quality")
        == "background_close_reconciliation"
        and recovery_decision.get("entry_allowed") is True
    )


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
    management_action: RecoveryManagementAction = RecoveryManagementAction.NONE


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
                management_action=RecoveryManagementAction.PRESERVE_OPERATOR_FAIL_CLOSED,
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
                management_action=RecoveryManagementAction.FLATTEN_OR_BLOCK_LIVE_ARTIFACT,
            )

        has_local_work = self._has_local_recovery_work(snapshot)
        has_truth_required_work = has_local_work or self._has_truth_required_recovery_context(
            snapshot
        )
        truth_available = self._truth_available(snapshot.exchange_truth)
        if has_truth_required_work and not truth_available:
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
                management_action=RecoveryManagementAction.WAIT_FOR_TRUTH,
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
                management_action=RecoveryManagementAction.MANAGE_OWNED_RECOVERY_WORK,
            )

        if self._has_background_close_reconciliation(snapshot.recovery_work_items):
            # V1 keeps close-accounting reconciliation visible in recovery work,
            # but a physically flat position must not be treated as open-risk
            # work.  Preserve that split centrally: diagnostics are non-clean,
            # while entry admission remains available.
            return RecoveryDecision(
                kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
                evidence_class=RecoveryEvidenceClass.BACKGROUND_CLOSE_RECONCILIATION,
                entry_allowed=True,
                clear_previous_block=self._should_clear_core_block(snapshot),
                clear_reason="core_background_close_reconciliation",
                recovery_work_items=snapshot.recovery_work_items,
                diagnostic_severity="warning",
                evidence_quality=(
                    RecoveryEvidenceClass.BACKGROUND_CLOSE_RECONCILIATION.value
                ),
                journal_event_name="recovery.core.background_close_reconciliation",
                maintenance_note=(
                    "Close-accounting reconciliation remains visible as "
                    "background work; no live execution owner requires an "
                    "entry block."
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
        for item in recovery_work_items:
            kind = str(_get(item, "kind", "") or "")
            if kind in LIVE_ARTIFACT_BLOCK_REASONS and _bool(
                _get(item, "blocking", True)
            ):
                return kind
        for order in _exchange_open_orders(snapshot.exchange_truth):
            # An open-orders endpoint contains only live orders; a non-reduce
            # order must not disappear from recovery merely because its size is
            # zero, missing, or malformed in a venue response.
            if _bool(_get(order, "reduce_only", False)):
                continue
            if _order_has_owned_work(order, recovery_work_items):
                continue
            return "orphan_maker_order"
        for position in _exchange_positions(snapshot.exchange_truth):
            quantity = self._finite_position_quantity(position)
            if quantity is None:
                return "unpaired_live_position"
            if abs(quantity) > EPSILON:
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

    def _has_truth_required_recovery_context(
        self, snapshot: RecoveryEvidenceSnapshot
    ) -> bool:
        if self._has_truth_required_recovery_work_item(snapshot.recovery_work_items):
            return True
        return any(
            _local_position_requires_truth(item)
            for item in _as_items(snapshot.local_open_positions)
        )

    def _has_blocking_recovery_work_item(self, work_items: Any) -> bool:
        for item in _as_items(work_items):
            if bool(_get(item, "blocking", True)):
                return True
        return False

    def _has_background_close_reconciliation(self, work_items: Any) -> bool:
        return any(
            str(_get(item, "kind", "") or "")
            == "pending_close_reconciliation"
            and not _bool(_get(item, "blocking", True))
            for item in _as_items(work_items)
        )

    def _has_truth_required_recovery_work_item(self, work_items: Any) -> bool:
        for item in _as_items(work_items):
            if _bool(_get(item, "requires_truth", _get(item, "truth_required", False))):
                return True
        return False

    @staticmethod
    def _truth_available(exchange_truth: Any | None) -> bool:
        if exchange_truth is None:
            return False
        if isinstance(exchange_truth, Mapping):
            if "truth_available" in exchange_truth:
                return bool(exchange_truth.get("truth_available"))
            if "available" in exchange_truth:
                return bool(exchange_truth.get("available"))
        return bool(_get(exchange_truth, "truth_available", _get(exchange_truth, "available", True)))

    @staticmethod
    def _has_partial_evidence_gap(exchange_truth: Any | None) -> bool:
        if exchange_truth is None:
            return True
        # Producers that report support must explicitly report True before
        # their account result can be treated as evidence.  V1 represents an
        # unsupported bulk probe separately from an empty response.
        if isinstance(exchange_truth, Mapping):
            if (
                "truth_supported" in exchange_truth
                and exchange_truth["truth_supported"] is not True
            ):
                return True
        elif (
            hasattr(exchange_truth, "truth_supported")
            and getattr(exchange_truth, "truth_supported") is not True
        ):
            return True
        missing = _get(exchange_truth, "missing_evidence", ())
        errors = _get(exchange_truth, "errors", ())
        probe_evidence = _get(exchange_truth, "probe_evidence", ())
        if _has_items(missing) or _has_items(errors):
            return True
        if _exchange_truth_has_error_placeholder(exchange_truth):
            return True
        return any(_probe_is_gap(item) for item in _as_items(probe_evidence))

    def _should_clear_core_block(self, snapshot: RecoveryEvidenceSnapshot) -> bool:
        reason = snapshot.prior_recovery_block_reason
        if reason in ACCOUNT_TRUTH_REQUIRED_BLOCK_REASONS:
            return self._exchange_truth_is_complete_flat(snapshot.exchange_truth)
        return reason is None or reason in CORE_CLEARABLE_BLOCK_REASONS

    @classmethod
    def is_complete_account_flat_truth(cls, exchange_truth: Any | None) -> bool:
        """Whether exchange truth can safely release any recovery lifecycle latch."""

        # A flat subset proves nothing about an account-level live artifact or
        # stale risk_only lifecycle.  Only complete account truth may release
        # either latch.
        scope = str(_get(exchange_truth, "truth_scope", "") or "").strip().lower()
        if scope != RecoveryTruthScope.ACCOUNT.value:
            return False
        if not cls._truth_explicitly_supported(exchange_truth):
            return False
        if not cls._truth_available(exchange_truth) or cls._has_partial_evidence_gap(
            exchange_truth
        ):
            return False

        # Complete truth requires both collections to be present.  A missing
        # collection is not evidence that the account is flat.
        positions_value = _get(exchange_truth, "positions", None)
        open_orders_value = _get(exchange_truth, "open_orders", None)
        if positions_value is None or open_orders_value is None:
            return False

        # A malformed or non-finite position amount must remain fail-closed;
        # comparisons with NaN otherwise look like a flat zero position.
        for position in _exchange_positions(exchange_truth):
            quantity = cls._finite_position_quantity(position)
            if quantity is None or abs(quantity) > EPSILON:
                return False

        # An account open-orders endpoint already returns only live orders.
        # Therefore *any* returned row is a live artifact, regardless of a
        # missing, zero, or malformed quantity field.
        return not _exchange_open_orders(exchange_truth)

    @staticmethod
    def _truth_explicitly_supported(exchange_truth: Any | None) -> bool:
        if isinstance(exchange_truth, Mapping):
            return exchange_truth.get("truth_supported") is True
        return getattr(exchange_truth, "truth_supported", None) is True

    @staticmethod
    def _finite_position_quantity(position: Any) -> float | None:
        for key in ("quantity", "qty", "size"):
            value = _get(position, key, None)
            if value is None:
                continue
            try:
                quantity = float(value)
            except (TypeError, ValueError):
                return None
            return quantity if isfinite(quantity) else None
        return None

    def _exchange_truth_is_complete_flat(self, exchange_truth: Any | None) -> bool:
        return self.is_complete_account_flat_truth(exchange_truth)


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
    if value is None or _is_exchange_error_placeholder(value):
        return ()
    if isinstance(value, Mapping):
        items: list[Any] = []
        for venue, venue_value in value.items():
            if _is_exchange_error_placeholder(venue_value):
                continue
            if isinstance(venue_value, Mapping):
                for symbol, symbol_value in venue_value.items():
                    if _is_exchange_error_placeholder(symbol_value):
                        continue
                    for item in _as_items(symbol_value):
                        if _is_exchange_error_placeholder(item):
                            continue
                        if isinstance(item, Mapping):
                            merged = dict(item)
                            merged.setdefault("venue", venue)
                            merged.setdefault("symbol", symbol)
                            items.append(merged)
                        else:
                            items.append(item)
            else:
                items.extend(
                    item
                    for item in _as_items(venue_value)
                    if not _is_exchange_error_placeholder(item)
                )
        return tuple(items)
    return tuple(
        item for item in _as_items(value) if not _is_exchange_error_placeholder(item)
    )


def _exchange_truth_has_error_placeholder(exchange_truth: Any | None) -> bool:
    """Whether a collection contains an unconfirmed endpoint-error placeholder.

    Diagnostics preserve failed endpoint payloads alongside successful venue data.
    Such placeholders are evidence gaps, not orders or positions.  Keep this
    classification next to collection flattening so every core decision path
    consumes the same distinction.
    """
    return any(
        _contains_exchange_error_placeholder(_get(exchange_truth, key, ()))
        for key in ("positions", "open_orders")
    )


def _contains_exchange_error_placeholder(value: Any) -> bool:
    if _is_exchange_error_placeholder(value):
        return True
    if isinstance(value, Mapping):
        return any(_contains_exchange_error_placeholder(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_contains_exchange_error_placeholder(item) for item in value)
    return False


def _is_exchange_error_placeholder(value: Any) -> bool:
    """Recognize a failed endpoint payload without hiding concrete live rows."""
    if not isinstance(value, Mapping) or not _get(value, "error", ""):
        return False
    # A concrete row remains live even when a venue annotates it with an error.
    # For a bare error container, there is no live artifact to classify.
    return not any(
        key in value
        for key in (
            "quantity",
            "qty",
            "size",
            "positionAmt",
            "order_id",
            "orderId",
            "ordId",
            "id",
            "client_order_id",
            "clientOrderId",
            "orderLinkId",
            "clientOid",
            "clOrdId",
        )
    )


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


def _local_position_requires_truth(position: Any) -> bool:
    return any(
        _bool(_get(position, key, False))
        for key in (
            "requires_truth",
            "truth_required",
            "recovery_required",
            "reconcile_required",
            "needs_recovery",
        )
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
