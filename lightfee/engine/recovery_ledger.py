"""Pure exchange-truth recovery ledger.

The ledger classifies local runtime state plus exchange truth into recovery
work. It intentionally performs no venue I/O and submits no orders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


EPSILON = 1e-9

GLOBAL_BLOCKING_KINDS = frozenset(
    {
        "orphan_maker_order",
        "unpaired_live_position",
        "ambiguous_exchange_truth",
    }
)


@dataclass(frozen=True)
class ExchangeArtifact:
    kind: str
    symbol: str = ""
    venue: str = ""
    side: str = ""
    quantity: float = 0.0
    price: float = 0.0
    reduce_only: bool = False
    order_id: str = ""
    client_order_id: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecoveryOwner:
    owner_type: str
    owner_id: str = ""
    confidence: str = "orphan"
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecoveryDecision:
    outcome: str
    reason: str = ""
    blocking: bool = True


@dataclass(frozen=True)
class RecoveryWorkItem:
    kind: str
    symbol: str = ""
    venues: frozenset[str] = field(default_factory=frozenset)
    artifacts: tuple[ExchangeArtifact, ...] = ()
    owner: RecoveryOwner | None = None
    decision: RecoveryDecision = field(
        default_factory=lambda: RecoveryDecision(
            outcome="fail_closed_operator_block",
            reason="unclassified_recovery_work",
        )
    )
    blocking: bool = True

    @property
    def blocks_all_new_entries(self) -> bool:
        return self.blocking and self.kind in GLOBAL_BLOCKING_KINDS


@dataclass
class RecoveryLedger:
    work_items: list[RecoveryWorkItem] = field(default_factory=list)
    truth_available: bool = True
    _live_symbols: set[str] = field(default_factory=set)
    _positive_fill_symbols: set[str] = field(default_factory=set)

    @classmethod
    def from_incident_fixture(cls, fixture: Mapping[str, Any]) -> "RecoveryLedger":
        return cls.from_local_and_exchange_truth(
            local=_get(fixture, "local", {}),
            exchange_truth=_get(fixture, "exchange_truth", {}),
        )

    @classmethod
    def from_local_and_exchange_truth(
        cls,
        *,
        local: Any,
        exchange_truth: Any,
        owner_index: Any | None = None,
    ) -> "RecoveryLedger":
        truth_available = _truth_available(exchange_truth)
        work_items: list[RecoveryWorkItem] = []
        seen_work: set[tuple[str, str, str]] = set()
        live_symbols: set[str] = set()
        positive_fill_symbols: set[str] = set()

        def add_work(item: RecoveryWorkItem) -> None:
            key = (
                item.kind,
                item.symbol,
                item.owner.owner_id if item.owner is not None else "",
            )
            if key in seen_work:
                return
            seen_work.add(key)
            work_items.append(item)

        if not truth_available:
            add_work(
                RecoveryWorkItem(
                    kind="ambiguous_exchange_truth",
                    decision=RecoveryDecision(
                        outcome="fail_closed_operator_block",
                        reason="exchange_truth_unavailable",
                    ),
                    blocking=True,
                )
            )

        local_open_positions = list(_local_collection(local, "open_positions"))
        local_pending_entries = list(_local_collection(local, "pending_entries"))
        local_residuals = list(_local_collection(local, "pending_residual_repairs"))
        local_passive_closes = list(_local_collection(local, "pending_passive_closes"))
        local_fill_evidence = list(_local_collection(local, "fill_evidence"))

        open_position_symbols = {
            _symbol(item)
            for item in local_open_positions
            if _symbol(item)
        }

        for pending in local_pending_entries:
            symbol = _symbol(pending)
            if not symbol:
                continue
            maker_fill = _float(_get(pending, "maker_leg_filled", 0.0))
            hedge_fill = _float(_get(pending, "hedge_leg_filled", 0.0))
            if maker_fill > EPSILON or hedge_fill > EPSILON:
                positive_fill_symbols.add(symbol)
            add_work(
                RecoveryWorkItem(
                    kind="owned_pending_entry",
                    symbol=symbol,
                    venues=_venues_from_local_entry(pending),
                    owner=RecoveryOwner(
                        owner_type="pending_entry",
                        owner_id=str(
                            _get(
                                pending,
                                "pending_id",
                                _get(pending, "position_id", symbol),
                            )
                        ),
                        confidence="proven",
                        evidence={"source": "local_pending_entry"},
                    ),
                    decision=RecoveryDecision(
                        outcome="managed_open_position",
                        reason="pending_entry_requires_terminalizer",
                    ),
                    blocking=True,
                )
            )

        for fill in local_fill_evidence:
            symbol = _symbol(fill)
            quantity = _float(
                _get(fill, "filled_quantity", _get(fill, "quantity", 0.0))
            )
            classification = str(_get(fill, "classification", "") or "").lower()
            if quantity <= EPSILON and "positive" not in classification:
                continue
            positive_fill_symbols.add(symbol)
            add_work(
                RecoveryWorkItem(
                    kind="owned_pending_entry",
                    symbol=symbol,
                    venues=frozenset(filter(None, [_venue(fill)])),
                    artifacts=(
                        ExchangeArtifact(
                            kind="fill_evidence",
                            symbol=symbol,
                            venue=_venue(fill),
                            side=str(_get(fill, "side", "") or "").lower(),
                            quantity=quantity,
                            price=_float(_get(fill, "average_price", 0.0)),
                            order_id=str(_get(fill, "order_id", "") or ""),
                            client_order_id=str(
                                _get(fill, "client_order_id", "") or ""
                            ),
                            raw=_raw_mapping(fill),
                        ),
                    ),
                    owner=RecoveryOwner(
                        owner_type="fill_evidence",
                        owner_id=str(
                            _get(
                                fill,
                                "client_order_id",
                                _get(fill, "order_id", symbol),
                            )
                            or symbol
                        ),
                        confidence="proven",
                        evidence={"source": "local_fill_evidence"},
                    ),
                    decision=RecoveryDecision(
                        outcome="managed_open_position",
                        reason="positive_fill_requires_recovery",
                    ),
                    blocking=True,
                )
            )

        positions = _exchange_positions(exchange_truth)
        open_orders = _exchange_open_orders(exchange_truth)

        for position in positions:
            quantity = _float(_get(position, "quantity", 0.0))
            if quantity <= EPSILON:
                continue
            artifact = ExchangeArtifact(
                kind="position",
                symbol=_symbol(position),
                venue=_venue(position),
                side=str(_get(position, "side", "") or "").lower(),
                quantity=quantity,
                price=_float(_get(position, "entry_price", 0.0)),
                raw=_raw_mapping(position),
            )
            live_symbols.add(artifact.symbol)
            owner = _owner_for_position(owner_index, artifact)
            if owner is not None and owner.confidence != "orphan":
                add_work(
                    RecoveryWorkItem(
                        kind="owned_open_position",
                        symbol=artifact.symbol,
                        venues=frozenset(filter(None, [artifact.venue])),
                        artifacts=(artifact,),
                        owner=owner,
                        decision=RecoveryDecision(
                            outcome="managed_open_position",
                            reason="exchange_position_has_runtime_owner",
                            blocking=False,
                        ),
                        blocking=False,
                    )
                )
                continue
            if artifact.symbol in open_position_symbols:
                add_work(
                    RecoveryWorkItem(
                        kind="owned_open_position",
                        symbol=artifact.symbol,
                        venues=frozenset(filter(None, [artifact.venue])),
                        artifacts=(artifact,),
                        owner=RecoveryOwner(
                            owner_type="open_position",
                            owner_id=artifact.symbol,
                            confidence="probable",
                            evidence={"source": "local_open_position_symbol"},
                        ),
                        decision=RecoveryDecision(
                            outcome="managed_open_position",
                            reason="exchange_position_matches_local_symbol",
                            blocking=False,
                        ),
                        blocking=False,
                    )
                )
            else:
                add_work(
                    RecoveryWorkItem(
                        kind="unpaired_live_position",
                        symbol=artifact.symbol,
                        venues=frozenset(filter(None, [artifact.venue])),
                        artifacts=(artifact,),
                        owner=RecoveryOwner(
                            owner_type="exchange_position",
                            owner_id=artifact.symbol,
                            confidence="orphan",
                            evidence={"source": "exchange_truth"},
                        ),
                        decision=RecoveryDecision(
                            outcome="fail_closed_operator_block",
                            reason="live_position_without_runtime_owner",
                        ),
                        blocking=True,
                    )
                )

        for order in open_orders:
            quantity = _float(_get(order, "quantity", _get(order, "qty", 0.0)))
            if quantity <= EPSILON:
                continue
            artifact = ExchangeArtifact(
                kind="open_order",
                symbol=_symbol(order),
                venue=_venue(order),
                side=str(_get(order, "side", "") or "").lower(),
                quantity=quantity,
                price=_float(_get(order, "price", 0.0)),
                reduce_only=bool(_get(order, "reduce_only", False)),
                order_id=str(_get(order, "order_id", "") or ""),
                client_order_id=str(_get(order, "client_order_id", "") or ""),
                raw=_raw_mapping(order),
            )
            live_symbols.add(artifact.symbol)
            owner = _owner_for_order(owner_index, artifact)
            if owner is not None and owner.confidence != "orphan":
                add_work(
                    RecoveryWorkItem(
                        kind="owned_pending_entry",
                        symbol=artifact.symbol,
                        venues=frozenset(filter(None, [artifact.venue])),
                        artifacts=(artifact,),
                        owner=owner,
                        decision=RecoveryDecision(
                            outcome="owned_order_cancel_requested",
                            reason="live_order_has_runtime_owner",
                        ),
                        blocking=True,
                    )
                )
                continue
            kind = "orphan_reduce_only_order" if artifact.reduce_only else "orphan_maker_order"
            outcome = (
                "reduce_only_cleanup_submitted"
                if artifact.reduce_only
                else "fail_closed_operator_block"
            )
            add_work(
                RecoveryWorkItem(
                    kind=kind,
                    symbol=artifact.symbol,
                    venues=frozenset(filter(None, [artifact.venue])),
                    artifacts=(artifact,),
                    owner=RecoveryOwner(
                        owner_type="exchange_order",
                        owner_id=artifact.order_id or artifact.client_order_id,
                        confidence="orphan",
                        evidence={"source": "exchange_truth"},
                    ),
                    decision=RecoveryDecision(
                        outcome=outcome,
                        reason="live_order_without_runtime_owner",
                    ),
                    blocking=True,
                )
            )

        live_symbol_order_scope = {
            item.symbol
            for item in work_items
            if item.kind in {"orphan_maker_order", "orphan_reduce_only_order"}
        }
        live_position_scope = {
            item.symbol
            for item in work_items
            if item.kind in {"unpaired_live_position", "owned_open_position"}
        }

        for task in local_residuals:
            symbol = _symbol(task)
            if not symbol:
                continue
            live_flat = symbol not in live_symbol_order_scope and symbol not in live_position_scope
            add_work(
                RecoveryWorkItem(
                    kind="pending_residual_repair",
                    symbol=symbol,
                    venues=frozenset(filter(None, [_venue(task)])),
                    owner=RecoveryOwner(
                        owner_type="residual_repair",
                        owner_id=str(_get(task, "repair_id", _get(task, "task_id", symbol))),
                        confidence="proven",
                        evidence={"source": "local_residual_repair"},
                    ),
                    decision=RecoveryDecision(
                        outcome="proven_flat" if live_flat and truth_available else "residual_repair_queued",
                        reason="residual_repair_requires_terminal_resolution",
                    ),
                    blocking=True,
                )
            )

        for close in local_passive_closes:
            symbol = _symbol(close)
            add_work(
                RecoveryWorkItem(
                    kind="pending_passive_close",
                    symbol=symbol,
                    venues=frozenset(filter(None, _venues_from_close(close))),
                    owner=RecoveryOwner(
                        owner_type="passive_close",
                        owner_id=str(_get(close, "position_id", symbol)),
                        confidence="proven",
                        evidence={"source": "local_passive_close"},
                    ),
                    decision=RecoveryDecision(
                        outcome="pending_passive_close",
                        reason="passive_close_requires_recovery",
                    ),
                    blocking=True,
                )
            )

        return cls(
            work_items=work_items,
            truth_available=truth_available,
            _live_symbols=live_symbols,
            _positive_fill_symbols=positive_fill_symbols,
        )

    @property
    def allows_new_entries(self) -> bool:
        return not self.has_blocking_work()

    def has_blocking_work(self) -> bool:
        return any(item.blocking for item in self.work_items)

    def allows_new_entry(self, candidate: Any) -> bool:
        if any(item.blocks_all_new_entries for item in self.work_items):
            return False
        candidate_symbol = _symbol(candidate)
        candidate_venues = _venues_from_candidate(candidate)
        for item in self.work_items:
            if not item.blocking:
                continue
            if candidate_symbol and item.symbol and candidate_symbol == item.symbol:
                if not candidate_venues or not item.venues or candidate_venues & item.venues:
                    return False
                continue
            if (
                candidate_symbol
                and item.symbol
                and candidate_symbol != item.symbol
            ):
                continue
            if candidate_venues and item.venues and candidate_venues & item.venues:
                return False
        return True

    def is_proven_flat(self, symbol: str) -> bool:
        normalized = str(symbol or "").upper()
        if not self.truth_available:
            return False
        if normalized in self._live_symbols:
            return False
        return all(item.symbol != normalized for item in self.work_items if item.blocking)

    def contains_positive_fill_evidence(self, symbol: str) -> bool:
        return str(symbol or "").upper() in self._positive_fill_symbols


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _raw_mapping(obj: Any) -> Mapping[str, Any]:
    if isinstance(obj, Mapping):
        return dict(obj)
    return {}


def _local_collection(local: Any, key: str) -> Iterable[Any]:
    value = _get(local, key, [])
    return _as_items(value)


def _as_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return list(value.values())
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _truth_available(exchange_truth: Any) -> bool:
    if exchange_truth is None:
        return False
    if isinstance(exchange_truth, Mapping) and "truth_available" in exchange_truth:
        return bool(exchange_truth.get("truth_available"))
    if isinstance(exchange_truth, Mapping) and "available" in exchange_truth:
        return bool(exchange_truth.get("available"))
    return bool(_get(exchange_truth, "truth_available", True))


def _exchange_positions(exchange_truth: Any) -> list[Any]:
    return _flatten_exchange_collection(_get(exchange_truth, "positions", []))


def _exchange_open_orders(exchange_truth: Any) -> list[Any]:
    return _flatten_exchange_collection(_get(exchange_truth, "open_orders", []))


def _flatten_exchange_collection(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        items: list[Any] = []
        for venue, venue_value in value.items():
            if isinstance(venue_value, Mapping):
                for symbol, symbol_value in venue_value.items():
                    if isinstance(symbol_value, Mapping):
                        item = dict(symbol_value)
                        item.setdefault("venue", venue)
                        item.setdefault("symbol", symbol)
                        items.append(item)
                    elif isinstance(symbol_value, (int, float)):
                        items.append(
                            {
                                "venue": venue,
                                "symbol": symbol,
                                "quantity": float(symbol_value),
                            }
                        )
                    else:
                        items.extend(_as_items(symbol_value))
            else:
                for item in _as_items(venue_value):
                    if isinstance(item, Mapping):
                        merged = dict(item)
                        merged.setdefault("venue", venue)
                        items.append(merged)
                    else:
                        items.append(item)
        return items
    return _as_items(value)


def _symbol(obj: Any) -> str:
    return str(_get(obj, "symbol", "") or "").upper()


def _venue(obj: Any) -> str:
    value = _get(obj, "venue", "")
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _venues_from_local_entry(entry: Any) -> frozenset[str]:
    return frozenset(
        venue
        for venue in (
            _venue_from_key(entry, "long_venue"),
            _venue_from_key(entry, "short_venue"),
            _venue_from_key(entry, "maker_venue"),
            _venue_from_key(entry, "hedge_venue"),
        )
        if venue
    )


def _venues_from_close(close: Any) -> tuple[str, ...]:
    position_snapshot = _get(close, "position_snapshot", None)
    return (
        _venue_from_key(close, "venue"),
        _venue_from_key(close, "long_venue"),
        _venue_from_key(close, "short_venue"),
        _venue_from_key(position_snapshot, "long_venue"),
        _venue_from_key(position_snapshot, "short_venue"),
    )


def _venues_from_candidate(candidate: Any) -> frozenset[str]:
    return frozenset(
        venue
        for venue in (
            _venue_from_key(candidate, "venue"),
            _venue_from_key(candidate, "long_venue"),
            _venue_from_key(candidate, "short_venue"),
        )
        if venue
    )


def _venue_from_key(obj: Any, key: str) -> str:
    value = _get(obj, key, "")
    if callable(value):
        value = value()
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _float(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if result != result:
        return 0.0
    return result


def _owner_for_order(owner_index: Any | None, artifact: ExchangeArtifact) -> RecoveryOwner | None:
    if owner_index is None or not hasattr(owner_index, "owner_for_order"):
        return None
    owner = owner_index.owner_for_order(artifact)
    return _coerce_owner(owner)


def _owner_for_position(owner_index: Any | None, artifact: ExchangeArtifact) -> RecoveryOwner | None:
    if owner_index is None or not hasattr(owner_index, "owner_for_position"):
        return None
    owner = owner_index.owner_for_position(artifact)
    return _coerce_owner(owner)


def _coerce_owner(owner: Any) -> RecoveryOwner | None:
    if owner is None:
        return None
    if isinstance(owner, RecoveryOwner):
        return owner
    return RecoveryOwner(
        owner_type=str(_get(owner, "owner_type", "unknown")),
        owner_id=str(_get(owner, "owner_id", "")),
        confidence=str(_get(owner, "confidence", "orphan")),
        evidence=_raw_mapping(_get(owner, "evidence", {})),
    )
