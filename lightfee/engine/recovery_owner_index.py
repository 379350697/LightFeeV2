"""Owner reconstruction for exchange-truth recovery work."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from lightfee.engine.recovery_ledger import ExchangeArtifact, RecoveryOwner


@dataclass
class RecoveryOwnerIndex:
    _orders_by_id: dict[str, RecoveryOwner] = field(default_factory=dict)
    _orders_by_client_id: dict[str, RecoveryOwner] = field(default_factory=dict)
    _positions_by_key: dict[tuple[str, str], RecoveryOwner] = field(default_factory=dict)
    _residuals_by_key: dict[tuple[str, str], RecoveryOwner] = field(default_factory=dict)
    _residuals_by_symbol: dict[str, RecoveryOwner] = field(default_factory=dict)
    _journal_position_facts: list[tuple[str, str, float, RecoveryOwner]] = field(
        default_factory=list
    )

    @classmethod
    def from_state(cls, state: Any) -> "RecoveryOwnerIndex":
        index = cls()
        index._add_state(state)
        return index

    @classmethod
    def from_state_and_journal(
        cls,
        state: Any,
        journal_events: Iterable[Any],
    ) -> "RecoveryOwnerIndex":
        index = cls.from_state(state)
        index._add_journal_events(journal_events)
        return index

    @classmethod
    def active_journal_owner_events(cls, journal_events: Iterable[Any]) -> list[Any]:
        active_events: list[Any] = []
        for event in journal_events:
            payload = _get(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            order_id, client_order_id = _journal_order_identifiers(payload)
            if not order_id and not client_order_id and not _journal_position_specs(
                event, payload
            ):
                continue
            active_events.append(event)
        return active_events

    def owner_for_order(self, artifact: ExchangeArtifact | Any) -> RecoveryOwner:
        order_id = _text(_get(artifact, "order_id", ""))
        client_order_id = _text(_get(artifact, "client_order_id", ""))
        if order_id and order_id in self._orders_by_id:
            return self._orders_by_id[order_id]
        if client_order_id and client_order_id in self._orders_by_client_id:
            return self._orders_by_client_id[client_order_id]
        return RecoveryOwner(
            owner_type="exchange_order",
            owner_id=order_id or client_order_id,
            confidence="orphan",
            evidence={"source": "exchange_truth"},
        )

    def owner_for_position(self, artifact: ExchangeArtifact | Any) -> RecoveryOwner:
        key = (_venue(artifact), _symbol(artifact))
        if key in self._positions_by_key:
            return self._positions_by_key[key]
        if key in self._residuals_by_key:
            return self._residuals_by_key[key]
        symbol = key[1]
        if symbol in self._residuals_by_symbol:
            return self._residuals_by_symbol[symbol]
        journal_owner = self._owner_for_journal_position_fact(artifact)
        if journal_owner is not None:
            return journal_owner
        return RecoveryOwner(
            owner_type="exchange_position",
            owner_id=symbol,
            confidence="orphan",
            evidence={"source": "exchange_truth"},
        )

    def _add_state(self, state: Any) -> None:
        for pending in _collection(state, "pending_entries"):
            owner = RecoveryOwner(
                owner_type="pending_entry",
                owner_id=_text(
                    _get(pending, "pending_id", _get(pending, "position_id", ""))
                ),
                confidence="proven",
                evidence={"source": "local_pending_entry"},
            )
            self._index_order_ids(
                owner,
                order_ids=(
                    _get(pending, "maker_order_id", ""),
                    _get(pending, "hedge_order_id", ""),
                ),
                client_order_ids=(
                    _get(pending, "maker_client_order_id", ""),
                    _get(pending, "hedge_client_order_id", ""),
                ),
            )
            self._index_pending_entry_position_keys(owner, pending)

        for position in _collection(state, "open_positions"):
            owner = RecoveryOwner(
                owner_type="open_position",
                owner_id=_text(
                    _get(position, "position_id", _get(position, "symbol", ""))
                ),
                confidence="proven",
                evidence={"source": "local_open_position"},
            )
            symbol = _symbol(position)
            for venue in (
                _venue_from_key(position, "long_venue"),
                _venue_from_key(position, "short_venue"),
            ):
                if venue and symbol:
                    self._positions_by_key[(venue, symbol)] = owner

        for pending in _collection(state, "pending_passive_closes"):
            owner = RecoveryOwner(
                owner_type="pending_passive_close",
                owner_id=_text(_get(pending, "position_id", _get(pending, "close_id", ""))),
                confidence="proven",
                evidence={"source": "local_pending_passive_close"},
            )
            phase_state = _get(pending, "phase_state", {})
            self._index_order_ids(
                owner,
                order_ids=(
                    _get(pending, "maker_order_id", ""),
                    _get(phase_state, "maker_order_id", ""),
                    _get(_get(pending, "maker_fill", {}), "order_id", ""),
                    _get(_get(pending, "hedge_fill", {}), "order_id", ""),
                ),
                client_order_ids=(
                    _get(pending, "maker_client_order_id", ""),
                    _get(phase_state, "maker_client_order_id", ""),
                    _get(_get(pending, "maker_fill", {}), "client_order_id", ""),
                    _get(_get(pending, "hedge_fill", {}), "client_order_id", ""),
                ),
            )

        for residual in _collection(state, "pending_residual_repairs"):
            owner = RecoveryOwner(
                owner_type="residual_repair",
                owner_id=_text(
                    _get(residual, "repair_id", _get(residual, "task_id", ""))
                    or _get(residual, "symbol", "")
                ),
                confidence="probable",
                evidence={"source": "local_residual_repair"},
            )
            symbol = _symbol(residual)
            venue = _venue(residual)
            if symbol:
                self._residuals_by_symbol[symbol] = owner
            if venue and symbol:
                self._residuals_by_key[(venue, symbol)] = owner

    def _add_journal_events(self, journal_events: Iterable[Any]) -> None:
        for event in self.active_journal_owner_events(journal_events):
            payload = _get(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            kind = _text(_get(event, "kind", ""))
            order_id, client_order_id = _journal_order_identifiers(payload)
            position_specs = _journal_position_specs(event, payload)
            if not order_id and not client_order_id and not position_specs:
                continue
            owner_id = _journal_owner_key(payload)
            symbol = _text(payload.get("symbol")).upper()
            is_passive_close = kind.startswith("exit.passive_close")
            owner = RecoveryOwner(
                owner_type=(
                    "journal_passive_close"
                    if is_passive_close
                    else "journal_pending_entry"
                ),
                owner_id=owner_id or symbol,
                confidence="probable",
                evidence={
                    "source": "journal_passive_close" if is_passive_close else "journal",
                    "kind": kind,
                    "symbol": symbol,
                },
            )
            self._index_order_ids(
                owner,
                order_ids=(order_id,),
                client_order_ids=(client_order_id,),
            )
            self._index_journal_position_facts(event, payload, owner)

    def _index_journal_position_facts(
        self,
        event: Any,
        payload: Mapping[str, Any],
        owner: RecoveryOwner,
    ) -> None:
        for symbol, side, quantity in _journal_position_specs(event, payload):
            position_owner = RecoveryOwner(
                owner_type=owner.owner_type,
                owner_id=owner.owner_id,
                confidence=owner.confidence,
                evidence={
                    **dict(owner.evidence),
                    "position_scope": "journal_positive_fill_live_conflict",
                    "expected_side": side,
                    "expected_quantity": quantity,
                },
            )
            self._journal_position_facts.append((symbol, side, quantity, position_owner))

    def _owner_for_journal_position_fact(
        self,
        artifact: ExchangeArtifact | Any,
    ) -> RecoveryOwner | None:
        symbol = _symbol(artifact)
        side = _side(_get(artifact, "side", ""))
        quantity = _float(_get(artifact, "quantity", 0.0))
        if not symbol or not side or quantity <= 0.0:
            return None
        for fact_symbol, fact_side, fact_quantity, owner in reversed(
            self._journal_position_facts
        ):
            if fact_symbol != symbol:
                continue
            if fact_side != side:
                continue
            if abs(fact_quantity - quantity) > 1e-9:
                continue
            return owner
        return None

    def _index_order_ids(
        self,
        owner: RecoveryOwner,
        *,
        order_ids: Iterable[Any],
        client_order_ids: Iterable[Any],
    ) -> None:
        for order_id in order_ids:
            key = _text(order_id)
            if key:
                self._orders_by_id[key] = owner
        for client_order_id in client_order_ids:
            key = _text(client_order_id)
            if key:
                self._orders_by_client_id[key] = owner

    def _index_pending_entry_position_keys(
        self,
        owner: RecoveryOwner,
        pending: Any,
    ) -> None:
        symbol = _symbol(pending)
        if not symbol:
            return
        maker_fill = _float(_get(pending, "maker_leg_filled", 0.0))
        hedge_fill = _float(_get(pending, "hedge_leg_filled", 0.0))
        if maker_fill <= 0.0 and hedge_fill <= 0.0:
            return
        maker_leg = _leg_text(_get(pending, "maker_leg", ""))
        if maker_leg not in {"long", "short"}:
            maker_side = _leg_text(_get(pending, "maker_side", ""))
            if maker_side in {"buy", "long"}:
                maker_leg = "long"
            elif maker_side in {"sell", "short"}:
                maker_leg = "short"
        if maker_leg not in {"long", "short"}:
            return
        long_fill = 0.0
        short_fill = 0.0
        if maker_leg == "short":
            short_fill += maker_fill
            long_fill += hedge_fill
        else:
            long_fill += maker_fill
            short_fill += hedge_fill
        position_owner = RecoveryOwner(
            owner_type=owner.owner_type,
            owner_id=owner.owner_id,
            confidence=owner.confidence,
            evidence={
                **dict(owner.evidence),
                "position_scope": "positive_fill_pending_entry",
            },
        )
        if long_fill > 0.0:
            long_venue = _venue_from_key(pending, "long_venue")
            if long_venue:
                self._positions_by_key[(long_venue, symbol)] = position_owner
        if short_fill > 0.0:
            short_venue = _venue_from_key(pending, "short_venue")
            if short_venue:
                self._positions_by_key[(short_venue, symbol)] = position_owner


def _collection(source: Any, key: str) -> list[Any]:
    value = _get(source, key, [])
    if value is None:
        return []
    if isinstance(value, Mapping):
        return list(value.values())
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _symbol(obj: Any) -> str:
    return _text(_get(obj, "symbol", "")).upper()


def _venue(obj: Any) -> str:
    return _normalize_venue(_get(obj, "venue", ""))


def _venue_from_key(obj: Any, key: str) -> str:
    value = _get(obj, key, "")
    if callable(value):
        value = value()
    return _normalize_venue(value)


def _normalize_venue(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    return _text(value).lower()


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _leg_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    return _text(value).lower()


def _side(value: Any) -> str:
    text = _leg_text(value)
    if text in {"buy", "long", "side.buy"}:
        return "long"
    if text in {"sell", "short", "side.sell"}:
        return "short"
    if text.endswith(".buy"):
        return "long"
    if text.endswith(".sell"):
        return "short"
    return text


def _journal_order_identifiers(payload: Mapping[str, Any]) -> tuple[str, str]:
    order_id = _text(
        payload.get("order_id")
        or payload.get("maker_order_id")
        or payload.get("exchange_order_id")
    )
    client_order_id = _text(
        payload.get("client_order_id")
        or payload.get("maker_client_order_id")
        or payload.get("clientOrderId")
    )
    return order_id, client_order_id


def _journal_owner_key(payload: Mapping[str, Any]) -> str:
    return _text(
        payload.get("entry_id")
        or payload.get("pending_id")
        or payload.get("position_id")
        or payload.get("source_entry_id")
        or payload.get("internal_entry_id")
    )


def _journal_position_specs(
    event: Any,
    payload: Mapping[str, Any],
) -> tuple[tuple[str, str, float], ...]:
    kind = _text(_get(event, "kind", "")).lower()
    outcome = _text(payload.get("outcome")).lower()
    if kind not in {
        "pending_entry.positive_fill_live_truth_conflict",
        "pending_entry.terminalizer_decision",
    }:
        return ()
    if (
        kind == "pending_entry.terminalizer_decision"
        and outcome != "positive_fill_live_truth_conflict"
    ):
        return ()
    symbol = _text(payload.get("symbol")).upper()
    if not symbol:
        return ()
    specs: list[tuple[str, str, float]] = []
    live_long = _float(payload.get("live_long_quantity"))
    live_short = _float(payload.get("live_short_quantity"))
    if live_long > 0.0:
        specs.append((symbol, "long", live_long))
    if live_short > 0.0:
        specs.append((symbol, "short", live_short))
    return tuple(specs)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value or "")
