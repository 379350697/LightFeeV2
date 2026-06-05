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
            if not order_id and not client_order_id:
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
            order_id, client_order_id = _journal_order_identifiers(payload)
            if not order_id and not client_order_id:
                continue
            owner_id = _journal_owner_key(payload)
            symbol = _text(payload.get("symbol")).upper()
            owner = RecoveryOwner(
                owner_type="journal_pending_entry",
                owner_id=owner_id or symbol,
                confidence="probable",
                evidence={
                    "source": "journal",
                    "kind": _text(_get(event, "kind", "")),
                    "symbol": symbol,
                },
            )
            self._index_order_ids(
                owner,
                order_ids=(order_id,),
                client_order_ids=(client_order_id,),
            )

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


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value or "")
