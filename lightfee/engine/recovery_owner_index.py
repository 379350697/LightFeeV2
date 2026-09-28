"""Owner reconstruction for exchange-truth recovery work."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter
from math import isfinite
from typing import Any, Iterable, Mapping

from lightfee.engine.recovery_ledger import ExchangeArtifact, RecoveryOwner
from lightfee.engine.recovery_decision_core import V1_PAIR_QUANTITY_EPSILON
from lightfee.venues.specs import canonical_symbol_from_venue


@dataclass
class RecoveryOwnerIndex:
    _orders_by_id: dict[str, RecoveryOwner] = field(default_factory=dict)
    _orders_by_client_id: dict[str, RecoveryOwner] = field(default_factory=dict)
    _positions_by_key: dict[tuple[str, str, str], list[tuple[float, RecoveryOwner]]] = field(default_factory=dict)
    _position_scopes: set[tuple[str, str]] = field(default_factory=set)
    _journal_position_facts: list[tuple[str, str, str, float, RecoveryOwner]] = field(
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
        events = list(journal_events)
        completed_claims = cls._durably_handed_off_claims(events)
        terminal_pending_owner_ids = cls._terminal_pending_owner_ids(events)
        active_events: list[Any] = []
        for event in events:
            payload = _get(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            kind = _text(_get(event, "kind", "")).lower()
            owner_ids = _journal_owner_identifiers(payload)
            if kind != "entry.opened" and owner_ids & terminal_pending_owner_ids:
                continue
            # This event says that the original pre-submit claim remains live;
            # it is not a second owner record.  Its diagnostic CIDs must never
            # override the claim when rebuilding the index at startup.
            if kind == "runtime.entry_owner_handoff_incomplete":
                continue
            if (
                kind == "runtime.entry_owner_claimed"
                and _journal_owner_key(payload) in completed_claims
            ):
                continue
            order_ids, client_order_ids = _journal_order_identifier_sets(payload)
            if (
                not order_ids
                and not client_order_ids
                and not _journal_position_specs(event, payload)
                and not _journal_live_position_probe_evidence(event, payload)
            ):
                continue
            active_events.append(event)
        return active_events

    @staticmethod
    def _terminal_pending_owner_ids(events: Iterable[Any]) -> set[str]:
        terminal_ids: set[str] = set()
        for event in events:
            if _text(_get(event, "kind", "")).lower() != (
                "pending_entry.removed_by_v1_lifecycle_closure"
            ):
                continue
            payload = _get(event, "payload", {})
            if isinstance(payload, Mapping):
                terminal_ids.update(_journal_owner_identifiers(payload))
        return terminal_ids

    @staticmethod
    def _durably_handed_off_claims(events: list[Any]) -> set[str]:
        """Return claims whose declared successor was journaled first.

        New handoff records name their destination.  Suppressing a pre-submit
        claim before the corresponding successor is durable creates an orphan
        window after process crash, so only verified ordered successors retire
        it.  Destination-less historical records keep their original terminal
        interpretation for backward compatibility.
        """
        completed: set[str] = set()
        prior_pending: set[str] = set()
        prior_open: set[str] = set()
        for event in events:
            payload = _get(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            kind = _text(_get(event, "kind", "")).lower()
            key = _journal_owner_key(payload)
            if kind == "entry.pending_registered" and key:
                prior_pending.add(key)
                continue
            if kind == "entry.opened" and key:
                prior_open.add(key)
                continue
            if kind != "runtime.entry_owner_handoff_complete" or not key:
                continue
            destination = _text(payload.get("owner_destination")).lower()
            if destination == "pending_entry":
                if key in prior_pending:
                    completed.add(key)
            elif destination == "open_position":
                if key in prior_open:
                    completed.add(key)
            else:
                # Legacy records did not carry a destination.  A rejected
                # executor outcome has no exchange successor to retain.
                completed.add(key)
        return completed

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

    def has_position_claim(self, artifact: ExchangeArtifact | Any) -> bool:
        """Protect another owner's partial or malformed claim during cleanup."""
        symbol = _symbol_for_venue(artifact)
        return (_venue(artifact), symbol) in self._position_scopes or ("", symbol) in self._position_scopes

    def owners_for_positions(self, positions: Iterable[Any]) -> list[RecoveryOwner]:
        """A venue position is one aggregate per contract/direction, never two."""
        positions = list(positions)
        keys = [(_venue(p), _symbol_for_venue(p), _side(_get(p, "side", ""))) for p in positions]
        duplicates = {key for key, count in Counter(keys).items() if count > 1}
        return [
            RecoveryOwner("exchange_position", key[1], "orphan", {
                "source": "exchange_truth", "reason": "duplicate_position_scope",
            }) if key in duplicates else self.owner_for_position(position)
            for key, position in zip(keys, positions)
        ]

    def owner_for_position(self, artifact: ExchangeArtifact | Any) -> RecoveryOwner:
        venue, symbol = _venue(artifact), _symbol_for_venue(artifact)
        side = _side(_get(artifact, "side", ""))
        quantity = _float(_get(artifact, "quantity", 0.0))
        claims = self._positions_by_key.get((venue, symbol, side), [])
        ambiguous_scope = any(
            claim_symbol == symbol and (not claim_venue or (claim_venue == venue and not claim_side))
            for claim_venue, claim_symbol, claim_side in self._positions_by_key
        )
        expected = sum(q for q, _ in claims)
        evidence = {
            "source": "exchange_truth", "expected_quantity": expected,
            "observed_quantity": quantity, "expected_side": side,
            "owner_ids": sorted({owner.owner_id for _, owner in claims}),
        }
        if (claims and not ambiguous_scope and side in {"long", "short"} and isfinite(quantity) and quantity > 0 and expected > 0
                and all(isfinite(q) and q >= 0 and owner.confidence != "orphan" for q, owner in claims)
                and abs(quantity - expected) <= V1_PAIR_QUANTITY_EPSILON):
            # A pending component must retain its recovery classification even
            # when an open position also covers part of the same exchange leg.
            owner = min((owner for q, owner in claims if q > 0), key=lambda o: (
                {"pending_entry": 0, "residual_repair": 1, "open_position": 2}.get(o.owner_type, 3),
                o.owner_id,
            ))
            return RecoveryOwner(owner.owner_type, owner.owner_id, owner.confidence,
                                 {**dict(owner.evidence), **evidence, "source": owner.evidence["source"]})
        if not self.has_position_claim(artifact):
            journal_owner = self._owner_for_journal_position_fact(artifact)
            if journal_owner is not None:
                return journal_owner
        return RecoveryOwner("exchange_position", symbol, "orphan", evidence)

    def missing_position_claims(self, positions: Iterable[Any]) -> list[ExchangeArtifact]:
        """Missing legs of still-live owners; callers must prove account coverage.

        Both legs absent means V1 flat retirement, not a synthetic live position.
        Each returned artifact records observed zero separately from expected size.
        """
        observed = {
            (_venue(p), _symbol_for_venue(p), _side(_get(p, "side", "")))
            for p in positions
            if isfinite(q := _float(_get(p, "quantity"))) and q > 1e-9
        }
        live_owners = {
            (owner.owner_type, owner.owner_id)
            for key, claims in self._positions_by_key.items() if key in observed
            for _, owner in claims
        }
        missing = []
        for (venue, symbol, side), claims in self._positions_by_key.items():
            if not venue or side not in {"long", "short"} or (venue, symbol, side) in observed:
                continue
            if not any(q != 0 and (owner.owner_type, owner.owner_id) in live_owners for q, owner in claims):
                continue
            missing.append(ExchangeArtifact(
                kind="missing_position", venue=venue, symbol=symbol, side=side, quantity=0.0,
                raw={"reason": "owned_live_pair_missing_leg", "observed_quantity": 0.0,
                     "expected_quantity": sum(q for q, _ in claims),
                     "owner_ids": sorted({owner.owner_id for _, owner in claims})},
            ))
        return missing

    def _index_position(self, owner: RecoveryOwner, venue: str, symbol: str, side: str, quantity: float) -> None:
        if not symbol:
            return
        self._position_scopes.add((venue, symbol))
        claims = self._positions_by_key.setdefault((venue, symbol, side), [])
        for old_quantity, old_owner in claims:
            if (old_owner.owner_type, old_owner.owner_id) == (owner.owner_type, owner.owner_id):
                # Repeated state records are not additive evidence. Conflicting
                # versions cannot prove quantity until recovery reconciles them.
                if old_quantity != quantity:
                    claims.append((float("nan"), owner))
                return
        claims.append((quantity, owner))

    def _add_state(self, state: Any) -> None:
        open_positions = _collection(state, "open_positions")
        open_ids = {_text(_get(p, "position_id", "")) for p in open_positions}
        pending_entries = _collection(state, "pending_entries")
        pending_ids = {_text(_get(p, "pending_id", "")) for p in pending_entries}
        managed_open_ids = pending_ids | {
            _text(_get(task, "position_id", ""))
            for collection in ("pending_residual_repairs", "pending_passive_closes")
            for task in _collection(state, collection)
        }
        managed_open_ids.discard("")
        for pending in pending_entries:
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
            if not owner.owner_id or owner.owner_id not in open_ids:
                self._index_pending_entry_position_keys(owner, pending)

        for position in open_positions:
            position_id = _text(_get(position, "position_id", _get(position, "symbol", "")))
            long_quantity = _float(_get(position, "long_quantity"))
            short_quantity = _float(_get(position, "short_quantity"))
            balanced_pair = (
                isfinite(long_quantity) and isfinite(short_quantity)
                and min(long_quantity, short_quantity) > 0
                and abs(long_quantity - short_quantity) <= V1_PAIR_QUANTITY_EPSILON
            )
            owner = RecoveryOwner(
                owner_type="open_position",
                owner_id=position_id,
                confidence="proven" if balanced_pair or position_id in managed_open_ids else "orphan",
                evidence={"source": "local_open_position", "balanced_pair": balanced_pair},
            )
            for leg in ("long", "short"):
                venue = _venue_from_key(position, f"{leg}_venue")
                self._index_position(owner, venue, _symbol_for_venue(position, venue), leg,
                                     _float(_get(position, f"{leg}_quantity")))

        for residual in _collection(state, "pending_residual_repairs"):
            position_id = _text(_get(residual, "position_id", ""))
            origin = _leg_text(_get(residual, "origin", ""))
            # Pending fills already include entry residuals; remaining open
            # legs already include close residuals. Count each exposure once.
            if position_id and (
                (position_id in pending_ids and position_id not in open_ids)
                or (position_id in open_ids and origin != "entry_open")
            ):
                continue
            owner = RecoveryOwner(
                owner_type="residual_repair",
                owner_id=_text(_get(residual, "repair_id", _get(residual, "task_id", "")))
                         or position_id or _symbol(residual),
                confidence="probable",
                evidence={"source": "local_residual_repair"},
            )
            venue = _normalize_venue(_get(residual, "repair_venue") or _get(residual, "exposure_venue")
                                     or _get(residual, "venue", ""))
            close_side = _side(_get(residual, "repair_side") or _get(residual, "exposure_side", ""))
            side = {"long": "short", "short": "long"}.get(close_side, "")
            quantity = _float(_get(residual, "repair_quantity", _get(residual, "exposure_quantity")))
            self._index_position(owner, venue, _symbol_for_venue(residual, venue), side, quantity)

    def _add_journal_events(self, journal_events: Iterable[Any]) -> None:
        for event in self.active_journal_owner_events(journal_events):
            payload = _get(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            order_ids, client_order_ids = _journal_order_identifier_sets(payload)
            position_specs = _journal_position_specs(event, payload)
            if not order_ids and not client_order_ids and not position_specs:
                continue
            owner_id = _journal_owner_key(payload)
            symbol = _text(payload.get("symbol")).upper()
            owner = RecoveryOwner(
                owner_type=(
                    "journal_entry_submission"
                    if _text(_get(event, "kind", "")).lower()
                    == "runtime.entry_owner_claimed"
                    else "journal_pending_entry"
                ),
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
                order_ids=order_ids,
                client_order_ids=client_order_ids,
            )
            self._index_journal_position_facts(event, payload, owner)

    def _index_journal_position_facts(
        self,
        event: Any,
        payload: Mapping[str, Any],
        owner: RecoveryOwner,
    ) -> None:
        for venue, symbol, side, quantity in _journal_position_specs(event, payload):
            position_owner = RecoveryOwner(
                owner_type=owner.owner_type,
                owner_id=owner.owner_id,
                confidence=owner.confidence,
                evidence={
                    **dict(owner.evidence),
                    "position_scope": (
                        "journal_entry_submission"
                        if _text(_get(event, "kind", "")).lower()
                        == "runtime.entry_owner_claimed"
                        else "journal_positive_fill_live_conflict"
                    ),
                    "expected_side": side,
                    "expected_quantity": quantity,
                    "expected_venue": venue,
                },
            )
            self._journal_position_facts.append(
                (venue, symbol, side, quantity, position_owner)
            )

    def _owner_for_journal_position_fact(
        self,
        artifact: ExchangeArtifact | Any,
    ) -> RecoveryOwner | None:
        symbol = _symbol_for_venue(artifact)
        side = _side(_get(artifact, "side", ""))
        quantity = _float(_get(artifact, "quantity", 0.0))
        if not symbol or not side or not isfinite(quantity) or quantity <= 0.0:
            return None
        venue = _venue(artifact)
        if not venue:
            return None
        for fact_venue, fact_symbol, fact_side, fact_quantity, owner in reversed(
            self._journal_position_facts
        ):
            if fact_venue != venue:
                continue
            if fact_symbol != symbol:
                continue
            if fact_side != side:
                continue
            # Exchange quantity steps can normalize a submitted quantity before
            # the local handoff is persisted.  This claim is already scoped by
            # venue, canonical symbol, and side; accept only a <=1% step delta.
            if (
                not isfinite(fact_quantity)
                or fact_quantity <= 0.0
                or abs(fact_quantity - quantity) > fact_quantity * 0.01
            ):
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
        maker_fill = _float(_get(pending, "maker_leg_filled"))
        hedge_fill = _float(_get(pending, "hedge_leg_filled"))
        if maker_fill == 0 and hedge_fill == 0:
            return
        maker_leg = _leg_text(_get(pending, "maker_leg", ""))
        if maker_leg not in {"long", "short"}:
            maker_side = _leg_text(_get(pending, "maker_side", ""))
            if maker_side in {"buy", "long"}:
                maker_leg = "long"
            elif maker_side in {"sell", "short"}:
                maker_leg = "short"
        if maker_leg not in {"long", "short"}:
            for leg in ("long", "short"):
                venue = _venue_from_key(pending, f"{leg}_venue")
                self._index_position(owner, venue, _symbol_for_venue(pending, venue), "", float("nan"))
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
        if long_fill != 0.0:
            long_venue = _venue_from_key(pending, "long_venue")
            symbol = _symbol_for_venue(pending, long_venue)
            self._index_position(position_owner, long_venue, symbol, "long", long_fill)
        if short_fill != 0.0:
            short_venue = _venue_from_key(pending, "short_venue")
            symbol = _symbol_for_venue(pending, short_venue)
            self._index_position(position_owner, short_venue, symbol, "short", short_fill)


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


def _symbol_for_venue(obj: Any, venue: str | None = None) -> str:
    return canonical_symbol_from_venue(venue or _venue(obj), _symbol(obj))


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
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


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
    order_ids, client_order_ids = _journal_order_identifier_sets(payload)
    return (
        order_ids[0] if order_ids else "",
        client_order_ids[0] if client_order_ids else "",
    )


def _journal_order_identifier_sets(
    payload: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    def unique_texts(*values: Any) -> tuple[str, ...]:
        return tuple(dict.fromkeys(text for value in values if (text := _text(value))))

    order_ids = unique_texts(
        payload.get("order_id"),
        payload.get("maker_order_id"),
        payload.get("hedge_order_id"),
        payload.get("exchange_order_id"),
    )
    client_order_ids = unique_texts(
        payload.get("client_order_id"),
        payload.get("maker_client_order_id"),
        payload.get("hedge_client_order_id"),
        payload.get("clientOrderId"),
    )
    return order_ids, client_order_ids


def _journal_owner_key(payload: Mapping[str, Any]) -> str:
    return _text(
        payload.get("entry_id")
        or payload.get("pending_id")
        or payload.get("position_id")
        or payload.get("source_entry_id")
        or payload.get("internal_entry_id")
    )


def _journal_owner_identifiers(payload: Mapping[str, Any]) -> set[str]:
    return {
        value
        for field in (
            "entry_id",
            "pending_id",
            "position_id",
            "owner_id",
            "source_entry_id",
            "internal_entry_id",
        )
        if (value := _text(payload.get(field)))
    }


def _journal_position_specs(
    event: Any,
    payload: Mapping[str, Any],
) -> tuple[tuple[str, str, str, float], ...]:
    kind = _text(_get(event, "kind", "")).lower()
    outcome = _text(payload.get("outcome")).lower()
    if kind not in {
        "pending_entry.positive_fill_live_truth_conflict",
        "pending_entry.terminalizer_decision",
        "runtime.entry_owner_claimed",
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
    specs: list[tuple[str, str, str, float]] = []
    if kind == "runtime.entry_owner_claimed":
        long_venue = _normalize_venue(payload.get("long_venue"))
        short_venue = _normalize_venue(payload.get("short_venue"))
        long_quantity = _float(payload.get("long_quantity"))
        short_quantity = _float(payload.get("short_quantity"))
        long_side = _side(payload.get("long_side"))
        short_side = _side(payload.get("short_side"))
        if long_venue and long_quantity > 0.0 and long_side:
            specs.append((
                long_venue,
                canonical_symbol_from_venue(long_venue, symbol),
                long_side,
                long_quantity,
            ))
        if short_venue and short_quantity > 0.0 and short_side:
            specs.append((
                short_venue,
                canonical_symbol_from_venue(short_venue, symbol),
                short_side,
                short_quantity,
            ))
        return tuple(specs)
    # Older positive-fill journal records do not include per-leg venue truth.
    # They must not claim a same-symbol/side/quantity position on an arbitrary
    # venue after restart; exchange truth is intentionally treated as orphan.
    return tuple(specs)


def _journal_live_position_probe_evidence(
    event: Any,
    payload: Mapping[str, Any],
) -> bool:
    """Keep legacy positive-fill events for probing, never for ownership."""
    kind = _text(_get(event, "kind", "")).lower()
    if kind == "pending_entry.terminalizer_decision" and (
        _text(payload.get("outcome")).lower()
        != "positive_fill_live_truth_conflict"
    ):
        return False
    if kind not in {
        "pending_entry.positive_fill_live_truth_conflict",
        "pending_entry.terminalizer_decision",
    }:
        return False
    return (
        _float(payload.get("live_long_quantity")) > 0.0
        or _float(payload.get("live_short_quantity")) > 0.0
    )


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value or "")
