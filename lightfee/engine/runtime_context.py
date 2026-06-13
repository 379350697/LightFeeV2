"""Typed runtime context ports for LiveRuntime delegates."""

from __future__ import annotations

from typing import Any, Protocol

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.engine.state import EngineState
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore


class RuntimeContext(Protocol):
    @property
    def state(self) -> EngineState: ...

    @property
    def config(self) -> AppConfig: ...

    @property
    def journal(self) -> Journal: ...

    @property
    def snapshot_store(self) -> SnapshotStore: ...

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None: ...

    def _flush_adapter_order_diagnostics(self, adapter: Any) -> None: ...

    def _recovery_state_collection(self, name: str) -> list[Any]: ...

    def _venue_min_notional(self, venue: Venue, symbol: str) -> float: ...

    def _safe_positive_float(self, value: Any) -> float: ...

    def _close_reconciliation_fill_qty(self, fill: Any) -> float: ...

    def _v1_lifecycle_event_fields(
        self,
        *,
        phase: str,
        owner_id: str = "",
        row_key: str = "",
        now_ms: int | None = None,
    ) -> dict[str, str]: ...
