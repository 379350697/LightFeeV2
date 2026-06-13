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

    @property
    def venue_adapters(self) -> dict[Venue, VenueAdapter]: ...

    @property
    def local_l2_runtime(self) -> Any: ...

    @property
    def ws_bbo_cache(self) -> Any: ...

    @property
    def entry_executor(self) -> Any: ...

    @property
    def _maker_event_state(self) -> dict[str, object]: ...

    @property
    def _last_maker_event_ms(self) -> int: ...

    @_last_maker_event_ms.setter
    def _last_maker_event_ms(self, value: int) -> None: ...

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None: ...

    def _flush_adapter_order_diagnostics(self, adapter: Any) -> None: ...

    def _refresh_runtime_market_data_config_state(self) -> None: ...

    def _local_l2_effective_enabled(self) -> bool: ...

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool: ...

    def _entry_quote_lease_max_age_ms(self) -> int: ...

    def _refresh_entry_l2_session_readiness(self, now_ms: int) -> None: ...

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
