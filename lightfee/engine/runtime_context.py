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
    def l2_data_plane(self) -> Any: ...

    @property
    def ws_bbo_data_plane(self) -> Any: ...

    @property
    def entry_l2_sessions(self) -> Any: ...

    @property
    def entry_readiness_provider(self) -> Any: ...

    @property
    def ws_bbo_rest_refresher(self) -> Any: ...

    @ws_bbo_rest_refresher.setter
    def ws_bbo_rest_refresher(self, value: Any) -> None: ...

    @property
    def entry_executor(self) -> Any: ...

    @property
    def close_executor(self) -> Any: ...

    @property
    def passive_close_executor(self) -> Any: ...

    @property
    def recovery_decision(self) -> Any: ...

    @property
    def _maker_event_state(self) -> dict[str, object]: ...

    @property
    def _last_maker_event_ms(self) -> int: ...

    @_last_maker_event_ms.setter
    def _last_maker_event_ms(self, value: int) -> None: ...

    @property
    def _entry_bbo_subscription_budgeted_keys(self) -> set[tuple[str, str]]: ...

    @_entry_bbo_subscription_budgeted_keys.setter
    def _entry_bbo_subscription_budgeted_keys(
        self,
        value: set[tuple[str, str]],
    ) -> None: ...

    @property
    def _entry_bbo_subscription_budget_excluded_keys(
        self,
    ) -> set[tuple[str, str]]: ...

    @_entry_bbo_subscription_budget_excluded_keys.setter
    def _entry_bbo_subscription_budget_excluded_keys(
        self,
        value: set[tuple[str, str]],
    ) -> None: ...

    @property
    def _entry_bbo_subscription_per_venue_budget(self) -> int: ...

    @_entry_bbo_subscription_per_venue_budget.setter
    def _entry_bbo_subscription_per_venue_budget(self, value: int) -> None: ...

    @property
    def _last_snapshot_freshness_filter_blockers(self) -> Any: ...

    @_last_snapshot_freshness_filter_blockers.setter
    def _last_snapshot_freshness_filter_blockers(self, value: Any) -> None: ...

    @property
    def _last_snapshot_freshness_filter_samples(self) -> Any: ...

    @_last_snapshot_freshness_filter_samples.setter
    def _last_snapshot_freshness_filter_samples(self, value: Any) -> None: ...

    @property
    def _snapshot_freshness_decision_last_emit_ms(self) -> dict[Any, int]: ...

    @property
    def _snapshot_freshness_decision_suppressed(self) -> Any: ...

    @property
    def _SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS(self) -> int: ...

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None: ...

    def _flush_adapter_order_diagnostics(self, adapter: Any) -> None: ...

    def _refresh_runtime_market_data_config_state(self) -> None: ...

    def _entry_readiness_provider_name(self) -> str: ...

    def _local_l2_effective_enabled(self) -> bool: ...

    def _entry_readiness_provider_uses_local_l2(self) -> bool: ...

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool: ...

    def _entry_quote_lease_max_age_ms(self) -> int: ...

    def _entry_local_l2_stale_after_ms(self) -> int: ...

    async def _filter_symbols_supported_by_venue(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        symbols: list[str],
        *,
        skip_event_kind: str,
    ) -> list[str]: ...

    def _append_runtime_diagnostic_event(self, *args: Any, **kwargs: Any) -> None: ...

    def _candidate_pair_id(self, candidate: Any) -> str: ...

    def _clear_local_l2_runtime_state(self) -> None: ...

    def _record_snapshot_scoped_status(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None: ...

    def _candidate_requires_sidecar_perp_liquidity(self, candidate: Any) -> bool: ...

    def _entry_liquidity_qualification_decisions(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...

    def _liquidity_degraded_reason_blocks_symbol(
        self,
        reason: str,
        symbol: str,
    ) -> bool: ...

    def _liquidity_lifecycle_payload(self, *args: Any, **kwargs: Any) -> dict: ...

    def _select_v1_entry_tracked_scope(
        self,
        candidates: Any,
    ) -> tuple[list, list]: ...

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
