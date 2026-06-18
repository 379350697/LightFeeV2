"""Live runtime: multi-lane tick loop, snapshot consumption, supervision, export."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
from collections import Counter
from types import SimpleNamespace
from typing import Any, Optional

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    AccountBalanceSnapshot,
    OrderFill,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.bybit_duplicate_reconcile import (
    BYBIT_DUPLICATE_RECONCILE_ENDPOINTS,
    BybitDuplicateReconcileResult,
    build_order_reconcile_result_payload,
    reconcile_bybit_duplicate_client_order,
)
from lightfee.engine.close_executor import _is_bybit_duplicate_order_link_id
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime
from lightfee.engine.entry_gate_runtime import EntryGateRuntime
from lightfee.engine.reconciliation import _recon_fill_price
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    active_position_tick_ready,
    full_tick_ready,
    prepare_runtime_symbols,
    wall_clock_now_ms,
)
from lightfee.engine.lifecycle import (
    can_enter_new_positions,
    enter_fail_closed,
    set_lifecycle,
    transition_to_reconciling,
)
from lightfee.engine.loop_control import (
    ExportState,
    current_state_export_interval_ms,
    maybe_export_current_state_snapshot,
    maybe_export_runtime_metrics,
)
from lightfee.engine.order_truth_ledger import (
    ORDER_TRUTH_LEDGER,
    OrderTruthFillStatus,
)
from lightfee.engine.recovery import (
    recover_from_snapshot,
    build_recovery_dedup_index,
    clear_stale_fail_closed_if_recovery_clean,
    clear_legacy_recovery_block_via_core,
    build_persistent_state_view,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.recovery_startup_runtime import RecoveryStartupRuntime
from lightfee.engine.market_data_runtime import MarketDataRuntime
from lightfee.engine.passive_maker_runtime import PassiveMakerRuntime
from lightfee.engine.pending_entry_runtime import PendingEntryRuntime
from lightfee.engine.residual_repair_runtime import ResidualRepairRuntime
from lightfee.engine.unpaired_live_position_recovery import (
    UnpairedLivePositionRecoveryRuntime,
)
from lightfee.engine.venue_private_health import (
    classify_private_health_error,
    is_private_health_admission_reason,
    private_health_status_for_admission_reason,
)
from lightfee.engine.v1_lifecycle_closure import (
    build_v1_lifecycle_closure_table,
    closure_event_fields,
    entry_gate_from_closure,
)
from lightfee.engine.pending_entry_lifecycle import (
    advance_pending_entry_zero_fill_phase,
    apply_pending_entry_passive_progress,
    candidate_for_terminal_taker_fallback,
    decide_terminal_taker_fallback,
    ensure_pending_entry_phase_state,
    note_pending_entry_passive_cycle_accepted,
    note_pending_entry_remainder_repost_accepted,
    note_passive_operation,
    pending_entry_phase_zero_fill_budget,
    prepare_pending_entry_passive_cycle,
    prepare_pending_entry_remainder_repost,
    record_pending_entry_zero_fill_cycle,
    terminal_recheck_is_tradeable,
)
from lightfee.engine.pending_entry_hedge_delta import (
    PendingEntryHedgeabilityPlan,
    PendingEntryHedgeDeltaDecision,
    adaptive_entry_hedge_deadline_decision,
    decide_pending_entry_hedge_delta_pre_submit,
    note_pending_entry_hedge_filled,
    note_pending_entry_hedge_submitted,
    releasable_hedge_quantity,
)
from lightfee.engine.state import (
    EngineState,
    HedgeInflight,
    OpenPosition,
)
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment
from lightfee.sidecar.snapshot import evaluate_snapshot_freshness, SnapshotFreshness
from lightfee.sidecar.publisher import load_snapshot
from lightfee.strategy.discovery import discover_tradeable_candidates
from lightfee.venues.transport import (
    TransportErrorCategory,
    is_hyperliquid_non_retryable_auth_signing_error,
)

logger = logging.getLogger("lightfee.engine.runtime")


class _PendingEntryPassiveSubmitFinalized(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class LiveRuntime:
    """Live trading runtime with multi-lane ticks and control-plane exports."""

    _MAX_STATIC_RECOVERY_PROBE_SYMBOLS = 1
    _MAX_BOUNDED_RECOVERY_FALLBACK_SYMBOLS = 25
    _PASSIVE_POST_ONLY_LADDER_FRACTIONS = (0.0, 0.5, 0.75, 1.0)
    _PASSIVE_POST_ONLY_CLOSEST_PRICE_EXTRA_RETRIES = 2
    _PASSIVE_POST_ONLY_TIGHT_SPREAD_LADDER_FRACTIONS = (0.0, 1.0)
    _PASSIVE_POST_ONLY_BALANCED_SPREAD_LADDER_FRACTIONS = (0.0, 0.5, 1.0)
    _PASSIVE_POST_ONLY_WIDE_SPREAD_LADDER_FRACTIONS = (0.0, 0.25, 0.5, 0.75, 1.0)
    _PASSIVE_POST_ONLY_TIGHT_SPREAD_BPS = 5.0
    _PASSIVE_POST_ONLY_WIDE_SPREAD_BPS = 20.0
    _PASSIVE_POST_ONLY_RETRY_BACKOFF_MS = (500, 1000, 2000, 4000, 6000, 8000, 10000)
    _MAKER_EDGE_AWARE_FULL_AGGRESSION_HEADROOM_BPS_FLOOR = 2.0

    @property
    def venue_contracts(self) -> Any:
        from lightfee.venues import specs as venue_specs

        return venue_specs

    @property
    def order_truth(self) -> Any:
        from lightfee.engine import order_submit_uncertainty

        return order_submit_uncertainty

    @property
    def lifecycle_closure(self) -> Any:
        return SimpleNamespace(
            build=build_v1_lifecycle_closure_table,
            event_fields=closure_event_fields,
            entry_gate=entry_gate_from_closure,
        )

    @property
    def quote_truth(self) -> Any:
        return self.market_data_runtime

    @property
    def catalog_support(self) -> Any:
        return SimpleNamespace(
            filter_symbols_supported_by_venue=self._filter_symbols_supported_by_venue,
            unsupported_symbol_diagnostic_last_ms=(
                self._unsupported_symbol_diagnostic_last_ms
            ),
        )

    @property
    def exchange_truth(self) -> Any:
        from lightfee.engine.exchange_truth import (
            build_venue_operation_request,
            request_venue_operation,
        )

        return SimpleNamespace(
            collect_recovery_ledger_account_truth=(
                self._collect_recovery_ledger_account_truth
            ),
            collect_recovery_ledger_exchange_truth=(
                self._collect_recovery_ledger_exchange_truth
            ),
            build_venue_operation_request=build_venue_operation_request,
            request_venue_operation=request_venue_operation,
            last_recovery_exchange_truth=self._last_recovery_exchange_truth,
        )

    def __init__(self, config: AppConfig, venue_adapters: Optional[dict[Venue, VenueAdapter]] = None) -> None:
        self.config = config
        self.state = EngineState()
        self.journal = Journal(
            config.persistence.event_log_path,
            max_bytes=config.persistence.event_log_compaction_max_bytes,
            archive_count=config.persistence.event_log_archive_count,
            retention_hours=config.persistence.event_log_retention_hours,
        )
        self.snapshot_store = SnapshotStore(config.persistence.snapshot_path)
        self.supervisor = Supervisor(config, self.state, self.journal)
        self._running = False
        self._export_state = ExportState()
        self._venue_adapters = venue_adapters or {}
        self.residual_repair_runtime = ResidualRepairRuntime(self)
        self.pending_entry_runtime = PendingEntryRuntime(self)
        self.recovery_startup_runtime = RecoveryStartupRuntime(self)
        self.unpaired_live_position_recovery_runtime = (
            UnpairedLivePositionRecoveryRuntime(self)
        )

        # Tick-failure backoff deadlines (ms since epoch). None = no backoff active.
        self._tick_backoff_until_ms: Optional[int] = None
        self._active_tick_backoff_until_ms: Optional[int] = None
        self._maker_tick_backoff_until_ms: Optional[int] = None

        # V1 entry executor — set after construction or defaults to None
        self.entry_executor: Optional[object] = None
        # V1 close executor — set after construction or defaults to None
        self.close_executor: Optional[object] = None
        # V1 passive close executor — set after construction or defaults to None
        self.passive_close_executor: Optional[object] = None
        # V1 reconciliation service — set after construction or defaults to None
        self.reconciler: Optional[object] = None
        self.close_runtime = CloseRuntime(self)
        # V1 rate-limit runtime for periodic reload
        self._rate_limit_runtime: Optional[object] = None
        # V1 rate-limit reload tracking
        self._last_rate_limit_reload_ms: int = 0

        # V1 private WS tracking: each venue gets workers started once.
        # Tracked per venue to handle reconfiguration gracefully.
        self._private_ws_started: set[Venue] = set()
        self._private_ws_symbols: dict[Venue, set[str]] = {}

        # V1 per-venue risk snapshot runtime cache
        #   key: venue → {fetched_at_ms, result: OK(Optional[ARS]) | Err(str)}
        self._risk_snapshot_cache: dict[Venue, dict] = {}
        self._entry_balance_snapshot_cache: dict[Venue, dict] = {}

        # V1 maker-event lane state
        #   Tracks pending passive maker entries with last known price for repricing.
        #   Values are either dicts (sidecar path) or (PassiveOrderManager, float) tuples
        #   (local-L2 parity path).
        self._maker_event_state: dict[str, object] = {}  # entry_id -> dict | (manager, price)
        self._last_maker_event_ms: int = 0

        # V1 local-L2 runtime (data-plane: book, assignment, events, metrics)
        from lightfee.marketdata.local_l2_runtime import LocalL2Runtime

        budget = config.strategy.local_l2_resource_budget()
        self.local_l2_runtime = LocalL2Runtime(
            max_hot_exec=budget["reserved_hot_global"],
            max_warm=budget["warm_global"],
        )

        # V1 local-L2 data plane (REST snapshot bootstrap + WS streaming)
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        self.l2_data_plane = LocalL2DataPlane(
            l2_runtime=self.local_l2_runtime,
            journal=self.journal,
        )
        self.l2_data_plane.hot_stale_after_ms = self._configured_entry_l2_stale_after_ms(config)

        from lightfee.marketdata.ws_bbo import VenueBboCache, VenueBboDataPlane
        self.ws_bbo_cache = VenueBboCache()
        self.ws_bbo_data_plane = VenueBboDataPlane(
            cache=self.ws_bbo_cache,
            journal=self.journal,
        )
        self.passive_maker_runtime = PassiveMakerRuntime(self)
        self._entry_bbo_subscription_budgeted_keys: set[tuple[str, str]] = set()
        self._entry_bbo_subscription_budget_excluded_keys: set[tuple[str, str]] = set()
        self._entry_bbo_subscription_per_venue_budget: int = 0
        self._entry_bbo_sticky_warm_until_ms: dict[tuple[str, str], int] = {}

        # V1 entry-local-L2 session runtime (tracked opportunities, readiness)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionRuntime
        self.entry_l2_sessions = EntryLocalL2SessionRuntime()
        from lightfee.engine.entry_readiness import build_entry_readiness_provider
        self.entry_readiness_provider = build_entry_readiness_provider(self)
        self.market_data_runtime = MarketDataRuntime(self)
        self.entry_gate_runtime = EntryGateRuntime(self)
        self.entry_dispatch_runtime = EntryDispatchRuntime(self)
        self._refresh_runtime_market_data_config_state()
        self._tracked_primary_pair_ids: set[str] = set()  # V1: primary_opportunities
        self._entry_l2_last_leg_diagnostics: dict[tuple[str, str], dict] = {}
        self._last_entry_l2_readiness_diag_fingerprint: str = ""
        self._last_entry_l2_readiness_diag_ts_ms: int = 0
        self._last_no_entry_diag_fingerprint: str = ""
        self._last_no_entry_diag_ts_ms: int = 0
        self._last_no_entry_full_diag_ts_ms: int = 0
        self._last_no_entry_full_diag_reason: str = ""
        self._last_no_entry_summary_fingerprint: str = ""
        self._no_entry_suppressed_full_payload_count: int = 0
        self._last_no_entry_diagnostics: dict | None = None
        self._last_candidate_catalog_filter_blockers: Counter[str] = Counter()
        self._last_candidate_catalog_filter_samples: list[dict] = []
        self._last_entry_admission_filter_blockers: Counter[str] = Counter()
        self._last_entry_admission_filter_samples: list[dict] = []
        self._last_snapshot_freshness_filter_blockers: Counter[str] = Counter()
        self._last_snapshot_freshness_filter_samples: list[dict] = []
        self._snapshot_freshness_decision_last_emit_ms: dict[tuple[str, str, str, str], int] = {}
        self._snapshot_freshness_decision_suppressed: Counter[tuple[str, str, str, str]] = Counter()
        self._runtime_diagnostic_event_last_emit_ms: dict[tuple[str, ...], int] = {}
        self._runtime_diagnostic_event_suppressed: Counter[tuple[str, ...]] = Counter()
        self._last_private_position_probe_ms: int = 0
        self._unsupported_symbol_diagnostic_last_ms: dict[tuple[str, str], int] = {}
        self._last_position_drift_check_ms: int = 0
        self._symbol_admission_blocked_until_ms: dict[tuple[str, str], int] = {}

        # V1 recovery dedup index: prevents duplicate orders after restart
        self._recovery_dedup_index: dict[str, str] = {}
        self.recovery_ledger: RecoveryLedger | None = None
        self._last_recovery_exchange_truth: dict[str, Any] | None = None

        # V1 entry gate cooldown state
        self._venue_cooldown_until_ms: dict[str, int] = {}
        self._zero_fill_cooldown_until_ms: dict[tuple, int] = {}
        self._post_only_reject_cooldown_until_ms: dict[tuple[str, str], int] = {}

        # V1 live scan recovery state (B2)
        self._live_scan_success_streak: int = 0
        self._last_good_snapshot = None

        # V1 maker venue request budget tracker (CONTRACT RECOVERY-005)
        # Per-venue sliding window of operation timestamps for cancel/submit
        # rate limiting. V1: try_consume_maker_venue_request_budget
        self._maker_venue_op_history: dict[str, list[int]] = {}
        self._maker_venue_request_budget_frozen_until_ms: dict[str, int] = {}

    # V1 risk snapshot TTL constants (Rust: execution_core/engine.rs:127, risk.rs:12)
    _RISK_SNAPSHOT_TTL_MS_DEFAULT = 1_000
    _RISK_SNAPSHOT_TTL_MS_ASTER = 30_000  # Aster lacks WS, avoid REST polling
    _UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS = 60_000
    _SYMBOL_ADMISSION_BLOCK_TTL_MS = 6 * 60 * 60 * 1000
    _SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS = 60_000
    _ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS = 60_000
    _NO_ENTRY_DIAGNOSTICS_COMPACT_INTERVAL_MS = 60_000
    _NO_ENTRY_DIAGNOSTICS_FULL_INTERVAL_MS = 5 * 60_000
    _ENTRY_BLOCKED_LOCAL_L2_SELECTION_LOG_INTERVAL_MS = 60_000
    _CANDIDATE_SYMBOL_SKIPPED_LOG_INTERVAL_MS = 60_000
    _V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS = frozenset({
        "entry_blocked_first_funding_too_close",
        "entry_blocked_first_funding_missing",
        "entry_blocked_recovery_ledger",
    })
    _BYBIT_ERROR_DOC_URL = "https://bybit-exchange.github.io/docs/v5/error"
    _BINANCE_USDM_ERROR_DOC_URL = (
        "https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code"
    )
    _HYPERLIQUID_ERROR_DOC_URL = (
        "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
    )
    _HYPERLIQUID_INFO_DOC_URL = (
        "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint"
    )
    _ASTER_API_DOC_URL = "https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation"
    _ASTER_OPENABLE_NOTIONAL_DOC_URL = (
        "https://asterdex.github.io/aster-api-website/futures/account%26trades/"
        "#remaining-openable-notional-value-user_data"
    )

    @staticmethod
    def _risk_snapshot_ttl_ms(venue: Venue) -> int:
        if venue == Venue.ASTER:
            return LiveRuntime._RISK_SNAPSHOT_TTL_MS_ASTER
        return LiveRuntime._RISK_SNAPSHOT_TTL_MS_DEFAULT

    @staticmethod
    def _entry_admission_reject_reason(venue: Venue, reason: str) -> str | None:
        metadata = LiveRuntime._entry_admission_reject_metadata(venue, reason)
        return str(metadata["reason"]) if metadata else None

    @staticmethod
    def _entry_admission_reject_metadata(venue: Venue, reason: str) -> dict | None:
        text = str(reason or "").lower()
        private_health = classify_private_health_error(venue, reason)
        if private_health is not None:
            private_health_reason, _status = private_health
            return LiveRuntime._entry_admission_evidence(private_health_reason)
        if venue == Venue.BYBIT and (
            "110007" in text
            or "available balance is insufficient" in text
            or "insufficient available balance" in text
        ):
            return LiveRuntime._entry_admission_evidence(
                "insufficient_balance_admission_blocked"
            )
        if venue == Venue.BYBIT and (
            "110126" in text
            or "must sign required agreement" in text
        ):
            return LiveRuntime._entry_admission_evidence("bybit_trading_terms_required")
        if venue == Venue.BYBIT and (
            "110125" in text
            or "110123" in text
            or "agree to the trading terms" in text
        ):
            return {
                **LiveRuntime._entry_admission_evidence("bybit_trading_terms_required"),
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if venue == Venue.BYBIT and (
            "30228" in text
            or "110023" in text
            or "110042" in text
            or "110137" in text
            or "no new positions during delisting" in text
            or ("delivery" in text and "reduce" in text)
            or "only reduce-only" in text
        ):
            return LiveRuntime._entry_admission_evidence("new_position_not_allowed")
        if venue == Venue.BINANCE and (
            "-2019" in text
            or "margin is insufficient" in text
        ):
            return LiveRuntime._entry_admission_evidence(
                "insufficient_margin_admission_blocked"
            )
        if venue == Venue.HYPERLIQUID and (
            "insufficient margin" in text
            or "perpmarginrejected" in text
            or "insufficientspotbalancerejected" in text
        ):
            return {
                "reason": "insufficient_margin_admission_blocked",
                "official_doc_url": LiveRuntime._HYPERLIQUID_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if venue == Venue.BINANCE and (
            "-2027" in text
            or "max_leverage_ratio" in text
            or "maximum allowable position at current leverage" in text
        ):
            return {
                "reason": "leverage_admission_blocked",
                "official_doc_url": LiveRuntime._BINANCE_USDM_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if venue == Venue.BINANCE and LiveRuntime._entry_reject_is_post_only_would_take(reason):
            return LiveRuntime._entry_admission_evidence("post_only_would_take")
        if venue == Venue.ASTER and (
            "-2027" in text
            or "max_leverage_ratio" in text
            or "maximum allowable position at current leverage" in text
        ):
            return LiveRuntime._entry_admission_evidence("leverage_admission_blocked")
        if venue == Venue.ASTER and (
            "-5018" in text
            or "maximum notional value limit" in text
            or "max notional" in text
        ):
            return LiveRuntime._entry_admission_evidence("max_notional_admission_blocked")
        if venue == Venue.ASTER and "aster_headroom_unavailable" in text:
            return LiveRuntime._entry_admission_evidence("aster_headroom_unavailable")
        return None

    @staticmethod
    def _entry_admission_evidence(reason: str) -> dict:
        if reason == "insufficient_balance_admission_blocked":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "bybit_trading_terms_required":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "new_position_not_allowed":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason in ("venue_auth_invalid", "venue_permission_denied"):
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason in (
            "venue_private_truth_unavailable",
            "venue_order_permission_unavailable",
        ):
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BYBIT_ERROR_DOC_URL,
                "evidence_gap": True,
            }
        if reason in ("insufficient_margin_admission_blocked", "post_only_would_take"):
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._BINANCE_USDM_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "insufficient_margin_admission_prefiltered":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._HYPERLIQUID_ERROR_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "hyperliquid_account_balance_unavailable":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._HYPERLIQUID_INFO_DOC_URL,
                "evidence_gap": True,
            }
        if reason == "leverage_admission_blocked":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._ASTER_API_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "max_notional_admission_blocked":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._ASTER_OPENABLE_NOTIONAL_DOC_URL,
                "evidence_gap": False,
            }
        if reason == "aster_headroom_unavailable":
            return {
                "reason": reason,
                "official_doc_url": LiveRuntime._ASTER_OPENABLE_NOTIONAL_DOC_URL,
                "evidence_gap": True,
            }
        return {"reason": reason, "official_doc_url": "", "evidence_gap": True}

    @staticmethod
    def _entry_admission_block_state_keys(venue: Venue, symbol: str, reason: str) -> list[str]:
        keys = [f"{venue.value}:{symbol}"]
        if is_private_health_admission_reason(reason):
            if symbol == "*":
                return [f"{venue.value}:*"]
            keys.append(f"{venue.value}:*")
        if venue == Venue.ASTER and reason in {
            "max_notional_admission_blocked",
            "aster_headroom_unavailable",
        }:
            keys.append(f"{venue.value}:*")
        if venue == Venue.HYPERLIQUID and reason in {
            "insufficient_margin_admission_blocked",
            "insufficient_margin_admission_prefiltered",
        }:
            if symbol == "*":
                return [f"{venue.value}:*"]
            keys.append(f"{venue.value}:*")
        return keys

    @staticmethod
    def _candidate_uses_venue(candidate: Any, venue: Venue) -> bool:
        target = venue.value
        return (
            str(getattr(candidate, "long_venue", "") or "").lower() == target
            or str(getattr(candidate, "short_venue", "") or "").lower() == target
        )

    def _entry_admission_margin_buffer_bps(self) -> float:
        strategy = self.config.strategy
        buffer_bps = 0.0
        for attr in (
            "execution_buffer_bps",
            "capital_buffer_bps",
            "entry_exit_reserve_bps",
        ):
            try:
                buffer_bps += float(getattr(strategy, attr, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
        return max(buffer_bps, 0.0)

    def _hyperliquid_entry_required_initial_margin_quote(
        self,
        entry_notional_quote: float,
    ) -> float:
        try:
            leverage = float(
                getattr(self.config.strategy, "live_target_leverage", 1.0) or 1.0
            )
        except (TypeError, ValueError):
            leverage = 1.0
        leverage = max(leverage, 1.0)
        try:
            notional = float(entry_notional_quote or 0.0)
        except (TypeError, ValueError):
            notional = 0.0
        if not math.isfinite(notional) or notional <= 0.0:
            return 0.0
        buffer_multiplier = 1.0 + (self._entry_admission_margin_buffer_bps() / 10_000.0)
        return max(notional / leverage, 0.0) * buffer_multiplier

    def _cached_entry_balance_snapshot(self, venue: Venue, now_ms: int):
        entry = self._entry_balance_snapshot_cache.get(venue)
        if entry is None:
            return None, False
        fetched_at = int(entry.get("fetched_at_ms", 0) or 0)
        ttl = self._RISK_SNAPSHOT_TTL_MS_DEFAULT
        if ttl <= 0 or (now_ms - fetched_at) > ttl:
            return None, False
        return entry.get("result"), True

    def _store_entry_balance_snapshot(self, venue: Venue, now_ms: int, result) -> None:
        self._entry_balance_snapshot_cache[venue] = {
            "fetched_at_ms": now_ms,
            "result": result,
        }

    async def _fetch_hyperliquid_entry_balance_snapshot(
        self,
        now_ms: int,
    ) -> tuple[AccountBalanceSnapshot | None, str | None]:
        return await self.entry_gate_runtime._fetch_hyperliquid_entry_balance_snapshot(
            now_ms
        )

    def _hyperliquid_balance_block_sample(
        self,
        *,
        candidate: Any | None,
        reason: str,
        now_ms: int,
        stage: str,
        source: str,
        available_balance_quote: float | None,
        required_initial_margin_quote: float,
        entry_notional_quote: float,
        raw_error: str = "",
        balance_classification: str = "",
        user_abstraction: str = "",
        spot_usdc_available: float | None = None,
    ) -> dict:
        return self.entry_gate_runtime._hyperliquid_balance_block_sample(
            candidate=candidate,
            reason=reason,
            now_ms=now_ms,
            stage=stage,
            source=source,
            available_balance_quote=available_balance_quote,
            required_initial_margin_quote=required_initial_margin_quote,
            entry_notional_quote=entry_notional_quote,
            raw_error=raw_error,
            balance_classification=balance_classification,
            user_abstraction=user_abstraction,
            spot_usdc_available=spot_usdc_available,
        )

    def _append_hyperliquid_balance_unavailable_event(
        self,
        *,
        now_ms: int,
        stage: str,
        source: str,
        raw_error: str,
        candidate_count: int,
        blocked_count: int,
        allowed_count: int,
        samples: list[dict],
    ) -> None:
        return self.entry_gate_runtime._append_hyperliquid_balance_unavailable_event(
            now_ms=now_ms,
            stage=stage,
            source=source,
            raw_error=raw_error,
            candidate_count=candidate_count,
            blocked_count=blocked_count,
            allowed_count=allowed_count,
            samples=samples,
        )

    async def _refresh_hyperliquid_entry_balance_admission(self, now_ms: int) -> bool:
        return await self.entry_gate_runtime._refresh_hyperliquid_entry_balance_admission(
            now_ms
        )

    async def _filter_candidates_by_entry_balance_admission(
        self,
        candidates: list,
        *,
        now_ms: int,
        stage: str,
    ) -> list:
        return await self.entry_gate_runtime._filter_candidates_by_entry_balance_admission(
            candidates,
            now_ms=now_ms,
            stage=stage,
        )

    def _candidate_admission_block(self, candidate, now_ms: int) -> dict | None:
        return self.entry_gate_runtime._candidate_admission_block(candidate, now_ms)

    def _filter_candidates_by_entry_admission(
        self,
        candidates: list,
        *,
        now_ms: int,
        stage: str,
    ) -> list:
        return self.entry_gate_runtime._filter_candidates_by_entry_admission(
            candidates,
            now_ms=now_ms,
            stage=stage,
        )

    def _record_symbol_admission_block(
        self,
        *,
        venue: Venue,
        symbol: str,
        reason: str,
        raw_error: str,
        now_ms: int,
        evidence: dict | None = None,
        source: str = "initial_entry",
        candidate_pair_id: str = "",
        extra_payload: dict | None = None,
    ) -> None:
        until_ms = now_ms + self._SYMBOL_ADMISSION_BLOCK_TTL_MS
        key = (venue.value, symbol)
        evidence = dict(evidence or self._entry_admission_evidence(reason))
        extra_payload = dict(extra_payload or {})
        state_keys = self._entry_admission_block_state_keys(venue, symbol, reason)
        is_venue_scope = f"{venue.value}:*" in state_keys
        if is_venue_scope:
            extra_payload.setdefault("cooldown_scope", "venue")
        self._symbol_admission_blocked_until_ms[key] = until_ms
        base_payload = {
            "venue": venue.value,
            "symbol": symbol,
            "reason": reason,
            "blocked_until_ms": until_ms,
            "ttl_ms": self._SYMBOL_ADMISSION_BLOCK_TTL_MS,
            "raw_error": raw_error[:500],
            "official_doc_url": evidence["official_doc_url"],
            "evidence_gap": evidence["evidence_gap"],
            "source": source,
        }
        if candidate_pair_id:
            base_payload["candidate_pair_id"] = candidate_pair_id
            base_payload["pair_id"] = candidate_pair_id
        base_payload.update(extra_payload)
        for state_key in state_keys:
            payload = dict(base_payload)
            if state_key.endswith(":*"):
                payload["symbol"] = "*"
                payload["blocked_symbol"] = symbol
                payload["block_scope"] = "venue"
            else:
                payload["block_scope"] = "symbol"
            self.state.venue_entry_cooldowns[state_key] = payload
        self.journal.append(
            "runtime.entry_admission_blocked",
            {
                **base_payload,
                "block_scope": "venue" if is_venue_scope else "symbol",
                "ts_ms": now_ms,
            },
        )
        if is_venue_scope:
            self.journal.append(
                "runtime.venue_cooldown_started",
                {
                    "venue": venue.value,
                    "reason": (
                        "aster_max_notional_limit"
                        if venue == Venue.ASTER and reason == "max_notional_admission_blocked"
                        else reason
                    ),
                    "blocked_until_ms": until_ms,
                    "ttl_ms": self._SYMBOL_ADMISSION_BLOCK_TTL_MS,
                    "raw_error": raw_error[:500],
                    "official_doc_url": evidence["official_doc_url"],
                    "evidence_gap": evidence["evidence_gap"],
                    "source": source,
                    "symbol": symbol,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                    **extra_payload,
                    "ts_ms": now_ms,
                },
            )
        return None

    async def _recover_pending_hedge_private_health_single_leg(
        self,
        *,
        pending,
        entry_id: str,
        hedge_venue: Venue,
        reason: str,
        error_text: str,
        now_ms: int,
    ) -> bool:
        pending.hedge_inflight = None
        pending.repair_state = f"single_leg_exposure_recovery:{reason}"
        set_lifecycle(self.state, EngineLifecycle.RISK_ONLY)
        self.state.recovery_blocked_reason = "single_leg_exposure_recovery"
        self.state.recovery_blocked_at_ms = now_ms

        live_truth = await self._pending_entry_positive_fill_live_truth(
            pending,
            entry_id,
            now_ms,
        )
        cleanup_venue = ""
        eps = 1e-9
        if max(float(live_truth.live_long_quantity or 0.0), 0.0) > eps:
            cleanup_venue = pending.long_venue.value
        elif max(float(live_truth.live_short_quantity or 0.0), 0.0) > eps:
            cleanup_venue = pending.short_venue.value

        self.journal.append(
            "pending_entry.single_leg_exposure_recovery_started",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "failed_hedge_venue": hedge_venue.value,
                "cleanup_venue": cleanup_venue,
                "reason": reason,
                "source": "pending_hedge",
                "raw_error": error_text[:500],
                "live_truth_available": live_truth.available,
                "has_live_open_order": live_truth.has_live_open_order,
                "has_live_position": live_truth.has_live_position,
                "live_long_quantity": live_truth.live_long_quantity,
                "live_short_quantity": live_truth.live_short_quantity,
                "ts_ms": now_ms,
            },
        )

        if not live_truth.available:
            self.journal.append(
                "pending_entry.single_leg_flatten_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "failed_hedge_venue": hedge_venue.value,
                    "cleanup_venue": cleanup_venue,
                    "reason": "single_leg_truth_unavailable",
                    "source": "pending_hedge",
                    "raw_error": live_truth.error or error_text[:500],
                    "ts_ms": now_ms,
                },
            )
            return False
        if live_truth.has_live_open_order:
            self.journal.append(
                "pending_entry.single_leg_flatten_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "failed_hedge_venue": hedge_venue.value,
                    "cleanup_venue": cleanup_venue,
                    "reason": "single_leg_open_order_truth_present",
                    "source": "pending_hedge",
                    "ts_ms": now_ms,
                },
            )
            return False
        if not live_truth.has_live_position:
            await self._complete_pending_entry_terminal_removal(
                entry_id,
                reason="single_leg_exposure_recovery_already_flat",
                symbol=pending.symbol,
                now_ms=now_ms,
            )
            self.journal.append(
                "pending_entry.terminalized_after_single_leg_recovery",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "failed_hedge_venue": hedge_venue.value,
                    "cleanup_venue": cleanup_venue,
                    "reason": "already_flat",
                    "ts_ms": now_ms,
                },
            )
            return True

        recovered = await self.pending_entry_runtime._cleanup_owned_single_leg_live_truth_conflict(
            pending=pending,
            entry_id=entry_id,
            live_long_quantity=live_truth.live_long_quantity,
            live_short_quantity=live_truth.live_short_quantity,
            matched_quantity=min(
                float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
                float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0),
            ),
            reason="single_leg_exposure_recovery",
            now_ms=now_ms,
        )
        if not recovered:
            self.state.recovery_blocked_reason = "single_leg_exposure_recovery"
            self.state.recovery_blocked_at_ms = now_ms
        return recovered

    async def _handle_pending_hedge_admission_reject(
        self,
        *,
        pending,
        entry_id: str,
        hedge_venue: Venue,
        error_text: str,
        hedge_client_order_id: str,
        hedge_attempt: int,
        now_ms: int,
    ) -> bool:
        metadata = self._entry_admission_reject_metadata(hedge_venue, error_text)
        if not metadata:
            return False

        reason = str(metadata["reason"])
        private_health_status = private_health_status_for_admission_reason(reason)
        candidate_pair_id = self._pending_entry_pair_id(pending)
        block_scope = (
            "venue"
            if f"{hedge_venue.value}:*" in self._entry_admission_block_state_keys(
                hedge_venue,
                pending.symbol,
                reason,
            )
            else "symbol"
        )
        source = "pending_hedge"
        extra_payload: dict[str, Any] = {}
        if private_health_status:
            extra_payload.update(
                {
                    "venue_private_health_status": private_health_status,
                    "cooldown_scope": "venue",
                    "reduce_only": False,
                    "order_role": "hedge",
                    "cleanup_action": "single_leg_exposure_recovery",
                }
            )
        blocked_until_ms = now_ms + self._SYMBOL_ADMISSION_BLOCK_TTL_MS
        self._record_symbol_admission_block(
            venue=hedge_venue,
            symbol=pending.symbol,
            reason=reason,
            raw_error=error_text,
            now_ms=now_ms,
            evidence=metadata,
            source=source,
            candidate_pair_id=candidate_pair_id,
            extra_payload=extra_payload,
        )
        pending.hedge_inflight = None
        pending.repair_state = (
            f"single_leg_exposure_recovery:{reason}"
            if private_health_status
            else f"hedge_admission_blocked:{reason}"
        )
        self.journal.append(
            "pending_entry.hedge_admission_blocked",
            {
                "venue": hedge_venue.value,
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "hedge_venue": hedge_venue.value,
                "hedge_client_order_id": hedge_client_order_id,
                "hedge_attempt": hedge_attempt,
                "reason": reason,
                "source": source,
                "block_scope": block_scope,
                "blocked_until_ms": blocked_until_ms,
                "ttl_ms": self._SYMBOL_ADMISSION_BLOCK_TTL_MS,
                "candidate_pair_id": candidate_pair_id,
                "pair_id": candidate_pair_id,
                "raw_error": error_text[:500],
                "official_doc_url": metadata["official_doc_url"],
                "evidence_gap": metadata["evidence_gap"],
                **extra_payload,
                "ts_ms": now_ms,
            },
        )
        if private_health_status:
            await self._recover_pending_hedge_private_health_single_leg(
                pending=pending,
                entry_id=entry_id,
                hedge_venue=hedge_venue,
                reason=reason,
                error_text=error_text,
                now_ms=now_ms,
            )
            return True
        await self._abort_pending_entry(
            pending,
            entry_id,
            f"hedge_admission_blocked:{reason}",
        )
        return True

    def _record_entry_result_admission_blocks(
        self,
        candidate,
        reject_reason: str,
        now_ms: int,
    ) -> None:
        return self.entry_dispatch_runtime._record_entry_result_admission_blocks(
            candidate,
            reject_reason,
            now_ms,
        )

    async def _precheck_bybit_entry_admission(
        self,
        *,
        candidate,
        now_ms: int,
        long_venue: Venue,
        short_venue: Venue,
        quantity: float,
        long_order_price_hint: float,
        short_order_price_hint: float,
        maker_venue: Venue,
        entry_type,
        maker_client_order_id: str,
        hedge_client_order_id: str,
    ) -> bool:
        return await self.entry_dispatch_runtime._precheck_bybit_entry_admission(
            candidate=candidate,
            now_ms=now_ms,
            long_venue=long_venue,
            short_venue=short_venue,
            quantity=quantity,
            long_order_price_hint=long_order_price_hint,
            short_order_price_hint=short_order_price_hint,
            maker_venue=maker_venue,
            entry_type=entry_type,
            maker_client_order_id=maker_client_order_id,
            hedge_client_order_id=hedge_client_order_id,
        )
    def get_venue_adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._venue_adapters.get(venue)

    @property
    def venue_adapters(self) -> dict[Venue, VenueAdapter]:
        return self._venue_adapters

    def get_venue_adapters(self) -> dict[Venue, VenueAdapter]:
        return dict(self._venue_adapters)

    def _entry_readiness_provider_name(self) -> str:
        return str(
            getattr(self.config.strategy, "entry_readiness_provider", "local_l2")
            or "local_l2"
        ).strip().lower()

    def _entry_readiness_provider_uses_local_l2(self) -> bool:
        return self._entry_readiness_provider_name() in {"local_l2", "ws_top_book"}

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self._entry_readiness_provider_name() == "ws_bbo_quote_lease"

    def _entry_readiness_provider_uses_quote_lease(self) -> bool:
        return self._entry_readiness_provider_name() in {
            "quote_lease",
            "ws_top_book",
            "ws_bbo_quote_lease",
        }

    def _local_l2_effective_enabled(self) -> bool:
        """Whether Local-L2 data plane is effective for this runtime profile."""
        return (
            bool(getattr(self.config.strategy, "local_l2_enabled", False))
            and self._entry_readiness_provider_uses_local_l2()
        )

    def _runtime_market_data_config_summary(self):
        return self.market_data_runtime._runtime_market_data_config_summary()

    def _refresh_runtime_market_data_config_state(self):
        return self.market_data_runtime._refresh_runtime_market_data_config_state()

    def _refresh_v1_lifecycle_closure_state(
        self,
        now_ms: int,
        *,
        recovery_ledger: RecoveryLedger | None = None,
        recovery_decision: Any | None = None,
    ) -> None:
        previous = dict(getattr(self.state, "v1_lifecycle_closure", {}) or {})
        ledger = (
            recovery_ledger
            if recovery_ledger is not None
            else getattr(self, "recovery_ledger", None)
        )
        exchange_truth = self._last_recovery_exchange_truth
        if exchange_truth is None and ledger is not None:
            exchange_truth = {
                "truth_available": bool(getattr(ledger, "truth_available", False))
            }
        self.state.v1_lifecycle_closure = build_v1_lifecycle_closure_table(
            local_state=self.state,
            exchange_truth=exchange_truth,
            generated_at_ms=now_ms,
            previous_table=previous,
            recovery_ledger=ledger,
            recovery_decision=(
                recovery_decision
                if recovery_decision is not None
                else getattr(self, "recovery_decision", None)
            ),
        ).to_dict()

    def _current_v1_lifecycle_closure(
        self,
        now_ms: int | None = None,
        *,
        recovery_ledger: RecoveryLedger | None = None,
        recovery_decision: Any | None = None,
    ) -> dict[str, Any]:
        self._refresh_v1_lifecycle_closure_state(
            now_ms if now_ms is not None else wall_clock_now_ms(),
            recovery_ledger=recovery_ledger,
            recovery_decision=recovery_decision,
        )
        return dict(getattr(self.state, "v1_lifecycle_closure", {}) or {})

    def _v1_lifecycle_entry_gate_decision(self) -> tuple[bool, str]:
        return entry_gate_from_closure(self._current_v1_lifecycle_closure())

    def _v1_lifecycle_closure_blocks_candidate(
        self,
        candidate: Any,
        *,
        reason: str,
    ) -> bool:
        closure = dict(getattr(self.state, "v1_lifecycle_closure", {}) or {})
        rows = [
            row
            for row in closure.get("rows", [])
            if isinstance(row, dict)
        ]
        blocking_row_keys = set(
            str(row_key or "")
            for row_key in (closure.get("summary", {}) or {}).get("blocking_rows", [])
        )
        blocking_rows = [
            row
            for row in rows
            if (
                str(row.get("row_key") or "") in blocking_row_keys
                or str(row.get("entry_policy") or "").startswith("block")
                or str(row.get("recovery_policy") or "").startswith("block")
            )
        ]
        blocking_rows = [
            row
            for row in blocking_rows
            if not self._v1_lifecycle_row_is_candidate_owner(row, candidate)
        ]
        if not blocking_rows:
            return False
        if reason in {
            "orphan_maker_order",
            "unpaired_live_position",
            "owned_pending_entry_live_conflict",
        }:
            return True
        phases = {str(row.get("phase") or "") for row in blocking_rows}
        if (
            reason == "truth_unavailable_for_required_recovery"
            and "PENDING_ENTRY" in phases
        ):
            return True
        return any(
            self._v1_lifecycle_row_conflicts_with_candidate(row, candidate)
            for row in blocking_rows
            if str(row.get("phase") or "") != "RECOVERY_TRUTH"
        )

    @staticmethod
    def _v1_lifecycle_row_is_candidate_owner(
        row: dict[str, Any],
        candidate: Any,
    ) -> bool:
        if str(row.get("phase") or "") != "PENDING_ENTRY":
            return False
        row_owner = str(row.get("owner_id") or "")
        if not row_owner:
            return False
        owner_ids = {
            str(value or "")
            for value in (
                getattr(candidate, "pending_owner_id", ""),
                getattr(candidate, "pending_id", ""),
                getattr(candidate, "entry_id", ""),
                getattr(candidate, "position_id", ""),
            )
            if str(value or "")
        }
        return row_owner in owner_ids

    @staticmethod
    def _v1_lifecycle_row_conflicts_with_candidate(
        row: dict[str, Any],
        candidate: Any,
    ) -> bool:
        details = row.get("details") if isinstance(row.get("details"), dict) else {}
        entry_policy = str(row.get("entry_policy") or "")
        if entry_policy == "block_all_new_risk":
            return True
        candidate_symbol = str(getattr(candidate, "symbol", "") or "").upper()
        row_symbol = str(details.get("symbol") or "").upper()
        if candidate_symbol and row_symbol and candidate_symbol != row_symbol:
            return False
        candidate_venues = {
            str(value.value if hasattr(value, "value") else value or "").lower()
            for value in (
                getattr(candidate, "venue", ""),
                getattr(candidate, "long_venue", ""),
                getattr(candidate, "short_venue", ""),
            )
            if str(value.value if hasattr(value, "value") else value or "")
        }
        raw_venues = details.get("venues") or ()
        if isinstance(raw_venues, str):
            raw_venues = (raw_venues,)
        row_venues = {
            str(value.value if hasattr(value, "value") else value or "").lower()
            for value in raw_venues
            if str(value.value if hasattr(value, "value") else value or "")
        }
        if candidate_venues and row_venues:
            return bool(candidate_venues & row_venues)
        return bool(candidate_symbol and row_symbol and candidate_symbol == row_symbol)

    @staticmethod
    def _v1_lifecycle_runtime_gate_reason(reason: str) -> str:
        if reason in {
            "orphan_maker_order",
            "unpaired_live_position",
            "owned_pending_entry_live_conflict",
            "owned_recovery_work",
            "pending_residual_repair",
            "pending_passive_close",
            "owned_pending_entry",
            "truth_unavailable_for_required_recovery",
            "exchange_truth_recovery_ledger_blocked",
        }:
            return "recovery_ledger_blocked"
        return reason or "v1_lifecycle_closure_blocked"

    def _v1_lifecycle_event_fields(
        self,
        *,
        phase: str,
        owner_id: str = "",
        row_key: str = "",
        now_ms: int | None = None,
    ) -> dict[str, str]:
        return closure_event_fields(
            self._current_v1_lifecycle_closure(now_ms),
            phase=phase,
            owner_id=owner_id,
            row_key=row_key,
        )

    def _maybe_export_current_state_snapshot(
        self,
        export_state: ExportState,
        now_ms: int,
    ) -> None:
        """Export current-state with runtime-owned static diagnostics refreshed."""
        self._refresh_runtime_market_data_config_state()
        self._refresh_v1_lifecycle_closure_state(now_ms)
        maybe_export_current_state_snapshot(
            self.state, self.config, export_state, now_ms
        )

    def _entry_quote_lease_max_age_ms(self):
        return self.market_data_runtime._entry_quote_lease_max_age_ms()

    @staticmethod
    def _quote_lease_blocker_family(reason: str) -> str:
        if reason in {"missing_quote_lease_provider", "missing_quote_lease"}:
            return "waiting_for_subscription"
        if reason in {"expired_quote_lease", "stale_quote_lease"}:
            return "stale_quote"
        if reason in {
            "quote_lease_provider_mismatch",
            "quote_lease_symbol_mismatch",
            "quote_lease_long_venue_mismatch",
            "quote_lease_short_venue_mismatch",
            "invalid_quote_lease",
        }:
            return "invalid_quote"
        return "unknown"

    @staticmethod
    def _ws_bbo_selection_blocker_family(reason: str) -> str:
        if reason == "entry_ws_bbo_quote_lease_waiting_for_subscription":
            return "subscription"
        if reason == "entry_ws_bbo_quote_lease_budget_exhausted":
            return "subscription_budget"
        if reason == "entry_ws_bbo_quote_lease_missing_quote":
            return "missing_quote"
        if reason == "entry_ws_bbo_quote_lease_stale_quote":
            return "stale_quote"
        if reason == "entry_ws_bbo_quote_lease_invalid_quote":
            return "invalid_quote"
        return "unknown"

    def _entry_ws_bbo_subscription_blocker(
        self,
        candidate: Any,
    ) -> tuple[str | None, dict[str, Any]]:
        if (
            not self._entry_readiness_provider_uses_ws_bbo()
            or self.config.runtime.mode != "live"
        ):
            return None, {}

        symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
        long_venue = str(getattr(candidate, "long_venue", "") or "").strip().lower()
        short_venue = str(getattr(candidate, "short_venue", "") or "").strip().lower()
        if not symbol or not long_venue or not short_venue:
            return None, {}

        cache = getattr(self, "ws_bbo_cache", None)
        data_plane = getattr(self, "ws_bbo_data_plane", None)
        if data_plane is None or not hasattr(data_plane, "stream_state"):
            return None, {}

        long_quote = cache.get_quote(long_venue, symbol) if cache is not None else None
        short_quote = cache.get_quote(short_venue, symbol) if cache is not None else None
        long_state = data_plane.stream_state(long_venue, symbol)
        short_state = data_plane.stream_state(short_venue, symbol)
        missing_long_subscription = (
            long_quote is None and not bool(long_state.get("tracked"))
        )
        missing_short_subscription = (
            short_quote is None and not bool(short_state.get("tracked"))
        )
        if not missing_long_subscription and not missing_short_subscription:
            return None, {}

        budgeted_keys = getattr(self, "_entry_bbo_subscription_budgeted_keys", set())
        budget_excluded_keys = getattr(
            self,
            "_entry_bbo_subscription_budget_excluded_keys",
            set(),
        )
        per_venue_budget = int(
            getattr(self, "_entry_bbo_subscription_per_venue_budget", 0) or 0
        )

        def budget_state(venue: str) -> dict[str, Any]:
            key = (venue, symbol)
            return {
                "venue": venue,
                "symbol": symbol,
                "budgeted": key in budgeted_keys,
                "excluded": key in budget_excluded_keys,
                "per_venue_budget": per_venue_budget,
            }

        long_budget = budget_state(long_venue)
        short_budget = budget_state(short_venue)
        budget_exhausted = (
            missing_long_subscription and bool(long_budget["excluded"])
        ) or (
            missing_short_subscription and bool(short_budget["excluded"])
        )
        reason = (
            "entry_ws_bbo_quote_lease_budget_exhausted"
            if budget_exhausted
            else "entry_ws_bbo_quote_lease_waiting_for_subscription"
        )
        coverage_reason = (
            "subscription_budget_exhausted"
            if budget_exhausted
            else "subscription_missing"
        )

        return reason, {
            "provider": "ws_bbo_quote_lease",
            "source": "ws_bbo_quote_lease",
            "domain": "ws_bbo_subscription",
            "blocker_family": (
                "subscription_budget"
                if budget_exhausted
                else "subscription"
            ),
            "coverage_reason": coverage_reason,
            "symbol": symbol,
            "missing_long_subscription": missing_long_subscription,
            "missing_short_subscription": missing_short_subscription,
            "long_stream_state": long_state,
            "short_stream_state": short_state,
            "per_venue_budget": per_venue_budget,
            "long_subscription_budget": long_budget,
            "short_subscription_budget": short_budget,
        }

    def _venue_min_notional(self, venue: Venue, symbol: str) -> float:
        """Return the minimum notional value for a venue/symbol pair.

        Used to prevent infinite retry of hedge orders that are below the
        venue's minimum trade notional (e.g., Hyperliquid $10 MinTradeNtl).
        """
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return 0.0
        passive_metadata = getattr(adapter, "passive_metadata", None)
        if callable(passive_metadata):
            try:
                metadata = passive_metadata(symbol) or {}
                min_notional = float(
                    metadata.get("min_notional", metadata.get("min_notional_quote", 0.0))
                    or 0.0
                )
                if min_notional > 0:
                    return min_notional
            except Exception:
                pass
        adapter_min = float(
            getattr(adapter, "min_notional_quote", getattr(adapter, "_min_notional_quote", 0.0))
            or 0.0
        )
        if adapter_min > 0:
            return adapter_min
        transport = getattr(adapter, "_transport", adapter)
        spec = getattr(transport, "_spec", None)
        if spec is not None:
            return float(getattr(spec, "min_notional", 0.0) or 0.0)
        return 0.0

    def _pending_entry_hedge_price_hint(self, pending) -> float:
        price_hint = 0.0
        weighted_average = getattr(pending, "unmatched_maker_weighted_average_price", None)
        if callable(weighted_average):
            try:
                price_hint = float(weighted_average() or 0.0)
            except (TypeError, ValueError):
                price_hint = 0.0
        if price_hint <= 0.0:
            price_hint = float(getattr(pending, "maker_fill_price", 0.0) or 0.0)
        if price_hint <= 0.0:
            price_hint = float(getattr(pending, "maker_price", 0.0) or 0.0)
        return max(0.0, price_hint)

    def _pending_entry_hedgeability_plan(
        self,
        pending,
        hedge_venue: Venue,
        desired_quantity: float,
        price_hint: float,
    ) -> PendingEntryHedgeabilityPlan:
        min_notional = self._venue_min_notional(hedge_venue, pending.symbol)
        min_hedgeable_chunk = 0.0
        if min_notional > 0.0 and price_hint > 0.0:
            min_hedgeable_chunk = min_notional / price_hint
        adapter = self.get_venue_adapter(hedge_venue)
        quantity_step = 0.0
        passive_metadata = getattr(adapter, "passive_metadata", None) if adapter else None
        if callable(passive_metadata):
            try:
                metadata = passive_metadata(pending.symbol) or {}
                quantity_step = float(
                    metadata.get("quantity_step", metadata.get("step_size", 0.0)) or 0.0
                )
            except Exception:
                quantity_step = 0.0
        if quantity_step > 0.0:
            min_hedgeable_chunk = max(min_hedgeable_chunk, quantity_step)
        aligned = releasable_hedge_quantity(desired_quantity, min_hedgeable_chunk)
        blocked_reason = "target_below_min_hedgeable_chunk" if aligned <= 1e-9 else ""
        return PendingEntryHedgeabilityPlan(
            min_hedgeable_chunk=min_hedgeable_chunk,
            aligned_target_quantity=aligned,
            blocked_reason=blocked_reason,
            diagnostics={
                "maker_venue": pending.maker_venue().value,
                "hedge_venue": hedge_venue.value,
                "exchange_min_notional_quote": min_notional,
                "price_hint": price_hint,
                "quantity_step": quantity_step,
            },
        )

    def _pending_entry_min_notional_violation(
        self,
        pending,
        hedge_venue: Venue,
        normalized_quantity: float,
        price_hint: float,
    ) -> tuple[float, float] | None:
        if normalized_quantity <= 1e-9:
            return None
        min_notional = self._venue_min_notional(hedge_venue, pending.symbol)
        if min_notional <= 0.0 or price_hint <= 0.0:
            return None
        leg_notional = abs(float(normalized_quantity or 0.0) * price_hint)
        if leg_notional + 1e-9 < min_notional:
            return (leg_notional, min_notional)
        return None

    async def _normalize_pending_entry_hedge_quantity(
        self,
        *,
        pending,
        hedge_venue: Venue,
        adapter,
        missing: float,
        hedge_price: float,
        hedgeability_plan: PendingEntryHedgeabilityPlan,
    ) -> tuple[float, tuple[float, float] | None, dict]:
        releasable = releasable_hedge_quantity(
            missing,
            hedgeability_plan.min_hedgeable_chunk,
        )
        full_normalized = missing
        if hasattr(adapter, "normalize_quantity"):
            full_normalized = await adapter.normalize_quantity(pending.symbol, missing)
        full_normalized = float(full_normalized or 0.0)
        full_min_notional_violation = self._pending_entry_min_notional_violation(
            pending,
            hedge_venue,
            full_normalized,
            hedge_price,
        )
        evidence = {
            "missing_hedge_quantity": missing,
            "releasable_quantity": releasable,
            "full_missing_normalized_quantity": full_normalized,
            "min_hedgeable_chunk": hedgeability_plan.min_hedgeable_chunk,
            "hedgeability": dict(hedgeability_plan.diagnostics),
        }
        if (
            full_normalized > 1e-9
            and full_min_notional_violation is None
            and full_normalized + 1e-9 >= missing
        ):
            evidence["quantity_source"] = "full_missing_exchange_normalized"
            return full_normalized, None, evidence

        normalized = releasable
        if hasattr(adapter, "normalize_quantity"):
            normalized = await adapter.normalize_quantity(pending.symbol, releasable)
        normalized = float(normalized or 0.0)
        min_notional_violation = self._pending_entry_min_notional_violation(
            pending,
            hedge_venue,
            normalized,
            hedge_price,
        )
        evidence["quantity_source"] = "releasable_chunk_normalized"
        evidence["normalized_quantity"] = normalized
        evidence["full_missing_min_notional_violation"] = (
            {
                "leg_notional_quote": full_min_notional_violation[0],
                "venue_min_notional_quote": full_min_notional_violation[1],
            }
            if full_min_notional_violation is not None
            else None
        )
        return normalized, min_notional_violation, evidence

    def _append_pending_entry_hedge_quantity_undercut(
        self,
        *,
        entry_id: str,
        pending,
        hedge_venue: Venue,
        normalized_quantity: float,
        quantity_evidence: dict,
    ) -> None:
        missing = float(quantity_evidence.get("missing_hedge_quantity", 0.0) or 0.0)
        if normalized_quantity + 1e-9 >= missing:
            return
        min_notional_gap = quantity_evidence.get("full_missing_min_notional_violation")
        if min_notional_gap:
            reason_family = "min_notional_buffer"
        elif str(quantity_evidence.get("quantity_source") or ""):
            reason_family = "exchange_step_rounding"
        else:
            reason_family = "unexpected_undercut"
        hedge_price_hint = float(
            getattr(pending, "hedge_fill_price", 0.0)
            or getattr(pending, "maker_price", 0.0)
            or getattr(pending, "maker_fill_price", 0.0)
            or 0.0
        )
        self.journal.append(
            "pending_entry.hedge_quantity_undercut",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "hedge_venue": hedge_venue.value,
                "reason_family": reason_family,
                "missing_hedge_quantity": missing,
                "normalized_quantity": normalized_quantity,
                "undercut_quantity": max(missing - normalized_quantity, 0.0),
                "hedge_price_hint": hedge_price_hint,
                "hedge_min_notional_quote": self._venue_min_notional(
                    hedge_venue,
                    pending.symbol,
                ),
                **dict(quantity_evidence),
            },
        )

    def _append_pending_entry_hedge_decision_event(
        self,
        decision: PendingEntryHedgeDeltaDecision,
    ) -> None:
        if not decision.event:
            return
        self.journal.append(decision.event, dict(decision.evidence))

    def _pending_entry_hedge_deadline_started(
        self,
        pending,
        *,
        submitted_at_ms: int,
        normalized_quantity: float,
        hedge_price: float,
        hedge_attempt: int,
        hedge_venue: Venue,
    ) -> None:
        strategy = self.config.strategy
        hard_ms = int(getattr(strategy, "maker_hedge_deadline_ms", 0) or 0)
        soft_ms = int(
            getattr(
                strategy,
                "maker_hedge_soft_deadline_ms",
                min(hard_ms, 800) if hard_ms > 0 else 800,
            )
            or 0
        )
        hedge_notional = abs(float(normalized_quantity or 0.0) * max(0.0, hedge_price))
        decision = note_pending_entry_hedge_submitted(
            pending,
            submitted_at_ms=submitted_at_ms,
            base_soft_deadline_ms=soft_ms,
            base_hard_deadline_ms=hard_ms,
            hedge_notional_quote=hedge_notional,
            quote_fresh=hedge_price > 0.0,
        )
        phase_state = ensure_pending_entry_phase_state(pending, submitted_at_ms)
        self.journal.append(
            "execution.hedge_deadline_started",
            {
                "entry_id": pending.pending_id,
                "symbol": pending.symbol,
                "hedge_venue": hedge_venue.value,
                "hedge_attempt": hedge_attempt,
                "soft_budget_ms": decision.effective_soft_deadline_ms,
                "budget_ms": decision.effective_hard_deadline_ms,
                "deadline_at_ms": phase_state.hedge_deadline_at_ms,
                "requested_quantity": normalized_quantity,
                "missing_hedge_quantity": pending.missing_hedge_quantity(),
            },
        )

    def _emit_startup_order_path_preflight(self) -> None:
        """Emit sanitized startup visibility for order signing/dependency readiness."""
        blocked = {"api_key", "api_secret", "secret", "signature", "private_key", "headers", "auth"}
        for venue, adapter in sorted(
            self._venue_adapters.items(),
            key=lambda item: item[0].value if hasattr(item[0], "value") else str(item[0]),
        ):
            transport = getattr(adapter, "_transport", adapter)
            preflight_fn = getattr(transport, "startup_preflight", None)
            if not callable(preflight_fn):
                continue
            try:
                raw_payload = preflight_fn()
            except Exception as exc:
                raw_payload = {
                    "venue": venue.value if hasattr(venue, "value") else str(venue),
                    "status": "failed",
                    "reason": str(exc),
                }
            payload = {}
            for key, value in dict(raw_payload or {}).items():
                key_s = str(key)
                if any(token in key_s.lower() for token in blocked):
                    continue
                payload[key_s] = value
            payload.setdefault("venue", venue.value if hasattr(venue, "value") else str(venue))
            payload.setdefault("status", "ok")
            self.journal.append("startup.order_path_preflight", payload)

    async def _verify_live_trading_preflights(self) -> None:
        """Run read-only venue admission checks before selector can trade."""
        blocked = {
            "api_key",
            "api_secret",
            "secret",
            "signature",
            "private_key",
            "headers",
        }
        allowed_auth_diagnostics = {
            "api_wallet_authorization_verified",
            "authorization_error",
            "authorization_mode",
            "authorization_verified",
        }
        for venue, adapter in sorted(
            self._venue_adapters.items(),
            key=lambda item: item[0].value if hasattr(item[0], "value") else str(item[0]),
        ):
            transport = getattr(adapter, "_transport", adapter)
            preflight_fn = getattr(transport, "verify_live_trading_preflight", None)
            if not callable(preflight_fn):
                continue
            try:
                raw_payload = await preflight_fn()
            except Exception as exc:
                raw_payload = {
                    "venue": venue.value if hasattr(venue, "value") else str(venue),
                    "status": "failed",
                    "trading_capability_trusted": False,
                    "reason": str(exc),
                }
            payload: dict[str, object] = {}
            for key, value in dict(raw_payload or {}).items():
                key_s = str(key)
                key_l = key_s.lower()
                if (
                    key_l not in allowed_auth_diagnostics
                    and (
                        key_l in blocked
                        or any(
                            token in key_l
                            for token in (
                                "api_key",
                                "api_secret",
                                "auth",
                                "header",
                                "private_key",
                                "secret",
                                "signature",
                            )
                        )
                    )
                ):
                    continue
                payload[key_s] = value
            payload.setdefault("venue", venue.value if hasattr(venue, "value") else str(venue))
            payload.setdefault("status", "ok")
            if venue == Venue.HYPERLIQUID:
                trusted = bool(payload.get("trading_capability_trusted"))
                status = str(payload.get("status", "")).lower()
                if status != "ok" or not trusted:
                    self.state.hyperliquid_trading_disabled_reason = str(
                        payload.get("reason") or "trading_preflight_failed"
                    )
                else:
                    self.state.hyperliquid_trading_disabled_reason = None
            self.journal.append("startup.trading_preflight", payload)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _run_startup_phase_with_timeout(self, phase: str, coro) -> None:
        timeout_ms = max(self.config.runtime.live_startup_phase_timeout_ms, 1)
        try:
            await asyncio.wait_for(coro, timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            self.journal.append(
                "runtime.startup_phase_timeout",
                {
                    "phase": phase,
                    "timeout_ms": timeout_ms,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

    async def start(self) -> None:
        """Booting sequence: phased private→market→local-L2 startup (V1 parity)."""
        self.journal.open()

        # Phase 1 – BOOTING
        set_lifecycle(self.state, EngineLifecycle.BOOTING)
        self.state.run_id = self.journal.run_id
        self.state.started_at_ms = wall_clock_now_ms()

        self.journal.append(
            "runtime.booting",
            {"run_id": self.state.run_id, "ts_ms": self.state.started_at_ms},
            flush=True,
        )
        self._emit_startup_order_path_preflight()
        await self._verify_live_trading_preflights()
        hyperliquid_trading_disabled_reason = (
            self.state.hyperliquid_trading_disabled_reason
        )

        # Phase 2 – Resolve runtime symbols (daily-universe integration point)
        symbol_info = await prepare_runtime_symbols(self.config)

        # Phase 3 – Recover or start fresh
        self.state = recover_from_snapshot(self.snapshot_store, self.journal)
        self.state.hyperliquid_trading_disabled_reason = (
            hyperliquid_trading_disabled_reason
        )
        self._restore_passive_order_manager_states()
        self.state.run_id = self.journal.run_id
        if self.state.started_at_ms == 0:
            self.state.started_at_ms = wall_clock_now_ms()

        # Build recovery dedup index from recovered pending state
        self._recovery_dedup_index = build_recovery_dedup_index(self.state)
        startup_live_probe_ms = wall_clock_now_ms()
        startup_live_recovery_result = await self._recover_startup_live_positions(
            self._startup_position_probe_symbols(symbol_info),
            startup_live_probe_ms,
        )
        current_startup_recovery_block = (
            bool(self.state.recovery_blocked_reason)
            and self.state.recovery_blocked_at_ms >= startup_live_probe_ms
        )

        # Phase 4 – Recovery-aware startup (Rust V1: finalize_startup_position_recovery)
        from lightfee.engine.recovery import classify_startup_recovery_state

        classified_recovery_state = classify_startup_recovery_state(self.state)

        stale_block_with_recovery_work = (
            bool(self.state.recovery_blocked_reason)
            and classified_recovery_state == "recovery_needed"
            and not current_startup_recovery_block
            and self.state.operator.requested_mode != GlobalRiskMode.FAIL_CLOSED
        )
        if stale_block_with_recovery_work:
            self.journal.append(
                "runtime.recovery_block_reconcile_attempt",
                {
                    "previous_recovery_blocked_reason": (
                        self.state.recovery_blocked_reason
                    ),
                    "pending_entries": len(self.state.pending_entries),
                    "open_positions": len(self.state.open_positions),
                    "pending_closes": len(self.state.pending_closes),
                    "pending_passive_closes": len(self.state.pending_passive_closes),
                    "pending_residual_repairs": len(
                        getattr(self.state, "pending_residual_repairs", []) or []
                    ),
                    "ts_ms": wall_clock_now_ms(),
                },
            )
            recovery_class = "recovery_needed"
        else:
            recovery_class = (
                "blocked"
                if self.state.recovery_blocked_reason
                else classified_recovery_state
            )

        if recovery_class == "clean":
            set_lifecycle(self.state, EngineLifecycle.RUNNING)
            clear_stale_fail_closed_if_recovery_clean(self.state, self.journal)
            self.journal.append(
                "runtime.running",
                {"reason": "startup_no_recovery_work", "ts_ms": wall_clock_now_ms()},
            )
        elif recovery_class == "recovery_needed":
            transition_to_reconciling(self.state)
            self.journal.append(
                "runtime.reconciling",
                {
                    "reason": "startup_recovery_required",
                    "open_positions": len(self.state.open_positions),
                    "pending_entries": len(self.state.pending_entries),
                    "pending_closes": len(self.state.pending_closes),
                    "ts_ms": wall_clock_now_ms(),
                },
            )

            # V1: finalize_startup_position_recovery — ordered recovery sequence
            now_ms = wall_clock_now_ms()

            # 1. reconcile_open_positions (force_reconcile — ignore backoff)
            await self._reconcile_pending_entries_force(now_ms)

            # 2. process pending_entry_hedges — re-drive any uncertain maker orders
            await self._recover_pending_entry_hedges(now_ms)

            # 3. process pending_passive_closes — resume passive close cycles
            await self._maybe_tick_passive_close(now_ms)

            # 4. process pending_close_reconciliations
            # (already handled by _reconcile_pending_state in housekeeping)

            # 5. residual repairs
            await self._recover_residual_repairs(now_ms)

            # 6. manage_open_positions — if still over max, enter fail_closed
            max_positions = self.config.strategy.max_concurrent_positions
            if len(self.state.open_positions) > max_positions:
                enter_fail_closed(self.state)
                self.journal.append(
                    "runtime.recovery_fail_closed",
                    {
                        "reason": "open_positions_exceed_max_after_recovery",
                        "open_positions": len(self.state.open_positions),
                        "max": max_positions,
                        "ts_ms": wall_clock_now_ms(),
                    },
                )
            elif self.state.lifecycle == EngineLifecycle.RECONCILING:
                # Safety net: if _recover_pending_entry_hedges returned early
                # (e.g. no venue adapters) without finalizing, do it now.
                self._finalize_startup_recovery()
        else:
            if self.state.recovery_blocked_reason:
                enter_fail_closed(self.state)
            self.journal.append(
                "runtime.recovery_blocked",
                {
                    "reason": "startup_fail_closed",
                    "lifecycle": self.state.lifecycle.value,
                    "risk_mode": self.state.risk_mode.value,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        # Phase 5 – Local-L2 startup activation (V1: local-L2 phased activation)
        await self._run_startup_phase_with_timeout(
            "local_l2_activation",
            self._activate_local_l2_phase(wall_clock_now_ms()),
        )

        # Phase 6 – Recover retained local-L2 state
        await self._restore_local_l2_state()

        # Phase 7 – Instantiate passive close executor
        if self._venue_adapters:
            from lightfee.engine.passive_close import PassiveCloseExecutor
            self.passive_close_executor = PassiveCloseExecutor(
                adapters=self._venue_adapters,
                journal=self.journal,
                config_overrides={
                    "runtime_mode": self.config.runtime.mode,
                    "maker_hedge_deadline_ms": self.config.strategy.maker_hedge_deadline_ms,
                },
            )
            # Inject provider-aware price evidence; legacy method names remain
            # on PassiveCloseExecutor for compatibility with existing callers.
            self.passive_close_executor.set_l2_mid_resolver(
                self._resolve_close_price_hint_mid_with_source
            )
            self.passive_close_executor.set_l2_quote_resolver(
                self._resolve_close_price_hint_quote_with_source
            )
            # Inject close executor for DUAL_TAKER fallback
            if self.close_executor is not None:
                self.passive_close_executor.set_close_executor(self.close_executor)

        # Phase 8 – Recover pending passive closes
        await self._recover_passive_closes()
        if startup_live_recovery_result not in {
            "mismatch_flattened",
            "mismatch_blocked",
        }:
            await self._refresh_recovery_ledger_for_symbols(
                self._startup_recovery_ledger_symbols(symbol_info),
                wall_clock_now_ms(),
                lifecycle_clear_reason="startup_recovery_ledger_current_state_clean",
            )

        self.journal.append(
            "runtime.started",
            {
                "run_id": self.state.run_id,
                "lifecycle": self.state.lifecycle.value,
                "risk_mode": self.state.risk_mode.value,
            },
            flush=True,
        )

    def _refresh_recovery_ledger_from_exchange_truth(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._refresh_recovery_ledger_from_exchange_truth(*args, **kwargs)

    def _clear_stale_recovery_lifecycle_if_core_clean(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._clear_stale_recovery_lifecycle_if_core_clean(*args, **kwargs)

    @staticmethod
    def _recovery_exchange_truth_flat(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_exchange_truth_flat(*args, **kwargs)

    @staticmethod
    def _recovery_exchange_truth_open_orders_empty(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_exchange_truth_open_orders_empty(*args, **kwargs)

    def _recovery_state_collection(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._recovery_state_collection(*args, **kwargs)

    async def _refresh_recovery_ledger_for_symbols(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._refresh_recovery_ledger_for_symbols(*args, **kwargs)

    async def _refresh_recovery_ledger_from_account_truth(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._refresh_recovery_ledger_from_account_truth(*args, **kwargs)

    async def _collect_recovery_ledger_account_truth(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._collect_recovery_ledger_account_truth(*args, **kwargs)

    async def _fetch_recovery_ledger_account_open_orders(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._fetch_recovery_ledger_account_open_orders(*args, **kwargs)

    async def _collect_recovery_ledger_exchange_truth(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._collect_recovery_ledger_exchange_truth(*args, **kwargs)

    @staticmethod
    def _recovery_ledger_position_payload(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_ledger_position_payload(*args, **kwargs)

    @staticmethod
    def _recovery_ledger_open_order_payloads(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_ledger_open_order_payloads(*args, **kwargs)

    @staticmethod
    def _truthy_recovery_order_field(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._truthy_recovery_order_field(*args, **kwargs)

    @staticmethod
    def _recovery_ledger_order_rows(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_ledger_order_rows(*args, **kwargs)

    def _startup_recovery_ledger_symbols(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._startup_recovery_ledger_symbols(*args, **kwargs)

    def _startup_recovery_owner_journal_symbols(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._startup_recovery_owner_journal_symbols(*args, **kwargs)

    @staticmethod
    def _has_journal_order_owner_evidence(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._has_journal_order_owner_evidence(*args, **kwargs)

    @staticmethod
    def _has_journal_position_owner_evidence(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._has_journal_position_owner_evidence(
            *args, **kwargs
        )

    def _recovery_owner_journal_events(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._recovery_owner_journal_events(*args, **kwargs)

    def _has_local_recovery_work(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._has_local_recovery_work(*args, **kwargs)

    def _only_active_unpaired_live_position_recovery_work(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._only_active_unpaired_live_position_recovery_work(*args, **kwargs)

    async def _align_stale_unpaired_risk_state_from_account_truth(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._align_stale_unpaired_risk_state_from_account_truth(*args, **kwargs)

    @staticmethod
    def _recovery_ledger_work_item_payload(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._recovery_ledger_work_item_payload(*args, **kwargs)

    def _startup_position_probe_symbols(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._startup_position_probe_symbols(*args, **kwargs)

    def _register_unpaired_live_position_recoveries_from_ledger(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return self.unpaired_live_position_recovery_runtime.register_from_ledger(
            *args,
            **kwargs,
        )

    async def _drive_unpaired_live_position_recoveries(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return await self.unpaired_live_position_recovery_runtime.drive(
            *args,
            **kwargs,
        )

    def _truth_required_recovery_probe_symbol_sources(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._truth_required_recovery_probe_symbol_sources(*args, **kwargs)

    async def _recover_startup_live_positions(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._recover_startup_live_positions(*args, **kwargs)

    @staticmethod
    def _live_position_snapshot_symbols(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._live_position_snapshot_symbols(*args, **kwargs)

    def _block_unpaired_startup_live_positions(self, *args: Any, **kwargs: Any) -> Any:
        return self.recovery_startup_runtime._block_unpaired_startup_live_positions(*args, **kwargs)

    async def _flatten_startup_live_position_mismatches(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._flatten_startup_live_position_mismatches(*args, **kwargs)

    async def _post_cleanup_position_truth(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._post_cleanup_position_truth(*args, **kwargs)

    @staticmethod
    def _combined_post_cleanup_truth(*args: Any, **kwargs: Any) -> Any:
        return RecoveryStartupRuntime._combined_post_cleanup_truth(*args, **kwargs)

    async def _maybe_recover_clean_live_positions(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._maybe_recover_clean_live_positions(*args, **kwargs)

    async def _position_probe_symbols_for_venue(self, *args: Any, **kwargs: Any) -> Any:
        return await self.recovery_startup_runtime._position_probe_symbols_for_venue(*args, **kwargs)


    async def _filter_symbols_supported_by_venue(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        symbols: list[str],
        *,
        skip_event_kind: str,
    ) -> list[str]:
        """Filter symbols through a venue-provided trading catalog when present."""
        ensure_loaded = getattr(adapter, "ensure_supported_symbols_loaded", None)
        ensure_available = callable(ensure_loaded)
        catalog_error = ""
        catalog_unavailable_reason = ""
        if callable(ensure_loaded):
            try:
                maybe_coro = ensure_loaded()
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
            except Exception as exc:
                catalog_error = str(exc)
                catalog_unavailable_reason = "ensure_supported_symbols_loaded_failed"

        supported_fn = getattr(adapter, "supported_symbols", None)
        supported_available = callable(supported_fn)
        try:
            supported_raw = supported_fn() if supported_available else []
        except Exception as exc:
            supported_raw = []
            catalog_error = str(exc)
            catalog_unavailable_reason = "supported_symbols_failed"
        supported = {str(symbol) for symbol in supported_raw if str(symbol)}
        if not supported:
            if not catalog_unavailable_reason:
                catalog_unavailable_reason = (
                    "supported_symbols_empty"
                    if supported_available
                    else "supported_symbols_unavailable"
                )
            if (
                skip_event_kind == "recovery.live_position_probe_symbol_skipped"
                and getattr(self.journal, "_file", None) is not None
            ):
                now_ms = wall_clock_now_ms()
                diagnostic_key = (
                    "recovery.live_position_probe_catalog_unavailable",
                    venue.value,
                    catalog_unavailable_reason,
                )
                last_ms = self._unsupported_symbol_diagnostic_last_ms.get(
                    diagnostic_key,
                    0,
                )
                if (
                    last_ms <= 0
                    or now_ms >= last_ms + self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS
                ):
                    self._unsupported_symbol_diagnostic_last_ms[diagnostic_key] = now_ms
                    self.journal.append(
                        "recovery.live_position_probe_catalog_unavailable",
                        {
                            "venue": venue.value,
                            "catalog_source": (
                                "adapter.supported_symbols"
                                if supported_available
                                else ""
                            ),
                            "catalog_available": False,
                            "catalog_unavailable_reason": catalog_unavailable_reason,
                            "catalog_error": catalog_error,
                            "ensure_supported_symbols_available": ensure_available,
                            "supported_symbols_available": supported_available,
                            "catalog_supported_count": 0,
                            "sample_supported_symbols": [],
                            "symbol_count": len(symbols),
                            "requested_symbols": [str(symbol) for symbol in symbols],
                            "diagnostic_key": list(diagnostic_key),
                            "diagnostic_rate_limit_ms": self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS,
                            "decision": "probe_unfiltered",
                            "reason": "catalog_unavailable",
                        },
                    )
            return symbols

        transport = getattr(adapter, "_transport", None)
        to_venue_symbol = getattr(transport, "_venue_symbol", None)
        spec = getattr(transport, "_spec", None)
        endpoint = str(getattr(spec, "position_path", "") or "fetch_position")
        catalog_supported_count = len(supported)
        sample_supported_symbols = sorted(supported)[:10]

        filtered: list[str] = []
        unsupported: list[dict[str, str]] = []
        for symbol in symbols:
            venue_symbol = str(symbol)
            if callable(to_venue_symbol):
                try:
                    venue_symbol = str(to_venue_symbol(symbol))
                except Exception:
                    venue_symbol = str(symbol)
            if str(symbol) in supported or venue_symbol in supported:
                filtered.append(symbol)
            else:
                if (
                    skip_event_kind
                    == "recovery.live_position_probe_symbol_skipped"
                ):
                    unsupported.append(
                        {"symbol": str(symbol), "venue_symbol": venue_symbol}
                    )
                elif skip_event_kind and getattr(self.journal, "_file", None) is not None:
                    self.journal.append(
                        skip_event_kind,
                        {
                            "venue": venue.value,
                            "symbol": symbol,
                            "venue_symbol": venue_symbol,
                            "reason": "unsupported_symbol",
                        },
                    )
        if (
            unsupported
            and skip_event_kind == "recovery.live_position_probe_symbol_skipped"
            and getattr(self.journal, "_file", None) is not None
        ):
            now_ms = wall_clock_now_ms()
            diagnostic_key = (
                "recovery.live_position_probe_unsupported_symbols",
                venue.value,
            )
            last_ms = self._unsupported_symbol_diagnostic_last_ms.get(diagnostic_key, 0)
            if (
                last_ms > 0
                and now_ms < last_ms + self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS
            ):
                return filtered
            self._unsupported_symbol_diagnostic_last_ms[diagnostic_key] = now_ms
            sample = unsupported[:10]
            self.journal.append(
                "recovery.live_position_probe_unsupported_symbols",
                {
                    "venue": venue.value,
                    "endpoint": endpoint,
                    "catalog_source": "adapter.supported_symbols",
                    "catalog_supported_count": catalog_supported_count,
                    "sample_supported_symbols": sample_supported_symbols,
                    "symbol_count": len(symbols),
                    "requested_symbols": [str(symbol) for symbol in symbols],
                    "skipped_by_catalog": [item["symbol"] for item in unsupported],
                    "unsupported_count": len(unsupported),
                    "sample_symbols": [item["symbol"] for item in sample],
                    "sample_venue_symbols": [
                        item["venue_symbol"] for item in sample
                    ],
                    "symbol_mapping_samples": [
                        {
                            "symbol": item["symbol"],
                            "venue_symbol": item["venue_symbol"],
                        }
                        for item in sample
                    ],
                    "diagnostic_key": list(diagnostic_key),
                    "diagnostic_rate_limit_ms": self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS,
                    "reason": "unsupported_symbol",
                },
            )
        return filtered

    def _position_probe_exception_payload(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        exc: Exception,
        *,
        symbol: str = "",
    ) -> dict[str, object]:
        transport = getattr(adapter, "_transport", None)
        spec = getattr(transport, "_spec", None)
        endpoint = str(getattr(spec, "position_path", "") or "fetch_position")
        normalized_symbol = str(symbol or "")
        venue_symbol = normalized_symbol
        to_venue_symbol = getattr(transport, "_venue_symbol", None)
        if normalized_symbol and callable(to_venue_symbol):
            try:
                venue_symbol = str(to_venue_symbol(normalized_symbol))
            except Exception:
                venue_symbol = normalized_symbol

        supported_fn = getattr(adapter, "supported_symbols", None)
        supported_available = callable(supported_fn)
        ensure_available = callable(getattr(adapter, "ensure_supported_symbols_loaded", None))
        catalog_error = ""
        catalog_unavailable_reason = ""
        try:
            supported_raw = supported_fn() if supported_available else []
        except Exception as catalog_exc:
            supported_raw = []
            catalog_error = str(catalog_exc)
            catalog_unavailable_reason = "supported_symbols_failed"
        supported = {str(item) for item in supported_raw if str(item)}
        if not supported and not catalog_unavailable_reason:
            catalog_unavailable_reason = (
                "supported_symbols_empty"
                if supported_available
                else "supported_symbols_unavailable"
            )
        catalog_supported_count = len(supported)
        catalog_supported = (
            bool(supported)
            and (
                normalized_symbol in supported
                or venue_symbol in supported
            )
        )

        body = str(getattr(exc, "body", "") or "")
        ret_code = ""
        ret_msg = ""
        if body:
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                ret_code = str(
                    parsed.get("retCode", parsed.get("code", "")) or ""
                )
                ret_msg = str(
                    parsed.get("retMsg", parsed.get("msg", "")) or ""
                )

        message = str(exc)
        exception_class = exc.__class__.__name__
        category = getattr(exc, "category", None)
        category_value = str(getattr(category, "value", category) or "")
        status_code = int(getattr(exc, "status_code", 0) or 0)
        headers = getattr(exc, "headers", {}) or {}
        retry_after_ms = 0
        retry_after = ""
        if isinstance(headers, dict):
            retry_after = str(
                headers.get("Retry-After", headers.get("retry-after", "")) or ""
            )
        if retry_after:
            try:
                retry_after_ms = max(int(float(retry_after) * 1000), 0)
            except (TypeError, ValueError):
                retry_after_ms = 0
        if not normalized_symbol and "instId=" in message:
            inst_id = message.split("instId=", 1)[1].split()[0].strip(",;")
            if inst_id:
                venue_symbol = inst_id
                normalized_symbol = inst_id
                from_venue_symbol = getattr(spec, "symbol_from_venue", None)
                if callable(from_venue_symbol):
                    try:
                        normalized_symbol = str(from_venue_symbol(inst_id))
                    except Exception:
                        normalized_symbol = inst_id
        is_timeout = (
            isinstance(exc, asyncio.TimeoutError)
            or exception_class == "TimeoutError"
        )
        retryable = (
            is_timeout
            or category_value == TransportErrorCategory.TRANSPORT_FAILURE.value
            or status_code in (408, 418, 429, 500, 502, 503, 504)
        )
        error = f"{exception_class}: {message}" if message else exception_class
        payload: dict[str, object] = {
            "venue": venue.value,
            "symbol": normalized_symbol,
            "normalized_symbol": normalized_symbol,
            "venue_symbol": venue_symbol,
            "endpoint": endpoint,
            "probe_category": "private_positions",
            "catalog_source": (
                "adapter.supported_symbols" if supported_available else ""
            ),
            "catalog_available": bool(supported),
            "catalog_unavailable_reason": (
                "" if supported else catalog_unavailable_reason
            ),
            "catalog_error": "" if supported else catalog_error,
            "ensure_supported_symbols_available": ensure_available,
            "supported_symbols_available": supported_available,
            "catalog_supported": catalog_supported,
            "catalog_supported_count": catalog_supported_count,
            "sample_supported_symbols": sorted(supported)[:10],
            "cooldown_scope": f"symbol:{venue.value}:{normalized_symbol or '*'}:private_positions",
            "cooldown_ms": 0,
            "exception_class": exception_class,
            "error": error,
            "retCode": ret_code,
            "retMsg": ret_msg,
            "body_summary": body[:500],
            "retryable": retryable,
        }
        if category_value:
            payload["category"] = category_value
        if status_code:
            payload["status_code"] = status_code
        if is_timeout:
            payload["classification"] = "timeout"
        if status_code in (418, 429) or ret_code in ("429", "50011", "50061"):
            payload["classification"] = "rate_limited"
            payload["cooldown_scope"] = f"venue:{venue.value}:private_positions"
            payload["retry_after_ms"] = retry_after_ms
            payload["cooldown_ms"] = retry_after_ms or 2000
            if venue == Venue.OKX and endpoint == "/api/v5/account/positions":
                payload["rate_limit_budget"] = {
                    "requests": 10,
                    "window_ms": 2000,
                    "scope": "User ID",
                }
        if "okx_contract_metadata_missing_ct_val" in message:
            if "classification=instrument_missing" in message:
                payload["classification"] = "instrument_missing"
            elif "classification=metadata_missing" in message:
                payload["classification"] = "metadata_missing"
            else:
                payload["classification"] = "metadata_missing"
            payload["retryable"] = False
            payload["skip_reason"] = "catalog_or_metadata_missing"
        return payload

    def _append_runtime_diagnostic_event(
        self,
        kind: str,
        payload: dict,
        *,
        now_ms: int,
        key_parts: tuple,
        interval_ms: int,
    ) -> bool:
        event_key = (kind, *tuple(str(part) for part in key_parts))
        last_emit_ms = self._runtime_diagnostic_event_last_emit_ms.get(event_key)
        suppressed = int(self._runtime_diagnostic_event_suppressed.get(event_key, 0))
        due = last_emit_ms is None or now_ms <= 0 or now_ms - last_emit_ms >= interval_ms
        if not due:
            self._runtime_diagnostic_event_suppressed[event_key] += 1
            return False

        event_payload = dict(payload)
        if suppressed > 0:
            event_payload["compact"] = True
            event_payload["suppressed_count"] = suppressed
        self._runtime_diagnostic_event_last_emit_ms[event_key] = now_ms
        self._runtime_diagnostic_event_suppressed.pop(event_key, None)
        self.journal.append(kind, event_payload)
        return True

    async def _filter_candidates_supported_by_venue_catalog(
        self,
        candidates: list,
        *,
        skip_event_kind: str = "runtime.candidate_symbol_skipped",
    ) -> list:
        return await self.entry_gate_runtime._filter_candidates_supported_by_venue_catalog(
            candidates,
            skip_event_kind=skip_event_kind,
        )

    async def _fetch_startup_live_position_snapshots(
        self, symbols: list[str]
    ) -> list[tuple[str, PositionSnapshot]]:
        timeout_budget_ms = max(
            int(self.config.runtime.live_recovery_rest_probe_timeout_ms),
            1,
        )
        timeout_s = timeout_budget_ms / 1000.0
        concurrency_limit = 8
        semaphore = asyncio.Semaphore(concurrency_limit)
        global_probe_started_at_ms = wall_clock_now_ms()
        requested_symbols = list(
            dict.fromkeys(str(symbol) for symbol in symbols if str(symbol))
        )
        probe_symbols_by_venue: dict[Venue, list[str]] = {}
        for venue, adapter in self._venue_adapters.items():
            probe_symbols_by_venue[venue] = await self._position_probe_symbols_for_venue(
                venue,
                adapter,
                requested_symbols,
            )
        truth_required_symbol_sources = (
            self._truth_required_recovery_probe_symbol_sources(requested_symbols)
        )
        truth_required_sources = sorted(truth_required_symbol_sources)
        truth_required_symbol_source_payload = {
            source: list(truth_required_symbol_sources.get(source, []))
            for source in truth_required_sources
        }
        recovery_truth_required_symbol_sources = {
            source: symbols
            for source, symbols in truth_required_symbol_sources.items()
            if source != "explicit_requested_symbol"
        }
        recovery_truth_required_sources = sorted(
            recovery_truth_required_symbol_sources
        )
        recovery_truth_required_symbol_source_payload = {
            source: list(recovery_truth_required_symbol_sources.get(source, []))
            for source in recovery_truth_required_sources
        }
        recovery_truth_required_symbols = list(
            dict.fromkeys(
                symbol
                for source in recovery_truth_required_sources
                for symbol in recovery_truth_required_symbol_sources.get(source, [])
            )
        )
        fallback_probe_symbol_cache: dict[tuple[Venue, bool], list[str]] = {}

        def truth_unavailable_core_decision(venue: Venue):
            synthetic_work_items = ()
            if recovery_truth_required_sources:
                synthetic_work_items = (
                    SimpleNamespace(
                        kind="truth_required_position_probe",
                        symbol="*",
                        venues={venue.value},
                        blocking=True,
                        requires_truth=True,
                    ),
                )
            return V1RecoveryDecisionCore().decide(
                RecoveryEvidenceSnapshot(
                    local_open_positions=tuple(
                        self._recovery_state_collection("open_positions")
                    ),
                    pending_entries=tuple(
                        self._recovery_state_collection("pending_entries")
                    ),
                    residual_repairs=tuple(
                        self._recovery_state_collection("pending_residual_repairs")
                    ),
                    passive_closes=tuple(
                        self._recovery_state_collection("pending_passive_closes")
                    ),
                    exchange_truth={
                        "available": False,
                        "missing_evidence": ("bulk_position_probe",),
                        "venue": venue.value,
                    },
                    prior_recovery_block_reason=self.state.recovery_blocked_reason,
                    operator_fail_closed=(
                        self.state.operator.requested_mode
                        == GlobalRiskMode.FAIL_CLOSED
                    ),
                    recovery_work_items=synthetic_work_items,
                )
            )

        async def fallback_symbols_for_venue(
            venue: Venue,
            adapter: VenueAdapter,
            *,
            recovery_required_only: bool = False,
        ) -> list[str]:
            cache_key = (venue, recovery_required_only)
            if cache_key in fallback_probe_symbol_cache:
                return fallback_probe_symbol_cache[cache_key]
            if not recovery_required_only:
                symbols_for_venue = probe_symbols_by_venue.get(venue, [])
                if symbols_for_venue:
                    fallback_probe_symbol_cache[cache_key] = symbols_for_venue
                    return symbols_for_venue
            if recovery_truth_required_symbols:
                bounded_symbols = await self._position_probe_symbols_for_venue(
                    venue,
                    adapter,
                    recovery_truth_required_symbols,
                )
                max_bounded = self._MAX_BOUNDED_RECOVERY_FALLBACK_SYMBOLS
                if len(bounded_symbols) > max_bounded:
                    if getattr(self.journal, "_file", None) is not None:
                        sample_symbols = bounded_symbols[:10]
                        core_decision = truth_unavailable_core_decision(venue)
                        self._append_runtime_diagnostic_event(
                            "recovery.live_position_fallback_bounded_skipped",
                            {
                                "event_scope": "bounded_summary",
                                "venue": venue.value,
                                "fallback_symbol_count": len(bounded_symbols),
                                "max_fallback_symbol_count": max_bounded,
                                "fallback_symbols_sample": sample_symbols,
                                "omitted_symbol_count": max(
                                    len(bounded_symbols) - len(sample_symbols),
                                    0,
                                ),
                                "truth_required_by": recovery_truth_required_sources,
                                "truth_required_symbol_sources": (
                                    recovery_truth_required_symbol_source_payload
                                ),
                                "reason": "truth_required_symbol_cap_exceeded",
                                "decision": "truth_unavailable_for_required_recovery",
                                "core_decision": core_decision.kind.value,
                                "core_block_reason": core_decision.block_reason,
                                "blocking": not core_decision.entry_allowed,
                                "suppressed_count": 0,
                                "ts_ms": global_probe_started_at_ms,
                            },
                            now_ms=global_probe_started_at_ms,
                            key_parts=(
                                venue.value,
                                "truth_required_symbol_cap_exceeded",
                            ),
                            interval_ms=(
                                self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS
                            ),
                        )
                    fallback_probe_symbol_cache[cache_key] = []
                    return []
                fallback_probe_symbol_cache[cache_key] = bounded_symbols
                return bounded_symbols
            if recovery_required_only:
                fallback_probe_symbol_cache[cache_key] = []
                return []
            static_symbols = [
                str(symbol)
                for symbol in getattr(self.config, "symbols", [])
                if str(symbol)
            ]
            max_static = self._MAX_STATIC_RECOVERY_PROBE_SYMBOLS
            if len(static_symbols) > max_static:
                if getattr(self.journal, "_file", None) is not None:
                    sample_symbols = static_symbols[:10]
                    self._append_runtime_diagnostic_event(
                        "recovery.live_position_static_config_probe_skipped",
                        {
                            "event_scope": "bounded_summary",
                            "venue": venue.value,
                            "static_symbol_count": len(static_symbols),
                            "max_static_symbol_count": max_static,
                            "requested_symbol_count": len(requested_symbols),
                            "sample_symbols": sample_symbols,
                            "omitted_symbol_count": max(
                                len(static_symbols) - len(sample_symbols),
                                0,
                            ),
                            "reason": "static_universe_too_large",
                            "decision": "skip_per_symbol_fallback",
                            "suppressed_count": 0,
                            "ts_ms": global_probe_started_at_ms,
                        },
                        now_ms=global_probe_started_at_ms,
                        key_parts=(venue.value, "static_universe_too_large"),
                        interval_ms=self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS,
                    )
                fallback_probe_symbol_cache[cache_key] = []
                return []
            fallback_probe_symbol_cache[cache_key] = await self._position_probe_symbols_for_venue(
                venue,
                adapter,
                static_symbols,
            )
            return fallback_probe_symbol_cache[cache_key]

        def canonical_position_symbol(
            venue: Venue,
            adapter: VenueAdapter,
            symbol: str,
        ) -> str:
            transport = getattr(adapter, "_transport", None)
            spec = getattr(transport, "_spec", None)
            from_venue_symbol = getattr(spec, "symbol_from_venue", None)
            if callable(from_venue_symbol):
                try:
                    return str(from_venue_symbol(symbol))
                except Exception:
                    pass
            if venue == Venue.OKX:
                return str(symbol).replace("-USDT-SWAP", "USDT").replace("-SWAP", "")
            return str(symbol)

        def is_active_bulk_position(pos: PositionSnapshot) -> bool:
            return abs(getattr(pos, "quantity", 0.0)) > 1e-9

        async def fetch_all_for_venue(
            venue: Venue,
            adapter: VenueAdapter,
            venue_symbols: list[str],
            *,
            probe_batch_index: int,
            probe_batch_count: int,
        ):
            venue_probe_symbols = set(venue_symbols)
            probe_queued_at_ms = wall_clock_now_ms()
            async with semaphore:
                probe_started_at_ms = wall_clock_now_ms()
                try:
                    positions = await asyncio.wait_for(
                        adapter.fetch_all_positions(),
                        timeout=timeout_s,
                    )
                except Exception as e:
                    probe_finished_at_ms = wall_clock_now_ms()
                    payload = self._position_probe_exception_payload(
                        venue,
                        adapter,
                        e,
                    )
                    if not payload.get("symbol"):
                        payload["symbol"] = "*"
                    if not payload.get("normalized_symbol"):
                        payload["normalized_symbol"] = "*"
                    if not payload.get("venue_symbol"):
                        payload["venue_symbol"] = "*"
                    payload["symbols"] = sorted(venue_probe_symbols)
                    payload["requested_symbols"] = sorted(venue_probe_symbols)
                    payload["symbol_count"] = len(venue_probe_symbols)
                    payload.update(
                        {
                            "probe_scope": "bulk_positions",
                            "probe_queued_at_ms": probe_queued_at_ms,
                            "probe_started_at_ms": probe_started_at_ms,
                            "probe_finished_at_ms": probe_finished_at_ms,
                            "probe_elapsed_ms": max(
                                probe_finished_at_ms - probe_started_at_ms,
                                0,
                            ),
                            "global_probe_started_at_ms": global_probe_started_at_ms,
                            "global_probe_elapsed_ms": max(
                                probe_finished_at_ms - global_probe_started_at_ms,
                                0,
                            ),
                            "timeout_budget_ms": timeout_budget_ms,
                            "timeout_budget_s": timeout_s,
                            "timeout_budget_source": (
                                "runtime.live_recovery_rest_probe_timeout_ms"
                            ),
                            "timeout_trigger": (
                                "per_venue_wait_for"
                                if payload.get("classification") == "timeout"
                                else ""
                            ),
                            "global_timeout_budget_ms": 0,
                            "global_timeout_triggered": False,
                            "global_budget_applied": False,
                            "concurrency_limit": concurrency_limit,
                            "probe_batch_index": probe_batch_index,
                            "probe_batch_count": probe_batch_count,
                            "probe_batch_symbol_count": len(venue_probe_symbols),
                        }
                    )
                    if payload.get("classification") in (
                        "instrument_missing",
                        "metadata_missing",
                    ):
                        self.journal.append(
                            "recovery.live_position_bulk_probe_metadata_missing",
                            payload,
                        )
                        return (venue, [])
                    if payload.get("classification") == "rate_limited":
                        self.journal.append(
                            "recovery.live_position_probe_venue_cooldown",
                            payload,
                        )
                        return (venue, [])
                    planned_fallback_symbols = await fallback_symbols_for_venue(
                        venue,
                        adapter,
                        recovery_required_only=True,
                    )
                    core_decision = truth_unavailable_core_decision(venue)
                    fallback_planned = bool(planned_fallback_symbols)
                    blocking_required_truth = (
                        bool(recovery_truth_required_sources)
                        and not fallback_planned
                        and not core_decision.entry_allowed
                    )
                    payload.update(
                        {
                            "fallback_symbol_count": len(planned_fallback_symbols),
                            "fallback_symbols_sample": planned_fallback_symbols[:10],
                            "fallback_planned": fallback_planned,
                            "truth_required_by": recovery_truth_required_sources,
                            "truth_required_symbol_sources": (
                                recovery_truth_required_symbol_source_payload
                            ),
                            "core_decision": core_decision.kind.value,
                            "core_block_reason": core_decision.block_reason,
                            "blocking": blocking_required_truth,
                        }
                    )
                    if not recovery_truth_required_sources:
                        payload.update(
                            {
                                "diagnostic_scope": "best_effort_bulk_positions",
                                "decision": (
                                    "running_with_nonblocking_health_diagnostic"
                                ),
                                "blocking": False,
                            }
                        )
                        self._append_runtime_diagnostic_event(
                            "recovery.live_position_bulk_diagnostic_error",
                            payload,
                            now_ms=probe_finished_at_ms,
                            key_parts=(venue.value, str(payload.get("classification", ""))),
                            interval_ms=self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS,
                        )
                        return (venue, [])
                    if not planned_fallback_symbols:
                        payload["diagnostic_scope"] = "required_recovery_truth"
                        payload["decision"] = (
                            "truth_unavailable_for_required_recovery"
                        )
                        self.journal.append(
                            "recovery.required_position_truth_unavailable",
                            payload,
                        )
                        return (venue, [])
                    payload["diagnostic_scope"] = "required_recovery_truth"
                    payload["decision"] = "bounded_symbol_fallback_required"
                    self.journal.append(
                        "recovery.required_position_bulk_fallback_planned",
                        payload,
                    )
                    fallback_probe_symbol_cache[(venue, False)] = (
                        planned_fallback_symbols
                    )
                    return (venue, None)
                if positions is None:
                    return (venue, None)
                return (
                    venue,
                    [
                        (
                            canonical_position_symbol(venue, adapter, pos.symbol),
                            pos,
                        )
                        for pos in positions
                        if is_active_bulk_position(pos)
                    ],
                )

        fallback_probe_failures: dict[Venue, list[dict]] = {}

        async def fetch_one(venue: Venue, adapter: VenueAdapter, symbol: str):
            async with semaphore:
                try:
                    pos = await asyncio.wait_for(
                        adapter.fetch_position(symbol),
                        timeout=timeout_s,
                    )
                    return (symbol, pos)
                except Exception as e:
                    payload = self._position_probe_exception_payload(
                        venue,
                        adapter,
                        e,
                        symbol=symbol,
                    )
                    classification = str(payload.get("classification", ""))
                    if classification in ("instrument_missing", "metadata_missing"):
                        self.journal.append(
                            "recovery.live_position_probe_metadata_missing",
                            payload,
                        )
                    else:
                        self.journal.append(
                            "recovery.live_position_probe_error",
                            payload,
                        )
                    if recovery_truth_required_sources:
                        fallback_probe_failures.setdefault(venue, []).append(payload)
                    return None

        bulk_results = await asyncio.gather(
            *[
                fetch_all_for_venue(
                    venue,
                    adapter,
                    probe_symbols_by_venue.get(venue, []),
                    probe_batch_index=idx,
                    probe_batch_count=len(self._venue_adapters),
                )
                for idx, (venue, adapter) in enumerate(
                    self._venue_adapters.items(),
                    start=1,
                )
            ]
        )
        snapshots: list[tuple[str, PositionSnapshot]] = []
        fallback_venues: set[Venue] = set()
        for venue, positions in bulk_results:
            if positions is None:
                fallback_venues.add(venue)
            else:
                snapshots.extend(positions)

        fallback_probe_symbols: dict[Venue, list[str]] = {}
        for venue in self._venue_adapters:
            if venue not in fallback_venues:
                continue
            fallback_probe_symbols[venue] = await fallback_symbols_for_venue(
                venue,
                self._venue_adapters[venue],
            )

        tasks = [
            fetch_one(venue, adapter, symbol)
            for venue, adapter in self._venue_adapters.items()
            if venue in fallback_probe_symbols
            for symbol in fallback_probe_symbols[venue]
        ]
        results = await asyncio.gather(*tasks) if tasks else []
        if recovery_truth_required_sources and fallback_probe_failures:
            failed_symbols: list[str] = []
            failed_samples: list[dict] = []
            for venue, failures in fallback_probe_failures.items():
                for failure in failures:
                    symbol = str(
                        failure.get("normalized_symbol")
                        or failure.get("symbol")
                        or failure.get("venue_symbol")
                        or ""
                    )
                    if symbol:
                        failed_symbols.append(symbol)
                    if len(failed_samples) < 10:
                        failed_samples.append(
                            {
                                "venue": venue.value,
                                "symbol": symbol,
                                "classification": failure.get("classification", ""),
                                "endpoint": failure.get("endpoint", ""),
                            }
                        )
            self.journal.append(
                "recovery.required_position_truth_unavailable",
                {
                    "probe_scope": "bounded_symbol_positions",
                    "diagnostic_scope": "required_recovery_truth",
                    "classification": "truth_unavailable",
                    "truth_required_by": recovery_truth_required_sources,
                    "truth_required_symbol_sources": (
                        recovery_truth_required_symbol_source_payload
                    ),
                    "failed_symbol_count": len(failed_symbols),
                    "failed_symbols_sample": failed_symbols[:10],
                    "failure_samples": failed_samples,
                    "fallback_planned": True,
                    "blocking": True,
                    "decision": "truth_unavailable_for_required_recovery",
                    "ts_ms": wall_clock_now_ms(),
                },
            )
        snapshots.extend(
            item for item in results
            if item is not None and abs(getattr(item[1], "quantity", 0.0)) > 1e-9
        )
        return snapshots

    def _hydrate_balanced_startup_live_positions(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
    ) -> tuple[int, set[int]]:
        by_symbol: dict[str, list[tuple[int, PositionSnapshot]]] = {}
        for idx, (requested_symbol, pos) in enumerate(snapshots):
            by_symbol.setdefault(requested_symbol, []).append((idx, pos))

        created = 0
        recovered_indices: set[int] = set()
        for symbol, indexed_positions in by_symbol.items():
            active = [
                (idx, p) for idx, p in indexed_positions
                if abs(p.quantity) > 1e-9
            ]
            if len(active) != 2:
                continue

            (idx_a, pos_a), (idx_b, pos_b) = active
            if pos_a.venue == pos_b.venue or pos_a.side == pos_b.side:
                continue
            if abs(abs(pos_a.quantity) - abs(pos_b.quantity)) > 1e-9:
                continue

            if pos_a.side == Side.BUY:
                long_idx, long_pos = idx_a, pos_a
                short_idx, short_pos = idx_b, pos_b
            else:
                long_idx, long_pos = idx_b, pos_b
                short_idx, short_pos = idx_a, pos_a

            if self._has_open_position_pair(symbol, long_pos.venue, short_pos.venue):
                recovered_indices.update({long_idx, short_idx})
                continue

            position_id = (
                f"live-recovered:{symbol}:"
                f"{long_pos.venue.value}->{short_pos.venue.value}"
            )
            matched_quantity = abs(long_pos.quantity)
            position = OpenPosition(
                position_id=position_id,
                symbol=symbol,
                long_venue=long_pos.venue,
                short_venue=short_pos.venue,
                long_quantity=abs(long_pos.quantity),
                short_quantity=abs(short_pos.quantity),
                long_entry_price=long_pos.entry_price,
                short_entry_price=short_pos.entry_price,
                opened_at_ms=now_ms,
                matched_quantity=matched_quantity,
                opportunity_hint_source=source,
            )
            self.state.open_positions[position_id] = position
            recovered_indices.update({long_idx, short_idx})
            self.journal.append(
                "recovery.live_detected",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "quantity": position.matched_quantity,
                    "long_quantity": position.long_quantity,
                    "short_quantity": position.short_quantity,
                    "long_entry_price": position.long_entry_price,
                    "short_entry_price": position.short_entry_price,
                    "opened_at_ms": position.opened_at_ms,
                    "matched_quantity": position.matched_quantity,
                    "opportunity_hint_source": position.opportunity_hint_source,
                    "source": source,
                    "ts_ms": now_ms,
                },
            )
            created += 1

        return created, recovered_indices

    def _has_open_position_pair(
        self, symbol: str, long_venue: Venue, short_venue: Venue
    ) -> bool:
        return any(
            pos.symbol == symbol
            and pos.long_venue == long_venue
            and pos.short_venue == short_venue
            for pos in self.state.open_positions.values()
        )

    async def _maybe_check_active_position_drift(self, now_ms: int) -> None:
        if str(getattr(self.config.runtime, "mode", "")).lower() != "live":
            return
        if not self.state.open_positions:
            return

        interval_ms = max(self.config.runtime.private_position_max_age_ms, 1)
        if (
            self._last_position_drift_check_ms > 0
            and now_ms < self._last_position_drift_check_ms + interval_ms
        ):
            return
        self._last_position_drift_check_ms = now_ms

        for position in list(self.state.open_positions.values()):
            if position.position_id in self.state.pending_passive_closes:
                ppc = self.state.pending_passive_closes[position.position_id]
                phase_state = getattr(ppc, "phase_state", None)
                reason = "pending_passive_close_owner"
                if getattr(phase_state, "maker_order_id", ""):
                    reason = "maker_order_active"
                elif getattr(phase_state, "maker_client_order_id", ""):
                    reason = "maker_client_order_active"
                elif (
                    bool(getattr(ppc, "short_legs", None))
                    or bool(getattr(ppc, "long_legs", None))
                ):
                    reason = "passive_close_live_action_settling"
                elif int(getattr(ppc, "next_retry_at_ms", 0) or 0) > now_ms:
                    reason = "passive_close_retry_scheduled"
                self.journal.append(
                    "runtime.position_drift_skipped_passive_close_owner",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "reason": reason,
                        "phase": str(getattr(phase_state, "phase", "") or ""),
                        "next_retry_at_ms": int(
                            getattr(ppc, "next_retry_at_ms", 0) or 0
                        ),
                        "ts_ms": now_ms,
                    },
                )
                continue
            if any(
                pending.position_id == position.position_id
                for pending in self.state.pending_closes.values()
            ):
                continue

            long_adapter = self.get_venue_adapter(position.long_venue)
            short_adapter = self.get_venue_adapter(position.short_venue)
            if long_adapter is None or short_adapter is None:
                continue

            try:
                long_pos = await long_adapter.fetch_position(position.symbol)
                short_pos = await short_adapter.fetch_position(position.symbol)
            except Exception as e:
                self.journal.append(
                    "runtime.position_drift_probe_error",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "error": str(e),
                    },
                )
                continue

            expected_long = abs(position.long_quantity or position.matched_quantity)
            expected_short = abs(position.short_quantity or position.matched_quantity)

            def valid_live_quantities(
                long_snapshot: PositionSnapshot,
                short_snapshot: PositionSnapshot,
            ) -> tuple[float, float]:
                long_qty = (
                    abs(long_snapshot.quantity)
                    if long_snapshot.side == Side.BUY
                    and abs(long_snapshot.quantity) > 1e-9
                    else 0.0
                )
                short_qty = (
                    abs(short_snapshot.quantity)
                    if short_snapshot.side == Side.SELL
                    and abs(short_snapshot.quantity) > 1e-9
                    else 0.0
                )
                return long_qty, short_qty

            long_valid_qty, short_valid_qty = valid_live_quantities(
                long_pos,
                short_pos,
            )

            if (
                abs(long_valid_qty - expected_long) <= 1e-9
                and abs(short_valid_qty - expected_short) <= 1e-9
            ):
                continue

            balanced_quantity = min(long_valid_qty, short_valid_qty)
            self.journal.append(
                "runtime.position_drift_detected",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "expected_long_quantity": expected_long,
                    "expected_short_quantity": expected_short,
                    "actual_long_side": long_pos.side.value,
                    "actual_long_quantity": long_pos.quantity,
                    "actual_short_side": short_pos.side.value,
                    "actual_short_quantity": short_pos.quantity,
                    "balanced_quantity": balanced_quantity,
                    "ts_ms": now_ms,
                },
            )

            long_excess = (
                abs(long_pos.quantity) - balanced_quantity
                if abs(long_pos.quantity) > 1e-9
                else 0.0
            )
            short_excess = (
                abs(short_pos.quantity) - balanced_quantity
                if abs(short_pos.quantity) > 1e-9
                else 0.0
            )
            long_ok = True
            short_ok = True
            if long_excess > 1e-9:
                long_ok = await self._flatten_live_position_leg_quantity(
                    position.long_venue,
                    position.symbol,
                    long_pos,
                    long_excess,
                    position.position_id,
                    "runtime_drift_flatten_long",
                )
            if short_excess > 1e-9:
                short_ok = await self._flatten_live_position_leg_quantity(
                    position.short_venue,
                    position.symbol,
                    short_pos,
                    short_excess,
                    position.position_id,
                    "runtime_drift_flatten_short",
                )

            if long_ok is not True or short_ok is not True:
                try:
                    refreshed_long_pos = await long_adapter.fetch_position(
                        position.symbol
                    )
                    refreshed_short_pos = await short_adapter.fetch_position(
                        position.symbol
                    )
                    refreshed_long_qty, refreshed_short_qty = valid_live_quantities(
                        refreshed_long_pos,
                        refreshed_short_pos,
                    )
                    if (
                        abs(refreshed_long_qty - balanced_quantity) <= 1e-9
                        and abs(refreshed_short_qty - balanced_quantity) <= 1e-9
                    ):
                        long_pos = refreshed_long_pos
                        short_pos = refreshed_short_pos
                        long_valid_qty = refreshed_long_qty
                        short_valid_qty = refreshed_short_qty
                        long_ok = True
                        short_ok = True
                        self.journal.append(
                            "runtime.position_drift_correction_verified",
                            {
                                "position_id": position.position_id,
                                "symbol": position.symbol,
                                "source": "post_flatten_live_truth",
                                "balanced_quantity": balanced_quantity,
                                "long_flatten_result": long_ok,
                                "short_flatten_result": short_ok,
                                "live_long_quantity": long_valid_qty,
                                "live_short_quantity": short_valid_qty,
                                "ts_ms": now_ms,
                            },
                        )
                except Exception as e:
                    self.journal.append(
                        "runtime.position_drift_correction_verify_error",
                        {
                            "position_id": position.position_id,
                            "symbol": position.symbol,
                            "error": str(e),
                            "ts_ms": now_ms,
                        },
                    )

            if long_ok is not True or short_ok is not True:
                enter_fail_closed(self.state)
                self.state.recovery_blocked_reason = "position_drift_correction_failed"
                self.state.recovery_blocked_at_ms = now_ms
                self.state.last_error = "position drift correction failed"
                self.journal.append(
                    "runtime.position_drift_correction_failed",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "long_flatten_result": long_ok,
                        "short_flatten_result": short_ok,
                        "ts_ms": now_ms,
                    },
                )
                continue

            if balanced_quantity <= 1e-9:
                self.state.open_positions.pop(position.position_id, None)
                self.journal.append(
                    "recovery.flat",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "source": "runtime_position_drift",
                        "ts_ms": now_ms,
                    },
                )
            else:
                current = self.state.open_positions.get(position.position_id)
                if current is not None:
                    current.long_quantity = balanced_quantity
                    current.short_quantity = balanced_quantity
                    current.matched_quantity = balanced_quantity
                self.journal.append(
                    "runtime.position_drift_corrected",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "new_quantity": balanced_quantity,
                        "ts_ms": now_ms,
                    },
                )
            self._sync_passive_order_manager_states()
            self.snapshot_store.write(build_persistent_state_view(self.state))

    async def _flatten_live_position_leg_quantity(
        self,
        venue: Venue,
        symbol: str,
        live_position: PositionSnapshot,
        quantity: float,
        position_id: str,
        stage: str,
    ) -> bool | None:
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None
        if quantity <= 1e-9:
            return True

        cleanup_side = live_position.side.opposite()
        from lightfee.venues.cid import generate_exchange_cid
        cleanup_client_order_id = generate_exchange_cid(
            f"{position_id}:{stage}:{symbol}", "c", venue
        )
        self.journal.append(
            "runtime.position_drift_flatten_leg",
            {
                "position_id": position_id,
                "stage": stage,
                "venue": venue.value,
                "symbol": symbol,
                "live_side": live_position.side.value,
                "quantity": quantity,
                "cleanup_side": cleanup_side.value,
                "cleanup_client_order_id": cleanup_client_order_id,
            },
        )

        try:
            from lightfee.core.domain import OrderRequest

            req = OrderRequest(
                venue=venue,
                symbol=symbol,
                side=cleanup_side,
                quantity=abs(quantity),
                price=None,
                post_only=False,
                reduce_only=True,
                client_order_id=cleanup_client_order_id,
            )
            fill = await adapter.place_order(req)
            self._flush_adapter_order_diagnostics(adapter)
            return fill.quantity >= abs(quantity) - 1e-9
        except Exception:
            self._flush_adapter_order_diagnostics(adapter)
            return False

    async def _activate_local_l2_phase(self, now_ms: int):
        return await self.market_data_runtime._activate_local_l2_phase(now_ms)

    async def _ensure_l2_active_for_candidates(self, candidates, now_ms: int, *, tracked_opportunities=None):
        return await self.market_data_runtime._ensure_l2_active_for_candidates(candidates, now_ms, tracked_opportunities=tracked_opportunities)

    async def _ensure_entry_bbo_active_for_candidates(self, candidates, now_ms: int):
        return await self.market_data_runtime._ensure_entry_bbo_active_for_candidates(candidates, now_ms)

    def _select_v1_entry_tracked_scope(self, candidates) -> tuple[list, list]:
        """Return V1 primary+shadow tracked opportunities and candidates."""
        from lightfee.engine.entry_local_l2 import select_tracked_opportunities

        source_candidates = list(candidates or [])
        primary_count = min(
            max(int(getattr(self.config.strategy, "entry_local_l2_primary_count", 0) or 0), 0),
            max(int(getattr(self.config.strategy, "max_concurrent_positions", 1) or 1), 1),
        )
        shadow_count = max(
            int(
                getattr(
                    self.config.strategy,
                    "shadow_entry_opportunity_count",
                    getattr(self.config.strategy, "entry_local_l2_shadow_count", 0),
                )
                or 0
            ),
            0,
        )
        tracked = select_tracked_opportunities(
            source_candidates,
            primary_count,
            shadow_count,
        )
        tracked_pair_ids = {opportunity.pair_id for opportunity in tracked}
        tracked_candidates = [
            candidate
            for candidate in source_candidates
            if self._candidate_pair_id(candidate) in tracked_pair_ids
        ]
        return tracked, tracked_candidates

    @staticmethod
    def _entry_quote_truth_empty_stats():
        return MarketDataRuntime._entry_quote_truth_empty_stats()

    def _entry_quote_truth_record_last_scan(self, stats: dict[str, Any]):
        return self.market_data_runtime._entry_quote_truth_record_last_scan(stats)

    def _entry_quote_probe_diagnostics_enabled(self):
        return self.market_data_runtime._entry_quote_probe_diagnostics_enabled()

    def _emit_entry_quote_revalidate_probe(self, *, stats: dict[str, Any], candidate_count: int, now_ms: int):
        return self.market_data_runtime._emit_entry_quote_revalidate_probe(stats=stats, candidate_count=candidate_count, now_ms=now_ms)

    def _entry_quote_truth_overlay_quote(self, overlay: dict[tuple[str, str], Any] | None, venue: str, symbol: str):
        return self.market_data_runtime._entry_quote_truth_overlay_quote(overlay, venue, symbol)

    def _entry_quote_truth_market_quotes(self, market_quotes: Any, overlay: dict[tuple[str, str], Any] | None):
        return self.market_data_runtime._entry_quote_truth_market_quotes(market_quotes, overlay)

    def _entry_quote_truth_price_hint(self, candidate: Any, *, price_hints: dict[str, float], overlay: dict[tuple[str, str], Any] | None):
        return self.market_data_runtime._entry_quote_truth_price_hint(candidate, price_hints=price_hints, overlay=overlay)

    def _entry_quote_revalidate_need(self, *, snapshot, quote: Any, now_ms: int, fallback_source: str):
        return self.market_data_runtime._entry_quote_revalidate_need(snapshot=snapshot, quote=quote, now_ms=now_ms, fallback_source=fallback_source)

    def _entry_quote_revalidate_targets(self, candidates: list, *, snapshot, now_ms: int):
        return self.market_data_runtime._entry_quote_revalidate_targets(candidates, snapshot=snapshot, now_ms=now_ms)

    def _entry_quote_truth_fresh_quote(self, venue: str, symbol: str, *, now_ms: int):
        return self.market_data_runtime._entry_quote_truth_fresh_quote(venue, symbol, now_ms=now_ms)

    def _entry_quote_truth_refresher(self):
        return self.market_data_runtime._entry_quote_truth_refresher()

    def _entry_quote_truth_accept_quote(self, quote: Any, *, now_ms: int):
        return self.market_data_runtime._entry_quote_truth_accept_quote(quote, now_ms=now_ms)

    async def _entry_quote_revalidate_for_candidates(self, candidates: list, *, snapshot, now_ms: int, candidate_scope: str='', skipped_untracked_count: int=0):
        return await self.market_data_runtime._entry_quote_revalidate_for_candidates(candidates, snapshot=snapshot, now_ms=now_ms, candidate_scope=candidate_scope, skipped_untracked_count=skipped_untracked_count)

    async def _refresh_entry_candidate_open_interest_evidence(self, candidates: list, *, snapshot, now_ms: int):
        return await self.market_data_runtime._refresh_entry_candidate_open_interest_evidence(candidates, snapshot=snapshot, now_ms=now_ms)

    def _clear_local_l2_runtime_state(self) -> None:
        """Drop Local-L2 runtime and persisted snapshots for non-Local-L2 profiles."""
        self.state.retained_local_l2_books = []
        self.state.local_l2_books_snapshot = []
        self.state.local_l2_session_snapshot = []
        self.local_l2_runtime.books.clear()
        self.local_l2_runtime.assignments.clear()
        self.local_l2_runtime.leases.clear()
        self.local_l2_runtime.pending_events.clear()
        self.local_l2_runtime._refresh_metrics(wall_clock_now_ms())
        self.entry_l2_sessions.sessions.clear()
        self.entry_l2_sessions.sticky_pair_ids.clear()

    async def _restore_local_l2_state(self) -> None:
        """Phase 6: Restore retained local-L2 books and session state from snapshot.

        V1: Restores PersistedRetainedLocalL2Book including bids/asks book data
        and generation tracking for stale-snapshot detection.
        """
        from lightfee.marketdata.l2 import PriceLevel

        if not self._local_l2_effective_enabled():
            self._clear_local_l2_runtime_state()
            return
        if not hasattr(self.state, "local_l2_books_snapshot"):
            return
        snap = getattr(self.state, "local_l2_books_snapshot", None)
        if not snap:
            return
        active_owner_pairs: set[tuple[str, str]] = set()

        def remember_owner_pair(venue, symbol) -> None:
            ven_str = venue.value if hasattr(venue, "value") else str(venue or "")
            sym = str(symbol or "")
            if ven_str and sym:
                active_owner_pairs.add((ven_str, sym))

        for book in getattr(self.state, "retained_local_l2_books", []) or []:
            remember_owner_pair(book.get("venue", ""), book.get("symbol", ""))

        open_positions = getattr(self.state, "open_positions", {}) or {}
        open_position_values = (
            open_positions.values() if hasattr(open_positions, "values") else open_positions
        )
        for position in open_position_values:
            sym = getattr(position, "symbol", "")
            remember_owner_pair(getattr(position, "long_venue", ""), sym)
            remember_owner_pair(getattr(position, "short_venue", ""), sym)
            remember_owner_pair(getattr(position, "venue", ""), sym)

        pending_entries = getattr(self.state, "pending_entries", {}) or {}
        pending_entry_values = (
            pending_entries.values() if hasattr(pending_entries, "values") else pending_entries
        )
        for pending in pending_entry_values:
            sym = getattr(pending, "symbol", "")
            remember_owner_pair(getattr(pending, "long_venue", ""), sym)
            remember_owner_pair(getattr(pending, "short_venue", ""), sym)

        pending_passive_closes = getattr(self.state, "pending_passive_closes", {}) or {}
        pending_close_values = (
            pending_passive_closes.values()
            if hasattr(pending_passive_closes, "values")
            else pending_passive_closes
        )
        for pending_close in pending_close_values:
            position = getattr(pending_close, "position_snapshot", None)
            if position is None:
                continue
            sym = getattr(position, "symbol", "")
            remember_owner_pair(getattr(position, "long_venue", ""), sym)
            remember_owner_pair(getattr(position, "short_venue", ""), sym)

        allowed_pairs: set[tuple[str, str]] = set()
        if self.config.runtime.mode != "paper":
            from lightfee.core.domain import Venue as VenueEnum

            venue_symbols: dict[str, list[str]] = {}
            for entry in snap:
                venue = entry.get("venue", "")
                symbol = entry.get("symbol", "")
                if venue and symbol:
                    venue_symbols.setdefault(venue, []).append(symbol)

            for venue_str, symbols in venue_symbols.items():
                try:
                    venue_enum = VenueEnum.from_str(venue_str)
                    adapter = (
                        self.get_venue_adapter(venue_enum)
                        if venue_enum in self._venue_adapters
                        else None
                    )
                except (ValueError, KeyError):
                    adapter = None
                    venue_enum = None
                if adapter is None or venue_enum is None:
                    allowed_pairs.update((venue_str, symbol) for symbol in symbols)
                    continue
                filtered_symbols = await self._filter_symbols_supported_by_venue(
                    venue_enum,
                    adapter,
                    sorted(set(symbols)),
                    skip_event_kind="runtime.local_l2_symbol_skipped",
                )
                allowed_pairs.update((venue_str, symbol) for symbol in filtered_symbols)
        else:
            allowed_pairs = {
                (entry.get("venue", ""), entry.get("symbol", ""))
                for entry in snap
                if entry.get("venue", "") and entry.get("symbol", "")
            }

        for entry in snap:
            venue = entry.get("venue", "")
            symbol = entry.get("symbol", "")
            if not venue or not symbol:
                continue
            if (venue, symbol) not in allowed_pairs:
                continue
            pool_str = str(entry.get("pool", "dropped"))
            if (
                pool_str in {
                    L2PoolAssignment.HOT_EXEC.value,
                    L2PoolAssignment.WARM.value,
                }
                and (venue, symbol) not in active_owner_pairs
            ):
                continue
            book = self.local_l2_runtime.ensure_book(venue, symbol)
            book.last_update_id = entry.get("last_update_id", 0)
            book.sequence = entry.get("sequence", 0)
            book.last_snapshot_ms = entry.get("last_snapshot_ms", 0)
            book.last_delta_ms = entry.get("last_delta_ms", 0)
            # V1: restore generation for stale-snapshot gating
            if hasattr(book, 'generation'):
                book.generation = entry.get("generation", 1)
            # V1: restore book data (bids/asks) if available
            if entry.get("bids"):
                book.bids = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["bids"]]
            if entry.get("asks"):
                book.asks = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["asks"]]
            # Restore the persisted pool — only RETAINED books should be
            # re-bootstrapped at startup (V1: retained_local_l2_books).
            try:
                book.pool = L2PoolAssignment(pool_str)
            except ValueError:
                book.pool = L2PoolAssignment.DROPPED
            # V1: retained books bootstrap directly (retained_local_l2_books)
            if book.pool == L2PoolAssignment.RETAINED:
                if book.status in (L2BookStatus.COLD, L2BookStatus.RESUME_WAITING):
                    book.transition_to_bootstrapping(0)
            # Restored book is never automatically HOT — must prove freshness.
            # But don't overwrite a book that is already being bootstrapped
            # (set by _activate_local_l2_phase for retained/hot symbols).
            elif book.status in (L2BookStatus.COLD,):
                book.status = L2BookStatus.RESUME_WAITING

    def _sync_passive_order_manager_states(self) -> None:
        """Write _maker_event_state manager runtime dicts to EngineState for snapshot."""
        from lightfee.engine.passive_order_manager import PassiveOrderManager
        states: dict[str, dict] = {}
        for entry_id, stored in self._maker_event_state.items():
            if isinstance(stored, tuple) and len(stored) == 2:
                manager, price = stored
                if isinstance(manager, PassiveOrderManager):
                    d = manager.runtime_dict()
                    d["maker_price"] = price
                    states[entry_id] = d
                else:
                    states[entry_id] = {"maker_price": price}
            elif isinstance(stored, dict):
                states[entry_id] = dict(stored)
        self.state.passive_order_manager_states = states

    def _restore_passive_order_manager_states(self) -> None:
        """Restore PassiveOrderManager states from EngineState after snapshot recovery."""
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerProfile,
        )
        profile = PassiveOrderManagerProfile(
            max_consecutive_failures=self.config.strategy.passive_max_consecutive_failures,
            failure_cooldown_ms=self.config.strategy.passive_failure_cooldown_ms,
            reprice_threshold_bps=self.config.strategy.passive_reprice_threshold_bps,
            cancel_replace_threshold_bps=self.config.strategy.passive_cancel_replace_threshold_bps,
        )
        restored: dict[str, object] = {}
        for entry_id, d in self.state.passive_order_manager_states.items():
            if not isinstance(d, dict):
                continue
            manager = PassiveOrderManager(profile)
            # Restore runtime state fields
            if d.get("consecutive_failures", 0) > 0:
                last_action = d.get("last_action_at_ms")
                if last_action is not None:
                    for _ in range(min(d.get("consecutive_failures", 0), profile.max_consecutive_failures)):
                        manager.note_failure(last_action)
            if d.get("ops_bucket_tokens") is not None:
                manager._ops_bucket_tokens = float(d["ops_bucket_tokens"])
            if d.get("cooldown_until_ms") is not None:
                manager._cooldown_until_ms = d["cooldown_until_ms"]
            # V1: restore refill anchor so next _refill_ops_bucket() does not
            # reset tokens to capacity (passive_order_manager.rs:341)
            if d.get("ops_bucket_last_refill_at_ms") is not None:
                manager._ops_bucket_last_refill_at_ms = d["ops_bucket_last_refill_at_ms"]
            if d.get("last_action_at_ms") is not None:
                manager._last_action_at_ms = d["last_action_at_ms"]
            price = float(d.get("maker_price", 0.0))
            restored[entry_id] = (manager, price)
        if restored:
            self._maker_event_state.update(restored)

    async def stop(self) -> None:
        """Graceful shutdown: stop loop, WS clients, adapter shutdown, export final state, flush journal."""
        self._running = False
        shutdown_timeout_s = max(
            int(getattr(self.config.runtime, "shutdown_grace_period_ms", 3000) or 3000),
            1,
        ) / 1000.0
        loop = asyncio.get_running_loop()
        shutdown_deadline = loop.time() + shutdown_timeout_s

        def _remaining_shutdown_timeout_s() -> float:
            return max(shutdown_deadline - loop.time(), 0.001)

        def _journal_shutdown_stage(stage: str, **payload) -> None:
            try:
                self.journal.append(
                    "runtime.shutdown_stage",
                    {"stage": stage, "ts_ms": wall_clock_now_ms(), **payload},
                    flush=True,
                )
            except Exception:
                logger.exception("shutdown stage=%s task=journal status=error", stage)

        async def _await_shutdown_task(stage: str, task_name: str, coro) -> bool:
            timeout_s = _remaining_shutdown_timeout_s()

            def _consume_result(task: asyncio.Task) -> None:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.warning(
                        "shutdown stage=%s task=%s status=late_error error=%s",
                        stage,
                        task_name,
                        exc,
                    )

            task = asyncio.create_task(coro, name=f"shutdown:{task_name}")
            done, pending = await asyncio.wait({task}, timeout=timeout_s)
            if task in done:
                try:
                    task.result()
                except asyncio.CancelledError:
                    return False
                except Exception as exc:
                    logger.warning(
                        "shutdown stage=%s task=%s status=error error=%s",
                        stage,
                        task_name,
                        exc,
                    )
                    _journal_shutdown_stage(
                        "error",
                        blocked_stage=stage,
                        task=task_name,
                        error=str(exc),
                    )
                    return False
                return True

            for pending_task in pending:
                pending_task.cancel()
                pending_task.add_done_callback(_consume_result)
            logger.error(
                "shutdown stage=%s task=%s status=timeout timeout_s=%.3f",
                stage,
                task_name,
                timeout_s,
            )
            _journal_shutdown_stage(
                "timeout",
                blocked_stage=stage,
                task=task_name,
                timeout_s=timeout_s,
            )
            return False

        def _stop_ws_streams_coro(owner, timeout_s: float):
            stop_fn = getattr(owner, "stop_ws_streams")
            try:
                return stop_fn(per_client_timeout_s=timeout_s)
            except TypeError as exc:
                if "per_client_timeout_s" not in str(exc):
                    raise
                return stop_fn()

        logger.info("shutdown stage=close_network")
        _journal_shutdown_stage("close_network")

        ws_bbo_data_plane = getattr(self, "ws_bbo_data_plane", None)
        if ws_bbo_data_plane is not None:
            await _await_shutdown_task(
                "close_network",
                "ws_bbo_data_plane.stop_ws_streams",
                _stop_ws_streams_coro(
                    ws_bbo_data_plane,
                    _remaining_shutdown_timeout_s(),
                ),
            )

        entry_open_interest_refresher = getattr(
            self,
            "entry_open_interest_refresher",
            None,
        )
        if entry_open_interest_refresher is not None and hasattr(
            entry_open_interest_refresher,
            "close",
        ):
            await _await_shutdown_task(
                "close_network",
                "entry_open_interest_refresher.close",
                entry_open_interest_refresher.close(),
            )

        # Stop WebSocket L2 streams (V1: abort workers before adapter shutdown)
        await _await_shutdown_task(
            "close_network",
            "l2_data_plane.stop_ws_streams",
            _stop_ws_streams_coro(
                self.l2_data_plane,
                _remaining_shutdown_timeout_s(),
            ),
        )

        # V1: stop private WS workers before adapter shutdown
        for venue, adapter in list(self._venue_adapters.items()):
            if getattr(adapter, "supports_private_health", False):
                transport = getattr(adapter, "_transport", None)
                if transport is not None:
                    transport.stop_private_ws()
                    self.journal.append(
                        "runtime.private_ws_stopped",
                        {"venue": venue.value},
                    )

        # V1 parity: per-adapter shutdown (cancels workers, flushes state)
        for venue, adapter in list(self._venue_adapters.items()):
            ok = await _await_shutdown_task(
                "close_network",
                f"adapter.shutdown:{venue.value}",
                adapter.shutdown(),
            )
            if not ok:
                self.journal.append(
                    "runtime.adapter_shutdown_error",
                    {"venue": venue.value, "error": "shutdown timeout or error"},
                )

        logger.info("shutdown stage=flush_state")
        _journal_shutdown_stage("flush_state")

        # Rate-limit runtime flush
        if self._rate_limit_runtime is not None:
            try:
                self._rate_limit_runtime.flush_recommendations()
            except Exception as exc:
                logger.warning(
                    "shutdown stage=flush_state task=rate_limit_runtime.flush_recommendations status=error error=%s",
                    exc,
                )

        # Final state snapshot
        if self.state:
            self._sync_passive_order_manager_states()
            self.snapshot_store.write(build_persistent_state_view(self.state))

        # Final current-state export
        now_ms = wall_clock_now_ms()
        path = self.config.persistence.snapshot_path.replace(".json", "-current.json")
        self._maybe_export_current_state_snapshot(self._export_state, now_ms)

        self.journal.append("runtime.stopped", {"ts_ms": wall_clock_now_ms()})
        _journal_shutdown_stage("exit_complete")
        self.journal.close()
        logger.info("shutdown stage=exit_complete")

    # ------------------------------------------------------------------
    # Tick lanes
    # ------------------------------------------------------------------

    async def tick(self) -> None:
        """Full engine tick: consume snapshot, scan, supervise, manage positions."""
        now_ms = wall_clock_now_ms()
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1
        try:
            self._maybe_export_current_state_snapshot(ExportState(), now_ms)
        except Exception as exc:
            self.journal.append(
                "runtime.current_state_heartbeat_export_error",
                {"error": str(exc)},
            )

        # --- Load sidecar snapshot ---
        snapshot = load_snapshot(self.config.runtime.sidecar_snapshot_path)
        max_age = self.config.runtime.sidecar_snapshot_max_age_ms
        last_good_max_age = self.config.runtime.live_scan_last_good_max_age_ms

        # V1: evaluate_snapshot_freshness — multi-state freshness evaluation
        freshness = evaluate_snapshot_freshness(
            snapshot=snapshot,
            max_age_ms=max_age,
            now_ms=now_ms,
            last_good=self._last_good_snapshot,
            last_good_max_age_ms=last_good_max_age,
            market_max_age_ms=self.config.runtime.max_market_age_ms,
        )
        if freshness == SnapshotFreshness.MISSING:
            self._live_scan_success_streak = 0
            self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
            return
        if freshness == SnapshotFreshness.STALE:
            self._live_scan_success_streak = 0
            self.journal.append(
                "runtime.snapshot_stale",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="stale",
                ),
            )
            return
        if freshness == SnapshotFreshness.DEGRADED:
            # Some venues degraded but can still trade on healthy ones
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot
            self.journal.append(
                "runtime.snapshot_degraded",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="degraded",
                ),
            )
        if freshness == SnapshotFreshness.LAST_GOOD_FALLBACK:
            # Current snapshot is stale/missing; fall back to last good
            snapshot = snapshot if snapshot is not None else self._last_good_snapshot
            if snapshot is None:
                self._live_scan_success_streak = 0
                self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
                return
            self._last_good_snapshot = snapshot
            self._live_scan_success_streak += 1
            self.journal.append(
                "runtime.snapshot_fallback_last_good",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="last_good_fallback",
                ),
            )
        if freshness == SnapshotFreshness.FRESH:
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot

        self.state.last_scan = {
            "ts_ms": now_ms,
            "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
            "candidate_count": len(snapshot.candidates) if snapshot is not None else 0,
            "tradeable_count": 0,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
            "max_concurrent_positions": max(self.config.strategy.max_concurrent_positions, 1),
            "open_position_count": len(self.state.open_positions),
            "remaining_slots": max(
                max(self.config.strategy.max_concurrent_positions, 1)
                - len(self.state.open_positions),
                0,
            ),
            "degraded_venues": list(getattr(snapshot, "degraded_venues", [])) if snapshot is not None else [],
            "no_entry_reason": None,
        }
        (
            snapshot_freshness_metrics,
            snapshot_freshness_ages,
            snapshot_freshness_budgets,
            snapshot_freshness_publish_intervals,
            snapshot_freshness_status,
        ) = (
            self._snapshot_freshness_observability(
                snapshot=snapshot,
                candidates=[],
                now_ms=now_ms,
            )
        )
        self.state.last_scan["snapshot_freshness_metrics"] = snapshot_freshness_metrics
        self.state.last_scan["snapshot_freshness_observed_age_ms"] = snapshot_freshness_ages
        self.state.last_scan["snapshot_freshness_budget_ms"] = snapshot_freshness_budgets
        self.state.last_scan["snapshot_freshness_publish_interval_ms"] = (
            snapshot_freshness_publish_intervals
        )
        self.state.last_scan["snapshot_freshness_status"] = snapshot_freshness_status
        self.state.last_scan["snapshot_freshness_candidate_scope"] = "global_snapshot"
        self.state.last_scan["snapshot_freshness_candidate_count"] = 0
        self.state.last_scan["snapshot_freshness_all_candidate_count"] = (
            len(getattr(snapshot, "candidates", []) or [])
            if snapshot is not None
            else 0
        )
        self.state.last_scan["snapshot_freshness_skipped_untracked_count"] = (
            self.state.last_scan["snapshot_freshness_all_candidate_count"]
        )
        try:
            self._maybe_export_current_state_snapshot(ExportState(), now_ms)
        except Exception as exc:
            self.journal.append(
                "runtime.current_state_scan_progress_export_error",
                {"error": str(exc)},
            )
        if freshness == SnapshotFreshness.LAST_GOOD_FALLBACK:
            self.journal.append(
                "runtime.live_scan_revalidate_required",
                {
                    "reason": "live_scan_revalidate_required:last_good_sidecar",
                    "fallback_source": "last_good_sidecar",
                    "targeted_revalidate_required": True,
                    "targeted_revalidate_outcome": "required_before_entry",
                    "targeted_revalidate_scope": "entry_candidate",
                    "candidate_count": len(snapshot.candidates) if snapshot is not None else 0,
                    "edge_buffer_bps": self.config.runtime.live_scan_revalidate_edge_buffer_bps,
                    "blocking": False,
                    "ts_ms": now_ms,
                },
            )

        # V1 pre-scan L2 sync: refresh execution-owned books only (scan_promoted=False)
        await self._sync_local_l2_data(now_ms, scan_promoted=False)

        # --- Build price lookup from snapshot quotes ---
        price_hints: dict[str, float] = {}
        stale_quote_records: list[tuple[tuple[str, str], dict]] = []
        stale_quote_diagnostics_emitted = False
        for quote_key, quote in snapshot.quotes.items():
            quote_observed_at_ms = (
                int(getattr(quote, "observed_at_ms", 0) or 0)
                or int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
            )
            quote_age_ms = (
                now_ms - quote_observed_at_ms
                if quote_observed_at_ms > 0
                else 0
            )
            venue = str(getattr(quote, "venue", "") or "")
            symbol = str(getattr(quote, "symbol", "") or "")
            if (not venue or not symbol) and ":" in str(quote_key):
                key_venue, key_symbol = str(quote_key).split(":", 1)
                venue = venue or key_venue
                symbol = symbol or key_symbol
            quote_scope_key = (venue.lower(), symbol.upper())
            if (
                quote_observed_at_ms > 0
                and quote_age_ms > self.config.runtime.max_order_quote_age_ms
            ):
                sample = {
                    "venue": venue,
                    "symbol": symbol,
                    "quote_age_ms": quote_age_ms,
                    "observed_at_ms": quote_observed_at_ms,
                    "max_age_ms": self.config.runtime.max_order_quote_age_ms,
                    "fallback_source": getattr(
                        snapshot,
                        "acquisition_mode",
                        "sidecar_snapshot",
                    ),
                    "blocker_family": "stale_quote",
                }
                stale_quote_records.append((quote_scope_key, sample))
                continue
            price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0 if quote.bid > 0 and quote.ask > 0 else 0.0

        def emit_stale_quote_diagnostics(
            entry_quote_keys: set[tuple[str, str]],
            resolved_quote_keys: set[tuple[str, str]] | None = None,
        ) -> None:
            nonlocal stale_quote_diagnostics_emitted
            if stale_quote_diagnostics_emitted:
                return
            stale_quote_diagnostics_emitted = True
            resolved_quote_keys = resolved_quote_keys or set()
            stale_order_quote_samples: list[dict] = []
            stale_health_quote_samples: list[dict] = []
            stale_order_quote_count = 0
            stale_health_quote_count = 0
            for quote_scope_key, sample in stale_quote_records:
                if quote_scope_key in resolved_quote_keys:
                    continue
                if quote_scope_key in entry_quote_keys:
                    stale_order_quote_count += 1
                    if len(stale_order_quote_samples) < 10:
                        stale_order_quote_samples.append(sample)
                else:
                    stale_health_quote_count += 1
                    if len(stale_health_quote_samples) < 10:
                        stale_health_quote_samples.append({
                            **sample,
                            "diagnostic_scope": "all_snapshot_quotes",
                            "blocking": False,
                        })
            if stale_order_quote_count > 0:
                self.journal.append(
                    "runtime.order_quote_stale_skipped",
                    {
                        "count": stale_order_quote_count,
                        "max_age_ms": self.config.runtime.max_order_quote_age_ms,
                        "samples": stale_order_quote_samples,
                        "ts_ms": now_ms,
                    },
                )
            if stale_health_quote_count > 0:
                self._append_runtime_diagnostic_event(
                    "runtime.order_quote_stale_health_summary",
                    {
                        "count": stale_health_quote_count,
                        "max_age_ms": self.config.runtime.max_order_quote_age_ms,
                        "samples": stale_health_quote_samples,
                        "diagnostic_scope": "all_snapshot_quotes",
                        "blocking": False,
                        "suppressed_count": 0,
                        "ts_ms": now_ms,
                    },
                    now_ms=now_ms,
                    key_parts=("all_snapshot_quotes",),
                    interval_ms=self._UNSUPPORTED_SYMBOL_DIAGNOSTIC_RATE_LIMIT_MS,
                )

        # --- Discover tradeable candidates ---
        # V1 live scan recovery gate: require consecutive fresh snapshots before entry
        live_scan_recovery_count = getattr(
            self.config.runtime,
            'live_scan_recovery_success_count',
            getattr(self.config.strategy, 'live_scan_recovery_success_count', 3),
        )
        if self._live_scan_success_streak < live_scan_recovery_count:
            self.state.last_scan["no_entry_reason"] = "live_scan_recovery_warmup"
            self.journal.append(
                "runtime.live_scan_recovery_warmup",
                {"success_streak": self._live_scan_success_streak,
                 "required": live_scan_recovery_count, "ts_ms": now_ms},
            )
            return

        if can_enter_new_positions(self.state) and self.entry_executor is not None:
            await self._refresh_hyperliquid_entry_balance_admission(now_ms)
            tradeable = discover_tradeable_candidates(
                snapshot.candidates, self.config.strategy, now_ms
            )
            strategy_tradeable_count = len(tradeable)
            raw_candidate_count = len(getattr(snapshot, "candidates", []) or [])
            self.state.last_scan["raw_candidate_count"] = raw_candidate_count
            self.state.last_scan["strategy_tradeable_count"] = strategy_tradeable_count
            self.journal.append(
                "scan.strategy_shortlist_ready",
                {
                    "stage": "strategy_passed",
                    "raw_candidate_count": raw_candidate_count,
                    "candidate_count": strategy_tradeable_count,
                    "strategy_tradeable_count": strategy_tradeable_count,
                    "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
                    "best_pair_id": tradeable[0].pair_id if tradeable else None,
                    "ts_ms": now_ms,
                },
            )
            tradeable = await self._filter_candidates_supported_by_venue_catalog(
                tradeable,
            )
            tradeable = self._filter_candidates_by_entry_admission(
                tradeable,
                now_ms=now_ms,
                stage="shortlist",
            )
            tradeable = await self._filter_candidates_by_entry_balance_admission(
                tradeable,
                now_ms=now_ms,
                stage="shortlist",
            )
            self.state.last_scan["catalog_admission_balance_passed_count"] = len(
                tradeable
            )
            entry_quote_keys: set[tuple[str, str]] = set()
            for candidate in tradeable:
                symbol = str(getattr(candidate, "symbol", "") or "").upper()
                if not symbol:
                    continue
                for venue_attr in ("long_venue", "short_venue"):
                    venue = str(getattr(candidate, venue_attr, "") or "").lower()
                    if venue:
                        entry_quote_keys.add((venue, symbol))
            l2_tracking_tradeable = list(tradeable)
            tracked, tracked_candidates = self._select_v1_entry_tracked_scope(
                l2_tracking_tradeable
            )
            if tracked_candidates:
                (
                    snapshot_freshness_metrics,
                    snapshot_freshness_ages,
                    snapshot_freshness_budgets,
                    snapshot_freshness_publish_intervals,
                    snapshot_freshness_status,
                ) = self._snapshot_freshness_observability(
                    snapshot=snapshot,
                    candidates=tracked_candidates,
                    now_ms=now_ms,
                )
                self.state.last_scan["snapshot_freshness_metrics"] = (
                    snapshot_freshness_metrics
                )
                self.state.last_scan["snapshot_freshness_observed_age_ms"] = (
                    snapshot_freshness_ages
                )
                self.state.last_scan["snapshot_freshness_budget_ms"] = (
                    snapshot_freshness_budgets
                )
                self.state.last_scan["snapshot_freshness_publish_interval_ms"] = (
                    snapshot_freshness_publish_intervals
                )
                self.state.last_scan["snapshot_freshness_status"] = (
                    snapshot_freshness_status
                )
                self.state.last_scan["snapshot_freshness_candidate_scope"] = (
                    "v1_primary_shadow"
                )
                self.state.last_scan["snapshot_freshness_candidate_count"] = len(
                    tracked_candidates
                )
                self.state.last_scan["snapshot_freshness_all_candidate_count"] = len(
                    l2_tracking_tradeable
                )
                self.state.last_scan["snapshot_freshness_skipped_untracked_count"] = max(
                    len(l2_tracking_tradeable) - len(tracked_candidates),
                    0,
                )
            entry_bbo_prewarm_attempted = (
                self._entry_readiness_provider_uses_ws_bbo()
                and bool(tracked_candidates)
            )
            skipped_untracked_quote_count = 0
            if entry_bbo_prewarm_attempted:
                all_quote_targets = self._entry_quote_revalidate_targets(
                    l2_tracking_tradeable,
                    snapshot=snapshot,
                    now_ms=now_ms,
                )
                tracked_quote_targets = self._entry_quote_revalidate_targets(
                    tracked_candidates,
                    snapshot=snapshot,
                    now_ms=now_ms,
                )
                skipped_untracked_quote_count = max(
                    len(all_quote_targets) - len(tracked_quote_targets),
                    0,
                )
            entry_quote_truth_overlay, _entry_quote_truth_stats = (
                await self._entry_quote_revalidate_for_candidates(
                    tracked_candidates,
                    snapshot=snapshot,
                    now_ms=now_ms,
                    candidate_scope=(
                        "v1_primary_shadow" if entry_bbo_prewarm_attempted else ""
                    ),
                    skipped_untracked_count=skipped_untracked_quote_count,
                )
            )
            emit_stale_quote_diagnostics(
                entry_quote_keys,
                resolved_quote_keys=set(entry_quote_truth_overlay.keys()),
            )
            for quote in entry_quote_truth_overlay.values():
                symbol = str(getattr(quote, "symbol", "") or "").upper()
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                if symbol and bid > 0.0 and ask > bid:
                    price_hints[symbol] = (bid + ask) / 2.0
            entry_freshness_filter_candidates = (
                tracked_candidates
                if (
                    self._entry_readiness_provider_uses_ws_bbo()
                    or self._local_l2_effective_enabled()
                )
                else tradeable
            )
            entry_freshness_filter_scope = (
                "v1_primary_shadow"
                if entry_freshness_filter_candidates is not tradeable
                else "tradeable"
            )
            self.state.last_scan["snapshot_freshness_filter_candidate_scope"] = (
                entry_freshness_filter_scope
            )
            self.state.last_scan["snapshot_freshness_filter_candidate_count"] = len(
                entry_freshness_filter_candidates
            )
            self.state.last_scan["snapshot_freshness_filter_all_candidate_count"] = len(
                l2_tracking_tradeable
            )
            self.state.last_scan[
                "snapshot_freshness_filter_skipped_untracked_count"
            ] = max(
                len(l2_tracking_tradeable) - len(entry_freshness_filter_candidates),
                0,
            )
            await self._refresh_entry_candidate_open_interest_evidence(
                entry_freshness_filter_candidates,
                snapshot=snapshot,
                now_ms=now_ms,
            )
            tradeable = self._filter_candidates_by_snapshot_freshness(
                entry_freshness_filter_candidates,
                snapshot=snapshot,
                now_ms=now_ms,
                metrics=snapshot_freshness_metrics,
                ages=snapshot_freshness_ages,
                budgets=snapshot_freshness_budgets,
                publish_intervals=snapshot_freshness_publish_intervals,
                entry_quote_truth_overlay=entry_quote_truth_overlay,
            )
            self.state.last_scan["tradeable_count"] = len(tradeable)
            self.state.last_scan["selected_candidate_count"] = 0
            self.state.last_scan["dispatched_candidate_count"] = 0
            await self._refresh_recovery_ledger_for_symbols(
                sorted(
                    {
                        str(getattr(candidate, "symbol", "") or "").upper()
                        for candidate in tradeable
                        if getattr(candidate, "symbol", "")
                    }
                ),
                now_ms,
            )
            if not tradeable:
                self.state.last_scan["no_entry_reason"] = "no_tradeable_candidates"
            if tradeable:
                # V1 market data warmup: funding coverage must meet threshold before entry
                if hasattr(snapshot, 'funding_lifecycle') and snapshot.funding_lifecycle:
                    funding_warmup_required = getattr(
                        self.config.strategy, 'funding_warmup_min_coverage_ratio', 0.5,
                    )
                    total_count = sum(
                        fl.symbol_count for fl in snapshot.funding_lifecycle
                    )
                    venue_count = len(snapshot.funding_lifecycle)
                    funding_warmup_ok = (
                        venue_count >= 1 and total_count > 0
                    )
                    if not funding_warmup_ok and not self.state.open_positions:
                        self.journal.append(
                            "runtime.funding_warmup_insufficient",
                            {
                                "funding_venue_count": venue_count,
                                "funding_symbol_count": total_count,
                                "warmup_ratio_required": funding_warmup_required,
                                "ts_ms": now_ms,
                            },
                        )
                        return

            # V1: refresh tracked entry local L2 opportunities from the scan
            # shortlist before quote freshness can block final entry selection.
            if (
                self._local_l2_effective_enabled()
                and l2_tracking_tradeable
            ):
                tracked_pair_ids = {t.pair_id for t in tracked}
                # V1: activity_local_l2_symbols() follows the tracked
                # primary+shadow scope, not the whole tradeable shortlist.
                await self._ensure_l2_active_for_candidates(
                    tracked_candidates,
                    now_ms,
                    tracked_opportunities=tracked,
                )
                self._tracked_primary_pair_ids = {
                    t.pair_id for t in tracked
                    if t.class_.value == "primary_tracked"
                }
                # Refresh session state for all tracked opportunities
                for t in tracked:
                    self.entry_l2_sessions.track_opportunity(t, now_ms)
                # V1 post-shortlist L2 sync after tracking: local books
                # drive session readiness before the selection blocker.
                await self._sync_local_l2_data(now_ms, scan_promoted=True)
                self._refresh_entry_l2_session_readiness(now_ms)
                # V1: shadow promotion — best shadow replaces worst primary
                # when score delta, hold window, execution guard, and readiness
                # all pass (execution_core/engine.rs:2643-2719)
                self._apply_shadow_promotion_if_eligible(
                    tracked, now_ms,
                )
            if (
                self._entry_readiness_provider_uses_ws_bbo()
                and l2_tracking_tradeable
                and not entry_bbo_prewarm_attempted
            ):
                await self._ensure_entry_bbo_active_for_candidates(
                    tracked_candidates,
                    now_ms,
                )

            if tradeable:
                self.journal.append(
                    "runtime.candidates_tradeable",
                    {"count": len(tradeable), "ts_ms": now_ms},
                )
                # V1: scan.shortlist_ready — basic shortlist generated, before post-shortlist processing
                self.journal.append(
                    "scan.shortlist_ready",
                    {
                        "stage": "execution_freshness_filtered",
                        "raw_candidate_count": raw_candidate_count,
                        "strategy_tradeable_count": strategy_tradeable_count,
                        "tracked_candidate_count": len(tracked_candidates),
                        "candidate_count": len(tradeable),
                        "tradeable_count": len(tradeable),
                        "shortlist_candidate_count": len(tradeable),
                        "shortlist_tradeable_count": len(tradeable),
                        "quote_truth_must_resolve_count": self.state.last_scan.get(
                            "quote_truth_must_resolve_count",
                            0,
                        ),
                        "quote_truth_failed_count": self.state.last_scan.get(
                            "quote_truth_failed_count",
                            0,
                        ),
                        "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
                        "best_pair_id": tradeable[0].pair_id if tradeable else None,
                        "ts_ms": now_ms,
                    },
                )
                # V1: selected_candidates is a final-entry list, not the raw
                # shortlist. It excludes candidates still waiting on the final
                # entry window, primary L2 tracking, or dual-ready books.
                max_slots = max(self.config.strategy.max_concurrent_positions, 1)
                remaining_slots = max(max_slots - len(self.state.open_positions), 0)
                self.state.last_scan["max_concurrent_positions"] = max_slots
                self.state.last_scan["open_position_count"] = len(self.state.open_positions)
                self.state.last_scan["remaining_slots"] = remaining_slots
                admission_blocker_counts: Counter[str] = Counter()
                selection_blocker_counts: Counter[str] = Counter()
                candidate_blockers: dict[str, str] = {}
                finalists = self._select_entry_candidates(
                    tradeable,
                    now_ms=now_ms,
                    remaining_slots=remaining_slots,
                    selection_blocker_counts=selection_blocker_counts,
                    candidate_blockers=candidate_blockers,
                    market_quotes=self._entry_quote_truth_market_quotes(
                        snapshot.quotes,
                        entry_quote_truth_overlay,
                    ),
                    admission_blocker_counts=admission_blocker_counts,
                )
                self.state.last_scan["selected_candidate_count"] = len(finalists)
                dispatched = 0
                for candidate in finalists:
                    if len(self.state.open_positions) >= max_slots:
                        break
                    mid_price = self._entry_quote_truth_price_hint(
                        candidate,
                        price_hints=price_hints,
                        overlay=entry_quote_truth_overlay,
                    )
                    if await self._dispatch_entry(candidate, now_ms, price_hint=mid_price):
                        dispatched += 1
                self.state.last_scan["dispatched_candidate_count"] = dispatched
                if dispatched > 0:
                    self._emit_entry_opportunity_funnel(
                        reason="entries_dispatched",
                        snapshot=snapshot,
                        tradeable=tradeable,
                        selected=finalists,
                        dispatched_candidate_count=dispatched,
                        remaining_slots=remaining_slots,
                        tradeable_selection_blocker_counts=selection_blocker_counts,
                        candidate_blockers=candidate_blockers,
                        now_ms=now_ms,
                        admission_blocker_counts=admission_blocker_counts,
                    )
                if dispatched == 0:
                    reason = (
                        self._v1_tradeable_no_entry_reason(
                            selection_blocker_counts,
                            admission_blocker_counts,
                        )
                        or "no_entry_dispatched"
                    )
                    self._emit_scan_no_entry_diagnostics(
                        reason=reason,
                        snapshot=snapshot,
                        tradeable=tradeable,
                        selected_candidate_count=len(finalists),
                        dispatched_candidate_count=dispatched,
                        remaining_slots=remaining_slots,
                        tradeable_selection_blocker_counts=selection_blocker_counts,
                        candidate_blockers=candidate_blockers,
                        now_ms=now_ms,
                        admission_blocker_counts=admission_blocker_counts,
                    )
            elif can_enter_new_positions(self.state) and self.entry_executor is not None:
                self._emit_scan_no_entry_diagnostics(
                    reason="no_tradeable_candidates",
                    snapshot=snapshot,
                    tradeable=[],
                    selected_candidate_count=0,
                    dispatched_candidate_count=0,
                    remaining_slots=max(
                        self.config.strategy.max_concurrent_positions,
                        1,
                    ) - len(self.state.open_positions),
                    tradeable_selection_blocker_counts=Counter(),
                    candidate_blockers={},
                    now_ms=now_ms,
                )

    # ------------------------------------------------------------------
    # Risk snapshot runtime cache (V1: fetch_account_risk_with_runtime_cache)
    # ------------------------------------------------------------------

    def _cached_risk_snapshot(self, venue: Venue, now_ms: int):
        """Return cached (result, was_cached) or (None, False) if stale/missing.

        V1: cached_runtime_risk_snapshot() — checks freshness against
        venue-specific TTL (1s default, 30s for Aster to avoid REST polling).
        """
        entry = self._risk_snapshot_cache.get(venue)
        if entry is None:
            return None, False
        fetched_at = entry.get("fetched_at_ms", 0)
        ttl = self._risk_snapshot_ttl_ms(venue)
        if ttl <= 0 or (now_ms - fetched_at) > ttl:
            return None, False
        return entry.get("result"), True

    def _store_risk_snapshot(self, venue: Venue, now_ms: int, result) -> None:
        """Store a risk snapshot fetch result in the per-venue cache.

        V1: store_runtime_risk_snapshot() — stores Ok(snapshot), Ok(None),
        or Err(error_string) with fetched_at_ms.
        """
        self._risk_snapshot_cache[venue] = {
            "fetched_at_ms": now_ms,
            "result": result,
        }

    async def _fetch_venue_risk_snapshot(
        self, venue: Venue, adapter, supports: bool, now_ms: int,
    ):
        """Fetch venue risk snapshot with runtime cache.

        V1: fetch_account_risk_with_runtime_cache().
        Returns (snapshot_or_none, supports_still_valid).

        Cache stores: Ok(snapshot), Ok(None=unsupported/missing), or Err(str).
        Failed fetches are cached to avoid retry storms; same-tick same-venue
        calls share the cached result.
        """
        if not supports or adapter is None:
            return None, supports

        # Check cache first
        cached_result, was_cached = self._cached_risk_snapshot(venue, now_ms)
        if was_cached:
            if isinstance(cached_result, tuple) and len(cached_result) == 2:
                # (ok=True, snapshot) or (ok=False, error_string)
                ok, val = cached_result
                if ok:
                    return val, True
                else:
                    # Error was already journaled on original fetch.
                    # Keep supports=True — a fetch error means snapshot_unavailable,
                    # NOT capability_unsupported (V1: venue capability unchanged).
                    return None, True
            # Legacy: direct snapshot stored
            return cached_result, True

        # Cache miss — fetch from adapter
        try:
            snapshot = await adapter.fetch_account_risk_snapshot()
            self._store_risk_snapshot(venue, now_ms, (True, snapshot))
            return snapshot, True
        except Exception as e:
            error_str = str(e)
            self.journal.append(
                "runtime.risk_snapshot_fetch_error",
                {"venue": venue.value, "error": error_str},
            )
            self._store_risk_snapshot(venue, now_ms, (False, error_str))
            # Fetch error → snapshot unavailable, but capability (supports) unchanged.
            # V1: venue supports_risk_health is independent of transient fetch errors.
            return None, True

    async def tick_active_positions(self) -> None:
        """Fast tick lane: active position monitoring with risk supervision.

        Evaluates risk for every open position and executes delever / protection
        plans when conditions are met. This is the primary close-driving path.

        V1: queries venue adapters for account risk snapshots, passes real
        supports_risk_health flags instead of hardcoded False (Fix 4).
        """
        now_ms = wall_clock_now_ms()
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1

        if not self.state.open_positions:
            return

        self.journal.append(
            "runtime.active_position_tick",
            {"position_count": len(self.state.open_positions), "ts_ms": now_ms},
        )

        await self._maybe_check_active_position_drift(now_ms)
        if not self.state.open_positions:
            return

        # --- Per-position risk supervision ---
        for position in list(self.state.open_positions.values()):
            # Determine risk health support from venue adapters (Fix 4)
            long_adapter = self.get_venue_adapter(position.long_venue)
            short_adapter = self.get_venue_adapter(position.short_venue)

            long_supports = (
                long_adapter is not None and long_adapter.supports_risk_health
            )
            short_supports = (
                short_adapter is not None and short_adapter.supports_risk_health
            )

            # Fetch real risk snapshots with runtime cache (V1: per-venue TTL)
            long_snapshot, long_supports = await self._fetch_venue_risk_snapshot(
                position.long_venue, long_adapter, long_supports, now_ms,
            )
            short_snapshot, short_supports = await self._fetch_venue_risk_snapshot(
                position.short_venue, short_adapter, short_supports, now_ms,
            )

            plan = self.supervisor.supervise_position(
                position, now_ms,
                long_supports_risk_health=long_supports,
                short_supports_risk_health=short_supports,
                long_snapshot=long_snapshot,
                short_snapshot=short_snapshot,
            )
            if plan is not None:
                self.journal.append(
                    "runtime.risk_plan_generated",
                    {
                        "position_id": position.position_id,
                        "kind": plan.kind.value,
                        "reason": plan.reason,
                    },
                )
                await self.supervisor.execute_risk_plan(position, plan, now_ms)

    # ------------------------------------------------------------------
    # Rate-limit reload (V1: rate_limit_reload_interval)
    # ------------------------------------------------------------------

    _RATE_LIMIT_RELOAD_INTERVAL_MS = 30_000

    async def _maybe_reload_rate_limits(self, now_ms: int) -> None:
        """Periodic rate-limit config reload (V1: rate_limit_reload_interval).

        Reloads rate_limits.toml every _RATE_LIMIT_RELOAD_INTERVAL_MS if the
        config file has changed. Also flushes pending recommendation events.
        """
        if self._rate_limit_runtime is None:
            return
        if now_ms < self._last_rate_limit_reload_ms + self._RATE_LIMIT_RELOAD_INTERVAL_MS:
            return
        self._last_rate_limit_reload_ms = now_ms
        try:
            await self._rate_limit_runtime.refresh(now_ms)
            self._rate_limit_runtime.flush_recommendations()
        except Exception as e:
            self.journal.append(
                "runtime.rate_limit_reload_error", {"error": str(e)}
            )

    # ------------------------------------------------------------------
    # Local-L2 data sync (V1: periodic snapshot refresh per book)
    # ------------------------------------------------------------------

    async def _sync_local_l2_data(self, now_ms: int, *, scan_promoted: bool = False) -> None:
        """Periodic snapshot refresh for local-L2 books without WS streaming.

        Called at two points per tick (V1 dual-phase):
        1. Pre-scan (scan_promoted=False): execution-owned books only
        2. Post-shortlist (scan_promoted=True): allows scan-promoted books

        Delegates to the data plane which respects per-book cooldown intervals.
        """
        self._refresh_runtime_market_data_config_state()
        if not self._local_l2_effective_enabled():
            return

        try:
            dispatched = await self.l2_data_plane.sync_snapshots(
                adapters=self._venue_adapters,
                now_ms=now_ms,
                scan_promoted=scan_promoted,
            )
            if dispatched > 0:
                self.local_l2_runtime.sync(now_ms)
        except Exception as e:
            self.journal.append(
                "runtime.local_l2_sync_error",
                {"error": str(e), "ts_ms": now_ms},
            )

    # ------------------------------------------------------------------
    # Maker-event lane (V1: maker_event_interval)
    # ------------------------------------------------------------------

    async def _maybe_tick_maker_event(self, now_ms: int) -> None:
        return await self.passive_maker_runtime._maybe_tick_maker_event(now_ms)

    async def _maybe_tick_maker_event_local_l2(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        return await self.passive_maker_runtime._maybe_tick_maker_event_local_l2(
            now_ms, pending_passive
        )

    async def _maybe_tick_maker_event_ws_bbo(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        return await self.passive_maker_runtime._maybe_tick_maker_event_ws_bbo(
            now_ms, pending_passive
        )

    async def _maybe_tick_maker_event_sidecar(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        return await self.passive_maker_runtime._maybe_tick_maker_event_sidecar(
            now_ms, pending_passive
        )

    async def _reprice_passive_maker(
        self, pending, new_price: float, old_price: float,
        action: str, now_ms: int, entry_id: str,
    ) -> None:
        return await self.passive_maker_runtime._reprice_passive_maker(
            pending, new_price, old_price, action, now_ms, entry_id
        )

    async def _reprice_passive_maker_l2(
        self, pending, new_price: float, old_price: float,
        action: str, now_ms: int, entry_id: str,
    ) -> HedgeDriveResult:
        return await self.passive_maker_runtime._reprice_passive_maker_l2(
            pending, new_price, old_price, action, now_ms, entry_id
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _runtime_lane_budget_ms(self) -> int:
        try:
            poll_ms = int(self.config.runtime.poll_interval_ms or 0)
        except (TypeError, ValueError):
            poll_ms = 0
        return max(poll_ms * 4, 60_000)

    def _maker_event_lane_fast_sleep_ms(self) -> int | None:
        if not self.config.runtime.maker_event_lane_enabled:
            return None
        pending_entries = getattr(self.state, "pending_entries", {}) or {}
        pending_values = (
            pending_entries.values()
            if hasattr(pending_entries, "values")
            else pending_entries
        )
        for pending in pending_values:
            entry_type = str(getattr(pending, "entry_type", "") or "").lower()
            if "passive" in entry_type:
                return max(
                    1,
                    int(self.config.runtime.maker_event_lane_min_wake_interval_ms or 1),
                )
        return None

    def _runtime_loop_sleep_ms(
        self,
        poll_ms: int,
        active_poll_ms: int,
        now_ms: int,
        next_full_tick_due_ms: int,
        next_active_tick_due_ms: int,
        active_tick_enabled: bool,
    ) -> int:
        deadlines = [max(1, int(next_full_tick_due_ms - now_ms))]
        if active_tick_enabled:
            deadlines.append(max(1, int(next_active_tick_due_ms - now_ms)))
        maker_sleep_ms = self._maker_event_lane_fast_sleep_ms()
        if maker_sleep_ms is not None:
            deadlines.append(maker_sleep_ms)
        deadlines.append(max(1, min(int(poll_ms), int(active_poll_ms))))
        return max(1, min(deadlines))

    def _ensure_runtime_progress(self) -> dict[str, Any]:
        progress = getattr(self.state, "runtime_progress", None)
        if not isinstance(progress, dict):
            progress = {}
            self.state.runtime_progress = progress
        progress.setdefault("loop_iteration_started_ms", 0)
        progress.setdefault("loop_iteration_completed_ms", 0)
        progress.setdefault("last_lane_progress_ms", 0)
        progress.setdefault("active_lane", "")
        progress.setdefault("active_lane_started_ms", 0)
        progress.setdefault("active_lane_budget_ms", 0)
        progress.setdefault("active_lane_overdue", False)
        return progress

    def _begin_runtime_loop_iteration(self, now_ms: int) -> None:
        progress = self._ensure_runtime_progress()
        progress["loop_iteration_started_ms"] = now_ms

    def _complete_runtime_loop_iteration(self, now_ms: int) -> None:
        progress = self._ensure_runtime_progress()
        progress["loop_iteration_completed_ms"] = now_ms

    def _begin_runtime_lane(
        self,
        lane: str,
        now_ms: int,
        *,
        budget_ms: int | None = None,
    ) -> None:
        progress = self._ensure_runtime_progress()
        progress["active_lane"] = lane
        progress["active_lane_started_ms"] = now_ms
        progress["active_lane_budget_ms"] = (
            self._runtime_lane_budget_ms() if budget_ms is None else int(budget_ms)
        )
        progress["active_lane_overdue"] = False

    def _complete_runtime_lane(self, lane: str, now_ms: int) -> None:
        progress = self._ensure_runtime_progress()
        progress["last_lane_progress_ms"] = now_ms
        if progress.get("active_lane") == lane:
            progress["active_lane"] = ""
            progress["active_lane_started_ms"] = 0
            progress["active_lane_budget_ms"] = 0
            progress["active_lane_overdue"] = False

    async def _current_state_heartbeat_loop(self) -> None:
        """Export current-state while long tick lanes are awaiting IO."""
        interval_s = current_state_export_interval_ms(self.config) / 1000.0
        while self._running:
            now_ms = wall_clock_now_ms()
            try:
                self._maybe_export_current_state_snapshot(self._export_state, now_ms)
            except Exception as exc:
                self.journal.append(
                    "runtime.current_state_heartbeat_loop_export_error",
                    {"error": str(exc)},
                )
            await asyncio.sleep(interval_s)

    async def run_loop(self) -> None:
        """Multi-lane tick loop with backoff, housekeeping, and periodic export."""
        self._running = True
        poll_ms = max(1, int(self.config.runtime.poll_interval_ms or 1))
        next_full_tick_due_ms = 0
        next_active_tick_due_ms = 0
        heartbeat_task = asyncio.create_task(self._current_state_heartbeat_loop())

        try:
            while self._running:
                now_ms = wall_clock_now_ms()
                self._begin_runtime_loop_iteration(now_ms)
                try:
                    active_count = len(self.state.open_positions)
                    active_poll_ms = active_position_poll_interval_ms(
                        self.state.lifecycle, poll_ms, active_count
                    )
                    active_tick_enabled = active_position_poll_enabled(
                        self.state.lifecycle, poll_ms, active_count
                    )
                    full_lane_due = now_ms >= next_full_tick_due_ms
                    active_lane_due = active_tick_enabled and now_ms >= next_active_tick_due_ms
                    periodic_lanes_due = full_lane_due or active_lane_due

                    # --- Full tick lane (backoff-gated) ---
                    if full_lane_due:
                        if full_tick_ready(self._tick_backoff_until_ms, now_ms):
                            self._begin_runtime_lane("full_tick", wall_clock_now_ms())
                            try:
                                await self.tick()
                                self._tick_backoff_until_ms = None
                            except Exception as e:
                                self._apply_tick_backoff(is_active=False)
                                self.journal.append("runtime.tick_error", {"error": str(e)})
                            finally:
                                self._complete_runtime_lane("full_tick", wall_clock_now_ms())
                        next_full_tick_due_ms = wall_clock_now_ms() + poll_ms

                    # --- Active-position fast tick lane ---
                    if active_lane_due:
                        if active_position_tick_ready(
                            self._active_tick_backoff_until_ms, now_ms
                        ):
                            self._begin_runtime_lane("active_positions", wall_clock_now_ms())
                            try:
                                await self.tick_active_positions()
                                self._active_tick_backoff_until_ms = None
                            except Exception as e:
                                self._apply_tick_backoff(is_active=True)
                                self.journal.append(
                                    "runtime.active_tick_error", {"error": str(e)}
                                )
                            finally:
                                self._complete_runtime_lane("active_positions", wall_clock_now_ms())
                        next_active_tick_due_ms = wall_clock_now_ms() + active_poll_ms

                    if periodic_lanes_due:
                        # --- Rate-limit periodic reload (V1: rate_limit_reload_interval) ---
                        self._begin_runtime_lane("rate_limit_reload", wall_clock_now_ms())
                        try:
                            await self._maybe_reload_rate_limits(now_ms)
                        finally:
                            self._complete_runtime_lane("rate_limit_reload", wall_clock_now_ms())

                        # --- Local-L2 snapshot refresh (periodic REST bootstrap for books) ---
                        self._begin_runtime_lane("local_l2_sync", wall_clock_now_ms())
                        try:
                            await self._sync_local_l2_data(now_ms)
                        finally:
                            self._complete_runtime_lane("local_l2_sync", wall_clock_now_ms())

                        # --- Passive close lane (V1: process_pending_passive_closes) ---
                        self._begin_runtime_lane("passive_close", wall_clock_now_ms())
                        try:
                            await self._maybe_tick_passive_close(now_ms)
                        finally:
                            self._complete_runtime_lane("passive_close", wall_clock_now_ms())

                        # --- Normal exit lane (V1: standard_close_reason → passive/aggressive routing) ---
                        self._begin_runtime_lane("normal_exit", wall_clock_now_ms())
                        try:
                            await self._maybe_process_normal_exits(now_ms)
                        finally:
                            self._complete_runtime_lane("normal_exit", wall_clock_now_ms())

                    # --- Maker-event lane (V1: maker_event_interval, optional, with backoff) ---
                    if full_tick_ready(self._maker_tick_backoff_until_ms, now_ms):
                        self._begin_runtime_lane("maker_event", wall_clock_now_ms())
                        try:
                            await self._maybe_tick_maker_event(now_ms)
                            self._maker_tick_backoff_until_ms = None
                        except Exception as e:
                            self._apply_tick_backoff(is_maker=True)
                            self.journal.append(
                                "runtime.maker_event_tick_error", {"error": str(e)}
                            )
                        finally:
                            self._complete_runtime_lane("maker_event", wall_clock_now_ms())

                    if periodic_lanes_due:
                        # --- Passive maker maintenance (V1: maintain_pending_entry_passive_order) ---
                        # Active tick-level lifecycle for resting maker orders:
                        # progress query → try_window check → rest_timeout → cancel → abort/finalize
                        self._begin_runtime_lane("pending_entry_maintenance", wall_clock_now_ms())
                        try:
                            await self._maintain_pending_entry_passive_orders(now_ms)
                        finally:
                            self._complete_runtime_lane(
                                "pending_entry_maintenance", wall_clock_now_ms()
                            )

                        # --- Post-tick housekeeping ---
                        self._begin_runtime_lane("housekeeping", wall_clock_now_ms())
                        try:
                            await self._post_tick_housekeeping(now_ms)
                        finally:
                            self._complete_runtime_lane("housekeeping", wall_clock_now_ms())

                    # --- Snapshot local-L2 state for persistence ---
                    self._snapshot_local_l2_state()

                    # --- Persist state snapshot ---
                    self._sync_passive_order_manager_states()
                    self.snapshot_store.write(build_persistent_state_view(self.state))

                finally:
                    self._complete_runtime_loop_iteration(wall_clock_now_ms())

                sleep_ms = self._runtime_loop_sleep_ms(
                    poll_ms,
                    active_poll_ms,
                    wall_clock_now_ms(),
                    next_full_tick_due_ms,
                    next_active_tick_due_ms,
                    active_tick_enabled,
                )
                await asyncio.sleep(sleep_ms / 1000.0)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # Passive entry maintenance (V1 maintain_pending_entry_passive_order — Fix 1)
    # ------------------------------------------------------------------

    def _pending_entry_terminal_fallback_candidate(self, pending):
        return candidate_for_terminal_taker_fallback(pending)

    @staticmethod
    def _candidate_to_runtime_namespace(candidate: Any, pending: Any) -> Any:
        if candidate is None:
            long_venue = getattr(pending, "long_venue", "")
            short_venue = getattr(pending, "short_venue", "")
            return SimpleNamespace(
                symbol=getattr(pending, "symbol", ""),
                long_venue=long_venue.value if hasattr(long_venue, "value") else str(long_venue),
                short_venue=(
                    short_venue.value if hasattr(short_venue, "value") else str(short_venue)
                ),
                pending_owner_id=str(
                    getattr(pending, "pending_id", "")
                    or getattr(pending, "position_id", "")
                    or getattr(pending, "entry_id", "")
                    or ""
                ),
                blocked=True,
                blocked_reasons=["legacy_no_frozen_candidate_out_of_scope"],
                ranking_edge_bps=0.0,
                expected_edge_bps=0.0,
                funding_edge_bps=0.0,
                entry_notional_quote=0.0,
            )
        if isinstance(candidate, SimpleNamespace):
            data = dict(vars(candidate))
        elif isinstance(candidate, dict):
            data = dict(candidate)
        else:
            return candidate

        long_venue = getattr(pending, "long_venue", "")
        short_venue = getattr(pending, "short_venue", "")
        data.setdefault("symbol", getattr(pending, "symbol", ""))
        data.setdefault(
            "pending_owner_id",
            str(
                getattr(pending, "pending_id", "")
                or getattr(pending, "position_id", "")
                or getattr(pending, "entry_id", "")
                or ""
            ),
        )
        data.setdefault(
            "long_venue",
            long_venue.value if hasattr(long_venue, "value") else str(long_venue),
        )
        data.setdefault(
            "short_venue",
            short_venue.value if hasattr(short_venue, "value") else str(short_venue),
        )
        data.setdefault("blocked", False)
        data.setdefault("blocked_reasons", [])
        data.setdefault("ranking_edge_bps", 0.0)
        data.setdefault("expected_edge_bps", 0.0)
        data.setdefault("funding_edge_bps", 0.0)
        data.setdefault("entry_notional_quote", 0.0)
        return SimpleNamespace(**data)

    def _apply_terminal_taker_runtime_entry_guards(
        self,
        candidate: Any,
        pending: Any,
        now_ms: int,
    ) -> Any:
        """V1: apply_runtime_entry_guards_excluding_pending for terminal fallback."""

        checked = self._candidate_to_runtime_namespace(candidate, pending)
        blocked_reasons = list(getattr(checked, "blocked_reasons", []) or [])

        if not self._candidate_is_tradeable_for_selection(checked):
            blocked_reasons.append("candidate_not_tradeable_for_selection")

        gates = [
            ("reduce_only", self._gate_reduce_only, ()),
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation, ()),
            ("passive_close_in_flight", self._gate_passive_close_pending, ()),
            ("recovery_ledger", self._gate_recovery_ledger, ()),
            ("entry_sizing", self._gate_entry_sizing, ()),
            ("venue_cooldown", self._gate_venue_cooldown, (now_ms,)),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown, (now_ms,)),
        ]
        for gate_name, gate_fn, gate_args in gates:
            allowed, reason = gate_fn(checked, *gate_args)
            if allowed:
                continue
            blocked_reasons.append(reason or gate_name)

        checked.blocked_reasons = blocked_reasons
        checked.blocked = bool(blocked_reasons)
        return checked

    @staticmethod
    def _candidate_float_hint(candidate: Any, *names: str) -> float:
        for name in names:
            try:
                value = float(getattr(candidate, name, 0.0) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0.0:
                return value
        return 0.0

    def _force_standard_candidate_context_values(self, candidate: Any, pending: Any) -> tuple[float, float, float]:
        long_price_hint = self._candidate_float_hint(
            candidate,
            "long_price_hint",
            "long_order_price_hint",
            "long_entry_vwap",
        )
        short_price_hint = self._candidate_float_hint(
            candidate,
            "short_price_hint",
            "short_order_price_hint",
            "short_entry_vwap",
        )
        fallback_price = (
            getattr(getattr(pending, "passive_order", None), "limit_price", None)
            or getattr(pending, "maker_price", 0.0)
            or 0.0
        )
        if long_price_hint <= 0.0:
            long_price_hint = float(fallback_price or 0.0)
        if short_price_hint <= 0.0:
            short_price_hint = float(fallback_price or 0.0)

        reference_price = long_price_hint if long_price_hint > 0.0 else short_price_hint
        entry_notional_quote = float(
            getattr(candidate, "entry_notional_quote", 0.0) or 0.0
        )
        target_quantity = (
            (entry_notional_quote / reference_price)
            if entry_notional_quote > 0.0 and reference_price > 0.0
            else (
                float(getattr(pending, "target_quantity", 0.0) or 0.0)
                or float(getattr(pending, "long_quantity", 0.0) or 0.0)
                or float(getattr(pending, "short_quantity", 0.0) or 0.0)
            )
        )
        return target_quantity, long_price_hint, short_price_hint

    async def _execute_pending_entry_terminal_taker_fallback(
        self,
        pending,
        entry_id: str,
        now_ms: int,
        terminal_reason: str,
    ) -> bool:
        """V1: try_terminal_taker_fallback for pending entry zero-fill terminal."""
        source_candidate = self._pending_entry_terminal_fallback_candidate(pending)
        candidate = self._apply_terminal_taker_runtime_entry_guards(
            source_candidate,
            pending,
            now_ms,
        )
        recheck = terminal_recheck_is_tradeable(candidate)
        if recheck.kind == "blocked":
            self.journal.append(
                "execution.entry_fallback_to_taker_skipped",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": recheck.reason,
                    "terminal_reason": terminal_reason,
                    "maker_venue": pending.maker_venue().value,
                    "maker_leg": getattr(pending, "maker_leg", "long"),
                    "blocked_reasons": recheck.evidence.get("blocked_reasons", []),
                },
            )
            return False

        action = decide_terminal_taker_fallback(candidate, terminal_reason)
        if action.kind == "skip_fallback":
            payload = {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "reason": action.reason,
                "terminal_reason": terminal_reason,
                "maker_venue": pending.maker_venue().value,
                "maker_leg": getattr(pending, "maker_leg", "long"),
            }
            if action.evidence.get("blocked_reasons"):
                payload["blocked_reasons"] = action.evidence["blocked_reasons"]
            self.journal.append("execution.entry_fallback_to_taker_skipped", payload)
            return False

        if self.entry_executor is None:
            return False

        from lightfee.engine.entry import EntryContext, EntryType

        maker_leg = Side.SELL if getattr(pending, "maker_leg", "long") == "short" else Side.BUY
        target_quantity, long_price_hint, short_price_hint = (
            self._force_standard_candidate_context_values(candidate, pending)
        )
        if target_quantity <= 1e-9 or long_price_hint <= 0.0 or short_price_hint <= 0.0:
            self.journal.append(
                "execution.entry_fallback_to_taker_skipped",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": "missing_force_standard_quantity_or_price",
                    "terminal_reason": terminal_reason,
                    "target_quantity": target_quantity,
                    "long_price_hint": long_price_hint,
                    "short_price_hint": short_price_hint,
                },
            )
            return False

        self.journal.append(
            "execution.entry_fallback_to_taker",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "reason": action.reason,
                "terminal_reason": terminal_reason,
                "maker_venue": pending.maker_venue().value,
                "maker_leg": getattr(pending, "maker_leg", "long"),
                "repost_attempt_count": pending.repost_attempt_count,
            },
        )

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_quantity=target_quantity,
            short_quantity=target_quantity,
            long_price_hint=long_price_hint,
            short_price_hint=short_price_hint,
            maker_leg=maker_leg,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
            created_at_ms=now_ms,
            opportunity_type=pending.opportunity_type,
            funding_timestamp_ms=pending.funding_timestamp_ms,
            first_funding_timestamp_ms=pending.first_funding_timestamp_ms,
            long_funding_timestamp_ms=pending.long_funding_timestamp_ms,
            short_funding_timestamp_ms=pending.short_funding_timestamp_ms,
            second_funding_timestamp_ms=pending.second_funding_timestamp_ms,
            first_funding_leg=pending.first_funding_leg,
            funding_edge_bps_entry=float(
                getattr(candidate, "funding_edge_bps", pending.funding_edge_bps_entry) or 0.0
            ),
            total_funding_edge_bps_entry=pending.total_funding_edge_bps_entry,
            expected_edge_bps_entry=float(
                getattr(candidate, "expected_edge_bps", pending.expected_edge_bps_entry) or 0.0
            ),
            worst_case_edge_bps_entry=pending.worst_case_edge_bps_entry,
            entry_maker_leg=pending.entry_maker_leg,
            exit_maker_leg=pending.exit_maker_leg,
            entry_cross_bps_entry=pending.entry_cross_bps_entry,
            fee_bps_entry=pending.fee_bps_entry,
            entry_slippage_bps_entry=pending.entry_slippage_bps_entry,
            transfer_bias_bps_entry=pending.transfer_bias_bps_entry,
            transfer_state_at_entry=pending.transfer_state_at_entry,
            entry_liquidity_source_at_entry=pending.entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=pending.long_volume_24h_quote_at_entry,
            short_volume_24h_quote_at_entry=pending.short_volume_24h_quote_at_entry,
            long_open_interest_quote_at_entry=pending.long_open_interest_quote_at_entry,
            short_open_interest_quote_at_entry=pending.short_open_interest_quote_at_entry,
            long_entry_vwap=pending.long_entry_vwap,
            short_entry_vwap=pending.short_entry_vwap,
            entry_capacity_constrained=pending.entry_capacity_constrained,
            entry_target_quantity=pending.entry_target_quantity,
            long_max_executable_quantity=pending.long_max_executable_quantity,
            short_max_executable_quantity=pending.short_max_executable_quantity,
            entry_max_executable_quantity=pending.entry_max_executable_quantity,
            entry_depth_shortfall_quantity=pending.entry_depth_shortfall_quantity,
            entry_max_executable_notional_quote=pending.entry_max_executable_notional_quote,
            entry_depth_capped_at_entry=pending.entry_depth_capped_at_entry,
            advisories=list(pending.advisories),
            blocked_reasons=list(getattr(candidate, "blocked_reasons", []) or []),
            exit_after_first_stage=pending.exit_after_first_stage,
        )
        result = await self.entry_executor.execute(ctx)
        if getattr(result, "open_position", None) is not None:
            self.state.open_positions[result.open_position.position_id] = result.open_position
            self.journal.append(
                "runtime.position_opened",
                {"position_id": result.open_position.position_id},
            )
            await self._complete_pending_entry_terminal_removal(
                entry_id,
                reason="pending_entry_terminal_fallback_to_taker",
                symbol=pending.symbol,
                now_ms=now_ms,
            )
            return True

        pending.next_progress_poll_ms = now_ms + (
            getattr(self.config.strategy, "maker_entry_reconcile_backoff_ms", 1000)
            or 1000
        )
        self.journal.append(
            "execution.entry_fallback_to_taker_deferred",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "terminal_reason": terminal_reason,
                "reason": "force_standard_open_not_materialized",
            },
        )
        return True

    async def _maintain_pending_entry_passive_orders(self, now_ms: int) -> None:
        """V1: maintain_pending_entry_passive_order() at tick level.

        Active maintenance for each pending entry with a resting passive maker
        order.  Replicates the V1 passive maker lifecycle:

        1. Query passive order progress from the venue adapter
        2. Apply progress (update fill quantities, progress state)
        3. maker_try_window_fill_shortfall — cancel if elapsed > 1500ms with
           fill ratio below 25% (zero-fill protection)
        4. maker_entry_rest_timeout — cancel if elapsed > 6000ms
        5. Post-cancel: zero-fill → V1 retry/repost cycle, partial-fill →
           hedge → finalize, uncertain → retain for reconciliation

        V1 ref: entry_sync.rs:1554 maintain_pending_entry_passive_order()
        """
        if not self._venue_adapters:
            return

        strategy = self.config.strategy
        try_window_ms = getattr(strategy, "maker_try_window_ms", 0) or 0
        min_fill_ratio = getattr(strategy, "maker_min_fill_ratio", 0.25) or 0.25
        rest_timeout_ms = getattr(strategy, "maker_entry_rest_timeout_ms", 6000) or 6000
        poll_ms = getattr(strategy, "maker_entry_progress_poll_ms", 500) or 500

        resolved: list[str] = []

        for entry_id, pending in list(self.state.pending_entries.items()):
            po = pending.passive_order
            if po is None:
                continue
            maker_venue = pending.maker_venue()

            # Guard: must have a valid exchange order id or client id to query/cancel.
            if not (po.order_id or po.client_order_id):
                continue

            # Respect poll interval — V1 next_progress_poll_ms gate
            if pending.next_progress_poll_ms > 0 and now_ms < pending.next_progress_poll_ms:
                continue

            adapter = self._venue_adapters.get(maker_venue)
            if adapter is None:
                continue

            # --- Step 1: Query passive order progress ---
            progress = None
            try:
                progress = await adapter.query_passive_order_progress(
                    symbol=pending.symbol,
                    order_id=po.order_id,
                    client_order_id=po.client_order_id or None,
                    side=pending.maker_side(),
                )
            except Exception as exc:
                self.journal.append(
                    "passive_maintenance.progress_query_error",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "venue": str(maker_venue), "error": str(exc)},
                )
                pending.next_progress_poll_ms = now_ms + poll_ms
                continue

            # --- Step 2: Apply progress to pending entry ---
            if progress is not None:
                prev_filled = pending.maker_leg_filled
                progress_changed = apply_pending_entry_passive_progress(pending, progress)
                if progress_changed and progress.cumulative_quantity > prev_filled:
                    self.journal.append(
                        "passive_maintenance.maker_progress",
                        {
                            "entry_id": entry_id, "symbol": pending.symbol,
                            "prev_filled": prev_filled,
                            "new_filled": progress.cumulative_quantity,
                            "state": progress.state.value,
                            "venue": str(maker_venue),
                        },
                    )
                    if (
                        pending.missing_hedge_quantity() > 1e-9
                        and pending.hedge_inflight is None
                        and not pending.repair_state
                    ):
                        hedge_driven = await self._drive_missing_hedge_live(
                            pending, entry_id, now_ms
                        )
                        if (
                            hedge_driven
                            and pending.missing_hedge_quantity() <= 1e-9
                            and pending.maker_completed()
                        ):
                            if await self._finalize_pending_entry(pending, entry_id, now_ms):
                                resolved.append(entry_id)
                            continue

            # --- Step 3: maker_try_window_fill_shortfall ---
            if (
                po.cancel_requested_at_ms <= 0
                and not po.maker_completed()
                and try_window_ms > 0
            ):
                shortfall = self._maker_try_window_fill_shortfall(
                    pending, po, now_ms, try_window_ms, min_fill_ratio
                )
                if shortfall is not None:
                    elapsed_ms, fill_ratio = shortfall
                    cancel_issued = await self._cancel_pending_passive_order(
                        pending, entry_id, po, adapter, now_ms,
                        "maker_try_window_fill_ratio_below_threshold",
                    )
                    if cancel_issued:
                        self.journal.append(
                            "passive_maintenance.cancel_try_window",
                            {
                                "entry_id": entry_id, "symbol": pending.symbol,
                                "elapsed_ms": elapsed_ms,
                                "fill_ratio": round(fill_ratio, 4),
                                "try_window_ms": try_window_ms,
                                "min_fill_ratio": min_fill_ratio,
                            },
                        )
                        continue

            # --- Step 4: maker_entry_rest_timeout ---
            if (
                po.cancel_requested_at_ms <= 0
                and not po.maker_completed()
                and po.timed_out(now_ms)
            ):
                cancel_issued = await self._cancel_pending_passive_order(
                    pending, entry_id, po, adapter, now_ms,
                    "maker_entry_rest_timeout_exceeded",
                )
                if cancel_issued:
                    self.journal.append(
                        "passive_maintenance.cancel_rest_timeout",
                        {
                            "entry_id": entry_id, "symbol": pending.symbol,
                            "venue": maker_venue.value,
                            "order_id": po.order_id,
                            "client_order_id": po.client_order_id,
                            "timeout_at_ms": po.timeout_at_ms,
                            "now_ms": now_ms,
                            "rest_timeout_ms": rest_timeout_ms,
                            "cancel_ack_terminal": False,
                            "truth_required_by": "pending_entry_passive_reconciliation",
                            "next_truth_probe": "query_passive_order_progress",
                            "post_cancel_state": "pending_truth_confirmation",
                        },
                    )
                    continue

            # --- Step 5: Post-cancel terminal handling ---
            if po.cancel_requested() and po.maker_completed():
                cancel_elapsed = now_ms - po.cancel_requested_at_ms
                if not pending.has_any_fill():
                    retained = await self._handle_pending_passive_zero_fill_completion(
                        pending, entry_id, po, adapter, now_ms
                    )
                    if retained:
                        continue
                    removed = await self._abort_pending_entry(
                        pending,
                        entry_id,
                        f"passive_maker_{po.last_progress_state.value}_zero_fill",
                    )
                    if removed:
                        resolved.append(entry_id)
                elif pending.missing_hedge_quantity() <= 1e-9:
                    if await self._try_repost_pending_entry_remainder(
                        pending,
                        entry_id,
                        po,
                        adapter,
                        now_ms,
                    ):
                        continue
                    if await self._finalize_pending_entry(pending, entry_id, now_ms):
                        resolved.append(entry_id)
                elif await self._maybe_finalize_pending_entry_terminal_hedge_dust(
                    pending,
                    entry_id,
                    now_ms,
                    source="passive_maintenance",
                ):
                    resolved.append(entry_id)
                elif cancel_elapsed > 30_000:
                    # Stale cancel with partial fill — force finalize what we have
                    if await self._finalize_pending_entry(pending, entry_id, now_ms):
                        resolved.append(entry_id)
                else:
                    # Drive hedge for partial fill
                    hedge_driven = await self._drive_missing_hedge_live(
                        pending, entry_id, now_ms
                    )
                    if hedge_driven and pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        if await self._finalize_pending_entry(pending, entry_id, now_ms):
                            resolved.append(entry_id)
                    else:
                        pending.next_progress_poll_ms = now_ms + poll_ms
            else:
                # Still resting — schedule next poll
                pending.next_progress_poll_ms = now_ms + poll_ms

        for eid in resolved:
            resolved_pending = self.state.pending_entries.get(eid)
            await self._complete_pending_entry_terminal_removal(
                eid,
                reason="passive_entry_maintenance_resolved",
                symbol=str(getattr(resolved_pending, "symbol", "") or ""),
                now_ms=now_ms,
            )

    async def _submit_pending_entry_passive_order_with_retries(
        self,
        *,
        pending,
        entry_id: str,
        adapter,
        quantity: float,
        price: float | None,
        stage_prefix: str,
        start_attempt_index: int = 0,
    ):
        """V1: passive post-only attempt loop for pending-entry submit/repost."""

        from lightfee.core.domain import OrderRequest
        from lightfee.venues.cid import compact_client_order_id

        attempt_limit = self._pending_entry_passive_post_only_attempt_limit()
        attempt_index = max(0, int(start_attempt_index or 0))
        last_error: Exception | None = None
        while attempt_index < attempt_limit:
            client_order_id = compact_client_order_id(
                entry_id,
                f"{stage_prefix}_attempt_{attempt_index}",
            )
            price_hint = self._pending_entry_post_only_price_hint_at_attempt(
                pending,
                attempt_index,
                fallback_price=price,
            )
            if price_hint is None:
                raise _PendingEntryPassiveSubmitFinalized("missing_passive_price_hint")
            request = OrderRequest(
                venue=pending.maker_venue(),
                symbol=pending.symbol,
                side=pending.maker_side(),
                quantity=quantity,
                price=price_hint if price_hint and price_hint > 0 else None,
                client_order_id=client_order_id,
                post_only=True,
                reduce_only=False,
                price_hint=price_hint if price_hint and price_hint > 0 else None,
            )
            try:
                ack = await adapter.submit_passive_order(request)
                return ack, request, attempt_index + 1
            except Exception as exc:
                last_error = exc
                if (
                    attempt_index + 1 >= attempt_limit
                    or not self._entry_reject_is_post_only_would_take(str(exc))
                ):
                    raise
                wait_ms = self._pending_entry_passive_retry_wait_ms(
                    str(exc),
                    attempt_index,
                )
                self._freeze_pending_entry_passive_maker_venue_from_error(
                    pending.maker_venue(),
                    str(exc),
                    wait_ms,
                )
                if wait_ms > 0:
                    await self._pending_entry_post_only_retry_sleep(wait_ms)
                attempt_index += 1
                await self._refresh_pending_entry_passive_market_snapshot(
                    pending,
                    adapter,
                )
                self.journal.append(
                    "execution.passive_entry_requote_retry",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_venue": pending.maker_venue().value,
                        "maker_leg": getattr(pending, "maker_leg", "long"),
                        "attempt": attempt_index,
                        "error": str(exc),
                        "wait_ms": wait_ms,
                        "price_hint": request.price,
                    },
                )
        raise _PendingEntryPassiveSubmitFinalized("max_passive_attempts_reached")

    @classmethod
    def _pending_entry_passive_post_only_attempt_limit(cls) -> int:
        """V1: passive_post_only_attempt_limit()."""

        return (
            len(cls._PASSIVE_POST_ONLY_WIDE_SPREAD_LADDER_FRACTIONS)
            + cls._PASSIVE_POST_ONLY_CLOSEST_PRICE_EXTRA_RETRIES
        )

    def _pending_entry_post_only_price_hint_at_attempt(
        self,
        pending,
        attempt_index: int,
        *,
        fallback_price: float | None,
    ) -> float | None:
        """V1 boundary: post_only_entry_price_hint_at_attempt for pending entry IO."""

        quote = self._pending_entry_passive_best_quote(pending)
        if quote is None:
            return None
        best_bid, best_ask = quote
        side = pending.maker_side()
        adaptive_ladder_enabled, queue_jump_enabled = (
            self._pending_entry_passive_ladder_profile(pending.maker_venue())
        )
        attempt = self._pending_entry_passive_queue_jump_attempt_index(
            attempt_index,
            queue_jump_enabled=queue_jump_enabled,
        )
        price = self._pending_entry_post_only_price_from_book_at_attempt(
            best_bid,
            best_ask,
            side,
            attempt,
            adaptive_ladder_enabled=adaptive_ladder_enabled,
        )
        price = self._pending_entry_apply_inventory_bias(
            pending,
            best_bid,
            best_ask,
            side,
            price,
        )
        price = self._pending_entry_apply_edge_headroom(
            pending,
            best_bid,
            best_ask,
            side,
            price,
            attempt,
        )
        tick_size = self._pending_entry_infer_price_tick_size([best_bid, best_ask])
        if tick_size > 0.0:
            from lightfee.venues.common import align_passive_price_to_tick

            price = align_passive_price_to_tick(price, tick_size, side)
        if math.isfinite(price) and price > 0.0:
            return price
        return fallback_price if fallback_price and fallback_price > 0 else None

    def _pending_entry_passive_best_quote(self, pending) -> tuple[float, float] | None:
        if self._entry_readiness_provider_uses_ws_bbo():
            return self._resolve_ws_bbo_close_quote(
                pending.maker_venue(), pending.symbol,
            )
        if self._entry_readiness_provider_uses_local_l2():
            quote = self._resolve_local_l2_quote(pending.maker_venue(), pending.symbol)
            if quote is not None:
                return quote
        return None

    def _pending_entry_passive_ladder_profile(self, venue: Venue) -> tuple[bool, bool]:
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        for venue_config in getattr(self.config, "venues", []) or []:
            if str(getattr(venue_config, "venue", "")).lower() != venue_value:
                continue
            passive_maker = getattr(getattr(venue_config, "live", None), "passive_maker", None)
            if passive_maker is not None:
                return (
                    bool(getattr(passive_maker, "adaptive_ladder_enabled", True)),
                    bool(getattr(passive_maker, "queue_jump_enabled", True)),
                )
        strategy = self.config.strategy
        return (
            bool(getattr(strategy, "passive_adaptive_ladder_enabled", True)),
            bool(getattr(strategy, "passive_queue_jump_enabled", True)),
        )

    @staticmethod
    def _pending_entry_passive_queue_jump_attempt_index(
        attempt: int,
        *,
        queue_jump_enabled: bool,
    ) -> int | None:
        """V1: passive_queue_jump_attempt_index; None is Rust usize::MAX."""

        if not queue_jump_enabled:
            return max(0, int(attempt or 0))
        if int(attempt or 0) == 0:
            return None
        return min(max(0, int(attempt or 0)), 2)

    @classmethod
    def _pending_entry_post_only_price_from_book_at_attempt(
        cls,
        best_bid: float,
        best_ask: float,
        side: Side,
        attempt: int | None,
        *,
        adaptive_ladder_enabled: bool,
    ) -> float:
        fallback = best_bid if side == Side.BUY else best_ask
        if (
            not math.isfinite(best_bid)
            or not math.isfinite(best_ask)
            or best_bid <= 0.0
            or best_ask <= 0.0
        ):
            return fallback
        if best_ask <= best_bid:
            return fallback

        closest = cls._pending_entry_post_only_closest_price(best_bid, best_ask, side)
        if attempt is None:
            return closest
        ladder = cls._pending_entry_passive_ladder_fractions_for_spread_bps(
            cls._pending_entry_passive_spread_bps(best_bid, best_ask),
            adaptive_ladder_enabled=adaptive_ladder_enabled,
        )
        stage_index = min(max(0, int(attempt or 0)), len(ladder) - 1)
        fraction = ladder[stage_index]
        if side == Side.BUY:
            return best_bid + (closest - best_bid) * fraction
        return best_ask - (best_ask - closest) * fraction

    @staticmethod
    def _pending_entry_post_only_closest_price(
        best_bid: float,
        best_ask: float,
        side: Side,
    ) -> float:
        if side == Side.BUY:
            return max(math.nextafter(best_ask, -math.inf), best_bid)
        return min(math.nextafter(best_bid, math.inf), best_ask)

    @classmethod
    def _pending_entry_passive_ladder_fractions_for_spread_bps(
        cls,
        spread_bps: float,
        *,
        adaptive_ladder_enabled: bool,
    ) -> tuple[float, ...]:
        if not adaptive_ladder_enabled:
            return cls._PASSIVE_POST_ONLY_LADDER_FRACTIONS
        if (
            not math.isfinite(spread_bps)
            or spread_bps <= cls._PASSIVE_POST_ONLY_TIGHT_SPREAD_BPS
        ):
            return cls._PASSIVE_POST_ONLY_TIGHT_SPREAD_LADDER_FRACTIONS
        if spread_bps <= cls._PASSIVE_POST_ONLY_WIDE_SPREAD_BPS:
            return cls._PASSIVE_POST_ONLY_BALANCED_SPREAD_LADDER_FRACTIONS
        return cls._PASSIVE_POST_ONLY_WIDE_SPREAD_LADDER_FRACTIONS

    @staticmethod
    def _pending_entry_passive_spread_bps(best_bid: float, best_ask: float) -> float:
        if (
            not math.isfinite(best_bid)
            or not math.isfinite(best_ask)
            or best_bid <= 0.0
            or best_ask <= best_bid
        ):
            return 0.0
        mid = max((best_bid + best_ask) * 0.5, sys.float_info.epsilon)
        return ((best_ask - best_bid) / mid) * 10_000.0

    @staticmethod
    def _pending_entry_infer_price_tick_size(values: list[float]) -> float:
        tick_size = 0.0
        for value in values:
            if not (math.isfinite(value) and value > 0.0):
                continue
            text = str(value)
            if "e" in text.lower():
                text = format(value, ".15f").rstrip("0").rstrip(".")
            if "." not in text:
                continue
            fractional = text.split(".", 1)[1].rstrip("0")
            if not fractional:
                continue
            inferred = 10.0 ** (-len(fractional))
            tick_size = inferred if tick_size <= 0.0 else min(tick_size, inferred)
        return tick_size

    def _pending_entry_apply_inventory_bias(
        self,
        pending,
        best_bid: float,
        best_ask: float,
        side: Side,
        base_price: float,
    ) -> float:
        if not (math.isfinite(base_price) and base_price > 0.0 and best_ask > best_bid):
            return base_price
        passive_maker = self._pending_entry_passive_maker_config(pending.maker_venue())
        inventory_bias_enabled = bool(
            getattr(passive_maker, "maker_inventory_bias_enabled", None)
            if passive_maker is not None and hasattr(passive_maker, "maker_inventory_bias_enabled")
            else getattr(self.config.strategy, "maker_inventory_bias_enabled", True)
        )
        if not inventory_bias_enabled:
            return base_price
        threshold = self._pending_entry_maker_inventory_bias_threshold_quote()
        signed_inventory = self._pending_entry_signed_inventory_notional_quote(
            pending.maker_venue(),
            pending.symbol,
            (best_bid + best_ask) * 0.5,
        )
        if (
            not math.isfinite(signed_inventory)
            or abs(signed_inventory) <= sys.float_info.epsilon
            or not math.isfinite(threshold)
            or threshold <= 0.0
        ):
            return base_price

        pressure = min(max(abs(signed_inventory) / threshold, 0.0), 1.0)
        if pressure <= sys.float_info.epsilon:
            return base_price
        bps_per_unit = float(getattr(self.config.strategy, "maker_inventory_bias_bps_per_unit", 25.0) or 0.0)
        max_bps = float(getattr(self.config.strategy, "maker_inventory_bias_max_bps", 25.0) or 0.0)
        shift_bps = min(max(bps_per_unit * pressure, 0.0), max_bps)
        if shift_bps <= sys.float_info.epsilon:
            return base_price

        shift = max((best_bid + best_ask) * 0.5, sys.float_info.epsilon) * (shift_bps / 10_000.0)
        closest = self._pending_entry_post_only_closest_price(best_bid, best_ask, side)
        biased_price = base_price - shift if signed_inventory > 0.0 else base_price + shift
        if side == Side.BUY:
            return max(min(biased_price, closest), best_bid)
        return min(max(biased_price, closest), best_ask)

    def _pending_entry_apply_edge_headroom(
        self,
        pending,
        best_bid: float,
        best_ask: float,
        side: Side,
        base_price: float,
        attempt: int | None,
    ) -> float:
        if attempt == 0:
            return base_price
        edge_headroom_bps = self._pending_entry_edge_headroom_bps(pending)
        if edge_headroom_bps is None:
            return base_price
        full_headroom = self._pending_entry_full_aggression_headroom_bps()
        if (
            not math.isfinite(edge_headroom_bps)
            or not math.isfinite(full_headroom)
            or full_headroom <= 0.0
            or best_ask <= best_bid
        ):
            return base_price

        pressure = min(max(edge_headroom_bps / full_headroom, -1.0), 1.0)
        if abs(pressure) <= sys.float_info.epsilon:
            return base_price
        closest = self._pending_entry_post_only_closest_price(best_bid, best_ask, side)
        if side == Side.BUY and pressure > 0.0:
            adjusted = base_price + max(closest - base_price, 0.0) * pressure
        elif side == Side.BUY:
            adjusted = base_price - max(base_price - best_bid, 0.0) * -pressure
        elif pressure > 0.0:
            adjusted = base_price - max(base_price - closest, 0.0) * pressure
        else:
            adjusted = base_price + max(best_ask - base_price, 0.0) * -pressure
        if side == Side.BUY:
            return max(min(adjusted, closest), best_bid)
        return min(max(adjusted, closest), best_ask)

    def _pending_entry_passive_maker_config(self, venue: Venue) -> Any | None:
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        for venue_config in getattr(self.config, "venues", []) or []:
            if str(getattr(venue_config, "venue", "")).lower() != venue_value:
                continue
            return getattr(getattr(venue_config, "live", None), "passive_maker", None)
        return None

    def _pending_entry_maker_inventory_bias_threshold_quote(self) -> float:
        strategy = self.config.strategy
        return max(
            float(getattr(strategy, "live_entry_notional_cap_quote", 0.0) or 0.0),
            float(getattr(strategy, "entry_notional_cap_quote", 0.0) or 0.0),
            float(getattr(strategy, "min_entry_leg_notional_quote", 0.0) or 0.0),
            1.0,
        )

    def _pending_entry_signed_inventory_notional_quote(
        self,
        venue: Venue,
        symbol: str,
        price_hint: float,
    ) -> float:
        if not (math.isfinite(price_hint) and price_hint > 0.0):
            return 0.0
        venue_value = venue.value if hasattr(venue, "value") else str(venue)
        signed_quantity = 0.0
        for position in getattr(self.state, "open_positions", {}).values():
            if getattr(position, "symbol", "") != symbol:
                continue
            if hasattr(position, "long_venue") or hasattr(position, "short_venue"):
                long_venue = getattr(position, "long_venue", "")
                short_venue = getattr(position, "short_venue", "")
                long_value = long_venue.value if hasattr(long_venue, "value") else str(long_venue)
                short_value = short_venue.value if hasattr(short_venue, "value") else str(short_venue)
                quantity = float(getattr(position, "quantity", 0.0) or 0.0)
                if long_value == venue_value:
                    signed_quantity += quantity
                if short_value == venue_value:
                    signed_quantity -= quantity
                continue
            pos_venue = getattr(position, "venue", "")
            pos_value = pos_venue.value if hasattr(pos_venue, "value") else str(pos_venue)
            if pos_value != venue_value:
                continue
            quantity = float(getattr(position, "quantity", 0.0) or 0.0)
            side = getattr(position, "side", None)
            signed_quantity += quantity if side == Side.BUY else -quantity
        return signed_quantity * price_hint

    def _pending_entry_edge_headroom_bps(self, pending) -> float | None:
        source = getattr(pending, "frozen_candidate", None)
        if not isinstance(source, dict):
            source = {
                "entry_notional_quote": getattr(pending, "entry_notional_quote", 0.0),
                "expected_edge_bps": getattr(pending, "expected_edge_bps_entry", 0.0),
                "worst_case_edge_bps": getattr(pending, "worst_case_edge_bps_entry", 0.0),
            }
        try:
            entry_notional = float(source.get("entry_notional_quote", 0.0) or 0.0)
            expected_edge = float(source.get("expected_edge_bps", 0.0) or 0.0)
            worst_case_edge = float(source.get("worst_case_edge_bps", 0.0) or 0.0)
        except (TypeError, ValueError):
            return None
        if not (
            math.isfinite(entry_notional)
            and math.isfinite(expected_edge)
            and math.isfinite(worst_case_edge)
        ):
            return None
        strategy = self.config.strategy
        min_expected = float(getattr(strategy, "min_expected_edge_bps", 0.0) or 0.0)
        min_worst = float(getattr(strategy, "min_worst_case_edge_bps", 0.0) or 0.0)
        return min(expected_edge - min_expected, worst_case_edge - min_worst)

    def _pending_entry_full_aggression_headroom_bps(self) -> float:
        return max(
            float(getattr(self.config.strategy, "execution_buffer_bps", 0.0) or 0.0) * 4.0,
            self._MAKER_EDGE_AWARE_FULL_AGGRESSION_HEADROOM_BPS_FLOOR,
        )

    def _pending_entry_passive_retry_wait_ms(self, error: str, attempt_index: int) -> int:
        schedule = self._PASSIVE_POST_ONLY_RETRY_BACKOFF_MS
        idx = min(max(0, int(attempt_index or 0)), len(schedule) - 1)
        minimum = schedule[idx]
        retry_after = self._pending_entry_retry_after_ms(error)
        if self._pending_entry_error_is_rate_limited(error):
            return max(minimum, retry_after)
        return minimum

    @staticmethod
    def _pending_entry_retry_after_ms(error: str) -> int:
        marker = "retry_after_ms="
        text = str(error or "").lower()
        start = text.find(marker)
        if start < 0:
            return 0
        digits = []
        for ch in text[start + len(marker):]:
            if not ch.isdigit():
                break
            digits.append(ch)
        return int("".join(digits) or "0")

    @staticmethod
    def _pending_entry_error_is_rate_limited(error: str) -> bool:
        text = str(error or "").lower()
        return (
            "status=429" in text
            or "too many requests" in text
            or "rate limited" in text
            or "retry_after" in text
        )

    def _freeze_pending_entry_passive_maker_venue_from_error(
        self,
        venue: Venue,
        error: str,
        wait_ms: int,
    ) -> None:
        if wait_ms <= 0 or not self._pending_entry_error_is_rate_limited(error):
            return
        venue_key = venue.value if hasattr(venue, "value") else str(venue)
        until_ms = wall_clock_now_ms() + wait_ms
        self._maker_venue_request_budget_frozen_until_ms[venue_key] = max(
            int(self._maker_venue_request_budget_frozen_until_ms.get(venue_key, 0) or 0),
            until_ms,
        )

    async def _pending_entry_post_only_retry_sleep(self, wait_ms: int) -> None:
        await asyncio.sleep(wait_ms / 1000.0)

    async def _refresh_pending_entry_passive_market_snapshot(self, pending, adapter) -> None:
        refresh = getattr(adapter, "refresh_market_snapshot", None)
        if refresh is None:
            return
        try:
            await refresh(pending.symbol)
        except Exception as exc:
            self.journal.append(
                "execution.passive_entry_market_refresh_failed",
                {
                    "entry_id": getattr(pending, "pending_id", ""),
                    "symbol": pending.symbol,
                    "maker_venue": pending.maker_venue().value,
                    "error": str(exc),
                },
            )

    async def _try_repost_pending_entry_remainder(
        self,
        pending,
        entry_id: str,
        po,
        adapter,
        now_ms: int,
    ) -> bool:
        """V1: `try_repost_pending_entry_remainder` runtime IO wrapper."""

        if not getattr(po, "maker_completed", lambda: False)():
            return False
        if not pending.has_any_fill():
            return False
        if pending.missing_hedge_quantity() > 1e-9:
            return False

        remaining_quantity = max(
            0.0,
            float(getattr(pending, "target_quantity", 0.0) or 0.0)
            - float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
        )
        if remaining_quantity <= 1e-9:
            return False

        try:
            normalized_quantity = float(
                await adapter.normalize_quantity(pending.symbol, remaining_quantity)
            )
        except Exception as exc:
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            self.journal.append(
                "execution.passive_entry_repost_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": pending.maker_venue().value,
                    "remaining_quantity": remaining_quantity,
                    "error": str(exc),
                },
            )
            return True

        action = prepare_pending_entry_remainder_repost(
            pending,
            self.config.strategy,
            normalized_quantity=normalized_quantity,
        )
        if action.kind == "finalized":
            remaining = float(action.evidence.get("remaining_quantity", remaining_quantity) or 0.0)
            if remaining > 1e-9:
                self.journal.append(
                    "execution.passive_entry_repost_exhausted",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": action.reason,
                        **action.evidence,
                    },
                )
            return False

        if not self._try_consume_maker_venue_budget(pending.maker_venue(), now_ms):
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "passive_maintenance.repost_budget_delayed",
                {"entry_id": entry_id, "venue": str(pending.maker_venue())},
            )
            return True

        from lightfee.core.domain import PassiveOrderState

        price = (
            getattr(po, "limit_price", None)
            if getattr(po, "limit_price", None) is not None
            else getattr(pending, "maker_price", 0.0)
        )
        try:
            ack, request, passive_attempt_count = (
                await self._submit_pending_entry_passive_order_with_retries(
                    pending=pending,
                    entry_id=entry_id,
                    adapter=adapter,
                    quantity=normalized_quantity,
                    price=price if price and price > 0 else None,
                    stage_prefix=(
                        f"{getattr(pending, 'maker_leg', 'long')}_repost_"
                        f"{pending.repost_attempt_count + 1}"
                    ),
                    start_attempt_index=int(getattr(pending, "passive_attempt_count", 0) or 0),
                )
            )
        except _PendingEntryPassiveSubmitFinalized as finalized:
            remaining = float(action.evidence.get("remaining_quantity", remaining_quantity) or 0.0)
            if remaining > 1e-9:
                self.journal.append(
                    "execution.passive_entry_repost_exhausted",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": finalized.reason,
                        **action.evidence,
                    },
                )
            return False
        except Exception as exc:
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            self.journal.append(
                "execution.passive_entry_repost_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": pending.maker_venue().value,
                    "remaining_quantity": remaining_quantity,
                    "error": str(exc),
                },
            )
            return True

        accepted_at_ms = int(getattr(ack, "accepted_at_ms", 0) or now_ms)
        order_id = getattr(ack, "order_id", "") or po.order_id
        ack_client_order_id = (
            getattr(ack, "client_order_id", "")
            or request.client_order_id
            or ""
        )
        ack_price = getattr(ack, "price", 0.0) or request.price or price or 0.0
        ack_quantity = getattr(ack, "quantity", 0.0) or normalized_quantity
        ack_state = getattr(ack, "state", None) or PassiveOrderState.OPEN

        pending.maker_order_id = order_id
        pending.maker_client_order_id = ack_client_order_id
        pending.maker_price = float(ack_price or 0.0)
        note_pending_entry_remainder_repost_accepted(
            pending,
            order_id=order_id,
            client_order_id=ack_client_order_id,
            accepted_at_ms=accepted_at_ms,
            limit_price=float(ack_price) if ack_price and ack_price > 0 else None,
            target_quantity=float(ack_quantity),
            passive_attempt_count=passive_attempt_count,
            rest_timeout_ms=getattr(self.config.strategy, "maker_entry_rest_timeout_ms", 6000) or 6000,
        )
        if pending.passive_order is not None:
            pending.passive_order.last_progress_state = ack_state
        self.journal.append(
            "execution.passive_entry_reposted",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "maker_venue": pending.maker_venue().value,
                "attempt": pending.repost_attempt_count,
                "remaining_quantity": remaining_quantity,
                "repost_quantity": normalized_quantity,
                "price_hint": request.price,
            },
        )
        return True

    async def _handle_pending_passive_zero_fill_completion(
        self,
        pending,
        entry_id: str,
        po,
        adapter,
        now_ms: int,
    ) -> bool:
        """V1 zero-fill passive cycle: record delay, then repost before terminal abort."""
        strategy = self.config.strategy
        max_reposts = getattr(strategy, "maker_entry_max_reposts", 0) or 0

        if not pending.has_any_fill():
            from lightfee.engine.v1_lifecycle import V1TradingLifecycle

            decision = V1TradingLifecycle.pending_entry_viability(
                pending,
                now_ms=now_ms,
                strategy=strategy,
            )
            if not decision.allowed:
                self.journal.append(
                    "pending_entry.viability_blocked",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": decision.reason,
                        **decision.evidence,
                        "ts_ms": now_ms,
                    },
                )
                if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                    self._apply_reconcile_backoff(pending, now_ms)
                return True

        metadata = pending.metadata if isinstance(pending.metadata, dict) else {}
        pending.metadata = metadata
        retry_pending = bool(metadata.get("passive_zero_fill_retry_pending"))

        if not retry_pending:
            retry_delay_ms = record_pending_entry_zero_fill_cycle(pending, strategy, now_ms)
            phase_state = ensure_pending_entry_phase_state(pending, now_ms)
            cycles = int(phase_state.zero_fill_cycles_in_phase or 0)
            metadata["passive_zero_fill_cycles"] = cycles
            metadata["passive_zero_fill_retry_pending"] = True
            metadata["passive_zero_fill_retry_at_ms"] = pending.next_progress_poll_ms
            self.journal.append(
                "passive_maintenance.zero_fill_cycle",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "cycle_attempt": cycles,
                    "retry_delay_ms": retry_delay_ms,
                    "retry_at_ms": pending.next_progress_poll_ms,
                    "state": po.last_progress_state.value,
                    "repost_count": pending.repost_count,
                    "max_reposts": max_reposts,
                },
            )
            return True

        retry_at_ms = int(metadata.get("passive_zero_fill_retry_at_ms", 0) or 0)
        if retry_at_ms > 0 and now_ms < retry_at_ms:
            pending.next_progress_poll_ms = retry_at_ms
            return True

        phase_state = ensure_pending_entry_phase_state(pending, now_ms)
        previous_phase = phase_state.phase
        zero_fill_candidate = self._apply_terminal_taker_runtime_entry_guards(
            self._pending_entry_terminal_fallback_candidate(pending),
            pending,
            now_ms,
        )
        action = advance_pending_entry_zero_fill_phase(
            pending,
            strategy,
            now_ms,
            candidate=zero_fill_candidate,
        )
        phase_budget = pending_entry_phase_zero_fill_budget(self.config.strategy)
        if action.reason == "candidate_not_tradeable_after_zero_fill_reprice":
            self.journal.append(
                "execution.direction_drift_blocked",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": action.reason,
                    "blocked_reasons": action.evidence.get("blocked_reasons", []),
                    "phase": previous_phase,
                    "phase_zero_fill_budget": phase_budget,
                },
            )
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True
        if action.reason == "phase_switched_to_low_slippage_maker":
            phase_state = ensure_pending_entry_phase_state(pending, now_ms)
            metadata["passive_zero_fill_cycles"] = 0
            self.journal.append(
                "execution.passive_phase_switched",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "from_phase": previous_phase,
                    "to_phase": phase_state.phase,
                    "maker_leg": pending.maker_leg,
                    "maker_venue": str(pending.maker_venue()),
                    "phase_zero_fill_budget": phase_budget,
                },
            )
        elif action.kind == "trigger_dual_taker":
            self.journal.append(
                "execution.dual_taker_armed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": action.reason,
                    "phase_zero_fill_budget": phase_budget,
                },
            )
            if await self._execute_pending_entry_terminal_taker_fallback(
                pending,
                entry_id,
                now_ms,
                action.reason,
            ):
                return True
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True
        elif action.reason == "dual_taker_phase_already_armed":
            if await self._execute_pending_entry_terminal_taker_fallback(
                pending,
                entry_id,
                now_ms,
                action.reason,
            ):
                return True
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True

        maker_venue = pending.maker_venue()
        if not self._try_consume_maker_venue_budget(maker_venue, now_ms):
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "passive_maintenance.repost_budget_delayed",
                {"entry_id": entry_id, "venue": str(maker_venue)},
            )
            return True

        from lightfee.core.domain import PassiveOrderState

        quantity = po.target_quantity or pending.target_quantity
        price = po.limit_price if po.limit_price is not None else pending.maker_price
        cycle_action = prepare_pending_entry_passive_cycle(
            pending,
            normalized_quantity=quantity,
        )
        if cycle_action.kind == "finalized":
            self.journal.append(
                "passive_maintenance.zero_fill_repost_exhausted",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": cycle_action.reason,
                    **cycle_action.evidence,
                },
            )
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True
        submit_adapter = self._venue_adapters.get(maker_venue)
        if submit_adapter is None:
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            self.journal.append(
                "passive_maintenance.repost_error",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": str(maker_venue),
                    "error": "maker venue adapter unavailable",
                },
            )
            return True
        try:
            ack, request, passive_attempt_count = (
                await self._submit_pending_entry_passive_order_with_retries(
                    pending=pending,
                    entry_id=entry_id,
                    adapter=submit_adapter,
                    quantity=quantity,
                    price=price if price and price > 0 else None,
                    stage_prefix=f"maker_repost_{pending.repost_count + 1}",
                    start_attempt_index=0,
                )
            )
        except _PendingEntryPassiveSubmitFinalized as finalized:
            self.journal.append(
                "passive_maintenance.zero_fill_repost_exhausted",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": finalized.reason,
                    **cycle_action.evidence,
                },
            )
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True
        except Exception as exc:
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            self.journal.append(
                "passive_maintenance.repost_error",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": str(maker_venue),
                    "error": str(exc),
                },
            )
            return True

        accepted_at_ms = getattr(ack, "accepted_at_ms", 0) or now_ms
        order_id = getattr(ack, "order_id", "") or po.order_id
        client_order_id = getattr(ack, "client_order_id", "") or request.client_order_id or ""
        ack_price = getattr(ack, "price", 0.0) or request.price or price or 0.0
        ack_quantity = getattr(ack, "quantity", 0.0) or quantity
        ack_state = getattr(ack, "state", None) or PassiveOrderState.UNKNOWN

        pending.repost_count += 1
        phase_state = ensure_pending_entry_phase_state(pending, now_ms)
        pending.maker_order_id = order_id
        pending.maker_client_order_id = client_order_id
        pending.maker_price = float(ack_price or 0.0)
        note_pending_entry_passive_cycle_accepted(
            pending,
            order_id=order_id,
            client_order_id=client_order_id,
            accepted_at_ms=accepted_at_ms,
            limit_price=float(ack_price) if ack_price and ack_price > 0 else po.limit_price,
            target_quantity=float(ack_quantity),
            passive_attempt_count=passive_attempt_count,
            rest_timeout_ms=getattr(strategy, "maker_entry_rest_timeout_ms", 6000) or 6000,
        )
        po = pending.passive_order
        if po is not None:
            po.last_progress_state = ack_state
        metadata["passive_zero_fill_retry_pending"] = False
        self.journal.append(
            "passive_maintenance.passive_entry_reposted",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": str(maker_venue),
                "order_id": order_id,
                "client_order_id": client_order_id,
                "repost_count": pending.repost_count,
                "quantity": pending.passive_order.target_quantity if pending.passive_order else 0.0,
                "price": pending.passive_order.limit_price if pending.passive_order else None,
            },
        )
        return True

    def _maker_try_window_fill_shortfall(
        self,
        pending,
        po,
        now_ms: int,
        try_window_ms: int,
        min_fill_ratio: float,
    ) -> Optional[tuple]:
        """V1: maker_try_window_fill_shortfall (entry_sync.rs:577-601).

        Only triggers for zero-fill orders.  Returns (elapsed_ms, fill_ratio)
        when the maker order has been resting beyond try_window_ms and the
        fill ratio is below min_fill_ratio.
        """
        if try_window_ms == 0:
            return None
        if pending.has_any_fill():
            return None
        if po.cancel_requested():
            return None
        if po.maker_completed():
            return None
        if po.accepted_at_ms <= 0:
            return None
        elapsed_ms = max(0, now_ms - po.accepted_at_ms)
        if elapsed_ms < try_window_ms:
            return None
        target = po.target_quantity
        if target <= 1e-9:
            return None
        fill_ratio = pending.maker_leg_filled / target
        if fill_ratio + 1e-9 >= min_fill_ratio:
            return None
        return (elapsed_ms, fill_ratio)

    async def _cancel_pending_passive_order(
        self,
        pending,
        entry_id: str,
        po,
        adapter,
        now_ms: int,
        reason: str,
    ) -> bool:
        """V1: cancel_pending_entry_passive_order (entry_sync.rs:2401-2445).

        1. Returns false if already canceled or maker completed
        2. Checks maker venue request budget
        3. Issues cancel_passive_order on the venue adapter
        4. Sets cancel_requested_at_ms and updates next_progress_poll_ms
        5. Returns true if cancel was successfully issued
        """
        if po.cancel_requested() or po.maker_completed():
            return False

        # Rate-limit gate
        maker_venue = pending.maker_venue()
        if not self._try_consume_maker_venue_budget(maker_venue, now_ms):
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "passive_maintenance.cancel_budget_delayed",
                {"entry_id": entry_id, "venue": str(maker_venue),
                 "reason": reason},
            )
            return False

        try:
            await adapter.cancel_passive_order(
                symbol=pending.symbol,
                order_id=po.order_id,
                client_order_id=po.client_order_id or None,
            )
        except Exception as exc:
            self.journal.append(
                "passive_maintenance.cancel_error",
                {"entry_id": entry_id, "symbol": pending.symbol,
                 "venue": str(maker_venue), "error": str(exc)},
            )
            # V1: on cancel error, query progress to see if order is already
            # done — then apply terminal state if confirmed
            try:
                progress = await adapter.query_passive_order_progress(
                    symbol=pending.symbol,
                    order_id=po.order_id,
                    client_order_id=po.client_order_id or None,
                    side=pending.maker_side(),
                )
                if progress is not None and progress.state.is_terminal():
                    po.last_progress_state = progress.state
                    self.journal.append(
                        "passive_maintenance.cancel_error_resolved_via_progress",
                        {"entry_id": entry_id,
                         "resolved_state": progress.state.value},
                    )
                    # Continue to post-cancel handling in next cycle
                    pending.next_progress_poll_ms = now_ms + (
                        self.config.strategy.maker_entry_rest_timeout_ms or 6000
                    ) // 2
                    return False
            except Exception:
                pass
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            return False

        note_passive_operation(pending)
        po.cancel_requested_at_ms = now_ms
        pending.next_progress_poll_ms = now_ms + self.config.strategy.maker_venue_budget_window_ms
        self.journal.append(
            "passive_maintenance.cancel_issued",
            {"entry_id": entry_id, "symbol": pending.symbol,
             "venue": str(maker_venue), "reason": reason,
             "cancel_requested_at_ms": now_ms},
        )
        return True

    # ------------------------------------------------------------------
    # Reconciliation (V1 recovery/reconciliation live path — Fix 3)
    # ------------------------------------------------------------------

    # V1 reconciliation retry constants (Rust V1 recovery.rs)
    _RECONCILE_RETRY_BASE_MS = 30_000
    _RECONCILE_RETRY_MAX_MS = 300_000
    _RECONCILE_HARD_DEADLINE_MS = 600_000  # 10 min hard deadline

    @staticmethod
    def _venue_from_close_reconciliation(value: Any) -> Venue | None:
        return CloseRuntime._venue_from_close_reconciliation(value)

    @staticmethod
    def _close_reconciliation_leg_identity(leg: Any) -> tuple[str, str]:
        return CloseRuntime._close_reconciliation_leg_identity(leg)

    @classmethod
    def _has_close_reconciliation_leg_identity(cls, legs: Any) -> bool:
        return CloseRuntime._has_close_reconciliation_leg_identity(legs)

    @staticmethod
    def _close_reconciliation_fill_qty(fill: Any) -> float:
        return CloseRuntime._close_reconciliation_fill_qty(fill)

    async def _fetch_close_leg_reconciliations(
        self,
        *,
        symbol: str,
        venue: Venue,
        legs: Any,
    ) -> list[Any] | None:
        return await self.close_runtime._fetch_close_leg_reconciliations(
            symbol=symbol,
            venue=venue,
            legs=legs,
        )

    @staticmethod
    def _close_reconciliation_live_size(position: Any) -> float:
        return CloseRuntime._close_reconciliation_live_size(position)

    async def _fetch_pending_close_terminal_live_sizes(
        self,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
    ) -> tuple[float, float] | None:
        return await self.close_runtime._fetch_pending_close_terminal_live_sizes(
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
        )

    async def _try_abandon_stale_pending_close_reconciliation(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
        *,
        symbol: str,
        long_venue: Venue,
        short_venue: Venue,
        error: str,
    ) -> bool:
        return await self.close_runtime._try_abandon_stale_pending_close_reconciliation(
            reconciliation,
            now_ms,
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            error=error,
        )

    def _venue_private_position_confirmed(self, venue: Venue, symbol: str) -> bool:
        return self.close_runtime._venue_private_position_confirmed(venue, symbol)

    def _open_positions_private_confirmation_ready(self) -> bool:
        return self.close_runtime._open_positions_private_confirmation_ready()

    @staticmethod
    def _aggregate_close_reconciliation_fills(fills: list[Any]) -> dict[str, Any]:
        return CloseRuntime._aggregate_close_reconciliation_fills(fills)

    def _apply_pending_close_reconciliation_backoff(
        self,
        reconciliation: dict[str, Any],
        now_ms: int,
    ) -> None:
        return self.close_runtime._apply_pending_close_reconciliation_backoff(
            reconciliation,
            now_ms,
        )

    def _exit_reconciled_payload_from_leg_fills(
        self,
        reconciliation: dict[str, Any],
        long_fills: list[Any],
        short_fills: list[Any],
        now_ms: int,
    ) -> dict[str, Any]:
        return self.close_runtime._exit_reconciled_payload_from_leg_fills(
            reconciliation,
            long_fills,
            short_fills,
            now_ms,
        )

    async def _process_pending_close_reconciliations(self, now_ms: int) -> None:
        return await self.close_runtime._process_pending_close_reconciliations(now_ms)

    async def _reconcile_pending_state(self, *args, **kwargs):
        return await self.pending_entry_runtime._reconcile_pending_state(*args, **kwargs)

    async def _try_abandon_stale_entry(self, pending, entry_id: str) -> bool:
        """V1-style stale entry abandonment via live-size probe.

        V1: try_abandon_stale_pending_close_reconciliation() — after 1 failed
        reconciliation, if the entry no longer references an active position AND
        both venues report ~zero live size, the entry is abandoned immediately.
        No hard deadline — real evidence only.
        """
        # Entry must reference a position_id that is no longer active
        pos_id = pending.position_id if hasattr(pending, 'position_id') else pending.pending_id
        if self.state.open_positions.get(pos_id) is not None:
            return False  # still active, don't abandon

        # Probe both venues for live position size
        try:
            from lightfee.core.domain import Venue as VenueEnum
            long_ven = VenueEnum.from_str(pending.long_venue) if isinstance(pending.long_venue, str) else pending.long_venue
            short_ven = VenueEnum.from_str(pending.short_venue) if isinstance(pending.short_venue, str) else pending.short_venue
            long_adapter = self._venue_adapters.get(long_ven)
            short_adapter = self._venue_adapters.get(short_ven)
        except (ValueError, KeyError):
            long_adapter = None
            short_adapter = None

        long_zero = True
        short_zero = True
        try:
            if long_adapter is not None:
                pos = await long_adapter.fetch_position(pending.symbol)
                long_zero = pos is None or abs(pos.quantity) <= 1e-9
        except Exception:
            long_zero = False  # can't probe → assume not zero

        try:
            if short_adapter is not None:
                pos = await short_adapter.fetch_position(pending.symbol)
                short_zero = pos is None or abs(pos.quantity) <= 1e-9
        except Exception:
            short_zero = False

        if long_zero and short_zero:
            if await self._pending_entry_has_unresolved_maker_order(pending, entry_id):
                self.journal.append(
                    "reconciliation.entry_abandon_retained_unresolved_maker",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": "both_venues_zero_but_maker_order_not_terminal",
                    },
                )
                return False
            self.journal.append(
                "reconciliation.entry_abandoned_flat",
                {"entry_id": entry_id, "reason": "both_venues_zero"},
            )
            return True

        return False

    @staticmethod
    def _apply_reconcile_backoff(pending, now_ms: int) -> None:
        """Apply exponential backoff to a PendingEntry or PendingClose.

        V1: CLOSE_RECONCILIATION_RETRY_BASE_MS=30s, max=300s.
        """
        backoff = min(
            LiveRuntime._RECONCILE_RETRY_BASE_MS * (2 ** max(pending.reconcile_attempt - 1, 0)),
            LiveRuntime._RECONCILE_RETRY_MAX_MS,
        )
        pending.reconcile_next_attempt_ms = now_ms + backoff

    @staticmethod
    def _pending_entry_reconcile_maker_status(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_reconcile_maker_status(*args, **kwargs)

    @staticmethod
    def _order_status_is_terminal_no_fill(*args, **kwargs):
        return PendingEntryRuntime._order_status_is_terminal_no_fill(*args, **kwargs)

    @staticmethod
    def _fill_reconciliation_terminal_no_fill(*args, **kwargs):
        return PendingEntryRuntime._fill_reconciliation_terminal_no_fill(*args, **kwargs)

    @staticmethod
    def _pending_entry_has_terminal_maker_zero_fill_evidence(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_has_terminal_maker_zero_fill_evidence(*args, **kwargs)

    @staticmethod
    def _pending_entry_has_maker_order_reference(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_has_maker_order_reference(*args, **kwargs)

    @staticmethod
    def _pending_entry_maker_order_identifiers(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_maker_order_identifiers(*args, **kwargs)

    @staticmethod
    def _pending_entry_maker_cancel_requested(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_maker_cancel_requested(*args, **kwargs)

    def _mark_pending_entry_maker_cancel_requested(self, *args, **kwargs):
        return self.pending_entry_runtime._mark_pending_entry_maker_cancel_requested(*args, **kwargs)

    @staticmethod
    def _pending_entry_open_order_matches(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_open_order_matches(*args, **kwargs)

    async def _pending_entry_maker_open_order_matches(self, *args, **kwargs):
        return await self.pending_entry_runtime._pending_entry_maker_open_order_matches(*args, **kwargs)

    async def _ensure_pending_entry_maker_not_open_before_abort(self, *args, **kwargs):
        return await self.pending_entry_runtime._ensure_pending_entry_maker_not_open_before_abort(*args, **kwargs)

    def _pending_entry_flat_clear_has_terminal_maker_evidence(self, *args, **kwargs):
        return self.pending_entry_runtime._pending_entry_flat_clear_has_terminal_maker_evidence(*args, **kwargs)

    async def _pending_entry_has_unresolved_maker_order(self, *args, **kwargs):
        return await self.pending_entry_runtime._pending_entry_has_unresolved_maker_order(*args, **kwargs)

    def _try_clear_stale_hedge_inflight(self, pending, entry_id: str, result, now_ms: int) -> None:
        """Clear hedge_inflight when order/fills/position all prove no hedge.

        Safety: only clears inflight after ALL three evidence sources
        (order status, fills, position) confirm the hedge order does not
        exist on the exchange. This prevents duplicate hedge exposure.
        """
        hedge_venue = pending.hedge_venue()
        is_long_hedge = pending.maker_leg != "long"
        is_short_hedge = pending.maker_leg != "short"

        hedge_status = result.short_status if is_short_hedge else result.long_status
        hedge_fill_obj = result.short_fill if is_short_hedge else result.long_fill
        hedge_pos_obj = result.short_position if is_short_hedge else result.long_position

        hedge_fill_qty = hedge_fill_obj.quantity if hedge_fill_obj is not None else 0.0
        hedge_pos_qty = abs(hedge_pos_obj.quantity) if hedge_pos_obj is not None else 0.0

        order_absent = hedge_status in ("missing", "canceled", "rejected", "unknown", "not_found")
        fills_zero = hedge_fill_qty <= 1e-9
        position_zero = hedge_pos_qty <= 1e-9

        if order_absent and fills_zero and position_zero:
            old_inflight = pending.hedge_inflight
            pending.hedge_inflight = None
            self.journal.append(
                "pending_entry.hedge_inflight_cleared",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "hedge_venue": hedge_venue.value,
                    "old_hedge_inflight": old_inflight.client_order_id if old_inflight else "",
                    "hedge_status": hedge_status,
                    "hedge_fill_quantity": hedge_fill_qty,
                    "hedge_position_quantity": hedge_pos_qty,
                    "ts_ms": now_ms,
                },
            )

    async def _maybe_finalize_pending_entry_terminal_hedge_dust(
        self,
        pending,
        entry_id: str,
        now_ms: int,
        *,
        source: str,
    ) -> bool:
        """Finalize balanced fills when only an untradeable hedge dust remains."""
        missing = float(pending.missing_hedge_quantity() or 0.0)
        if missing <= 1e-9:
            return False
        if getattr(pending, "hedge_inflight", None) is not None:
            return False

        balanced_quantity = min(
            float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
            float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0),
        )
        if balanced_quantity <= 1e-9:
            return False

        hedge_venue = pending.hedge_venue()
        adapter = self.get_venue_adapter(hedge_venue)
        if adapter is None:
            return False

        normalized = missing
        try:
            if hasattr(adapter, "normalize_quantity"):
                normalized = await adapter.normalize_quantity(pending.symbol, missing)
        except Exception as exc:
            self.journal.append(
                "pending_entry.hedge_dust_terminalization_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "hedge_venue": hedge_venue.value,
                    "raw_missing_hedge_quantity": missing,
                    "error": str(exc),
                    "source": source,
                },
            )
            return False

        normalized = float(normalized or 0.0)
        hedge_price = float(
            getattr(pending, "maker_fill_price", 0.0)
            or getattr(pending, "maker_price", 0.0)
            or 0.0
        )
        min_notional = self._venue_min_notional(hedge_venue, pending.symbol)
        hedge_notional = abs(normalized * hedge_price)
        terminal_by_quantity = normalized <= 1e-9
        terminal_by_notional = (
            min_notional > 0.0
            and hedge_price > 0.0
            and hedge_notional + 1e-12 < min_notional
        )
        if not terminal_by_quantity and not terminal_by_notional:
            return False

        pending.repair_state = "hedge_residual_below_min_notional"
        self.journal.append(
            "pending_entry.hedge_residual_below_min_notional_terminalized",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "hedge_venue": hedge_venue.value,
                "raw_missing_hedge_quantity": missing,
                "normalized_quantity": normalized,
                "balanced_quantity": balanced_quantity,
                "hedge_price": hedge_price,
                "hedge_notional": hedge_notional,
                "hedge_min_notional": min_notional,
                "terminal_by_quantity": terminal_by_quantity,
                "terminal_by_notional": terminal_by_notional,
                "source": source,
            },
        )
        return await self._finalize_pending_entry(pending, entry_id, now_ms)

    # ------------------------------------------------------------------
    # V1 parity: hedge deadline, terminalization budget, abort/cleanup
    # ------------------------------------------------------------------

    def _pending_entry_hedge_deadline_decision(
        self, pending, now_ms: int
    ) -> dict:
        """V1: pending_entry_hedge_deadline_decision + adaptive_hedge_deadline_status.

        Returns dict with:
          - hard_breached: bool — elapsed >= hard_deadline_ms
          - soft_breached: bool — elapsed >= soft_deadline_ms
          - hard_deadline_ms: int — effective hard deadline
          - soft_deadline_ms: int — effective soft deadline
          - hedge_elapsed_ms: int — time since hedge submission
        """
        if pending.hedge_inflight is None:
            return {
                "hard_breached": False,
                "soft_breached": False,
                "hard_deadline_ms": 0,
                "soft_deadline_ms": 0,
                "hedge_elapsed_ms": 0,
            }

        hedge_elapsed_ms = pending.hedge_inflight.elapsed_ms(now_ms)
        strategy = self.config.strategy
        base_hard_ms = int(getattr(strategy, "maker_hedge_deadline_ms", 800) or 800)
        base_soft_ms = int(
            getattr(
                strategy,
                "maker_hedge_soft_deadline_ms",
                min(base_hard_ms, 800) if base_hard_ms > 0 else 800,
            )
            or 0
        )
        hedge_price = self._pending_entry_hedge_price_hint(pending)
        hedge_notional = abs(float(pending.hedge_inflight.quantity or 0.0) * hedge_price)
        deadline_decision = adaptive_entry_hedge_deadline_decision(
            hedge_elapsed_ms=hedge_elapsed_ms,
            base_soft_deadline_ms=base_soft_ms,
            base_hard_deadline_ms=base_hard_ms,
            hedge_notional_quote=hedge_notional,
            quote_fresh=hedge_price > 0.0,
            has_execution_progress=float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0) > 1e-9,
            reconciled=False,
        )
        hard_deadline_ms = deadline_decision.effective_hard_deadline_ms
        soft_deadline_ms = deadline_decision.effective_soft_deadline_ms
        phase_state = getattr(pending, "phase_state", None)
        stored_deadline_at_ms = getattr(phase_state, "hedge_deadline_at_ms", None)
        if stored_deadline_at_ms is not None and pending.hedge_inflight.submitted_at_ms > 0:
            hard_deadline_ms = max(
                1,
                int(stored_deadline_at_ms or 0) - pending.hedge_inflight.submitted_at_ms,
            )
            soft_deadline_ms = min(soft_deadline_ms, hard_deadline_ms)

        # V1: legacy inflight (submitted_at_ms=0) has no timestamp — fall back
        # to entry lifetime as a conservative proxy so old production pending
        # entries eventually get a deadline decision instead of blocking
        # hedge drive indefinitely.
        if pending.hedge_inflight.submitted_at_ms <= 0:
            entry_lifetime = pending.compute_lifetime_ms(now_ms)
            if entry_lifetime >= getattr(strategy, "pending_entry_hard_ceiling_ms", 120000):
                hedge_elapsed_ms = entry_lifetime

        hard_breached = hedge_elapsed_ms > hard_deadline_ms
        soft_breached = hedge_elapsed_ms > soft_deadline_ms

        return {
            "hard_breached": hard_breached,
            "soft_breached": soft_breached,
            "hard_deadline_ms": hard_deadline_ms,
            "soft_deadline_ms": soft_deadline_ms,
            "hedge_elapsed_ms": hedge_elapsed_ms,
        }

    def _pending_entry_terminalization_budget(
        self, pending, now_ms: int
    ) -> dict | None:
        """V1: pending_entry_terminalization_budget_from_input.

        Returns None if no budget is active, else dict with:
          - hard_ceiling_reached: bool
          - force_terminal_reached: bool
          - final_reason: str
          - lifetime_ms: int
        """
        strategy = self.config.strategy
        hard_ceiling_ms = getattr(strategy, "pending_entry_hard_ceiling_ms", 120000)
        force_terminal_after_ms = getattr(strategy, "pending_entry_force_terminal_after_ms", 60000)
        selected_terminal_sla_ms = int(
            getattr(strategy, "entry_selected_terminal_sla_ms", 300000) or 0
        )

        lifetime_ms = pending.compute_lifetime_ms(now_ms)
        selected_at_ms = self._pending_entry_selected_at_ms(pending)
        selected_lifetime_ms = (
            max(0, now_ms - selected_at_ms) if selected_at_ms > 0 else lifetime_ms
        )

        hard_ceiling_reached = lifetime_ms >= hard_ceiling_ms
        selected_sla_reached = (
            selected_terminal_sla_ms > 0
            and selected_at_ms > 0
            and selected_lifetime_ms >= selected_terminal_sla_ms
        )
        force_terminal_reached = (
            lifetime_ms >= force_terminal_after_ms
            and (not pending.has_any_fill() or pending.missing_hedge_quantity() <= 1e-9)
        )

        has_inflight = pending.hedge_inflight is not None
        if has_inflight and not hard_ceiling_reached and not selected_sla_reached:
            # V1: inflight hedge blocks terminalization until hard ceiling
            return None

        if not hard_ceiling_reached and not force_terminal_reached and not selected_sla_reached:
            return None

        final_reason = (
            "pending_entry_max_lifetime_exhausted"
            if hard_ceiling_reached
            else (
                "long_lived_pending_entry"
                if selected_sla_reached
                else "pending_entry_zero_fill_lifetime_exhausted"
            )
        )

        return {
            "hard_ceiling_reached": hard_ceiling_reached,
            "selected_sla_reached": selected_sla_reached,
            "force_terminal_reached": force_terminal_reached,
            "final_reason": final_reason,
            "lifetime_ms": lifetime_ms,
            "selected_at_ms": selected_at_ms,
            "selected_lifetime_ms": selected_lifetime_ms,
            "selected_terminal_sla_ms": selected_terminal_sla_ms,
        }

    @staticmethod
    def _pending_entry_selected_at_ms(pending) -> int:
        metadata = getattr(pending, "metadata", None)
        if isinstance(metadata, dict):
            for key in ("entry_selected_at_ms", "selected_at_ms"):
                try:
                    value = int(metadata.get(key) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    return value
        return int(getattr(pending, "created_at_ms", 0) or 0)

    async def _force_terminalize_pending_entry_if_budget_exhausted(
        self, pending, entry_id: str, now_ms: int
    ) -> bool:
        """V1 force_terminalize_pending_entry_if_budget_exhausted.

        Runs before flat-position retention, matching V1's pending entry
        driver. A stale zero-fill maker order must first go through maker
        cancel and abort/cleanup once hard ceiling is reached; lack of maker
        terminal evidence must not retain it forever.

        Returns True when this pending entry was handled for this tick, even if
        cleanup failed and the entry was deliberately retained fail-closed.
        """
        budget = self._pending_entry_terminalization_budget(pending, now_ms)
        if budget is None:
            return False

        hard_ceiling_reached = bool(budget.get("hard_ceiling_reached"))
        selected_sla_reached = bool(budget.get("selected_sla_reached"))
        force_terminal_reached = bool(budget.get("force_terminal_reached"))
        final_reason = str(budget["final_reason"])
        terminal_deadline_reached = hard_ceiling_reached or selected_sla_reached

        if selected_sla_reached:
            self.journal.append(
                "pending_entry.long_lived_pending_entry",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": final_reason,
                    "selected_at_ms": int(budget.get("selected_at_ms") or 0),
                    "selected_lifetime_ms": int(
                        budget.get("selected_lifetime_ms") or 0
                    ),
                    "pending_lifetime_ms": int(budget.get("lifetime_ms") or 0),
                    "sla_ms": int(budget.get("selected_terminal_sla_ms") or 0),
                    "hard_ceiling_reached": hard_ceiling_reached,
                    "force_terminal_reached": force_terminal_reached,
                },
            )

        if hard_ceiling_reached and pending.repair_state:
            self.journal.append(
                "pending_entry.min_notional_hard_ceiling_cleanup",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "repair_state": pending.repair_state,
                    "final_reason": final_reason,
                    "lifetime_ms": budget["lifetime_ms"],
                },
            )

        if not pending.maker_completed():
            cancel_issued = False
            if self._pending_entry_has_maker_order_reference(pending):
                cancel_issued = await self._recover_cancel_maker_order(
                    pending, entry_id, final_reason
                )

            if terminal_deadline_reached:
                if pending.has_any_fill() and pending.missing_hedge_quantity() <= 1e-9:
                    if await self._finalize_pending_entry(pending, entry_id, now_ms):
                        await self._complete_pending_entry_terminal_removal(
                            entry_id,
                            reason="terminal_budget_balanced_finalize",
                            symbol=pending.symbol,
                            now_ms=now_ms,
                        )
                        self.journal.append(
                            "recovery.pending_entry_finalized",
                            {
                                "entry_id": entry_id,
                                "symbol": pending.symbol,
                                "reason": final_reason,
                            },
                        )
                    else:
                        self._apply_reconcile_backoff(pending, now_ms)
                    return True

                await self._abort_pending_entry(pending, entry_id, final_reason)
                return True

            if cancel_issued:
                pending.reconcile_attempt += 1
                self._apply_reconcile_backoff(pending, now_ms)
                return True

            return False

        if terminal_deadline_reached:
            if not pending.has_any_fill():
                if getattr(
                    self.config.strategy,
                    "pending_entry_force_fallback_when_tradeable",
                    False,
                ):
                    fallback_ok = await self._recover_try_taker_fallback(
                        pending, entry_id, final_reason
                    )
                    if fallback_ok:
                        return True
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    await self._complete_pending_entry_terminal_removal(
                        entry_id,
                        reason="terminal_budget_abandoned_flat",
                        symbol=pending.symbol,
                        now_ms=now_ms,
                    )
                    return True

            # P6: if there are actual fills (not repair-state), give
            # exactly one terminal truth checkpoint before abort. Hard ceiling
            # must not drift into a multi-minute pending retention loop.
            if hard_ceiling_reached and pending.has_any_fill() and not pending.repair_state:
                hard_ceiling_ms = getattr(
                    self.config.strategy, "pending_entry_hard_ceiling_ms", 120000
                )
                extension_budget = budget["lifetime_ms"] - hard_ceiling_ms
                self.journal.append(
                    "pending_entry.hard_ceiling_reconcile_before_abort",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "has_fill": True,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "extension_budget_ms": extension_budget,
                        "reconcile_extension_ms": 0,
                        "reason": final_reason,
                        "action_taken": "abort_after_single_truth_pass",
                    },
                )

            await self._abort_pending_entry(pending, entry_id, final_reason)
            return True

        if force_terminal_reached:
            if await self._pending_entry_has_unresolved_maker_order(
                pending, entry_id
            ):
                self.journal.append(
                    "pending_entry.force_terminal_retained_unresolved_maker",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": final_reason,
                        "lifetime_ms": budget["lifetime_ms"],
                    },
                )
                self._apply_reconcile_backoff(pending, now_ms)
                return True
            if not await self._finalize_pending_entry(pending, entry_id, now_ms):
                self._apply_reconcile_backoff(pending, now_ms)
            return True

        return False

    async def _abort_pending_entry_fail_closed(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: abort_pending_entry_fail_closed — enter fail_closed, then abort.

        entry_sync.rs:2448-2456

        Returns True if pending was removed, False if retained (cleanup failed).
        """
        previous_risk_mode = self.state.risk_mode.value
        enter_fail_closed(self.state)
        long_venue = getattr(pending, "long_venue", None)
        short_venue = getattr(pending, "short_venue", None)
        event_payload = {
            "source": "auto_pending_entry_abort",
            "reason": reason,
            "entry_id": entry_id,
            "symbol": getattr(pending, "symbol", ""),
            "symbols": [getattr(pending, "symbol", "")],
            "venues": [
                getattr(long_venue, "value", str(long_venue or "")),
                getattr(short_venue, "value", str(short_venue or "")),
            ],
            "cleanup_actions": ["abort_pending_entry"],
            "previous_risk_mode": previous_risk_mode,
            "new_risk_mode": self.state.risk_mode.value,
            "ts_ms": wall_clock_now_ms(),
        }
        self.journal.append(
            "runtime.auto_fail_closed_entered",
            event_payload,
        )
        removed = await self._abort_pending_entry(pending, entry_id, reason)
        residual_blockers = []
        if self.state.recovery_blocked_reason:
            residual_blockers.append(self.state.recovery_blocked_reason)
        if self.state.pending_entries.get(entry_id) is not None:
            residual_blockers.append("pending_entry_retained")
        outcome_payload = {
            **event_payload,
            "cleanup_actions": ["abort_pending_entry_cleanup_succeeded"]
            if removed
            else ["abort_pending_entry_cleanup_failed"],
            "exchange_truth_summary": {
                "source": "abort_pending_entry_cleanup",
                "confidence": "high" if removed else "incomplete",
                "positions_flat": removed,
                "open_orders_absent": removed,
                "venues": event_payload["venues"],
                "symbol": event_payload["symbol"],
            },
            "new_risk_mode": self.state.risk_mode.value,
            "residual_blockers": residual_blockers,
            "ts_ms": wall_clock_now_ms(),
        }
        self.journal.append_critical(
            outcome_payload["ts_ms"],
            "runtime.auto_fail_closed_recovered"
            if removed and not residual_blockers
            else "runtime.auto_fail_closed_cleanup_failed",
            outcome_payload,
        )
        return removed

    async def _abort_pending_entry(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: abort_pending_entry — cleanup maker & hedge exposure, then remove.

        entry_sync.rs:4612-4708

        Two-tier exposure cleanup:
        1. cleanup_failed_leg_exposure for both maker and hedge legs
        2. If cleanup fails, compensation_hard_stop for both legs
        3. If hard stop also fails, enter fail_closed and retain pending
        4. On success, remove pending and emit entry.aborted

        Returns True if pending was removed, False if retained (cleanup failed).
        """
        maker_venue = pending.maker_venue()
        hedge_venue = pending.hedge_venue()
        symbol = pending.symbol

        if not await self._ensure_pending_entry_maker_not_open_before_abort(
            pending,
            entry_id,
            reason,
        ):
            enter_fail_closed(self.state)
            self.state.last_error = reason
            return False

        # Tier 1: cleanup/flatten residual exposure on both legs
        maker_cleaned = await self._cleanup_failed_leg_exposure(
            maker_venue, symbol, entry_id, "maker"
        )
        hedge_cleaned = await self._cleanup_failed_leg_exposure(
            hedge_venue, symbol, entry_id, "hedge"
        )

        # V1: None (adapter missing) means uncertain — treat as failure
        if maker_cleaned is not True or hedge_cleaned is not True:
            # Tier 2: compensation hard stop (market order to flatten at any price)
            maker_stopped = await self._cleanup_failed_leg_exposure(
                maker_venue, symbol, entry_id, "maker_hard_stop"
            )
            hedge_stopped = await self._cleanup_failed_leg_exposure(
                hedge_venue, symbol, entry_id, "hedge_hard_stop"
            )

            if maker_stopped is not True or hedge_stopped is not True:
                # Tier 3: cleanup failed → fail_closed, retain pending
                enter_fail_closed(self.state)
                self.state.last_error = reason
                self.journal.append(
                    "entry.abort_failed_pending_retained",
                    {
                        "entry_id": entry_id,
                        "symbol": symbol,
                        "reason": reason,
                        "maker_cleaned": maker_cleaned,
                        "hedge_cleaned": hedge_cleaned,
                        "maker_hard_stop": maker_stopped,
                        "hedge_hard_stop": hedge_stopped,
                    },
                )
                return False

        # Success: remove pending entry
        await self._complete_pending_entry_terminal_removal(
            entry_id,
            reason="abort_pending_entry_cleanup_succeeded",
            symbol=symbol,
            now_ms=wall_clock_now_ms(),
        )
        self.state.last_error = reason
        self.journal.append(
            "entry.aborted",
            {
                "entry_id": entry_id,
                "symbol": symbol,
                "reason": reason,
                "maker_quantity": pending.maker_leg_filled,
                "hedge_quantity": pending.hedge_leg_filled,
            },
        )
        return True

    async def _cleanup_failed_leg_exposure(
        self, venue, symbol: str, entry_id: str, stage: str
    ) -> bool | None:
        """V1: flatten residual startup/recovery exposure on one venue.

        entry.rs:2711-2801, recovery.rs:1750-1870

        Returns:
          True: position was flattened (or was already zero)
          False: cleanup failed (position remains or can't verify)
          None: no adapter available (caller treats as uncertain — not success)
        """
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        from lightfee.venues.cid import generate_exchange_cid

        def cleanup_client_order_id_for_attempt(attempt: int) -> str:
            seed = f"{entry_id}:{stage}:{symbol}"
            if attempt > 1:
                seed = f"{seed}:attempt:{attempt}"
            return generate_exchange_cid(seed, "c", venue)

        max_attempts = 3
        retry_quantity_by_attempt: dict[int, float] = {}
        for attempt in range(1, max_attempts + 1):
            cleanup_client_order_id = cleanup_client_order_id_for_attempt(attempt)
            try:
                pos = await adapter.fetch_position(symbol)
            except Exception as e:
                private_health = classify_private_health_error(venue, str(e))
                if private_health is not None:
                    _reason, status = private_health
                    self.journal.append(
                        "cleanup_blocked_by_venue_auth_invalid",
                        {
                            "severity": "critical",
                            "entry_id": entry_id,
                            "stage": stage,
                            "venue": venue.value,
                            "symbol": symbol,
                            "reason": "cleanup_blocked_by_venue_auth_invalid",
                            "venue_private_health_status": status,
                            "action": "fetch_position_truth",
                            "reduce_only": True,
                            "raw_error": str(e)[:500],
                        },
                    )
                return False  # can't verify — assume position exists

            if pos is None or abs(pos.quantity) <= 1e-9:
                return True  # Already flat

            # V1: direction is based on position.side, NOT signed quantity.
            # V2 PositionSnapshot.quantity is always abs(size); side carries direction.
            # side=BUY (long) → cleanup SELL; side=SELL (short) → cleanup BUY
            cleanup_side = pos.side.opposite()
            live_quantity = abs(pos.quantity)
            cleanup_quantity = live_quantity
            retry_quantity = retry_quantity_by_attempt.get(attempt)
            if retry_quantity is not None:
                cleanup_quantity = min(live_quantity, retry_quantity)
                if cleanup_quantity <= 1e-9:
                    return True

            event_kind = (
                "entry.cleanup_leg_exposure"
                if attempt == 1
                else "entry.cleanup_leg_exposure_retry"
            )
            self.journal.append(
                event_kind,
                {
                    "entry_id": entry_id,
                    "stage": stage,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "venue": venue.value,
                    "symbol": symbol,
                    "size": pos.quantity,
                    "target_qty": cleanup_quantity,
                    "side": pos.side.value,
                    "cleanup_side": cleanup_side.value,
                    "cleanup_client_order_id": cleanup_client_order_id,
                    "client_order_id": cleanup_client_order_id,
                },
            )

            try:
                from lightfee.core.domain import OrderRequest

                req = OrderRequest(
                    venue=venue,
                    symbol=symbol,
                    side=cleanup_side,
                    quantity=cleanup_quantity,
                    price=None,
                    post_only=False,
                    reduce_only=True,  # V1: cleanup always reduce-only
                    time_in_force=TimeInForce.IOC,
                    client_order_id=cleanup_client_order_id,
                )
                fill = await adapter.place_order(req)
                self._flush_adapter_order_diagnostics(adapter)

                # V1: cleanup success needs EITHER fill covering target qty
                # OR verified-flat position after partial/ambiguous fill.
                target_qty = cleanup_quantity
                if fill.quantity >= target_qty - 1e-9:
                    return True

                try:
                    verify_pos = await adapter.fetch_position(symbol)
                    if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                        return True
                except Exception:
                    pass
            except Exception as e:
                self._flush_adapter_order_diagnostics(adapter)
                private_health = classify_private_health_error(venue, str(e))
                if private_health is not None:
                    _reason, status = private_health
                    self.journal.append(
                        "cleanup_blocked_by_venue_auth_invalid",
                        {
                            "severity": "critical",
                            "entry_id": entry_id,
                            "stage": stage,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "venue": venue.value,
                            "symbol": symbol,
                            "reason": "cleanup_blocked_by_venue_auth_invalid",
                            "venue_private_health_status": status,
                            "action": "reduce_only_flatten",
                            "reduce_only": True,
                            "target_qty": cleanup_quantity,
                            "cleanup_side": cleanup_side.value,
                            "cleanup_client_order_id": cleanup_client_order_id,
                            "client_order_id": cleanup_client_order_id,
                            "raw_error": str(e)[:500],
                        },
                    )
                    return False
                is_bybit_duplicate = (
                    venue == Venue.BYBIT
                    and _is_bybit_duplicate_order_link_id(str(e))
                )
                if is_bybit_duplicate:
                    next_client_order_id = (
                        cleanup_client_order_id_for_attempt(attempt + 1)
                        if attempt < max_attempts
                        else ""
                    )
                    duplicate_reconcile = await self._reconcile_bybit_duplicate_cleanup_order(
                        adapter=adapter,
                        symbol=symbol,
                        entry_id=entry_id,
                        stage=stage,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        client_order_id=cleanup_client_order_id,
                        next_client_order_id=next_client_order_id,
                        target_qty=cleanup_quantity,
                        live_pos_before=pos,
                        original_error=str(e),
                    )
                    if duplicate_reconcile.clear_state:
                        return True
                    if not duplicate_reconcile.should_retry_with_new_client_id:
                        return False
                    if attempt >= max_attempts:
                        return False
                    retry_quantity_by_attempt[attempt + 1] = duplicate_reconcile.retry_qty
                    self.journal.append(
                        "entry.cleanup_leg_exposure_retry_scheduled",
                        {
                            "entry_id": entry_id,
                            "stage": stage,
                            "next_attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "venue": venue.value,
                            "symbol": symbol,
                            "client_order_id": cleanup_client_order_id,
                            "original_client_order_id": cleanup_client_order_id,
                            "next_client_order_id": next_client_order_id,
                            "reason": "duplicate_client_order_id_partial",
                            "target_qty": duplicate_reconcile.target_qty,
                            "reconciled_qty": duplicate_reconcile.reconciled_qty,
                            "live_qty": duplicate_reconcile.live_qty,
                            "remaining_qty": duplicate_reconcile.remaining_qty,
                            "retry_qty": duplicate_reconcile.retry_qty,
                            "retry_quantity": duplicate_reconcile.retry_qty,
                            "decision": duplicate_reconcile.decision,
                            "classification": duplicate_reconcile.classification,
                        },
                    )
                    continue
                try:
                    verify_pos = await adapter.fetch_position(symbol)
                    if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                        return True
                except Exception:
                    pass

            if attempt >= max_attempts:
                return False

            self.journal.append(
                "entry.cleanup_leg_exposure_retry_scheduled",
                {
                    "entry_id": entry_id,
                    "stage": stage,
                    "next_attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "venue": venue.value,
                    "symbol": symbol,
                },
            )

        return False

    async def _reconcile_bybit_duplicate_cleanup_order(
        self,
        *,
        adapter,
        symbol: str,
        entry_id: str,
        stage: str,
        attempt: int,
        max_attempts: int,
        client_order_id: str,
        next_client_order_id: str,
        target_qty: float,
        live_pos_before: PositionSnapshot,
        original_error: str,
    ) -> BybitDuplicateReconcileResult:
        """Reconcile Bybit duplicate cleanup order ids before retrying.

        This intentionally uses the same adapter.fetch_order_fill_reconciliation
        contract as passive close/close execution so Bybit endpoint semantics
        stay centralized in the venue adapter.
        """
        result = await reconcile_bybit_duplicate_client_order(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
            target_qty=target_qty,
            live_pos_before=live_pos_before,
        )

        payload = {
            "entry_id": entry_id,
            "stage": stage,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "venue": Venue.BYBIT.value,
            "symbol": symbol,
            "client_order_id": client_order_id,
            "next_client_order_id": next_client_order_id,
            "reconcile_endpoints": list(BYBIT_DUPLICATE_RECONCILE_ENDPOINTS),
            "classification": result.classification,
            "reconciled_quantity": result.reconciled_qty,
            "target_quantity": result.target_qty,
            "reconciled_qty": result.reconciled_qty,
            "target_qty": result.target_qty,
            "live_qty": result.live_qty,
            "remaining_qty": result.remaining_qty,
            "retry_qty": result.retry_qty,
            "order_id": result.order_id,
            "live_exposure": {
                "quantity": result.live_qty,
                "side": result.live_side,
            },
            "decision": result.decision,
            "original_error": original_error,
        }
        if result.reconcile_error:
            payload["reconcile_error"] = result.reconcile_error
        if result.live_fetch_error:
            payload["live_fetch_error"] = result.live_fetch_error

        self.journal.append(
            "order.reconcile_result",
            build_order_reconcile_result_payload(
                result=result,
                symbol=symbol,
                client_order_id=client_order_id,
                reason="duplicate_client_id",
            ),
        )
        self.journal.append(
            "entry.cleanup_duplicate_client_order_reconcile_result",
            payload,
        )
        return result

    async def _reconcile_pending_entries_force(self, now_ms: int) -> None:
        """Force-reconcile pending entries ignoring backoff windows.

        V1: reconcile_open_positions() with force_reconcile=true — used at
        startup recovery to immediately resolve any uncertain outcomes before
        resuming normal operations.
        """
        if self.reconciler is None or not self._venue_adapters:
            return

        resolved_ids: list[str] = []
        for entry_id, pending in list(self.state.pending_entries.items()):
            if getattr(pending, "outcome", "") == "rejected" and pending.has_any_fill():
                if await self._maybe_finalize_rejected_pending_with_fill(
                    pending,
                    entry_id,
                    now_ms,
                    source="startup_force_reconcile",
                ):
                    continue
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            if not pending.uncertain_outcome:
                resolved_ids.append(entry_id)
                continue

            try:
                hedge_lookup_cid = (
                    pending.hedge_inflight.client_order_id
                    if pending.hedge_inflight
                    else ""
                )
                maker_order_id, maker_client_order_id = (
                    self._pending_entry_maker_order_identifiers(pending)
                )
                result = await self.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=maker_order_id,
                    short_order_id=pending.hedge_order_id,
                    long_client_order_id=maker_client_order_id,
                    short_client_order_id=hedge_lookup_cid,
                )
                self._flush_reconciler_order_diagnostics()
            except Exception as e:
                self._flush_reconciler_order_diagnostics()
                self.journal.append(
                    "recovery.force_reconcile_entry_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                pending.maker_leg_filled = result.long_fill.quantity if result.long_fill else pending.maker_leg_filled
                pending.hedge_leg_filled = result.short_fill.quantity if result.short_fill else pending.hedge_leg_filled
                if result.long_fill and _recon_fill_price(result.long_fill) > 0:
                    pending.maker_fill_price = _recon_fill_price(result.long_fill)
                if result.short_fill and _recon_fill_price(result.short_fill) > 0:
                    pending.hedge_fill_price = _recon_fill_price(result.short_fill)
                if await self._finalize_pending_entry(pending, entry_id, now_ms):
                    resolved_ids.append(entry_id)
                else:
                    self._apply_reconcile_backoff(pending, now_ms)
            elif result.is_flat:
                if not self._pending_entry_flat_clear_has_terminal_maker_evidence(
                    pending, result
                ):
                    self.journal.append(
                        "recovery.force_reconcile_flat_unresolved_maker_retained",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_status": self._pending_entry_reconcile_maker_status(
                                pending, result
                            ),
                            "reason": "flat_position_without_terminal_maker_order_evidence",
                        },
                    )
                    continue
                resolved_ids.append(entry_id)

        for eid in resolved_ids:
            resolved_pending = self.state.pending_entries.get(eid)
            await self._complete_pending_entry_terminal_removal(
                eid,
                reason="force_reconcile_pending_entry_resolved",
                symbol=str(getattr(resolved_pending, "symbol", "") or ""),
                now_ms=now_ms,
            )

        self.journal.append(
            "recovery.force_reconcile_complete",
            {"resolved_entries": len(resolved_ids), "ts_ms": now_ms},
        )

    async def _maybe_finalize_rejected_pending_with_fill(
        self,
        pending,
        entry_id: str,
        now_ms: int,
        *,
        source: str,
    ) -> bool:
        """Close V1 recovery for retained rejected entries with positive fills.

        A deterministic maker reject with zero fill can clear. Once either leg
        has positive fill evidence, local false-flat is no longer a terminal
        state: finalize the matched quantity and queue residual cleanup, or
        retain with explicit deferred evidence if fill details are incomplete.
        """
        if getattr(pending, "outcome", "") != "rejected" or not pending.has_any_fill():
            return False

        before_maker = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
        before_hedge = float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0)
        before_pending_residuals = len(
            getattr(self.state, "pending_residual_repairs", []) or []
        )
        hydrated = await self._recover_hydrate_from_live_positions(pending)

        finalized = await self._finalize_pending_entry(pending, entry_id, now_ms)

        opened_position = self.state.open_positions.get(entry_id)
        if opened_position is not None:
            await self._complete_pending_entry_terminal_removal(
                entry_id,
                reason="rejected_pending_without_fill",
                symbol=pending.symbol,
                now_ms=now_ms,
            )
            finalized = True

        if finalized:
            self.journal.append(
                "recovery.rejected_pending_positive_fill_finalized",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "source": source,
                    "hydrated_from_live_truth": hydrated,
                    "before_maker_leg_filled": before_maker,
                    "before_hedge_leg_filled": before_hedge,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "balanced_quantity": min(
                        float(pending.maker_leg_filled or 0.0),
                        float(pending.hedge_leg_filled or 0.0),
                    ),
                    "opened_position": opened_position is not None,
                    "pending_residual_repairs_added": max(
                        0,
                        len(getattr(self.state, "pending_residual_repairs", []) or [])
                        - before_pending_residuals,
                    ),
                },
            )
            return True

        self.journal.append(
            "recovery.rejected_pending_positive_fill_deferred",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "source": source,
                "hydrated_from_live_truth": hydrated,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "maker_fill_price": pending.maker_fill_price,
                "hedge_fill_price": pending.hedge_fill_price,
                "reason": "positive_fill_evidence_incomplete_for_finalization",
            },
        )
        return False

    def _flush_reconciler_order_diagnostics(self) -> None:
        if self.reconciler is None:
            return
        drain = getattr(self.reconciler, "drain_order_diagnostics", None)
        if not callable(drain):
            return
        for event in drain():
            kind = event.get("kind", "")
            payload = event.get("payload", {})
            if isinstance(kind, str) and isinstance(payload, dict):
                self.journal.append(kind, payload)

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        """Drain order diagnostics from a venue adapter's transport into the journal."""
        transport = getattr(adapter, "_transport", adapter)
        drain = getattr(transport, "drain_order_diagnostics", None)
        if not callable(drain):
            return
        for event in drain():
            kind = event.get("kind", "")
            payload = event.get("payload", {})
            if isinstance(kind, str) and isinstance(payload, dict):
                self.journal.append(kind, payload)

    async def _recover_pending_entry_hedges(self, now_ms: int) -> None:
        """Re-drive pending entry hedges with full V1 startup recovery semantics.

        V1: process_pending_entry_hedges() + drive_pending_entry_hedge() +
            force_terminalize_pending_entry_if_budget_exhausted() +
            hydrate_pending_entry_from_live_balanced_exposure()

        For each pending entry that is startup_recovery_ready:
        1. Query venue order status to resolve uncertain outcomes
        2. Try to hydrate from live balanced exposure (reconcile from exchange positions)
        3. Compute terminalization budget (lifetime vs hard_ceiling/force_terminal)
        4. If budget exhausted → abort or finalize per V1 rules
        5. If maker filled but hedge missing → attempt to drive hedge
        """
        if not self._venue_adapters:
            return

        strategy = self.config.strategy
        hard_ceiling_ms = strategy.pending_entry_hard_ceiling_ms
        force_terminal_after_ms = strategy.pending_entry_force_terminal_after_ms

        for entry_id, pending in list(self.state.pending_entries.items()):
            if await self._maybe_finalize_rejected_pending_with_fill(
                pending,
                entry_id,
                now_ms,
                source="startup_recovery",
            ):
                continue

            # V1: startup_recovery_ready gate — skip entries that don't need recovery yet
            if not pending.startup_recovery_ready():
                continue

            lifetime_ms = pending.compute_lifetime_ms(now_ms)

            # --- Step 1: Query venue for order status (resolve uncertain outcomes) ---
            if pending.uncertain_outcome:
                await self._recover_poll_order_status(entry_id, pending)

            # Re-check after order status poll: if no longer startup_recovery_ready, skip
            if not pending.startup_recovery_ready():
                continue

            # --- Step 2: Try hydrate from live balanced exposure ---
            # V1: hydrate_pending_entry_from_live_balanced_exposure —
            # fetches live positions from both venues; if there's already balanced
            # exposure, applies fills and may finalize.
            hydrated = await self._recover_hydrate_from_live_positions(pending)
            if hydrated:
                self.journal.append(
                    "recovery.pending_entry_live_balance_hydrated",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_filled": pending.maker_leg_filled,
                        "hedge_filled": pending.hedge_leg_filled,
                    },
                )

            # Re-check after hydration: if no longer needs recovery, skip
            if not pending.startup_recovery_ready():
                continue

            if await self._maybe_finalize_pending_entry_terminal_hedge_dust(
                pending,
                entry_id,
                now_ms,
                source="startup_recovery",
            ):
                await self._complete_pending_entry_terminal_removal(
                    entry_id,
                    reason="terminal_hedge_dust_after_live_truth",
                    symbol=pending.symbol,
                    now_ms=now_ms,
                )
                self.journal.append(
                    "recovery.pending_entry_finalized",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": "terminal_hedge_dust_after_live_truth",
                    },
                )
                continue

            # --- Step 3: Terminalization budget (shared helper) ---
            budget = self._pending_entry_terminalization_budget(pending, now_ms)
            if budget is None:
                # Below terminalization thresholds — re-poll, don't force
                pending.reconcile_attempt += 1
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            hard_ceiling_reached = budget["hard_ceiling_reached"]
            final_reason = budget["final_reason"]

            # --- Step 4: Handle terminalization ---
            # V1: force_terminalize_pending_entry_if_budget_exhausted
            # Two main paths: maker not completed (cancel first) vs maker completed

            # --- 4a: Maker not completed → cancel maker order first (V1 cancel-before-abort) ---
            if (
                not pending.maker_completed()
                and self._pending_entry_has_maker_order_reference(pending)
            ):
                cancel_issued = await self._recover_cancel_maker_order(
                    pending, entry_id, final_reason
                )
                if hard_ceiling_reached and not cancel_issued:
                    # V1: hard ceiling + cancel failed → abort (with cleanup)
                    await self._abort_pending_entry(pending, entry_id, final_reason)
                    continue
                if cancel_issued:
                    # V1: cancel was issued
                    if hard_ceiling_reached:
                        if pending.has_any_fill() and pending.missing_hedge_quantity() <= 1e-9:
                            # Balanced fill → finalize even on hard ceiling
                            if await self._finalize_pending_entry(pending, entry_id, now_ms):
                                await self._complete_pending_entry_terminal_removal(
                                    entry_id,
                                    reason="cancel_completed_entry_balanced",
                                    symbol=pending.symbol,
                                    now_ms=now_ms,
                                )
                                self.journal.append(
                                    "recovery.pending_entry_finalized",
                                    {"entry_id": entry_id, "symbol": pending.symbol,
                                     "reason": "cancel_completed_entry_balanced"},
                                )
                            else:
                                pending.reconcile_attempt += 1
                                self._apply_reconcile_backoff(pending, now_ms)
                        else:
                            # Has fills with missing hedge or no fills → abort (with cleanup)
                            await self._abort_pending_entry(pending, entry_id, final_reason)
                        continue
                    # Cancel issued but below hard ceiling → keep for progress poll
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue
                # Cancel not issued (e.g. budget delayed) and below hard ceiling → keep
                if not hard_ceiling_reached:
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue

            # --- 4b: Zero fills → abort or try taker fallback ---
            if not pending.has_any_fill():
                # V1: try taker fallback when tradeable (config gated)
                if getattr(strategy, "pending_entry_force_fallback_when_tradeable", False):
                    fallback_ok = await self._recover_try_taker_fallback(
                        pending, entry_id, final_reason
                    )
                    if fallback_ok:
                        continue
                # Zero fills — live-size probe first to verify no exchange residual.
                # V1: zero local fills doesn't guarantee zero exchange exposure.
                # Must attempt cleanup/flatten before removing pending.
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    await self._complete_pending_entry_terminal_removal(
                        entry_id,
                        reason="startup_zero_fill_abandoned_flat",
                        symbol=pending.symbol,
                        now_ms=now_ms,
                    )
                    continue  # Both venues flat → safe to clear
                removed = await self._abort_pending_entry(pending, entry_id, final_reason)
                if removed:
                    continue  # Cleanup succeeded → pending removed
                # Cleanup failed → fail_closed, pending retained
                continue

            # --- 4c: Has fills + missing hedge → try to drive hedge ---
            if pending.missing_hedge_quantity() > 1e-9:
                # V1: check if tradeable before hedging (config gated)
                if not getattr(strategy, "pending_entry_force_fallback_when_tradeable", False):
                    # When fallback_when_tradeable is false (default), skip tradeability
                    # check and go straight to abort on hard ceiling
                    if hard_ceiling_reached:
                        await self._abort_pending_entry(pending, entry_id, final_reason)
                        continue

                hedge_driven = await self._recover_drive_missing_hedge(
                    pending, final_reason
                )
                if hedge_driven:
                    # V1: if hedge completes the entry → finalize immediately
                    if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        if await self._finalize_pending_entry(pending, entry_id, now_ms):
                            await self._complete_pending_entry_terminal_removal(
                                entry_id,
                                reason="recovery_hedge_completed_entry",
                                symbol=pending.symbol,
                                now_ms=now_ms,
                            )
                            self.journal.append(
                                "recovery.pending_entry_finalized",
                                {
                                    "entry_id": entry_id,
                                    "symbol": pending.symbol,
                                    "reason": "recovery_hedge_completed_entry",
                                },
                            )
                        else:
                            pending.reconcile_attempt += 1
                            self._apply_reconcile_backoff(pending, now_ms)
                        continue

                    # Hedge submitted but entry not yet complete — keep for reconciliation
                    self.journal.append(
                        "recovery.pending_entry_hedge_driven",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "reason": final_reason,
                            "missing_hedge": pending.missing_hedge_quantity(),
                        },
                    )
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue

                if hard_ceiling_reached:
                    # Hard ceiling with unresolved hedge → abort (with cleanup)
                    await self._abort_pending_entry(pending, entry_id, final_reason)
                    continue

            # --- 4d: Fully filled → finalize ---
            if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                if await self._finalize_pending_entry(pending, entry_id, now_ms):
                    await self._complete_pending_entry_terminal_removal(
                        entry_id,
                        reason="startup_fully_filled_finalized",
                        symbol=pending.symbol,
                        now_ms=now_ms,
                    )
                    self.journal.append(
                        "recovery.pending_entry_finalized",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "reason": final_reason,
                        },
                    )
                else:
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                continue

            # --- 4e: Fallback — still pending and hard ceiling reached → abort (with cleanup) ---
            if hard_ceiling_reached:
                await self._abort_pending_entry(pending, entry_id, final_reason)
                continue

            pending.reconcile_attempt += 1
            self._apply_reconcile_backoff(pending, now_ms)

        # --- Post-recovery lifecycle transition ---
        self._finalize_startup_recovery()

    async def _recover_poll_order_status(self, entry_id: str, pending) -> None:
        """Query each venue for its respective order status.

        V1: queries maker venue with maker_order_id and hedge venue with
        hedge_order_id independently, rather than using a fallback chain
        that shadows the hedge order when a maker order exists.
        """
        # Query maker venue with maker passive order identity.
        order_id, client_order_id = self._pending_entry_maker_order_identifiers(pending)
        if order_id or client_order_id:
            maker_ven = pending.maker_venue()
            maker_adapter = self.get_venue_adapter(maker_ven)
            if maker_adapter is not None and hasattr(
                maker_adapter,
                "query_passive_order_progress",
            ):
                try:
                    progress = await maker_adapter.query_passive_order_progress(
                        symbol=pending.symbol,
                        order_id=order_id,
                        client_order_id=client_order_id or None,
                        side=pending.maker_side(),
                    )
                    if progress is not None:
                        state = getattr(
                            progress,
                            "state",
                            PassiveOrderState.UNKNOWN,
                        ) or PassiveOrderState.UNKNOWN
                        if not isinstance(state, PassiveOrderState):
                            try:
                                state = PassiveOrderState(str(state))
                            except ValueError:
                                state = PassiveOrderState.UNKNOWN
                        passive_order = getattr(pending, "passive_order", None)
                        if passive_order is not None:
                            passive_order.last_progress_state = state
                        filled_qty = float(
                            getattr(progress, "cumulative_quantity", 0.0) or 0.0
                        )
                        if filled_qty > 0:
                            pending.maker_leg_filled = max(
                                pending.maker_leg_filled,
                                filled_qty,
                            )
                            avg_price = float(
                                getattr(progress, "average_price", 0.0) or 0.0
                            )
                            if avg_price > 0:
                                pending.maker_fill_price = avg_price
                        if state == PassiveOrderState.FILLED:
                            pending.uncertain_outcome = False
                            pending.outcome = "filled"
                            self.journal.append(
                                "recovery.maker_order_status_resolved",
                                {
                                    "entry_id": entry_id,
                                    "venue": str(maker_ven),
                                    "status": state.value,
                                },
                            )
                            return
                        if state in {
                            PassiveOrderState.CANCELED,
                            PassiveOrderState.REJECTED,
                            PassiveOrderState.EXPIRED,
                        }:
                            pending.uncertain_outcome = False
                            pending.outcome = state.value
                            self.journal.append(
                                "recovery.maker_order_canceled",
                                {
                                    "entry_id": entry_id,
                                    "venue": str(maker_ven),
                                    "status": state.value,
                                },
                            )
                            return
                except Exception:
                    pass

            if (
                order_id
                and maker_adapter is not None
                and hasattr(maker_adapter, "get_order_status")
            ):
                try:
                    status = await maker_adapter.get_order_status(
                        symbol=pending.symbol,
                        order_id=order_id,
                    )
                    if status and getattr(status, "status", "") == "filled":
                        pending.uncertain_outcome = False
                        pending.outcome = "filled"
                        filled_qty = getattr(status, "filled_quantity", 0.0) or getattr(status, "executed_qty", 0.0)
                        if filled_qty and filled_qty > 0:
                            pending.maker_leg_filled = max(pending.maker_leg_filled, float(filled_qty))
                        self.journal.append(
                            "recovery.maker_order_status_resolved",
                            {"entry_id": entry_id, "venue": str(maker_ven), "status": status.status},
                        )
                        return
                    elif status and getattr(status, "status", "") == "canceled":
                        pending.uncertain_outcome = False
                        pending.outcome = "canceled"
                        self.journal.append(
                            "recovery.maker_order_canceled",
                            {"entry_id": entry_id, "venue": str(maker_ven)},
                        )
                        return
                except Exception:
                    pass

        # Query hedge venue with hedge order (independent of maker query)
        if pending.hedge_order_id:
            hedge_ven = pending.hedge_venue()
            hedge_adapter = self.get_venue_adapter(hedge_ven)
            if hedge_adapter is not None and hasattr(hedge_adapter, "get_order_status"):
                try:
                    status = await hedge_adapter.get_order_status(
                        symbol=pending.symbol,
                        order_id=pending.hedge_order_id,
                    )
                    if status and getattr(status, "status", "") == "filled":
                        pending.uncertain_outcome = False
                        pending.outcome = "filled"
                        filled_qty = getattr(status, "filled_quantity", 0.0) or getattr(status, "executed_qty", 0.0)
                        if filled_qty and filled_qty > 0:
                            pending.hedge_leg_filled = max(pending.hedge_leg_filled, float(filled_qty))
                        self.journal.append(
                            "recovery.hedge_order_status_resolved",
                            {"entry_id": entry_id, "venue": str(hedge_ven), "status": status.status},
                        )
                        return
                    elif status and getattr(status, "status", "") == "canceled":
                        self.journal.append(
                            "recovery.hedge_order_canceled",
                            {"entry_id": entry_id, "venue": str(hedge_ven)},
                        )
                except Exception:
                    pass

    async def _recover_hydrate_from_live_positions(self, pending) -> bool:
        """Try to hydrate pending entry from live exchange positions.

        V1: hydrate_pending_entry_from_live_balanced_exposure() —
        If both venues have position size > 0 (long) and < 0 (short),
        and the balanced quantity exceeds current fill, apply fills.

        V1 skips hydration when inflight_hedge is active (the hedge may
        still fill). We approximate this by checking for an active hedge
        order id with uncertain outcome.
        """
        # V1: skip hydration while a hedge is actively inflight. A restored
        # hedge order id alone is not active-order evidence; live/open-order
        # truth below decides whether it is safe to hydrate.
        if pending.uncertain_outcome and pending.hedge_inflight is not None:
            return False

        try:
            long_adapter = self.get_venue_adapter(pending.long_venue)
            short_adapter = self.get_venue_adapter(pending.short_venue)
            if long_adapter is None or short_adapter is None:
                return False

            long_pos = await long_adapter.fetch_position(pending.symbol)
            short_pos = await short_adapter.fetch_position(pending.symbol)

            # V1: need long position (BUY side, qty > 0) and short (SELL side, qty > 0)
            # V2 transport returns side=SELL with quantity=abs(net) for shorts,
            # so checking quantity >= 0 is wrong — must check side field.
            from lightfee.core.domain import Side
            long_has_position = (
                long_pos.side == Side.BUY and long_pos.quantity > 1e-9
            )
            short_has_position = (
                short_pos.side == Side.SELL and short_pos.quantity > 1e-9
            )
            if not long_has_position or not short_has_position:
                return False

            live_balanced = min(long_pos.quantity, short_pos.quantity)
            current_balanced = min(pending.maker_leg_filled, pending.hedge_leg_filled)
            live_imbalanced = abs(long_pos.quantity - short_pos.quantity) > 1e-9
            local_overstates_live = (
                pending.maker_leg_filled > live_balanced + 1e-9
                or pending.hedge_leg_filled > live_balanced + 1e-9
            )
            if live_imbalanced and local_overstates_live and live_balanced > 1e-9:
                try:
                    long_open_orders = await self._fetch_residual_repair_open_orders(
                        long_adapter,
                        pending.long_venue,
                        pending.symbol,
                    )
                    short_open_orders = await self._fetch_residual_repair_open_orders(
                        short_adapter,
                        pending.short_venue,
                        pending.symbol,
                    )
                except Exception as exc:
                    self.journal.append(
                        "pending_entry.live_position_imbalanced_hydration_blocked",
                        {
                            "entry_id": pending.pending_id,
                            "symbol": pending.symbol,
                            "reason": "open_order_truth_unavailable",
                            "error": str(exc),
                        },
                    )
                    return False
                if long_open_orders or short_open_orders:
                    self.journal.append(
                        "pending_entry.live_position_imbalanced_hydration_blocked",
                        {
                            "entry_id": pending.pending_id,
                            "symbol": pending.symbol,
                            "reason": "open_orders_present",
                            "long_open_order_count": len(long_open_orders),
                            "short_open_order_count": len(short_open_orders),
                        },
                    )
                    return False

                before_maker = float(pending.maker_leg_filled or 0.0)
                before_hedge = float(pending.hedge_leg_filled or 0.0)
                before_target = float(pending.target_quantity or 0.0)
                before_maker_price = float(pending.maker_fill_price or 0.0)
                before_hedge_price = float(pending.hedge_fill_price or 0.0)

                if pending.maker_leg == "short":
                    maker_live_position = short_pos
                    hedge_live_position = long_pos
                    maker_price_source = short_pos.entry_price
                    hedge_price_source = long_pos.entry_price
                else:
                    maker_live_position = long_pos
                    hedge_live_position = short_pos
                    maker_price_source = long_pos.entry_price
                    hedge_price_source = short_pos.entry_price

                pending.maker_leg_filled = live_balanced
                pending.hedge_leg_filled = live_balanced
                pending.target_quantity = live_balanced
                if pending.maker_fill_price <= 0:
                    if maker_price_source > 0:
                        pending.maker_fill_price = float(maker_price_source)
                    elif float(getattr(pending, "maker_price", 0.0) or 0.0) > 0:
                        pending.maker_fill_price = float(pending.maker_price)
                if pending.hedge_fill_price <= 0 and hedge_price_source > 0:
                    pending.hedge_fill_price = float(hedge_price_source)
                pending.uncertain_outcome = False
                pending.outcome = "filled"

                self.journal.append(
                    "pending_entry.live_position_imbalanced_hydrated",
                    {
                        "entry_id": pending.pending_id,
                        "symbol": pending.symbol,
                        "long_venue": pending.long_venue.value,
                        "short_venue": pending.short_venue.value,
                        "maker_leg": pending.maker_leg,
                        "before_maker_leg_filled": before_maker,
                        "before_hedge_leg_filled": before_hedge,
                        "before_target_quantity": before_target,
                        "after_maker_leg_filled": pending.maker_leg_filled,
                        "after_hedge_leg_filled": pending.hedge_leg_filled,
                        "after_target_quantity": pending.target_quantity,
                        "before_maker_fill_price": before_maker_price,
                        "before_hedge_fill_price": before_hedge_price,
                        "after_maker_fill_price": pending.maker_fill_price,
                        "after_hedge_fill_price": pending.hedge_fill_price,
                        "live_balanced_quantity": live_balanced,
                        "live_long_excess_quantity": max(0.0, long_pos.quantity - live_balanced),
                        "live_short_excess_quantity": max(0.0, short_pos.quantity - live_balanced),
                        "live_positions": {
                            "long": self._position_snapshot_evidence(long_pos),
                            "short": self._position_snapshot_evidence(short_pos),
                        },
                        "maker_live_position": self._position_snapshot_evidence(
                            maker_live_position
                        ),
                        "hedge_live_position": self._position_snapshot_evidence(
                            hedge_live_position
                        ),
                        "open_order_truth": "flat",
                        "quantity_source": "live_exchange_position_truth_imbalanced",
                    },
                )
                return True

            if live_balanced <= current_balanced + 1e-9:
                return False

            before_maker = float(pending.maker_leg_filled or 0.0)
            before_hedge = float(pending.hedge_leg_filled or 0.0)
            before_maker_price = float(pending.maker_fill_price or 0.0)
            before_hedge_price = float(pending.hedge_fill_price or 0.0)

            if pending.maker_leg == "short":
                maker_live_position = short_pos
                hedge_live_position = long_pos
                recovered_maker_qty = min(live_balanced, short_pos.quantity)
                recovered_hedge_qty = min(live_balanced, long_pos.quantity)
                maker_price_source = short_pos.entry_price
                hedge_price_source = long_pos.entry_price
            else:
                maker_live_position = long_pos
                hedge_live_position = short_pos
                recovered_maker_qty = min(live_balanced, long_pos.quantity)
                recovered_hedge_qty = min(live_balanced, short_pos.quantity)
                maker_price_source = long_pos.entry_price
                hedge_price_source = short_pos.entry_price

            if recovered_maker_qty > pending.maker_leg_filled + 1e-9:
                pending.maker_leg_filled = recovered_maker_qty
            if recovered_hedge_qty > pending.hedge_leg_filled + 1e-9:
                pending.hedge_leg_filled = recovered_hedge_qty
            if pending.maker_fill_price <= 0 and maker_price_source > 0:
                pending.maker_fill_price = float(maker_price_source)
            if pending.hedge_fill_price <= 0 and hedge_price_source > 0:
                pending.hedge_fill_price = float(hedge_price_source)

            self.journal.append(
                "pending_entry.live_position_hydrated",
                {
                    "entry_id": pending.pending_id,
                    "symbol": pending.symbol,
                    "long_venue": pending.long_venue.value,
                    "short_venue": pending.short_venue.value,
                    "maker_leg": pending.maker_leg,
                    "before_maker_leg_filled": before_maker,
                    "before_hedge_leg_filled": before_hedge,
                    "after_maker_leg_filled": pending.maker_leg_filled,
                    "after_hedge_leg_filled": pending.hedge_leg_filled,
                    "before_maker_fill_price": before_maker_price,
                    "before_hedge_fill_price": before_hedge_price,
                    "after_maker_fill_price": pending.maker_fill_price,
                    "after_hedge_fill_price": pending.hedge_fill_price,
                    "live_balanced_quantity": live_balanced,
                    "live_positions": {
                        "long": self._position_snapshot_evidence(long_pos),
                        "short": self._position_snapshot_evidence(short_pos),
                    },
                    "maker_live_position": self._position_snapshot_evidence(
                        maker_live_position
                    ),
                    "hedge_live_position": self._position_snapshot_evidence(
                        hedge_live_position
                    ),
                    "quantity_source": "live_exchange_position_truth",
                },
            )

            # If both legs now filled, mark as resolved
            if pending.maker_completed() and pending.missing_hedge_quantity() <= 1e-9:
                pending.uncertain_outcome = False
                pending.outcome = "filled"

            return True
        except Exception:
            return False

    def _try_consume_maker_venue_budget(
        self, venue, now_ms: int
    ) -> bool:
        """Check and consume maker venue request budget for a cancel/submit op.

        V1: try_consume_maker_venue_request_budget (entry_sync.rs:2410-2431)
        Uses sliding-window budget: max_ops per window_ms, submit costs 2.
        Returns True if the operation is allowed (budget consumed).

        During recovery, operations are rare (one cancel per stuck entry),
        but the budget prevents accidental tight-loop retries.
        """
        strategy = self.config.strategy
        window_ms = strategy.maker_venue_budget_window_ms
        max_ops = strategy.maker_venue_budget_max_ops
        cost = strategy.maker_venue_submit_cost  # cancel uses submit cost

        venue_key = str(venue) if hasattr(venue, "value") else str(venue)
        frozen_until_ms = int(
            self._maker_venue_request_budget_frozen_until_ms.get(venue_key, 0) or 0
        )
        if frozen_until_ms > 0 and now_ms < frozen_until_ms:
            return False
        history = self._maker_venue_op_history.setdefault(venue_key, [])

        # Prune expired timestamps
        cutoff = now_ms - window_ms
        history[:] = [ts for ts in history if ts > cutoff]

        # V1: check if budget remaining allows this operation
        current_ops = sum(1 for _ in history)
        if current_ops + cost > max_ops:
            return False

        # Consume budget: record this operation
        history.append(now_ms)
        return True

    async def _recover_cancel_maker_order(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """Attempt to cancel the maker order before abort.

        V1: cancel_pending_entry_passive_order (entry_sync.rs:2401-2445) —
        1. Returns false if maker already completed or cancel already requested
        2. Checks make_venue_request_budget (rate-limit gate)
        3. If budget exhausted → sets backoff, returns false
        4. Issues cancel_order on the maker venue adapter
        5. Returns true if cancel was successfully issued
        """
        if pending.maker_completed():
            return False

        maker_venue = pending.maker_venue()
        adapter = self.get_venue_adapter(maker_venue)
        if adapter is None:
            return False

        order_id, client_order_id = self._pending_entry_maker_order_identifiers(pending)
        if not (order_id or client_order_id):
            return False

        if self._pending_entry_maker_cancel_requested(pending):
            return False

        # V1: check maker venue request budget before issuing cancel
        now_ms = wall_clock_now_ms()
        if not self._try_consume_maker_venue_budget(maker_venue, now_ms):
            # Budget exhausted — delay and retry later
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "recovery.maker_cancel_budget_delayed",
                {"entry_id": entry_id, "venue": str(maker_venue),
                 "reason": reason, "next_poll_ms": pending.next_progress_poll_ms},
            )
            return False

        try:
            await adapter.cancel_passive_order(
                symbol=pending.symbol,
                order_id=order_id,
                client_order_id=client_order_id or None,
            )
            self._mark_pending_entry_maker_cancel_requested(pending, now_ms)
            pending.reconcile_next_attempt_ms = (
                now_ms + self._RECONCILE_RETRY_BASE_MS
            )
            self.journal.append(
                "recovery.maker_cancel_requested",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": str(maker_venue),
                    "maker_order_id": order_id,
                    "maker_client_order_id": client_order_id,
                    "reason": reason,
                },
            )
            return True
        except Exception as e:
            if not pending.has_any_fill():
                self.journal.append(
                    "recovery.maker_cancel_failed_assumed_terminal",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "error": str(e), "action": "abort_without_fail_closed"},
                )
                return False
            from lightfee.engine.lifecycle import enter_fail_closed
            enter_fail_closed(self.state)
            self.state.last_error = (
                f"pending_entry_lifetime_cancel_failed:{entry_id}: {e}"
            )
            self.journal.append(
                "recovery.maker_cancel_failed_fail_closed",
                {"entry_id": entry_id, "symbol": pending.symbol,
                 "error": str(e), "reason": reason},
            )
            return False

    async def _recover_try_taker_fallback(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: try_terminal_taker_fallback() — taker order for zero-fill entries.

        Requires MarketView for tradeability check. Not available during
        recovery without a snapshot. Returns False → caller proceeds to abort.
        """
        return False

    async def _recover_drive_missing_hedge(self, pending, reason: str) -> bool:
        """Submit a hedge order for the missing quantity.

        V1: hedge_pending_entry_delta() —
        1. Normalize quantity via adapter.normalize_quantity (exchange lot size)
        2. Use maker fill price as hedge price hint (better than pure market)
        3. Submit order; gate only on FAIL_CLOSED (recovery lifecycle is RECONCILING)
        """
        hedge_venue = pending.hedge_venue()
        adapter = self.get_venue_adapter(hedge_venue)
        if adapter is None:
            return False

        missing = pending.missing_hedge_quantity()
        if missing <= 1e-9:
            return False

        if self.state.risk_mode == GlobalRiskMode.FAIL_CLOSED:
            self._try_journal(
                "recovery.hedge_blocked_fail_closed",
                {"entry_id": pending.pending_id, "reason": reason},
            )
            return False
        if pending.hedge_inflight is not None:
            return False

        try:
            from lightfee.core.domain import OrderRequest

            now_ms = wall_clock_now_ms()
            hedge_price = self._pending_entry_hedge_price_hint(pending)
            hedgeability_plan = self._pending_entry_hedgeability_plan(
                pending,
                hedge_venue,
                missing,
                hedge_price,
            )
            if hedgeability_plan.aligned_target_quantity <= 1e-9:
                early_decision = decide_pending_entry_hedge_delta_pre_submit(
                    pending,
                    strategy=self.config.strategy,
                    hedgeability_plan=hedgeability_plan,
                    normalized_quantity=None,
                    min_notional_violation=None,
                    now_ms=now_ms,
                    maker_progress_updated=False,
                )
                if early_decision.kind in {"buffer_small_fill", "keep_pending"}:
                    self._append_pending_entry_hedge_decision_event(early_decision)
                    return False
            normalized, min_notional_violation, quantity_evidence = (
                await self._normalize_pending_entry_hedge_quantity(
                    pending=pending,
                    hedge_venue=hedge_venue,
                    adapter=adapter,
                    missing=missing,
                    hedge_price=hedge_price,
                    hedgeability_plan=hedgeability_plan,
                )
            )
            decision = decide_pending_entry_hedge_delta_pre_submit(
                pending,
                strategy=self.config.strategy,
                hedgeability_plan=hedgeability_plan,
                normalized_quantity=normalized,
                min_notional_violation=min_notional_violation,
                now_ms=now_ms,
                maker_progress_updated=False,
            )
            self._append_pending_entry_hedge_decision_event(decision)
            if decision.kind in {
                "buffer_small_fill",
                "wait_min_notional_accumulation",
                "wait_passive_small_fill_buffer",
                "keep_pending",
            }:
                return False
            if decision.kind == "abort_and_flatten":
                await self._abort_pending_entry(
                    pending,
                    pending.pending_id,
                    "entry hedge leg below minimum notional",
                )
                return False
            normalized = decision.normalized_quantity
            self._append_pending_entry_hedge_quantity_undercut(
                entry_id=pending.pending_id,
                pending=pending,
                hedge_venue=hedge_venue,
                normalized_quantity=normalized,
                quantity_evidence=quantity_evidence,
            )

            if normalized <= 1e-9:
                self.journal.append(
                    "recovery.hedge_quantity_below_min_notional",
                    {"entry_id": pending.pending_id, "symbol": pending.symbol,
                     "raw_quantity": missing, "normalized_quantity": normalized},
                )
                return False

            from lightfee.venues.cid import generate_exchange_cid
            recovery_cid = generate_exchange_cid(pending.pending_id, "h", hedge_venue)
            pending.hedge_client_order_id = recovery_cid
            pending.hedge_attempt_count = int(
                getattr(pending, "hedge_attempt_count", 0) or 0
            ) + 1
            self._pending_entry_hedge_deadline_started(
                pending,
                submitted_at_ms=now_ms,
                normalized_quantity=normalized,
                hedge_price=hedge_price,
                hedge_attempt=pending.hedge_attempt_count,
                hedge_venue=hedge_venue,
            )
            pending.hedge_inflight = HedgeInflight(
                client_order_id=recovery_cid,
                venue=hedge_venue,
                side=pending.hedge_side(),
                quantity=normalized,
                attempt=pending.hedge_attempt_count,
                submitted_at_ms=now_ms,
            )

            req = OrderRequest(
                venue=hedge_venue,
                symbol=pending.symbol,
                side=pending.hedge_side(),
                quantity=normalized,
                price=hedge_price,
                post_only=False,
                time_in_force=TimeInForce.IOC,
                reduce_only=False,
                client_order_id=recovery_cid,
            )
            fill = await adapter.place_order(req)
            if fill.quantity > 0:
                pending.hedge_leg_filled += fill.quantity
                pending.consume_hedge_quantity_fifo(fill.quantity)
                pending.hedge_order_id = fill.order_id
                pending.hedge_fill_price = fill.price
                pending.hedge_inflight = None
                note_pending_entry_hedge_filled(pending)
                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True
            pending.hedge_inflight = None
            note_pending_entry_hedge_filled(pending)
            return False
        except OrderSubmitError as e:
            hedge_client_order_id = (
                pending.hedge_inflight.client_order_id
                if pending.hedge_inflight
                else pending.hedge_client_order_id
            )
            reconciliation_error_text = ""
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    pending.symbol,
                    "",
                    hedge_client_order_id,
                )
            except Exception as reconcile_error:
                reconciliation = None
                reconciliation_error_text = str(reconcile_error)
            reconciliation_quantity = (
                float(getattr(reconciliation, "quantity", 0.0) or 0.0)
                if reconciliation is not None
                else 0.0
            )
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=hedge_venue,
                symbol=pending.symbol,
                order_id="",
                client_order_id=hedge_client_order_id,
                target_qty=normalized,
                reconciliation=reconciliation,
                metadata=(
                    getattr(reconciliation, "metadata", None)
                    if reconciliation is not None
                    else None
                ),
            )
            reconciliation_quantity = truth_decision.reconciled_qty
            if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
                fill_qty = reconciliation_quantity
                pending.hedge_leg_filled += fill_qty
                pending.consume_hedge_quantity_fifo(fill_qty)
                pending.hedge_order_id = getattr(reconciliation, "order_id", "") or ""
                pending.hedge_fill_price = float(
                    getattr(reconciliation, "average_price", 0.0)
                    or getattr(reconciliation, "price", 0.0)
                    or pending.hedge_fill_price
                    or 0.0
                )
                pending.hedge_inflight = None
                note_pending_entry_hedge_filled(pending)
                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True
            if e.is_rejected:
                pending.hedge_inflight = None
                phase_state = ensure_pending_entry_phase_state(pending)
                phase_state.hedge_deadline_at_ms = None
            self.journal.append(
                "recovery.hedge_submit_error",
                {
                    "entry_id": pending.pending_id,
                    "symbol": pending.symbol,
                    "error": str(e),
                    "reason": reason,
                    "is_rejected": e.is_rejected,
                    "hedge_client_order_id": hedge_client_order_id,
                    "fill_reconciliation_attempted": True,
                    "fill_reconciliation_result": (
                        "error" if reconciliation_error_text else "missing_or_zero_fill"
                    ),
                    "fill_reconciliation_error": reconciliation_error_text,
                    "fill_reconciliation_quantity": reconciliation_quantity,
                    "order_truth_fill_status": truth_decision.fill_status.value,
                    "order_truth_evidence_status": truth_decision.evidence_status.value,
                    "order_truth_decision": truth_decision.decision,
                    "order_truth_missing_evidence": list(
                        truth_decision.missing_evidence
                    ),
                },
            )
            return False
        except Exception as e:
            self.journal.append(
                "recovery.hedge_submit_error",
                {"entry_id": pending.pending_id, "symbol": pending.symbol,
                 "error": str(e), "reason": reason},
            )
            return False

    async def _drive_missing_hedge_live(self, pending, entry_id: str, now_ms: int) -> bool:
        """Submit a hedge IOC/taker order for the missing quantity during normal tick.

        V1: hedge_pending_entry_delta() — called from the normal live tick after
        reconciliation detects a maker fill but the hedge leg is still missing.

        Idempotency: sets pending.hedge_inflight to the client_order_id before
        submitting; skips if already inflight.  On success updates
        hedge_leg_filled and clears inflight.

        Unlike _recover_drive_missing_hedge(), this does NOT gate on FAIL_CLOSED
        — normal ticks always attempt to complete the entry.
        """
        hedge_venue = pending.hedge_venue()
        adapter = self.get_venue_adapter(hedge_venue)
        if adapter is None:
            return False

        missing = pending.missing_hedge_quantity()
        if missing <= 1e-9:
            return False

        # Idempotency: skip if a hedge is already inflight
        if pending.hedge_inflight is not None:
            # Do not retry while inflight; reconciliation will clear it
            # after order/fills/position prove no hedge exists.
            return False

        # Terminal: do not drive hedge from a residual repair state
        if pending.repair_state:
            return False

        try:
            from lightfee.core.domain import OrderRequest

            hedge_price = self._pending_entry_hedge_price_hint(pending)
            hedgeability_plan = self._pending_entry_hedgeability_plan(
                pending,
                hedge_venue,
                missing,
                hedge_price,
            )
            if hedgeability_plan.aligned_target_quantity <= 1e-9:
                early_decision = decide_pending_entry_hedge_delta_pre_submit(
                    pending,
                    strategy=self.config.strategy,
                    hedgeability_plan=hedgeability_plan,
                    normalized_quantity=None,
                    min_notional_violation=None,
                    now_ms=now_ms,
                    maker_progress_updated=True,
                )
                if early_decision.kind in {"buffer_small_fill", "keep_pending"}:
                    self._append_pending_entry_hedge_decision_event(early_decision)
                    return False

            normalized, min_notional_violation, quantity_evidence = (
                await self._normalize_pending_entry_hedge_quantity(
                    pending=pending,
                    hedge_venue=hedge_venue,
                    adapter=adapter,
                    missing=missing,
                    hedge_price=hedge_price,
                    hedgeability_plan=hedgeability_plan,
                )
            )
            decision = decide_pending_entry_hedge_delta_pre_submit(
                pending,
                strategy=self.config.strategy,
                hedgeability_plan=hedgeability_plan,
                normalized_quantity=normalized,
                min_notional_violation=min_notional_violation,
                now_ms=now_ms,
                maker_progress_updated=True,
            )
            self._append_pending_entry_hedge_decision_event(decision)
            if decision.kind in {
                "buffer_small_fill",
                "wait_min_notional_accumulation",
                "wait_passive_small_fill_buffer",
                "keep_pending",
            }:
                return False
            if decision.kind == "abort_and_flatten":
                await self._abort_pending_entry(
                    pending,
                    entry_id,
                    "entry hedge leg below minimum notional",
                )
                return False
            normalized = decision.normalized_quantity
            self._append_pending_entry_hedge_quantity_undercut(
                entry_id=entry_id,
                pending=pending,
                hedge_venue=hedge_venue,
                normalized_quantity=normalized,
                quantity_evidence=quantity_evidence,
            )

            if normalized <= 1e-9:
                self.journal.append(
                    "pending_entry.hedge_quantity_below_min_notional",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "raw_quantity": missing, "normalized_quantity": normalized},
                )
                return False

            from lightfee.venues.cid import generate_exchange_cid
            attempt = int(getattr(pending, "hedge_attempt_count", 0) or 0) + 1
            pending.hedge_attempt_count = attempt
            hedge_cloid = generate_exchange_cid(entry_id, f"h{attempt}", hedge_venue)
            pending.hedge_client_order_id = hedge_cloid
            self._pending_entry_hedge_deadline_started(
                pending,
                submitted_at_ms=now_ms,
                normalized_quantity=normalized,
                hedge_price=hedge_price,
                hedge_attempt=attempt,
                hedge_venue=hedge_venue,
            )
            pending.hedge_inflight = HedgeInflight(
                client_order_id=hedge_cloid,
                venue=hedge_venue,
                side=pending.hedge_side(),
                quantity=normalized,
                attempt=attempt,
                submitted_at_ms=now_ms,
            )

            self.journal.append(
                "pending_entry.hedge_submit_attempt",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "hedge_venue": hedge_venue.value,
                    "hedge_side": pending.hedge_side().value,
                    "hedge_quantity": normalized,
                    "hedge_price_hint": hedge_price,
                    "hedge_client_order_id": hedge_cloid,
                    "hedge_attempt": attempt,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                },
            )

            req = OrderRequest(
                venue=hedge_venue,
                symbol=pending.symbol,
                side=pending.hedge_side(),
                quantity=normalized,
                price=hedge_price,
                post_only=False,
                time_in_force=TimeInForce.IOC,
                reduce_only=False,
                client_order_id=hedge_cloid,
            )
            fill = await adapter.place_order(req)

            self._flush_adapter_order_diagnostics(adapter)

            if fill.quantity > 0:
                pending.hedge_leg_filled += fill.quantity
                pending.consume_hedge_quantity_fifo(fill.quantity)
                pending.hedge_order_id = fill.order_id
                pending.hedge_fill_price = fill.price
                pending.hedge_inflight = None
                note_pending_entry_hedge_filled(pending)

                self.journal.append(
                    "pending_entry.hedge_submit_result",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "outcome": "filled",
                        "hedge_fill_quantity": fill.quantity,
                        "hedge_fill_price": fill.price,
                        "hedge_order_id": fill.order_id,
                        "hedge_client_order_id": hedge_cloid,
                        "hedge_attempt": attempt,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "missing_hedge_remaining": pending.missing_hedge_quantity(),
                    },
                )

                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True

            # Zero fill — hedge order was placed but didn't fill (IOC/taker)
            pending.hedge_inflight = None
            note_pending_entry_hedge_filled(pending)
            self.journal.append(
                "pending_entry.hedge_submit_result",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "outcome": "zero_fill",
                    "hedge_client_order_id": hedge_cloid,
                    "hedge_attempt": attempt,
                    "order_id": getattr(fill, "order_id", ""),
                },
            )
            return False

        except OrderSubmitError as e:
            # V1: retain inflight on UNCERTAIN so reconciliation can query it;
            # only clear on REJECTED where we know the order never reached the exchange.
            submitted_inflight = pending.hedge_inflight
            reconciliation_error_text = ""
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    pending.symbol,
                    "",
                    hedge_cloid,
                )
            except Exception as reconcile_error:
                reconciliation = None
                reconciliation_error_text = str(reconcile_error)
                self.journal.append(
                    "pending_entry.hedge_submit_reconcile_error",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "hedge_client_order_id": hedge_cloid,
                        "error": str(reconcile_error),
                    },
                )
            reconciliation_quantity = (
                float(getattr(reconciliation, "quantity", 0.0) or 0.0)
                if reconciliation is not None
                else 0.0
            )
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=hedge_venue,
                symbol=pending.symbol,
                order_id="",
                client_order_id=hedge_cloid,
                target_qty=normalized,
                reconciliation=reconciliation,
                metadata=(
                    getattr(reconciliation, "metadata", None)
                    if reconciliation is not None
                    else None
                ),
            )
            reconciliation_quantity = truth_decision.reconciled_qty
            if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
                fill_qty = reconciliation_quantity
                pending.hedge_leg_filled += fill_qty
                pending.consume_hedge_quantity_fifo(fill_qty)
                pending.hedge_order_id = getattr(reconciliation, "order_id", "") or ""
                pending.hedge_fill_price = float(
                    getattr(reconciliation, "average_price", 0.0)
                    or getattr(reconciliation, "price", 0.0)
                    or pending.hedge_fill_price
                    or 0.0
                )
                pending.hedge_inflight = None
                note_pending_entry_hedge_filled(pending)
                self._flush_adapter_order_diagnostics(adapter)
                self.journal.append(
                    "pending_entry.hedge_submit_result",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "outcome": "filled",
                        "reconciled": True,
                        "hedge_fill_quantity": fill_qty,
                        "hedge_fill_price": pending.hedge_fill_price,
                        "hedge_order_id": pending.hedge_order_id,
                        "hedge_client_order_id": hedge_cloid,
                        "hedge_attempt": attempt,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "missing_hedge_remaining": pending.missing_hedge_quantity(),
                    },
                )
                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True
            if (
                hedge_venue == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                pending.hedge_inflight = None
                pending.repair_state = "non_retryable_auth_signing_failure"
                pending.uncertain_outcome = True
                enter_fail_closed(self.state)
                self.state.last_error = (
                    f"non_retryable_hyperliquid_auth_signing_failure:{entry_id}"
                )
                self._flush_adapter_order_diagnostics(adapter)
                self.journal.append(
                    "pending_entry.hedge_non_retryable_auth_signing_failure",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "hedge_venue": hedge_venue.value,
                        "hedge_client_order_id": (
                            submitted_inflight.client_order_id
                            if submitted_inflight
                            else hedge_cloid
                        ),
                        "hedge_attempt": attempt,
                        "error": str(e),
                        "reason": "non_retryable_auth_signing_failure",
                    },
                )
                return False
            if e.is_rejected:
                pending.hedge_inflight = None
            self._flush_adapter_order_diagnostics(adapter)
            if e.is_rejected and await self._handle_pending_hedge_admission_reject(
                pending=pending,
                entry_id=entry_id,
                hedge_venue=hedge_venue,
                error_text=str(e),
                hedge_client_order_id=(
                    submitted_inflight.client_order_id if submitted_inflight else hedge_cloid
                ),
                hedge_attempt=attempt,
                now_ms=now_ms,
            ):
                return False
            error_payload = {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "outcome": "error",
                "error": str(e),
                "is_rejected": e.is_rejected,
                "hedge_client_order_id": submitted_inflight.client_order_id if submitted_inflight else "",
                "hedge_attempt": attempt,
                "fill_reconciliation_attempted": True,
                "fill_reconciliation_result": (
                    "error" if reconciliation_error_text else "missing_or_zero_fill"
                ),
                "fill_reconciliation_error": reconciliation_error_text,
                "fill_reconciliation_order_id": (
                    getattr(reconciliation, "order_id", "") if reconciliation is not None else ""
                ),
                "fill_reconciliation_client_order_id": hedge_cloid,
                "fill_reconciliation_quantity": reconciliation_quantity,
                "order_truth_fill_status": truth_decision.fill_status.value,
                "order_truth_evidence_status": truth_decision.evidence_status.value,
                "order_truth_decision": truth_decision.decision,
                "order_truth_missing_evidence": list(
                    truth_decision.missing_evidence
                ),
            }
            error_payload.update(
                self._order_submit_error_runtime_evidence(
                    e,
                    venue=hedge_venue,
                    operation="place_order",
                    request=req,
                    default_client_order_id=(
                        submitted_inflight.client_order_id
                        if submitted_inflight
                        else hedge_cloid
                    ),
                )
            )
            self.journal.append(
                "pending_entry.hedge_submit_result",
                error_payload,
            )
            return False
        except Exception as e:
            pending.hedge_inflight = None
            self._flush_adapter_order_diagnostics(adapter)
            self.journal.append(
                "pending_entry.hedge_submit_result",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "outcome": "error",
                    "error": str(e),
                },
            )
            return False

    async def _ensure_pending_entry_open_fill_details(
        self,
        pending,
        entry_id: str,
        now_ms: int,
    ) -> bool:
        """Gate entry.opened on confirmed price and order id for both legs."""
        reconciliation_by_leg: dict[str, Any] = {}
        reconciliation_attempted: set[str] = set()

        async def _reconcile_leg(label: str, venue: Venue) -> None:
            adapter = self.get_venue_adapter(venue)
            if adapter is None:
                return
            if label == "maker":
                order_id, client_order_id = (
                    self._pending_entry_maker_order_identifiers(pending)
                )
            else:
                order_id = getattr(pending, f"{label}_order_id", "") or ""
                client_order_id = getattr(pending, f"{label}_client_order_id", "") or ""
            if not order_id and not client_order_id:
                return
            reconciliation_attempted.add(label)
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    pending.symbol,
                    order_id,
                    client_order_id,
                )
            except Exception as exc:
                self.journal.append(
                    "pending_entry.finalize_fill_reconciliation_error",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "error": str(exc),
                    },
                )
                return
            if reconciliation is None:
                return
            reconciliation_by_leg[label] = reconciliation
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=venue,
                symbol=pending.symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                target_qty=float(getattr(reconciliation, "quantity", 0.0) or 0.0),
                reconciliation=reconciliation,
                metadata=getattr(reconciliation, "metadata", None),
            )
            if truth_decision.fill_status != OrderTruthFillStatus.CONFIRMED_FILL:
                self.journal.append(
                    "pending_entry.finalize_fill_truth_gap",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "order_truth_fill_status": truth_decision.fill_status.value,
                        "order_truth_evidence_status": (
                            truth_decision.evidence_status.value
                        ),
                        "order_truth_decision": truth_decision.decision,
                        "order_truth_missing_evidence": list(
                            truth_decision.missing_evidence
                        ),
                        "reconciliation_metadata": getattr(
                            reconciliation, "metadata", None
                        ),
                    },
                )
                return
            qty = truth_decision.reconciled_qty
            avg_price = float(
                getattr(reconciliation, "average_price", 0.0)
                or getattr(reconciliation, "price", 0.0)
                or 0.0
            )
            reconciled_order_id = getattr(reconciliation, "order_id", "") or order_id
            before_qty = float(getattr(pending, f"{label}_leg_filled", 0.0) or 0.0)
            before_price = float(getattr(pending, f"{label}_fill_price", 0.0) or 0.0)
            before_order_id = getattr(pending, f"{label}_order_id", "") or ""
            if math.isfinite(qty) and (qty > 0.0 or before_qty <= 0.0):
                setattr(pending, f"{label}_leg_filled", qty)
            elif math.isfinite(qty) and qty <= 0.0 and before_qty > 0.0:
                self.journal.append(
                    "pending_entry.finalize_fill_reconciliation_ignored_stale_zero",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "before_quantity": before_qty,
                        "reconciliation_quantity": qty,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "reconciliation_order_id": reconciled_order_id,
                        "metadata": getattr(reconciliation, "metadata", None),
                    },
                )
            if avg_price > 0:
                setattr(pending, f"{label}_fill_price", avg_price)
            if reconciled_order_id:
                setattr(pending, f"{label}_order_id", reconciled_order_id)
            after_qty = float(getattr(pending, f"{label}_leg_filled", 0.0) or 0.0)
            after_price = float(getattr(pending, f"{label}_fill_price", 0.0) or 0.0)
            after_order_id = getattr(pending, f"{label}_order_id", "") or ""
            if (
                abs(after_qty - before_qty) > 1e-12
                or abs(after_price - before_price) > 1e-12
                or after_order_id != before_order_id
            ):
                self.journal.append(
                    "pending_entry.finalize_fill_reconciled",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "before_quantity": before_qty,
                        "after_quantity": after_qty,
                        "before_price": before_price,
                        "after_price": after_price,
                        "before_order_id": before_order_id,
                        "after_order_id": after_order_id,
                    },
                )

        if (
            float(getattr(pending, "maker_leg_filled", 0.0) or 0.0) > 0.0
            or self._pending_entry_has_maker_order_reference(pending)
        ):
            await _reconcile_leg("maker", pending.maker_venue())
        if (
            float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0) > 0.0
            or getattr(pending, "hedge_order_id", "")
            or getattr(pending, "hedge_inflight", None) is not None
            or int(getattr(pending, "hedge_attempt_count", 0) or 0) > 0
        ):
            await _reconcile_leg("hedge", pending.hedge_venue())

        balanced_quantity = min(
            float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
            float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0),
        )
        if balanced_quantity <= 0.0:
            maker_reconciliation = reconciliation_by_leg.get("maker")
            if (
                self._pending_entry_has_maker_order_reference(pending)
                and "maker" in reconciliation_attempted
                and not self._pending_entry_has_terminal_maker_zero_fill_evidence(
                    pending,
                    maker_reconciliation,
                )
            ):
                open_order_truth = await self._pending_entry_zero_fill_has_live_maker_open_order(
                    pending,
                    entry_id,
                    now_ms,
                )
                live_position_truth = await self._pending_entry_zero_fill_has_live_maker_position(
                    pending,
                    entry_id,
                    now_ms,
                )
                if (
                    open_order_truth.available
                    and live_position_truth.available
                    and not open_order_truth.has_live_open_order
                    and live_position_truth.has_live_position
                ):
                    return True

                pending.uncertain_outcome = True
                pending.reconcile_next_attempt_ms = max(
                    int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                    now_ms + 1_000,
                )
                metadata = (
                    getattr(maker_reconciliation, "metadata", None)
                    if maker_reconciliation is not None
                    else None
                )
                self.journal.append(
                    "pending_entry.finalize_deferred_unresolved_maker_zero_fill",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "maker_order_id": pending.maker_order_id,
                        "maker_client_order_id": pending.maker_client_order_id,
                        "reconciliation_found": maker_reconciliation is not None,
                        "reconciliation_metadata": metadata,
                        "reason": "maker_zero_fill_without_terminal_no_fill_evidence",
                    },
                )
                return False
            return True

        missing: list[str] = []
        if float(getattr(pending, "maker_fill_price", 0.0) or 0.0) <= 0.0:
            missing.append("maker_fill_price")
        if float(getattr(pending, "hedge_fill_price", 0.0) or 0.0) <= 0.0:
            missing.append("hedge_fill_price")

        if not missing:
            return True

        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 1_000,
        )
        self.journal.append(
            "pending_entry.finalize_deferred_incomplete_fill",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "missing_fields": missing,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "maker_fill_price": pending.maker_fill_price,
                "hedge_fill_price": pending.hedge_fill_price,
                "maker_order_id": pending.maker_order_id,
                "hedge_order_id": pending.hedge_order_id,
            },
        )
        return False

    async def _pending_entry_zero_fill_has_live_maker_open_order(self, *args, **kwargs):
        return await self.pending_entry_runtime._pending_entry_zero_fill_has_live_maker_open_order(*args, **kwargs)

    async def _pending_entry_zero_fill_has_live_maker_position(self, *args, **kwargs):
        return await self.pending_entry_runtime._pending_entry_zero_fill_has_live_maker_position(*args, **kwargs)

    async def _pending_entry_positive_fill_live_truth(self, *args, **kwargs):
        return await self.pending_entry_runtime._pending_entry_positive_fill_live_truth(*args, **kwargs)

    async def _finalize_pending_entry(self, *args, **kwargs):
        return await self.pending_entry_runtime._finalize_pending_entry(*args, **kwargs)

    def _queue_pending_residual_repair(
        self,
        residual_task,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        """Persist a residual repair task using the V1 runtime field contract."""
        from lightfee.engine.close_executor import _residual_task_to_dict

        task_dict = _residual_task_to_dict(residual_task)
        self.state.pending_residual_repairs = [
            task for task in self.state.pending_residual_repairs
            if not (
                isinstance(task, dict)
                and task.get("position_id") == task_dict["position_id"]
                and task.get("pair_id") == task_dict["pair_id"]
                and task.get("origin") == task_dict["origin"]
                and (task.get("repair_venue") or task.get("exposure_venue")) == task_dict["repair_venue"]
                and (task.get("repair_side") or task.get("exposure_side")) == task_dict["repair_side"]
            )
        ]
        self.state.pending_residual_repairs.append(task_dict)
        payload = dict(task_dict)
        payload["reason"] = reason
        if evidence:
            payload.update(evidence)
        self.journal.append("execution.residual_repair_queued", payload)

    def _finalize_startup_recovery(self) -> None:
        """Transition lifecycle after startup recovery per V1 semantics.

        V1: finalize_startup_position_recovery() lifecycle transitions:
        - No open positions, no pending entries, no pending work → RUNNING
        - Has pending entries but no open positions → RISK_ONLY with blocked reason
        - Has open positions → RUNNING (normal, positions are managed)
        """
        from lightfee.engine.lifecycle import enter_fail_closed

        has_opens = len(self.state.open_positions) > 0
        has_pending = len(self.state.pending_entries) > 0
        has_pending_closes = len(self.state.pending_closes) > 0
        has_passive_closes = len(self.state.pending_passive_closes) > 0
        has_residual_repairs = bool(
            getattr(self.state, "pending_residual_repairs", []) or []
        )
        core_decision = V1RecoveryDecisionCore().decide(
            RecoveryEvidenceSnapshot(
                local_open_positions=tuple(
                    self._recovery_state_collection("open_positions")
                ),
                pending_entries=tuple(
                    self._recovery_state_collection("pending_entries")
                ),
                residual_repairs=tuple(
                    self._recovery_state_collection("pending_residual_repairs")
                ),
                passive_closes=tuple(
                    self._recovery_state_collection("pending_passive_closes")
                ),
                exchange_truth=None,
                prior_recovery_block_reason=self.state.recovery_blocked_reason,
                operator_fail_closed=(
                    self.state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED
                ),
            )
        )
        self.recovery_decision = core_decision

        if (
            not has_opens
            and not has_pending
            and not has_pending_closes
            and not has_passive_closes
            and not has_residual_repairs
        ):
            # All clear — transition to RUNNING
            from lightfee.engine.lifecycle import clear_risk_mode_for_recovery
            if clear_risk_mode_for_recovery(self.state, core_decision):
                self.state.last_error = None
                self._try_journal("runtime.running",
                    {
                        "reason": "startup_recovery_completed",
                        "decision": core_decision.kind.value,
                        "management_action": core_decision.management_action.value,
                        "ts_ms": wall_clock_now_ms(),
                    })
            elif core_decision.block_reason:
                self.state.recovery_blocked_reason = core_decision.block_reason
                self.state.recovery_blocked_at_ms = wall_clock_now_ms()
                self._try_journal("recovery.blocked", {
                    "reason": core_decision.block_reason,
                    "decision": core_decision.kind.value,
                    "management_action": core_decision.management_action.value,
                    "ts_ms": wall_clock_now_ms(),
                })
            return

        if has_opens:
            # Has open positions — normal operation
            max_positions = self.config.strategy.max_concurrent_positions
            if len(self.state.open_positions) > max_positions:
                enter_fail_closed(self.state)
                self.state.last_error = "open_positions_exceed_configured_max"
                self._try_journal("recovery.blocked", {
                    "reason": "open_positions_exceed_configured_max",
                    "open_positions": len(self.state.open_positions),
                    "max": max_positions,
                })
            else:
                from lightfee.engine.lifecycle import clear_risk_mode_for_recovery
                if clear_risk_mode_for_recovery(self.state, core_decision):
                    self.state.last_error = None
                    self._try_journal("runtime.running", {
                        "reason": "startup_recovery_completed_with_positions",
                        "decision": core_decision.kind.value,
                        "management_action": core_decision.management_action.value,
                        "open_positions": len(self.state.open_positions),
                        "ts_ms": wall_clock_now_ms(),
                    })
                elif core_decision.block_reason:
                    self.state.recovery_blocked_reason = (
                        core_decision.block_reason
                    )
                    self.state.recovery_blocked_at_ms = wall_clock_now_ms()
                    self.state.last_error = (
                        f"startup recovery blocked: {core_decision.block_reason}"
                    )
                    set_lifecycle(self.state, EngineLifecycle.RISK_ONLY)
                    self._try_journal("recovery.blocked", {
                        "reason": core_decision.block_reason,
                        "decision": core_decision.kind.value,
                        "management_action": (
                            core_decision.management_action.value
                        ),
                        "open_positions": len(self.state.open_positions),
                        "ts_ms": wall_clock_now_ms(),
                    })
            return

        # No open positions but has pending work → RISK_ONLY
        if has_pending or has_pending_closes or has_passive_closes or has_residual_repairs:
            blocked_reason = (
                core_decision.block_reason
                or "truth_unavailable_for_required_recovery"
            )
            self.state.recovery_blocked_reason = blocked_reason
            self.state.recovery_blocked_at_ms = wall_clock_now_ms()
            self.state.last_error = (
                f"startup recovery blocked: pending_entries={len(self.state.pending_entries)}, "
                f"pending_closes={len(self.state.pending_closes)}, "
                f"pending_passive_closes={len(self.state.pending_passive_closes)}, "
                f"pending_residual_repairs={len(getattr(self.state, 'pending_residual_repairs', []) or [])}"
            )
            set_lifecycle(self.state, EngineLifecycle.RISK_ONLY)
            self._try_journal("recovery.blocked", {
                "reason": blocked_reason,
                "decision": core_decision.kind.value,
                "management_action": core_decision.management_action.value,
                "pending_entries": list(self.state.pending_entries.keys()),
                "ts_ms": wall_clock_now_ms(),
            })

    def _try_journal(self, kind: str, payload: dict) -> None:
        """Append to journal if open; temporarily open if not (for recovery diagnostics)."""
        try:
            self.journal.append(kind, payload)
        except RuntimeError:
            # Journal not open — temporarily open for this event
            try:
                self.journal.open()
                self.journal.append(kind, payload)
            except Exception:
                pass
            finally:
                try:
                    self.journal.close()
                except Exception:
                    pass

    async def _recover_residual_repairs(self, now_ms: int) -> None:
        return await self.residual_repair_runtime.recover_residual_repairs(now_ms)

    @staticmethod
    def _residual_repair_client_order_id(
        position_id: str,
        duplicate_attempt: int,
    ) -> str:
        return ResidualRepairRuntime._residual_repair_client_order_id(
            position_id,
            duplicate_attempt,
        )

    def _pending_residual_repair_fields(self, task: dict) -> tuple[Venue, Side, float] | None:
        return self.residual_repair_runtime._pending_residual_repair_fields(task)

    def _signed_position_size(self, position: PositionSnapshot | None) -> float:
        return self.residual_repair_runtime._signed_position_size(position)

    @staticmethod
    def _position_snapshot_evidence(position: PositionSnapshot | None) -> dict[str, Any]:
        return ResidualRepairRuntime._position_snapshot_evidence(position)

    def _live_positions_evidence(
        self,
        live_positions: dict[Venue, PositionSnapshot | None],
    ) -> dict[str, dict[str, Any]]:
        return self.residual_repair_runtime._live_positions_evidence(live_positions)

    def _venue_symbol_metadata_evidence(self, venue: Venue, symbol: str) -> dict[str, Any]:
        return self.residual_repair_runtime._venue_symbol_metadata_evidence(venue, symbol)

    def _order_submit_error_runtime_evidence(
        self,
        error: OrderSubmitError,
        *,
        venue: Venue | None = None,
        operation: str = "",
        request: Any = None,
        default_client_order_id: str = "",
    ) -> dict[str, Any]:
        return self.residual_repair_runtime._order_submit_error_runtime_evidence(
            error,
            venue=venue,
            operation=operation,
            request=request,
            default_client_order_id=default_client_order_id,
        )

    @staticmethod
    def _order_truth_probe_paths(venue: Venue | None) -> dict[str, str]:
        return ResidualRepairRuntime._order_truth_probe_paths(venue)

    async def _resolve_residual_repair_accepted_order(
        self,
        *,
        task: dict,
        adapter: VenueAdapter,
        repair_venue: Venue,
        repair_side: Side,
        symbol: str,
        baseline: float,
        probe_venues: list[Venue],
        accepted_order_id: str,
        accepted_client_order_id: str,
        now_ms: int,
    ) -> tuple[str, OrderFill | None, dict[str, Any]]:
        return await self.residual_repair_runtime._resolve_residual_repair_accepted_order(
            task=task,
            adapter=adapter,
            repair_venue=repair_venue,
            repair_side=repair_side,
            symbol=symbol,
            baseline=baseline,
            probe_venues=probe_venues,
            accepted_order_id=accepted_order_id,
            accepted_client_order_id=accepted_client_order_id,
            now_ms=now_ms,
        )

    def _retain_residual_repair_accepted_order_gap(
        self,
        task: dict,
        now_ms: int,
        *,
        status: str,
        accepted_order_id: str,
        accepted_client_order_id: str,
    ) -> None:
        return self.residual_repair_runtime._retain_residual_repair_accepted_order_gap(
            task,
            now_ms,
            status=status,
            accepted_order_id=accepted_order_id,
            accepted_client_order_id=accepted_client_order_id,
        )

    def _clear_residual_repair_accepted_order_gap(self, task: dict) -> None:
        return self.residual_repair_runtime._clear_residual_repair_accepted_order_gap(task)

    async def _fetch_residual_repair_open_orders(
        self, adapter: VenueAdapter, venue: Venue, symbol: str,
    ) -> list[Any]:
        return await self.residual_repair_runtime._fetch_residual_repair_open_orders(
            adapter,
            venue,
            symbol,
        )

    @staticmethod
    def _residual_repair_open_order_items(raw: Any) -> list[Any]:
        return ResidualRepairRuntime._residual_repair_open_order_items(raw)

    def _residual_repair_baseline_size(self, task: dict, repair_venue: Venue) -> float:
        return self.residual_repair_runtime._residual_repair_baseline_size(
            task,
            repair_venue,
        )

    @staticmethod
    def _residual_repair_retry_delay_ms(attempt_count: int) -> int:
        return ResidualRepairRuntime._residual_repair_retry_delay_ms(attempt_count)

    @staticmethod
    def _residual_repair_attempt_count(task: dict) -> int:
        return ResidualRepairRuntime._residual_repair_attempt_count(task)

    def _residual_repair_deadline_or_attempts_exhausted(
        self, task: dict, now_ms: int,
    ) -> bool:
        return self.residual_repair_runtime._residual_repair_deadline_or_attempts_exhausted(
            task,
            now_ms,
        )

    def _pause_pending_residual_repair(
        self, task: dict, now_ms: int, evidence: dict[str, Any] | None = None
    ) -> None:
        return self.residual_repair_runtime._pause_pending_residual_repair(
            task,
            now_ms,
            evidence,
        )

    def _release_residual_repair_pair_gate(self, pair_id: str, symbol: str) -> None:
        return self.residual_repair_runtime._release_residual_repair_pair_gate(
            pair_id,
            symbol,
        )

    def _terminalize_residual_repair_task(
        self,
        task: dict,
        now_ms: int,
        *,
        terminal_reason: str,
        repair_venue: Venue,
        repair_side: Side,
        repair_quantity: float,
        live_price: float,
        min_notional: float,
    ) -> None:
        return self.residual_repair_runtime._terminalize_residual_repair_task(
            task,
            now_ms,
            terminal_reason=terminal_reason,
            repair_venue=repair_venue,
            repair_side=repair_side,
            repair_quantity=repair_quantity,
            live_price=live_price,
            min_notional=min_notional,
        )

    def _reschedule_pending_residual_repair_task(
        self, task: dict, now_ms: int, error: str
    ) -> None:
        return self.residual_repair_runtime._reschedule_pending_residual_repair_task(
            task,
            now_ms,
            error,
        )


    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _ensure_private_ws_started(self, now_ms: int) -> None:
        """V1: start private WS workers for live adapters when credentials/symbols ready.

        Called each tick until all live adapters with private health support have
        workers running. Tracked symbol changes trigger worker replacement.
        Idempotent: skips venues that already have workers for the same symbol set.
        """
        if self.config.runtime.mode == "paper":
            return

        tracked_symbols = self._current_tracked_private_symbols()
        for venue, adapter in self._venue_adapters.items():
            if not getattr(adapter, 'supports_private_health', False):
                continue

            transport = getattr(adapter, '_transport', None)
            if transport is None:
                continue

            symbols = tracked_symbols.get(venue, set())
            prev_symbols = self._private_ws_symbols.get(venue, set())

            # V1: empty symbols → stop any existing workers, clear tracking
            if not symbols:
                if prev_symbols:
                    transport.stop_private_ws()
                    self._private_ws_started.discard(venue)
                    self._private_ws_symbols.pop(venue, None)
                    self.journal.append(
                        "runtime.private_ws_stopped",
                        {
                            "venue": venue.value,
                            "reason": "no tracked symbols",
                        },
                    )
                continue

            # Start if never started or symbols changed
            if venue not in self._private_ws_started or symbols != prev_symbols:
                if symbols != prev_symbols and venue in self._private_ws_started:
                    # V1: worker replacement on symbol change
                    transport.stop_private_ws()

                transport.start_private_ws(list(symbols))
                self._private_ws_started.add(venue)
                self._private_ws_symbols[venue] = set(symbols)
                self.journal.append(
                    "runtime.private_ws_started",
                    {
                        "venue": venue.value,
                        "symbol_count": len(symbols),
                    },
                )

    def _current_tracked_private_symbols(self) -> dict[Venue, set[str]]:
        """Collect symbols that need private WS tracking from current state.

        V1: symbols from primary tracked entry pairs, open positions, and
        pending passive closes.
        """
        result: dict[Venue, set[str]] = {}

        # from open positions — use long/short venue + symbol if present
        for pos in self.state.open_positions.values():
            sym = getattr(pos, 'symbol', '')
            long_v = getattr(pos, 'long_venue', None)
            short_v = getattr(pos, 'short_venue', None)
            if sym:
                if long_v is not None and isinstance(long_v, Venue):
                    result.setdefault(long_v, set()).add(sym)
                if short_v is not None and isinstance(short_v, Venue):
                    result.setdefault(short_v, set()).add(sym)

        # from tracked entry pairs (V1: symbols tracked for entry)
        # pair_id format: "{symbol.lower()}:{long_venue}->{short_venue}"
        # (see entry_local_l2.py:make_candidate_pair_id)
        # IMPORTANT: make_candidate_pair_id() lowercases the symbol for stable
        # identity, so we must canonicalize it back to V2 internal uppercase
        # (e.g. "ethusdt" → "ETHUSDT") before passing to venue private WS.
        for pair_id in getattr(self, '_tracked_primary_pair_ids', set()):
            if not pair_id:
                continue
            # Try canonical format first: "sym:long->short"
            sym = ""
            long_v = None
            short_v = None
            if "->" in pair_id:
                try:
                    before_arrow, short_str = pair_id.rsplit("->", 1)
                    sym, long_str = before_arrow.split(":", 1)
                    sym = sym.upper()  # canonical V2 symbol (was lowercased by make_candidate_pair_id)
                    long_v = Venue(long_str)
                    short_v = Venue(short_str)
                except (ValueError, KeyError):
                    pass
            # Fallback: pipe-delimited format (backward compat / tests)
            if long_v is None:
                parts = pair_id.split("|")
                if len(parts) >= 3:
                    sym = parts[0].upper()  # canonical V2 symbol
                    try:
                        long_v = Venue(parts[1])
                        short_v = Venue(parts[2])
                    except ValueError:
                        continue
            if long_v is not None and short_v is not None and sym:
                result.setdefault(long_v, set()).add(sym)
                result.setdefault(short_v, set()).add(sym)

        # from pending entries (entries being executed that haven't opened yet)
        for entry in getattr(self.state, 'pending_entries', {}).values():
            sym = getattr(entry, 'symbol', '')
            long_v = getattr(entry, 'long_venue', None)
            short_v = getattr(entry, 'short_venue', None)
            if sym:
                if long_v is not None and isinstance(long_v, Venue):
                    result.setdefault(long_v, set()).add(sym)
                if short_v is not None and isinstance(short_v, Venue):
                    result.setdefault(short_v, set()).add(sym)

        # from pending passive closes (maker legs need private WS for progress)
        for pclose in getattr(self.state, 'pending_passive_closes', {}).values():
            pos = getattr(pclose, 'position_snapshot', None)
            # V1: when position_snapshot is not set, try to resolve from open_positions
            if pos is None:
                pid = getattr(pclose, 'position_id', '')
                if pid:
                    pos = self.state.open_positions.get(pid)
            if pos is not None:
                sym = getattr(pos, 'symbol', '')
                long_v = getattr(pos, 'long_venue', None)
                short_v = getattr(pos, 'short_venue', None)
                if sym:
                    if long_v is not None and isinstance(long_v, Venue):
                        result.setdefault(long_v, set()).add(sym)
                    if short_v is not None and isinstance(short_v, Venue):
                        result.setdefault(short_v, set()).add(sym)

        # from pending residual repairs — repair venue must be privately tracked
        # while the task is pending so live excess can converge without restart.
        for task in getattr(self.state, "pending_residual_repairs", []):
            if not isinstance(task, dict):
                continue
            sym = str(task.get("symbol", "") or "")
            venue_raw = task.get("repair_venue") or task.get("exposure_venue")
            if not sym or venue_raw is None:
                continue
            try:
                venue = Venue.from_str(str(venue_raw))
            except Exception:
                continue
            result.setdefault(venue, set()).add(sym)

        return result

    async def _post_tick_housekeeping(self, now_ms: int) -> None:
        """Run after every tick cycle: supervisor, reconciliation, periodic exports."""
        # V1 latch parity: a fail-closed state with no operator override,
        # no recovery block, and no recovery work is stale even after live
        # entry/recovery cleanup, not only during startup snapshot recovery.
        clear_stale_fail_closed_if_recovery_clean(self.state, self.journal)

        # V1: ensure private WS workers are running for live adapters
        self._ensure_private_ws_started(now_ms)

        # Risk-line supervision — V1: refresh_venue_health_supervisor + recompute_global_risk_mode
        # CRITICAL: risk_snapshot_cache must be injected BEFORE supervise() so
        # _collect_venue_health_views() sees current-tick AccountRiskSnapshot data.
        # If the cache is stale/empty, supervisor misdiagnoses risk_snapshot_unavailable
        # and enters fail-closed despite healthy venues.
        self.supervisor.supervise(
            now_ms,
            self.state.venue_health,
            adapters=self._venue_adapters,
            risk_snapshot_cache=self._risk_snapshot_cache,
        )

        # Reconciliation of pending/uncertain outcomes
        await self._reconcile_pending_state(now_ms)

        # V1: residual repairs are normal runtime work, not startup-only work.
        await self._recover_residual_repairs(now_ms)

        # Detect false-clean state where exchanges hold positions but V2 missed them.
        await self._maybe_recover_clean_live_positions(now_ms)

        # Independent no-owner live-position recovery route.
        await self._drive_unpaired_live_position_recoveries(now_ms=now_ms)

        # Periodic Prometheus & state exports
        maybe_export_runtime_metrics(
            self.state, self.config, self._export_state, now_ms
        )
        self._maybe_export_current_state_snapshot(self._export_state, now_ms)

    # ------------------------------------------------------------------
    # Backoff
    # ------------------------------------------------------------------

    def _snapshot_local_l2_state(self):
        return self.market_data_runtime._snapshot_local_l2_state()

    # ------------------------------------------------------------------
    # Entry guards (V1: apply_runtime_entry_guards)
    # ------------------------------------------------------------------

    def _gate_pending_close_reconciliation(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_pending_close_reconciliation(*args, **kwargs)

    def _gate_passive_close_pending(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_passive_close_pending(*args, **kwargs)

    def _gate_reduce_only(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_reduce_only(*args, **kwargs)

    def _gate_venue_cooldown(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_venue_cooldown(*args, **kwargs)

    def _gate_zero_fill_cooldown(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_zero_fill_cooldown(*args, **kwargs)

    @staticmethod
    def _entry_reject_is_post_only_would_take(reason: str) -> bool:
        text = str(reason or "").lower()
        return (
            "-5022" in text
            or "could not be executed as maker" in text
            or "post only order will be rejected" in text
            or "gtx_order_reject" in text
            or "post_only_would_take" in text
        )

    def _record_post_only_reject_cooldown(
        self,
        candidate,
        now_ms: int,
        reason: str,
        *,
        venue: str = "",
        side: str = "",
        price: float = 0.0,
        bbo: dict | None = None,
    ) -> None:
        cooldown_ms = int(
            getattr(
                self.config.strategy,
                "pending_entry_zero_fill_terminal_cooldown_ms",
                30_000,
            )
            or 30_000
        )
        pair_key = (
            getattr(candidate, "symbol", ""),
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        )
        until_ms = now_ms + cooldown_ms
        self._zero_fill_cooldown_until_ms[pair_key] = until_ms
        venue = venue or pair_key[1]
        if venue:
            self._post_only_reject_cooldown_until_ms[(pair_key[0], venue)] = until_ms
        bbo_payload = dict(bbo or {})
        evidence = self._entry_admission_evidence("post_only_would_take")
        self.journal.append(
            "runtime.entry_post_only_reject_cooldown",
            {
                "symbol": pair_key[0],
                "venue": venue,
                "operation": "entry_post_only_submit",
                "reason": "post_only_would_take",
                "raw_error": reason[:500],
                "post_only": True,
                "reduce_only": False,
                "blocked_until_ms": until_ms,
                "ttl_ms": cooldown_ms,
                "official_doc_url": evidence["official_doc_url"],
                "evidence_gap": evidence["evidence_gap"],
                "long_venue": pair_key[1],
                "short_venue": pair_key[2],
                "side": side or bbo_payload.get("side", ""),
                "price": price or bbo_payload.get("price", 0.0),
                "best_bid": bbo_payload.get("best_bid"),
                "best_ask": bbo_payload.get("best_ask"),
                "book_age_ms": bbo_payload.get("book_age_ms"),
                "stale_after_ms": bbo_payload.get("stale_after_ms"),
                "freshness": bbo_payload.get("freshness", "unknown"),
                "would_cross": bbo_payload.get("would_cross", False),
                "cooldown_until_ms": until_ms,
                "cooldown_until": until_ms,
                "cooldown_ms": cooldown_ms,
            },
        )

    def _post_only_maker_bbo_guard(
        self,
        *,
        venue: Venue,
        symbol: str,
        side: Side,
        price: float,
        now_ms: int,
    ) -> tuple[bool, str, dict]:
        venue_str = venue.value if hasattr(venue, "value") else str(venue)
        side_str = side.value if hasattr(side, "value") else str(side)
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        payload = {
            "venue": venue_str,
            "symbol": symbol,
            "side": side_str,
            "price": price,
            "best_bid": None,
            "best_ask": None,
            "book_age_ms": None,
            "stale_after_ms": stale_after_ms,
            "freshness": "not_checked_local_l2_disabled",
            "would_cross": False,
            "source": "local_l2",
            "provider": self._entry_readiness_provider_name(),
            "domain": "local_l2_book",
            "blocker_family": "post_only_bbo",
            "repriced_attempted": False,
        }
        if self._entry_readiness_provider_uses_ws_bbo():
            stale_after_ms = self._entry_quote_lease_max_age_ms()
            payload["source"] = "ws_bbo_quote_lease"
            payload["provider"] = "ws_bbo_quote_lease"
            payload["domain"] = "ws_bbo_cache"
            payload["stale_after_ms"] = stale_after_ms
            quote = self.ws_bbo_cache.get_quote(venue_str, symbol)
            if quote is None:
                payload["freshness"] = "missing"
                return False, "missing_bbo", payload
            best_bid = float(getattr(quote, "bid", 0.0) or 0.0)
            best_ask = float(getattr(quote, "ask", 0.0) or 0.0)
            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            fresh = observed_at_ms > 0 and age_ms <= stale_after_ms
            valid_bbo = best_bid > 0.0 and best_ask > best_bid
            would_cross = (
                valid_bbo
                and price > 0.0
                and (
                    (side == Side.BUY and price >= best_ask)
                    or (side == Side.SELL and price <= best_bid)
                )
            )
            payload.update(
                {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "book_age_ms": age_ms,
                    "observed_at_ms": observed_at_ms,
                    "freshness": "fresh" if fresh else "stale",
                    "would_cross": would_cross,
                }
            )
            if not valid_bbo:
                payload["freshness"] = "invalid_bbo"
                return False, "invalid_bbo", payload
            if not fresh:
                return False, "stale_bbo", payload
            if would_cross:
                repriced_price = best_bid if side == Side.BUY else best_ask
                repriced_would_cross = (
                    repriced_price > 0.0
                    and (
                        (side == Side.BUY and repriced_price >= best_ask)
                        or (side == Side.SELL and repriced_price <= best_bid)
                    )
                )
                payload.update(
                    {
                        "repriced_attempted": True,
                        "original_price": price,
                        "repriced_price": repriced_price,
                        "repriced_would_cross": repriced_would_cross,
                    }
                )
                if repriced_price > 0.0 and not repriced_would_cross:
                    payload["price"] = repriced_price
                    payload["would_cross"] = False
                    return True, "", payload
                return False, "would_cross_bbo", payload
            return True, "", payload

        if not self._local_l2_effective_enabled():
            return True, "", payload

        book = self.local_l2_runtime.get_book(venue_str, symbol)
        if book is None:
            payload["freshness"] = "missing"
            return False, "missing_bbo", payload

        try:
            best_bid = float(book.best_bid())
            best_ask = float(book.best_ask())
        except Exception:
            best_bid = 0.0
            best_ask = 0.0
        try:
            age_ms = int(book.age_ms(now_ms))
        except Exception:
            observed = int(getattr(book, "observed_at_ms", 0) or 0)
            age_ms = now_ms - observed if observed > 0 else 0

        status = getattr(getattr(book, "status", None), "value", str(getattr(book, "status", "")))
        try:
            stale = bool(book.is_stale(stale_after_ms, now_ms))
        except Exception:
            stale = age_ms > stale_after_ms
        fresh = status == "hot" and not stale
        valid_bbo = best_bid > 0.0 and best_ask > best_bid
        would_cross = (
            valid_bbo
            and price > 0.0
            and (
                (side == Side.BUY and price >= best_ask)
                or (side == Side.SELL and price <= best_bid)
            )
        )

        payload.update(
            {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "book_age_ms": age_ms,
                "freshness": "fresh" if fresh else "stale",
                "would_cross": would_cross,
            }
        )
        if not valid_bbo:
            payload["freshness"] = "invalid_bbo"
            return False, "invalid_bbo", payload
        if not fresh:
            return False, "stale_bbo", payload
        if would_cross:
            return False, "would_cross_bbo", payload
        return True, "", payload

    def _gate_pending_entry_dedup(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_pending_entry_dedup(*args, **kwargs)

    def _gate_recovery_ledger(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_recovery_ledger(*args, **kwargs)

    def _remove_pending_entry_after_terminal_decision(self, *args, **kwargs):
        return self.pending_entry_runtime._remove_pending_entry_after_terminal_decision(*args, **kwargs)

    async def _complete_pending_entry_terminal_removal(self, *args, **kwargs):
        return await self.pending_entry_runtime._complete_pending_entry_terminal_removal(*args, **kwargs)

    def _pending_entry_terminal_needs_recovery_core_refresh(self, *args, **kwargs):
        return self.pending_entry_runtime._pending_entry_terminal_needs_recovery_core_refresh(*args, **kwargs)

    async def _refresh_recovery_core_after_pending_entry_terminal(
        self,
        *,
        reason: str,
        symbol: str,
        source_symbols: dict[str, list[str]],
        now_ms: int,
    ) -> None:
        symbols: set[str] = {str(symbol or "").upper()} if symbol else set()
        for values in source_symbols.values():
            symbols.update(str(value or "").upper() for value in values if value)
        symbols.discard("")

        if symbols:
            refreshed = await self._refresh_recovery_ledger_for_symbols(
                sorted(symbols),
                now_ms,
            )
            if refreshed is not None:
                self.journal.append(
                    "recovery.pending_entry_terminal_core_refresh",
                    {
                        "symbol": symbol,
                        "reason": reason,
                        "symbols": sorted(symbols),
                        "truth_required_symbol_sources": source_symbols,
                        "decision": (
                            self.recovery_decision.kind.value
                            if self.recovery_decision is not None
                            else ""
                        ),
                        "entry_allowed": (
                            self.recovery_decision.entry_allowed
                            if self.recovery_decision is not None
                            else False
                        ),
                        "ts_ms": now_ms,
                    },
                )
                return

        self._refresh_recovery_ledger_from_exchange_truth(
            {
                "truth_available": False,
                "positions": [],
                "open_orders": [],
                "probe_evidence": [
                    {
                        "symbol": symbol,
                        "method": "pending_entry_terminal_release",
                        "classification": (
                            "terminal_pending_entry_exchange_truth_unavailable"
                        ),
                        "finished_at_ms": now_ms,
                    }
                ],
                "errors": ["pending_entry_terminal_release_truth_unavailable"],
            },
            now_ms=now_ms,
        )

    @staticmethod
    def _pending_entry_terminalizer_decision_payload(*args, **kwargs):
        return PendingEntryRuntime._pending_entry_terminalizer_decision_payload(*args, **kwargs)

    def _gate_entry_sizing(self, *args, **kwargs):
        return self.entry_gate_runtime._gate_entry_sizing(*args, **kwargs)

    def _entry_local_l2_stale_after_ms(self) -> int:
        return self._configured_entry_l2_stale_after_ms(self.config)

    @staticmethod
    def _configured_entry_l2_stale_after_ms(config) -> int:
        for field_name in (
            "entry_local_l2_book_stale_after_ms",
            "local_l2_quiet_book_grace_ms",
            "local_l2_max_age_ms",
        ):
            value = int(getattr(config.strategy, field_name, 0) or 0)
            if value > 0:
                return value
        return 300_000

    def _snapshot_domain_budget_ms(self, domain: str, row=None):
        return self.market_data_runtime._snapshot_domain_budget_ms(domain, row)

    @staticmethod
    def _snapshot_metric_key(venue: str, symbol: str, domain: str):
        return MarketDataRuntime._snapshot_metric_key(venue, symbol, domain)

    @staticmethod
    def _record_snapshot_metric(metrics: dict, key: str, fresh: bool):
        return MarketDataRuntime._record_snapshot_metric(metrics, key, fresh)

    def _snapshot_fallback_source(self, snapshot):
        return self.market_data_runtime._snapshot_fallback_source(snapshot)

    @staticmethod
    def _candidate_requires_sidecar_perp_liquidity(candidate) -> bool:
        for field_name in (
            "liquidity_source",
            "entry_liquidity_source",
            "sizing_liquidity_source",
            "execution_liquidity_source",
        ):
            source = str(getattr(candidate, field_name, "") or "").lower()
            if source in {
                "sidecar",
                "coarse_sidecar",
                "perp_liquidity",
                "sidecar_perp_liquidity",
            }:
                return True
        return False

    def _entry_liquidity_qualification_state(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_liquidity_qualification_state(*args, **kwargs)

    def _entry_liquidity_volume_floor_quote(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_liquidity_volume_floor_quote(*args, **kwargs)

    def _entry_liquidity_open_interest_floor_quote(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_liquidity_open_interest_floor_quote(*args, **kwargs)

    def _entry_liquidity_decision_payload(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_liquidity_decision_payload(*args, **kwargs)

    def _entry_liquidity_qualification_decisions(
        self,
        candidate,
        *,
        snapshot,
        quote_lookup: dict,
        now_ms: int,
        fallback_source: str,
        record_result: bool = False,
    ) -> list[dict]:
        return self.entry_gate_runtime._entry_liquidity_qualification_decisions(
            candidate,
            snapshot=snapshot,
            quote_lookup=quote_lookup,
            now_ms=now_ms,
            fallback_source=fallback_source,
            record_result=record_result,
        )

    @staticmethod
    def _liquidity_degraded_reason_blocks_symbol(reason: str, symbol: str) -> bool:
        reason_upper = str(reason or "").upper()
        symbol_upper = str(symbol or "").upper()
        if not reason_upper or not symbol_upper:
            return False
        return symbol_upper in reason_upper

    def _liquidity_lifecycle_payload(
        self,
        *,
        row,
        venue: str,
        symbol: str,
        now_ms: int,
        decision: str,
        reason: str,
        fallback_source: str,
    ) -> dict:
        observed_at_ms = (
            int(getattr(row, "observed_at_ms", 0) or 0)
            if row is not None else 0
        )
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        budget_ms = self._snapshot_domain_budget_ms("liquidity", row)
        source_domain = (
            str(getattr(row, "domain", "perp_liquidity") or "perp_liquidity")
            if row is not None else "perp_liquidity"
        )
        source = (
            str(
                getattr(
                    row,
                    "source",
                    "sidecar_perp_liquidity",
                )
                or "sidecar_perp_liquidity"
            )
            if row is not None else "sidecar_perp_liquidity"
        )
        return {
            "venue": venue,
            "symbol": symbol,
            "domain": "liquidity",
            "source_domain": source_domain,
            "source": source,
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": budget_ms,
            "publish_interval_ms": (
                int(getattr(row, "publish_interval_ms", 0) or 0)
                if row is not None else 0
            ),
            "published_at_ms": (
                int(getattr(row, "published_at_ms", 0) or 0)
                if row is not None else 0
            ),
            "coverage_usable": (
                int(getattr(row, "coverage_usable", 0) or 0)
                if row is not None else 0
            ),
            "symbol_count": (
                int(getattr(row, "symbol_count", 0) or 0)
                if row is not None else 0
            ),
            "degraded_reason": (
                str(getattr(row, "degraded_reason", "") or "")
                if row is not None else "missing_liquidity_lifecycle"
            ),
            "decision": decision,
            "fallback_source": fallback_source,
            "reason": reason,
            "event_kind": f"runtime.{reason}",
            "blocking": decision == "skip_entry",
        }

    @staticmethod
    def _snapshot_quote_direct_observed_at_ms(quote):
        return MarketDataRuntime._snapshot_quote_direct_observed_at_ms(quote)

    @staticmethod
    def _snapshot_quote_source(quote):
        return MarketDataRuntime._snapshot_quote_source(quote)

    def _snapshot_quote_observed_at_ms(self, snapshot, quote):
        return self.market_data_runtime._snapshot_quote_observed_at_ms(snapshot, quote)

    @staticmethod
    def _snapshot_scoped_status_key(domain: str, venue: str, symbol: str, source: str):
        return MarketDataRuntime._snapshot_scoped_status_key(domain, venue, symbol, source)

    def _record_snapshot_scoped_status(
        self,
        statuses: dict[str, dict],
        *,
        domain: str,
        venue: str,
        symbol: str,
        source: str,
        observed_at_ms: int,
        age_ms: int,
        budget_ms: int,
        fresh: bool,
    ) -> None:
        statuses[self._snapshot_scoped_status_key(domain, venue, symbol, source)] = {
            "domain": str(domain).lower(),
            "venue": str(venue).lower(),
            "symbol": str(symbol).upper(),
            "source": str(source).lower(),
            "status": "fresh" if fresh else "stale",
            "observed_at_ms": int(observed_at_ms),
            "age_ms": int(age_ms),
            "budget_ms": int(budget_ms),
        }

    def _snapshot_lifecycle_rows_by_venue(self, snapshot, domain: str):
        return self.market_data_runtime._snapshot_lifecycle_rows_by_venue(snapshot, domain)

    def _snapshot_freshness_observability(self, *, snapshot, candidates: list, now_ms: int):
        return self.market_data_runtime._snapshot_freshness_observability(snapshot=snapshot, candidates=candidates, now_ms=now_ms)

    def _candidate_snapshot_freshness_decisions(self, candidate, *, snapshot, now_ms: int, record_liquidity_qualification: bool=False, entry_quote_truth_overlay: dict[tuple[str, str], Any] | None=None):
        return self.market_data_runtime._candidate_snapshot_freshness_decisions(candidate, snapshot=snapshot, now_ms=now_ms, record_liquidity_qualification=record_liquidity_qualification, entry_quote_truth_overlay=entry_quote_truth_overlay)

    @staticmethod
    def _snapshot_quote_evidence(*, quote, observed_at_ms: int, age_ms: int, budget_ms: int):
        return MarketDataRuntime._snapshot_quote_evidence(quote=quote, observed_at_ms=observed_at_ms, age_ms=age_ms, budget_ms=budget_ms)

    def _ws_bbo_entry_quote_resolution(self, *, venue: str, symbol: str, now_ms: int, sidecar_reason: str, sidecar_source: str, sidecar_observed_at_ms: int, sidecar_age_ms: int, sidecar_budget_ms: int, fallback_source: str):
        return self.market_data_runtime._ws_bbo_entry_quote_resolution(venue=venue, symbol=symbol, now_ms=now_ms, sidecar_reason=sidecar_reason, sidecar_source=sidecar_source, sidecar_observed_at_ms=sidecar_observed_at_ms, sidecar_age_ms=sidecar_age_ms, sidecar_budget_ms=sidecar_budget_ms, fallback_source=fallback_source)

    def _entry_quote_truth_decision(self, *, venue: str, symbol: str, quote: Any | None, now_ms: int, fallback_source: str, sidecar_source: str, sidecar_observed_at_ms: int, sidecar_age_ms: int, sidecar_budget_ms: int, sidecar_reason: str):
        return self.market_data_runtime._entry_quote_truth_decision(venue=venue, symbol=symbol, quote=quote, now_ms=now_ms, fallback_source=fallback_source, sidecar_source=sidecar_source, sidecar_observed_at_ms=sidecar_observed_at_ms, sidecar_age_ms=sidecar_age_ms, sidecar_budget_ms=sidecar_budget_ms, sidecar_reason=sidecar_reason)

    @staticmethod
    def _snapshot_freshness_evidence_fields(decision: dict):
        return MarketDataRuntime._snapshot_freshness_evidence_fields(decision)

    def _candidate_snapshot_freshness_failures(self, candidate, *, snapshot, now_ms: int, entry_quote_truth_overlay: dict[tuple[str, str], Any] | None=None):
        return self.market_data_runtime._candidate_snapshot_freshness_failures(candidate, snapshot=snapshot, now_ms=now_ms, entry_quote_truth_overlay=entry_quote_truth_overlay)

    def _snapshot_fallback_duration_ms(self, *, snapshot, now_ms: int, max_age_ms: int | None=None):
        return self.market_data_runtime._snapshot_fallback_duration_ms(snapshot=snapshot, now_ms=now_ms, max_age_ms=max_age_ms)

    def _snapshot_candidate_scope_sample(self, *, candidate, domain: str, venue: str, source: str, source_age_ms: int, fallback_duration_ms: int, blocked: bool, block_reason: str=''):
        return self.market_data_runtime._snapshot_candidate_scope_sample(candidate=candidate, domain=domain, venue=venue, source=source, source_age_ms=source_age_ms, fallback_duration_ms=fallback_duration_ms, blocked=blocked, block_reason=block_reason)

    @staticmethod
    def _canonical_degraded_domain(domain: str):
        return MarketDataRuntime._canonical_degraded_domain(domain)

    def _snapshot_health_candidate_freshness_scope(self, *, snapshot, now_ms: int, degraded_domains: list[str], stale_degraded_domains: list[str], fallback_duration_ms: int, candidates: list | None=None):
        return self.market_data_runtime._snapshot_health_candidate_freshness_scope(snapshot=snapshot, now_ms=now_ms, degraded_domains=degraded_domains, stale_degraded_domains=stale_degraded_domains, fallback_duration_ms=fallback_duration_ms, candidates=candidates)

    def _snapshot_health_candidate_scope_candidates(self, snapshot):
        return self.market_data_runtime._snapshot_health_candidate_scope_candidates(snapshot)

    def _snapshot_freshness_decision_log_key(self, payload: dict):
        return self.market_data_runtime._snapshot_freshness_decision_log_key(payload)

    def _append_snapshot_freshness_decision_event(self, *, payload: dict, event_kind: str, now_ms: int):
        return self.market_data_runtime._append_snapshot_freshness_decision_event(payload=payload, event_kind=event_kind, now_ms=now_ms)

    def _filter_candidates_by_snapshot_freshness(self, candidates: list, *, snapshot, now_ms: int, metrics: dict, ages: dict, budgets: dict | None=None, publish_intervals: dict | None=None, entry_quote_truth_overlay: dict[tuple[str, str], Any] | None=None):
        return self.market_data_runtime._filter_candidates_by_snapshot_freshness(candidates, snapshot=snapshot, now_ms=now_ms, metrics=metrics, ages=ages, budgets=budgets, publish_intervals=publish_intervals, entry_quote_truth_overlay=entry_quote_truth_overlay)

    def _snapshot_health_payload(self, *, snapshot, now_ms: int, max_age_ms: int, freshness: str):
        return self.market_data_runtime._snapshot_health_payload(snapshot=snapshot, now_ms=now_ms, max_age_ms=max_age_ms, freshness=freshness)

    def _refresh_entry_l2_session_readiness(self, now_ms: int) -> None:
        """Sync entry-local-L2 session legs from local-L2 book readiness."""
        if not self._local_l2_effective_enabled():
            return
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        stale_after_ms = self._entry_local_l2_stale_after_ms()
        for pair_id, session in list(self.entry_l2_sessions.sessions.items()):
            for leg in session.legs.values():
                book = self.local_l2_runtime.get_book(leg.venue, leg.symbol)
                diag = dict(
                    apply_book_readiness_to_leg(
                        leg, book, now_ms=now_ms, stale_after_ms=stale_after_ms,
                    )
                )
                diag["pair_id"] = pair_id
                diag["leg_state"] = leg.state.value if hasattr(leg.state, "value") else str(leg.state)
                self._entry_l2_last_leg_diagnostics[(pair_id, leg.venue)] = diag
            session.refresh_state(now_ms, stale_after_ms=stale_after_ms)

        self._maybe_emit_entry_l2_readiness_diagnostics(now_ms)

    def _entry_l2_readiness_diagnostics_payload(self) -> dict:
        primary_pair_ids = sorted(self._tracked_primary_pair_ids)
        if primary_pair_ids:
            pair_ids = primary_pair_ids
        else:
            pair_ids = sorted(self.entry_l2_sessions.sessions.keys())

        not_ready: list[dict] = []
        reason_totals: Counter[str] = Counter()
        for pair_id in pair_ids:
            session = self.entry_l2_sessions.sessions.get(pair_id)
            if session is None:
                continue
            for venue in sorted(session.legs.keys()):
                leg = session.legs[venue]
                diag = self._entry_l2_last_leg_diagnostics.get((pair_id, venue))
                if diag is None:
                    diag = {
                        "pair_id": pair_id,
                        "venue": leg.venue,
                        "symbol": leg.symbol,
                        "ready": False,
                        "reason": (
                            leg.fault.value if getattr(leg, "fault", None) is not None
                            else (
                                leg.arming_reason.value
                                if getattr(leg, "arming_reason", None) is not None
                                else "not_ready"
                            )
                        ),
                        "detail": getattr(leg, "fault_detail", "") or "",
                        "book_status": "unknown",
                        "age_ms": None,
                        "observed_at_ms": getattr(leg, "last_seen_at_ms", 0),
                        "sequence": 0,
                        "leg_state": leg.state.value if hasattr(leg.state, "value") else str(leg.state),
                    }
                if diag.get("ready") is True:
                    continue
                reason = str(diag.get("reason", "not_ready"))
                reason_totals[reason] += 1
                if len(not_ready) < 24:
                    not_ready.append({
                        "pair_id": pair_id,
                        "venue": str(diag.get("venue", leg.venue)),
                        "symbol": str(diag.get("symbol", leg.symbol)),
                        "reason": reason,
                        "detail": str(diag.get("detail", "")),
                        "book_status": str(diag.get("book_status", "unknown")),
                        "age_ms": diag.get("age_ms"),
                        "observed_at_ms": int(diag.get("observed_at_ms", 0) or 0),
                        "sequence": int(diag.get("sequence", 0) or 0),
                        "leg_state": str(diag.get("leg_state", "")),
                    })

        reason_counts = Counter(sample["reason"] for sample in not_ready)
        return {
            "primary_pair_ids": primary_pair_ids,
            "not_ready": not_ready,
            "reason_counts": dict(sorted(reason_counts.items())),
            "reason_totals": dict(sorted(reason_totals.items())),
        }

    @staticmethod
    def _payload_fingerprint(payload: dict) -> str:
        import json

        return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))

    def _maybe_emit_entry_l2_readiness_diagnostics(self, now_ms: int) -> None:
        if getattr(self.journal, "_file", None) is None:
            return
        diag = self._entry_l2_readiness_diagnostics_payload()
        if not diag["not_ready"]:
            return
        payload = {
            "primary_pair_ids": diag["primary_pair_ids"],
            "not_ready": diag["not_ready"],
            "reason_totals": diag["reason_totals"],
            "ts_ms": now_ms,
        }
        fingerprint = self._payload_fingerprint({
            "primary_pair_ids": payload["primary_pair_ids"],
            "not_ready": [
                {
                    "pair_id": s["pair_id"],
                    "venue": s["venue"],
                    "reason": s["reason"],
                    "detail": s["detail"],
                    "book_status": s["book_status"],
                }
                for s in payload["not_ready"]
            ],
        })
        if (
            fingerprint == self._last_entry_l2_readiness_diag_fingerprint
            and now_ms - self._last_entry_l2_readiness_diag_ts_ms < 60_000
        ):
            return
        self._last_entry_l2_readiness_diag_fingerprint = fingerprint
        self._last_entry_l2_readiness_diag_ts_ms = now_ms
        self.journal.append("runtime.entry_local_l2_readiness_diagnostics", payload)

    @staticmethod
    def _v1_tradeable_no_entry_reason(
        selection_blocker_counts: Counter,
        admission_blocker_counts: Counter | None = None,
    ) -> str | None:
        blocker_counts: Counter[str] = Counter()
        for key, value in selection_blocker_counts.items():
            count = int(value)
            if count > 0:
                blocker_counts[str(key)] += count
        if admission_blocker_counts is not None:
            for key, value in admission_blocker_counts.items():
                count = int(value)
                if count > 0:
                    blocker_counts[str(key)] += count

        blockers = {key for key, count in blocker_counts.items() if count > 0}
        if not blockers:
            return None
        if blockers == {"entry_waiting_for_finalization_window_too_early"}:
            return "tradeable_candidates_waiting_for_entry_finalization_window_too_early"
        if blockers == {"entry_finalization_window_expired"}:
            return "tradeable_candidates_expired_after_entry_finalization_window"
        if blockers <= {
            "entry_waiting_for_finalization_window_too_early",
            "entry_finalization_window_expired",
        }:
            return "tradeable_candidates_outside_entry_finalization_window"
        if blockers == {"entry_local_l2_waiting_for_prewarm_window"}:
            return "tradeable_candidates_waiting_for_entry_local_l2_prewarm_window"
        if blockers == {"entry_local_l2_waiting_for_dual_ready"}:
            return "tradeable_candidates_waiting_for_entry_local_l2_dual_ready"
        ws_bbo_blockers = {
            key for key in blockers
            if key.startswith("entry_ws_bbo_quote_lease_")
        }
        if ws_bbo_blockers and blockers <= ws_bbo_blockers:
            return "tradeable_candidates_blocked_by_entry_ws_bbo_readiness"
        admission_blockers = {
            key for key in blockers
            if key.endswith("_admission_blocked")
            or key in {
                "bybit_trading_terms_required",
                "insufficient_margin_admission_prefiltered",
                "hyperliquid_account_balance_unavailable",
            }
        }
        if admission_blockers and blockers <= admission_blockers:
            return "tradeable_candidates_blocked_by_entry_admission"
        lifecycle_blockers = LiveRuntime._V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS
        if "entry_blocked_recovery_ledger" in blockers:
            return "tradeable_candidates_blocked_by_recovery_ledger"
        if blockers & lifecycle_blockers:
            return "tradeable_candidates_blocked_by_lifecycle"
        return "tradeable_candidates_blocked_by_entry_local_l2_readiness"

    @staticmethod
    def _no_tradeable_reason_from_candidate_blockers(
        blocked_reason_counts: Counter,
        snapshot_freshness_blockers: Counter,
    ) -> str:
        if snapshot_freshness_blockers:
            return "candidate_snapshot_domain_stale"
        blockers = {
            str(key) for key, count in blocked_reason_counts.items()
            if int(count) > 0
        }
        if blockers & {
            "funding_edge_below_floor",
            "expected_edge_below_floor",
            "worst_case_edge_below_floor",
            "zero_order_size",
        }:
            return "candidate_edge_insufficient"
        if blockers & {
            "funding_window_passed",
            "outside_scan_window",
            "no_near_term_settlement",
            "stagger_gap_too_wide",
            "missing_candidate_identity_or_funding_timestamp",
        }:
            return "candidate_window_mismatch"
        return "no_tradeable_candidates"

    def _compact_scan_no_entry_diagnostics_payload(
        self,
        payload: dict,
        *,
        suppressed_full_payload_count: int,
    ) -> dict:
        return self.entry_gate_runtime._compact_scan_no_entry_diagnostics_payload(
            payload,
            suppressed_full_payload_count=suppressed_full_payload_count,
        )

    def _emit_scan_no_entry_diagnostics(
        self,
        *,
        reason: str,
        snapshot,
        tradeable: list,
        selected_candidate_count: int,
        dispatched_candidate_count: int,
        remaining_slots: int,
        tradeable_selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        now_ms: int,
        admission_blocker_counts: Counter | None = None,
    ) -> None:
        return self.entry_gate_runtime._emit_scan_no_entry_diagnostics(
            reason=reason,
            snapshot=snapshot,
            tradeable=tradeable,
            selected_candidate_count=selected_candidate_count,
            dispatched_candidate_count=dispatched_candidate_count,
            remaining_slots=remaining_slots,
            tradeable_selection_blocker_counts=tradeable_selection_blocker_counts,
            candidate_blockers=candidate_blockers,
            now_ms=now_ms,
            admission_blocker_counts=admission_blocker_counts,
        )

    def _emit_entry_opportunity_funnel(
        self,
        *,
        reason: str,
        snapshot,
        tradeable: list,
        selected: list,
        dispatched_candidate_count: int,
        remaining_slots: int,
        tradeable_selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        now_ms: int,
        admission_blocker_counts: Counter | None = None,
    ) -> None:
        return self.entry_gate_runtime._emit_entry_opportunity_funnel(
            reason=reason,
            snapshot=snapshot,
            tradeable=tradeable,
            selected=selected,
            dispatched_candidate_count=dispatched_candidate_count,
            remaining_slots=remaining_slots,
            tradeable_selection_blocker_counts=tradeable_selection_blocker_counts,
            candidate_blockers=candidate_blockers,
            now_ms=now_ms,
            admission_blocker_counts=admission_blocker_counts,
        )

    # ------------------------------------------------------------------
    # Entry dispatch
    # ------------------------------------------------------------------

    def _entry_selection_target(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_selection_target(*args, **kwargs)

    def _candidate_pair_id(self, candidate) -> str:
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        pair_id = getattr(candidate, "pair_id", "")
        if pair_id:
            return str(pair_id)
        return make_candidate_pair_id(
            str(getattr(candidate, "symbol", "")),
            str(getattr(candidate, "long_venue", "")),
            str(getattr(candidate, "short_venue", "")),
        )

    def _pending_entry_pair_id(self, pending) -> str:
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        pair_id = getattr(pending, "pair_id", "")
        if pair_id:
            return str(pair_id)

        def venue_value(value) -> str:
            raw = getattr(value, "value", value)
            return str(raw or "")

        return make_candidate_pair_id(
            str(getattr(pending, "symbol", "")),
            venue_value(getattr(pending, "long_venue", "")),
            venue_value(getattr(pending, "short_venue", "")),
        )

    def _candidate_is_tradeable_for_selection(self, *args, **kwargs):
        return self.entry_gate_runtime._candidate_is_tradeable_for_selection(*args, **kwargs)

    def _market_quote_lookup(self, market_quotes):
        return self.market_data_runtime._market_quote_lookup(market_quotes)

    def _candidate_quote(self, *args, **kwargs):
        return self.entry_gate_runtime._candidate_quote(*args, **kwargs)

    def _entry_leg_depth_score(self, *args, **kwargs):
        return self.entry_gate_runtime._entry_leg_depth_score(*args, **kwargs)

    def _runtime_candidate_risk_score(self, *args, **kwargs):
        return self.entry_gate_runtime._runtime_candidate_risk_score(*args, **kwargs)

    def _runtime_candidate_selection_score(self, *args, **kwargs):
        return self.entry_gate_runtime._runtime_candidate_selection_score(*args, **kwargs)

    def _candidate_final_selection_sort_key(self, *args, **kwargs):
        return self.entry_gate_runtime._candidate_final_selection_sort_key(*args, **kwargs)

    def _has_pending_residual_pair(self, *args, **kwargs):
        return self.entry_gate_runtime._has_pending_residual_pair(*args, **kwargs)

    def _apply_shadow_promotion_if_eligible(
        self, tracked: list, now_ms: int,
    ) -> None:
        """V1: shadow_promotion swap — best shadow replaces worst primary.

        Rejects promotion when primary is executing, shadow not ready,
        score delta insufficient, or hold window not elapsed.
        Logs primary_hold_blocked when score qualifies but hold blocks.
        (execution_core/engine.rs:2643-2719)
        """
        if not tracked:
            return

        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunityClass,
            primary_hold_window_allows_replacement,
            shadow_promotion_is_eligible,
        )

        tracked_lookup = {t.pair_id: t for t in tracked}
        primaries = [t for t in tracked if t.class_ == TrackedOpportunityClass.PRIMARY]
        shadows = [t for t in tracked if t.class_ == TrackedOpportunityClass.SHADOW]

        if not primaries or not shadows:
            return

        score_delta_bps = getattr(
            self.config.strategy,
            "shadow_promotion_score_delta_bps",
            5.0,
        )
        primary_min_hold_ms = getattr(
            self.config.strategy, "primary_min_hold_ms", 30_000,
        )

        best_shadow = max(shadows, key=lambda t: t.ranking_edge_bps)
        worst_primary = min(primaries, key=lambda t: t.ranking_edge_bps)

        primary_session = self.entry_l2_sessions.sessions.get(worst_primary.pair_id)
        primary_assigned_at = (
            primary_session.primary_assigned_at_ms if primary_session else 0
        )

        shadow_session = self.entry_l2_sessions.sessions.get(best_shadow.pair_id)
        shadow_ready = (
            shadow_session.state.value == "ready" if shadow_session else False
        )
        primary_executing = self._tracked_pair_is_executing(worst_primary.pair_id)

        hold_allows = primary_hold_window_allows_replacement(
            primary_assigned_at, now_ms, primary_min_hold_ms)

        eligible = shadow_promotion_is_eligible(
            primary=worst_primary,
            shadow=best_shadow,
            primary_assigned_at_ms=primary_assigned_at,
            now_ms=now_ms,
            primary_min_hold_ms=primary_min_hold_ms,
            shadow_promotion_score_delta_bps=score_delta_bps,
            primary_executing=primary_executing,
            shadow_ready=shadow_ready,
        )

        if eligible:
            if worst_primary.pair_id in self._tracked_primary_pair_ids:
                self._tracked_primary_pair_ids.discard(worst_primary.pair_id)
            self._tracked_primary_pair_ids.add(best_shadow.pair_id)
            best_shadow.class_ = TrackedOpportunityClass.PRIMARY
            worst_primary.class_ = TrackedOpportunityClass.SHADOW
            if shadow_session:
                shadow_session.shadow_promoted_at_ms = now_ms
            self.journal.append(
                "runtime.entry_local_l2_primary_changed",
                {
                    "promoted_pair_id": best_shadow.pair_id,
                    "demoted_pair_id": worst_primary.pair_id,
                    "reason": "shadow_promotion",
                    "ts_ms": now_ms,
                },
            )
        else:
            score_delta = best_shadow.ranking_edge_bps - worst_primary.ranking_edge_bps
            if score_delta >= score_delta_bps and not hold_allows:
                self.journal.append(
                    "runtime.entry_local_l2_shadow_blocked",
                    {
                        "shadow_pair_id": best_shadow.pair_id,
                        "primary_pair_id": worst_primary.pair_id,
                        "reason": "primary_hold_window",
                        "ts_ms": now_ms,
                    },
                )

    def _tracked_pair_is_executing(self, pair_id: str) -> bool:
        """Check if a tracked pair has a pending entry currently executing.

        V1: tracked_entry_local_l2_is_executing (engine.rs).
        """
        parts = pair_id.split(":", 2)
        if len(parts) < 3:
            return False
        long_v, short_v, symbol = parts[0], parts[1], parts[2]
        for pending in self.state.pending_entries.values():
            if (
                pending.symbol == symbol
                and pending.long_venue.value == long_v
                and pending.short_venue.value == short_v
            ):
                return True
        return False

    def _select_entry_candidates(
        self,
        tradeable: list,
        *,
        now_ms: int,
        remaining_slots: int,
        selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        market_quotes=None,
        admission_blocker_counts: Counter | None = None,
    ) -> list:
        return self.entry_gate_runtime._select_entry_candidates(
            tradeable,
            now_ms=now_ms,
            remaining_slots=remaining_slots,
            selection_blocker_counts=selection_blocker_counts,
            candidate_blockers=candidate_blockers,
            market_quotes=market_quotes,
            admission_blocker_counts=admission_blocker_counts,
        )

    def _entry_finalization_window_blocker(
        self,
        first_funding_timestamp_ms: int,
        now_ms: int,
    ) -> str:
        return self.entry_gate_runtime._entry_finalization_window_blocker(
            first_funding_timestamp_ms,
            now_ms,
        )

    def _entry_local_l2_selection_blocker(self, candidate, now_ms: int) -> str:
        return self.entry_gate_runtime._entry_local_l2_selection_blocker(
            candidate,
            now_ms,
        )

    @staticmethod
    def _safe_positive_float(value) -> float:
        try:
            result = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return result if math.isfinite(result) and result > 0 else 0.0

    async def _okx_entry_base_quantity_step(
        self,
        venue: Venue,
        symbol: str,
    ) -> float | None:
        return await self.entry_dispatch_runtime._okx_entry_base_quantity_step(
            venue,
            symbol,
        )

    async def _okx_aligned_entry_quantity(
        self,
        *,
        long_venue: Venue,
        short_venue: Venue,
        symbol: str,
        quantity: float,
        now_ms: int,
    ) -> tuple[float, float | None]:
        return await self.entry_dispatch_runtime._okx_aligned_entry_quantity(
            long_venue=long_venue,
            short_venue=short_venue,
            symbol=symbol,
            quantity=quantity,
            now_ms=now_ms,
        )

    async def _entry_venue_quantity_step(
        self,
        venue: Venue,
        symbol: str,
    ) -> float | None:
        return await self.entry_dispatch_runtime._entry_venue_quantity_step(
            venue,
            symbol,
        )

    async def _entry_venue_quantity_metadata(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[float | None, list[str]]:
        return await self.entry_dispatch_runtime._entry_venue_quantity_metadata(
            venue,
            symbol,
        )

    def _entry_quote_lease_execution_check(
        self,
        candidate,
        now_ms: int,
    ) -> tuple[str, object | None, dict]:
        return self.entry_dispatch_runtime._entry_quote_lease_execution_check(
            candidate,
            now_ms,
        )

    def _refresh_entry_quote_lease_for_execution(
        self,
        candidate,
        now_ms: int,
        quote_lease_reason: str,
        quote_lease: object | None,
        quote_lease_evidence: dict,
    ) -> tuple[str, object | None, dict]:
        return self.entry_dispatch_runtime._refresh_entry_quote_lease_for_execution(
            candidate,
            now_ms,
            quote_lease_reason,
            quote_lease,
            quote_lease_evidence,
        )

    @staticmethod
    def _quote_lease_reference_price(lease) -> float:
        return EntryDispatchRuntime._quote_lease_reference_price(lease)

    def _entry_final_gate_skew_blocker(
        self,
        candidate,
        *,
        long_venue,
        short_venue,
        now_ms: int,
    ) -> dict | None:
        return self.entry_dispatch_runtime._entry_final_gate_skew_blocker(
            candidate,
            long_venue=long_venue,
            short_venue=short_venue,
            now_ms=now_ms,
        )

    async def _dispatch_entry(
        self,
        candidate,
        now_ms: int,
        price_hint: float = 0.0,
    ) -> bool:
        return await self.entry_dispatch_runtime._dispatch_entry(
            candidate,
            now_ms,
            price_hint=price_hint,
        )

    # ------------------------------------------------------------------
    # Passive close recovery (V1: recovery after restart)
    # ------------------------------------------------------------------

    async def _recover_passive_closes(self) -> None:
        """Probe and recover pending passive closes after restart.

        V1: On recovery, restored PendingPassiveClose records are probed
        for live flatness. Flat positions are cleared; still-open positions
        resume passive maintenance.
        """
        if self.passive_close_executor is None:
            return
        if not self.state.pending_passive_closes:
            return

        for position_id in list(self.state.pending_passive_closes.keys()):
            result = await self.passive_close_executor.recover_passive_close(
                self.state,
                position_id,
                self._venue_adapters,
            )
            self.journal.append(
                "runtime.passive_close_recovery_result",
                {
                    "position_id": position_id,
                    "result": result,
                },
            )

    # ------------------------------------------------------------------
    # Passive close lane (V1: process_pending_passive_closes)
    # ------------------------------------------------------------------

    def _arm_overdue_passive_close_fallbacks(self, now_ms: int) -> None:
        """Escalate pending passive closes that are past the V1 force deadline."""
        if not self.state.pending_passive_closes:
            return

        from lightfee.engine.exit_decision import passive_close_fallback_due
        from lightfee.engine.state import PassiveExecutionPhase

        for position_id, pending in list(self.state.pending_passive_closes.items()):
            position = self.state.open_positions.get(position_id) or getattr(
                pending, "position_snapshot", None
            )
            if position is None:
                continue
            if not passive_close_fallback_due(position, self.config.strategy, now_ms):
                continue

            phase_state = getattr(pending, "phase_state", None)
            previous_phase = getattr(getattr(phase_state, "phase", None), "value", "")
            previous_retry_at_ms = int(getattr(pending, "next_retry_at_ms", 0) or 0)

            if phase_state is not None:
                phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                phase_state.phase_started_at_ms = now_ms
                phase_state.cycle_started_at_ms = now_ms
            pending.next_retry_at_ms = 0

            if previous_phase != PassiveExecutionPhase.DUAL_TAKER.value:
                self.journal.append(
                    "runtime.passive_close_deadline_fallback_armed",
                    {
                        "position_id": position_id,
                        "symbol": position.symbol,
                        "reason": getattr(pending, "reason", ""),
                        "opportunity_type": position.opportunity_type,
                        "funding_timestamp_ms": position.funding_timestamp_ms,
                        "second_funding_timestamp_ms": position.second_funding_timestamp_ms,
                        "exit_after_first_stage": position.exit_after_first_stage,
                        "previous_phase": previous_phase,
                        "previous_next_retry_at_ms": previous_retry_at_ms,
                        "ts_ms": now_ms,
                    },
                )

    async def _maybe_tick_passive_close(self, now_ms: int) -> None:
        """Drive pending passive closes each tick.

        V1: process_pending_passive_closes() in exit.rs line 2987.
        Runs after local-L2 sync so repricing has fresh book state.
        """
        if self.passive_close_executor is None:
            return
        if not self.state.pending_passive_closes:
            return

        self._arm_overdue_passive_close_fallbacks(now_ms)

        try:
            await self.passive_close_executor.process_pending_passive_closes(
                self.state, now_ms,
            )
            if (
                not self.state.pending_passive_closes
                and self.state.recovery_blocked_reason
            ):
                core_decision = V1RecoveryDecisionCore().decide(
                    RecoveryEvidenceSnapshot(
                        local_open_positions=tuple(
                            self._recovery_state_collection("open_positions")
                        ),
                        pending_entries=tuple(
                            self._recovery_state_collection("pending_entries")
                        ),
                        residual_repairs=tuple(
                            self._recovery_state_collection(
                                "pending_residual_repairs"
                            )
                        ),
                        passive_closes=tuple(
                            self._recovery_state_collection(
                                "pending_passive_closes"
                            )
                        ),
                        exchange_truth=None,
                        prior_recovery_block_reason=(
                            self.state.recovery_blocked_reason
                        ),
                        operator_fail_closed=(
                            self.state.operator.requested_mode
                            == GlobalRiskMode.FAIL_CLOSED
                        ),
                    )
                )
                self.recovery_decision = core_decision
                clear_legacy_recovery_block_via_core(
                    self.state,
                    core_decision,
                    journal=self.journal,
                )
        except Exception as e:
            self.journal.append(
                "runtime.passive_close_tick_error",
                {"error": str(e), "ts_ms": now_ms},
            )

    # ------------------------------------------------------------------
    # Normal exit lane (V1: standard_close_reason → passive/aggressive)
    # ------------------------------------------------------------------

    async def _maybe_process_normal_exits(self, now_ms: int) -> None:
        return await self.close_runtime._maybe_process_normal_exits(now_ms)

    def _resolve_ws_bbo_close_mid(self, venue_value: str, symbol: str, now_ms: int) -> float:
        return self.close_runtime._resolve_ws_bbo_close_mid(venue_value, symbol, now_ms)

    def _resolve_local_l2_mid(self, venue, symbol: str, now_ms: int | None = None) -> float:
        return self.close_runtime._resolve_local_l2_mid(venue, symbol, now_ms=now_ms)

    def _resolve_local_l2_quote(self, venue, symbol: str) -> tuple[float, float] | None:
        return self.close_runtime._resolve_local_l2_quote(venue, symbol)

    def _resolve_ws_bbo_close_quote(
        self,
        venue,
        symbol: str,
        now_ms: int | None = None,
    ) -> tuple[float, float] | None:
        return self.close_runtime._resolve_ws_bbo_close_quote(
            venue,
            symbol,
            now_ms=now_ms,
        )

    def _resolve_close_price_hint_mid_with_source(self, venue, symbol: str):
        return self.close_runtime._resolve_close_price_hint_mid_with_source(venue, symbol)

    def _resolve_close_price_hint_quote_with_source(self, venue, symbol: str):
        return self.close_runtime._resolve_close_price_hint_quote_with_source(venue, symbol)

    def _apply_tick_backoff(self, is_active: bool = False, is_maker: bool = False) -> None:
        """Apply incremental tick-failure backoff from config floors / caps.

        V1: separate FailureBackoff per lane with unique jitter seeds:
        - full tick: seed 0x1F7A_11FE
        - active tick: seed 0x1F7A_11FF
        - maker tick: seed 0x1F7A_1200
        """
        init_ms = self.config.runtime.tick_failure_backoff_initial_ms
        max_ms = self.config.runtime.tick_failure_backoff_max_ms

        if is_maker:
            current = self._maker_tick_backoff_until_ms
        elif is_active:
            current = self._active_tick_backoff_until_ms
        else:
            current = self._tick_backoff_until_ms

        now_ms = wall_clock_now_ms()
        base_backoff = max(init_ms, (current - now_ms) * 2 if current and current > now_ms else init_ms)
        deadline_ms = now_ms + min(base_backoff, max_ms)

        if is_maker:
            self._maker_tick_backoff_until_ms = deadline_ms
        elif is_active:
            self._active_tick_backoff_until_ms = deadline_ms
        else:
            self._tick_backoff_until_ms = deadline_ms
