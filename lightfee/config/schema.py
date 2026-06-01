"""Configuration schema matching V1 Rust config shape (Chillybot-free)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# V1: src/runtime_state/config.rs  DailyUniverseConfig.generate_time_local
_GENERATE_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
ENTRY_READINESS_PROVIDERS = (
    "local_l2",
    "rest_top_book",
    "quote_lease",
    "ws_top_book",
    "ws_bbo_quote_lease",
)


def _is_valid_generate_time(s: str) -> bool:
    if not _GENERATE_TIME_RE.match(s):
        return False
    parts = s.split(":")
    h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
    return 0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59


@dataclass
class DirectedPairConfig:
    """V1 DirectedPairConfig: restricts pair direction independently from global symbols.

    When symbols is empty, all global symbols are allowed for this direction.
    V1 anchor: src/runtime_state/config.rs  DirectedPairConfig
    """

    long: str = ""
    short: str = ""
    symbols: list[str] = field(default_factory=list)


@dataclass
class DailyUniverseConfig:
    """V1 DailyUniverseConfig: daily symbol universe with fallback-to-last-good.

    V1 anchor: src/runtime_state/config.rs  DailyUniverseConfig
    """

    enabled: bool = False
    generate_time_local: str = "08:00:00"
    max_symbols: int = 128
    fallback_to_last_good: bool = True
    path: str = ""


@dataclass
class RuntimeConfig:
    mode: str = "paper"
    opportunity_input_mode: str = "coarse_sidecar"
    # V1 directed_pairs: pair direction restriction (CONFIG-001)
    directed_pairs: list[DirectedPairConfig] = field(default_factory=list)
    # V1 daily_universe: generated symbol universe (CONFIG-002)
    daily_universe: DailyUniverseConfig = field(default_factory=DailyUniverseConfig)
    sidecar_snapshot_path: str = "runtime/opportunity-input-snapshot.json"
    sidecar_snapshot_max_age_ms: int = 10000
    sidecar_refresh_ms: int = 3000
    sidecar_perp_liquidity_budget_ms: int = 30000
    sidecar_funding_timeout_s: float = 30.0
    sidecar_liquidity_timeout_s: float = 10.0
    sidecar_transfer_timeout_s: float = 5.0
    sidecar_hint_budget_ms: int = 500
    sidecar_transfer_budget_ms: int = 1000
    live_scan_last_good_max_age_ms: int = 600000
    live_scan_recovery_success_count: int = 3
    live_scan_revalidate_edge_buffer_bps: float = 2.0
    poll_interval_ms: int = 3000
    max_market_age_ms: int = 3000
    private_position_max_age_ms: int = 15000
    live_recovery_rest_probe_timeout_ms: int = 2000
    max_order_quote_age_ms: int = 6000
    uncertain_order_cooldown_ms: int = 30000
    transfer_outage_warn_ms: int = 120000
    transfer_backup_source_after_ms: int = 180000
    transfer_outage_grace_ms: int = 600000
    tick_failure_backoff_initial_ms: int = 1000
    tick_failure_backoff_max_ms: int = 30000
    ws_reconnect_initial_ms: int = 1000
    ws_reconnect_max_ms: int = 30000
    ws_unhealthy_after_failures: int = 5
    journal_async_queue_capacity: int = 4096
    auto_trade_enabled: bool = True
    live_startup_phase_timeout_ms: int = 15000
    exchange_http_timeout_ms: int = 12000
    transfer_status_cache_ms: int = 15000
    debug_journal_diagnostics_enabled: bool = False
    maker_event_lane_enabled: bool = False
    maker_event_lane_min_wake_interval_ms: int = 15000
    shutdown_grace_period_ms: int = 3000


@dataclass
class StrategyConfig:
    entry_window_secs: int = 300
    entry_local_l2_prewarm_window_secs: int = 480
    post_funding_hold_secs: int = 0
    entry_notional_cap_quote: float = 1000.0
    live_entry_notional_cap_quote: float = 30.0
    min_entry_leg_notional_quote: float = 8.0
    max_concurrent_positions: int = 2
    max_single_venue_exposure_quote: float = 200.0
    max_symbol_exposure_quote: float = 100.0
    max_global_net_exposure_quote: float = 0.0
    max_concurrent_venue_pairs: int = 0
    max_strategy_bucket_exposure_quote: float = 0.0
    entry_candidate_batch_quiet_window_secs: int = 0
    entry_candidate_batch_freeze_minutes_before_funding: int = 0
    max_scan_minutes_before_funding: int = 25
    min_scan_minutes_before_funding: int = 5
    max_stagger_gap_minutes: int = 480
    max_top_book_usage_ratio: float = 0.85
    staggered_exit_mode: str = "after_first_stage"
    min_funding_edge_bps: float = 6.0
    min_expected_edge_bps: float = 1.0
    min_worst_case_edge_bps: float = 0.0
    small_test_max_entry_notional_quote: float = 60.0
    small_test_expected_edge_relaxation_bps: float = 0.5
    small_test_worst_case_edge_relaxation_bps: float = 0.5
    small_test_max_entry_slippage_relaxation_bps: float = 5.0
    entry_exit_reserve_bps: float = 3.0
    normal_close_slippage_limit_bps: float = 3.0
    execution_buffer_bps: float = 2.0
    capital_buffer_bps: float = 1.0
    transfer_healthy_bias_bps: float = 0.25
    transfer_unknown_bias_bps: float = 0.0
    transfer_degraded_bias_bps: float = -0.5
    profit_take_quote: float = 30.0
    net_stop_loss_quote: float = 20.0
    mark_price_delta_hard_stop_quote: float = 20.0
    trailing_drawdown_quote: float = 10.0
    settlement_remainder_close_delay_secs: int = 300
    settlement_force_close_delay_secs: int = 1200
    risk_monitor_enabled: bool = False
    unsupported_risk_snapshot_behavior: str = "death_line"
    warning_line_enabled: bool = True
    warning_health_ratio: float = 3.0
    warning_pause_new_entries_enabled: bool = True
    delever_line_enabled: bool = True
    delever_health_ratio: float = 1.5
    delever_auto_execute_enabled: bool = True
    partial_delever_ratio: float = 0.2
    partial_delever_cooldown_ms: int = 30000
    max_partial_delever_steps: int = 4
    health_recovery_ratio: float = 2.0
    death_line_enabled: bool = True
    death_health_ratio: float = 1.1
    death_single_side_protection_enabled: bool = True
    max_risk_snapshot_age_ms: int = 30000
    execution_liquidity_enabled: bool = True
    entry_sizing_mode: str = "fixed_notional"
    fixed_live_entry_notional_quote: float = 50.0
    live_target_leverage: int = 4
    entry_local_l2_primary_count: int = 6  # V1 default (V2 was 8 — misaligned)
    shadow_entry_opportunity_count: int = 2  # V1 default
    maker_entry_max_reposts: int = 2
    max_liquidity_snapshot_age_ms: int = 5000
    entry_vwap_required: bool = False
    delever_vwap_required: bool = False
    max_entry_slippage_bps: float = 25.0
    max_delever_slippage_bps: float = 50.0
    maker_entry_rest_timeout_ms: int = 6000
    passive_reprice_threshold_ticks: int = 2
    passive_cancel_replace_threshold_ticks: int = 5
    passive_min_amend_interval_ms: int = 300
    passive_reprice_threshold_bps: float = 2.0
    passive_cancel_replace_threshold_bps: float = 6.0
    passive_ops_bucket_capacity: float = 8.0
    passive_ops_bucket_refill_per_sec: float = 8.0
    passive_adaptive_ladder_enabled: bool = True
    passive_queue_jump_enabled: bool = True
    passive_max_ops_per_sec: int = 8
    passive_max_consecutive_failures: int = 5
    passive_failure_cooldown_ms: int = 5000
    passive_small_fill_buffer_notional_quote: float = 25.0
    passive_small_fill_buffer_max_wait_ms: int = 1500
    maker_venue_budget_window_ms: int = 1000
    maker_venue_budget_max_ops: int = 24
    maker_venue_query_cost: int = 1
    maker_venue_submit_cost: int = 2
    maker_inventory_bias_enabled: bool = True
    maker_inventory_bias_bps_per_unit: float = 25.0
    maker_inventory_bias_max_bps: float = 25.0
    maker_phase_max_zero_fill_cycles: int = 3
    maker_cycle_retry_delays_ms: list[int] = field(default_factory=lambda: [500, 1000, 1000])
    maker_hedge_deadline_ms: int = 800
    maker_min_notional_accumulation_attempts: int = 3
    pending_entry_max_lifetime_ms: int = 30000
    pending_entry_force_abort_after_zero_fill_ms: int = 12000
    pending_entry_force_fallback_when_tradeable: bool = False
    pending_entry_phase_zero_fill_budget: int = 2
    pending_entry_force_terminal_after_ms: int = 60000
    pending_entry_hard_ceiling_ms: int = 120000
    pending_entry_zero_fill_terminal_cooldown_ms: int = 30000  # V1 default (CONTRACT RECOVERY-004)
    # Entry book readiness uses entry_local_l2_book_stale_after_ms first, then
    # local_l2_quiet_book_grace_ms/local_l2_max_age_ms when explicitly set,
    # and finally a 300s fallback so quiet HOT books do not flap.
    entry_local_l2_book_stale_after_ms: int = 0
    local_l2_quiet_book_grace_ms: int = 0
    local_l2_max_age_ms: int = 0
    local_l2_bootstrap_batch_size: int = 4
    local_l2_bootstrap_jitter_ms: int = 250
    local_l2_bootstrap_retry_backoff_ms: int = 5000
    local_l2_startup_background_enabled: bool = True
    auto_heal_enabled: bool = True
    auto_heal_cooldown_ms: int = 60000
    auto_heal_max_retries: int = 3
    hyperliquid_max_submit_ack_p95_ms: int = 1200
    hyperliquid_submit_ack_window_size: int = 5
    hyperliquid_submit_ack_min_samples: int = 3
    maker_try_window_ms: int = 1500
    maker_min_fill_ratio: float = 0.25
    maker_entry_progress_poll_ms: int = 500  # V1: 1000; V2: 500 for tighter active polling
    maker_initial_slice_ratio: float = 0.5
    entry_max_initial_clip_ratio: float = 0.8
    maker_leg_default: str = "buy"
    entry_readiness_provider: str = "local_l2"
    entry_quote_lease_ttl_ms: int = 1500
    entry_ws_bbo_per_venue_budget: int = 4
    local_l2_enabled: bool = True
    local_l2_ws_enabled: bool = True  # WS L2 delta streaming
    bybit_local_l2_depth: int = 50
    local_l2_short_prewarm_enabled: bool = False  # V1 default (V2 was True — misaligned)
    local_l2_short_prewarm_window_secs: int = 900
    local_l2_short_prewarm_max_pairs: int = 1  # V1 default (V2 was 3 — misaligned)
    local_l2_short_prewarm_max_rank: int = 1  # V1 default (V2 was 3 — misaligned)
    local_l2_scan_assignment_lease_enabled: bool = False  # V1 default (V2 was True — misaligned)
    local_l2_scan_assignment_lease_ttl_secs: int = 90
    entry_final_gate_max_skew_ms: int = 250
    passive_unavailable_fallback_enabled: bool = False
    passive_unavailable_fallback_max_entry_notional_quote: float = 25.0
    # V1 real config knobs (gap closure)
    local_l2_global_max_books: int = 64  # V1 default: 64
    local_l2_max_books_per_venue: int = 16  # V1 default: 16
    local_l2_hot_exec_global_budget: int = 16
    local_l2_hot_exec_per_venue_budget: int = 4
    entry_min_size_round_up_whitelist: list[str] = field(default_factory=list)


    def local_l2_resource_budget(self) -> dict:
        """V1 local_l2_resource_budget(): spread max_books across active/warm/retained/topo pools.

        V1 anchor: src/runtime_state/config.rs  StrategyConfig::local_l2_resource_budget()
        """
        gmax = max(self.local_l2_global_max_books, 1)
        return {
            "max_active_books": self.local_l2_global_max_books,
            "max_active_books_per_venue": self.local_l2_max_books_per_venue,
            "reserved_hot_global": max(self.local_l2_hot_exec_global_budget, 1),
            "reserved_hot_per_venue": max(self.local_l2_hot_exec_per_venue_budget, 1),
            "warm_global": gmax,
            "warm_per_venue": self.local_l2_max_books_per_venue,
            "retained_global": gmax,
            "retained_per_venue": self.local_l2_max_books_per_venue,
            "topology_coupled_market_global": gmax,
            "topology_coupled_market_per_venue": self.local_l2_max_books_per_venue,
        }


@dataclass
class PersistenceConfig:
    event_log_path: str = "runtime/events.jsonl"
    snapshot_path: str = "runtime/state.json"
    tuning_diagnostics_enabled: bool = True
    tuning_diagnostics_max_records: int = 10000
    event_log_compaction_max_bytes: int = 134217728
    event_log_retention_hours: int = 168
    event_log_archive_count: int = 6


@dataclass
class TradeCredentials:
    api_key_env: Optional[str] = None
    api_secret_env: Optional[str] = None
    api_passphrase_env: Optional[str] = None
    wallet_private_key_env: Optional[str] = None
    account_address_env: Optional[str] = None
    wallet_mode: str = "account_wallet"
    permission_mode: str = "trade"


@dataclass
class VenuePassiveMakerConfig:
    prefer_amend_over_cancel: bool = True
    reprice_threshold_ticks: int = 2
    cancel_replace_threshold_ticks: int = 5
    min_amend_interval_ms: int = 300
    reprice_threshold_bps: float = 2.0
    cancel_replace_threshold_bps: float = 6.0
    ops_bucket_capacity: float = 8.0
    ops_bucket_refill_per_sec: float = 8.0
    adaptive_ladder_enabled: bool = True
    queue_jump_enabled: bool = True
    passive_market_refresh_max_age_ms: int = 500
    max_ops_per_sec: int = 8
    max_consecutive_failures: int = 5
    failure_cooldown_ms: int = 5000
    small_fill_buffer_notional_quote: float = 25.0
    small_fill_buffer_max_wait_ms: int = 1500
    follow_market_reprice_enabled: bool = False
    high_slippage_phase_max_zero_fill_cycles: int = 1
    low_slippage_phase_max_zero_fill_cycles: int = 1
    skip_high_slippage_symbols: list[str] = field(default_factory=list)
    force_taker_symbols: list[str] = field(default_factory=list)


@dataclass
class VenueLiveConfig:
    trade_credentials: TradeCredentials = field(default_factory=TradeCredentials)
    passive_maker: VenuePassiveMakerConfig = field(default_factory=VenuePassiveMakerConfig)
    is_testnet: bool = False
    okx_passive_px_amend_type: int = 0


@dataclass
class VenueConfig:
    venue: str = ""
    taker_fee_bps: float = 0.5
    max_notional: float = 1000.0
    market_data_file: Optional[str] = None
    live: VenueLiveConfig = field(default_factory=VenueLiveConfig)


@dataclass
class AppConfig:
    symbols: list[str] = field(default_factory=list)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    venues: list[VenueConfig] = field(default_factory=list)
