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
V1_ENTRY_VOLUME_FLOOR_DEFAULT_QUOTE = 1_000_000.0
V1_ENTRY_VOLUME_FLOOR_QUOTE_BY_VENUE = {
    "bitget": 2_000_000.0,
    "bybit": 2_000_000.0,
    "binance": 5_000_000.0,
    "okx": 5_000_000.0,
}
V1_ENTRY_OPEN_INTEREST_FLOOR_DEFAULT_QUOTE = 1_000_000.0


def _is_valid_generate_time(s: str) -> bool:
    if not _GENERATE_TIME_RE.match(s):
        return False
    parts = s.split(":")
    h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
    return 0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59


def _v1_entry_volume_floor_quote_by_venue() -> dict[str, float]:
    return dict(V1_ENTRY_VOLUME_FLOOR_QUOTE_BY_VENUE)


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
    sidecar_hint_budget_ms: int = 500
    spread_sidecar_snapshot_path: str = "runtime/spread-opportunities-current.json"
    # Bounded signed-basis state survives a process restart.  A missing,
    # malformed, stale or wrong-epoch checkpoint deliberately cold-starts
    # instead of borrowing statistics from another model.
    spread_stats_checkpoint_path: str = "runtime/spread-stats-v2-checkpoint.json"
    # Dynamic funding basis-risk evidence is persisted independently from the
    # sidecar snapshot.  It is an entry-admission input, so a missing or stale
    # checkpoint cold-starts rather than silently reusing an arbitrary price
    # history after a restart.
    funding_basis_risk_checkpoint_path: str = (
        "runtime/funding-basis-risk-v1-checkpoint.json"
    )
    # Account-scoped maker/taker schedules are short-lived evidence, distinct
    # from static configuration defaults.  Live canaries and official paper
    # cohorts consume this file fail-closed; shadow diagnostics may still use
    # the static conservative fee floor when it is absent.
    fee_evidence_path: str = "runtime/account-fee-evidence.json"
    # A seven-day fee snapshot cannot safely authorize a small live canary.
    # Schema-v3 evidence uses a code-owned fixed HMAC environment name.  This
    # legacy field is retained only to parse old TOML and must be blank (or
    # the exact fixed name) for live canary validation.
    fee_evidence_max_age_ms: int = 24 * 60 * 60 * 1000
    fee_evidence_integrity_key_env: str = ""
    # Non-sensitive SHA256(account/subaccount UID) binding per venue.  The
    # signed fee schedule must match this configured trading-account identity.
    fee_evidence_account_identity_hashes: dict[str, str] = field(
        default_factory=dict
    )
    spread_sidecar_refresh_ms: int = 1000
    spread_sidecar_fetch_timeout_s: float = 10.0
    spread_sidecar_source_mode: str = "sidecar_snapshot"
    spread_sidecar_direct_fetch_enabled: bool = False
    live_scan_last_good_max_age_ms: int = 600000
    live_scan_recovery_success_count: int = 3
    live_scan_revalidate_edge_buffer_bps: float = 2.0
    poll_interval_ms: int = 3000
    max_market_age_ms: int = 3000
    private_position_max_age_ms: int = 15000
    live_recovery_rest_probe_timeout_ms: int = 2000
    max_order_quote_age_ms: int = 6000
    # One-way, bounded IPC from the live Local-L2 runtime to the public
    # sidecar.  It reuses already-held books and never creates a second public
    # depth-fetch path for spread paper execution.
    local_l2_depth_bridge_enabled: bool = True
    local_l2_depth_bridge_path: str = "runtime/local-l2-depth-current.json"
    local_l2_depth_bridge_max_levels: int = 20
    local_l2_depth_bridge_publish_interval_ms: int = 500
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
    maker_event_lane_enabled: bool = True
    maker_event_lane_min_wake_interval_ms: int = 40
    shutdown_grace_period_ms: int = 3000


@dataclass
class StrategyConfig:
    entry_window_secs: int = 300
    entry_local_l2_prewarm_window_secs: int = 480
    entry_min_first_funding_remaining_secs: int = 60
    post_funding_hold_secs: int = 0
    entry_notional_cap_quote: float = 1000.0
    live_entry_notional_cap_quote: float = 50.0
    min_entry_leg_notional_quote: float = 8.0
    max_concurrent_positions: int = 8
    max_concurrent_positions_per_venue: int = 2
    max_concurrent_positions_per_symbol: int = 1
    max_concurrent_positions_per_venue_pair: int = 2
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
    # Funding entry safety and economics. Existing positions, pending hedges,
    # residual repair and close/recovery paths intentionally ignore this flag.
    # New funding positions are opt-in.  Existing positions, pending hedges,
    # recovery and close/reconciliation paths are deliberately independent of
    # this switch, so a missing config key can never strand an existing leg.
    funding_new_entries_enabled: bool = False
    # A funding canary is an additional admission layer for new positions
    # only.  It cannot interfere with pending hedges, recovery or closing an
    # existing live position.
    funding_canary_enabled: bool = False
    funding_canary_allowed_venues: list[str] = field(
        default_factory=lambda: [
            "aster",
            "binance",
            "bitget",
            "bybit",
            "gate",
            "hyperliquid",
            "okx",
        ]
    )
    funding_canary_max_concurrent_positions: int = 8
    funding_canary_max_entry_notional_quote: float = 50.0
    funding_canary_min_expected_net_edge_bps: float = 3.0
    funding_canary_min_worst_case_edge_bps: float = 0.0
    funding_canary_min_expected_net_edge_bps_by_venue_pair: dict[str, float] = field(
        default_factory=dict
    )
    funding_canary_min_worst_case_edge_bps_by_venue_pair: dict[str, float] = field(
        default_factory=dict
    )
    # Strict mode requires signed account-specific evidence for both legs.
    # When false, a pair without that evidence may only use the smaller
    # conservative tier below, priced from configured fee upper bounds.
    funding_canary_require_account_fee_evidence: bool = False
    funding_canary_conservative_fee_max_entry_notional_quote: float = 15.0
    funding_canary_conservative_fee_buffer_bps: float = 2.0
    funding_economics_mode: str = "v1_exact"
    funding_forecast_mode: str = "shadow"
    funding_forecast_uncertainty_haircut_bps: float = 2.0
    # A 7-day calibration window holds at most 21 independent settlement
    # errors for a conventional 8-hour perp.  Keep the default below that
    # physical ceiling; the separate 7-day shadow gate still prevents an
    # early enhanced-live rollout.
    funding_forecast_min_samples: int = 18
    funding_forecast_shadow_min_days: int = 7
    # A calibrated forecast must have a stable realised-error distribution,
    # not merely enough observations.  We compare the median and p90 absolute
    # error in the older and newer halves of the bounded calibration window.
    funding_forecast_stability_max_quantile_drift_bps: float = 2.0
    funding_venue_risk_haircut_bps_by_venue: dict[str, float] = field(
        default_factory=dict
    )
    funding_missing_margin_fallback_notional_quote: float = 15.0
    funding_risk_health_buffer_ratio: float = 0.5
    # Portfolio limits apply to a common-base paired position. Zero keeps the
    # optional enhanced limit disabled; the existing venue/symbol caps remain
    # mandatory admission limits in live mode.
    funding_max_venue_pair_exposure_quote: float = 0.0
    funding_max_global_gross_exposure_quote: float = 0.0
    funding_max_settlement_bucket_exposure_quote: float = 0.0
    funding_settlement_crowding_bucket_ms: int = 300_000
    funding_max_correlation_group_exposure_quote: float = 0.0
    funding_correlation_group_by_symbol: dict[str, str] = field(
        default_factory=dict
    )
    funding_expected_shortfall_bps: float = 0.0
    funding_expected_shortfall_budget_quote: float = 0.0
    funding_es_cold_start_max_entry_notional_quote: float = 15.0
    funding_es_cold_start_bps: float = 100.0
    # Historical Expected Shortfall of the *paired basis*, not outright asset
    # volatility.  These defaults collect bounded shadow evidence while the
    # entry freeze is in force.  A live funding entry may only be enabled once
    # this model and its positive capital budget are explicitly configured.
    funding_dynamic_expected_shortfall_enabled: bool = True
    funding_dynamic_expected_shortfall_window_ms: int = 21_600_000
    funding_dynamic_expected_shortfall_max_samples: int = 7_200
    funding_dynamic_expected_shortfall_max_pairs: int = 1_024
    funding_dynamic_expected_shortfall_horizon_ms: int = 60_000
    funding_dynamic_expected_shortfall_min_samples: int = 120
    funding_dynamic_expected_shortfall_min_history_ms: int = 300_000
    funding_dynamic_expected_shortfall_confidence: float = 0.95
    funding_dynamic_expected_shortfall_quote_skew_ms: int = 250
    funding_dynamic_expected_shortfall_checkpoint_max_age_ms: int = 21_600_000
    funding_dynamic_expected_shortfall_checkpoint_publish_interval_ms: int = 60_000
    spread_reversion_enabled: bool = False
    # This remains an independent paper-only strategy. The explicit live gate
    # stays false even when signal generation or paper execution is enabled.
    spread_live_enabled: bool = False
    spread_model_epoch: str = "v2_signed_reversion"
    spread_live_notional_quote: float = 20.0
    spread_max_gross_quote: float = 50.0
    spread_max_concurrent_positions: int = 1
    spread_min_samples: int = 120
    spread_entry_z: float = 2.0
    spread_exit_z: float = 0.5
    spread_stop_z: float = 3.5
    spread_min_net_edge_bps: float = 5.0
    spread_signal_ttl_ms: int = 1000
    spread_quote_skew_ms: int = 250
    spread_max_hold_ms: int = 1800000
    spread_slippage_reserve_bps: float = 2.0
    spread_adverse_selection_buffer_bps: float = 1.0
    spread_expected_hold_ms: int = 1800000
    spread_fair_price_max_venue_premium_bps: float = 150.0
    spread_fair_price_min_venues: int = 3
    spread_min_fair_price_confidence: float = 1.0
    spread_single_venue_dislocation_enabled: bool = False
    spread_single_venue_dislocation_min_anchor_venues: int = 3
    spread_min_liquidity_capacity_ratio: float = 1.25
    spread_min_history_ms: int = 300000
    spread_min_executable_spread_bps: float = 50.0
    spread_max_executable_spread_bps: float = 300.0
    # The v3 research epoch may replace the absolute spread floor with a
    # cost-normalized threshold.  Disabled by default so the historical v2
    # cohort remains comparable.
    spread_dynamic_net_edge_enabled: bool = False
    spread_min_gross_profit_multiple: float = 1.0
    spread_min_profit_buffer_bps: float = 5.0
    spread_rank_by_capital_efficiency: bool = False
    spread_volatility_high_std_bps: float = 10.0
    spread_mean_reversion_min_std_bps: float = 0.05
    spread_mean_reversion_max_half_life_ms: int = 1800000
    spread_stats_window_ms: int = 21600000
    spread_stats_max_samples: int = 7200
    spread_stats_short_window_ms: int = 900000
    spread_structural_break_sigma: float = 3.0
    spread_structural_break_consecutive: int = 5
    spread_structural_break_cooldown_ms: int = 1800000
    spread_ranker_max_candidates: int = 10
    spread_score_liquidity_weight: float = 8.0
    spread_score_z_cap: float = 5.0
    spread_liquidity_small_quote: float = 50.0
    spread_liquidity_medium_quote: float = 100.0
    spread_liquidity_large_quote: float = 500.0
    spread_liquidity_small_penalty_bps: float = 60.0
    spread_liquidity_medium_penalty_bps: float = 30.0
    spread_liquidity_sublarge_penalty_bps: float = 10.0
    spread_paper_enabled: bool = False
    spread_paper_model_epoch: str = "v2_signed_reversion"
    spread_paper_primary_fill_model: str = "taker_taker"
    spread_paper_require_taker_taker: bool = True
    spread_paper_finalist_limit: int = 10
    # A signal snapshot is a decision input, not executable fill evidence.
    # Paper entry requires a later coherent quote after this positive delay.
    spread_paper_min_decision_latency_ms: int = 250
    # Symbol exclusions are an explicit research choice, never a hidden
    # hard-coded good/bad pair in the strategy or its reports.
    spread_paper_excluded_symbols: list[str] = field(default_factory=list)
    spread_paper_allowed_opportunity_labels: list[str] = field(
        default_factory=lambda: ["spread_reversion"]
    )
    spread_paper_episode_cooldown_ms: int = 1_800_000
    spread_paper_bot_ids: list[str] = field(
        default_factory=lambda: ["tt_conservative"]
    )
    spread_paper_research_manifest_path: str = (
        "config/research/spread_v2_signed_reversion.json"
    )
    spread_paper_markout_secs: list[int] = field(
        default_factory=lambda: [60, 300, 900, 1800]
    )
    spread_paper_terminal_secs: int = 1800
    spread_paper_slippage_buffer_bps: float = 0.0
    # Apply an explicit latency/adverse-selection reserve in addition to the
    # later-quote execution model.  It is charged on every paper entry/exit
    # leg and is journalled separately from raw book VWAP.
    spread_paper_latency_buffer_bps: float = 0.5
    # Official paper results require executable L2 on both entry and exit
    # legs as well as account-scoped fee evidence.  Turning either off keeps
    # the tracker useful for diagnostics but suppresses official PnL.
    spread_paper_require_l2_vwap: bool = True
    spread_paper_require_account_fee_evidence: bool = True
    # Maker rebates can improve paper economics but are admitted only when a
    # signed private-account evidence document explicitly records them.
    spread_allow_verified_maker_rebates: bool = False
    # A deterministic cutover labels every paper observation.  Zero preserves
    # current in-sample diagnostics; non-zero is required for an OOS-only
    # acceptance run.
    spread_paper_oos_start_ms: int = 0
    spread_paper_require_out_of_sample: bool = False
    entry_exit_reserve_bps: float = 3.0
    normal_close_slippage_limit_bps: float = 3.0
    exit_shadow_enabled: bool = False
    exit_shadow_markout_horizons_ms: list[int] = field(
        default_factory=lambda: [1000, 2000, 5000]
    )
    exit_shadow_take_profit_bps: list[float] = field(
        default_factory=lambda: [10.0, 20.0]
    )
    exit_shadow_adverse_stop_bps: float = 3.0
    exit_shadow_max_quote_age_ms: int = 1000
    exit_shadow_max_l2_age_ms: int = 1000
    exit_shadow_cost_buffer_bps: float = 3.0
    execution_buffer_bps: float = 2.0
    capital_buffer_bps: float = 1.0
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
    maker_entry_reconcile_backoff_ms: int = 1000
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
    maker_hedge_soft_deadline_ms: int = 800
    maker_hedge_deadline_ms: int = 800
    maker_min_notional_accumulation_attempts: int = 3
    pending_entry_max_lifetime_ms: int = 30000
    pending_entry_force_abort_after_zero_fill_ms: int = 12000
    pending_entry_force_fallback_when_tradeable: bool = False
    pending_entry_pre_submit_hedgeable_fill_guard_enabled: bool = True
    pending_entry_phase_zero_fill_budget: int = 2
    pending_entry_force_terminal_after_ms: int = 60000
    pending_entry_hard_ceiling_ms: int = 120000
    candidate_lease_ms: int = 60000
    selected_submit_deadline_ms: int = 15000
    maker_resting_soft_ms: int = 30000
    maker_resting_hard_ms: int = 60000
    entry_selected_warning_ms: int = 120000
    entry_selected_terminal_sla_ms: int = 300000
    close_terminal_soft_ms: int = 60000
    close_terminal_hard_ms: int = 300000
    recovery_terminal_hard_ms: int = 300000
    unpaired_live_position_auto_recovery_enabled: bool = False
    unpaired_live_position_max_notional_quote: float = 0.0
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
    entry_quote_prewarm_extra_candidate_count: int = 24
    entry_ws_bbo_per_venue_budget: int = 10
    entry_volume_floor_default_quote: float = V1_ENTRY_VOLUME_FLOOR_DEFAULT_QUOTE
    entry_volume_floor_quote_by_venue: dict[str, float] = field(
        default_factory=_v1_entry_volume_floor_quote_by_venue
    )
    entry_open_interest_floor_default_quote: float = V1_ENTRY_OPEN_INTEREST_FLOOR_DEFAULT_QUOTE
    entry_open_interest_floor_quote_by_venue: dict[str, float] = field(default_factory=dict)
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

    @staticmethod
    def _venue_key(venue) -> str:
        if hasattr(venue, "value"):
            venue = venue.value
        return str(venue or "").lower()

    def entry_volume_floor_quote(self, venue) -> float:
        return float(
            self.entry_volume_floor_quote_by_venue.get(
                self._venue_key(venue),
                self.entry_volume_floor_default_quote,
            )
        )

    def entry_open_interest_floor_quote(self, venue) -> float:
        return float(
            self.entry_open_interest_floor_quote_by_venue.get(
                self._venue_key(venue),
                self.entry_open_interest_floor_default_quote,
            )
        )


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
    spread_paper_event_log_path: str = "runtime/spread-paper-events.jsonl"
    # Must be an absolute path on an independently retained failure domain
    # (for example, a separately snapshotted/mounted state volume).  Spread
    # paper admission fails closed when this is blank or shares the journal
    # directory, because journal+head rollback is otherwise undetectable.
    spread_paper_rollback_anchor_path: str = ""
    spread_paper_event_log_hard_max_bytes: int = 67_108_864
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
    wallet_mode_env: Optional[str] = None
    wallet_mode: str = "account_wallet"
    allow_api_wallet_authorization_probe: bool = False
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
    maker_fee_bps: Optional[float] = None
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
