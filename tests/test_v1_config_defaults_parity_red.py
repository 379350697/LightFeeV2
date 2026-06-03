"""Red test: V1 config default parity.

V1 anchor: src/engine/reliability_contract.rs:3-6 (default values)

Validates that V2 StrategyConfig defaults match V1 Rust defaults.
This test is a RED test — it verifies the alignment is correct now,
and catches any regression that would re-introduce misalignment.
"""

from lightfee.config.schema import StrategyConfig


class TestV1ConfigDefaultsParity:
    """Assert V2 default values match V1 reliability-contract defaults."""

    def test_entry_local_l2_primary_count_is_6(self):
        """V1 default: 6 (V2 was 8 — misaligned)."""
        cfg = StrategyConfig()
        assert cfg.entry_local_l2_primary_count == 6, (
            f"entry_local_l2_primary_count={cfg.entry_local_l2_primary_count}, expected 6 (V1 default)"
        )

    def test_local_l2_short_prewarm_disabled_by_default(self):
        """V1 default: false (V2 was true — misaligned)."""
        cfg = StrategyConfig()
        assert cfg.local_l2_short_prewarm_enabled is False, (
            f"local_l2_short_prewarm_enabled={cfg.local_l2_short_prewarm_enabled}, expected False (V1 default)"
        )

    def test_local_l2_short_prewarm_max_pairs_is_1(self):
        """V1 default: 1 (V2 was 3 — misaligned)."""
        cfg = StrategyConfig()
        assert cfg.local_l2_short_prewarm_max_pairs == 1, (
            f"local_l2_short_prewarm_max_pairs={cfg.local_l2_short_prewarm_max_pairs}, expected 1 (V1 default)"
        )

    def test_local_l2_short_prewarm_max_rank_is_1(self):
        """V1 default: 1 (V2 was 3 — misaligned)."""
        cfg = StrategyConfig()
        assert cfg.local_l2_short_prewarm_max_rank == 1, (
            f"local_l2_short_prewarm_max_rank={cfg.local_l2_short_prewarm_max_rank}, expected 1 (V1 default)"
        )

    def test_local_l2_scan_assignment_lease_disabled_by_default(self):
        """V1 default: false (V2 was true — misaligned)."""
        cfg = StrategyConfig()
        assert cfg.local_l2_scan_assignment_lease_enabled is False, (
            f"local_l2_scan_assignment_lease_enabled={cfg.local_l2_scan_assignment_lease_enabled}, expected False (V1 default)"
        )

    def test_pending_entry_zero_fill_terminal_cooldown_ms_is_30000(self):
        """V1 default: 30000 (was missing in V2 — field added)."""
        cfg = StrategyConfig()
        assert cfg.pending_entry_zero_fill_terminal_cooldown_ms == 30000, (
            f"pending_entry_zero_fill_terminal_cooldown_ms={cfg.pending_entry_zero_fill_terminal_cooldown_ms}, expected 30000 (V1 default)"
        )

    def test_local_l2_entry_book_stale_window_falls_back_to_runtime_default(self):
        """Production entry-L2 uses a 300s fallback unless explicitly configured."""
        cfg = StrategyConfig()
        assert cfg.entry_local_l2_book_stale_after_ms == 0
        assert cfg.local_l2_quiet_book_grace_ms == 0
        assert cfg.local_l2_max_age_ms == 0

    def test_pending_entry_hard_ceiling_defaults(self):
        """V1 reliability-contract defaults for pending entry terminalization."""
        cfg = StrategyConfig()
        assert cfg.pending_entry_phase_zero_fill_budget == 2
        assert cfg.pending_entry_force_terminal_after_ms == 60000
        assert cfg.pending_entry_hard_ceiling_ms == 120000

    def test_local_l2_resource_budget_defaults(self):
        """V1 local-L2 book budget defaults."""
        cfg = StrategyConfig()
        assert cfg.local_l2_global_max_books == 64
        assert cfg.local_l2_max_books_per_venue == 16
        assert cfg.local_l2_hot_exec_global_budget == 16
        assert cfg.local_l2_hot_exec_per_venue_budget == 4

    def test_local_l2_bootstrap_defaults(self):
        """V1 local-L2 bootstrap defaults."""
        cfg = StrategyConfig()
        assert cfg.local_l2_bootstrap_batch_size == 4
        assert cfg.local_l2_bootstrap_jitter_ms == 250
        assert cfg.local_l2_bootstrap_retry_backoff_ms == 5000
        assert cfg.local_l2_startup_background_enabled is True

    def test_entry_perp_liquidity_threshold_defaults(self):
        """V1 entry perp-liquidity hard-gate defaults."""
        cfg = StrategyConfig()
        assert cfg.entry_volume_floor_default_quote == 1_000_000.0
        assert cfg.entry_volume_floor_quote("gate") == 1_000_000.0
        assert cfg.entry_volume_floor_quote("aster") == 1_000_000.0
        assert cfg.entry_volume_floor_quote("hyperliquid") == 1_000_000.0
        assert cfg.entry_volume_floor_quote("bitget") == 2_000_000.0
        assert cfg.entry_volume_floor_quote("bybit") == 2_000_000.0
        assert cfg.entry_volume_floor_quote("binance") == 5_000_000.0
        assert cfg.entry_volume_floor_quote("okx") == 5_000_000.0
        assert cfg.entry_open_interest_floor_default_quote == 1_000_000.0
        assert cfg.entry_open_interest_floor_quote("okx") == 1_000_000.0
