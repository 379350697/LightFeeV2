"""Tests for Tasks 23-27: daily universe, market data, sidecar lifecycle, CLI, config compat."""

import json
import os
import tempfile

import pytest

from lightfee.config.compatibility import apply_legacy_aliases, LEGACY_FIELD_ALIASES
from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
    PriceLevel,
    promote_warm_to_hot,
)
from lightfee.marketdata.liquidity import (
    ExecutionLiquiditySnapshot,
    LiquidityLevel,
    chunked_l2_close_capacity,
)
from lightfee.marketdata.local_book import (
    count_by_status,
    get_active_books,
    get_books_by_pool,
    get_stale_books,
    get_unhealthy_books,
)
from lightfee.sidecar.lifecycle import (
    DomainLifecycle,
    DomainStatus,
    SidecarLifecycleState,
    create_domain_lifecycle,
)
from lightfee.strategy.universe import (
    PersistedDailyUniverse,
    RuntimeSymbolResolutionSummary,
    today_trading_date,
)


class TestDailyUniverse:
    def test_trading_date_format(self):
        d = today_trading_date()
        assert len(d) == 10
        assert "-" in d

    def test_persisted_universe_validate_ok(self):
        u = PersistedDailyUniverse(
            trading_date="2026-05-10",
            generated_at_ms=1000000,
            source_symbol_count=3,
            selected_symbol_count=2,
            selected_symbols=["BTCUSDT", "ETHUSDT"],
        )
        assert u.validate() == []

    def test_persisted_universe_count_mismatch(self):
        u = PersistedDailyUniverse(
            trading_date="2026-05-10",
            generated_at_ms=1000000,
            source_symbol_count=1,
            selected_symbol_count=5,
            selected_symbols=["A"],
        )
        assert len(u.validate()) > 0

    def test_persisted_universe_duplicate_symbols(self):
        u = PersistedDailyUniverse(
            trading_date="2026-05-10",
            generated_at_ms=1000000,
            source_symbol_count=2,
            selected_symbol_count=2,
            selected_symbols=["BTCUSDT", "BTC-USDT"],
        )
        # After normalization, BTCUSDT == BTCUSDT → duplicate
        assert len(u.validate()) > 0

    def test_save_and_load_roundtrip(self):
        u = PersistedDailyUniverse(
            trading_date="2026-05-10",
            generated_at_ms=1000000,
            source_symbol_count=3,
            selected_symbol_count=2,
            selected_symbols=["BTCUSDT", "ETHUSDT"],
        )
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmp = f.name
        try:
            u.save(tmp)
            loaded = PersistedDailyUniverse.load(tmp)
            assert loaded is not None
            assert loaded.trading_date == "2026-05-10"
            assert loaded.selected_symbols == ["BTCUSDT", "ETHUSDT"]
        finally:
            os.unlink(tmp)

    def test_load_missing_returns_none(self):
        assert PersistedDailyUniverse.load("/nonexistent/path.json") is None

    def test_resolution_summary_defaults(self):
        s = RuntimeSymbolResolutionSummary()
        assert s.daily_universe_enabled is False
        assert s.global_symbol_count == 0


class TestMarketDataL2:
    def test_state_machine_cold_to_bootstrapping(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        assert book.status == L2BookStatus.COLD
        book.transition_to_bootstrapping(now_ms=1000)
        assert book.status == L2BookStatus.BOOTSTRAPPING

    def test_state_machine_bootstrapping_to_hot(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.BOOTSTRAPPING)
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT

    def test_state_machine_degrade_and_rebuild(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_degraded("ws error")
        assert book.status == L2BookStatus.DEGRADED
        assert book.degrade_count == 1
        assert book.last_error == "ws error"
        book.transition_to_rebuilding()
        assert book.status == L2BookStatus.REBUILDING

    def test_is_healthy(self):
        hot = LocalL2Book(venue="a", symbol="X", status=L2BookStatus.HOT)
        assert hot.is_healthy()
        degraded = LocalL2Book(venue="b", symbol="Y", status=L2BookStatus.DEGRADED)
        assert not degraded.is_healthy()

    def test_is_stale(self):
        book = LocalL2Book(venue="a", symbol="X", observed_at_ms=1000)
        assert book.is_stale(max_age_ms=500, now_ms=2000)
        assert not book.is_stale(max_age_ms=2000, now_ms=2000)

    def test_promote_warm_to_hot(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM),
            "b": LocalL2Book(venue="a", symbol="Y", status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM),
        }
        n = promote_warm_to_hot(books, max_hot=3)
        assert n == 2
        assert books["a"].pool == L2PoolAssignment.HOT_EXEC


class TestMarketDataLiquidity:
    def test_vwap_walk_levels(self):
        snap = ExecutionLiquiditySnapshot(
            symbol="BTCUSDT",
            venue="binance",
            asks=[
                LiquidityLevel(price=50000, size=0.1),
                LiquidityLevel(price=50100, size=0.2),
            ],
        )
        filled, avg = snap.estimate_vwap_buy(target_quote=6000)
        # Level 1: 50000*0.1=5000 filled, remaining 1000
        # Level 2: take 1000/50100≈0.02 → cost=1000
        # total=6000, avg=(5000*50000+1000*50100)/6000 ≈ 50016.67
        assert filled > 0
        assert avg > 50000

    def test_slippage_positive_for_buy_above_ref(self):
        snap = ExecutionLiquiditySnapshot(
            symbol="BTCUSDT",
            venue="binance",
            asks=[LiquidityLevel(price=50500, size=0.1)],
        )
        bps = snap.buy_slippage_bps(target_quote=5000, reference_price=50000)
        assert bps > 0

    def test_max_fillable_within_slippage(self):
        snap = ExecutionLiquiditySnapshot(
            symbol="BTCUSDT",
            venue="binance",
            asks=[
                LiquidityLevel(price=50000, size=0.1),
                LiquidityLevel(price=51000, size=0.1),  # 2% away
            ],
        )
        cap = snap.max_fillable_buy(slippage_limit_bps=100)  # 1% limit
        assert cap == 5000.0  # only first level

    def test_chunked_l2_capacity(self):
        snap = ExecutionLiquiditySnapshot(
            symbol="BTCUSDT",
            venue="binance",
            bids=[LiquidityLevel(price=50000, size=0.2)],
            asks=[LiquidityLevel(price=50100, size=0.1)],
        )
        cap = chunked_l2_close_capacity(snap, 10000, 50, "sell")
        assert cap <= 10000


class TestMarketDataLocalBook:
    def test_get_active_books(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", status=L2BookStatus.HOT, observed_at_ms=5000),
            "b": LocalL2Book(venue="b", symbol="Y", status=L2BookStatus.COLD, observed_at_ms=5000),
        }
        active = get_active_books(books, max_age_ms=100, now_ms=5050)
        assert len(active) == 1

    def test_get_books_by_pool(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", pool=L2PoolAssignment.HOT_EXEC),
            "b": LocalL2Book(venue="b", symbol="Y", pool=L2PoolAssignment.WARM),
        }
        hot = get_books_by_pool(books, L2PoolAssignment.HOT_EXEC)
        assert len(hot) == 1

    def test_get_unhealthy_books(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", status=L2BookStatus.DEGRADED),
            "b": LocalL2Book(venue="b", symbol="Y", status=L2BookStatus.HOT),
        }
        unhealthy = get_unhealthy_books(books)
        assert len(unhealthy) == 1

    def test_get_stale_books(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", observed_at_ms=1000, status=L2BookStatus.HOT),
            "b": LocalL2Book(venue="b", symbol="Y", observed_at_ms=5000, status=L2BookStatus.HOT),
        }
        stale = get_stale_books(books, max_age_ms=500, now_ms=2000)
        assert len(stale) == 1

    def test_count_by_status(self):
        books = {
            "a": LocalL2Book(venue="a", symbol="X", status=L2BookStatus.HOT),
            "b": LocalL2Book(venue="b", symbol="Y", status=L2BookStatus.HOT),
            "c": LocalL2Book(venue="c", symbol="Z", status=L2BookStatus.COLD),
        }
        counts = count_by_status(books)
        assert counts.get(L2BookStatus.HOT, 0) == 2
        assert counts.get(L2BookStatus.COLD, 0) == 1


class TestSidecarLifecycle:
    def test_domain_starts_unknown(self):
        d = create_domain_lifecycle("funding")
        assert d.status == DomainStatus.UNKNOWN

    def test_domain_becomes_fresh_within_max_age(self):
        d = DomainLifecycle(domain="market", observed_at_ms=5000, venue_count=3)
        assert d.evaluate(now_ms=6000, max_age_ms=2000) == DomainStatus.FRESH

    def test_domain_becomes_stale_past_max_age(self):
        d = DomainLifecycle(domain="market", observed_at_ms=5000, venue_count=3)
        assert d.evaluate(now_ms=10000, max_age_ms=2000) == DomainStatus.STALE

    def test_lifecycle_state_all_fresh(self):
        state = SidecarLifecycleState(
            domains={
                "funding": DomainLifecycle(domain="funding", observed_at_ms=5000, venue_count=2),
                "market": DomainLifecycle(domain="market", observed_at_ms=5000, venue_count=3),
            }
        )
        assert state.all_fresh(now_ms=6000, max_age_ms=2000) is True

    def test_lifecycle_state_any_degraded(self):
        state = SidecarLifecycleState(degraded_venues=["binance"])
        assert state.any_degraded() is True

    def test_fresh_and_stale_domains(self):
        state = SidecarLifecycleState(
            domains={
                "funding": DomainLifecycle(domain="funding", observed_at_ms=5000, venue_count=2),
                "market": DomainLifecycle(domain="market", observed_at_ms=1000, venue_count=1),
            }
        )
        fresh = state.fresh_domains(now_ms=6000, max_age_ms=2000)
        stale = state.stale_domains(now_ms=6000, max_age_ms=2000)
        assert "funding" in fresh
        assert "market" in stale  # 1000 + 2000 < 6000


class TestConfigCompat:
    def test_legacy_aliases_are_rewritten(self):
        raw = {"min_funding_edge": 5.0, "tick_interval_ms": 2000, "strategy": {"max_concurrent": 4}}
        result = apply_legacy_aliases(raw)
        assert "min_funding_edge_bps" in result
        assert "poll_interval_ms" in result
        assert result["strategy"]["max_concurrent_positions"] == 4

    def test_non_legacy_keys_pass_through(self):
        raw = {"symbols": ["BTCUSDT"], "runtime": {"mode": "paper"}}
        result = apply_legacy_aliases(raw)
        assert result["symbols"] == ["BTCUSDT"]
        assert result["runtime"]["mode"] == "paper"

    def test_all_known_aliases(self):
        # Every alias maps to a non-empty canonical name
        for alias, canonical in LEGACY_FIELD_ALIASES.items():
            assert canonical, f"missing canonical for {alias}"
            assert canonical != alias, f"alias {alias} must differ from canonical"


class TestExplainCLI:
    def test_render_text(self):
        from lightfee.apps.explain import RuntimePostureReport, render_runtime_posture_text
        report = RuntimePostureReport(
            lifecycle="running",
            risk_mode="running",
            run_id="abc123",
            tick_count=42,
            open_positions=1,
        )
        text = render_runtime_posture_text(report)
        assert "running" in text
        assert "abc123" in text
        assert "42" in text

    def test_load_missing_returns_none(self):
        from lightfee.apps.explain import load_runtime_posture_report
        assert load_runtime_posture_report("/nonexistent/path.json") is None


class TestDailyDBSnapshot:
    def test_ensure_schema_creates_table(self):
        import sqlite3
        from lightfee.apps.scheduler import _ensure_schema
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            _ensure_schema(db_path)
            conn = sqlite3.connect(db_path)
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("daily_930_reports",) in tables
            conn.close()
        finally:
            os.unlink(db_path)
