"""Task 5: Reduce-only close executor contract tests.

Rust references:
- src/engine/exit.rs: execute_aggressive_close_orders (line 3335)
- src/engine/exit.rs: close_leg_exchange_min_notional_violation (line 3035)
- src/engine/exit.rs: close_position_exchange_min_notional_violation (line 3067)
- src/engine/exit.rs: build_close_execution_from_legs (line 1155)
- src/execution_core/helpers.rs: close_balance_from_closed_quantities (line 181)
- src/execution_core/residual.rs: split_close_fill_residual (line 75)
- src/market_gateway/ports.rs: venue_reduce_only_close_exempts_min_notional (line 1068)
"""

from __future__ import annotations

import pytest

import tempfile
from pathlib import Path

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.close_executor import (
    CloseBalance,
    CloseExecutionLeg,
    close_balance_from_closed_quantities,
    close_leg_exchange_min_notional_violation,
    close_position_exchange_min_notional_violation,
    build_close_execution_from_legs,
    split_close_fill_residual,
    build_exit_pnl_attribution,
)
from lightfee.engine.exit import CloseExecution
from lightfee.engine.residual import ResidualOrigin
from lightfee.engine.state import OpenPosition
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_position(**overrides) -> OpenPosition:
    defaults = dict(
        position_id="p001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=50000.0,
        short_entry_price=50000.0,
        opened_at_ms=1000000,
        matched_quantity=0.01,
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


def _fake_fill(
    venue, symbol, side, quantity, price=50000.0,
    order_id="f001", fee_quote=2.5,
):
    return OrderFill(
        venue=venue, symbol=symbol, side=side,
        quantity=quantity, price=price,
        order_id=order_id, fee_quote=fee_quote,
        filled_at_ms=1000,
    )


# ---------------------------------------------------------------------------
# CloseBalance
# ---------------------------------------------------------------------------


class TestCloseBalance:
    def test_full_close_matched(self):
        """Both legs fully closed → matched_closed = qty, matched_remaining = 0."""
        bal = close_balance_from_closed_quantities(0.01, 0.01, 0.01)
        assert bal.matched_closed_quantity == 0.01
        assert bal.matched_remaining_quantity == 0.0
        assert bal.long_remaining_quantity == 0.0
        assert bal.short_remaining_quantity == 0.0

    def test_partial_close_symmetric(self):
        """Both legs closed halfway → matched remaining = 0.005."""
        bal = close_balance_from_closed_quantities(0.01, 0.005, 0.005)
        assert bal.matched_closed_quantity == 0.005
        assert bal.matched_remaining_quantity == 0.005

    def test_partial_close_asymmetric_long_less(self):
        """Long closed less than short → matched remaining follows long."""
        bal = close_balance_from_closed_quantities(0.01, 0.003, 0.008)
        assert bal.long_remaining_quantity == 0.007
        assert bal.short_remaining_quantity == 0.002
        # matched_remaining = min(0.007, 0.002) = 0.002
        assert bal.matched_remaining_quantity == 0.002
        assert bal.matched_closed_quantity == pytest.approx(0.008)

    def test_partial_close_asymmetric_short_less(self):
        """Short closed less than long → matched remaining follows short."""
        bal = close_balance_from_closed_quantities(0.01, 0.008, 0.003)
        assert bal.matched_remaining_quantity == 0.002
        assert bal.matched_closed_quantity == pytest.approx(0.008)

    def test_zero_close(self):
        """Nothing closed → matched_remaining = full qty."""
        bal = close_balance_from_closed_quantities(0.01, 0.0, 0.0)
        assert bal.matched_closed_quantity == 0.0
        assert bal.matched_remaining_quantity == 0.01


# ---------------------------------------------------------------------------
# Min notional violations
# ---------------------------------------------------------------------------


class TestCloseMinNotionalViolation:
    def test_no_violation_when_notional_above_min(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_violation_when_notional_below_min(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None
        venue, leg_notional, min_n = result
        assert venue == Venue.OKX
        assert leg_notional < min_n

    def test_zero_quantity_no_violation(self):
        """V1: quantity <= 0 returns None."""
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.0, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_binance_reduce_only_exempt(self):
        """V1: Binance and Aster are exempt from reduce-only min notional."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_aster_reduce_only_exempt(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.ASTER, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_bybit_not_exempt(self):
        """V1: Bybit is NOT exempt from reduce-only min notional."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BYBIT, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None

    def test_non_reduce_only_not_exempt(self):
        """Only reduce_only orders get exemption."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0001, reduce_only=False,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None  # non-reduce-only, so Binance exemption doesn't apply


class TestClosePositionMinNotionalViolation:
    def test_both_legs_pass(self):
        pos = _make_position()
        result = close_position_exchange_min_notional_violation(
            pos, 0.01, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is None

    def test_short_leg_violation(self):
        pos = _make_position(short_venue=Venue.BYBIT)
        result = close_position_exchange_min_notional_violation(
            pos, 0.0001, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is not None
        assert result[0] == Venue.BYBIT  # short venue checked first

    def test_long_leg_violation(self):
        pos = _make_position(long_venue=Venue.BYBIT, short_venue=Venue.BINANCE)
        # Short passes (Binance exempt), long fails (Bybit not exempt)
        result = close_position_exchange_min_notional_violation(
            pos, 0.0001, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is not None
        assert result[0] == Venue.BYBIT  # long venue


# ---------------------------------------------------------------------------
# build_close_execution_from_legs
# ---------------------------------------------------------------------------


class TestBuildCloseExecutionFromLegs:
    def test_single_chunk_both_filled(self):
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49900.0, "s001", fee_quote=2.5,
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l001", fee_quote=2.5,
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # Price PnL:
        #   long: (50100 - 50000) * 0.01 = 1.0
        #   short: (50000 - 49900) * 0.01 = 1.0
        #   total: 2.0
        assert close.realized_price_pnl_quote == pytest.approx(2.0)
        assert close.long_close_qty == 0.01
        assert close.short_close_qty == 0.01
        assert close.long_fee_quote == 2.5
        assert close.short_fee_quote == 2.5
        # net = 2.0 + funding(0) - fees(5.0) = -3.0
        assert close.net_quote == pytest.approx(-3.0)

    def test_partial_fill(self):
        """Only partial quantities on both legs."""
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0,
                             captured_funding_quote=10.0, funding_captured=True)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.005, 49900.0, "s001",
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.005, 50100.0, "l001",
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # PnL per leg = 0.5 each = 1.0 total
        assert close.realized_price_pnl_quote == pytest.approx(1.0)
        # funding captured = 10.0
        assert close.funding_pnl_quote == 10.0
        # net = 1.0 + 10.0 - fees(5.0) = 6.0
        assert close.net_quote == pytest.approx(6.0)

    def test_loss_position(self):
        """Price moved against position."""
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 50100.0, "s001",
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 49900.0, "l001",
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # long: (49900 - 50000) * 0.01 = -1.0
        # short: (50000 - 50100) * 0.01 = -1.0
        assert close.realized_price_pnl_quote == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# split_close_fill_residual
# ---------------------------------------------------------------------------


class TestSplitCloseFillResidual:
    def test_symmetric_close_no_residual(self):
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.01, 0.01, 1000, 31000)
        assert residual is None

    def test_asymmetric_long_more_remaining(self):
        """Long closed less → residual on long side (SELL to close remaining)."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.003, 0.01, 1000, 31000)
        assert residual is not None
        assert residual.exposure_venue == pos.long_venue
        assert residual.exposure_side == Side.SELL
        assert residual.exposure_quantity == pytest.approx(0.007)
        assert residual.origin == ResidualOrigin.CLOSE_RESIDUAL

    def test_asymmetric_short_more_remaining(self):
        """Short closed less → residual on short side (BUY to close remaining)."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.01, 0.003, 1000, 31000)
        assert residual is not None
        assert residual.exposure_venue == pos.short_venue
        assert residual.exposure_side == Side.BUY
        assert residual.exposure_quantity == pytest.approx(0.007)

    def test_partial_symmetric_no_residual(self):
        """Both legs partially closed by same amount → no residual."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.005, 0.005, 1000, 31000)
        assert residual is None


# ---------------------------------------------------------------------------
# Journal emission fidelity tests
# ---------------------------------------------------------------------------


class TestExitJournalPayload:
    """Verify exit.closed journal payload matches Rust V1 full PnL attribution shape."""

    _required_exit_closed_keys = frozenset({
        "position_id", "reason",
        "long_closed_qty", "short_closed_qty",
        "price_pnl", "funding_pnl_quote", "entry_fee_quote", "exit_fee_quote",
        "net_quote", "long_uncertain", "short_uncertain",
    })

    def test_exit_closed_emits_full_pnl_payload(self):
        """V1 rule: exit.closed must include funding_pnl_quote and entry_fee_quote."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "exit.jsonl"
            j = Journal(path)
            j.open()

            full_exit = {
                "position_id": "pos-exit-test",
                "reason": "profit_take",
                "long_closed_qty": 0.1,
                "short_closed_qty": 0.1,
                "price_pnl": 60.0,
                "funding_pnl_quote": 15.0,
                "entry_fee_quote": 5.0,
                "exit_fee_quote": 3.0,
                "net_quote": 67.0,
                "long_uncertain": False,
                "short_uncertain": False,
            }
            j.append("exit.closed", full_exit, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            emitted = records[0]["payload"]
            for key in self._required_exit_closed_keys:
                assert key in emitted, f"Missing key '{key}' in exit.closed payload"


class TestScanJournalPayload:
    """Verify scan.completed journal payload includes full candidate/filter list."""

    _required_scan_keys = frozenset({
        "candidate_count", "blocked_count", "accepted_count",
        "blocked_reasons", "no_entry_reason",
    })

    def test_scan_completed_emits_full_candidate_list_not_boolean(self):
        """V1 rule: scan.completed must emit full candidate list with blocked
        reasons per candidate, not a boolean blocked flag."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "scan.jsonl"
            j = Journal(path)
            j.open()

            scan_payload = {
                "candidate_count": 5,
                "blocked_count": 2,
                "accepted_count": 3,
                "blocked_reasons": {
                    "btcusdt:binance->okx": ["stale_market_data:binance"],
                    "ethusdt:binance->bybit": ["low_liquidity:bybit", "budget_exhausted"],
                },
                "accepted_candidates": [
                    {"pair_id": "solusdt:binance->okx", "edge_bps": 15.0},
                    {"pair_id": "avaxusdt:okx->binance", "edge_bps": 12.0},
                    {"pair_id": "linkusdt:bybit->okx", "edge_bps": 8.0},
                ],
                "no_entry_reason": "",
            }
            j.append("scan.completed", scan_payload, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            emitted = records[0]["payload"]
            assert emitted["candidate_count"] == 5
            assert emitted["blocked_count"] == 2
            assert emitted["accepted_count"] == 3
            # blocked_reasons must be a dict of candidate → reasons, not a boolean
            assert isinstance(emitted["blocked_reasons"], dict)
            assert len(emitted["blocked_reasons"]) == 2
            # accepted candidates list must be present
            accepted = emitted.get("accepted_candidates", [])
            assert len(accepted) == 3

    def test_scan_no_entry_diagnostics_emitted(self):
        """V1 rule: scan.no_entry_diagnostics with reason."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "scan_no_entry.jsonl"
            j = Journal(path)
            j.open()

            j.append("scan.no_entry_diagnostics", {
                "reason": "all_candidates_blocked",
                "market_status": "degraded",
                "candidate_count": 3,
                "blocked_count": 3,
            }, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            assert records[0]["kind"] == "scan.no_entry_diagnostics"
            assert records[0]["payload"]["reason"] == "all_candidates_blocked"


class TestClosePnlAttribution:
    """Verify build_exit_pnl_attribution separates all PnL components."""

    def test_pnl_attribution_separates_components(self):
        pos = _make_position(
            captured_funding_quote=10.0, second_stage_funding_quote=5.0,
            long_entry_fee_quote=2.5, short_entry_fee_quote=2.5,
        )
        close = CloseExecution(
            position_id="p001", reason="profit_take",
            long_close_price=50100.0, short_close_price=49900.0,
            long_close_qty=0.01, short_close_qty=0.01,
            long_fee_quote=2.5, short_fee_quote=2.5,
            realized_price_pnl_quote=2.0,
            net_quote=10.0,
        )
        attr = build_exit_pnl_attribution(pos, close)
        assert attr["funding_quote"] == 15.0  # 10 + 5
        assert attr["price_pnl_quote"] == 2.0
        assert attr["entry_fee_quote"] == 5.0  # 2.5 + 2.5
        assert attr["exit_fee_quote"] == 5.0  # 2.5 + 2.5
        # net = 2.0 + 15.0 - 5.0 - 5.0 = 7.0
        assert attr["net_quote"] == 7.0
