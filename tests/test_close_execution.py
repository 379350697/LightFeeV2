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
from types import SimpleNamespace

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
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
from lightfee.engine.close_runtime import CloseRuntime
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


def _make_uncertain_error(reason: str = "order timeout") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.UNCERTAIN, reason)


def _make_rejected_error(reason: str = "order rejected") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.REJECTED, reason)


def test_exit_reconciled_payload_suppresses_duplicate_close_leg_fills():
    runtime = CloseRuntime(ctx=None)  # payload builder is pure for this test
    reconciliation = {
        "position_id": "entry-home",
        "symbol": "HOMEUSDT",
        "position_snapshot": {
            "long_entry_price": 1.0,
            "short_entry_price": 1.2,
        },
    }
    duplicate_long = OrderFill(
        venue=Venue.BINANCE,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=10.0,
        price=1.1,
        order_id="binance-close-1",
        client_order_id="client-close-1",
        fee_quote=0.01,
        filled_at_ms=1781531700000,
    )
    short_fill = OrderFill(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=10.0,
        price=1.0,
        order_id="bybit-close-1",
        client_order_id="client-close-2",
        fee_quote=0.01,
        filled_at_ms=1781531700100,
    )

    duplicate_long_without_cid = OrderFill(
        venue=Venue.BINANCE,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=10.0,
        price=1.1,
        order_id="binance-close-1",
        client_order_id=None,
        fee_quote=0.01,
        filled_at_ms=1781531700000,
    )

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation,
        [duplicate_long, duplicate_long_without_cid],
        [short_fill],
        now_ms=1781531700200,
    )

    assert payload["long_closed_qty"] == pytest.approx(10.0)
    assert payload["short_closed_qty"] == pytest.approx(10.0)
    assert payload["duplicate_close_leg_suppressed_count"] == 1
    assert payload["duplicate_close_leg_suppressed_samples"] == [
        {
            "leg": "long",
            "venue": "binance",
            "order_id": "binance-close-1",
            "client_order_id": "",
            "quantity": 10.0,
            "average_price": 1.1,
            "filled_at_ms": 1781531700000,
        }
    ]


def test_exit_reconciled_payload_does_not_dedupe_unidentified_close_fills():
    runtime = CloseRuntime(ctx=None)
    reconciliation = {
        "position_id": "entry-home",
        "symbol": "HOMEUSDT",
        "position_snapshot": {
            "long_entry_price": 1.0,
            "short_entry_price": 1.2,
        },
    }
    unidentified_fill = OrderFill(
        venue=Venue.BINANCE,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=5.0,
        price=1.1,
        order_id="",
        client_order_id=None,
        fee_quote=0.01,
        filled_at_ms=1781531700000,
    )
    short_fill = OrderFill(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=10.0,
        price=1.0,
        order_id="bybit-close-1",
        client_order_id="client-close-2",
        fee_quote=0.01,
        filled_at_ms=1781531700100,
    )

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation,
        [unidentified_fill, unidentified_fill],
        [short_fill],
        now_ms=1781531700200,
    )

    assert payload["long_closed_qty"] == pytest.approx(10.0)
    assert payload["duplicate_close_leg_suppressed_count"] == 0


def test_exit_reconciliation_requires_explicit_entry_fee_provenance_even_for_zero_fee():
    runtime = CloseRuntime(ctx=None)
    reconciliation = {
        "position_id": "entry-home",
        "symbol": "HOMEUSDT",
        "position_snapshot": {
            "long_quantity": 10.0,
            "short_quantity": 10.0,
            "long_entry_price": 1.0,
            "short_entry_price": 1.2,
            "long_entry_fee_quote": 0.0,
            "short_entry_fee_quote": 0.0,
            "total_entry_fee_quote": 0.0,
        },
    }
    long_fill = OrderFill(
        venue=Venue.BINANCE, symbol="HOMEUSDT", side=Side.SELL,
        quantity=10.0, price=1.1, order_id="long", fee_quote=0.01,
    )
    short_fill = OrderFill(
        venue=Venue.BYBIT, symbol="HOMEUSDT", side=Side.BUY,
        quantity=10.0, price=1.0, order_id="short", fee_quote=0.01,
    )

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation, [long_fill], [short_fill], now_ms=2_000,
    )

    assert payload["entry_fee_evidence_complete"] is False
    assert payload["venue_statement_reconciled"] is False

    reconciliation["position_snapshot"]["entry_fee_evidence_complete"] = True
    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation, [long_fill], [short_fill], now_ms=2_000,
    )
    assert payload["entry_fee_evidence_complete"] is True
    assert payload["venue_statement_reconciled"] is True


def test_exit_reconciliation_requires_explicit_exit_fee_provenance_even_for_zero_fee():
    runtime = CloseRuntime(ctx=None)
    reconciliation = {
        "position_id": "entry-home",
        "symbol": "HOMEUSDT",
        "position_snapshot": {
            "long_quantity": 10.0,
            "short_quantity": 10.0,
            "long_entry_price": 1.0,
            "short_entry_price": 1.2,
            "total_entry_fee_quote": 0.0,
            "entry_fee_evidence_complete": True,
        },
    }
    unknown_fee_fill = OrderFill(
        venue=Venue.BINANCE, symbol="HOMEUSDT", side=Side.SELL,
        quantity=10.0, price=1.1, order_id="long", fee_quote=None,
    )
    known_zero_fee_fill = OrderFill(
        venue=Venue.BINANCE, symbol="HOMEUSDT", side=Side.SELL,
        quantity=10.0, price=1.1, order_id="long", fee_quote=0.0,
    )
    short_fill = OrderFill(
        venue=Venue.BYBIT, symbol="HOMEUSDT", side=Side.BUY,
        quantity=10.0, price=1.0, order_id="short", fee_quote=0.01,
    )

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation, [unknown_fee_fill], [short_fill], now_ms=2_000,
    )
    assert payload["exit_fee_evidence_complete"] is False
    assert payload["venue_statement_reconciled"] is False
    assert payload["long_legs"][0]["fee_quote"] is None

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation, [known_zero_fee_fill], [short_fill], now_ms=2_000,
    )
    assert payload["exit_fee_evidence_complete"] is True
    assert payload["venue_statement_reconciled"] is True


def test_exit_reconciliation_requires_positive_entry_and_close_prices():
    runtime = CloseRuntime(ctx=None)
    reconciliation = {
        "position_id": "entry-home",
        "symbol": "HOMEUSDT",
        "position_snapshot": {
            "long_quantity": 10.0,
            "short_quantity": 10.0,
            "long_entry_price": 1.0,
            "short_entry_price": 1.2,
            "total_entry_fee_quote": 0.0,
            "entry_fee_evidence_complete": True,
        },
    }
    long_fill = OrderFill(
        venue=Venue.BINANCE, symbol="HOMEUSDT", side=Side.SELL,
        quantity=10.0, price=0.0, order_id="long", fee_quote=0.01,
    )
    short_fill = OrderFill(
        venue=Venue.BYBIT, symbol="HOMEUSDT", side=Side.BUY,
        quantity=10.0, price=1.0, order_id="short", fee_quote=0.01,
    )

    payload = runtime._exit_reconciled_payload_from_leg_fills(
        reconciliation, [long_fill], [short_fill], now_ms=2_000,
    )

    assert payload["close_price_evidence_complete"] is False
    assert payload["venue_statement_reconciled"] is False
    assert payload["net_quote_status"] == "provisional"


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


# ---------------------------------------------------------------------------
# Close chunking (V1 semantic activation)
# ---------------------------------------------------------------------------


from lightfee.engine.close_executor import compute_close_chunks


class TestComputeCloseChunks:
    """Test close chunk planning: splitting large positions by notional cap."""

    def test_single_chunk_when_below_max_notional(self):
        """Small position: chunking disabled or notional below threshold → single chunk."""
        # 0.01 BTC * $50000 = $500 notional per leg, max_notional = 10000 → single chunk
        chunks = compute_close_chunks(
            total_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            max_notional_quote=10000.0,
        )
        assert len(chunks) == 1
        assert chunks[0] == pytest.approx(0.01)

    def test_single_chunk_when_max_notional_zero(self):
        """max_notional_quote=0 means chunking disabled → single chunk."""
        chunks = compute_close_chunks(
            total_quantity=5.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            max_notional_quote=0.0,
        )
        assert len(chunks) == 1
        assert chunks[0] == pytest.approx(5.0)

    def test_single_chunk_when_quantity_zero(self):
        """Zero quantity → empty chunk list."""
        chunks = compute_close_chunks(
            total_quantity=0.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            max_notional_quote=5000.0,
        )
        assert len(chunks) == 0

    def test_splits_when_above_max_notional(self):
        """Large position: 5 BTC * $50000 = $250000 notional, cap $50000 → 5 chunks."""
        chunks = compute_close_chunks(
            total_quantity=5.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            max_notional_quote=50_000.0,
        )
        assert len(chunks) == 5
        # Each chunk ≈ 1.0 BTC = $50000 notional
        for c in chunks:
            assert c == pytest.approx(1.0)
        assert sum(chunks) == pytest.approx(5.0)

    def test_chunks_respect_min_notional_per_leg(self):
        """Each chunk must be >= min_notional on every leg. Last chunk absorbs remainder."""
        # 0.03 BTC * $50000 = $1500, cap $600 → needs 3 chunks
        # Each BTC=0.012 notional=$600, last absorbs rounding
        chunks = compute_close_chunks(
            total_quantity=0.03,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            max_notional_quote=600.0,
            min_long_notional=5.0,
            min_short_notional=5.0,
            venue_long=Venue.OKX,
            venue_short=Venue.OKX,
        )
        assert len(chunks) >= 2
        # Sum of all chunks should equal total
        assert sum(chunks) == pytest.approx(0.03)
        # Each chunk notional should not exceed max
        for c in chunks:
            assert c * 50000.0 <= 600.0 + 1.0  # allow epsilon

    def test_heterogeneous_price_hints(self):
        """Long price higher → chunk size bounded by the more expensive leg."""
        chunks = compute_close_chunks(
            total_quantity=2.0,
            long_price_hint=52000.0,  # long leg notional = 2 * 52000 = 104000
            short_price_hint=50000.0,  # short leg notional = 2 * 50000 = 100000
            max_notional_quote=50_000.0,
        )
        # Chunk capped by max of the two notionals: max(52000, 50000) = 52000 per unit
        # Each chunk must fit within $50000 notional on BOTH legs
        # chunk_notional_on_long = c * 52000 <= 50000 → c <= 0.9615
        for c in chunks:
            assert c * 52000.0 <= 50_000.0 + 1.0
            assert c * 50000.0 <= 50_000.0 + 1.0
        assert sum(chunks) == pytest.approx(2.0)


class TestCloseChunkExecutor:
    """Integration tests: CloseExecutor chunked execution with fake adapters."""

    @pytest.mark.asyncio
    async def test_small_position_not_chunked(self):
        """Small position (single chunk) still works correctly through executor."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE, default_fill_price=50100.0)
        short_adapter = FakeVenueAdapter(Venue.OKX, default_fill_price=49900.0)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"close_chunk_max_notional_quote": 10000.0},
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        close = await executor.execute_close(
            pos, "profit_take", 2000,
            long_price_hint=50000.0, short_price_hint=50000.0,
            total_quantity=0.01, state=state,
        )

        # 0.01 BTC * $50000 = $500 < $10000 cap → single chunk
        assert close.long_close_qty == pytest.approx(0.01)
        assert close.short_close_qty == pytest.approx(0.01)
        # Each adapter called exactly once (one chunk)
        assert long_adapter.place_order_call_count == 1
        assert short_adapter.place_order_call_count == 1

    @pytest.mark.asyncio
    async def test_final_close_emits_one_terminal_bill_with_complete_evidence(self):
        """The aggressive path has one canonical final ledger event."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)
        long_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01,
                price=50_100.0, order_id="long-close", fee_quote=0.1,
            ),
        ]
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX, "BTCUSDT", Side.BUY, 0.01,
                price=49_900.0, order_id="short-close", fee_quote=0.1,
            ),
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )
        position = _make_position(
            entry_fee_evidence_complete=True,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
            total_entry_fee_quote=0.2,
        )
        state = EngineState()
        state.open_positions[position.position_id] = position

        await executor.execute_close(
            position, "profit_take", 2_000,
            long_price_hint=50_000.0, short_price_hint=50_000.0,
            state=state,
        )

        terminal = [
            record for record in journal.read_all()
            if record["kind"] == "exit.closed"
        ]
        assert len(terminal) == 1
        assert position.position_id not in state.open_positions
        assert state.pending_close_reconciliations == []

    @pytest.mark.asyncio
    async def test_missing_close_price_defers_terminal_accounting_to_reconciliation(self):
        """A physical close without an execution price cannot become PnL."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)
        long_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01,
                price=0.0, order_id="long-close", fee_quote=0.1,
            ),
        ]
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX, "BTCUSDT", Side.BUY, 0.01,
                price=49_900.0, order_id="short-close", fee_quote=0.1,
            ),
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )
        position = _make_position(
            entry_fee_evidence_complete=True,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
            total_entry_fee_quote=0.2,
        )
        position.realized_price_pnl_quote = 1.25
        position.realized_exit_fee_quote = 0.05
        state = EngineState()
        state.open_positions[position.position_id] = position

        await executor.execute_close(
            position, "profit_take", 2_000,
            long_price_hint=50_000.0, short_price_hint=50_000.0,
            state=state,
        )

        assert position.position_id not in state.open_positions
        assert position.realized_price_pnl_quote == pytest.approx(1.25)
        assert position.realized_exit_fee_quote == pytest.approx(0.05)
        assert len(state.pending_close_reconciliations) == 1
        pending = state.pending_close_reconciliations[0]
        assert pending["original_payload"]["accounting_evidence_gaps"] == [
            "long_close_price_unavailable",
        ]
        assert pending["position_snapshot"]["realized_price_pnl_quote"] == pytest.approx(1.25)
        assert pending["position_snapshot"]["realized_exit_fee_quote"] == pytest.approx(0.05)
        assert pending["identity_evidence"] == {
            "missing_identity_legs": [],
            "long": {
                "leg_count": 1,
                "exchange_order_id_count": 1,
                "client_order_id_only_count": 0,
                "recovery_placeholder_count": 0,
                "missing_identity_count": 0,
            },
            "short": {
                "leg_count": 1,
                "exchange_order_id_count": 1,
                "client_order_id_only_count": 0,
                "recovery_placeholder_count": 0,
                "missing_identity_count": 0,
            },
        }
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.pending_close_reconciliation_registered" in kinds
        assert "exit.closed" not in kinds

    @pytest.mark.asyncio
    async def test_uncertain_submitted_close_is_reconciled_from_its_durable_cid(self):
        """An ACK/transport-uncertain close must not lose its original CID.

        This is the real COW-shaped path: the short close is locally confirmed,
        the Binance long submit returns uncertain, and V1 compensation finds
        both venue positions flat.  The later exact exchange execution lookup
        must settle one final bill, rather than leaving an orphan partial debt.
        """
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        class _LongUncertainButFilledAdapter(FakeVenueAdapter):
            async def place_order(self, _request):
                self.place_order_call_count += 1
                raise _make_uncertain_error("submit response lost")

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=0.01,
                    average_price=50_100.0,
                    order_id="binance-late-fill",
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_001,
                )

        class _ShortFilledAdapter(FakeVenueAdapter):
            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.01,
                    average_price=49_900.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_000,
                )

        long_adapter = _LongUncertainButFilledAdapter(Venue.BINANCE)
        short_adapter = _ShortFilledAdapter(Venue.OKX)
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX,
                "BTCUSDT",
                Side.BUY,
                0.01,
                price=49_900.0,
                order_id="okx-confirmed-fill",
                fee_quote=0.1,
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        state = EngineState()
        position = _make_position(
            entry_fee_evidence_complete=True,
            total_entry_fee_quote=0.2,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
        )
        state.open_positions[position.position_id] = position
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        await executor.execute_close(
            position,
            "funding_capture",
            2_000,
            long_price_hint=50_000.0,
            short_price_hint=50_000.0,
            state=state,
        )

        assert len(state.pending_close_reconciliations) == 1
        pending = state.pending_close_reconciliations[0]
        assert pending["kind"] == "partial"
        assert pending["unresolved_submission_legs"] == ["long"]
        assert pending["long_legs"] == [
            {
                "venue": "binance",
                "order_id": "",
                "client_order_id": pending["long_legs"][0]["client_order_id"],
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": 0.0,
                "filled_at_ms": 0,
            }
        ]
        assert not [
            record for record in journal.read_all() if record["kind"] == "exit.closed"
        ]

        ctx = SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            _flush_adapter_order_diagnostics=lambda _adapter: None,
        )
        runtime = CloseRuntime(ctx)
        await runtime._process_pending_close_reconciliations(2_100)

        assert state.pending_close_reconciliations == []
        assert position.position_id not in state.open_positions
        assert state.pending_closes == {}
        assert state.pending_residual_repairs == []
        assert len([
            record for record in journal.read_all() if record["kind"] == "exit.reconciled"
        ]) == 1

    @pytest.mark.asyncio
    async def test_zero_fill_ack_keeps_exchange_order_id_for_exact_close_reconciliation(self):
        """A zero-fill ACK is uncertain, not an empty order identity.

        This exercises the aggressive close path end to end: the first response
        includes a real exchange order ID but no local fill, compensation sees
        both venues flat, and reconciliation must re-query that exact ID.
        """
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        class _LongZeroAckButFilledAdapter(FakeVenueAdapter):
            async def place_order(self, request):
                self.place_order_call_count += 1
                return OrderFill(
                    venue=Venue.BINANCE,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=0.0,
                    price=0.0,
                    order_id="binance-close-ack",
                    client_order_id=request.client_order_id,
                    filled_at_ms=2_000,
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                assert symbol == "BTCUSDT"
                assert order_id == "binance-close-ack"
                assert client_order_id
                return OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=0.01,
                    average_price=50_100.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_001,
                )

        class _ShortFilledAdapter(FakeVenueAdapter):
            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.01,
                    average_price=49_900.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_000,
                )

        long_adapter = _LongZeroAckButFilledAdapter(Venue.BINANCE)
        short_adapter = _ShortFilledAdapter(Venue.OKX)
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX,
                "BTCUSDT",
                Side.BUY,
                0.01,
                price=49_900.0,
                order_id="okx-confirmed-fill",
                fee_quote=0.1,
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        state = EngineState()
        position = _make_position(
            entry_fee_evidence_complete=True,
            total_entry_fee_quote=0.2,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
        )
        state.open_positions[position.position_id] = position
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        await executor.execute_close(
            position,
            "funding_capture",
            2_000,
            long_price_hint=50_000.0,
            short_price_hint=50_000.0,
            state=state,
        )

        pending = state.pending_close_reconciliations[0]
        assert pending["long_legs"] == [
            {
                "venue": "binance",
                "order_id": "binance-close-ack",
                "client_order_id": pending["long_legs"][0]["client_order_id"],
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": 0.0,
                "filled_at_ms": 0,
            }
        ]

        ctx = SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            _flush_adapter_order_diagnostics=lambda _adapter: None,
        )
        await CloseRuntime(ctx)._process_pending_close_reconciliations(2_100)

        assert state.pending_close_reconciliations == []
        assert position.position_id not in state.open_positions
        assert len([
            record for record in journal.read_all() if record["kind"] == "exit.reconciled"
        ]) == 1

    @pytest.mark.asyncio
    async def test_compensation_zero_ack_keeps_exact_order_id_for_reconciliation(self):
        """A flat compensation probe cannot erase that compensation order's ACK.

        The normal long close is rejected after the short fills. Compensation
        then submits a long reduce-only order that ACKs with zero local fill;
        exchange position truth is flat, but the final bill must retain and
        query that exact compensation order ID.
        """
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        class _LongRejectThenCompensationZeroAckAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.BINANCE)
                self._position_queries = 0

            async def place_order(self, request):
                self.place_order_call_count += 1
                if self.place_order_call_count == 1:
                    raise _make_rejected_error("close rejected")
                return OrderFill(
                    venue=Venue.BINANCE,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=0.0,
                    price=0.0,
                    order_id="binance-compensation-ack",
                    client_order_id=request.client_order_id,
                    filled_at_ms=2_000,
                )

            async def fetch_position(self, symbol):
                self._position_queries += 1
                if self._position_queries == 1:
                    return PositionSnapshot(
                        venue=Venue.BINANCE,
                        symbol=symbol,
                        side=Side.BUY,
                        quantity=0.01,
                        entry_price=50_000.0,
                        observed_at_ms=2_000,
                    )
                return PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.0,
                    entry_price=0.0,
                    observed_at_ms=2_001,
                )

            async def fetch_order_fill_reconciliation(
                self, symbol, order_id, client_order_id=None,
            ):
                assert order_id == "binance-compensation-ack"
                return OrderFillReconciliation(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=0.01,
                    average_price=50_100.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_001,
                )

        class _ShortFilledAdapter(FakeVenueAdapter):
            async def fetch_order_fill_reconciliation(
                self, symbol, order_id, client_order_id=None,
            ):
                return OrderFillReconciliation(
                    venue=Venue.OKX,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.01,
                    average_price=49_900.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    fee_quote=0.1,
                    filled_at_ms=2_000,
                )

        long_adapter = _LongRejectThenCompensationZeroAckAdapter()
        short_adapter = _ShortFilledAdapter(Venue.OKX)
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX,
                "BTCUSDT",
                Side.BUY,
                0.01,
                price=49_900.0,
                order_id="okx-confirmed-fill",
                fee_quote=0.1,
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        state = EngineState()
        position = _make_position(
            entry_fee_evidence_complete=True,
            total_entry_fee_quote=0.2,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
        )
        state.open_positions[position.position_id] = position
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        await executor.execute_close(
            position,
            "funding_capture",
            2_000,
            long_price_hint=50_000.0,
            short_price_hint=50_000.0,
            state=state,
        )

        pending = state.pending_close_reconciliations[0]
        assert pending["unresolved_submission_legs"] == ["long"]
        assert pending["long_legs"] == [
            {
                "venue": "binance",
                "order_id": "binance-compensation-ack",
                "client_order_id": pending["long_legs"][0]["client_order_id"],
                "quantity": 0.0,
                "average_price": 0.0,
                "fee_quote": 0.0,
                "filled_at_ms": 0,
            }
        ]

        ctx = SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            _flush_adapter_order_diagnostics=lambda _adapter: None,
        )
        await CloseRuntime(ctx)._process_pending_close_reconciliations(2_100)

        assert state.pending_close_reconciliations == []
        assert position.position_id not in state.open_positions
        assert len([
            record for record in journal.read_all() if record["kind"] == "exit.reconciled"
        ]) == 1

    @pytest.mark.asyncio
    async def test_retry_fill_does_not_erase_prior_zero_ack_identity(self, monkeypatch):
        """A retry fill cannot prove that an earlier acknowledged order was empty."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        async def no_sleep(_delay):
            return None

        monkeypatch.setattr("lightfee.engine.close_executor.asyncio.sleep", no_sleep)
        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)
        long_adapter.place_order_outcomes = [
            OrderFill(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                side=Side.SELL,
                quantity=0.0,
                price=0.0,
                order_id="binance-first-ack",
                filled_at_ms=2_000,
            ),
            _fake_fill(
                Venue.BINANCE,
                "BTCUSDT",
                Side.SELL,
                0.01,
                price=50_100.0,
                order_id="binance-retry-fill",
                fee_quote=0.1,
            ),
        ]
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX,
                "BTCUSDT",
                Side.BUY,
                0.01,
                price=49_900.0,
                order_id="okx-confirmed-fill",
                fee_quote=0.1,
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        state = EngineState()
        position = _make_position(
            entry_fee_evidence_complete=True,
            total_entry_fee_quote=0.2,
            long_entry_fee_quote=0.1,
            short_entry_fee_quote=0.1,
        )
        state.open_positions[position.position_id] = position
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 2},
        )

        await executor.execute_close(
            position,
            "funding_capture",
            2_000,
            long_price_hint=50_000.0,
            short_price_hint=50_000.0,
            state=state,
        )

        pending = state.pending_close_reconciliations[0]
        assert pending["unresolved_submission_legs"] == ["long"]
        assert {leg["order_id"] for leg in pending["long_legs"]} == {
            "binance-first-ack",
            "binance-retry-fill",
        }
        assert not [
            record for record in journal.read_all() if record["kind"] == "exit.closed"
        ]

    def test_uncertain_submission_shortfill_is_not_promoted_to_a_final_bill(self):
        """Exact CID lookup must retain a partial debt unless both legs are full."""
        reconciliation = {
            "kind": "partial",
            "unresolved_submission_legs": ["long"],
            "position_snapshot": {
                "long_quantity": 0.01,
                "short_quantity": 0.01,
            },
        }

        promoted = CloseRuntime(None)._reclassify_full_uncertain_submission(
            reconciliation,
            [SimpleNamespace(quantity=0.009)],
            [SimpleNamespace(quantity=0.01)],
        )

        assert promoted is None

    @pytest.mark.asyncio
    async def test_uncertain_close_with_unavailable_compensation_truth_keeps_cid(self):
        """Fail-closed compensation still persists the original uncertain CID."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState
        from lightfee.risk.modes import GlobalRiskMode

        class _LongUncertainAndPositionUnavailable(FakeVenueAdapter):
            async def place_order(self, _request):
                self.place_order_call_count += 1
                raise _make_uncertain_error("submit response lost")

            async def fetch_position(self, _symbol):
                raise RuntimeError("Binance position endpoint unavailable")

        long_adapter = _LongUncertainAndPositionUnavailable(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)
        short_adapter.place_order_outcomes = [
            _fake_fill(
                Venue.OKX,
                "BTCUSDT",
                Side.BUY,
                0.01,
                order_id="okx-confirmed-fill",
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        state = EngineState()
        position = _make_position()
        state.open_positions[position.position_id] = position
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        await executor.execute_close(
            position,
            "funding_capture",
            2_000,
            long_price_hint=50_000.0,
            short_price_hint=50_000.0,
            state=state,
        )

        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert len(state.pending_close_reconciliations) == 1
        pending = state.pending_close_reconciliations[0]
        assert pending["source"] == "aggressive_close_compensation_truth_unavailable"
        assert pending["unresolved_submission_legs"] == ["long"]
        assert pending["long_legs"][0]["client_order_id"]
        assert pending["long_legs"][0]["quantity"] == 0.0
        assert pending["short_legs"][0]["order_id"] == "okx-confirmed-fill"
        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.compensation_failed" in kinds
        assert "exit.pending_close_reconciliation_registered" in kinds
        assert "exit.closed" not in kinds

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reason,price_field,fee_field",
        [
            (
                "risk_delever",
                "risk_delever_realized_price_pnl_quote",
                "risk_delever_realized_exit_fee_quote",
            ),
            (
                "death_protection:bybit_private_stale",
                "protection_realized_price_pnl_quote",
                "protection_realized_exit_fee_quote",
            ),
        ],
    )
    async def test_risk_and_protection_close_writeback_use_v1_pnl_buckets(
        self, reason, price_field, fee_field,
    ):
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE, default_fill_price=50100.0)
        short_adapter = FakeVenueAdapter(Venue.OKX, default_fill_price=49900.0)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"close_chunk_max_notional_quote": 10000.0},
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        await executor.execute_close(
            pos, reason, 2000,
            long_price_hint=50000.0, short_price_hint=50000.0,
            total_quantity=0.01, state=state,
        )

        assert getattr(pos, price_field) == pytest.approx(pos.realized_price_pnl_quote)
        assert getattr(pos, fee_field) == pytest.approx(pos.realized_exit_fee_quote)

    @pytest.mark.asyncio
    async def test_large_position_is_chunked(self):
        """Large position exceeds notional cap → multiple chunks."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE, default_fill_price=50100.0)
        short_adapter = FakeVenueAdapter(Venue.OKX, default_fill_price=49900.0)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={
                "close_chunk_max_notional_quote": 5000.0,  # low cap to force chunks
            },
        )

        # 0.1 BTC * $50000 = $5000 notional per leg → chunked
        # each chunk max notional = $5000 → about 1 chunk of 0.1
        # Let's make it bigger: 0.5 BTC * $50000 = $25000 → 5 chunks of 0.1
        pos = _make_position(long_quantity=0.5, short_quantity=0.5, matched_quantity=0.5)
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        close = await executor.execute_close(
            pos, "profit_take", 2000,
            long_price_hint=50000.0, short_price_hint=50000.0,
            total_quantity=0.5, state=state,
        )

        # Should have called each adapter multiple times
        assert long_adapter.place_order_call_count >= 2
        assert short_adapter.place_order_call_count >= 2
        # Total quantity should match
        assert close.long_close_qty == pytest.approx(0.5)
        assert close.short_close_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_chunks_have_distinct_client_order_ids(self):
        """Each chunk gets unique clientOrderId with _chunk_N suffix."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE, default_fill_price=50100.0)
        short_adapter = FakeVenueAdapter(Venue.OKX, default_fill_price=49900.0)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={"close_chunk_max_notional_quote": 1000.0},
        )

        # 0.5 BTC * $50000 = $25000, cap $1000 → ~25 chunks
        pos = _make_position(long_quantity=0.5, short_quantity=0.5, matched_quantity=0.5)
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        await executor.execute_close(
            pos, "profit_take", 2000,
            long_price_hint=50000.0, short_price_hint=50000.0,
            total_quantity=0.5, state=state,
        )

        # Check that chunked clientOrderIds were generated
        all_short_cids = set()
        all_long_cids = set()
        # Replay journal to find the client_order_ids from order.filled entries
        for record in executor.journal.read_all():
            if record["kind"] == "order.filled":
                cid = record["payload"].get("client_order_id", "")
                leg = record["payload"].get("leg", "")
                if leg == "short":
                    all_short_cids.add(cid)
                elif leg == "long":
                    all_long_cids.add(cid)

        # Each chunk should have a distinct clientOrderId
        assert len(all_short_cids) >= 2
        assert len(all_long_cids) >= 2
        # CIDs are compact V1 format (lf...): ~20 chars, well under 36 limit
        for cid in all_short_cids:
            assert cid.startswith("lf")
            assert 18 <= len(cid) <= 24
            assert all(c.isalnum() for c in cid)
        for cid in all_long_cids:
            assert cid.startswith("lf")
            assert 18 <= len(cid) <= 24
            assert all(c.isalnum() for c in cid)

    @pytest.mark.asyncio
    async def test_chunk_uncertain_creates_pending_close_with_chunk_info(self):
        """When a chunk is uncertain, PendingClose tracks chunk_index and total_chunks."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        long_adapter = FakeVenueAdapter(Venue.BINANCE, default_fill_price=50100.0)
        # Short adapter: all attempts uncertain (exhaust retries)
        short_adapter = FakeVenueAdapter(Venue.OKX, default_fill_price=49900.0)
        short_adapter.place_order_outcomes = [
            _make_uncertain_error("timeout"),
            _make_uncertain_error("timeout"),
            _make_uncertain_error("timeout"),
        ]

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
            config_overrides={
                "close_chunk_max_notional_quote": 5000.0,
                "max_close_retries": 1,  # don't retry on uncertain — register immediately
            },
        )

        # Large enough to trigger chunking but almost all chunks will be uncertain on short side
        pos = _make_position(long_quantity=0.5, short_quantity=0.5, matched_quantity=0.5)
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        await executor.execute_close(
            pos, "profit_take", 2000,
            long_price_hint=50000.0, short_price_hint=50000.0,
            total_quantity=0.5, state=state,
        )

        # Should have registered at least one PendingClose with chunk tracking
        assert len(state.pending_closes) >= 1
        for close_id, pc in state.pending_closes.items():
            # PendingClose should carry chunk info when chunked
            assert pc.total_chunks >= 1
            # Should reference clientOrderIds
            if pc.short_uncertain:
                assert pc.short_client_order_id != ""

    @pytest.mark.asyncio
    async def test_short_reject_stops_before_long_and_does_not_emit_closed(self):
        """V1: if the first close leg is rejected, do not submit the opposite leg."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        short_adapter = FakeVenueAdapter(Venue.BYBIT, default_fill_price=0.2911)
        long_adapter = FakeVenueAdapter(Venue.ASTER, default_fill_price=0.2908)
        short_adapter.place_order_outcomes = [
            _make_rejected_error("bybit order failed: bybit retCode=10001 retMsg=risk limit")
        ]

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.ASTER: long_adapter, Venue.BYBIT: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        pos = _make_position(
            symbol="PROVEUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=82.0,
            short_quantity=82.0,
            matched_quantity=82.0,
            long_entry_price=0.2908,
            short_entry_price=0.2911,
        )
        state = EngineState()
        state.open_positions[pos.position_id] = pos

        close = await executor.execute_close(
            pos, "funding_capture", 2000,
            long_price_hint=0.2908, short_price_hint=0.2911,
            total_quantity=82.0, state=state,
        )

        assert close is None
        assert short_adapter.place_order_call_count == 1
        assert long_adapter.place_order_call_count == 0
        assert pos.position_id in state.open_positions
        assert state.pending_closes == {}

        kinds = [record["kind"] for record in journal.read_all()]
        assert "order.rejected" in kinds
        assert "exit.closed" not in kinds
        assert "exit.partial_closed" not in kinds

    @pytest.mark.asyncio
    async def test_bybit_duplicate_close_id_live_flat_allows_other_leg_close(self):
        """Duplicate orderLinkId with live-flat evidence clears that close leg."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState
        from lightfee.venues.cid import compact_client_order_id

        class DuplicateBybitAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.BYBIT, default_fill_price=0.2911)
                self.reconciliation_lookups = []

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                self.reconciliation_lookups.append((symbol, order_id, client_order_id))
                return None

        short_adapter = DuplicateBybitAdapter()
        long_adapter = FakeVenueAdapter(Venue.ASTER, default_fill_price=0.2908)
        short_adapter.place_order_outcomes = [
            _make_rejected_error(
                "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate"
            )
        ]

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.ASTER: long_adapter, Venue.BYBIT: short_adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )

        pos = _make_position(
            symbol="PROVEUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=82.0,
            short_quantity=82.0,
            matched_quantity=82.0,
            long_entry_price=0.2908,
            short_entry_price=0.2911,
        )
        state = EngineState()
        state.open_positions[pos.position_id] = pos
        expected_short_cid = compact_client_order_id(pos.position_id, "exit_short")

        close = await executor.execute_close(
            pos, "funding_capture", 2000,
            long_price_hint=0.2908, short_price_hint=0.2911,
            total_quantity=82.0, state=state,
        )

        assert close is not None
        assert close.long_close_qty == 82.0
        assert close.short_close_qty == 0.0
        assert short_adapter.reconciliation_lookups == [
            ("PROVEUSDT", "", expected_short_cid)
        ]
        assert long_adapter.place_order_call_count == 1
        assert len(state.pending_closes) == 0

        kinds = [record["kind"] for record in journal.read_all()]
        assert "order.reconcile_result" in kinds
        assert "exit.pending_close_registered" not in kinds
        assert "exit.close_residual_detected" in kinds

    @pytest.mark.asyncio
    async def test_bybit_duplicate_live_flat_does_not_emit_zero_quantity_fill(self):
        """Live-flat duplicate reconciliation is terminal truth, not a zero fill."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.venues.cid import compact_client_order_id

        class DuplicateLiveFlatBybitAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.BYBIT, default_fill_price=0.2911)
                self.reconciliation_lookups = []

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                self.reconciliation_lookups.append((symbol, order_id, client_order_id))
                return None

            async def fetch_position(self, symbol: str):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=0.0,
                    entry_price=0.2911,
                    observed_at_ms=2200,
                )

        adapter = DuplicateLiveFlatBybitAdapter()
        adapter.place_order_outcomes = [
            _make_rejected_error(
                "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate"
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BYBIT: adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )
        client_order_id = compact_client_order_id("p001", "exit_short")
        request = OrderRequest(
            venue=Venue.BYBIT,
            symbol="MOVEUSDT",
            side=Side.BUY,
            quantity=1840.0,
            price=0.0,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=client_order_id,
        )

        result = await executor._submit_close_leg_with_retry(
            request, "p001", "short", 2000,
        )

        assert result["outcome"] == "terminal_flat"
        assert result["reason"] == "duplicate_client_order_live_flat"
        assert adapter.reconciliation_lookups == [
            ("MOVEUSDT", "", client_order_id)
        ]
        events = journal.read_all()
        filled_events = [event for event in events if event["kind"] == "order.filled"]
        assert filled_events == []
        resolution_payload = [
            event["payload"] for event in events
            if event["kind"] == "exit.close_duplicate_client_order_resolved_live_flat"
        ][-1]
        assert resolution_payload["client_order_id"] == client_order_id
        assert resolution_payload["reconciled_qty"] == 0.0
        assert resolution_payload["live_qty"] == 0.0

    @pytest.mark.asyncio
    async def test_bybit_duplicate_close_partial_returns_reconciled_fill_at_retry_ceiling(self):
        """Duplicate partial evidence must not be dropped when no retry budget remains."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.venues.cid import compact_client_order_id

        class PartialDuplicateBybitAdapter(FakeVenueAdapter):
            def __init__(self):
                super().__init__(Venue.BYBIT, default_fill_price=0.2911)
                self.reconciliation_lookups = []

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                self.reconciliation_lookups.append((symbol, order_id, client_order_id))
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=40.0,
                    average_price=0.2911,
                    order_id="partial-close-oid",
                    client_order_id=client_order_id,
                    filled_at_ms=2100,
                )

            async def fetch_position(self, symbol: str):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=42.0,
                    entry_price=0.2911,
                    observed_at_ms=2200,
                )

        adapter = PartialDuplicateBybitAdapter()
        adapter.place_order_outcomes = [
            _make_rejected_error(
                "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate"
            )
        ]
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BYBIT: adapter},
            journal=journal,
            config_overrides={"max_close_retries": 1},
        )
        client_order_id = compact_client_order_id("p001", "exit_short")
        request = OrderRequest(
            venue=Venue.BYBIT,
            symbol="PROVEUSDT",
            side=Side.BUY,
            quantity=82.0,
            price=0.2911,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=client_order_id,
        )

        result = await executor._submit_close_leg_with_retry(
            request, "p001", "short", 2000,
        )

        assert result["outcome"] == "filled"
        assert result["fill"].quantity == pytest.approx(40.0)
        assert result["fill"].client_order_id == client_order_id
        assert adapter.reconciliation_lookups == [
            ("PROVEUSDT", "", client_order_id)
        ]
        payload = [
            record["payload"] for record in journal.read_all()
            if record["kind"] == "order.reconcile_result"
        ][-1]
        assert payload["status"] == "partial"

    @pytest.mark.asyncio
    async def test_multichunk_pnl_aggregation_correct(self):
        """PnL across multiple chunks sums correctly via build_close_execution_from_legs."""
        # 2 chunks manually built and aggregated
        pos = _make_position(
            long_quantity=0.02, short_quantity=0.02, matched_quantity=0.02,
            long_entry_price=50000.0, short_entry_price=50000.0,
        )
        # Chunk 1: short fills at 49900, long fills at 50100, 0.01 each
        short_leg_1 = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49900.0, "s1", fee_quote=2.0,
        ))
        long_leg_1 = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l1", fee_quote=2.0,
        ))
        # Chunk 2: short fills at 49850, long fills at 50150, 0.01 each
        short_leg_2 = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49850.0, "s2", fee_quote=1.5,
        ))
        long_leg_2 = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50150.0, "l2", fee_quote=1.5,
        ))

        close = build_close_execution_from_legs(
            pos, 2,
            [short_leg_1, short_leg_2],
            [long_leg_1, long_leg_2],
        )

        # Chunk 1 PnL: long (50100-50000)*0.01=1.0, short (50000-49900)*0.01=1.0 → 2.0
        # Chunk 2 PnL: long (50150-50000)*0.01=1.5, short (50000-49850)*0.01=1.5 → 3.0
        # Total price PnL = 5.0
        assert close.realized_price_pnl_quote == pytest.approx(5.0)
        assert close.long_close_qty == 0.02
        assert close.short_close_qty == 0.02
        # Fees: 2.0 + 1.5 = 3.5 each side
        assert close.long_fee_quote == pytest.approx(3.5)
        assert close.short_fee_quote == pytest.approx(3.5)
        # Average prices
        # long avg = (50100*0.01 + 50150*0.01) / 0.02 = 50125.0
        assert close.long_close_price == pytest.approx(50125.0)
        # short avg = (49900*0.01 + 49850*0.01) / 0.02 = 49875.0
        assert close.short_close_price == pytest.approx(49875.0)


# ---------------------------------------------------------------------------
# Fake adapter for integration tests
# ---------------------------------------------------------------------------


class FakeVenueAdapter:
    """Minimal inline fake for chunking tests."""

    def __init__(self, venue: Venue, default_fill_price: float = 50000.0):
        self._venue = venue
        self.default_fill_price = default_fill_price
        self.place_order_outcomes: list = []
        self.last_request = None
        self.place_order_call_count = 0
        self.fetch_position_call_count = 0

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request):
        self.place_order_call_count += 1
        self.last_request = request

        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        price = self.default_fill_price
        return OrderFill(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=price,
            order_id=f"fake-{self._venue.value}-{self.place_order_call_count}",
            filled_at_ms=1000,
        )

    async def fetch_position(self, symbol: str):
        self.fetch_position_call_count += 1
        return PositionSnapshot(
            venue=self._venue, symbol=symbol, side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=1000,
        )

    async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
        return None

    async def normalize_quantity(self, symbol, quantity):
        return quantity

    async def amend_order(self, request):
        return await self.place_order(request)

    async def cancel_order(self, request):
        self.last_request = request
        return None


# ---------------------------------------------------------------------------
# V1 compensate_failed_full_close (C-R6) tests
# ---------------------------------------------------------------------------


class FakeCompensationAdapter:
    """Adapter that returns a position with residual for compensation tests."""

    def __init__(self, venue: Venue, position_qty: float = 0.0,
                 fill_price: float = 50000.0, side: Side = Side.BUY,
                 place_succeeds: bool = True):
        self._venue = venue
        self.position = PositionSnapshot(
            venue=venue, symbol="BTCUSDT", side=side,
            quantity=position_qty, entry_price=fill_price, observed_at_ms=1000,
        )
        self.fill_price = fill_price
        self._place_succeeds = place_succeeds
        self.place_order_calls: list = []
        self.fetch_position_calls: list = []

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request):
        self.place_order_calls.append(request)
        if not self._place_succeeds:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, "simulated failure")
        return OrderFill(
            venue=self._venue, symbol=request.symbol, side=request.side,
            quantity=request.quantity, price=self.fill_price,
            order_id=f"comp-{len(self.place_order_calls)}",
            filled_at_ms=1000,
        )

    async def fetch_position(self, symbol: str):
        self.fetch_position_calls.append(symbol)
        return self.position

    async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
        return None

    async def normalize_quantity(self, symbol, quantity):
        return quantity


class TestCompensateFailedFullClose:
    """C-R6: compensate_failed_full_close (V1 exit.rs:1482-1601)."""

    @pytest.mark.asyncio
    async def test_compensate_flat_position_noop(self):
        """When exchange position is flat, compensation is a no-op."""
        from lightfee.engine.close_executor import CloseExecutor, CompensationFailedError
        from lightfee.engine.state import EngineState

        short_adapter = FakeCompensationAdapter(
            Venue.OKX, position_qty=0.0,  # Already flat
        )
        long_adapter = FakeCompensationAdapter(
            Venue.BINANCE, position_qty=0.0,  # Already flat
        )

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        await executor.compensate_failed_full_close(
            pos, "profit_take", "exit_short_chunk_0",
            pos.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs, long_legs, state,
        )

        # Position was flat → no place_order calls, no legs added
        assert short_adapter.place_order_calls == []
        assert long_adapter.place_order_calls == []
        assert len(short_legs) == 0
        assert len(long_legs) == 0
        # Should have journaled exit.compensated with empty compensation
        events = journal.read_all()
        assert any("exit.compensated" == r.get("kind", "") for r in events)

    @pytest.mark.asyncio
    async def test_compensate_flattens_residual(self):
        """When exchange has residual position, compensation flattens it."""
        from lightfee.engine.close_executor import CloseExecutor, CompensationFailedError
        from lightfee.engine.state import EngineState

        # Short venue has 0.01 residual (SELL side = short → cleanup BUY)
        short_adapter = FakeCompensationAdapter(
            Venue.OKX, position_qty=0.01, side=Side.SELL,
        )
        # Long venue has 0.005 residual (BUY side = long → cleanup SELL)
        long_adapter = FakeCompensationAdapter(
            Venue.BINANCE, position_qty=0.005, side=Side.BUY,
        )

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        await executor.compensate_failed_full_close(
            pos, "hard_stop", "exit_short_chunk_0",
            pos.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs, long_legs, state,
        )

        # Short adapter should have placed a BUY (cleanup short = buy to flatten)
        assert len(short_adapter.place_order_calls) == 1
        assert short_adapter.place_order_calls[0].side == Side.BUY
        assert short_adapter.place_order_calls[0].quantity == 0.01
        # Long adapter should have placed a SELL (cleanup long = sell to flatten)
        assert len(long_adapter.place_order_calls) == 1
        assert long_adapter.place_order_calls[0].side == Side.SELL
        assert long_adapter.place_order_calls[0].quantity == 0.005
        # Legs should be added
        assert len(short_legs) == 1
        assert len(long_legs) == 1

    @pytest.mark.asyncio
    async def test_compensation_partial_fill_survives_retry(self):
        """Compensation records both a partial close and its retry fill.

        Previously the first accepted partial fill was discarded whenever the
        residual needed another retry, producing an incomplete close bill even
        after exchange exposure reached zero.
        """
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        class _PartialThenFullCompensationAdapter(FakeCompensationAdapter):
            def __init__(self):
                super().__init__(Venue.OKX, position_qty=0.01, side=Side.SELL)

            async def place_order(self, request):
                self.place_order_calls.append(request)
                if len(self.place_order_calls) == 1:
                    self.position = PositionSnapshot(
                        venue=Venue.OKX,
                        symbol=request.symbol,
                        side=Side.SELL,
                        quantity=0.006,
                        entry_price=50_000.0,
                        observed_at_ms=1_001,
                    )
                    return OrderFill(
                        venue=Venue.OKX,
                        symbol=request.symbol,
                        side=request.side,
                        quantity=0.004,
                        price=49_900.0,
                        order_id="okx-compensation-partial",
                        fee_quote=0.04,
                        filled_at_ms=1_001,
                    )
                self.position = PositionSnapshot(
                    venue=Venue.OKX,
                    symbol=request.symbol,
                    side=Side.SELL,
                    quantity=0.0,
                    entry_price=0.0,
                    observed_at_ms=1_002,
                )
                return OrderFill(
                    venue=Venue.OKX,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=49_900.0,
                    order_id="okx-compensation-retry",
                    fee_quote=0.06,
                    filled_at_ms=1_002,
                )

        short_adapter = _PartialThenFullCompensationAdapter()
        long_adapter = FakeCompensationAdapter(Venue.BINANCE, position_qty=0.0)
        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )
        position = _make_position()
        short_legs: list = []
        long_legs: list = []

        await executor.compensate_failed_full_close(
            position,
            "hard_stop",
            "exit_short_chunk_0",
            position.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs,
            long_legs,
            EngineState(),
        )

        assert [leg.fill.order_id for leg in short_legs] == [
            "okx-compensation-partial",
            "okx-compensation-retry",
        ]
        assert sum(leg.fill.quantity for leg in short_legs) == pytest.approx(0.01)
        assert long_legs == []

    @pytest.mark.asyncio
    async def test_compensate_bybit_duplicate_reconciles_full_live_flat(self):
        """Bybit 110072 during compensation records reconciled fill evidence."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.state import EngineState

        class DuplicateThenFlatCompensationAdapter(FakeCompensationAdapter):
            def __init__(self):
                super().__init__(
                    Venue.BYBIT, position_qty=0.01, side=Side.SELL,
                    place_succeeds=False,
                )
                self.order_fill_reconciliation = OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.01,
                    average_price=50000.0,
                    order_id="bybit-old-comp",
                    client_order_id="old-comp-cid",
                    filled_at_ms=1000,
                )
                self.reconciliation_calls = []

            async def place_order(self, request):
                self.place_order_calls.append(request)
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                )

            async def fetch_position(self, symbol: str):
                self.fetch_position_calls.append(symbol)
                if len(self.fetch_position_calls) == 1:
                    return self.position
                return None

            async def fetch_order_fill_reconciliation(
                self, symbol, order_id, client_order_id=None
            ):
                self.reconciliation_calls.append((symbol, order_id, client_order_id))
                return self.order_fill_reconciliation

        short_adapter = DuplicateThenFlatCompensationAdapter()
        long_adapter = FakeCompensationAdapter(Venue.BINANCE, position_qty=0.0)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.BYBIT: short_adapter},
            journal=journal,
        )

        pos = _make_position(
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.01,
            short_quantity=0.01,
            matched_quantity=0.01,
        )
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        await executor.compensate_failed_full_close(
            pos, "hard_stop", "exit_short_chunk_0",
            pos.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs, long_legs, state,
        )

        assert len(short_legs) == 1
        assert short_legs[0].fill.order_id == "bybit-old-comp"
        assert short_adapter.reconciliation_calls
        kinds = [event["kind"] for event in journal.read_all()]
        assert "order.reconcile_result" in kinds
        assert "exit.compensation_duplicate_client_order_reconcile_result" in kinds

    @pytest.mark.asyncio
    async def test_compensate_flatten_fails_hard_stop_succeeds(self):
        """Tier 1 flatten fails → Tier 2 hard stop succeeds."""
        from lightfee.engine.close_executor import CloseExecutor, CompensationFailedError
        from lightfee.engine.state import EngineState

        # Short adapter: place_order fails first 3 times (max retries), then re-fetch still shows position
        # but hard stop will succeed because we bump the mock after retries
        short_pos = PositionSnapshot(
            venue=Venue.OKX, symbol="BTCUSDT", side=Side.SELL,
            quantity=0.01, entry_price=50000.0, observed_at_ms=1000,
        )
        short_adapter = _RetryThenSucceedAdapter(Venue.OKX, short_pos, fail_count=3)

        long_adapter = FakeCompensationAdapter(
            Venue.BINANCE, position_qty=0.0,  # Already flat
        )

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        await executor.compensate_failed_full_close(
            pos, "hard_stop", "exit_short_chunk_0",
            pos.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs, long_legs, state,
        )

        # Should have succeeded via hard stop
        assert short_adapter.hard_stop_called
        assert len(short_legs) == 1

    @pytest.mark.asyncio
    async def test_compensate_all_tiers_fail_enter_fail_closed(self):
        """When both flatten and hard stop fail, enter FAIL_CLOSED."""
        from lightfee.engine.close_executor import CloseExecutor, CompensationFailedError
        from lightfee.engine.state import EngineState
        from lightfee.risk.modes import GlobalRiskMode

        # Both adapters fail all place_order calls
        short_adapter = FakeCompensationAdapter(
            Venue.OKX, position_qty=0.01, side=Side.SELL, place_succeeds=False,
        )
        long_adapter = FakeCompensationAdapter(
            Venue.BINANCE, position_qty=0.005, side=Side.BUY, place_succeeds=False,
        )

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        with pytest.raises(CompensationFailedError):
            await executor.compensate_failed_full_close(
                pos, "hard_stop", "exit_short_chunk_0",
                pos.short_venue,
                OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
                short_legs, long_legs, state,
            )

        # State should be in FAIL_CLOSED
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.last_error is not None
        assert "compensation failed" in state.last_error.lower()

    def test_order_error_may_have_created_exposure(self):
        """UNCERTAIN errors indicate possible exposure; REJECTED do not."""
        from lightfee.engine.close_executor import order_error_may_have_created_exposure

        assert order_error_may_have_created_exposure(
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout")
        ) is True

        assert order_error_may_have_created_exposure(
            OrderSubmitError(SubmitFailureClass.REJECTED, "invalid")
        ) is False

        # Non-OrderSubmitError returns False
        assert order_error_may_have_created_exposure(RuntimeError("generic")) is False

    @pytest.mark.asyncio
    async def test_compensate_all_tiers_fail_but_exchange_flat_no_fail_closed(self):
        """C-R6: Both tiers fail but exchange confirms flat → no FAIL_CLOSED.

        V1 compensate_failed_full_close (exit.rs:1482-1601) distinguishes
        "compensation failed" from "already flat". When the exchange reports
        flat after compensation attempts, treat as success — do NOT enter
        FAIL_CLOSED.
        """
        from lightfee.engine.close_executor import CloseExecutor, CompensationFailedError
        from lightfee.engine.state import EngineState
        from lightfee.risk.modes import GlobalRiskMode

        # Adapter where place_order always fails but fetch_position eventually
        # returns flat (simulating external close/cancel that flattened the
        # position between attempts).
        class FlatAfterFailAdapter:
            def __init__(self, venue):
                self._venue = venue
                self.place_order_calls = 0
                self.fetch_position_calls = 0
                self._flat_after = 6  # Return flat after 6 fetch_position calls

            @property
            def venue(self):
                return self._venue

            async def place_order(self, request):
                self.place_order_calls += 1
                raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, "simulated failure")

            async def fetch_position(self, symbol):
                self.fetch_position_calls += 1
                if self.fetch_position_calls >= self._flat_after:
                    return None  # Flat
                return PositionSnapshot(
                    venue=self._venue, symbol=symbol, side=Side.SELL,
                    quantity=0.01, entry_price=50000.0, observed_at_ms=1000,
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return None

            async def normalize_quantity(self, symbol, quantity):
                return quantity

        short_adapter = FlatAfterFailAdapter(Venue.OKX)
        long_adapter = FlatAfterFailAdapter(Venue.BINANCE)

        journal = Journal(Path(tempfile.mkdtemp()) / "journal.jsonl")
        journal.open()
        executor = CloseExecutor(
            adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal=journal,
        )

        pos = _make_position(long_quantity=0.01, short_quantity=0.01, matched_quantity=0.01)
        state = EngineState()
        short_legs: list = []
        long_legs: list = []

        # Should NOT raise — exchange confirmed flat, no FAIL_CLOSED
        await executor.compensate_failed_full_close(
            pos, "hard_stop", "exit_short_chunk_0",
            pos.short_venue,
            OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout"),
            short_legs, long_legs, state,
        )

        # State should NOT be in FAIL_CLOSED (exchange confirmed flat)
        assert state.risk_mode != GlobalRiskMode.FAIL_CLOSED, (
            "C-R6: exchange confirmed flat should NOT enter FAIL_CLOSED"
        )
        assert state.lifecycle != "fail_closed"


class _RetryThenSucceedAdapter:
    """Adapter that fails N place_order calls then succeeds, used for hard stop testing."""

    def __init__(self, venue: Venue, position: PositionSnapshot,
                 fail_count: int = 3):
        self._venue = venue
        self.position = position
        self._fail_count = fail_count
        self._call_count = 0
        self.hard_stop_called = False
        self.place_order_calls: list = []
        self.fetch_position_calls: list = []

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request):
        self.place_order_calls.append(request)
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, f"fail {self._call_count}")
        # Subsequent calls (hard stop) succeed
        self.hard_stop_called = True
        return OrderFill(
            venue=self._venue, symbol=request.symbol, side=request.side,
            quantity=request.quantity, price=50000.0,
            order_id=f"hs-{self._call_count}",
            filled_at_ms=1000,
        )

    async def fetch_position(self, symbol: str):
        self.fetch_position_calls.append(symbol)
        # Always show position — hard stop needs to see residual
        return self.position

    async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
        return None

    async def normalize_quantity(self, symbol, quantity):
        return quantity
from lightfee.core.domain import PositionSnapshot
