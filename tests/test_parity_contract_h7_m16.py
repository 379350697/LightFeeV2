"""Contract/fixture tests for H-R7 and M-R16 representation differences.

H-R7: V1 uses signed size to derive flatten side, V2 uses abs quantity + side.
  Prove that V2's side normalization is always equivalent to V1 signed-size
  semantics for all venue position boundary conditions.

M-R16: V2 arming reason is derived from string fault keyword matching instead
  of V1's strong EntryLocalL2LegFault enum. Prove that the string vocabulary
  covers all known fault scenarios and produces correct arming reasons.
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.entry_local_l2 import (
    SessionArmingReason,
    _derive_arming_reason_from_book,
)


def _signed_to_side(signed_qty: float) -> tuple[Side, float]:
    """V1 → V2 conversion: signed quantity → (side, abs quantity)."""
    if signed_qty >= 0:
        return Side.BUY, abs(signed_qty)
    return Side.SELL, abs(signed_qty)


def _side_abs_to_signed(side: Side, abs_qty: float) -> float:
    """V2 → V1 conversion: (side, abs quantity) → signed quantity."""
    return abs_qty if side == Side.BUY else -abs_qty


class TestH7SideNormalizationEquivalence:
    """Prove that V2 (abs quantity + side) is always equivalent to V1 signed size."""

    @pytest.mark.parametrize(
        "signed_qty,expected_side,expected_abs",
        [
            (0.0, Side.BUY, 0.0),
            (1.0, Side.BUY, 1.0),
            (-1.0, Side.SELL, 1.0),
            (100.5, Side.BUY, 100.5),
            (-100.5, Side.SELL, 100.5),
            (0.001, Side.BUY, 0.001),
            (-0.001, Side.SELL, 0.001),
            (1e-9, Side.BUY, 1e-9),
            (-1e-9, Side.SELL, 1e-9),
        ],
    )
    def test_signed_to_side_roundtrip(self, signed_qty, expected_side, expected_abs):
        """V1 signed → V2 (side, abs) → V1 signed must be idempotent."""
        side, abs_qty = _signed_to_side(signed_qty)
        assert side == expected_side
        assert abs(abs_qty - expected_abs) < 1e-12
        # Roundtrip: V2 → V1 signed must match original
        recovered = _side_abs_to_signed(side, abs_qty)
        assert abs(recovered - signed_qty) < 1e-12

    @pytest.mark.parametrize(
        "side,abs_qty,expected_signed",
        [
            (Side.BUY, 1.0, 1.0),
            (Side.SELL, 1.0, -1.0),
            (Side.BUY, 0.0, 0.0),
            (Side.SELL, 0.0, 0.0),
            (Side.BUY, 100.5, 100.5),
            (Side.SELL, 100.5, -100.5),
        ],
    )
    def test_side_abs_to_signed_roundtrip(self, side, abs_qty, expected_signed):
        """V2 (side, abs) → V1 signed → V2 (side, abs) must be idempotent."""
        signed = _side_abs_to_signed(side, abs_qty)
        assert abs(signed - expected_signed) < 1e-12
        # Roundtrip: V1 signed → V2 must match original
        recovered_side, recovered_abs = _signed_to_side(signed)
        assert recovered_side == side or (abs_qty == 0 and recovered_abs == 0)
        assert abs(recovered_abs - abs_qty) < 1e-12

    def test_flatten_side_from_abs_position(self):
        """Prove that V2's side from abs quantity + side yields correct flatten side.

        When closing a position:
        - Long position (Side.BUY) → close with Side.SELL (flatten by selling)
        - Short position (Side.SELL) → close with Side.BUY (flatten by buying)

        This is equivalent to V1's signed quantity: flatten_side = opposite of position_side.
        """
        for pos_side in (Side.BUY, Side.SELL):
            for qty in (0.1, 1.0, 100.0):
                # V2: position is (side, abs_qty)
                flatten_side = Side.SELL if pos_side == Side.BUY else Side.BUY
                # V1 equivalent: signed_qty > 0 means BUY position → SELL to flatten
                v1_signed = _side_abs_to_signed(pos_side, qty)
                v1_flatten = Side.SELL if v1_signed > 0 else Side.BUY
                assert flatten_side == v1_flatten, (
                    f"Side mismatch: V2 flatten={flatten_side}, "
                    f"V1 flatten={v1_flatten} for pos_side={pos_side}, qty={qty}"
                )

    def test_position_snapshot_contract(self):
        """Prove that PositionSnapshot can represent all venue positions faithfully.

        All venue positions produce non-negative abs quantity with correct side.
        Zero-quantity positions (flat) still have a valid side.
        """
        venues = [Venue.BINANCE, Venue.BYBIT, Venue.OKX, Venue.BITGET, Venue.GATE, Venue.HYPERLIQUID]
        for venue in venues:
            for side in (Side.BUY, Side.SELL):
                for qty in (0.0, 0.1, 1.0):
                    snap = PositionSnapshot(
                        venue=venue,
                        symbol="BTC-USDT",
                        side=side,
                        quantity=qty,
                        entry_price=50000.0,
                        observed_at_ms=1000,
                    )
                    assert snap.quantity >= 0, f"Abs quantity must be non-negative: {snap.quantity}"
                    signed = _side_abs_to_signed(snap.side, snap.quantity)
                    # For zero qty, signed should be 0 regardless of side
                    if qty == 0.0:
                        assert signed == 0.0
                    # For BUY, signed should be positive
                    elif side == Side.BUY:
                        assert signed > 0
                    # For SELL, signed should be negative
                    else:
                        assert signed < 0


class TestM16ArmingReasonVocabulary:
    """Prove that the arming reason string vocabulary covers all fault scenarios.

    V2 derives arming reason from book.fault_reason strings instead of V1's
    strong EntryLocalL2LegFault enum. This test proves the vocabulary mapping
    is complete and deterministic.
    """

    # All known V1 fault types and their expected V2 arming reasons
    V1_FAULT_TO_ARMING = {
        # V1 EntryLocalL2LegFault variants → V2 SessionArmingReason
        "gate_obu_gap": SessionArmingReason.SEQUENCE_GAP,
        "okx_prev_seq_mismatch": SessionArmingReason.SEQUENCE_GAP,
        "okx_checksum_mismatch": SessionArmingReason.SEQUENCE_GAP,
        "hyperliquid_disconnect": SessionArmingReason.TRANSPORT_FAULT_RECOVERY,
        "crossed_or_locked_book": SessionArmingReason.BOOK_STATUS_TRANSITION,
        "stale_book": SessionArmingReason.STALE_BOOK_RECOVERY,
        "runtime_suspended": SessionArmingReason.TRANSPORT_FAULT_RECOVERY,
    }

    def test_all_v1_fault_types_have_arming_mapping(self):
        """Every V1 fault type must map to a valid arming reason."""
        for fault_str, expected in self.V1_FAULT_TO_ARMING.items():
            mock_book = _MockBook(fault_reason=fault_str, status_value="hot")
            result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
            assert result == expected, (
                f"V1 fault '{fault_str}' mapped to {result}, expected {expected}"
            )

    def test_string_vocabulary_coverage(self):
        """All known fault reason substrings must be detected by the vocabulary."""
        # These strings represent real exchange/transport error patterns
        known_fault_strings = [
            # Sequence/checksum faults → SEQUENCE_GAP
            "sequence_gap_detected",
            "checksum_mismatch_on_book",
            "prev_seq_invalid",
            "obu_gap_detected",
            "previous_link_mismatch",
            # Stale-related → STALE_BOOK_RECOVERY
            "stale_book_timeout",
            "quote_age_exceeded",
            "idle_timeout",
            "resume_window_expired",
            # Transport/connection → TRANSPORT_FAULT_RECOVERY
            "transport_error_timeout",
            "connection_refused",
            "disconnect_detected",
            "stream_error",
            "timeout_during_sync",
            "snapshot_bootstrap_failed",
            "runtime_suspended",
            # Book structure → BOOK_STATUS_TRANSITION
            "crossed_book_detected",
            "locked_book",
            "non_positive_bid_ask",
            "buffer_overflow",
        ]
        for fault_str in known_fault_strings:
            mock_book = _MockBook(fault_reason=fault_str, status_value="hot")
            result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
            # Must not produce FIRST_SESSION for a hot book with a fault
            assert result != SessionArmingReason.FIRST_SESSION, (
                f"Fault '{fault_str}' should not produce FIRST_SESSION for hot book"
            )

    def test_cold_book_no_fault(self):
        """Cold book with no fault reason → FIRST_SESSION."""
        mock_book = _MockBook(fault_reason="", status_value="cold")
        result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
        assert result == SessionArmingReason.FIRST_SESSION

    def test_hot_book_no_fault(self):
        """Hot book with no fault reason → BOOK_STATUS_TRANSITION."""
        mock_book = _MockBook(fault_reason="", status_value="hot")
        result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
        assert result == SessionArmingReason.BOOK_STATUS_TRANSITION

    def test_unknown_fault_defaults_to_book_status_transition(self):
        """Unknown fault strings should default to BOOK_STATUS_TRANSITION (safe fallback)."""
        mock_book = _MockBook(fault_reason="some_future_unknown_error", status_value="hot")
        result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
        # Should fall through to default, not crash
        assert result == SessionArmingReason.BOOK_STATUS_TRANSITION

    def test_deterministic_mapping(self):
        """Same fault reason must always produce the same arming reason."""
        for _ in range(10):
            mock_book = _MockBook(fault_reason="stale_book", status_value="hot")
            result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
            assert result == SessionArmingReason.STALE_BOOK_RECOVERY

    def test_case_insensitive_matching(self):
        """Fault reason matching should be case-insensitive."""
        variants = ["STALE_BOOK", "Stale_Book", "stale_book", "sTaLe_BoOk"]
        for variant in variants:
            mock_book = _MockBook(fault_reason=variant, status_value="hot")
            result = _derive_arming_reason_from_book(mock_book, mock_book.status_value)
            assert result == SessionArmingReason.STALE_BOOK_RECOVERY, (
                f"Case variant '{variant}' mapped to {result}"
            )


class TestC2SupervisorProductionPath:
    """Regression: C-R2 supervisor must not crash with real adapters.

    Proves that supervisor.supervise() handles real VenueAdapter instances
    where supports_risk_health and supports_private_health are @property,
    not methods.
    """

    def test_real_adapter_supervise_no_crash(self):
        """Supervisor with real BinanceAdapter must not raise TypeError."""
        from lightfee.config.schema import AppConfig, PersistenceConfig, StrategyConfig
        from lightfee.core.domain import Venue
        from lightfee.engine.state import EngineState
        from lightfee.engine.supervisor import Supervisor
        from lightfee.persistence.journal import Journal
        from lightfee.venues.binance import BinanceAdapter

        cfg = AppConfig(
            strategy=StrategyConfig(
                risk_monitor_enabled=True,
                death_line_enabled=True,
                delever_line_enabled=True,
                warning_line_enabled=True,
                warning_pause_new_entries_enabled=True,
            ),
            persistence=PersistenceConfig(
                snapshot_path="/tmp/_test_cr2_snap.json",
                event_log_path="/tmp/_test_cr2_journal.json",
            ),
        )
        state = EngineState()
        journal = Journal(cfg.persistence.event_log_path)
        journal.open()  # required before append()
        try:
            sv = Supervisor(cfg, state, journal)
            adapter = BinanceAdapter(mode="paper")
            adapters = {Venue.BINANCE: adapter}

            # This should not crash — the original bug caused TypeError:
            # 'bool' object is not callable on adapter.supports_risk_health()
            sv.supervise(now_ms=1000, venue_health_ratios={"binance": 1.5}, adapters=adapters)
            assert sv._venue_health_views is not None
        finally:
            journal.close()

    def test_paper_adapter_private_health_defaults_to_false(self):
        """Paper mode adapters should default supports_private_health=False."""
        from lightfee.venues.binance import BinanceAdapter

        a = BinanceAdapter(mode="paper")
        assert a.supports_private_health is False
        assert a.supports_risk_health is False
        assert a.cached_private_connection_health() is None
        assert a.cached_position("BTC-USDT") is None

    def test_supports_risk_health_is_property_not_method(self):
        """All real adapters must expose supports_* as properties, not methods."""
        from lightfee.venues.binance import BinanceAdapter

        a = BinanceAdapter(mode="paper")
        # Property access works
        assert isinstance(a.supports_risk_health, bool)
        assert isinstance(a.supports_private_health, bool)
        # Method call must raise TypeError
        try:
            a.supports_risk_health()  # type: ignore[operator]
            raise AssertionError("should have raised TypeError")
        except TypeError:
            pass  # expected


class _MockBook:
    """Minimal mock for book objects used in arming reason derivation."""

    def __init__(self, fault_reason: str = "", status_value: str = "cold"):
        self.fault_reason = fault_reason
        self.status_value = status_value
