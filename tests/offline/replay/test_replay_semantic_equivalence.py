"""Semantic parity tests for replay equivalence (REPLAY-001).

Validates that V2 replay reconstructs the full V1-visible state:
open positions, pending entries, pending closes, lifecycle, risk mode,
scan stats, recovery events, risk events, local-L2 events, and timeline.
Replay must be idempotent and produce the same semantic summary.
"""

from __future__ import annotations

import pytest
from lightfee.persistence.journal import Journal, replay_journal_records


# ---------------------------------------------------------------------------
# Full journal roundtrip fixtures
# ---------------------------------------------------------------------------

def _make_full_lifecycle_journal() -> list[dict]:
    """Build a synthetic journal covering V1's full event lifecycle."""
    return [
        {"seq": 1, "run_id": "run-1", "ts_ms": 1000,
         "kind": "runtime.lifecycle_changed",
         "payload": {"from": "booting", "to": "reconciling"}},
        {"seq": 2, "run_id": "run-1", "ts_ms": 1100,
         "kind": "runtime.lifecycle_changed",
         "payload": {"from": "reconciling", "to": "running"}},

        # Position 1: open and close fully
        {"seq": 3, "run_id": "run-1", "ts_ms": 2000,
         "kind": "entry.opened",
         "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                     "long_venue": "binance", "short_venue": "okx",
                     "quantity": 5.0, "long_quantity": 5.0, "short_quantity": 5.0,
                     "long_entry_price": 3000.0, "short_entry_price": 3000.0,
                     "opened_at_ms": 2000}},
        {"seq": 4, "run_id": "run-1", "ts_ms": 3000,
         "kind": "exit.closed",
         "payload": {"position_id": "p1"}},

        # Position 2: open, partial close, then final close
        {"seq": 5, "run_id": "run-1", "ts_ms": 4000,
         "kind": "entry.opened",
         "payload": {"position_id": "p2", "symbol": "BTC-USDT",
                     "long_venue": "binance", "short_venue": "gate",
                     "quantity": 1.0, "long_quantity": 1.0, "short_quantity": 1.0,
                     "long_entry_price": 50000.0, "short_entry_price": 50000.0,
                     "opened_at_ms": 4000}},
        {"seq": 6, "run_id": "run-1", "ts_ms": 5000,
         "kind": "exit.partial_closed",
         "payload": {"position_id": "p2", "quantity": 0.5,
                     "current_net_quote": -50.0}},
        {"seq": 7, "run_id": "run-1", "ts_ms": 6000,
         "kind": "exit.closed",
         "payload": {"position_id": "p2"}},

        # Risk and recovery events
        {"seq": 8, "run_id": "run-1", "ts_ms": 7000,
         "kind": "risk.warning_triggered",
         "payload": {"pnl_quote": -500, "position_id": "p2"}},
        {"seq": 9, "run_id": "run-1", "ts_ms": 8000,
         "kind": "risk.single_side_protection_triggered",
         "payload": {"position_id": "p2"}},
        {"seq": 10, "run_id": "run-1", "ts_ms": 9000,
         "kind": "recovery.mismatch_detected",
         "payload": {"position_id": "p2", "reason": "qty_drift"}},

        # Scan completion
        {"seq": 11, "run_id": "run-1", "ts_ms": 10000,
         "kind": "scan.completed",
         "payload": {"candidate_count": 15, "blocked_count": 5,
                     "accepted_count": 10,
                     "blocked_reasons": {"risk": 3, "min_notional": 2},
                     "no_entry_reason": ""}},

        # Risk mode change
        {"seq": 12, "run_id": "run-1", "ts_ms": 11000,
         "kind": "runtime.risk_mode_changed",
         "payload": {"from": "running", "to": "reduce_only"}},

        # Recovery events
        {"seq": 13, "run_id": "run-1", "ts_ms": 12000,
         "kind": "recovery.blocked",
         "payload": {"reason": "test_block", "open_position_count": 0}},
        {"seq": 14, "run_id": "run-1", "ts_ms": 13000,
         "kind": "recovery.resumed",
         "payload": {"reason": "test_resume"}},
    ]


# ---------------------------------------------------------------------------
# REPLAY-001 tests
# ---------------------------------------------------------------------------

class TestReplaySemanticEquivalence:
    """REPLAY-001: Replay reconstructs full V1-visible state."""

    def test_replay_reconstructs_full_state(self):
        records = _make_full_lifecycle_journal()
        result = replay_journal_records(records)
        # All positions should be closed
        assert result["open_position_count"] == 0
        assert result["final_lifecycle"] == "running"
        assert result["final_risk_mode"] == "reduce_only"

    def test_replay_timeline_includes_all_interesting_events(self):
        records = _make_full_lifecycle_journal()
        result = replay_journal_records(records)
        timeline_kinds = [e["kind"] for e in result["timeline"]]
        assert "entry.opened" in timeline_kinds
        assert "exit.closed" in timeline_kinds
        assert "exit.partial_closed" in timeline_kinds
        assert "runtime.lifecycle_changed" in timeline_kinds
        assert "runtime.risk_mode_changed" in timeline_kinds
        assert "risk.warning_triggered" in timeline_kinds

    def test_replay_idempotent_full_lifecycle(self):
        records = _make_full_lifecycle_journal()
        r1 = replay_journal_records(records)
        r2 = replay_journal_records(records)

        assert r1["open_position_count"] == r2["open_position_count"]
        assert r1["pending_entry_count"] == r2["pending_entry_count"]
        assert r1["pending_close_count"] == r2["pending_close_count"]
        assert r1["final_lifecycle"] == r2["final_lifecycle"]
        assert r1["final_risk_mode"] == r2["final_risk_mode"]
        assert r1["open_position_ids"] == r2["open_position_ids"]
        assert len(r1["recovery_events"]) == len(r2["recovery_events"])
        assert len(r1["risk_events"]) == len(r2["risk_events"])
        assert len(r1["timeline"]) == len(r2["timeline"])

    def test_replay_positions_have_normalized_schema(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 2.0, "long_quantity": 2.0, "short_quantity": 2.0,
                         "long_entry_price": 3000.0, "short_entry_price": 3000.0}},
        ]
        result = replay_journal_records(records)
        assert "p1" in result["positions"]
        pos = result["positions"]["p1"]
        V1_NORMALIZED_FIELDS = {
            "position_id", "symbol", "long_venue", "short_venue",
            "quantity", "long_quantity", "short_quantity",
            "long_entry_price", "short_entry_price", "opened_at_ms",
            "matched_quantity", "current_net_quote", "peak_net_quote",
            "captured_funding_quote", "second_stage_funding_quote",
            "long_entry_fee_quote", "short_entry_fee_quote",
            "realized_price_pnl_quote", "realized_exit_fee_quote",
            "funding_captured", "second_stage_funding_captured",
        }
        for field in V1_NORMALIZED_FIELDS:
            assert field in pos, f"Normalized position missing field: {field}"

    def test_replay_partial_close_updates_position(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 2.0, "long_quantity": 2.0, "short_quantity": 2.0,
                         "current_net_quote": 0.0, "peak_net_quote": 0.0,
                         "funding_captured": False,
                         "second_stage_funding_captured": False}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "exit.partial_closed",
             "payload": {"position_id": "p1", "quantity": 0.5,
                         "current_net_quote": -50.0, "peak_net_quote": 10.0,
                         "funding_captured": True,
                         "second_stage_funding_captured": True}},
        ]
        result = replay_journal_records(records)
        pos = result["positions"]["p1"]
        assert pos["quantity"] == 0.5
        assert pos["current_net_quote"] == -50.0
        assert pos["peak_net_quote"] == 10.0
        assert pos["funding_captured"] is True

    def test_replay_scan_stats_complete(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "scan.completed",
             "payload": {
                 "candidate_count": 100,
                 "blocked_count": 20,
                 "accepted_count": 80,
                 "blocked_reasons": {"min_notional": 10, "risk": 5, "directed_pairs": 5},
                 "no_entry_reason": "risk_mode_reduce_only",
             }},
        ]
        result = replay_journal_records(records)
        assert result["scan_stats"]["candidate_count"] == 100
        assert result["scan_stats"]["blocked_count"] == 20
        assert result["scan_stats"]["accepted_count"] == 80
        assert result["scan_stats"]["no_entry_reason"] == "risk_mode_reduce_only"


class TestReplayEdgeCases:
    """REPLAY-001: Replay handles edge cases correctly."""

    def test_replay_duplicate_position_ids_handled(self):
        """Two entry.opened with same position_id: last one wins."""
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 1.0, "long_quantity": 1.0, "short_quantity": 1.0}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 3.0, "long_quantity": 3.0, "short_quantity": 3.0}},
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 1
        assert result["positions"]["p1"]["quantity"] == 3.0

    def test_replay_close_nonexistent_position_no_error(self):
        """Closing a position not in the map should not crash."""
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "exit.closed",
             "payload": {"position_id": "p-nonexistent"}},
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0

    def test_replay_missing_payload_fields_use_defaults(self):
        """Records with missing payload fields should not crash."""
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p-minimal"}},
        ]
        result = replay_journal_records(records)
        pos = result["positions"]["p-minimal"]
        assert pos["quantity"] == 0.0
        assert pos["symbol"] == ""

    def test_replay_unrecognized_kind_no_error(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "unknown.custom.event",
             "payload": {"data": "ignored"}},
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0


class TestReplayWithJournalRoundtrip:
    """REPLAY-001: Write-then-read journal roundtrip is lossless."""

    def test_journal_write_read_replay(self, tmp_path):
        journal = Journal(tmp_path / "roundtrip.jsonl")
        journal.open()
        try:
            journal.append("entry.opened", {
                "position_id": "p1", "symbol": "ETH-USDT",
                "quantity": 2.0, "long_quantity": 2.0, "short_quantity": 2.0,
            })
            journal.append("runtime.lifecycle_changed", {"to": "running"})
        finally:
            journal.close()

        records = journal.read_all()
        result = replay_journal_records(records)
        assert result["open_position_count"] == 1
        assert result["final_lifecycle"] == "running"

    def test_paper_outcome_events_roundtrip(self, tmp_path):
        """Paper outcome events survive journal write-read and are analyzable."""
        journal = Journal(tmp_path / "paper_outcome_roundtrip.jsonl")
        journal.open()
        try:
            journal.append("opportunity.paper_registered", {
                "paper_id": "p1", "review_id": "rvw-1",
                "symbol": "LABUSDT",
                "paper_order_status": "open",
            })
            journal.append("opportunity.paper_markout", {
                "paper_id": "p1", "review_id": "rvw-1",
                "symbol": "LABUSDT",
                "horizon_kind": "markout_300s",
                "opportunity_label": "good_trade_missed",
                "paper_net_quote": 0.33,
                "paper_fee_quote": 0.01,
                "paper_slippage_quote": 0.02,
                "paper_funding_quote": 0.03,
                "evaluated_at_ms": 301000,
            })
            journal.append("opportunity.paper_closed", {
                "paper_id": "p1", "review_id": "rvw-1",
                "symbol": "LABUSDT",
                "horizon_kind": "settlement",
                "opportunity_label": "bad_trade_correctly_rejected",
                "paper_net_quote": -0.10,
                "paper_fee_quote": 0.01,
                "paper_slippage_quote": 0.02,
                "paper_funding_quote": -0.01,
                "evaluated_at_ms": 3600100,
            })
            journal.append("opportunity.real_vs_paper_joined", {
                "paper_id": "p1", "review_id": "rvw-1",
                "position_id": "pos-1",
                "symbol": "LABUSDT",
                "opportunity_label": "good_trade_executed",
                "real_net_quote": 0.25,
                "evaluated_at_ms": 370000,
            })
        finally:
            journal.close()

        records = journal.read_all()
        # Paper outcome events should not break replay
        result = replay_journal_records(records)
        assert result is not None

        # Verify analysis layer can process them
        from lightfee.offline.analysis.journal import analyze_journal_records
        report = analyze_journal_records(records)
        assert report.paper_outcome_registered_count == 1
        assert report.paper_outcome_markout_count == 1
        assert report.paper_outcome_closed_count == 1
        assert report.paper_outcome_joined_count == 1
        assert report.paper_outcome_by_label["good_trade_missed"] == 1
        assert report.paper_outcome_by_label["bad_trade_correctly_rejected"] == 1
        assert report.paper_outcome_by_label["good_trade_executed"] == 1
        assert report.paper_outcome_net_quote_total == pytest.approx(0.23)
        assert report.paper_outcome_fee_quote_total == pytest.approx(0.02)
        assert report.paper_outcome_slippage_quote_total == pytest.approx(0.04)
        assert report.paper_outcome_funding_quote_total == pytest.approx(0.02)
