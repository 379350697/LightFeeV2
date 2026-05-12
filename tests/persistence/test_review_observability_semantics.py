"""Semantic parity tests for review observability in persistence and replay.

V1 references:
- src/observability_ops/journal_bridge.rs
- src/observability_ops/replay_bridge.rs

Validates that review_id survives:
1. Journal write → read round-trip
2. Journal replay state reconstruction
3. Position normalization in replay
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from lightfee.persistence.journal import Journal, replay_journal_records


class TestReviewIdJournalRoundTrip:
    """review_id must survive journal write → read round-trip."""

    def test_review_id_in_entry_opened_survives_round_trip(self):
        journal_path = Path(tempfile.mkdtemp()) / "roundtrip.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            journal.append(
                "entry.opened",
                {
                    "position_id": "pos-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 50000.0,
                    "short_entry_price": 50000.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 1.0,
                    "review_id": "rev-abc123def456",
                },
            )
            records = journal.read_all()
            assert len(records) == 1
            assert records[0]["payload"]["review_id"] == "rev-abc123def456"
        finally:
            journal.close()


class TestReviewIdReplay:
    """review_id survives replay reconstruction."""

    def test_replay_preserves_review_id_in_position_snapshot(self):
        records = [
            {
                "seq": 1,
                "run_id": "test-run",
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-replay-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 50000.0,
                    "short_entry_price": 50000.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 1.0,
                    "review_id": "rev-replay-001",
                },
            },
        ]
        state = replay_journal_records(records)
        assert state["open_position_count"] == 1
        assert "pos-replay-1" in state["positions"]
        pos = state["positions"]["pos-replay-1"]
        assert pos["review_id"] == "rev-replay-001"

    def test_replay_preserves_null_review_id(self):
        """When review_id is not present in payload, it should be None after replay."""
        records = [
            {
                "seq": 1,
                "run_id": "test-run",
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-noreview-1",
                    "symbol": "ETHUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 0.1,
                    "long_quantity": 0.1,
                    "short_quantity": 0.1,
                    "long_entry_price": 3000.0,
                    "short_entry_price": 3000.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 0.1,
                },
            },
        ]
        state = replay_journal_records(records)
        pos = state["positions"]["pos-noreview-1"]
        assert pos["review_id"] is None

    def test_replay_preserves_review_id_in_live_detected(self):
        """review_id in recovery.live_detected should also be preserved."""
        records = [
            {
                "seq": 1,
                "run_id": "test-run",
                "ts_ms": 1000,
                "kind": "recovery.live_detected",
                "payload": {
                    "position_id": "pos-live-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 50000.0,
                    "short_entry_price": 50000.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 1.0,
                    "review_id": "rev-live-001",
                },
            },
        ]
        state = replay_journal_records(records)
        pos = state["positions"]["pos-live-1"]
        assert pos["review_id"] == "rev-live-001"

    def test_review_assigned_in_timeline(self):
        """review.assigned events should appear in replay timeline."""
        records = [
            {
                "seq": 1,
                "run_id": "test-run",
                "ts_ms": 1000,
                "kind": "review.assigned",
                "payload": {
                    "position_id": "pos-1",
                    "review_id": "rev-timeline-001",
                },
            },
            {
                "seq": 2,
                "run_id": "test-run",
                "ts_ms": 1001,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 50000.0,
                    "short_entry_price": 50000.0,
                    "opened_at_ms": 1001,
                    "matched_quantity": 1.0,
                    "review_id": "rev-timeline-001",
                },
            },
        ]
        state = replay_journal_records(records)
        timeline_kinds = {t["kind"] for t in state["timeline"]}
        assert "review.assigned" in timeline_kinds


class TestReviewIdWithSeedState:
    """review_id preserved when replay starts from a seed state."""

    def test_seed_state_preserves_review_id(self):
        seed = {
            "lifecycle": "running",
            "risk_mode": "running",
            "open_positions": {
                "pos-seed-1": {
                    "position_id": "pos-seed-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "bybit",
                    "short_venue": "bybit",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 50000.0,
                    "short_entry_price": 50000.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 1.0,
                    "review_id": "rev-seed-001",
                },
            },
        }
        state = replay_journal_records([], seed_state=seed)
        pos = state["positions"]["pos-seed-1"]
        assert pos["review_id"] == "rev-seed-001"
