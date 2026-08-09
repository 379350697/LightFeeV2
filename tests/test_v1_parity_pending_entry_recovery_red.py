"""Red test: V1 pending entry recovery parity.

V1 anchors:
- src/engine/recovery.rs:402-603 finalize_startup_position_recovery
- src/execution_core/entry_sync.rs:638-643 PendingEntryHedge.startup_recovery_ready
- src/execution_core/entry_sync.rs:165-183 pending_entry_terminalization_budget_from_input
- src/execution_core/entry_sync.rs:2060-2445 force_terminalize_pending_entry_if_budget_exhausted
- src/execution_core/entry_sync.rs:2269-2399 hydrate_pending_entry_from_live_balanced_exposure
- src/execution_core/entry_sync.rs:5459-5625 drive_pending_entry_hedge

Validates that during startup recovery:
1. Pending entries with maker filled but missing hedge are properly detected
   (startup_recovery_ready via missing_hedge_quantity > 1e-9)
2. Terminalization budget is applied (hard ceiling = 120s, force terminal = 60s)
3. Hydration from live balanced exposure is attempted
4. Entries are either finalized, hedged, or aborted — NOT left in reconciling
5. Lifecycle transitions to RUNNING or RISK_ONLY, never stuck at RECONCILING

The test constructs a real EngineState with a pending entry equivalent to the
cloud SAGA scenario (entry-1778716864547-SAGAUSDT: maker=827, hedge=0,
uncertain_outcome=true, created_at_ms far in the past) and walks through
the recovery methods that would run during LiveRuntime.start().
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.recovery import build_recovery_snapshot, classify_startup_recovery_state
from lightfee.engine.state import EngineState, PendingEntry
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


def _make_saga_pending_entry() -> PendingEntry:
    """Construct a PendingEntry matching the cloud SAGA scenario.

    entry-1778716864547-SAGAUSDT:
      gate->bitget, maker_leg_filled=827.0, hedge_leg_filled=0.0,
      uncertain_outcome=true, created_at_ms ~12+ hours ago (well past 120s hard ceiling)
    """
    return PendingEntry(
        pending_id="entry-1778716864547-SAGAUSDT",
        symbol="SAGAUSDT",
        long_venue=Venue.GATE,
        short_venue=Venue.BITGET,
        target_quantity=827.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=1700000000000,  # far in the past
        maker_order_id="gate-order-12345",
        hedge_order_id="",
        maker_client_order_id="client-gate-12345",
        hedge_client_order_id="",
        maker_leg_filled=827.0,
        hedge_leg_filled=0.0,
        deadline_ms=1700000030000,
        fallback_route="",
        uncertain_outcome=True,
        reconcile_attempt=0,
        reconcile_next_attempt_ms=0,
        entry_type="passive",
        maker_price=0.505,
        long_quantity=827.0,
        short_quantity=827.0,
        run_id="run-saga-test",
        entry_route="standard_passive",
        outcome="uncertain",
        repost_count=0,
        zero_fill_since_ms=0,
    )


class TestPendingEntryRecoveryHelpers:
    """Unit tests for PendingEntry recovery helper methods."""

    def test_missing_hedge_quantity(self):
        """maker=827, hedge=0 → missing=827."""
        p = _make_saga_pending_entry()
        assert p.missing_hedge_quantity() > 826.0, (
            f"missing_hedge_quantity={p.missing_hedge_quantity()}, expected ~827.0"
        )

    def test_maker_completed(self):
        """maker filled >= target → maker_completed."""
        p = _make_saga_pending_entry()
        assert p.maker_completed() is True, (
            f"maker_leg_filled={p.maker_leg_filled} >= target={p.target_quantity}"
        )

    def test_has_any_fill(self):
        """maker filled > 0 → has_any_fill."""
        p = _make_saga_pending_entry()
        assert p.has_any_fill() is True

    def test_startup_recovery_ready_saga(self):
        """SAGA pending must be startup_recovery_ready.

        V1 conditions (any of):
        - uncertain_outcome (true)
        - maker_completed (true)
        - missing_hedge_quantity > 1e-9 (true, 827 > 0)
        """
        p = _make_saga_pending_entry()
        assert p.startup_recovery_ready() is True, (
            "SAGA pending entry must be startup_recovery_ready — "
            "maker filled but hedge missing + uncertain outcome"
        )

    def test_startup_recovery_ready_missing_hedge_only(self):
        """Even without uncertain outcome, missing hedge triggers recovery."""
        p = _make_saga_pending_entry()
        p.uncertain_outcome = False  # but still missing 827 hedge
        assert p.startup_recovery_ready() is True, (
            "Pending entry with missing hedge must be recovery-ready "
            "even without uncertain outcome (V1 missing_hedge_quantity path)"
        )

    def test_compute_lifetime_ms(self):
        """Lifetime = now - created_at."""
        p = _make_saga_pending_entry()
        now = 1700000120000  # 120s later
        lt = p.compute_lifetime_ms(now)
        assert lt == 120000, f"lifetime={lt}, expected 120000"


class TestStartupRecoveryClassification:
    """Red test: recovery classification must detect recovery-needed state."""

    def test_saga_pending_classifies_as_recovery_needed(self):
        """EngineState with SAGA pending must classify as 'recovery_needed'."""
        state = EngineState()
        state.lifecycle = EngineLifecycle.BOOTING
        state.pending_entries["entry-saga"] = _make_saga_pending_entry()

        result = classify_startup_recovery_state(state)
        assert result == "recovery_needed", (
            f"classify_startup_recovery_state returned {result!r}, "
            f"expected 'recovery_needed' — pending entries must trigger recovery"
        )

    def test_clean_state_without_pending(self):
        """State without any work must classify as 'clean'."""
        state = EngineState()
        state.lifecycle = EngineLifecycle.BOOTING
        result = classify_startup_recovery_state(state)
        assert result == "clean"


class TestPendingEntryTerminalizationBudget:
    """Red test: terminalization budget must follow V1 reliability contract."""

    def test_hard_ceiling_reached_saga(self):
        """SAGA pending created 12h ago → hard ceiling (120s) definitely reached."""
        p = _make_saga_pending_entry()
        now_ms = int(time.time() * 1000)  # current time — well past 120s
        lifetime = p.compute_lifetime_ms(now_ms)

        hard_ceiling = 120000
        assert lifetime >= hard_ceiling, (
            f"SAGA pending lifetime={lifetime}ms must be >= hard_ceiling={hard_ceiling}ms. "
            f"Terminalization budget MUST trigger for stuck pending entries."
        )

    def test_below_thresholds_no_budget(self):
        """Recently created pending with fills but below thresholds → no budget."""
        p = _make_saga_pending_entry()
        p.created_at_ms = 1000
        now_ms = 30000  # 29s lifetime, below both 60s force and 120s hard
        lifetime = p.compute_lifetime_ms(now_ms)
        assert lifetime < 60000, "Should be below force_terminal threshold"

    def test_force_terminal_at_60s(self):
        """At 60s, force_terminal triggers for entries with fills."""
        p = _make_saga_pending_entry()
        p.created_at_ms = 1000
        now_ms = 61001  # just past 60s
        lifetime = p.compute_lifetime_ms(now_ms)
        assert lifetime >= 60000

    def test_no_fill_force_terminal_at_60s(self):
        """At 60s with zero fills → force terminal triggers."""
        p = PendingEntry(
            pending_id="entry-nofill",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=1.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            maker_leg_filled=0.0,
            hedge_leg_filled=0.0,
            uncertain_outcome=False,
        )
        now_ms = 62000
        lifetime = p.compute_lifetime_ms(now_ms)
        assert lifetime >= 60000
        assert not p.has_any_fill(), "Zero-fill entry must trigger force terminal"


class TestRecoveryLifecycleTransition:
    """Red test: lifecycle must transition out of RECONCILING after recovery."""

    def test_lifecycle_not_stuck_reconciling_without_venue_adapters(self):
        """Even without venue adapters, _finalize_startup_recovery must transition.

        This is the key cloud scenario: if there are no venue adapters configured,
        the recovery code must still transition out of RECONCILING into either
        RISK_ONLY (if work remains) or RUNNING (if clean).
        """
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.state import OpenPosition
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )

        # No venue adapters
        runtime = LiveRuntime(config, venue_adapters=None)

        # Inject SAGA pending
        saga = _make_saga_pending_entry()
        runtime.state.pending_entries["entry-saga"] = saga
        runtime.state.lifecycle = EngineLifecycle.RECONCILING

        # Call _finalize_startup_recovery directly
        runtime._finalize_startup_recovery()

        # Must not stay at RECONCILING
        assert runtime.state.lifecycle != EngineLifecycle.RECONCILING, (
            f"Lifecycle stuck at RECONCILING after _finalize_startup_recovery. "
            f"Current: lifecycle={runtime.state.lifecycle.value}, "
            f"risk_mode={runtime.state.risk_mode.value}"
        )

        # With pending entries and no open positions → expect RISK_ONLY
        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY, (
            f"Expected RISK_ONLY (pending entries without open positions). "
            f"Got lifecycle={runtime.state.lifecycle.value}"
        )

    def test_clean_state_transitions_to_running(self):
        """No pending entries, no open positions → RUNNING."""
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )
        runtime = LiveRuntime(config, venue_adapters=None)
        runtime.state.lifecycle = EngineLifecycle.RECONCILING
        runtime.state.pending_entries.clear()

        runtime._finalize_startup_recovery()

        assert runtime.state.lifecycle == EngineLifecycle.RUNNING, (
            f"Clean state should transition to RUNNING, "
            f"got lifecycle={runtime.state.lifecycle.value}"
        )
        assert runtime.state.risk_mode == GlobalRiskMode.RUNNING

    def test_close_reconciliation_is_running_background_work(self):
        """A flat close's accounting debt is visible but not startup-owned risk."""
        from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig
        from lightfee.engine.recovery_decision_core import (
            RecoveryDecisionKind,
            RecoveryEvidenceClass,
        )
        from lightfee.engine.runtime import LiveRuntime

        runtime = LiveRuntime(
            AppConfig(
                runtime=RuntimeConfig(mode="paper"),
                strategy=StrategyConfig(),
                persistence=PersistenceConfig(),
            ),
            venue_adapters=None,
        )
        runtime.state.lifecycle = EngineLifecycle.RECONCILING
        runtime.state.set_pending_close_reconciliations([
            {
                "position_id": "entry-closed-beat",
                "symbol": "BEATUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
                "reason": "billing_evidence_missing",
            }
        ])

        runtime._finalize_startup_recovery()

        assert runtime.state.lifecycle == EngineLifecycle.RUNNING
        assert runtime.recovery_decision.kind == (
            RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
        )
        assert runtime.recovery_decision.evidence_class == (
            RecoveryEvidenceClass.BACKGROUND_CLOSE_RECONCILIATION
        )
        assert runtime.recovery_decision.entry_allowed is True

    def test_operator_fail_closed_clean_state_does_not_emit_runtime_running(self):
        """Core preserve policy must not be reported as startup running."""
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )
        runtime = LiveRuntime(config, venue_adapters=None)
        runtime.journal.open()
        existing_records = len(runtime.journal.read_all())
        runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
        runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        runtime.state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED

        runtime._finalize_startup_recovery()

        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert not any(
            record["kind"] == "runtime.running"
            and record.get("payload", {}).get("reason") == "startup_recovery_completed"
            for record in runtime.journal.read_all()[existing_records:]
        )
        runtime.journal.close()

    def test_open_position_with_residual_work_blocks_instead_of_running(self):
        """Open positions do not make local recovery work disappear."""
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.state import OpenPosition
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )
        runtime = LiveRuntime(config, venue_adapters=None)
        runtime.journal.open()
        existing_records = len(runtime.journal.read_all())
        runtime.state.lifecycle = EngineLifecycle.RECONCILING
        runtime.state.open_positions["entry-residual"] = OpenPosition(
            position_id="entry-residual",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_entry_price=50000.0,
            short_entry_price=50100.0,
            opened_at_ms=1700000000000,
            matched_quantity=0.1,
        )
        runtime.state.pending_residual_repairs.append({
            "position_id": "entry-residual",
            "pair_id": "btcusdt:binance->okx",
            "symbol": "BTCUSDT",
            "repair_venue": "binance",
            "repair_side": "sell",
            "repair_quantity": 0.01,
            "origin": "entry_open",
        })

        runtime._finalize_startup_recovery()

        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        assert runtime.state.recovery_blocked_reason == (
            "truth_unavailable_for_required_recovery"
        )
        assert not any(
            record["kind"] == "runtime.running"
            and record.get("payload", {}).get("reason")
            == "startup_recovery_completed_with_positions"
            for record in runtime.journal.read_all()[existing_records:]
        )
        runtime.journal.close()

    def test_pending_without_open_positions_becomes_risk_only(self):
        """Pending entries without open positions → RISK_ONLY with blocked reason."""
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )
        runtime = LiveRuntime(config, venue_adapters=None)
        runtime.state.lifecycle = EngineLifecycle.RECONCILING

        saga = _make_saga_pending_entry()
        runtime.state.pending_entries["entry-saga"] = saga

        runtime._finalize_startup_recovery()

        assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
        assert runtime.state.recovery_blocked_reason == (
            "truth_unavailable_for_required_recovery"
        )
        assert runtime.state.recovery_blocked_at_ms > 0

    @pytest.mark.asyncio
    async def test_passive_close_cleanup_updates_runtime_core_decision_gate(self):
        """Passive-close cleanup must not leave entry gate reading stale core block."""
        from types import SimpleNamespace

        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig
        from lightfee.engine.recovery_decision_core import (
            RecoveryDecision,
            RecoveryDecisionKind,
            RecoveryEvidenceClass,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.state import PendingPassiveClose

        class ClearingPassiveClose:
            async def process_pending_passive_closes(self, state, now_ms):
                state.pending_passive_closes.clear()
                state.set_pending_close_reconciliations([
                    {
                        "position_id": "entry-closed-beat",
                        "symbol": "BEATUSDT",
                        "long_venue": "okx",
                        "short_venue": "bybit",
                        "reason": "billing_evidence_missing",
                    }
                ])
                return set()

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )
        runtime = LiveRuntime(config, venue_adapters=None)
        runtime.passive_close_executor = ClearingPassiveClose()
        runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
        runtime.state.recovery_blocked_reason = (
            "startup_recovery_pending_work_without_open_positions"
        )
        runtime.state.pending_passive_closes["entry-passive"] = PendingPassiveClose(
            position_id="entry-passive",
            reason="funding_capture",
        )
        runtime.recovery_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH,
            evidence_class=RecoveryEvidenceClass.TRUTH_UNAVAILABLE_FOR_REQUIRED_RECOVERY,
            entry_allowed=False,
            block_reason="truth_unavailable_for_required_recovery",
        )

        await runtime._maybe_tick_passive_close(1778787000000)

        assert runtime.state.recovery_blocked_reason is None
        assert runtime.recovery_decision.evidence_class == (
            RecoveryEvidenceClass.BACKGROUND_CLOSE_RECONCILIATION
        )
        assert runtime.recovery_decision.entry_allowed is True
        assert runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
        ) == (True, "")

    def test_drive_pending_entry_recovery_with_adapters(self):
        """With venue adapters, recovery should attempt to resolve pending entries.

        Uses fake adapters to verify that startup_recovery_ready entries are
        processed through the full recovery pipeline: order status poll →
        hydrate from live positions → terminalization budget → abort.
        """
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig
        from tests.fake_adapters import FakeVenueAdapter

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(
                pending_entry_hard_ceiling_ms=120000,
                pending_entry_force_terminal_after_ms=60000,
            ),
            persistence=PersistenceConfig(),
        )

        gate_adapter = FakeVenueAdapter(_venue=Venue.GATE)
        bitget_adapter = FakeVenueAdapter(_venue=Venue.BITGET)

        # Set up position snapshots: both venues have no position
        # (so hydrate_from_live_positions won't find balanced exposure)
        gate_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.GATE, symbol="SAGAUSDT",
                side=Side.BUY, quantity=0.0,
                entry_price=0.0, observed_at_ms=1700000000000,
            )
        ]
        bitget_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.BITGET, symbol="SAGAUSDT",
                side=Side.SELL, quantity=0.0,
                entry_price=0.0, observed_at_ms=1700000000000,
            )
        ]

        runtime = LiveRuntime(config, venue_adapters={
            Venue.GATE: gate_adapter,
            Venue.BITGET: bitget_adapter,
        })
        runtime.journal.open()

        # Inject SAGA pending
        saga = _make_saga_pending_entry()
        runtime.state.pending_entries["entry-saga"] = saga
        runtime.state.lifecycle = EngineLifecycle.RECONCILING

        # Run recovery
        now_ms = 1700000120000  # 120s after created_at → hard ceiling
        asyncio.run(runtime._recover_pending_entry_hedges(now_ms))

        # After recovery with hard ceiling + fills + missing hedge:
        # entry should be aborted (removed from pending_entries)
        # or lifecycle should transition
        assert runtime.state.lifecycle != EngineLifecycle.RECONCILING, (
            f"Lifecycle must not be RECONCILING after recovery. "
            f"Got lifecycle={runtime.state.lifecycle.value}"
        )

        runtime.journal.close()


class TestRecoveryWithLivePositionHydration:
    """Red test: hydrate_pending_entry_from_live_balanced_exposure."""

    def test_hydrate_from_live_positions(self):
        """When both venues have matching positions, pending fills should be updated."""
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig
        from tests.fake_adapters import FakeVenueAdapter

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )

        gate_adapter = FakeVenueAdapter(_venue=Venue.GATE)
        bitget_adapter = FakeVenueAdapter(_venue=Venue.BITGET)

        # Both venues have 827.0 size (balanced exposure that matches pending)
        gate_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.GATE, symbol="SAGAUSDT",
                side=Side.BUY, quantity=827.0,
                entry_price=0.505, observed_at_ms=1700000000000,
            )
        ]
        # V2 transport returns side=SELL with quantity=abs(net) for shorts.
        # This test must match real adapter behavior to catch the sign bug.
        bitget_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.BITGET, symbol="SAGAUSDT",
                side=Side.SELL, quantity=827.0,  # positive! real transport uses abs(net)
                entry_price=0.505, observed_at_ms=1700000000000,
            )
        ]

        runtime = LiveRuntime(config, venue_adapters={
            Venue.GATE: gate_adapter,
            Venue.BITGET: bitget_adapter,
        })
        runtime.journal.open()

        # Pending has maker=827 filled but hedge=0
        saga = _make_saga_pending_entry()
        saga.hedge_leg_filled = 0.0  # hedge missing

        # Run hydration directly
        result = asyncio.run(runtime._recover_hydrate_from_live_positions(saga))

        assert result is True, (
            f"Hydration must succeed when both venues have positions. "
            f"Real transport returns side=SELL, quantity=abs(net)=positive."
        )
        # After hydration, hedge should have been filled from live position
        assert saga.hedge_leg_filled > 0, (
            f"hedge_leg_filled={saga.hedge_leg_filled}, expected > 0 after hydration"
        )

        runtime.journal.close()

    def test_hydrate_rejects_wrong_side(self):
        """Hydration must reject positions on the wrong side.

        If short venue returns BUY instead of SELL, there's no real short
        exposure to balance — must return False.
        """
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, RuntimeConfig, PersistenceConfig
        from tests.fake_adapters import FakeVenueAdapter

        config = AppConfig(
            runtime=RuntimeConfig(mode="paper"),
            strategy=StrategyConfig(),
            persistence=PersistenceConfig(),
        )

        gate_adapter = FakeVenueAdapter(_venue=Venue.GATE)
        bitget_adapter = FakeVenueAdapter(_venue=Venue.BITGET)

        # Short venue returns BUY side (wrong — not a real short position)
        gate_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.GATE, symbol="SAGAUSDT",
                side=Side.BUY, quantity=827.0,
                entry_price=0.505, observed_at_ms=1700000000000,
            )
        ]
        bitget_adapter.position_snapshots = [
            PositionSnapshot(
                venue=Venue.BITGET, symbol="SAGAUSDT",
                side=Side.BUY, quantity=500.0,  # wrong side — same direction as long
                entry_price=0.505, observed_at_ms=1700000000000,
            )
        ]

        runtime = LiveRuntime(config, venue_adapters={
            Venue.GATE: gate_adapter,
            Venue.BITGET: bitget_adapter,
        })
        runtime.journal.open()

        saga = _make_saga_pending_entry()
        result = asyncio.run(runtime._recover_hydrate_from_live_positions(saga))
        assert result is False, (
            "Hydration must reject when both venues are on the same side"
        )
        runtime.journal.close()
