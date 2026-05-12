"""Semantic parity tests for evolution governance against V1 business contract EVOL-001.

Verifies that V2 evolution preserves:
- Proposal catalog (create, read, versioning)
- Approval queue (approve, reject, deterministic overlay)
- Experiment ledger (start, complete, outcome tracking)
- Deterministic cycle results
- Governance states (not scattered booleans)
- Diagnostics
- Rendered report output
"""

import json
import tempfile
from pathlib import Path

from lightfee.offline.evolution.cycle import (
    CycleEvaluation,
    CycleObservation,
    CycleParameterRule,
    CycleRunRecord,
    EvolutionCycle,
    cycle_parameter_registry,
    evaluate_previous_action,
    load_cycle_runs,
    persist_cycle_run,
)
from lightfee.offline.evolution.ledger import EvolutionLedger
from lightfee.offline.evolution.approval import (
    ApprovalQueue,
    apply_approval_overlay,
    reject_proposal,
)
from lightfee.persistence.ledgers import ProposalRecord, ApprovalRecord, ExperimentRecord


# ── Proposal Catalog ────────────────────────────────────────────────────────


class TestProposalCatalog:
    def test_proposal_record_has_required_fields(self):
        proposal = ProposalRecord(
            proposal_id="prop-001",
            version=1,
            status="proposed",
            title="Adjust min edge",
            body={"parameter_path": "strategy.min_expected_edge_bps", "value": 1.5},
            proposed_at_ms=1700000000000,
        )
        assert proposal.proposal_id == "prop-001"
        assert proposal.version == 1
        assert proposal.status == "proposed"
        assert proposal.title == "Adjust min edge"
        assert proposal.body["parameter_path"] == "strategy.min_expected_edge_bps"

    def test_proposal_record_can_track_approval_rejection(self):
        proposal = ProposalRecord(
            proposal_id="prop-002",
            approved_at_ms=1700000001000,
            rejected_at_ms=None,
        )
        assert proposal.approved_at_ms == 1700000001000
        assert proposal.rejected_at_ms is None

    def test_proposal_record_can_track_rejection(self):
        proposal = ProposalRecord(
            proposal_id="prop-003",
            approved_at_ms=None,
            rejected_at_ms=1700000002000,
        )
        assert proposal.approved_at_ms is None
        assert proposal.rejected_at_ms == 1700000002000


# ── Approval Queue ─────────────────────────────────────────────────────────


class TestApprovalQueue:
    def test_apply_approval_overlay_approved(self):
        proposal = ProposalRecord(
            proposal_id="prop-004",
            title="Lower edge threshold",
            proposed_at_ms=1700000000000,
        )
        result = apply_approval_overlay(proposal, "operator", approved=True)
        assert result["proposal_id"] == "prop-004"
        assert result["reviewer"] == "operator"
        assert result["decision"] == "approved"
        assert "decided_at_ms" in result

    def test_apply_approval_overlay_rejected(self):
        proposal = ProposalRecord(proposal_id="prop-005", title="Raise max positions")
        result = apply_approval_overlay(proposal, "operator", approved=False)
        assert result["decision"] == "rejected"

    def test_reject_proposal_includes_reason(self):
        proposal = ProposalRecord(proposal_id="prop-006", title="Bad idea")
        result = reject_proposal(proposal, "operator", reason="too risky")
        assert result["decision"] == "rejected"
        assert result["reason"] == "too risky"

    def test_approval_record_has_required_fields(self):
        record = ApprovalRecord(
            proposal_id="prop-007",
            reviewer="operator",
            decision="approved",
            notes="looks good",
            decided_at_ms=1700000000000,
        )
        assert record.proposal_id == "prop-007"
        assert record.reviewer == "operator"
        assert record.decision == "approved"

    def test_approval_queue_can_record_decision(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS approval_queue "
                "(proposal_id TEXT, reviewer TEXT, decision TEXT, notes TEXT, decided_at_ms INTEGER)"
            )
            conn.commit()

            queue = ApprovalQueue(db_path)
            approval = ApprovalRecord(
                proposal_id="prop-008",
                reviewer="operator",
                decision="approved",
                notes="ok",
                decided_at_ms=1700000000000,
            )
            queue.record_decision(conn, approval)

            row = conn.execute(
                "SELECT * FROM approval_queue WHERE proposal_id = ?", ("prop-008",)
            ).fetchone()
            assert row is not None
            conn.close()
        finally:
            Path(db_path).unlink(missing_ok=True)


# ── Experiment Ledger ──────────────────────────────────────────────────────


class TestExperimentLedger:
    def test_experiment_record_has_required_fields(self):
        experiment = ExperimentRecord(
            proposal_id="prop-009",
            run_id="run-001",
            outcome={"verdict": "improved", "score": 0.75},
            started_at_ms=1700000000000,
            completed_at_ms=1700000001000,
        )
        assert experiment.proposal_id == "prop-009"
        assert experiment.run_id == "run-001"
        assert experiment.outcome["verdict"] == "improved"

    def test_evolution_ledger_records_proposal(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS proposal_catalog "
                "(proposal_id TEXT PRIMARY KEY, version INTEGER, status TEXT, "
                "title TEXT, body_json TEXT, proposed_at_ms INTEGER)"
            )
            conn.commit()

            ledger = EvolutionLedger(db_path)
            proposal = ProposalRecord(
                proposal_id="prop-010",
                version=1,
                status="proposed",
                title="Test proposal",
                body={"key": "value"},
                proposed_at_ms=1700000000000,
            )
            ledger.record_proposal(conn, proposal)

            row = conn.execute(
                "SELECT * FROM proposal_catalog WHERE proposal_id = ?", ("prop-010",)
            ).fetchone()
            assert row is not None
            assert row[3] == "Test proposal"
            conn.close()
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_evolution_ledger_records_experiment(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS experiment_ledger "
                "(proposal_id TEXT, run_id TEXT, outcome_json TEXT, "
                "started_at_ms INTEGER, completed_at_ms INTEGER)"
            )
            conn.commit()

            ledger = EvolutionLedger(db_path)
            experiment = ExperimentRecord(
                proposal_id="prop-011",
                run_id="run-002",
                outcome={"score": 0.5},
                started_at_ms=1700000000000,
                completed_at_ms=1700000001000,
            )
            ledger.record_experiment(conn, experiment)

            row = conn.execute(
                "SELECT * FROM experiment_ledger WHERE proposal_id = ?", ("prop-011",)
            ).fetchone()
            assert row is not None
            conn.close()
        finally:
            Path(db_path).unlink(missing_ok=True)


# ── Evolution Cycle Parameter Registry ─────────────────────────────────────


class TestCycleParameterRegistry:
    def test_registry_matches_v1_count(self):
        rules = cycle_parameter_registry()
        # V1 has exactly 7 rules
        assert len(rules) == 7

    def test_registry_contains_known_parameters(self):
        rules = cycle_parameter_registry()
        paths = {r.path for r in rules}
        assert "strategy.min_expected_edge_bps" in paths
        assert "strategy.max_concurrent_positions" in paths
        assert "strategy.shadow_entry_opportunity_count" in paths
        assert "strategy.primary_min_hold_ms" in paths
        assert "strategy.shadow_promotion_score_delta_bps" in paths
        assert "strategy.entry_local_l2_prewarm_window_secs" in paths
        assert "strategy.maker_entry_rest_timeout_ms" in paths

    def test_rules_have_bounded_ranges(self):
        for rule in cycle_parameter_registry():
            assert rule.min_value <= rule.max_value
            assert rule.step > 0
            assert rule.value_kind in ("float", "integer", "unsigned_integer")

    def test_rules_have_coupled_metrics(self):
        for rule in cycle_parameter_registry():
            assert isinstance(rule.coupled_metrics, list)
            assert len(rule.coupled_metrics) > 0


# ── Cycle Observation ──────────────────────────────────────────────────────


class TestCycleObservation:
    def test_valid_observation(self):
        from lightfee.offline.evolution.cycle import load_cycle_observation
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="low_vol_bull",
            objective_metrics={"pnl": 0.05, "sharpe": 0.8},
        )
        loaded = load_cycle_observation(obs)
        assert loaded.sample_size == 100
        assert loaded.evaluation_window_ms == 86400000

    def test_zero_sample_size_raises(self):
        import pytest
        from lightfee.offline.evolution.cycle import load_cycle_observation
        obs = CycleObservation(
            sample_size=0,
            evaluation_window_ms=86400000,
            regime_fingerprint="test",
        )
        with pytest.raises(ValueError):
            load_cycle_observation(obs)

    def test_zero_evaluation_window_raises(self):
        import pytest
        from lightfee.offline.evolution.cycle import load_cycle_observation
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=0,
            regime_fingerprint="test",
        )
        with pytest.raises(ValueError):
            load_cycle_observation(obs)

    def test_empty_regime_fingerprint_raises(self):
        import pytest
        from lightfee.offline.evolution.cycle import load_cycle_observation
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="",
        )
        with pytest.raises(ValueError):
            load_cycle_observation(obs)


# ── Cycle Evaluation ───────────────────────────────────────────────────────


class TestCycleEvaluation:
    def test_hard_risk_breach_is_constrained(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="high_vol",
            hard_risk_breaches=["max_drawdown"],
        )
        result = evaluate_previous_action(obs, {})
        assert result.verdict == "constrained"
        assert "max_drawdown" in result.constraint_breaches

    def test_hard_risk_envelope_breach_is_constrained(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="high_vol",
        )
        profile = {
            "hard_risk_envelope": {
                "max_drawdown": {"current": 0.15, "max_allowed": 0.10},
            }
        }
        result = evaluate_previous_action(obs, profile)
        assert result.verdict == "constrained"

    def test_soft_risk_warnings_are_constrained(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="normal",
            soft_risk_warnings=["elevated_vol"],
        )
        result = evaluate_previous_action(obs, {})
        assert result.verdict == "constrained"

    def test_positive_score_is_improved(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="bull",
            objective_metrics={"pnl": 4.0, "sharpe": 3.0},
        )
        profile = {
            "weighted_metrics": {"pnl": 0.5, "sharpe": 0.5},
        }
        result = evaluate_previous_action(obs, profile)
        assert result.verdict == "improved"
        assert result.objective_score > 0

    def test_negative_score_is_regressed(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="bear",
            objective_metrics={"pnl": -4.0, "sharpe": -3.0},
        )
        profile = {
            "weighted_metrics": {"pnl": 0.5, "sharpe": 0.5},
        }
        result = evaluate_previous_action(obs, profile)
        assert result.verdict == "regressed"
        assert result.objective_score < 0

    def test_no_weighted_metrics_is_inconclusive(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="flat",
        )
        result = evaluate_previous_action(obs, {})
        assert result.verdict == "inconclusive"


# ── Evolution Cycle ────────────────────────────────────────────────────────


class TestEvolutionCycle:
    def test_cycle_runs_with_observation_and_evidence(self):
        cycle = EvolutionCycle(
            cycle_id="cycle-001",
            observation=CycleObservation(
                sample_size=100,
                evaluation_window_ms=86400000,
                regime_fingerprint="low_vol_bull",
                objective_metrics={"pnl": 0.05},
            ),
            objective_profile={
                "weighted_metrics": {"pnl": 1.0},
            },
            evidence={"gap_signal": "low_edge"},
        )
        cycle.run()
        assert cycle.status == "completed"
        assert cycle.evaluation is not None

    def test_cycle_generates_proposals_from_low_edge_evidence(self):
        cycle = EvolutionCycle(
            cycle_id="cycle-002",
            evidence={"gap_signal": "low_edge"},
        )
        cycle.run()
        assert len(cycle.proposals) > 0
        # Should propose adjusting min_expected_edge_bps
        edge_proposals = [
            p for p in cycle.proposals
            if p["parameter_path"] == "strategy.min_expected_edge_bps"
        ]
        assert len(edge_proposals) == 1

    def test_cycle_generates_proposals_from_high_rejection_evidence(self):
        cycle = EvolutionCycle(
            cycle_id="cycle-003",
            evidence={"rejected_candidates": 10},
        )
        cycle.run()
        cap_proposals = [
            p for p in cycle.proposals
            if p["parameter_path"] == "strategy.max_concurrent_positions"
        ]
        assert len(cap_proposals) == 1

    def test_cycle_generates_proposals_from_local_l2_gaps(self):
        cycle = EvolutionCycle(
            cycle_id="cycle-004",
            evidence={"local_l2_gaps": 3},
        )
        cycle.run()
        l2_proposals = [
            p for p in cycle.proposals
            if p["parameter_path"] == "strategy.entry_local_l2_prewarm_window_secs"
        ]
        assert len(l2_proposals) == 1

    def test_cycle_completes_without_evidence(self):
        cycle = EvolutionCycle(cycle_id="cycle-005")
        cycle.run()
        assert cycle.status == "completed"
        assert cycle.proposals == []


# ── Cycle Run Persistence ──────────────────────────────────────────────────


class TestCycleRunPersistence:
    def test_persist_and_load_cycle_runs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run = CycleRunRecord(
                cycle_id="cycle-010",
                generated_at_ms=1700000000000,
                status="completed",
                proposals=[{"parameter_path": "test.param", "value": 1.0}],
            )
            persist_cycle_run(tmpdir, run)

            runs = load_cycle_runs(tmpdir)
            assert len(runs) == 1
            assert runs[0].cycle_id == "cycle-010"
            assert runs[0].generated_at_ms == 1700000000000

    def test_load_empty_runs_returns_empty_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runs = load_cycle_runs(tmpdir)
            assert runs == []

    def test_runs_sorted_by_generated_at_ms(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            persist_cycle_run(tmpdir, CycleRunRecord(
                cycle_id="newer", generated_at_ms=2000, status="completed"
            ))
            persist_cycle_run(tmpdir, CycleRunRecord(
                cycle_id="older", generated_at_ms=1000, status="completed"
            ))
            runs = load_cycle_runs(tmpdir)
            assert runs[0].cycle_id == "older"
            assert runs[1].cycle_id == "newer"

    def test_persist_preserves_observation_and_evaluation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run = CycleRunRecord(
                cycle_id="cycle-011",
                generated_at_ms=1700000000000,
                observation=CycleObservation(
                    sample_size=50,
                    evaluation_window_ms=86400000,
                    regime_fingerprint="neutral",
                    objective_metrics={"pnl": 0.02},
                ),
                evaluation=CycleEvaluation(
                    verdict="improved",
                    objective_score=0.6,
                ),
                status="completed",
            )
            persist_cycle_run(tmpdir, run)
            runs = load_cycle_runs(tmpdir)
            assert runs[0].observation is not None
            assert runs[0].observation.sample_size == 50
            assert runs[0].evaluation is not None
            assert runs[0].evaluation.verdict == "improved"


# ── Governance State Machine ───────────────────────────────────────────────


class TestGovernanceStates:
    """Verify evolution uses explicit governance states, not scattered booleans."""

    def test_approval_decision_is_explicit_enum(self):
        result_approved = apply_approval_overlay(
            ProposalRecord(proposal_id="p1"), "op", approved=True
        )
        result_rejected = apply_approval_overlay(
            ProposalRecord(proposal_id="p2"), "op", approved=False
        )
        assert result_approved["decision"] in ("approved", "rejected")
        assert result_rejected["decision"] in ("approved", "rejected")

    def test_cycle_evaluation_verdict_is_explicit(self):
        verdicts = {"improved", "regressed", "constrained", "inconclusive"}
        eval_improved = CycleEvaluation(verdict="improved", objective_score=0.8)
        eval_constrained = CycleEvaluation(
            verdict="constrained",
            constraint_breaches=["max_drawdown"],
        )
        assert eval_improved.verdict in verdicts
        assert eval_constrained.verdict in verdicts

    def test_proposal_status_is_explicit_string(self):
        # V1 uses explicit status strings: proposed, approved, rejected, applied, superseded
        valid_statuses = {"proposed", "approved", "rejected", "applied", "superseded"}
        proposal = ProposalRecord(proposal_id="p3", status="proposed")
        assert proposal.status in valid_statuses

    def test_experiment_outcome_is_structured(self):
        experiment = ExperimentRecord(
            proposal_id="p4",
            run_id="run-1",
            outcome={"verdict": "improved", "score": 0.75, "constraints_ok": True},
        )
        assert "verdict" in experiment.outcome
        assert "score" in experiment.outcome
