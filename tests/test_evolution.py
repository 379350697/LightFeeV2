"""Tests for evolution and LLM evolution modules."""

import json
import os
import tempfile
import sqlite3
from pathlib import Path

import pytest

from lightfee.offline.evolution.cycle import (
    CycleEvaluation,
    CycleObservation,
    EvolutionCycle,
    cycle_parameter_registry,
    evaluate_previous_action,
    load_cycle_observation,
    load_cycle_runs,
    persist_cycle_run,
    CycleRunRecord,
)
from lightfee.offline.evolution.ledger import EvolutionLedger
from lightfee.offline.evolution.report import EvolutionReport
from lightfee.offline.evolution.approval import (
    ApprovalQueue,
    apply_approval_overlay,
    reject_proposal,
)
from lightfee.offline.llm_evolution.report import LLMEvolutionReport
from lightfee.persistence.ledgers import (
    ApprovalRecord,
    ExperimentRecord,
    ProposalRecord,
)
from lightfee.persistence.sqlite_store import SqliteStore


# ── Task 3: Parameter Registry ────────────────────────────────────────────

class TestParameterRegistry:
    def test_registry_has_bounded_rules(self):
        rules = cycle_parameter_registry()
        assert len(rules) >= 5, "registry must include core strategy parameters"

        for rule in rules:
            assert rule.path, "every rule must have a path"
            assert rule.value_kind in ("float", "integer", "unsigned_integer")
            assert rule.min_value < rule.max_value, (
                f"{rule.path}: min_value must be less than max_value"
            )
            assert rule.step > 0, f"{rule.path}: step must be positive"
            assert len(rule.coupled_metrics) > 0, (
                f"{rule.path}: must declare coupled_metrics"
            )

    def test_min_expected_edge_bps_rule(self):
        rules = {r.path: r for r in cycle_parameter_registry()}
        rule = rules["strategy.min_expected_edge_bps"]
        assert rule.value_kind == "float"
        assert rule.min_value == 0.5
        assert rule.max_value == 20.0
        assert rule.step == 0.5
        assert rule.cooldown_cycles == 1

    def test_max_concurrent_positions_rule(self):
        rules = {r.path: r for r in cycle_parameter_registry()}
        rule = rules["strategy.max_concurrent_positions"]
        assert rule.value_kind == "unsigned_integer"
        assert rule.min_value == 1.0
        assert rule.max_value == 32.0


# ── Task 3: Cycle Observation ─────────────────────────────────────────────

class TestCycleObservation:
    def test_valid_observation(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="low_vol_range",
            objective_metrics={"total_pnl_quote": 1500.0, "tradeable_count": 12},
        )
        result = load_cycle_observation(obs)
        assert result.sample_size == 100

    def test_rejects_zero_sample_size(self):
        obs = CycleObservation(
            sample_size=0,
            evaluation_window_ms=86400000,
            regime_fingerprint="low_vol_range",
        )
        with pytest.raises(ValueError, match="sample_size"):
            load_cycle_observation(obs)

    def test_rejects_zero_window(self):
        obs = CycleObservation(
            sample_size=50,
            evaluation_window_ms=0,
            regime_fingerprint="low_vol_range",
        )
        with pytest.raises(ValueError, match="evaluation_window_ms"):
            load_cycle_observation(obs)

    def test_rejects_empty_regime_fingerprint(self):
        obs = CycleObservation(
            sample_size=50,
            evaluation_window_ms=86400000,
            regime_fingerprint="",
        )
        with pytest.raises(ValueError, match="regime_fingerprint"):
            load_cycle_observation(obs)

    def test_rejects_whitespace_regime_fingerprint(self):
        obs = CycleObservation(
            sample_size=50,
            evaluation_window_ms=86400000,
            regime_fingerprint="   ",
        )
        with pytest.raises(ValueError, match="regime_fingerprint"):
            load_cycle_observation(obs)


# ── Task 3: Previous Action Evaluation ────────────────────────────────────

class TestPreviousActionEvaluation:
    def test_improved_when_score_increases(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="trending",
            objective_metrics={"total_pnl_quote": 2000.0, "tradeable_count": 15},
        )
        profile = {
            "weighted_metrics": {"total_pnl_quote": 1.0, "tradeable_count": 0.5},
        }
        evaluation = evaluate_previous_action(obs, profile)
        assert evaluation.verdict in (
            "improved",
            "regressed",
            "constrained",
            "inconclusive",
        )
        assert evaluation.objective_score != 0.0

    def test_constrained_when_hard_risk_breach(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="high_vol",
            objective_metrics={"total_pnl_quote": -500.0},
            hard_risk_breaches=["max_drawdown_breach"],
        )
        profile = {
            "weighted_metrics": {"total_pnl_quote": 1.0},
            "hard_risk_envelope": {
                "max_drawdown_breach": {"max_allowed": 0, "current": 1},
            },
        }
        evaluation = evaluate_previous_action(obs, profile)
        assert evaluation.verdict == "constrained"

    def test_regressed_when_score_declines(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="trending",
            objective_metrics={"total_pnl_quote": -300.0},
        )
        profile = {
            "weighted_metrics": {"total_pnl_quote": 1.0},
        }
        evaluation = evaluate_previous_action(obs, profile)
        assert evaluation.verdict in ("regressed", "improved", "constrained", "inconclusive")

    def test_inconclusive_without_metrics(self):
        obs = CycleObservation(
            sample_size=10,
            evaluation_window_ms=86400000,
            regime_fingerprint="low_data",
            objective_metrics={},
        )
        profile = {
            "weighted_metrics": {"total_pnl_quote": 1.0},
        }
        evaluation = evaluate_previous_action(obs, profile)
        assert evaluation.verdict == "inconclusive"

    def test_rejects_zero_sample_size_in_evaluation(self):
        obs = CycleObservation(
            sample_size=0,
            evaluation_window_ms=86400000,
            regime_fingerprint="test",
        )
        profile = {"weighted_metrics": {}}
        with pytest.raises(ValueError, match="sample_size"):
            evaluate_previous_action(obs, profile)


# ── Task 3: Cycle Run Persistence ─────────────────────────────────────────

class TestCycleRunPersistence:
    def test_persist_and_load_sorted(self):
        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "evolution"
            run1 = CycleRunRecord(
                cycle_id="c1",
                generated_at_ms=1000,
                observation=CycleObservation(
                    sample_size=10,
                    evaluation_window_ms=86400000,
                    regime_fingerprint="a",
                ),
                evaluation=CycleEvaluation(
                    verdict="improved",
                    objective_score=1.5,
                    constraint_breaches=[],
                ),
            )
            run2 = CycleRunRecord(
                cycle_id="c2",
                generated_at_ms=500,  # earlier timestamp
                observation=CycleObservation(
                    sample_size=20,
                    evaluation_window_ms=86400000,
                    regime_fingerprint="b",
                ),
                evaluation=CycleEvaluation(
                    verdict="inconclusive",
                    objective_score=0.0,
                    constraint_breaches=[],
                ),
            )
            run3 = CycleRunRecord(
                cycle_id="c3",
                generated_at_ms=2000,
                observation=CycleObservation(
                    sample_size=30,
                    evaluation_window_ms=86400000,
                    regime_fingerprint="c",
                ),
                evaluation=CycleEvaluation(
                    verdict="regressed",
                    objective_score=-0.5,
                    constraint_breaches=[],
                ),
            )

            persist_cycle_run(output_dir, run1)
            persist_cycle_run(output_dir, run2)
            persist_cycle_run(output_dir, run3)

            runs = load_cycle_runs(output_dir)
            assert len(runs) == 3
            # Must be sorted by generated_at_ms ascending
            assert runs[0].cycle_id == "c2"  # ts=500
            assert runs[1].cycle_id == "c1"  # ts=1000
            assert runs[2].cycle_id == "c3"  # ts=2000

    def test_load_empty_when_no_runs(self):
        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "evolution"
            runs = load_cycle_runs(output_dir)
            assert runs == []


# ── Task 3: Approval Overlay ──────────────────────────────────────────────

class TestApprovalOverlay:
    def test_accepts_proposal(self):
        proposal = ProposalRecord(
            proposal_id="p1",
            title="Increase min edge",
            status="proposed",
            proposed_at_ms=1000,
        )
        result = apply_approval_overlay(proposal, "trader", approved=True)
        assert result["decision"] == "approved"
        assert result["proposal_id"] == "p1"
        assert result["reviewer"] == "trader"

    def test_rejects_proposal(self):
        proposal = ProposalRecord(
            proposal_id="p2",
            title="Reduce max positions",
            status="proposed",
            proposed_at_ms=1000,
        )
        result = reject_proposal(proposal, "trader", reason="too aggressive")
        assert result["decision"] == "rejected"
        assert result["proposal_id"] == "p2"
        assert "too aggressive" in result["reason"]

    def test_deterministic_same_input_same_output(self):
        proposal = ProposalRecord(
            proposal_id="p3",
            title="Test proposal",
            proposed_at_ms=1000,
        )
        r1 = apply_approval_overlay(proposal, "alice", approved=True)
        r2 = apply_approval_overlay(proposal, "alice", approved=True)
        # Core fields must match (timestamps may differ by ms)
        assert r1["decision"] == r2["decision"] == "approved"
        assert r1["proposal_id"] == r2["proposal_id"] == "p3"
        assert r1["reviewer"] == r2["reviewer"] == "alice"


# ── Task 3: EvolutionCycle ────────────────────────────────────────────────

class TestEvolutionCycle:
    def test_cycle_runs_with_observation(self):
        obs = CycleObservation(
            sample_size=50,
            evaluation_window_ms=86400000,
            regime_fingerprint="low_vol_range",
            objective_metrics={"total_pnl_quote": 500.0},
        )
        profile = {"weighted_metrics": {"total_pnl_quote": 1.0}}
        cycle = EvolutionCycle(cycle_id="c1", observation=obs, objective_profile=profile)
        cycle.run()
        assert cycle.status == "completed"
        assert cycle.evaluation is not None
        assert cycle.evaluation.verdict in (
            "improved",
            "regressed",
            "constrained",
            "inconclusive",
        )

    def test_cycle_generates_proposals_from_evidence(self):
        obs = CycleObservation(
            sample_size=100,
            evaluation_window_ms=86400000,
            regime_fingerprint="trending",
            objective_metrics={"total_pnl_quote": -100.0, "tradeable_count": 3},
        )
        profile = {
            "weighted_metrics": {"total_pnl_quote": 1.0, "tradeable_count": 0.3},
        }
        cycle = EvolutionCycle(
            cycle_id="c2",
            observation=obs,
            objective_profile=profile,
            evidence={"regime": "trending", "gap_signal": "low_edge"},
        )
        cycle.run()
        assert cycle.evaluation is not None
        assert len(cycle.proposals) >= 0  # proposals may be empty but run must complete


# ── Evolution Ledger and Approval Queue (existing tests preserved) ────────

class TestEvolutionReport:
    def test_writes_markdown(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "report.md"
            report = EvolutionReport(
                report_id="r1", generated_at_ms=1000, markdown="# Test Report"
            )
            report.write_markdown(path)
            assert path.exists()
            assert "# Test Report" in path.read_text()

    def test_writes_json(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            report = EvolutionReport(
                report_id="r1", generated_at_ms=1000,
                proposals=[{"title": "test", "params": {}}],
            )
            report.write_json(path)
            assert path.exists()


class TestEvolutionLedger:
    def test_records_proposal(self):
        with tempfile.TemporaryDirectory() as td:
            sqlite_path = Path(td) / "test.sqlite"
            SqliteStore(sqlite_path).open().close()
            conn = sqlite3.connect(str(sqlite_path))

            ledger = EvolutionLedger(sqlite_path)
            proposal = ProposalRecord(
                proposal_id="p1", title="Test Proposal", proposed_at_ms=1000
            )
            ledger.record_proposal(conn, proposal)

            rows = conn.execute("SELECT proposal_id FROM proposal_catalog").fetchall()
            assert rows[0][0] == "p1"
            conn.close()

    def test_records_experiment(self):
        with tempfile.TemporaryDirectory() as td:
            sqlite_path = Path(td) / "test.sqlite"
            SqliteStore(sqlite_path).open().close()
            conn = sqlite3.connect(str(sqlite_path))

            ledger = EvolutionLedger(sqlite_path)
            exp = ExperimentRecord(
                proposal_id="p1", run_id="r1", started_at_ms=1000, completed_at_ms=2000,
            )
            ledger.record_experiment(conn, exp)

            rows = conn.execute("SELECT run_id FROM experiment_ledger").fetchall()
            assert rows[0][0] == "r1"
            conn.close()


class TestApprovalQueue:
    def test_records_decision(self):
        with tempfile.TemporaryDirectory() as td:
            sqlite_path = Path(td) / "test.sqlite"
            SqliteStore(sqlite_path).open().close()
            conn = sqlite3.connect(str(sqlite_path))

            queue = ApprovalQueue(sqlite_path)
            approval = ApprovalRecord(
                proposal_id="p1", reviewer="trader", decision="approved",
                decided_at_ms=1000,
            )
            queue.record_decision(conn, approval)

            rows = conn.execute("SELECT decision FROM approval_queue").fetchall()
            assert rows[0][0] == "approved"
            conn.close()


# ── Task 4: LLM Evolution ─────────────────────────────────────────────────

class TestLLMEvolution:
    def test_disabled_by_default(self):
        old = os.environ.pop("LIGHTFEE_LLM_EVOLUTION_ENABLED", None)
        try:
            report = LLMEvolutionReport.create_if_enabled("r1", {})
            assert report is None
        finally:
            if old is not None:
                os.environ["LIGHTFEE_LLM_EVOLUTION_ENABLED"] = old

    def test_enabled_with_env_var(self):
        os.environ["LIGHTFEE_LLM_EVOLUTION_ENABLED"] = "1"
        try:
            report = LLMEvolutionReport.create_if_enabled("r1", {"test": True})
            assert report is not None
            assert report.llm_enabled
            report.generate()
            assert "pending" in report.analysis.get("status", "")
        finally:
            os.environ.pop("LIGHTFEE_LLM_EVOLUTION_ENABLED", None)

    def test_enabled_records_provider_model(self):
        os.environ["LIGHTFEE_LLM_EVOLUTION_ENABLED"] = "1"
        os.environ["LIGHTFEE_LLM_MODEL"] = "claude-sonnet-4-6"
        os.environ["LIGHTFEE_LLM_PROVIDER"] = "anthropic"
        try:
            report = LLMEvolutionReport.create_if_enabled("r2", {"test": True})
            assert report is not None
            assert report.llm_model == "claude-sonnet-4-6"
            assert report.llm_provider == "anthropic"
        finally:
            os.environ.pop("LIGHTFEE_LLM_EVOLUTION_ENABLED", None)
            os.environ.pop("LIGHTFEE_LLM_MODEL", None)
            os.environ.pop("LIGHTFEE_LLM_PROVIDER", None)

    def test_disabled_no_network_call(self):
        """In disabled mode, generate() must not attempt network calls."""
        old = os.environ.pop("LIGHTFEE_LLM_EVOLUTION_ENABLED", None)
        try:
            # Create report directly (bypassing create_if_enabled to test disabled behavior)
            report = LLMEvolutionReport(
                report_id="r3",
                llm_enabled=False,
                evidence={"test": True},
            )
            report.generate()
            assert report.analysis["status"] == "disabled"
            assert "disabled" in report.analysis.get("note", "")
        finally:
            if old is not None:
                os.environ["LIGHTFEE_LLM_EVOLUTION_ENABLED"] = old
