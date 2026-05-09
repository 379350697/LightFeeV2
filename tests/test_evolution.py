"""Tests for evolution and LLM evolution modules."""

import os
import tempfile
import sqlite3
from pathlib import Path

import pytest

from lightfee.offline.evolution.cycle import EvolutionCycle
from lightfee.offline.evolution.ledger import EvolutionLedger
from lightfee.offline.evolution.report import EvolutionReport
from lightfee.offline.evolution.approval import ApprovalQueue
from lightfee.offline.llm_evolution.report import LLMEvolutionReport
from lightfee.persistence.ledgers import (
    ApprovalRecord,
    ExperimentRecord,
    ProposalRecord,
)
from lightfee.persistence.sqlite_store import SqliteStore


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


class TestEvolutionCycle:
    def test_cycle_runs(self):
        cycle = EvolutionCycle(cycle_id="c1")
        cycle.run()
        assert cycle.status == "completed"


class TestLLMEvolution:
    def test_disabled_by_default(self):
        # Ensure env var is not set
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
