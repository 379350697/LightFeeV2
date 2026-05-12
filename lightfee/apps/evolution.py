"""lightfee-evolution: proposal/review/outcome CLI."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from lightfee.config.loader import load_config
from lightfee.offline.evolution.cycle import (
    EvolutionCycle,
    CycleObservation,
    cycle_parameter_registry,
    load_cycle_observation,
    load_cycle_runs,
    persist_cycle_run,
    CycleRunRecord,
    evaluate_previous_action,
    CycleEvaluation,
)
from lightfee.offline.evolution.approval import apply_approval_overlay, reject_proposal
from lightfee.offline.llm_evolution.report import LLMEvolutionReport
from lightfee.persistence.ledgers import ProposalRecord


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-evolution: Parameter evolution")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--stage", choices=["propose", "review", "apply", "list-params", "list-runs"],
                        default="propose")
    parser.add_argument("--output-dir", "-o", default="data/evolution", help="Evolution output directory")
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--window-hours", type=int, default=24)
    parser.add_argument("--regime", default="unknown")
    parser.add_argument("--evidence", help="Path to evidence JSON")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.stage == "list-params":
        rules = cycle_parameter_registry()
        for r in rules:
            print(f"{r.path}: {r.value_kind} [{r.min_value}, {r.max_value}] step={r.step}")
        return

    if args.stage == "list-runs":
        runs = load_cycle_runs(args.output_dir)
        if not runs:
            print("No cycle runs found.")
        else:
            for r in runs:
                verdict = r.evaluation.verdict if r.evaluation else "none"
                print(f"{r.cycle_id} @ {r.generated_at_ms}: {verdict}")
        return

    if args.stage == "propose":
        evidence: dict = {}
        if args.evidence:
            evidence = json.loads(Path(args.evidence).read_text())

        observation = CycleObservation(
            sample_size=args.sample_size,
            evaluation_window_ms=args.window_hours * 3600 * 1000,
            regime_fingerprint=args.regime,
            objective_metrics=evidence.get("objective_metrics", {}),
        )
        load_cycle_observation(observation)

        profile = evidence.get("objective_profile", {
            "weighted_metrics": {"total_pnl_quote": 1.0},
        })

        cycle_id = f"cycle-{int(time.time() * 1000)}"
        cycle = EvolutionCycle(
            cycle_id=cycle_id,
            observation=observation,
            objective_profile=profile,
            evidence=evidence,
        )
        cycle.run()

        run = CycleRunRecord(
            cycle_id=cycle_id,
            generated_at_ms=int(time.time() * 1000),
            observation=observation,
            evaluation=cycle.evaluation,
            proposals=cycle.proposals,
            status=cycle.status,
        )
        path = persist_cycle_run(args.output_dir, run)

        print(f"Cycle {cycle_id} completed: {cycle.evaluation.verdict if cycle.evaluation else 'no evaluation'}")
        print(f"Proposals: {len(cycle.proposals)}")
        for p in cycle.proposals:
            print(f"  {p['parameter_path']}: {p['rationale']}")
        print(f"Saved to: {path}")

    elif args.stage == "review":
        # Check for LLM-enhanced review
        llm_report = LLMEvolutionReport.create_if_enabled("review", {})
        if llm_report:
            print(f"LLM evolution enabled (model={llm_report.llm_model})")
        else:
            print("LLM evolution disabled — using deterministic review")

        print(f"Evolution stage '{args.stage}': loaded config with {len(config.symbols)} symbols")

    elif args.stage == "apply":
        print(f"Evolution stage '{args.stage}': loaded config with {len(config.symbols)} symbols")
