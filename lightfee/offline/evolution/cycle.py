"""Evolution cycle: parameter registry, observation, evaluation, persistence.

Semantically equivalent to V1 Rust evolution/cycle.rs with:
- Bounded parameter registry (CycleParameterRule)
- Cycle observation validation (sample_size, window, regime_fingerprint)
- Previous action evaluation (improved/regressed/constrained/inconclusive)
- Cycle run persistence sorted by generated_at_ms
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CycleParameterRule:
    path: str
    value_kind: str  # "float", "integer", "unsigned_integer"
    min_value: float
    max_value: float
    step: float
    cooldown_cycles: int | None = 1
    coupled_metrics: list[str] = field(default_factory=list)


@dataclass
class CycleObservation:
    sample_size: int
    evaluation_window_ms: int
    regime_fingerprint: str
    objective_metrics: dict[str, float] = field(default_factory=dict)
    hard_risk_breaches: list[str] = field(default_factory=list)
    soft_risk_warnings: list[str] = field(default_factory=list)


@dataclass
class CycleEvaluation:
    verdict: str  # "improved", "regressed", "constrained", "inconclusive"
    objective_score: float = 0.0
    constraint_breaches: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class CycleRunRecord:
    cycle_id: str
    generated_at_ms: int
    observation: CycleObservation | None = None
    evaluation: CycleEvaluation | None = None
    proposals: list[dict[str, Any]] = field(default_factory=list)
    approvals: list[dict[str, Any]] = field(default_factory=list)
    status: str = "completed"


@dataclass
class EvolutionCycle:
    cycle_id: str
    status: str = "pending"
    observation: CycleObservation | None = None
    objective_profile: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    proposals: list[dict[str, Any]] = field(default_factory=list)
    approved: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)
    outcomes: list[dict[str, Any]] = field(default_factory=list)
    evaluation: CycleEvaluation | None = None

    def run(self) -> None:
        """Run one evolution cycle: evaluate previous action, generate proposals from evidence."""
        if self.observation is not None:
            self.evaluation = evaluate_previous_action(
                self.observation, self.objective_profile
            )

        if self.evidence:
            self._generate_proposals()

        self.status = "completed"

    def _generate_proposals(self) -> None:
        """Deterministic proposal generation from evidence signals.

        V1 semantics: proposals are driven by evidence gaps, not LLM.
        LLM enhancement is a separate opt-in layer.
        """
        rules = {r.path: r for r in cycle_parameter_registry()}
        proposals: list[dict[str, Any]] = []

        gap_signal = self.evidence.get("gap_signal", "")
        if gap_signal == "low_edge":
            if "strategy.min_expected_edge_bps" in rules:
                rule = rules["strategy.min_expected_edge_bps"]
                proposals.append({
                    "parameter_path": rule.path,
                    "current_value": rule.min_value,
                    "proposed_value": max(rule.min_value, rule.min_value - rule.step),
                    "rationale": "Edge threshold too low for current regime",
                    "coupled_metrics": rule.coupled_metrics,
                })

        rejected_count = self.evidence.get("rejected_candidates", 0)
        if rejected_count > 5 and "strategy.max_concurrent_positions" in rules:
            rule = rules["strategy.max_concurrent_positions"]
            proposals.append({
                "parameter_path": rule.path,
                "current_value": rule.min_value,
                "proposed_value": rule.min_value + rule.step,
                "rationale": "High candidate rejection suggests position cap too tight",
                "coupled_metrics": rule.coupled_metrics,
            })

        local_l2_gaps = self.evidence.get("local_l2_gaps", 0)
        if local_l2_gaps > 0 and "strategy.entry_local_l2_prewarm_window_secs" in rules:
            rule = rules["strategy.entry_local_l2_prewarm_window_secs"]
            proposals.append({
                "parameter_path": rule.path,
                "current_value": rule.min_value,
                "proposed_value": rule.min_value + rule.step,
                "rationale": "Local-L2 sequence gaps suggest prewarm window too short",
                "coupled_metrics": rule.coupled_metrics,
            })

        self.proposals = proposals


# ── Parameter Registry ────────────────────────────────────────────────────

def cycle_parameter_registry() -> list[CycleParameterRule]:
    """V1-equivalent bounded parameter registry.

    Mirrors V1 Rust evolution/cycle.rs::cycle_parameter_registry().
    """
    return [
        CycleParameterRule(
            path="strategy.min_expected_edge_bps",
            value_kind="float",
            min_value=0.5,
            max_value=20.0,
            step=0.5,
            cooldown_cycles=1,
            coupled_metrics=[
                "tradeable_count",
                "blocked_edge_count",
            ],
        ),
        CycleParameterRule(
            path="strategy.max_concurrent_positions",
            value_kind="unsigned_integer",
            min_value=1.0,
            max_value=32.0,
            step=1.0,
            cooldown_cycles=1,
            coupled_metrics=[
                "rejected_reason:max_concurrent_positions_reached",
                "counterfactual:possibly_overfiltered",
            ],
        ),
        CycleParameterRule(
            path="strategy.shadow_entry_opportunity_count",
            value_kind="unsigned_integer",
            min_value=0.0,
            max_value=8.0,
            step=1.0,
            cooldown_cycles=1,
            coupled_metrics=[
                "shadow_basket:tracked_snapshot_count",
                "shadow_basket:shadow_ready_ratio_avg",
            ],
        ),
        CycleParameterRule(
            path="strategy.primary_min_hold_ms",
            value_kind="integer",
            min_value=0.0,
            max_value=120000.0,
            step=1000.0,
            cooldown_cycles=1,
            coupled_metrics=[
                "shadow_basket:shadow_promotion_count",
                "shadow_blocked_reason:primary_hold_window",
            ],
        ),
        CycleParameterRule(
            path="strategy.shadow_promotion_score_delta_bps",
            value_kind="float",
            min_value=0.0,
            max_value=20.0,
            step=0.5,
            cooldown_cycles=1,
            coupled_metrics=[
                "shadow_basket:shadow_promotion_count",
                "shadow_blocked_reason:primary_hold_window",
            ],
        ),
        CycleParameterRule(
            path="strategy.entry_local_l2_prewarm_window_secs",
            value_kind="integer",
            min_value=60.0,
            max_value=900.0,
            step=30.0,
            cooldown_cycles=1,
            coupled_metrics=[
                "selection_blocker:entry_local_l2_waiting_for_prewarm_window",
                "entry_local_l2_primary_ready_count",
            ],
        ),
        CycleParameterRule(
            path="strategy.maker_entry_rest_timeout_ms",
            value_kind="unsigned_integer",
            min_value=200.0,
            max_value=30000.0,
            step=100.0,
            cooldown_cycles=1,
            coupled_metrics=[
                "order_error:timeout",
                "maker_fill_latency_ms",
            ],
        ),
    ]


# ── Cycle Observation ─────────────────────────────────────────────────────

def load_cycle_observation(observation: CycleObservation) -> CycleObservation:
    """Validate and return a cycle observation.

    V1: load_cycle_observation() in evolution/cycle.rs.
    Requires: sample_size > 0, evaluation_window_ms > 0, non-empty regime_fingerprint.
    """
    if observation.sample_size == 0:
        raise ValueError(
            f"cycle observation must include sample_size > 0"
        )
    if observation.evaluation_window_ms <= 0:
        raise ValueError(
            f"cycle observation must include evaluation_window_ms > 0"
        )
    if not observation.regime_fingerprint.strip():
        raise ValueError(
            f"cycle observation must include a non-empty regime_fingerprint"
        )
    return observation


# ── Previous Action Evaluation ────────────────────────────────────────────

def evaluate_previous_action(
    observation: CycleObservation,
    objective_profile: dict[str, Any],
) -> CycleEvaluation:
    """Evaluate previous cycle action against objective profile.

    V1: evaluate_previous_action() in evolution/cycle.rs.
    Returns: improved, regressed, constrained, or inconclusive.
    """
    if observation.sample_size == 0:
        raise ValueError("cycle observation must include sample_size > 0")
    if observation.evaluation_window_ms <= 0:
        raise ValueError("cycle observation must include evaluation_window_ms > 0")

    weighted_metrics: dict[str, float] = objective_profile.get("weighted_metrics", {})
    hard_risk: dict[str, dict[str, float]] = objective_profile.get(
        "hard_risk_envelope", {}
    )

    # Check hard risk breaches first — always constrained
    if observation.hard_risk_breaches:
        return CycleEvaluation(
            verdict="constrained",
            objective_score=0.0,
            constraint_breaches=list(observation.hard_risk_breaches),
            notes=[f"Hard risk breach: {b}" for b in observation.hard_risk_breaches],
        )

    if hard_risk:
        for breach_name, limits in hard_risk.items():
            current = limits.get("current", 0)
            max_allowed = limits.get("max_allowed", 0)
            if current > max_allowed:
                return CycleEvaluation(
                    verdict="constrained",
                    objective_score=0.0,
                    constraint_breaches=[breach_name],
                    notes=[f"Hard risk envelope breach: {breach_name}"],
                )

    # Compute weighted objective score
    weighted_sum = 0.0
    total_weight = 0.0
    for metric, weight in weighted_metrics.items():
        if metric in observation.objective_metrics:
            value = observation.objective_metrics[metric]
            weighted_sum += value * weight
            total_weight += abs(weight)

    if total_weight == 0.0:
        return CycleEvaluation(
            verdict="inconclusive",
            objective_score=0.0,
            constraint_breaches=[],
            notes=["No weighted metrics available for evaluation"],
        )

    score = weighted_sum / total_weight

    # Soft risk warnings
    if observation.soft_risk_warnings:
        return CycleEvaluation(
            verdict="constrained",
            objective_score=score,
            constraint_breaches=[],
            notes=[f"Soft risk warning: {w}" for w in observation.soft_risk_warnings],
        )

    # Determine verdict from score
    if score > 0.5:
        verdict = "improved"
        notes = [f"Objective score {score:.2f} indicates improvement"]
    elif score < -0.5:
        verdict = "regressed"
        notes = [f"Objective score {score:.2f} indicates regression"]
    else:
        verdict = "inconclusive"
        notes = [f"Objective score {score:.2f} is inconclusive"]

    return CycleEvaluation(
        verdict=verdict,
        objective_score=score,
        constraint_breaches=[],
        notes=notes,
    )


# ── Cycle Run Persistence ─────────────────────────────────────────────────

def cycle_runs_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / "cycle-runs.json"


def load_cycle_runs(output_dir: str | Path) -> list[CycleRunRecord]:
    """Load persisted cycle runs, sorted by generated_at_ms.

    V1: load_cycle_runs() in evolution/cycle.rs.
    """
    path = cycle_runs_path(output_dir)
    if not path.exists():
        return []

    raw = json.loads(path.read_text())
    runs: list[CycleRunRecord] = []
    for entry in raw:
        obs_data = entry.get("observation")
        observation = None
        if obs_data:
            observation = CycleObservation(
                sample_size=obs_data.get("sample_size", 0),
                evaluation_window_ms=obs_data.get("evaluation_window_ms", 0),
                regime_fingerprint=obs_data.get("regime_fingerprint", ""),
                objective_metrics=obs_data.get("objective_metrics", {}),
                hard_risk_breaches=obs_data.get("hard_risk_breaches", []),
                soft_risk_warnings=obs_data.get("soft_risk_warnings", []),
            )

        eval_data = entry.get("evaluation")
        evaluation = None
        if eval_data:
            evaluation = CycleEvaluation(
                verdict=eval_data.get("verdict", "inconclusive"),
                objective_score=eval_data.get("objective_score", 0.0),
                constraint_breaches=eval_data.get("constraint_breaches", []),
                notes=eval_data.get("notes", []),
            )

        runs.append(CycleRunRecord(
            cycle_id=entry["cycle_id"],
            generated_at_ms=entry["generated_at_ms"],
            observation=observation,
            evaluation=evaluation,
            proposals=entry.get("proposals", []),
            approvals=entry.get("approvals", []),
            status=entry.get("status", "completed"),
        ))

    runs.sort(key=lambda r: r.generated_at_ms)
    return runs


def persist_cycle_run(
    output_dir: str | Path,
    run: CycleRunRecord,
) -> Path:
    """Persist a cycle run and return the path.

    V1: persist_cycle_run() in evolution/cycle.rs.
    Loads existing runs, appends, sorts by generated_at_ms, and writes.
    """
    path = cycle_runs_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    runs = load_cycle_runs(output_dir)
    runs.append(run)
    runs.sort(key=lambda r: r.generated_at_ms)

    serialized: list[dict[str, Any]] = []
    for r in runs:
        entry: dict[str, Any] = {
            "cycle_id": r.cycle_id,
            "generated_at_ms": r.generated_at_ms,
            "status": r.status,
            "proposals": r.proposals,
            "approvals": r.approvals,
        }
        if r.observation:
            entry["observation"] = {
                "sample_size": r.observation.sample_size,
                "evaluation_window_ms": r.observation.evaluation_window_ms,
                "regime_fingerprint": r.observation.regime_fingerprint,
                "objective_metrics": r.observation.objective_metrics,
                "hard_risk_breaches": r.observation.hard_risk_breaches,
                "soft_risk_warnings": r.observation.soft_risk_warnings,
            }
        if r.evaluation:
            entry["evaluation"] = {
                "verdict": r.evaluation.verdict,
                "objective_score": r.evaluation.objective_score,
                "constraint_breaches": r.evaluation.constraint_breaches,
                "notes": r.evaluation.notes,
            }
        serialized.append(entry)

    path.write_text(json.dumps(serialized, indent=2, ensure_ascii=False))
    return path
