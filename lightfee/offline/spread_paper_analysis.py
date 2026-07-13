"""Offline, epoch-safe analysis for the spread-paper journal.

The acceptance cohort is deliberately limited to official, taker/taker v2
closures.  Legacy and control results remain inspectable but cannot silently
inflate the sample count or the reported edge.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from math import sqrt
from random import Random
from statistics import median, stdev
from typing import Iterable


DEFAULT_ALLOWED_OPPORTUNITY_LABELS = ("spread_reversion",)
DEFAULT_MODEL_EPOCH = "v2_signed_reversion"


@dataclass
class SpreadPaperGroupStats:
    closed_count: int = 0
    win_count: int = 0
    net_quote_total: float = 0.0
    gross_quote_total: float = 0.0
    fee_quote_total: float = 0.0
    slippage_quote_total: float = 0.0
    funding_quote_total: float = 0.0
    hedge_delay_quote_total: float = 0.0
    residual_quote_total: float = 0.0
    adverse_selection_assumption_quote_total: float = 0.0
    _net_quotes: list[float] = field(default_factory=list, repr=False)

    @property
    def win_rate(self) -> float:
        return self.win_count / self.closed_count if self.closed_count else 0.0

    @property
    def mean_net_quote(self) -> float:
        return self.net_quote_total / self.closed_count if self.closed_count else 0.0

    @property
    def median_net_quote(self) -> float:
        return float(median(self._net_quotes)) if self._net_quotes else 0.0

    @property
    def net_quote_stddev(self) -> float:
        return float(stdev(self._net_quotes)) if len(self._net_quotes) >= 2 else 0.0

    @property
    def profit_factor(self) -> float:
        gains = sum(value for value in self._net_quotes if value > 0.0)
        losses = -sum(value for value in self._net_quotes if value < 0.0)
        return gains / losses if losses > 0.0 else (float("inf") if gains else 0.0)

    @property
    def max_drawdown_quote(self) -> float:
        peak = running = 0.0
        drawdown = 0.0
        for value in self._net_quotes:
            running += value
            peak = max(peak, running)
            drawdown = max(drawdown, peak - running)
        return drawdown

    def add(self, payload: dict) -> None:
        net_quote = _float(payload.get("paper_net_quote"))
        self.closed_count += 1
        self.win_count += int(net_quote > 0.0)
        self.net_quote_total += net_quote
        self.gross_quote_total += _float(payload.get("paper_gross_quote"))
        self.fee_quote_total += _float(payload.get("paper_fee_quote"))
        self.slippage_quote_total += _float(payload.get("paper_slippage_quote"))
        self.funding_quote_total += _float(payload.get("paper_funding_quote"))
        self.hedge_delay_quote_total += _float(payload.get("paper_hedge_delay_quote"))
        self.residual_quote_total += _float(payload.get("paper_residual_quote"))
        self.adverse_selection_assumption_quote_total += _float(
            payload.get("paper_adverse_selection_assumption_quote")
        )
        self._net_quotes.append(net_quote)


@dataclass
class SpreadPaperAnalysisReport(SpreadPaperGroupStats):
    model_epoch: str = DEFAULT_MODEL_EPOCH
    excluded_symbols: list[str] = field(default_factory=list)
    allowed_opportunity_labels: list[str] = field(default_factory=list)
    independent_episode_count: int = 0
    partial_count: int = 0
    unfilled_count: int = 0
    expired_count: int = 0
    stale_or_unpriced_count: int = 0
    excluded_legacy_count: int = 0
    excluded_nonofficial_count: int = 0
    excluded_execution_cohort_count: int = 0
    calculation_version_mismatch_count: int = 0
    invalid_economics_count: int = 0
    duplicate_episode_count: int = 0
    filled_count: int = 0
    stress_net_quote_mean_by_multiplier: dict[str, float] = field(default_factory=dict)
    by_symbol: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_venue_pair: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_label: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_bot: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_cohort: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_volatility_regime: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_sample_split: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    _stress_net_quotes: dict[str, list[float]] = field(default_factory=dict, repr=False)

    @property
    def stale_or_unpriced_ratio(self) -> float:
        denominator = self.closed_count + self.stale_or_unpriced_count
        return self.stale_or_unpriced_count / denominator if denominator else 0.0

    @property
    def bootstrap_net_quote_ci95(self) -> tuple[float, float]:
        return block_bootstrap_mean_ci95(self._net_quotes)

    def add_stress_observation(self, payload: dict) -> None:
        net = _float(payload.get("paper_net_quote"))
        costs = (
            _float(payload.get("paper_fee_quote"))
            + _float(payload.get("paper_slippage_quote"))
            + _float(payload.get("paper_adverse_selection_assumption_quote"))
        )
        for multiplier in (1.5, 2.0):
            key = f"{multiplier:g}x"
            self._stress_net_quotes.setdefault(key, []).append(net - (multiplier - 1.0) * costs)

    def finalize_stress(self) -> None:
        self.stress_net_quote_mean_by_multiplier = {
            key: sum(values) / len(values) if values else 0.0
            for key, values in sorted(self._stress_net_quotes.items())
        }


def analyze_spread_paper_events(
    records: Iterable[dict],
    *,
    excluded_symbols: Iterable[str] | None = None,
    allowed_opportunity_labels: Iterable[str] | None = DEFAULT_ALLOWED_OPPORTUNITY_LABELS,
    model_epoch: str = DEFAULT_MODEL_EPOCH,
    include_nonofficial: bool = False,
    require_taker_taker: bool = True,
) -> SpreadPaperAnalysisReport:
    """Analyse a single model epoch without silently mixing control/legacy PnL."""
    excluded = _symbol_set(excluded_symbols)
    allowed_labels = _label_set(allowed_opportunity_labels)
    report = SpreadPaperAnalysisReport(
        model_epoch=str(model_epoch or DEFAULT_MODEL_EPOCH),
        excluded_symbols=sorted(excluded),
        allowed_opportunity_labels=sorted(allowed_labels),
    )
    episode_ids: set[str] = set()
    accepted: list[tuple[int, int, dict, str, str]] = []

    for ordinal, record in enumerate(records):
        kind = str(record.get("kind", "") or "")
        if kind not in {"opportunity.paper_closed", "opportunity.paper_expired"}:
            continue
        payload = record.get("payload", {})
        if not isinstance(payload, dict):
            continue
        if str(payload.get("model_epoch", "v1_legacy") or "v1_legacy") != report.model_epoch:
            report.excluded_legacy_count += 1
            continue
        if str(payload.get("calculation_version", "") or "") != "spread_paper_v3":
            report.calculation_version_mismatch_count += 1
            continue
        symbol = str(payload.get("symbol", "") or "").upper()
        if not symbol or symbol in excluded:
            continue
        label = str(payload.get("candidate_opportunity_label", "") or "spread_reversion")
        if allowed_labels and label not in allowed_labels:
            continue
        if kind == "opportunity.paper_expired":
            report.expired_count += 1
        status = str(payload.get("paper_order_status", "") or "")
        if status == "FILLED":
            report.filled_count += 1
        elif status == "PARTIAL":
            report.partial_count += 1
        elif status in {"WORKING", "CANCELED", "EXPIRED", "UNKNOWN"}:
            report.unfilled_count += 1
        # Journal records are an untrusted persistence boundary.  A string
        # such as ``"false"`` is truthy in Python, while a string ``"true"``
        # could otherwise create an official/eligible result.  Acceptance
        # reporting must require literal JSON booleans for all gating fields.
        if payload.get("paper_unpriced") is not False:
            report.stale_or_unpriced_count += 1
            continue
        if payload.get("official_pnl") is not True and not include_nonofficial:
            report.excluded_nonofficial_count += 1
            continue
        if require_taker_taker and not _is_taker_taker_acceptance_payload(payload):
            report.excluded_execution_cohort_count += 1
            continue
        if payload.get("paper_net_quote") is None:
            report.stale_or_unpriced_count += 1
            continue
        if not _paper_economics_are_finite(payload):
            report.invalid_economics_count += 1
            continue

        accepted.append((_event_time_ms(record, payload), ordinal, payload, label, symbol))

    # JSONL append order is normally chronological, but restored/merged
    # journal segments need not be.  Drawdown, group paths and block bootstrap
    # must be invariant to the input-file ordering, not merely to their totals.
    for _, _, payload, label, symbol in sorted(accepted, key=lambda item: (item[0], item[1])):
        episode_id = _episode_id(payload)
        if episode_id in episode_ids:
            report.duplicate_episode_count += 1
            continue
        episode_ids.add(episode_id)

        bot_id = str(payload.get("paper_bot_id", "") or "tt_conservative")
        cohort = str(payload.get("paper_cohort", "") or "baseline_current")
        pair = str(payload.get("pair_id", "") or _pair_id(payload))
        regime = str(payload.get("volatility_regime", "") or "unknown")
        sample_split = str(payload.get("research_sample_split", "") or "unspecified")
        report.add(payload)
        report.add_stress_observation(payload)
        report.by_symbol.setdefault(symbol, SpreadPaperGroupStats()).add(payload)
        report.by_venue_pair.setdefault(pair, SpreadPaperGroupStats()).add(payload)
        report.by_label.setdefault(label, SpreadPaperGroupStats()).add(payload)
        report.by_bot.setdefault(bot_id, SpreadPaperGroupStats()).add(payload)
        report.by_cohort.setdefault(cohort, SpreadPaperGroupStats()).add(payload)
        report.by_volatility_regime.setdefault(regime, SpreadPaperGroupStats()).add(payload)
        report.by_sample_split.setdefault(sample_split, SpreadPaperGroupStats()).add(payload)

    report.independent_episode_count = len(episode_ids)
    report.finalize_stress()
    return report


def spread_paper_report_dict(report: SpreadPaperAnalysisReport) -> dict:
    """Return a stable JSON-ready report without private sample buffers."""
    return {
        **_group_stats_dict(report),
        "model_epoch": report.model_epoch,
        "excluded_symbols": list(report.excluded_symbols),
        "allowed_opportunity_labels": list(report.allowed_opportunity_labels),
        "independent_episode_count": report.independent_episode_count,
        "partial_count": report.partial_count,
        "unfilled_count": report.unfilled_count,
        "expired_count": report.expired_count,
        "stale_or_unpriced_count": report.stale_or_unpriced_count,
        "stale_or_unpriced_ratio": report.stale_or_unpriced_ratio,
        "excluded_legacy_count": report.excluded_legacy_count,
        "excluded_nonofficial_count": report.excluded_nonofficial_count,
        "excluded_execution_cohort_count": report.excluded_execution_cohort_count,
        "calculation_version_mismatch_count": report.calculation_version_mismatch_count,
        "invalid_economics_count": report.invalid_economics_count,
        "duplicate_episode_count": report.duplicate_episode_count,
        "filled_count": report.filled_count,
        "bootstrap_net_quote_ci95": list(report.bootstrap_net_quote_ci95),
        "stress_net_quote_mean_by_multiplier": dict(report.stress_net_quote_mean_by_multiplier),
        "by_symbol": _grouped_stats_dict(report.by_symbol),
        "by_venue_pair": _grouped_stats_dict(report.by_venue_pair),
        "by_label": _grouped_stats_dict(report.by_label),
        "by_bot": _grouped_stats_dict(report.by_bot),
        "by_cohort": _grouped_stats_dict(report.by_cohort),
        "by_volatility_regime": _grouped_stats_dict(report.by_volatility_regime),
        "by_sample_split": _grouped_stats_dict(report.by_sample_split),
    }


def block_bootstrap_mean_ci95(
    observations: Iterable[float],
    *,
    replicas: int = 2_000,
    seed: int = 0,
) -> tuple[float, float]:
    """Deterministic moving-block bootstrap CI for the independent episodes."""
    values = [float(value) for value in observations]
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], values[0]
    block_size = max(1, min(len(values), int(sqrt(len(values)))))
    random = Random(seed)
    means: list[float] = []
    for _ in range(max(int(replicas), 1)):
        sample: list[float] = []
        while len(sample) < len(values):
            start = random.randrange(len(values))
            sample.extend(values[(start + offset) % len(values)] for offset in range(block_size))
        means.append(sum(sample[: len(values)]) / len(values))
    means.sort()
    lower = means[int((len(means) - 1) * 0.025)]
    upper = means[int((len(means) - 1) * 0.975)]
    return lower, upper


def _episode_id(payload: dict) -> str:
    candidate_id = str(payload.get("candidate_id", "") or "")
    registered_at_ms = int(payload.get("registered_at_ms", 0) or 0)
    if candidate_id:
        return f"{candidate_id}:{registered_at_ms}"
    return _pair_id(payload) + f":{registered_at_ms}"


def _pair_id(payload: dict) -> str:
    return ":".join(
        (
            str(payload.get("long_venue", "") or "").lower(),
            str(payload.get("short_venue", "") or "").lower(),
            str(payload.get("symbol", "") or "").upper(),
        )
    )


def _symbol_set(symbols: Iterable[str] | None) -> set[str]:
    return {str(symbol).upper() for symbol in symbols or () if str(symbol).strip()}


def _label_set(labels: Iterable[str] | None) -> set[str]:
    return {str(label) for label in labels or () if str(label).strip()}


def _is_taker_taker_acceptance_payload(payload: dict) -> bool:
    return (
        str(payload.get("paper_entry_mode", "") or "") == "long_taker:short_taker"
        and str(payload.get("paper_exit_mode", "") or "") == "long_taker:short_taker"
        and payload.get("paper_control_group") is False
        and payload.get("acceptance_eligible") is True
    )


def _paper_economics_are_finite(payload: dict) -> bool:
    """Keep malformed JSON values out of official paper statistics.

    ``NaN`` and infinity are valid Python floats and can enter permissive JSON
    written by another process.  They must invalidate the record, never turn
    into a zero-cost or a positive outcome in acceptance reporting.
    """
    fields = (
        "paper_net_quote",
        "paper_gross_quote",
        "paper_fee_quote",
        "paper_slippage_quote",
        "paper_funding_quote",
        "paper_hedge_delay_quote",
        "paper_residual_quote",
        "paper_adverse_selection_assumption_quote",
    )
    for metric_name in fields:
        value = payload.get(metric_name)
        if value is None:
            continue
        try:
            if not math.isfinite(float(value)):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _event_time_ms(record: dict, payload: dict) -> int:
    for value in (
        payload.get("evaluated_at_ms"),
        record.get("ts_ms"),
        record.get("time_ms"),
        payload.get("registered_at_ms"),
    ):
        try:
            return max(int(value or 0), 0)
        except (TypeError, ValueError, OverflowError):
            continue
    return 0


def _group_stats_dict(group: SpreadPaperGroupStats) -> dict:
    profit_factor = group.profit_factor
    return {
        "closed_count": group.closed_count,
        "win_count": group.win_count,
        "win_rate": group.win_rate,
        "net_quote_total": group.net_quote_total,
        "gross_quote_total": group.gross_quote_total,
        "fee_quote_total": group.fee_quote_total,
        "slippage_quote_total": group.slippage_quote_total,
        "funding_quote_total": group.funding_quote_total,
        "hedge_delay_quote_total": group.hedge_delay_quote_total,
        "residual_quote_total": group.residual_quote_total,
        "adverse_selection_assumption_quote_total": group.adverse_selection_assumption_quote_total,
        "mean_net_quote": group.mean_net_quote,
        "median_net_quote": group.median_net_quote,
        "net_quote_stddev": group.net_quote_stddev,
        "max_drawdown_quote": group.max_drawdown_quote,
        # A no-loss cohort has an unbounded profit factor.  JSON has no
        # portable Infinity literal, so preserve that state as null rather
        # than emitting a non-standard number into the acceptance artefact.
        "profit_factor": profit_factor if math.isfinite(profit_factor) else None,
    }


def _grouped_stats_dict(groups: dict[str, SpreadPaperGroupStats]) -> dict[str, dict]:
    return {key: _group_stats_dict(value) for key, value in sorted(groups.items())}


def _float(value: object) -> float:
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return numeric if math.isfinite(numeric) else 0.0
