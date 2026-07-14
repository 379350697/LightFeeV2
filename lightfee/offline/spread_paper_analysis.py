"""Offline, epoch-safe analysis for the spread-paper journal.

The acceptance cohort is deliberately limited to official, taker/taker v2
closures.  Legacy and control results remain inspectable but cannot silently
inflate the sample count or the reported edge.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from math import sqrt
from random import Random
from statistics import median, stdev
from typing import Iterable

from lightfee.strategy.fee_evidence import TRUSTED_FEE_EVIDENCE_KEY_ID


DEFAULT_ALLOWED_OPPORTUNITY_LABELS = ("spread_reversion",)
DEFAULT_MODEL_EPOCH = "v2_signed_reversion"
CURRENT_PAPER_JOURNAL_SCHEMA_VERSION = 6


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
    source_evidence_verified: bool = False
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
    excluded_evidence_count: int = 0
    journal_schema_mismatch_count: int = 0
    calculation_version_mismatch_count: int = 0
    invalid_economics_count: int = 0
    duplicate_episode_count: int = 0
    manifest_digest_missing_count: int = 0
    manifest_digest_mismatch_count: int = 0
    research_manifest_digest: str = ""
    excluded_in_sample_count: int = 0
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

    @property
    def acceptance_ready(self) -> bool:
        """Whether the input is internally coherent enough for promotion.

        This deliberately says nothing about statistical sufficiency or
        profitability.  It only prevents a report containing ambiguous,
        malformed, duplicate, or unauthenticated evidence from being treated
        as a clean acceptance artefact.
        """
        return bool(
            self.source_evidence_verified
            and self.independent_episode_count > 0
            and not any(
                (
                    self.excluded_evidence_count,
                    self.invalid_economics_count,
                    self.duplicate_episode_count,
                    self.manifest_digest_missing_count,
                    self.manifest_digest_mismatch_count,
                )
            )
        )

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
    model_epoch: str,
    include_nonofficial: bool = False,
    require_taker_taker: bool = True,
    require_out_of_sample: bool = False,
    required_journal_schema_version: int | None = CURRENT_PAPER_JOURNAL_SCHEMA_VERSION,
    source_evidence_verified: bool = False,
) -> SpreadPaperAnalysisReport:
    """Analyse a single model epoch without silently mixing control/legacy PnL."""
    excluded = _symbol_set(excluded_symbols)
    allowed_labels = _label_set(allowed_opportunity_labels)
    requested_epoch = str(model_epoch or "").strip()
    if not requested_epoch:
        raise ValueError("model_epoch is required")
    if requested_epoch.startswith("v3_"):
        # v3 is the cost-normalized acceptance cohort, not a general-purpose
        # paper PnL viewer.  Enforce its OOS/official/taker-only contract at
        # the library boundary as well as in the CLI, so an integration cannot
        # recreate an optimistic v3 report by omitting command-line flags.
        if require_out_of_sample is not True:
            raise ValueError("out_of_sample is required for a v3 model epoch")
        if include_nonofficial:
            raise ValueError("official_pnl is required for a v3 model epoch")
        if require_taker_taker is not True:
            raise ValueError("taker_taker execution is required for a v3 model epoch")
        if required_journal_schema_version != CURRENT_PAPER_JOURNAL_SCHEMA_VERSION:
            raise ValueError("journal schema v6 is required for a v3 model epoch")
        if allowed_labels != set(DEFAULT_ALLOWED_OPPORTUNITY_LABELS):
            raise ValueError("spread_reversion label is required for a v3 model epoch")
        if source_evidence_verified is not True:
            raise ValueError("verified source evidence is required for a v3 model epoch")
    report = SpreadPaperAnalysisReport(
        model_epoch=requested_epoch,
        source_evidence_verified=source_evidence_verified is True,
        excluded_symbols=sorted(excluded),
        allowed_opportunity_labels=sorted(allowed_labels),
    )
    episode_ids: set[str] = set()
    manifest_digests: set[str] = set()
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
        if (
            required_journal_schema_version is not None
            and not _journal_schema_matches(payload, required_journal_schema_version)
        ):
            report.journal_schema_mismatch_count += 1
            continue
        if str(payload.get("calculation_version", "") or "") != "spread_paper_v3":
            report.calculation_version_mismatch_count += 1
            continue
        if requested_epoch.startswith("v3_"):
            manifest_digest = str(
                payload.get("research_manifest_digest", "") or ""
            ).lower()
            if not _sha256_hex(manifest_digest):
                report.manifest_digest_missing_count += 1
                continue
            manifest_digests.add(manifest_digest)
            if len(manifest_digests) > 1:
                report.manifest_digest_mismatch_count += 1
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
        if (
            require_out_of_sample
            and str(payload.get("research_sample_split", "") or "")
            != "out_of_sample"
        ):
            report.excluded_in_sample_count += 1
            continue
        if require_taker_taker and not _is_taker_taker_acceptance_payload(payload):
            report.excluded_execution_cohort_count += 1
            continue
        if requested_epoch.startswith("v3_") and not _has_v3_acceptance_evidence(
            payload
        ):
            # `official_pnl` is a statement made by the state machine, not a
            # cryptographic signature over an arbitrary JSONL row.  The v3
            # acceptance reader must therefore recheck the frozen L2, funding
            # and signed account-fee receipts before trusting that statement.
            report.excluded_evidence_count += 1
            continue
        if payload.get("paper_net_quote") is None:
            report.stale_or_unpriced_count += 1
            continue
        if not _paper_economics_are_finite(
            payload, require_complete=requested_epoch.startswith("v3_")
        ) or (
            requested_epoch.startswith("v3_")
            and not _paper_economics_reconciled(payload)
        ):
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
    if len(manifest_digests) == 1:
        report.research_manifest_digest = next(iter(manifest_digests))
    report.finalize_stress()
    return report


def spread_paper_report_dict(report: SpreadPaperAnalysisReport) -> dict:
    """Return a stable JSON-ready report without private sample buffers."""
    return {
        **_group_stats_dict(report),
        "model_epoch": report.model_epoch,
        "source_evidence_verified": report.source_evidence_verified,
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
        "excluded_evidence_count": report.excluded_evidence_count,
        "journal_schema_mismatch_count": report.journal_schema_mismatch_count,
        "calculation_version_mismatch_count": report.calculation_version_mismatch_count,
        "invalid_economics_count": report.invalid_economics_count,
        "duplicate_episode_count": report.duplicate_episode_count,
        "manifest_digest_missing_count": report.manifest_digest_missing_count,
        "manifest_digest_mismatch_count": report.manifest_digest_mismatch_count,
        "research_manifest_digest": report.research_manifest_digest,
        "acceptance_ready": report.acceptance_ready,
        "excluded_in_sample_count": report.excluded_in_sample_count,
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


def _has_v3_acceptance_evidence(payload: dict) -> bool:
    """Verify the frozen evidence a v3 official-paper closure must carry.

    The source journal remains a persistence input, so the analysis boundary
    cannot rely on a literal ``official_pnl`` flag alone.  These checks mirror
    the paper admission and markout contracts without reopening a live fee
    document (which could have changed after the historical entry).
    """
    if (
        payload.get("account_fee_evidence_complete") is not True
        or payload.get("funding_settlement_evidence_complete") is not True
        or payload.get("paper_fill_capacity_source") != "l2_vwap"
        or payload.get("paper_exit_capacity_source") != "l2_vwap"
    ):
        return False
    registered_at_ms = _positive_int(payload.get("registered_at_ms"))
    observed_at_ms = _positive_int(payload.get("account_fee_evidence_observed_at_ms"))
    source = str(payload.get("account_fee_evidence_source") or "").strip()
    fingerprint = str(payload.get("account_fee_evidence_fingerprint") or "").lower()
    provenance = payload.get("account_fee_evidence_provenance")
    if (
        registered_at_ms is None
        or observed_at_ms is None
        or observed_at_ms > registered_at_ms
        or not source
        or not _sha256_hex(fingerprint)
        or not isinstance(provenance, list)
    ):
        return False
    long_venue = str(payload.get("long_venue") or "").strip().lower()
    short_venue = str(payload.get("short_venue") or "").strip().lower()
    if not long_venue or not short_venue or long_venue == short_venue:
        return False
    if not _account_fee_provenance_matches(
        provenance,
        venues={long_venue, short_venue},
        observed_at_ms=observed_at_ms,
        fingerprint=fingerprint,
    ):
        return False
    candidate = payload.get("candidate_snapshot")
    if not isinstance(candidate, dict):
        return False
    if (
        candidate.get("account_fee_evidence_complete") is not True
        or candidate.get("account_fee_evidence_observed_at_ms") != observed_at_ms
        or str(candidate.get("account_fee_evidence_source") or "") != source
        or str(candidate.get("account_fee_evidence_fingerprint") or "").lower()
        != fingerprint
        or candidate.get("account_fee_evidence_provenance") != provenance
    ):
        return False
    return all(
        isinstance(payload.get(leg_name), dict)
        and payload[leg_name].get("entry_execution_source") == "l2_vwap"
        and payload[leg_name].get("exit_execution_source") == "l2_vwap"
        for leg_name in ("long_leg", "short_leg")
    )


def _account_fee_provenance_matches(
    provenance: list[object],
    *,
    venues: set[str],
    observed_at_ms: int,
    fingerprint: str,
) -> bool:
    if len(provenance) != len(venues) or any(
        not isinstance(row, dict) for row in provenance
    ):
        return False
    rows = [dict(row) for row in provenance]
    row_venues = {str(row.get("venue") or "").strip().lower() for row in rows}
    if row_venues != venues:
        return False
    row_observed: list[int] = []
    for row in rows:
        row_observed_at_ms = _positive_int(row.get("observed_at_ms"))
        if (
            row_observed_at_ms is None
            or row.get("integrity_verified") is not True
            or not str(row.get("source") or "").strip()
            or not str(row.get("evidence_ref") or "").strip()
            or not _sha256_hex(str(row.get("account_identity_hash") or "").lower())
            or not _sha256_hex(str(row.get("document_sha256") or "").lower())
            or str(row.get("integrity_key_id") or "").strip()
            != TRUSTED_FEE_EVIDENCE_KEY_ID
            or not _finite_number(row.get("taker_fee_bps"), nonnegative=True)
            or not _finite_number(row.get("maker_fee_bps"), nonnegative=False)
        ):
            return False
        row_observed.append(row_observed_at_ms)
    if min(row_observed, default=0) != observed_at_ms:
        return False
    canonical_rows = sorted(rows, key=lambda row: str(row["venue"]).lower())
    actual_fingerprint = hashlib.sha256(
        json.dumps(
            canonical_rows,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return actual_fingerprint == fingerprint


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer() or parsed <= 0.0:
        return None
    return int(parsed)


def _finite_number(value: object, *, nonnegative: bool) -> bool:
    if isinstance(value, bool):
        return False
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(parsed) and (not nonnegative or parsed >= 0.0)


def _journal_schema_matches(payload: dict, expected: int) -> bool:
    value = payload.get("journal_schema_version")
    if isinstance(value, bool):
        return False
    try:
        return int(value) == int(expected)
    except (TypeError, ValueError, OverflowError):
        return False


def _paper_economics_are_finite(payload: dict, *, require_complete: bool) -> bool:
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
            if require_complete:
                return False
            continue
        if require_complete and not _is_finite_json_number(value):
            return False
        try:
            if not math.isfinite(float(value)):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _is_finite_json_number(value: object) -> bool:
    """Accept only native JSON number values for a strict v3 receipt."""
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _paper_economics_reconciled(payload: dict) -> bool:
    """Require v3 JSON to obey the same PnL identity as the paper engine."""
    try:
        net = float(payload["paper_net_quote"])
        gross = float(payload["paper_gross_quote"])
        fee = float(payload["paper_fee_quote"])
        slippage = float(payload["paper_slippage_quote"])
        funding = float(payload["paper_funding_quote"])
        adverse_selection = float(
            payload["paper_adverse_selection_assumption_quote"]
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    expected = gross + funding - fee - slippage - adverse_selection
    tolerance = max(1e-9, 1e-9 * max(abs(net), abs(expected), 1.0))
    return abs(net - expected) <= tolerance


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
