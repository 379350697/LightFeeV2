"""Offline spread-paper journal analysis helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


DEFAULT_EXCLUDED_SYMBOLS = ("BBUSDT", "QNTUSDT")
DEFAULT_ALLOWED_OPPORTUNITY_LABELS = ("spread_reversion",)


@dataclass
class SpreadPaperGroupStats:
    closed_count: int = 0
    win_count: int = 0
    net_quote_total: float = 0.0
    gross_quote_total: float = 0.0
    fee_quote_total: float = 0.0
    slippage_quote_total: float = 0.0
    funding_quote_total: float = 0.0

    @property
    def win_rate(self) -> float:
        if self.closed_count <= 0:
            return 0.0
        return self.win_count / self.closed_count

    def add(self, payload: dict) -> None:
        net_quote = _float(payload.get("paper_net_quote"))
        self.closed_count += 1
        self.win_count += 1 if net_quote > 0.0 else 0
        self.net_quote_total += net_quote
        self.gross_quote_total += _float(payload.get("paper_gross_quote"))
        self.fee_quote_total += _float(payload.get("paper_fee_quote"))
        self.slippage_quote_total += _float(payload.get("paper_slippage_quote"))
        self.funding_quote_total += _float(payload.get("paper_funding_quote"))


@dataclass
class SpreadPaperAnalysisReport(SpreadPaperGroupStats):
    excluded_symbols: list[str] = field(default_factory=list)
    allowed_opportunity_labels: list[str] = field(default_factory=list)
    by_symbol: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)
    by_label: dict[str, SpreadPaperGroupStats] = field(default_factory=dict)


def analyze_spread_paper_events(
    records: Iterable[dict],
    *,
    excluded_symbols: Iterable[str] | None = None,
    allowed_opportunity_labels: Iterable[str] | None = DEFAULT_ALLOWED_OPPORTUNITY_LABELS,
) -> SpreadPaperAnalysisReport:
    excluded = _symbol_set(
        DEFAULT_EXCLUDED_SYMBOLS if excluded_symbols is None else excluded_symbols
    )
    allowed_labels = _label_set(allowed_opportunity_labels)
    report = SpreadPaperAnalysisReport(
        excluded_symbols=sorted(excluded),
        allowed_opportunity_labels=sorted(allowed_labels),
    )

    for record in records:
        if str(record.get("kind", "") or "") != "opportunity.paper_closed":
            continue
        payload = record.get("payload", {})
        if not isinstance(payload, dict):
            continue
        symbol = str(payload.get("symbol", "") or "").upper()
        if not symbol or symbol in excluded:
            continue
        label = str(payload.get("candidate_opportunity_label", "") or "spread_reversion")
        if allowed_labels and label not in allowed_labels:
            continue
        report.add(payload)
        report.by_symbol.setdefault(symbol, SpreadPaperGroupStats()).add(payload)
        report.by_label.setdefault(label, SpreadPaperGroupStats()).add(payload)
    return report


def _symbol_set(symbols: Iterable[str]) -> set[str]:
    return {str(symbol).upper() for symbol in symbols if str(symbol).strip()}


def _label_set(labels: Iterable[str] | None) -> set[str]:
    if labels is None:
        return set()
    return {str(label) for label in labels if str(label).strip()}


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
