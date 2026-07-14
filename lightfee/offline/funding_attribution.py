"""Evidence-safe realised-PnL reporting for funding-arbitrage lifecycles.

Terminal close events carry V1 lifecycle funding estimates because a private
statement often arrives later.  This report joins the later
``funding.settlement_reconciled`` fact by position id and deliberately keeps
unreconciled/expired positions out of the official net-PnL total.  It is
therefore safe to use for the forecast-error and realised-cost parts of a
canary review without rewriting the execution-time event history.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping


_TERMINAL_KINDS = frozenset({"exit.closed", "exit.passive_close_resolved"})
_RECONCILED_KIND = "funding.settlement_reconciled"
_EXPIRED_KIND = "funding.settlement_reconciliation_expired"
_TERMINAL_RECEIPT_KIND = "funding.settlement_reconciliations_finalized"


@dataclass
class FundingAttributionGroup:
    position_count: int = 0
    official_position_count: int = 0
    awaiting_statement_count: int = 0
    expired_statement_count: int = 0
    lifecycle_forecast_funding_quote: float = 0.0
    settled_funding_quote: float = 0.0
    funding_forecast_error_quote: float = 0.0
    price_pnl_quote: float = 0.0
    entry_fee_quote: float = 0.0
    exit_fee_quote: float = 0.0
    official_net_quote: float = 0.0

    @property
    def official_coverage_ratio(self) -> float:
        return self.official_position_count / self.position_count if self.position_count else 0.0

    def add(self, row: Mapping[str, object]) -> None:
        self.position_count += 1
        self.lifecycle_forecast_funding_quote += _number(
            row.get("lifecycle_forecast_funding_quote")
        )
        self.price_pnl_quote += _number(row.get("price_pnl_quote"))
        self.entry_fee_quote += _number(row.get("entry_fee_quote"))
        self.exit_fee_quote += _number(row.get("exit_fee_quote"))
        status = str(row.get("statement_status") or "awaiting")
        if status == "official":
            self.official_position_count += 1
            self.settled_funding_quote += _number(row.get("settled_funding_quote"))
            self.funding_forecast_error_quote += _number(
                row.get("funding_forecast_error_quote")
            )
            self.official_net_quote += _number(row.get("official_net_quote"))
        elif status == "expired":
            self.expired_statement_count += 1
        else:
            self.awaiting_statement_count += 1


@dataclass
class FundingAttributionReport(FundingAttributionGroup):
    duplicate_terminal_event_count: int = 0
    orphan_statement_reconciliation_count: int = 0
    calculation_version_mismatch_count: int = 0
    model_epoch_mismatch_count: int = 0
    by_symbol: dict[str, FundingAttributionGroup] = field(default_factory=dict)
    by_venue_pair: dict[str, FundingAttributionGroup] = field(default_factory=dict)
    by_exit_reason: dict[str, FundingAttributionGroup] = field(default_factory=dict)


def analyze_funding_attribution_events(
    records: Iterable[Mapping[str, object]],
) -> FundingAttributionReport:
    """Join terminal lifecycle estimates to later private-statement facts.

    A duplicate terminal event never creates a second trade.  The latest
    terminal payload is retained only to preserve recovery compatibility; the
    duplicate counter makes any such historical journal anomaly observable.
    """
    terminals: dict[str, dict[str, object]] = {}
    reconciled: dict[str, dict[str, object]] = {}
    expired: set[str] = set()
    report = FundingAttributionReport()

    for record in records:
        if not isinstance(record, Mapping):
            continue
        kind = str(record.get("kind") or "")
        payload = record.get("payload")
        if not isinstance(payload, Mapping):
            continue
        if kind == _TERMINAL_RECEIPT_KIND:
            # The critical batch receipt is authoritative when a crash occurs
            # after active-state compaction but before best-effort per-task
            # diagnostic events are appended.
            rows = payload.get("tasks")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                terminal_position_id = str(row.get("position_id") or "")
                if (
                    terminal_position_id
                    and str(row.get("status") or "")
                    == "expired_statement_evidence"
                ):
                    expired.add(terminal_position_id)
            continue
        position_id = str(payload.get("position_id") or "")
        if not position_id:
            continue
        if kind in _TERMINAL_KINDS:
            if position_id in terminals:
                report.duplicate_terminal_event_count += 1
            terminals[position_id] = dict(payload)
        elif kind == _RECONCILED_KIND:
            reconciled[position_id] = dict(payload)
        elif kind == _EXPIRED_KIND:
            expired.add(position_id)

    for position_id, terminal in terminals.items():
        settlement = reconciled.get(position_id)
        row = {
            "lifecycle_forecast_funding_quote": terminal.get(
                "lifecycle_forecast_funding_quote",
                terminal.get("funding_pnl_quote"),
            ),
            "price_pnl_quote": terminal.get("price_pnl_quote", terminal.get("price_pnl")),
            "entry_fee_quote": terminal.get("entry_fee_quote"),
            "exit_fee_quote": terminal.get("exit_fee_quote"),
            "statement_status": "official" if settlement else (
                "expired" if position_id in expired else "awaiting"
            ),
            "settled_funding_quote": (settlement or {}).get("official_funding_quote"),
            "funding_forecast_error_quote": (settlement or {}).get(
                "funding_forecast_error_quote"
            ),
            "official_net_quote": (settlement or {}).get("official_net_quote"),
        }
        _record_group(report, terminal, settlement, row)

    # A reconciliation can survive journal compaction when its original
    # terminal event has been archived.  Retain its official cash-flow fact
    # instead of silently discarding it, but expose the identity mismatch.
    for position_id, settlement in reconciled.items():
        if position_id in terminals:
            continue
        report.orphan_statement_reconciliation_count += 1
        row = {
            "lifecycle_forecast_funding_quote": settlement.get(
                "lifecycle_forecast_funding_quote"
            ),
            "price_pnl_quote": settlement.get("price_pnl_quote"),
            "entry_fee_quote": settlement.get("entry_fee_quote"),
            "exit_fee_quote": settlement.get("exit_fee_quote"),
            "statement_status": "official",
            "settled_funding_quote": settlement.get("official_funding_quote"),
            "funding_forecast_error_quote": settlement.get("funding_forecast_error_quote"),
            "official_net_quote": settlement.get("official_net_quote"),
        }
        _record_group(report, settlement, settlement, row)

    return report


def _record_group(
    report: FundingAttributionReport,
    terminal: Mapping[str, object],
    settlement: Mapping[str, object] | None,
    row: Mapping[str, object],
) -> None:
    report.add(row)
    terminal_calc = str(terminal.get("calculation_version") or "")
    terminal_epoch = str(terminal.get("model_epoch") or "")
    if settlement is not None:
        if terminal_calc and terminal_calc != str(settlement.get("calculation_version") or ""):
            report.calculation_version_mismatch_count += 1
        if terminal_epoch and terminal_epoch != str(settlement.get("model_epoch") or ""):
            report.model_epoch_mismatch_count += 1

    symbol = str(terminal.get("symbol") or "unknown").upper()
    venue_pair = ":".join(
        value for value in (
            str(terminal.get("long_venue") or ""),
            str(terminal.get("short_venue") or ""),
        ) if value
    ) or "unknown"
    reason = str(terminal.get("reason") or "unknown")
    report.by_symbol.setdefault(symbol, FundingAttributionGroup()).add(row)
    report.by_venue_pair.setdefault(venue_pair, FundingAttributionGroup()).add(row)
    report.by_exit_reason.setdefault(reason, FundingAttributionGroup()).add(row)


def _number(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
