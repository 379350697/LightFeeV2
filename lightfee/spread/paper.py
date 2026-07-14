"""Conservative, journalled paper execution for signed-basis spread signals.

The paper engine deliberately models an executable two-leg trade, not a mark-to-
market of a theoretical cross-exchange spread.  ``taker/taker`` is the only
official acceptance cohort.  Maker experiments are retained as controls but
remain non-official unless a trade-tape/queue adapter is added.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from math import isfinite

from lightfee.offline.paper_outcome import classify_paper_outcome
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.research_manifest import (
    DEFAULT_SPREAD_RESEARCH_MANIFEST,
    SpreadResearchManifest,
)
from lightfee.strategy.fee_evidence import (
    FEE_EVIDENCE_SCHEMA_VERSION,
    TRUSTED_FEE_EVIDENCE_KEY_ID,
    FeeEvidenceBook,
)


# v6 persists the fee schedule, signed account-evidence provenance, manifest
# digest and latency reserve captured at entry.  Older
# rows remain historical diagnostics but cannot be resumed as official paper
# positions because re-pricing their exit fee under today's schedule would
# rewrite their economics.
SPREAD_PAPER_JOURNAL_SCHEMA_VERSION = 6


class PaperOrderState(StrEnum):
    NEW = "NEW"
    WORKING = "WORKING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class SpreadPaperBotSpec:
    bot_id: str
    cohort: str
    hypothesis: str = ""
    manifest_version: str = "spread_research_manifest_v2"
    manifest_digest: str = ""
    acceptance_eligible: bool = False
    entry_long_role: str = "taker"
    entry_short_role: str = "taker"
    exit_long_role: str = "taker"
    exit_short_role: str = "taker"
    maker_leg: str = ""
    hedge_delay_ms: int = 0
    control_group: bool = False


@dataclass(frozen=True)
class SpreadPaperConfig:
    enabled: bool = False
    finalist_limit: int = 0
    # A paper signal cannot fill itself.  Only a later coherent public quote
    # observed after this decision latency may create the simulated pair.
    min_decision_latency_ms: int = 250
    markout_secs: list[int] = field(default_factory=lambda: [60, 300, 900, 1800])
    terminal_secs: int = 1800
    active_exit_enabled: bool = False
    exit_z: float = 0.5
    stop_z: float = 3.5
    max_hold_ms: int = 0
    taker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    maker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    slippage_buffer_bps: float = 0.0
    # This is an additional, explicit adverse execution allowance.  It is
    # applied on every entry and exit in addition to the book/VWAP price.
    latency_buffer_bps: float = 0.0
    # Library callers can retain diagnostic BBO paper by default.  The live
    # service sets both gates to True from StrategyConfig for official cohorts.
    require_l2_vwap: bool = False
    require_account_fee_evidence: bool = False
    account_fee_evidence: FeeEvidenceBook | None = None
    # Schema-v3 paper records bind the fee schedule to the configured trading
    # account for each venue.  Keeping the expected hashes in config makes a
    # copied valid document from a different sub-account fail closed.
    fee_evidence_account_identity_hashes: dict[str, str] = field(
        default_factory=dict
    )
    # A caller-provided negative maker map is not a rebate unless it exactly
    # matches a signed account-fee schedule.
    allow_verified_maker_rebates: bool = False
    # A non-zero cutoff labels registrations after it as out-of-sample.  When
    # strict, pre-cutoff observations are not registered at all.
    oos_start_ms: int = 0
    require_out_of_sample: bool = False
    excluded_symbols: list[str] = field(default_factory=list)
    allowed_opportunity_labels: list[str] = field(
        default_factory=lambda: ["spread_reversion"]
    )
    episode_cooldown_ms: int = 1_800_000
    paper_bot_ids: list[str] = field(default_factory=lambda: ["tt_conservative"])
    model_epoch: str = "v2_signed_reversion"
    primary_fill_model: str = "taker_taker"
    require_taker_taker: bool = True
    quote_ttl_ms: int = 1_000
    # Paper PnL must use a contemporaneous two-venue observation.  Reuse the
    # signal skew contract rather than treating two individually fresh quotes
    # as an executable exit.
    quote_skew_ms: int = 250
    research_manifest: SpreadResearchManifest = field(
        default_factory=lambda: DEFAULT_SPREAD_RESEARCH_MANIFEST
    )


def _is_strict_positive_int(value: object) -> bool:
    """Accept scheduling counts only when they are literal positive integers."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _has_strict_positive_horizons(value: object) -> bool:
    return isinstance(value, list) and bool(value) and all(
        _is_strict_positive_int(item) for item in value
    )


@dataclass(frozen=True)
class FundingSettlement:
    """An exact funding credit/debit allocated to one paper position leg.

    Funding records supplied by an exchange are account-level facts. They may
    only enter an individual paper position after the caller has supplied the
    position and leg allocation key, rather than being inferred from the entry
    quote. ``amount_quote`` uses account PnL sign convention: positive is a
    credit and negative is a debit.
    """

    paper_id: str
    leg_side: str
    settlement_timestamp_ms: int
    amount_quote: float
    observed_at_ms: int
    source: str


@dataclass(frozen=True)
class SpreadPaperLeg:
    venue: str
    side: str
    entry_liquidity_role: str
    exit_liquidity_role: str
    entry_pending: bool
    entry_bid: float
    entry_ask: float
    entry_bid_size: float
    entry_ask_size: float
    entry_observed_at_ms: int
    mark_price: float
    index_price: float
    volume_24h_quote: float
    open_interest: float
    entry_raw_price: float | None
    entry_price: float | None
    qty: float
    entry_notional_quote: float
    entry_fee_bps: float
    entry_fee_quote: float
    entry_slippage_quote: float
    funding_rate_bps: float
    funding_timestamp_ms: int
    # Fees and latency are immutable entry-time assumptions.  They must not
    # silently change if a service refreshes its account-fee evidence later.
    exit_fee_bps: float = 0.0
    entry_latency_buffer_quote: float = 0.0
    funding_interval_ms: int = 0
    entry_filled_at_ms: int = 0
    order_state: str = PaperOrderState.NEW.value
    requested_qty: float = 0.0
    residual_qty: float = 0.0
    funding_settlements: tuple[FundingSettlement, ...] = ()
    funding_settlement_conflict: bool = False
    # ``l2_vwap`` is only emitted when the published snapshot supplied a
    # coherent depth ladder.  All legacy and BBO-only positions keep the
    # explicit top-book label, rather than pretending to have VWAP evidence.
    entry_execution_source: str = "top_book_only"


@dataclass(frozen=True)
class SpreadPaperPosition:
    paper_id: str
    candidate_id: str
    symbol: str
    long_venue: str
    short_venue: str
    candidate_opportunity_label: str
    paper_bot_id: str
    paper_cohort: str
    paper_entry_mode: str
    paper_exit_mode: str
    paper_maker_leg: str
    paper_hedge_delay_ms: int
    paper_control_group: bool
    paper_fill_assumption: str
    finalist_rank: int
    registered_at_ms: int
    entry_eligible_at_ms: int
    entry_notional_quote: float
    long_leg: SpreadPaperLeg
    short_leg: SpreadPaperLeg
    candidate_snapshot: dict
    entry_market_snapshot: dict
    due_horizons: list[dict]
    requested_base_qty: float = 0.0
    filled_base_qty: float = 0.0
    residual_base_qty: float = 0.0
    # Matched quantity is distinct from temporary naked maker exposure.
    delta_exposure_base_qty: float = 0.0
    maker_fill_observed_at_ms: int = 0
    model_epoch: str = "v2_signed_reversion"
    # Official PnL is permissioned evidence, never a dataclass convenience
    # default.  The paper state machine grants it only to a complete baseline
    # taker/taker fill, and markout additionally requires settlement proof.
    official_pnl: bool = False
    research_manifest_version: str = "spread_research_manifest_v2"
    research_manifest_digest: str = ""
    research_hypothesis: str = ""
    acceptance_eligible: bool = False
    account_fee_evidence_complete: bool = False
    account_fee_evidence_observed_at_ms: int = 0
    account_fee_evidence_source: str = ""
    account_fee_evidence_fingerprint: str = ""
    account_fee_evidence_provenance: list[dict[str, object]] = field(
        default_factory=list
    )
    research_sample_split: str = "in_sample"
    volatility_regime: str = "unknown"


@dataclass(frozen=True)
class _ExecutionEstimate:
    """A price/capacity observation for one paper leg.

    ``capacity`` is what the actually observed book can absorb.  It is not a
    liquidity forecast, so callers must use a common quantity across both
    legs and re-evaluate the VWAP at that matched quantity.
    """

    price: float
    capacity: float
    source: str


class SpreadPaperTracker:
    """State machine for paper-only execution and PnL attribution."""

    def __init__(self, config: SpreadPaperConfig) -> None:
        self.config = config
        self._positions: dict[str, SpreadPaperPosition] = {}
        self._emitted_horizons: dict[str, set[str]] = {}
        self._known_paper_ids: set[str] = set()
        self._episode_started_at_ms: dict[tuple[str, str, str, str], int] = {}

    @property
    def enabled(self) -> bool:
        # The primary acceptance cohort is deliberately fixed at taker/taker.
        # A config typo must disable paper admission rather than silently turn
        # a maker control into the official result stream.
        return (
            self.config.enabled is True
            and _is_strict_positive_int(self.config.finalist_limit)
            and _is_strict_positive_int(self.config.min_decision_latency_ms)
            and _is_strict_positive_int(self.config.terminal_secs)
            and _has_strict_positive_horizons(self.config.markout_secs)
            and str(self.config.primary_fill_model or "").lower() == "taker_taker"
            and self.config.require_taker_taker is True
            and self.config.research_manifest.model_epoch == self.config.model_epoch
        )

    @property
    def tracked_count(self) -> int:
        return len(self._positions)

    def missing_due_quote_keys(
        self, now_ms: int, quotes: dict[str, QuoteSnapshot]
    ) -> set[tuple[str, str]]:
        return self.missing_evaluation_quote_keys(now_ms, quotes)

    def missing_evaluation_quote_keys(
        self, now_ms: int, quotes: dict[str, QuoteSnapshot]
    ) -> set[tuple[str, str]]:
        if not self.enabled:
            return set()
        missing: set[tuple[str, str]] = set()
        for position in self._positions.values():
            if _position_has_pending_entry(position):
                missing.update(_missing_quote_keys(position, quotes))
                continue
            if self._has_due_evaluation(position, now_ms):
                missing.update(_missing_quote_keys(position, quotes))
        return missing

    def register(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
        decision_at_ms: int | None = None,
    ) -> dict | None:
        events = self.register_many(
            candidate,
            quotes,
            finalist_rank=finalist_rank,
            decision_at_ms=decision_at_ms,
        )
        return events[0] if events else None

    def register_many(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
        decision_at_ms: int | None = None,
    ) -> list[dict]:
        if not self.enabled or finalist_rank >= self.config.finalist_limit:
            return []
        if (
            candidate.signal_status != "entry_ready"
            or candidate.economics_complete is not True
            or candidate.fee_evidence_complete is not True
        ):
            return []
        # Do not let a manually constructed candidate turn an unverified
        # contract into an official paper observation.  Snapshot v1/v2
        # compatibility defaults to ``unknown`` and must remain diagnostic
        # only until the signed-basis builder proves the two legs compatible.
        if str(candidate.contract_normalization_status or "").lower() != "complete":
            return []
        if candidate.calculation_version != _candidate_calculation_version(self.config):
            return []
        if str(candidate.model_epoch or "") != self.config.model_epoch:
            return []
        if not _candidate_account_fee_evidence_complete(self.config, candidate):
            return []
        if candidate.symbol.upper() in _excluded_symbols(self.config):
            return []
        if _allowed_labels(self.config) and candidate.opportunity_label not in _allowed_labels(self.config):
            return []
        try:
            decision_ms = int(
                candidate.signal_ts_ms if decision_at_ms is None else decision_at_ms
            )
        except (TypeError, ValueError, OverflowError):
            return []
        if decision_ms <= 0 or int(candidate.signal_ts_ms or 0) > decision_ms:
            return []
        long_quote = _quote_for(quotes, candidate.long_venue, candidate.symbol)
        short_quote = _quote_for(quotes, candidate.short_venue, candidate.symbol)
        if long_quote is None or short_quote is None:
            return []
        if not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=decision_ms,
            ttl_ms=self.config.quote_ttl_ms,
            quote_skew_ms=self.config.quote_skew_ms,
        ):
            return []

        events: list[dict] = []
        for bot in _paper_bot_specs(self.config):
            if not _paper_fee_evidence_complete(
                self.config,
                candidate,
                bot,
            ):
                continue
            paper_id = _paper_id(candidate, bot.bot_id)
            if paper_id in self._known_paper_ids or self._episode_in_cooldown(candidate, bot.bot_id):
                continue
            position = self._build_position(
                paper_id=paper_id,
                candidate=candidate,
                long_quote=long_quote,
                short_quote=short_quote,
                finalist_rank=finalist_rank,
                bot=bot,
                decision_at_ms=decision_ms,
            )
            if position is None:
                continue
            self._positions[paper_id] = position
            self._emitted_horizons[paper_id] = set()
            self._known_paper_ids.add(paper_id)
            self._record_episode(position)
            events.append(_registration_event(position))
        return events

    def restore_from_records(self, records: list[dict]) -> None:
        """Restore v5 open positions; prior journal schemas stay diagnostic-only."""
        self._positions.clear()
        self._emitted_horizons.clear()
        self._known_paper_ids.clear()
        self._episode_started_at_ms.clear()
        for record in records:
            kind = str(record.get("kind", "") or "")
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            paper_id = str(payload.get("paper_id", "") or "")
            if not paper_id:
                continue
            if kind in {
                "opportunity.paper_registered",
                "opportunity.paper_maker_fill_observed",
                "opportunity.paper_hedge_filled",
                "opportunity.paper_taker_pair_filled",
                "opportunity.paper_funding_settlement_observed",
            }:
                # A journal is an external restart boundary.  Older records
                # remain readable on disk, but may not acquire current-model
                # execution authority or official-PnL status by being loaded
                # into a newer process.
                if not _restorable_current_v3_record(payload, self.config):
                    self._known_paper_ids.add(paper_id)
                    continue
                position = _position_from_payload(payload)
                if position is None:
                    continue
                if not _restored_position_has_fee_evidence(position, self.config):
                    self._known_paper_ids.add(paper_id)
                    continue
                if not _restored_position_is_official_eligible(position, self.config):
                    position = replace(position, official_pnl=False)
                self._known_paper_ids.add(paper_id)
                self._positions[paper_id] = position
                self._emitted_horizons.setdefault(paper_id, set())
                self._record_episode(position)
            elif kind in {"opportunity.paper_markout", "opportunity.paper_delta_markout", "opportunity.paper_closed", "opportunity.paper_expired"}:
                self._known_paper_ids.add(paper_id)
                horizon = str(payload.get("horizon_kind", "") or "")
                if horizon:
                    self._emitted_horizons.setdefault(paper_id, set()).add(horizon)
                if kind in {"opportunity.paper_closed", "opportunity.paper_expired"}:
                    self._positions.pop(paper_id, None)
                    self._emitted_horizons.pop(paper_id, None)

    def record_funding_settlements(
        self,
        settlements: list[FundingSettlement],
    ) -> list[dict]:
        """Attach actual, pre-allocated funding facts to live paper positions.

        The paper tracker deliberately refuses account-level or forecast rates:
        callers must provide the target ``paper_id`` and ``leg_side``. This
        keeps overlapping positions from silently sharing a settlement and
        makes official PnL fail closed until a real allocation is present.
        """
        events: list[dict] = []
        for settlement in settlements:
            position = self._positions.get(str(settlement.paper_id or ""))
            if position is None or not _valid_funding_settlement(position, settlement):
                continue
            current_leg = (
                position.long_leg
                if settlement.leg_side == "long"
                else position.short_leg
            )
            existing = next(
                (
                    item
                    for item in current_leg.funding_settlements
                    if int(item.settlement_timestamp_ms)
                    == int(settlement.settlement_timestamp_ms)
                ),
                None,
            )
            if current_leg.funding_settlement_conflict:
                continue
            if existing is not None and float(existing.amount_quote) == float(
                settlement.amount_quote
            ):
                # A sidecar can refresh several times inside the post-settlement
                # proof window.  One immutable cash fact gets one journal
                # event; replay is still defensively idempotent for crashes.
                continue
            next_leg = _with_funding_settlement(current_leg, settlement)
            next_position = (
                replace(position, long_leg=next_leg)
                if settlement.leg_side == "long"
                else replace(position, short_leg=next_leg)
            )
            self._positions[position.paper_id] = next_position
            events.append(_funding_settlement_observed_event(next_position, settlement))
        return events

    def record_observed_public_funding_settlements(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict]:
        """Allocate a just-observed public settled rate to paper legs.

        A paper position has no exchange account statement, so account-level
        funding ledgers cannot identify it.  The public sidecar instead
        supplies an explicitly labelled *settled* rate and a contemporaneous
        mark.  We use that fact only when it was observed within the normal
        quote TTL immediately after the known settlement timestamp.  A late,
        absent, malformed, or schedule-inconsistent observation deliberately
        produces no ledger row; official PnL then remains fail-closed.
        """
        settlements: list[FundingSettlement] = []
        for position in self._positions.values():
            settlements.extend(
                _public_settled_funding_for_position(
                    position,
                    now_ms=int(now_ms),
                    quotes=quotes,
                    config=self.config,
                )
            )
        return self.record_funding_settlements(settlements)

    def evaluate_due(self, now_ms: int, quotes: dict[str, QuoteSnapshot]) -> list[dict]:
        if not self.enabled:
            return []
        events: list[dict] = []
        closed: set[str] = set()
        for position in list(self._positions.values()):
            if _position_has_pending_entry(position):
                # The pending-entry terminal is a strict admission boundary.
                # Do this before inspecting executable quotes so a delayed
                # evaluator cannot backdate an entry with a quote that first
                # arrived after the paper order had already expired.
                if self._terminal_due(position, now_ms):
                    events.append(
                        _expired_event(
                            position,
                            now_ms,
                            quotes,
                            "entry_not_filled",
                            self.config,
                        )
                    )
                    closed.add(position.paper_id)
                    continue

                transition = self._advance_pending_entry(position, now_ms, quotes)
                if transition is not None:
                    position, transition_kind = transition
                    self._positions[position.paper_id] = position
                    # A maker-only delta observation belongs to the incomplete
                    # exposure phase.  Once a later transition rebases the
                    # lifecycle horizons (especially the delayed hedge fill),
                    # it must not suppress the same named markout of the
                    # fully matched pair.
                    self._emitted_horizons[position.paper_id] = set()
                    if transition_kind == "maker_fill_observed":
                        events.append(_maker_fill_observed_event(position))
                    elif transition_kind == "taker_pair_filled":
                        events.append(_taker_pair_filled_event(position))
                    else:
                        events.append(_hedge_filled_event(position))
                elif position.maker_fill_observed_at_ms > 0:
                    # A maker cross leaves a directional exposure until the
                    # delayed hedge is eligible.  Persist every scheduled
                    # markout instead of hiding adverse movement until expiry.
                    horizon = self._next_horizon(position, now_ms)
                    if horizon is not None:
                        events.append(
                            _delta_markout_event(
                                position,
                                horizon,
                                now_ms,
                                quotes,
                                self.config,
                            )
                        )
                        self._emitted_horizons.setdefault(position.paper_id, set()).add(
                            str(horizon["kind"])
                        )
                continue

            active = self._active_exit_horizon(position, now_ms, quotes)
            # A delayed evaluator must emit every outstanding observation in
            # chronological order.  Emitting only the first due markout would
            # leave the terminal close open until another refresh, which is a
            # false lifecycle state for a paper position.
            horizons = [active] if active is not None else [
                horizon
                for horizon in position.due_horizons
                if int(horizon["due_at_ms"]) <= now_ms
            ]
            if not horizons:
                continue
            emitted = self._emitted_horizons.setdefault(position.paper_id, set())
            for horizon in horizons:
                horizon_kind = str(horizon["kind"])
                if horizon_kind in emitted:
                    continue
                if not self._exit_quotes_fresh(position, now_ms, quotes):
                    event = _unpriced_event(
                        position, horizon, now_ms, quotes, self.config
                    )
                else:
                    event = self._build_due_event(position, horizon, now_ms, quotes)
                events.append(event)
                emitted.add(horizon_kind)
                if bool(horizon.get("terminal")):
                    closed.add(position.paper_id)
                    break
        for paper_id in closed:
            self._positions.pop(paper_id, None)
            self._emitted_horizons.pop(paper_id, None)
        return events

    def _build_position(
        self,
        *,
        paper_id: str,
        candidate: SpreadReversionCandidate,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
        finalist_rank: int,
        bot: SpreadPaperBotSpec,
        decision_at_ms: int,
    ) -> SpreadPaperPosition | None:
        # A signal observation is only a decision.  Register a pending entry
        # now, then require a strictly newer, coherent quote after the
        # configured latency before using any executable price or opening the
        # markout/funding lifecycle.
        registered_at_ms = int(decision_at_ms or 0)
        if registered_at_ms <= 0:
            return None
        sample_split = _research_sample_split(candidate, self.config, registered_at_ms)
        if self.config.require_out_of_sample and sample_split != "out_of_sample":
            return None
        entry_eligible_at_ms = registered_at_ms + self.config.min_decision_latency_ms
        target_notional = float(candidate.entry_notional_quote or 0.0)
        long_raw = _entry_raw_price(long_quote, "long", bot.entry_long_role)
        short_raw = _entry_raw_price(short_quote, "short", bot.entry_short_role)
        if target_notional <= 0.0 or long_raw <= 0.0 or short_raw <= 0.0:
            return None
        requested_qty = target_notional / max(long_raw, short_raw)
        if requested_qty <= 0.0:
            return None
        if not _paper_bot_execution_supported(bot):
            return None
        long_leg = _pending_leg(
            long_quote, candidate.long_venue, "long", bot.entry_long_role, long_raw,
            requested_qty, self.config,
        )
        short_leg = _pending_leg(
            short_quote, candidate.short_venue, "short", bot.entry_short_role, short_raw,
            requested_qty, self.config,
        )
        filled_qty = 0.0
        residual_qty = requested_qty
        entry_mode = f"long_{bot.entry_long_role}:short_{bot.entry_short_role}"
        exit_mode = f"long_{bot.exit_long_role}:short_{bot.exit_short_role}"
        acceptance_eligible = bot.acceptance_eligible is True
        control_group = bot.control_group is True
        return SpreadPaperPosition(
            paper_id=paper_id,
            candidate_id=candidate.candidate_id,
            symbol=candidate.symbol,
            long_venue=candidate.long_venue,
            short_venue=candidate.short_venue,
            candidate_opportunity_label=candidate.opportunity_label,
            paper_bot_id=bot.bot_id,
            paper_cohort=bot.cohort,
            paper_entry_mode=entry_mode,
            paper_exit_mode=exit_mode,
            paper_maker_leg=bot.maker_leg,
            paper_hedge_delay_ms=bot.hedge_delay_ms,
            paper_control_group=control_group,
            paper_fill_assumption=_fill_assumption(bot),
            finalist_rank=finalist_rank,
            registered_at_ms=registered_at_ms,
            entry_eligible_at_ms=entry_eligible_at_ms,
            entry_notional_quote=target_notional,
            long_leg=_with_exit_terms(long_leg, bot.exit_long_role, self.config),
            short_leg=_with_exit_terms(short_leg, bot.exit_short_role, self.config),
            candidate_snapshot=_candidate_snapshot(candidate),
            entry_market_snapshot=_market_snapshot_payload(long_quote, short_quote),
            # Pending entries can expire after the decision window; once
            # filled, horizons are rebuilt from the actual fill timestamp.
            due_horizons=_due_horizons(entry_eligible_at_ms, self.config),
            requested_base_qty=requested_qty,
            filled_base_qty=filled_qty,
            residual_base_qty=residual_qty,
            delta_exposure_base_qty=0.0,
            model_epoch=self.config.model_epoch,
            official_pnl=False,
            research_manifest_version=bot.manifest_version,
            research_manifest_digest=bot.manifest_digest,
            research_hypothesis=bot.hypothesis,
            acceptance_eligible=acceptance_eligible,
            account_fee_evidence_complete=(
                candidate.account_fee_evidence_complete is True
            ),
            account_fee_evidence_observed_at_ms=int(
                candidate.account_fee_evidence_observed_at_ms or 0
            ),
            account_fee_evidence_source=str(candidate.account_fee_evidence_source or ""),
            account_fee_evidence_fingerprint=str(
                candidate.account_fee_evidence_fingerprint or ""
            ),
            account_fee_evidence_provenance=list(
                candidate.account_fee_evidence_provenance or []
            ),
            research_sample_split=sample_split,
            volatility_regime=str(candidate.volatility_regime or "unknown"),
        )

    def _advance_pending_entry(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> tuple[SpreadPaperPosition, str] | None:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or short_quote is None:
            return None
        if not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=self.config.quote_ttl_ms,
            quote_skew_ms=self.config.quote_skew_ms,
        ):
            return None
        # A refresh may carry the identical quote object/timestamp long after
        # registration.  Time passing alone is not new execution evidence.
        # Both legs must be observed no earlier than the modelled decision
        # latency; otherwise a snapshot captured before the permitted order
        # instant could be retroactively priced as a fill.
        try:
            quote_observed_at_ms = min(
                int(long_quote.observed_at_ms or 0),
                int(short_quote.observed_at_ms or 0),
            )
        except (TypeError, ValueError, OverflowError):
            return None
        if (
            now_ms < position.entry_eligible_at_ms
            or quote_observed_at_ms < position.entry_eligible_at_ms
        ):
            return None
        maker_leg = position.paper_maker_leg
        if not maker_leg:
            return self._fill_pending_taker_pair(
                position,
                max(int(long_quote.observed_at_ms), int(short_quote.observed_at_ms)),
                long_quote,
                short_quote,
            )
        if position.maker_fill_observed_at_ms <= 0:
            maker_position_leg = position.long_leg if maker_leg == "long" else position.short_leg
            maker_quote = long_quote if maker_leg == "long" else short_quote
            if not _maker_crossed(maker_position_leg, maker_quote):
                return None
            maker_qty = min(
                position.requested_base_qty,
                _entry_capacity(maker_quote, maker_leg),
            )
            if maker_qty <= 0.0 or maker_position_leg.entry_raw_price is None:
                return None
            filled_maker = _filled_leg(
                maker_quote,
                maker_position_leg.venue,
                maker_leg,
                maker_position_leg.entry_liquidity_role,
                maker_position_leg.entry_raw_price,
                maker_qty,
                position.requested_base_qty,
                self.config,
                PaperOrderState.UNKNOWN,
                filled_at_ms=int(maker_quote.observed_at_ms),
                execution_source="maker_bbo_unknown",
            )
            if maker_leg == "long":
                next_position = replace(
                    position,
                    long_leg=_with_exit_terms(
                        filled_maker,
                        position.long_leg.exit_liquidity_role,
                        self.config,
                    ),
                    maker_fill_observed_at_ms=int(maker_quote.observed_at_ms),
                    delta_exposure_base_qty=maker_qty,
                    # Delay attribution begins at the maker fill, not at the
                    # earlier signal/decision snapshot.  Preserve the
                    # contemporaneous hedge-side BBO as the causal baseline.
                    entry_market_snapshot=_market_snapshot_payload(
                        long_quote,
                        short_quote,
                    ),
                    due_horizons=_due_horizons(
                        int(maker_quote.observed_at_ms),
                        self.config,
                    ),
                )
            else:
                next_position = replace(
                    position,
                    short_leg=_with_exit_terms(
                        filled_maker,
                        position.short_leg.exit_liquidity_role,
                        self.config,
                    ),
                    maker_fill_observed_at_ms=int(maker_quote.observed_at_ms),
                    delta_exposure_base_qty=-maker_qty,
                    entry_market_snapshot=_market_snapshot_payload(
                        long_quote,
                        short_quote,
                    ),
                    due_horizons=_due_horizons(
                        int(maker_quote.observed_at_ms),
                        self.config,
                    ),
                )
            return next_position, "maker_fill_observed"

        hedge_eligible_at_ms = position.maker_fill_observed_at_ms + max(
            position.paper_hedge_delay_ms,
            0,
        )
        if now_ms < hedge_eligible_at_ms:
            return None
        maker_position_leg = position.long_leg if maker_leg == "long" else position.short_leg
        hedge_position_leg = position.short_leg if maker_leg == "long" else position.long_leg
        hedge_quote = short_quote if maker_leg == "long" else long_quote
        hedge_side = "short" if maker_leg == "long" else "long"
        # The hedge delay is a market-data causality boundary, not merely a
        # scheduler boundary.  A quote observed before that boundary cannot
        # be reused after the clock reaches it: doing so would fabricate an
        # executable hedge price during the unobserved delay interval.
        try:
            hedge_observed_at_ms = int(hedge_quote.observed_at_ms or 0)
        except (TypeError, ValueError, OverflowError):
            return None
        if hedge_observed_at_ms < hedge_eligible_at_ms:
            return None
        hedge_execution = _entry_execution(
            hedge_quote,
            hedge_side,
            hedge_position_leg.entry_liquidity_role,
            maker_position_leg.qty,
        )
        if hedge_execution is None:
            return None
        qty = min(maker_position_leg.qty, hedge_execution.capacity)
        qty = min(
            qty,
            _entry_quantity_under_notional_cap(
                hedge_quote,
                hedge_side,
                hedge_position_leg.entry_liquidity_role,
                qty,
                position.entry_notional_quote,
            ),
        )
        if qty <= 0.0:
            return None
        residual = max(position.requested_base_qty - qty, 0.0)
        hedge_execution = _entry_execution(
            hedge_quote,
            hedge_side,
            hedge_position_leg.entry_liquidity_role,
            qty,
        )
        if hedge_execution is None:
            return None
        filled_hedge = _filled_leg(
            hedge_quote,
            hedge_position_leg.venue,
            hedge_side,
            hedge_position_leg.entry_liquidity_role,
            hedge_execution.price,
            qty,
            position.requested_base_qty,
            self.config,
            PaperOrderState.UNKNOWN,
            filled_at_ms=int(hedge_quote.observed_at_ms),
            execution_source=hedge_execution.source,
        )
        if maker_leg == "long":
            next_position = replace(
                position,
                short_leg=_with_exit_terms(
                    filled_hedge,
                    position.short_leg.exit_liquidity_role,
                    self.config,
                ),
                filled_base_qty=qty,
                residual_base_qty=residual,
                delta_exposure_base_qty=position.long_leg.qty - qty,
                due_horizons=_due_horizons(
                    max(
                        int(position.long_leg.entry_filled_at_ms),
                        int(filled_hedge.entry_filled_at_ms),
                    ),
                    self.config,
                ),
            )
        else:
            next_position = replace(
                position,
                long_leg=_with_exit_terms(
                    filled_hedge,
                    position.long_leg.exit_liquidity_role,
                    self.config,
                ),
                filled_base_qty=qty,
                residual_base_qty=residual,
                delta_exposure_base_qty=qty - position.short_leg.qty,
                due_horizons=_due_horizons(
                    max(
                        int(position.short_leg.entry_filled_at_ms),
                        int(filled_hedge.entry_filled_at_ms),
                    ),
                    self.config,
                ),
            )
        return next_position, "hedge_filled"

    def _fill_pending_taker_pair(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
    ) -> tuple[SpreadPaperPosition, str] | None:
        """Fill a taker/taker paper pair only from later executable quotes."""
        requested_qty = position.requested_base_qty
        if requested_qty <= 0.0:
            return None
        long_execution = _entry_execution(
            long_quote, "long", position.long_leg.entry_liquidity_role, requested_qty
        )
        short_execution = _entry_execution(
            short_quote, "short", position.short_leg.entry_liquidity_role, requested_qty
        )
        if long_execution is None or short_execution is None:
            return None
        if self.config.require_l2_vwap and not _executions_have_l2(
            long_execution,
            short_execution,
        ):
            # Do not turn a BBO fallback into a claimed executable paper fill.
            # Keeping it pending makes the terminal journal state explicit.
            return None
        quantity = min(requested_qty, long_execution.capacity, short_execution.capacity)
        quantity = min(
            quantity,
            _entry_quantity_under_notional_cap(
                long_quote,
                "long",
                position.long_leg.entry_liquidity_role,
                quantity,
                position.entry_notional_quote,
            ),
            _entry_quantity_under_notional_cap(
                short_quote,
                "short",
                position.short_leg.entry_liquidity_role,
                quantity,
                position.entry_notional_quote,
            ),
        )
        if quantity <= 0.0:
            return None
        residual = max(requested_qty - quantity, 0.0)
        state = (
            PaperOrderState.FILLED
            if residual <= 1e-12
            else PaperOrderState.PARTIAL
        )
        long_execution = _entry_execution(
            long_quote, "long", position.long_leg.entry_liquidity_role, quantity
        )
        short_execution = _entry_execution(
            short_quote, "short", position.short_leg.entry_liquidity_role, quantity
        )
        if long_execution is None or short_execution is None:
            return None
        long_leg = _filled_leg(
            long_quote, position.long_venue, "long",
            position.long_leg.entry_liquidity_role, long_execution.price, quantity,
            requested_qty, self.config, state,
            filled_at_ms=int(long_quote.observed_at_ms),
            execution_source=long_execution.source,
        )
        short_leg = _filled_leg(
            short_quote, position.short_venue, "short",
            position.short_leg.entry_liquidity_role, short_execution.price, quantity,
            requested_qty, self.config, state,
            filled_at_ms=int(short_quote.observed_at_ms),
            execution_source=short_execution.source,
        )
        official_pnl = bool(
            residual <= 1e-12
            and position.acceptance_eligible is True
            and position.paper_control_group is False
            and not position.paper_maker_leg
            # Disabling either strict gate retains diagnostic paper lifecycle
            # events, but must never relabel a BBO/unauthenticated simulation
            # as an official acceptance observation.
            and self.config.require_l2_vwap is True
            and self.config.require_account_fee_evidence is True
            and _position_account_fee_evidence_complete(position, self.config)
            and _executions_have_l2(long_execution, short_execution)
        )
        return (
            replace(
                position,
                long_leg=_with_exit_terms(
                    long_leg,
                    position.long_leg.exit_liquidity_role,
                    self.config,
                ),
                short_leg=_with_exit_terms(
                    short_leg,
                    position.short_leg.exit_liquidity_role,
                    self.config,
                ),
                entry_market_snapshot=_market_snapshot_payload(long_quote, short_quote),
                due_horizons=_due_horizons(now_ms, self.config),
                filled_base_qty=quantity,
                residual_base_qty=residual,
                delta_exposure_base_qty=0.0,
                official_pnl=official_pnl,
            ),
            "taker_pair_filled",
        )

    def _has_due_evaluation(self, position: SpreadPaperPosition, now_ms: int) -> bool:
        return self._next_horizon(position, now_ms) is not None or (
            self.config.active_exit_enabled
            and now_ms > _position_entry_completed_at_ms(position)
        )

    def _next_horizon(self, position: SpreadPaperPosition, now_ms: int) -> dict | None:
        emitted = self._emitted_horizons.setdefault(position.paper_id, set())
        for horizon in position.due_horizons:
            if int(horizon["due_at_ms"]) <= now_ms and str(horizon["kind"]) not in emitted:
                return horizon
        return None

    def _terminal_due(self, position: SpreadPaperPosition, now_ms: int) -> bool:
        return any(bool(item["terminal"]) and int(item["due_at_ms"]) <= now_ms for item in position.due_horizons)

    def _exit_quotes_fresh(self, position: SpreadPaperPosition, now_ms: int, quotes: dict[str, QuoteSnapshot]) -> bool:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or short_quote is None:
            return False
        if not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=self.config.quote_ttl_ms,
            quote_skew_ms=self.config.quote_skew_ms,
        ):
            return False
        long_execution = _exit_execution(
            long_quote,
            "long",
            position.long_leg.exit_liquidity_role,
            position.long_leg.qty,
        )
        short_execution = _exit_execution(
            short_quote,
            "short",
            position.short_leg.exit_liquidity_role,
            position.short_leg.qty,
        )
        return bool(
            long_execution is not None
            and short_execution is not None
            and (
                not self.config.require_l2_vwap
                or _executions_have_l2(long_execution, short_execution)
            )
            and long_execution.capacity + 1e-12 >= position.long_leg.qty
            and short_execution.capacity + 1e-12 >= position.short_leg.qty
        )

    def _active_exit_horizon(self, position: SpreadPaperPosition, now_ms: int, quotes: dict[str, QuoteSnapshot]) -> dict | None:
        if not self.config.active_exit_enabled:
            return None
        if (
            self.config.max_hold_ms > 0
            and now_ms - _position_entry_completed_at_ms(position)
            >= self.config.max_hold_ms
        ):
            return {"kind": "active_exit:max_hold", "due_at_ms": now_ms, "terminal": True, "close_reason": "spread_max_hold_elapsed"}
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        spread = _position_signed_spread_bps(position, long_quote, short_quote)
        z = _position_exit_z_score(position, spread)
        if z is None:
            return None
        if self.config.stop_z > 0.0 and abs(z) >= self.config.stop_z:
            return {"kind": "active_exit:stop", "due_at_ms": now_ms, "terminal": True, "close_reason": "spread_stop_z_reached", "exit_z_score": z}
        if abs(z) <= max(self.config.exit_z, 0.0):
            return {"kind": "active_exit:converged", "due_at_ms": now_ms, "terminal": True, "close_reason": "spread_converged", "exit_z_score": z}
        return None

    def _build_due_event(self, position: SpreadPaperPosition, horizon: dict, now_ms: int, quotes: dict[str, QuoteSnapshot]) -> dict:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        payload = _payload(position, str(horizon["kind"]), now_ms, long_quote, short_quote, self.config)
        if horizon.get("close_reason"):
            payload["paper_close_reason"] = str(horizon["close_reason"])
        if horizon.get("exit_z_score") is not None:
            payload["paper_exit_z_score"] = float(horizon["exit_z_score"])
        return {"kind": "opportunity.paper_closed" if bool(horizon["terminal"]) else "opportunity.paper_markout", "payload": payload}

    def _episode_in_cooldown(self, candidate: SpreadReversionCandidate, bot_id: str) -> bool:
        if self.config.episode_cooldown_ms <= 0:
            return False
        key = _episode_key(candidate, bot_id)
        prior = self._episode_started_at_ms.get(key, 0)
        return prior > 0 and candidate.signal_ts_ms - prior < self.config.episode_cooldown_ms

    def _record_episode(self, position: SpreadPaperPosition) -> None:
        self._episode_started_at_ms[_episode_key_from_position(position)] = position.registered_at_ms


def _paper_bot_specs(config: SpreadPaperConfig) -> list[SpreadPaperBotSpec]:
    specs: list[SpreadPaperBotSpec] = []
    manifest = config.research_manifest
    requested = config.paper_bot_ids or list(manifest.enabled_bot_ids)
    for bot_id in requested:
        cohort = manifest.cohort_for(str(bot_id or "").strip())
        if cohort is None or not cohort.enabled:
            continue
        spec = SpreadPaperBotSpec(
            bot_id=cohort.bot_id,
            cohort=cohort.cohort,
            hypothesis=cohort.hypothesis,
            manifest_version=manifest.version,
            manifest_digest=manifest.digest,
            acceptance_eligible=cohort.acceptance_eligible,
            entry_long_role=cohort.entry_long_role,
            entry_short_role=cohort.entry_short_role,
            exit_long_role=cohort.exit_long_role,
            exit_short_role=cohort.exit_short_role,
            maker_leg=cohort.maker_leg,
            hedge_delay_ms=cohort.hedge_delay_ms,
            control_group=cohort.control_group,
        )
        # The loader validates JSON manifests.  This second gate protects
        # programmatically assembled manifests used by integrations/tests.
        if _paper_bot_execution_supported(spec):
            specs.append(spec)
    return specs


def _paper_bot_has_entry_maker(bot: SpreadPaperBotSpec) -> bool:
    return bot.entry_long_role == "maker" or bot.entry_short_role == "maker"


def _paper_bot_execution_supported(bot: SpreadPaperBotSpec) -> bool:
    """Mirror the manifest's state-machine contract at the runtime boundary."""
    if (
        not isinstance(bot.acceptance_eligible, bool)
        or not isinstance(bot.control_group, bool)
    ):
        return False
    acceptance_eligible = bot.acceptance_eligible is True
    control_group = bot.control_group is True
    entry_maker_legs = tuple(
        leg
        for leg, role in (
            ("long", str(bot.entry_long_role or "").lower()),
            ("short", str(bot.entry_short_role or "").lower()),
        )
        if role == "maker"
    )
    roles = (
        str(bot.entry_long_role or "").lower(),
        str(bot.entry_short_role or "").lower(),
        str(bot.exit_long_role or "").lower(),
        str(bot.exit_short_role or "").lower(),
    )
    if any(role not in {"maker", "taker"} for role in roles):
        return False
    if str(bot.exit_long_role).lower() == "maker" or str(bot.exit_short_role).lower() == "maker":
        return False
    if len(entry_maker_legs) > 1:
        return False
    if entry_maker_legs:
        return (
            str(bot.maker_leg or "").lower() == entry_maker_legs[0]
            and control_group
            and not acceptance_eligible
        )
    return (
        not str(bot.maker_leg or "")
        and (not acceptance_eligible or not control_group)
    )


def _pending_leg(quote: QuoteSnapshot, venue: str, side: str, role: str, raw_price: float, requested_qty: float, config: SpreadPaperConfig) -> SpreadPaperLeg:
    return _leg(quote, venue, side, role, raw_price, 0.0, requested_qty, config, pending=True, state=PaperOrderState.WORKING)


def _filled_leg(
    quote: QuoteSnapshot,
    venue: str,
    side: str,
    role: str,
    raw_price: float,
    qty: float,
    requested_qty: float,
    config: SpreadPaperConfig,
    state: PaperOrderState,
    *,
    filled_at_ms: int,
    execution_source: str = "top_book_only",
) -> SpreadPaperLeg:
    return _leg(
        quote,
        venue,
        side,
        role,
        raw_price,
        qty,
        requested_qty,
        config,
        pending=False,
        state=state,
        filled_at_ms=filled_at_ms,
        execution_source=execution_source,
    )


def _leg(
    quote: QuoteSnapshot,
    venue: str,
    side: str,
    role: str,
    raw_price: float,
    qty: float,
    requested_qty: float,
    config: SpreadPaperConfig,
    *,
    pending: bool,
    state: PaperOrderState,
    filled_at_ms: int = 0,
    execution_source: str = "top_book_only",
) -> SpreadPaperLeg:
    entry_price = None
    entry_slippage = 0.0
    entry_latency = 0.0
    if not pending:
        entry_price, entry_slippage, entry_latency = _slippage_terms(
            raw_price,
            qty,
            config,
            "buy" if side == "long" else "sell",
        )
    notional = 0.0 if entry_price is None else qty * entry_price
    fee_bps = _fee_bps(config, venue, role)
    return SpreadPaperLeg(
        venue=str(venue).lower(), side=side, entry_liquidity_role=_liquidity_role(role), exit_liquidity_role="taker", entry_pending=pending,
        entry_bid=float(quote.bid or 0.0), entry_ask=float(quote.ask or 0.0), entry_bid_size=float(quote.bid_size or 0.0), entry_ask_size=float(quote.ask_size or 0.0), entry_observed_at_ms=int(quote.observed_at_ms or 0),
        mark_price=float(quote.mark_price or 0.0), index_price=float(quote.index_price or 0.0), volume_24h_quote=float(quote.volume_24h_quote or 0.0), open_interest=float(quote.open_interest or 0.0),
        entry_raw_price=raw_price, entry_price=entry_price, qty=qty, entry_notional_quote=notional, entry_fee_bps=fee_bps, entry_fee_quote=notional * fee_bps / 10_000.0, entry_slippage_quote=entry_slippage,
        entry_latency_buffer_quote=entry_latency,
        funding_rate_bps=float(quote.funding_rate_bps or 0.0), funding_timestamp_ms=int(quote.funding_timestamp_ms or 0), funding_interval_ms=int(quote.funding_interval_ms or 0),
        entry_filled_at_ms=0 if pending else max(int(filled_at_ms or 0), 0), order_state=state.value, requested_qty=requested_qty, residual_qty=max(requested_qty - qty, 0.0),
        entry_execution_source=execution_source,
    )


def _with_exit_terms(
    leg: SpreadPaperLeg,
    exit_role: str,
    config: SpreadPaperConfig,
) -> SpreadPaperLeg:
    """Freeze exit role/fee when a paper position is registered or filled."""
    normalized_role = _liquidity_role(exit_role)
    return replace(
        leg,
        exit_liquidity_role=normalized_role,
        exit_fee_bps=_fee_bps(config, leg.venue, normalized_role),
    )


def _slippage_terms(
    raw_price: float,
    quantity: float,
    config: SpreadPaperConfig,
    action: str,
) -> tuple[float, float, float]:
    """Price total execution reserve and expose its latency component."""
    base_price = _apply_slippage(
        raw_price,
        bps=config.slippage_buffer_bps,
        action=action,
    )
    total_price = _apply_slippage(
        raw_price,
        bps=(config.slippage_buffer_bps + config.latency_buffer_bps),
        action=action,
    )
    quantity = max(float(quantity or 0.0), 0.0)
    return (
        total_price,
        abs(total_price - raw_price) * quantity,
        abs(total_price - base_price) * quantity,
    )


def _entry_capacity(quote: QuoteSnapshot, side: str) -> float:
    """BBO-only capacity used for queue-unknown maker experiments."""
    return max(float(quote.ask_size if side == "long" else quote.bid_size) or 0.0, 0.0)


def _exit_capacity(quote: QuoteSnapshot, side: str) -> float:
    """BBO-only capacity retained for maker/legacy paths."""
    return max(float(quote.bid_size if side == "long" else quote.ask_size) or 0.0, 0.0)


def _entry_execution(
    quote: QuoteSnapshot,
    side: str,
    role: str,
    quantity: float,
) -> _ExecutionEstimate | None:
    if _liquidity_role(role) == "maker":
        price = _entry_raw_price(quote, side, role)
        capacity = _entry_capacity(quote, side)
        return _execution_estimate(price, capacity, quantity, "maker_bbo_unknown")
    return _taker_execution(quote, side, quantity, entry=True)


def _exit_execution(
    quote: QuoteSnapshot,
    side: str,
    role: str,
    quantity: float,
) -> _ExecutionEstimate | None:
    if _liquidity_role(role) == "maker":
        price = _exit_raw_price(quote, side, role)
        capacity = _exit_capacity(quote, side)
        return _execution_estimate(price, capacity, quantity, "maker_bbo_unknown")
    return _taker_execution(quote, side, quantity, entry=False)


def _taker_execution(
    quote: QuoteSnapshot,
    side: str,
    quantity: float,
    *,
    entry: bool,
) -> _ExecutionEstimate | None:
    # A long entry/short exit buys asks.  A short entry/long exit sells bids.
    buy = (entry and side == "long") or (not entry and side == "short")
    book_side = "ask" if buy else "bid"
    levels = _coherent_depth_levels(quote, book_side)
    if levels:
        capacity = sum(level_qty for _, level_qty in levels)
        return _execution_estimate(
            _vwap(levels, min(max(float(quantity or 0.0), 0.0), capacity)),
            capacity,
            quantity,
            "l2_vwap",
        )
    price = float((quote.ask if book_side == "ask" else quote.bid) or 0.0)
    capacity = float((quote.ask_size if book_side == "ask" else quote.bid_size) or 0.0)
    return _execution_estimate(price, capacity, quantity, "top_book_only")


def _execution_estimate(
    price: float,
    capacity: float,
    quantity: float,
    source: str,
) -> _ExecutionEstimate | None:
    try:
        price = float(price)
        capacity = float(capacity)
        quantity = float(quantity)
    except (TypeError, ValueError, OverflowError):
        return None
    if not (isfinite(price) and isfinite(capacity) and isfinite(quantity)):
        return None
    if price <= 0.0 or capacity <= 0.0 or quantity <= 0.0:
        return None
    return _ExecutionEstimate(price=price, capacity=capacity, source=source)


def _coherent_depth_levels(
    quote: QuoteSnapshot,
    book_side: str,
) -> tuple[tuple[float, float], ...]:
    """Return a validated L2 side or no levels.

    The paper engine must not combine a stale/deformed ladder with a newer
    BBO.  A nonempty ladder only qualifies when it is finite, correctly
    sorted, and its best price agrees with the published top-of-book.
    """
    raw = quote.ask_depth if book_side == "ask" else quote.bid_depth
    if not isinstance(raw, (tuple, list)) or not raw:
        return ()
    expected_top = float((quote.ask if book_side == "ask" else quote.bid) or 0.0)
    if not isfinite(expected_top) or expected_top <= 0.0:
        return ()
    levels: list[tuple[float, float]] = []
    prior_price: float | None = None
    for item in raw:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            return ()
        try:
            price = float(item[0])
            qty = float(item[1])
        except (TypeError, ValueError, OverflowError):
            return ()
        if not (isfinite(price) and isfinite(qty) and price > 0.0 and qty > 0.0):
            return ()
        if prior_price is not None:
            if (book_side == "ask" and price < prior_price) or (book_side == "bid" and price > prior_price):
                return ()
        levels.append((price, qty))
        prior_price = price
    tolerance = max(abs(expected_top) * 1e-8, 1e-9)
    if abs(levels[0][0] - expected_top) > tolerance:
        return ()
    return tuple(levels)


def _vwap(levels: tuple[tuple[float, float], ...], quantity: float) -> float:
    remaining = max(float(quantity or 0.0), 0.0)
    if remaining <= 0.0:
        return 0.0
    notional = 0.0
    filled = 0.0
    for price, available in levels:
        take = min(remaining, available)
        notional += take * price
        filled += take
        remaining -= take
        if remaining <= 1e-12:
            break
    return notional / filled if filled > 0.0 and remaining <= 1e-9 else 0.0


def _entry_quantity_under_notional_cap(
    quote: QuoteSnapshot,
    side: str,
    role: str,
    maximum_quantity: float,
    notional_cap: float,
) -> float:
    """Largest executable quantity whose actual entry cashflow fits the cap."""
    maximum_quantity = max(float(maximum_quantity or 0.0), 0.0)
    notional_cap = max(float(notional_cap or 0.0), 0.0)
    if maximum_quantity <= 0.0 or notional_cap <= 0.0:
        return 0.0
    estimate = _entry_execution(quote, side, role, maximum_quantity)
    if estimate is None:
        return 0.0
    if maximum_quantity * estimate.price <= notional_cap + 1e-9:
        return maximum_quantity
    low = 0.0
    high = maximum_quantity
    # Books are piecewise-linear, so binary search is deterministic enough
    # here and avoids accidentally choosing a quantity above the cap.
    for _ in range(48):
        mid = (low + high) / 2.0
        if mid <= 0.0:
            break
        mid_estimate = _entry_execution(quote, side, role, mid)
        if mid_estimate is not None and mid * mid_estimate.price <= notional_cap:
            low = mid
        else:
            high = mid
    return low


def _maker_crossed(leg: SpreadPaperLeg, quote: QuoteSnapshot) -> bool:
    if leg.entry_raw_price is None:
        return False
    # Strict inequality: merely touching a displayed bid/ask is not a fill.
    if leg.side == "long":
        return float(quote.ask or 0.0) < float(leg.entry_raw_price)
    return float(quote.bid or 0.0) > float(leg.entry_raw_price)


def _position_has_pending_entry(position: SpreadPaperPosition) -> bool:
    return position.long_leg.entry_pending or position.short_leg.entry_pending


def _position_entry_completed_at_ms(position: SpreadPaperPosition) -> int:
    """Return the pair-level lifecycle start: the later leg fill time."""
    timestamps = (
        int(position.long_leg.entry_filled_at_ms or 0),
        int(position.short_leg.entry_filled_at_ms or 0),
    )
    return max(timestamps) if min(timestamps) > 0 else position.registered_at_ms


def _quotes_fresh(
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
    *,
    now_ms: int,
    ttl_ms: int,
    quote_skew_ms: int,
) -> bool:
    # A quote with a non-finite price/size is not executable evidence.  In
    # particular, ``nan <= 0`` is false in Python, so merely testing price
    # positivity later would let a corrupted source timestamp fabricate an
    # official paper PnL.
    for quote in (long_quote, short_quote):
        try:
            bid = float(quote.bid or 0.0)
            ask = float(quote.ask or 0.0)
            bid_size = float(quote.bid_size or 0.0)
            ask_size = float(quote.ask_size or 0.0)
        except (TypeError, ValueError):
            return False
        if (
            not all(isfinite(value) for value in (bid, ask, bid_size, ask_size))
            or bid <= 0.0
            or ask <= 0.0
            or bid_size < 0.0
            or ask_size < 0.0
        ):
            return False
    try:
        timestamps = (
            int(long_quote.observed_at_ms or 0),
            int(short_quote.observed_at_ms or 0),
        )
    except (TypeError, ValueError, OverflowError):
        return False
    if min(timestamps) <= 0:
        return False
    if any(stamp > int(now_ms or 0) for stamp in timestamps):
        return False
    if max(int(now_ms or 0) - stamp for stamp in timestamps) > max(int(ttl_ms or 0), 0):
        return False
    if abs(timestamps[0] - timestamps[1]) > max(int(quote_skew_ms or 0), 0):
        return False
    return all(str(quote.source or "") not in {"spread_paper_last_good_quote"} for quote in (long_quote, short_quote))


def _missing_quote_keys(position: SpreadPaperPosition, quotes: dict[str, QuoteSnapshot]) -> set[tuple[str, str]]:
    missing: set[tuple[str, str]] = set()
    for venue in (position.long_venue, position.short_venue):
        if _quote_for(quotes, venue, position.symbol) is None:
            missing.add((venue, position.symbol))
    return missing


def _due_horizons(registered_at_ms: int, config: SpreadPaperConfig) -> list[dict]:
    horizons = [{"kind": f"markout_{int(seconds)}s", "due_at_ms": registered_at_ms + int(seconds) * 1000, "terminal": False} for seconds in config.markout_secs if int(seconds) > 0]
    horizons.append({"kind": f"terminal_{int(config.terminal_secs)}s", "due_at_ms": registered_at_ms + int(config.terminal_secs) * 1000, "terminal": True})
    return sorted(horizons, key=lambda item: (int(item["due_at_ms"]), str(item["kind"])))


def _payload(position: SpreadPaperPosition, horizon_kind: str, now_ms: int, long_quote: QuoteSnapshot | None, short_quote: QuoteSnapshot | None, config: SpreadPaperConfig) -> dict:
    base = _base_payload(position, horizon_kind, now_ms, long_quote, short_quote)
    if long_quote is None or short_quote is None or not _quotes_fresh(
        long_quote,
        short_quote,
        now_ms=now_ms,
        ttl_ms=config.quote_ttl_ms,
        quote_skew_ms=config.quote_skew_ms,
    ):
        return _unpriced_payload(base, position, "missing_or_stale_exit_quotes")
    long_execution = _exit_execution(
        long_quote, "long", position.long_leg.exit_liquidity_role, position.long_leg.qty
    )
    short_execution = _exit_execution(
        short_quote, "short", position.short_leg.exit_liquidity_role, position.short_leg.qty
    )
    if (
        long_execution is None
        or short_execution is None
        or long_execution.capacity + 1e-12 < position.long_leg.qty
        or short_execution.capacity + 1e-12 < position.short_leg.qty
    ):
        return _unpriced_payload(
            base,
            position,
            _exit_capacity_failure_reason(long_execution, short_execution),
        )
    return _markout_payload(base, position, now_ms, long_quote, short_quote, config)


def _base_payload(position: SpreadPaperPosition, horizon_kind: str, now_ms: int, long_quote: QuoteSnapshot | None, short_quote: QuoteSnapshot | None) -> dict:
    return {
        "journal_schema_version": SPREAD_PAPER_JOURNAL_SCHEMA_VERSION, "calculation_version": "spread_paper_v3", "model_epoch": position.model_epoch,
        "paper_id": position.paper_id, "candidate_id": position.candidate_id, "review_id": None, "symbol": position.symbol,
        "pair_id": f"{position.long_venue}:{position.short_venue}:{position.symbol}", "long_venue": position.long_venue, "short_venue": position.short_venue,
        "candidate_opportunity_label": position.candidate_opportunity_label, "paper_bot_id": position.paper_bot_id, "paper_cohort": position.paper_cohort,
        "research_manifest_version": position.research_manifest_version, "research_manifest_digest": position.research_manifest_digest, "research_hypothesis": position.research_hypothesis, "acceptance_eligible": position.acceptance_eligible,
        "paper_entry_mode": position.paper_entry_mode, "paper_exit_mode": position.paper_exit_mode, "paper_execution_model": position.paper_entry_mode,
        "paper_maker_leg": position.paper_maker_leg, "paper_hedge_delay_ms": position.paper_hedge_delay_ms, "paper_control_group": position.paper_control_group,
        "paper_fill_assumption": position.paper_fill_assumption, "horizon_kind": horizon_kind, "registered_at_ms": position.registered_at_ms, "entry_eligible_at_ms": position.entry_eligible_at_ms, "evaluated_at_ms": now_ms,
        "account_fee_evidence_complete": position.account_fee_evidence_complete,
        "account_fee_evidence_observed_at_ms": position.account_fee_evidence_observed_at_ms,
        "account_fee_evidence_source": position.account_fee_evidence_source,
        "account_fee_evidence_fingerprint": position.account_fee_evidence_fingerprint,
        "account_fee_evidence_provenance": list(position.account_fee_evidence_provenance),
        "research_sample_split": position.research_sample_split,
        "volatility_regime": position.volatility_regime,
        "selected_real_trade": False, "not_selected_reason": "spread_shadow_paper", "paper_order_status": _position_order_state(position),
        "paper_entry_notional_quote": position.entry_notional_quote, "requested_base_qty": position.requested_base_qty, "filled_base_qty": position.filled_base_qty, "residual_base_qty": position.residual_base_qty,
        "delta_exposure_base_qty": position.delta_exposure_base_qty, "maker_fill_observed_at_ms": position.maker_fill_observed_at_ms,
        "paper_fill_capacity_source": _entry_capacity_source(position),
        "official_pnl": position.official_pnl is True, "candidate_snapshot": dict(position.candidate_snapshot), "entry_market_snapshot": dict(position.entry_market_snapshot),
        "exit_market_snapshot": _market_snapshot_payload(long_quote, short_quote), "funding_advantage_bps": position.short_leg.funding_rate_bps - position.long_leg.funding_rate_bps,
    }


def _markout_payload(base: dict, position: SpreadPaperPosition, now_ms: int, long_quote: QuoteSnapshot, short_quote: QuoteSnapshot, config: SpreadPaperConfig) -> dict:
    long_execution = _exit_execution(
        long_quote,
        "long",
        position.long_leg.exit_liquidity_role,
        max(position.long_leg.qty, 1e-12),
    )
    short_execution = _exit_execution(
        short_quote,
        "short",
        position.short_leg.exit_liquidity_role,
        max(position.short_leg.qty, 1e-12),
    )
    if (
        long_execution is None
        or short_execution is None
        or position.long_leg.entry_raw_price is None
        or position.short_leg.entry_raw_price is None
    ):
        return _unpriced_payload(base, position, "invalid_exit_prices")
    if config.require_l2_vwap and not _executions_have_l2(
        long_execution,
        short_execution,
    ):
        return _unpriced_payload(base, position, "missing_l2_exit_depth")
    if (
        (position.long_leg.qty > 0.0 and long_execution.capacity + 1e-12 < position.long_leg.qty)
        or (position.short_leg.qty > 0.0 and short_execution.capacity + 1e-12 < position.short_leg.qty)
    ):
        return _unpriced_payload(
            base,
            position,
            _exit_capacity_failure_reason(long_execution, short_execution),
        )
    long_raw = long_execution.price
    short_raw = short_execution.price
    long_exit, long_exit_slippage, long_exit_latency = _slippage_terms(
        long_raw,
        position.long_leg.qty,
        config,
        "sell",
    )
    short_exit, short_exit_slippage, short_exit_latency = _slippage_terms(
        short_raw,
        position.short_leg.qty,
        config,
        "buy",
    )
    long_gross = position.long_leg.qty * (long_raw - position.long_leg.entry_raw_price)
    short_gross = position.short_leg.qty * (position.short_leg.entry_raw_price - short_raw)
    long_exit_fee = position.long_leg.qty * long_exit * position.long_leg.exit_fee_bps / 10_000.0
    short_exit_fee = position.short_leg.qty * short_exit * position.short_leg.exit_fee_bps / 10_000.0
    entry_fee = position.long_leg.entry_fee_quote + position.short_leg.entry_fee_quote
    entry_slippage = position.long_leg.entry_slippage_quote + position.short_leg.entry_slippage_quote
    exit_slippage = long_exit_slippage + short_exit_slippage
    entry_latency = (
        position.long_leg.entry_latency_buffer_quote
        + position.short_leg.entry_latency_buffer_quote
    )
    exit_latency = long_exit_latency + short_exit_latency
    settled = _settlement_funding_quote(position, now_ms)
    accrued = _accrued_funding_quote(position, now_ms)
    gross = long_gross + short_gross
    fee = entry_fee + long_exit_fee + short_exit_fee
    slippage = entry_slippage + exit_slippage
    matched_notional = max(
        (position.long_leg.entry_notional_quote + position.short_leg.entry_notional_quote) / 2.0,
        1e-12,
    )
    adverse_selection_assumption = (
        matched_notional
        * max(
            float(position.candidate_snapshot.get("adverse_selection_bps", 0.0) or 0.0),
            0.0,
        )
        / 10_000.0
    )
    # This is a baseline cost promised by the paper economics contract, not
    # only a stress-display field.  Excluding it here would overstate every
    # headline and make the 1x statistic incomparable with stress outcomes.
    net = gross + settled - fee - slippage - adverse_selection_assumption
    funding_evidence_complete = _funding_settlement_evidence_complete(
        position,
        now_ms,
        long_quote,
        short_quote,
    )
    official_pnl = bool(
        position.official_pnl is True
        and abs(position.delta_exposure_base_qty) <= 1e-12
        and position.residual_base_qty <= 1e-12
        # Entry was permissioned only with L2, but an in-process config reload
        # must not let a later diagnostic BBO exit inherit that permission.
        # Official acceptance therefore requires executable L2 on this exit
        # as an immutable per-observation fact, independent of today's gate.
        and _executions_have_l2(long_execution, short_execution)
        and funding_evidence_complete
    )
    base.update({
        "paper_gross_quote": gross, "paper_fee_quote": fee, "paper_entry_fee_quote": entry_fee, "paper_exit_fee_quote": long_exit_fee + short_exit_fee,
        "paper_funding_quote": settled, "accrued_funding_estimate_quote": accrued, "settlement_realized_funding_quote": settled,
        "settlement_funding_rate_evidence": _settlement_funding_evidence(
            position,
            now_ms,
            long_quote,
            short_quote,
        ),
        "funding_settlement_evidence_complete": funding_evidence_complete,
        "paper_slippage_quote": slippage, "paper_entry_slippage_quote": entry_slippage, "paper_exit_slippage_quote": exit_slippage,
        "paper_latency_buffer_quote": entry_latency + exit_latency,
        "paper_entry_latency_buffer_quote": entry_latency,
        "paper_exit_latency_buffer_quote": exit_latency,
        "paper_hedge_delay_quote": _hedge_delay_quote(position), "paper_residual_quote": _residual_gross_quote(position, long_gross, short_gross),
        "paper_adverse_selection_quote": 0.0,
        "paper_adverse_selection_assumption_quote": adverse_selection_assumption,
        "paper_matched_entry_notional_quote": matched_notional,
        "paper_net_quote": net, "paper_net_bps": net / matched_notional * 10_000.0,
        "paper_unpriced": False, "official_pnl": official_pnl, "opportunity_label": classify_paper_outcome(False, net, None),
        "long_leg": _leg_payload(position.long_leg, long_raw, long_exit, long_exit_fee, long_gross, long_execution.source),
        "short_leg": _leg_payload(position.short_leg, short_raw, short_exit, short_exit_fee, short_gross, short_execution.source),
        "paper_exit_capacity_source": _sources_summary(long_execution.source, short_execution.source),
    })
    return base


def _unpriced_payload(base: dict, position: SpreadPaperPosition, reason: str) -> dict:
    base.update({
        "paper_gross_quote": None, "paper_fee_quote": position.long_leg.entry_fee_quote + position.short_leg.entry_fee_quote,
        "paper_entry_fee_quote": position.long_leg.entry_fee_quote + position.short_leg.entry_fee_quote, "paper_exit_fee_quote": 0.0,
        "paper_funding_quote": 0.0, "accrued_funding_estimate_quote": 0.0, "settlement_realized_funding_quote": 0.0,
        "paper_slippage_quote": position.long_leg.entry_slippage_quote + position.short_leg.entry_slippage_quote,
        "paper_entry_slippage_quote": position.long_leg.entry_slippage_quote + position.short_leg.entry_slippage_quote, "paper_exit_slippage_quote": 0.0,
        "paper_latency_buffer_quote": position.long_leg.entry_latency_buffer_quote + position.short_leg.entry_latency_buffer_quote,
        "paper_entry_latency_buffer_quote": position.long_leg.entry_latency_buffer_quote + position.short_leg.entry_latency_buffer_quote,
        "paper_exit_latency_buffer_quote": 0.0,
        "paper_hedge_delay_quote": None, "paper_residual_quote": None, "paper_adverse_selection_quote": None,
        "paper_adverse_selection_assumption_quote": None, "paper_matched_entry_notional_quote": None,
        "paper_net_quote": None, "paper_net_bps": None, "paper_unpriced": True, "paper_skip_reason": reason, "official_pnl": False,
        "opportunity_label": classify_paper_outcome(False, None, None), "long_leg": _leg_payload(position.long_leg), "short_leg": _leg_payload(position.short_leg),
    })
    return base


def _unpriced_event(
    position: SpreadPaperPosition,
    horizon: dict,
    now_ms: int,
    quotes: dict[str, QuoteSnapshot],
    config: SpreadPaperConfig,
) -> dict:
    long_quote = _quote_for(quotes, position.long_venue, position.symbol)
    short_quote = _quote_for(quotes, position.short_venue, position.symbol)
    reason = "missing_or_stale_exit_quotes"
    if (
        long_quote is not None
        and short_quote is not None
        and _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=config.quote_ttl_ms,
            quote_skew_ms=config.quote_skew_ms,
        )
    ):
        long_execution = _exit_execution(
            long_quote,
            "long",
            position.long_leg.exit_liquidity_role,
            position.long_leg.qty,
        )
        short_execution = _exit_execution(
            short_quote,
            "short",
            position.short_leg.exit_liquidity_role,
            position.short_leg.qty,
        )
        if long_execution is None or short_execution is None:
            reason = "invalid_exit_prices"
        elif config.require_l2_vwap and not _executions_have_l2(
            long_execution,
            short_execution,
        ):
            reason = "missing_l2_exit_depth"
        elif (
            long_execution.capacity + 1e-12 < position.long_leg.qty
            or short_execution.capacity + 1e-12 < position.short_leg.qty
        ):
            reason = _exit_capacity_failure_reason(long_execution, short_execution)
    payload = _unpriced_payload(
        _base_payload(position, str(horizon["kind"]), now_ms, long_quote, short_quote),
        position,
        reason,
    )
    return {"kind": "opportunity.paper_closed" if bool(horizon["terminal"]) else "opportunity.paper_evaluation_skipped", "payload": payload}


def _delta_markout_event(
    position: SpreadPaperPosition,
    horizon: dict,
    now_ms: int,
    quotes: dict[str, QuoteSnapshot],
    config: SpreadPaperConfig,
) -> dict:
    """Record naked maker-leg markout without manufacturing two-leg PnL."""
    long_quote = _quote_for(quotes, position.long_venue, position.symbol)
    short_quote = _quote_for(quotes, position.short_venue, position.symbol)
    base = _base_payload(position, str(horizon["kind"]), now_ms, long_quote, short_quote)
    if (
        long_quote is None
        or short_quote is None
        or not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=config.quote_ttl_ms,
            quote_skew_ms=config.quote_skew_ms,
        )
    ):
        payload = _unpriced_payload(base, position, "missing_or_stale_delta_markout_quotes")
        return {"kind": "opportunity.paper_delta_markout", "payload": payload}

    leg = position.long_leg if position.paper_maker_leg == "long" else position.short_leg
    quote = long_quote if position.paper_maker_leg == "long" else short_quote
    execution = _exit_execution(quote, leg.side, "taker", leg.qty)
    if (
        leg.entry_raw_price is None
        or execution is None
        or execution.capacity + 1e-12 < leg.qty
        or leg.qty <= 0.0
    ):
        payload = _unpriced_payload(base, position, "invalid_delta_markout_price")
        return {"kind": "opportunity.paper_delta_markout", "payload": payload}
    exit_raw = execution.price
    gross = (
        leg.qty * (exit_raw - leg.entry_raw_price)
        if leg.side == "long"
        else leg.qty * (leg.entry_raw_price - exit_raw)
    )
    base.update({
        "paper_delta_markout_quote": gross,
        "paper_delta_markout_price": exit_raw,
        "paper_delta_capacity_source": execution.source,
        "paper_unpriced": False,
        # A one-leg observation is diagnostic evidence only, never an
        # executable two-leg PnL for the acceptance cohort.
        "paper_net_quote": None,
        "paper_net_bps": None,
        "official_pnl": False,
        "opportunity_label": classify_paper_outcome(False, None, None),
        "long_leg": _leg_payload(position.long_leg),
        "short_leg": _leg_payload(position.short_leg),
    })
    return {"kind": "opportunity.paper_delta_markout", "payload": base}


def _expired_event(
    position: SpreadPaperPosition,
    now_ms: int,
    quotes: dict[str, QuoteSnapshot],
    reason: str,
    config: SpreadPaperConfig,
) -> dict:
    long_quote = _quote_for(quotes, position.long_venue, position.symbol)
    short_quote = _quote_for(quotes, position.short_venue, position.symbol)
    base = _base_payload(position, "terminal_expired", now_ms, long_quote, short_quote)
    if (
        long_quote is not None
        and short_quote is not None
        and _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=config.quote_ttl_ms,
            quote_skew_ms=config.quote_skew_ms,
        )
    ):
        # A failed hedge is never official PnL, but a fresh executable quote is
        # still valuable evidence of the residual directional markout.
        payload = _markout_payload(
            base,
            position,
            now_ms,
            long_quote,
            short_quote,
            config,
        )
        payload["official_pnl"] = False
        payload["paper_skip_reason"] = reason
    else:
        payload = _unpriced_payload(base, position, reason)
    payload["paper_order_status"] = PaperOrderState.EXPIRED.value
    return {"kind": "opportunity.paper_expired", "payload": payload}


def _registration_event(position: SpreadPaperPosition) -> dict:
    payload = _base_payload(position, "", position.registered_at_ms, None, None)
    payload.update({"paper_funding_quote": 0.0, "accrued_funding_estimate_quote": 0.0, "settlement_realized_funding_quote": 0.0, "due_horizons": position.due_horizons, "long_leg": _leg_payload(position.long_leg), "short_leg": _leg_payload(position.short_leg)})
    return {"kind": "opportunity.paper_registered", "payload": payload}


def _hedge_filled_event(position: SpreadPaperPosition) -> dict:
    payload = _registration_event(position)["payload"]
    payload["paper_order_status"] = _position_order_state(position)
    return {"kind": "opportunity.paper_hedge_filled", "payload": payload}


def _taker_pair_filled_event(position: SpreadPaperPosition) -> dict:
    """Persist the later quote that actually filled a delayed taker pair."""
    payload = _registration_event(position)["payload"]
    payload["evaluated_at_ms"] = _position_entry_completed_at_ms(position)
    payload["paper_order_status"] = _position_order_state(position)
    payload["paper_fill_evidence"] = "strictly_newer_coherent_quote_after_decision_latency"
    return {"kind": "opportunity.paper_taker_pair_filled", "payload": payload}


def _maker_fill_observed_event(position: SpreadPaperPosition) -> dict:
    """Persist naked maker exposure before its delayed hedge is eligible.

    A BBO cross is intentionally not treated as queue/trade-tape-confirmed
    execution.  Keeping the state as ``UNKNOWN`` prevents experimental maker
    observations from entering the official taker/taker acceptance cohort.
    """
    payload = _registration_event(position)["payload"]
    payload.update({
        "paper_order_status": PaperOrderState.UNKNOWN.value,
        "paper_maker_fill_evidence": "crossed_without_trade_tape_or_queue",
    })
    return {"kind": "opportunity.paper_maker_fill_observed", "payload": payload}


def _funding_settlement_observed_event(
    position: SpreadPaperPosition,
    settlement: FundingSettlement,
) -> dict:
    payload = _registration_event(position)["payload"]
    payload.update({
        "paper_order_status": _position_order_state(position),
        "funding_settlement": _funding_settlement_payload(settlement),
        "settlement_funding_rate_evidence": "actual_position_allocated_funding_ledger",
    })
    return {"kind": "opportunity.paper_funding_settlement_observed", "payload": payload}


def _position_from_payload(payload: dict) -> SpreadPaperPosition | None:
    long_leg = _leg_from_payload(payload.get("long_leg"))
    short_leg = _leg_from_payload(payload.get("short_leg"))
    paper_id = str(payload.get("paper_id", "") or "")
    if long_leg is None or short_leg is None or not paper_id:
        return None
    horizons = _valid_restored_horizons(payload.get("due_horizons"))
    if horizons is None:
        return None
    paper_control_group = _strict_json_bool(payload.get("paper_control_group"))
    official_pnl = _strict_json_bool(payload.get("official_pnl"))
    acceptance_eligible = _strict_json_bool(payload.get("acceptance_eligible"))
    account_fee_evidence_complete = _strict_json_bool(
        payload.get("account_fee_evidence_complete")
    )
    if (
        paper_control_group is None
        or official_pnl is None
        or acceptance_eligible is None
        or account_fee_evidence_complete is None
    ):
        return None
    try:
        position = SpreadPaperPosition(
        paper_id=paper_id, candidate_id=str(payload.get("candidate_id", "") or ""), symbol=str(payload.get("symbol", "") or ""), long_venue=str(payload.get("long_venue", "") or ""), short_venue=str(payload.get("short_venue", "") or ""),
        candidate_opportunity_label=str(payload.get("candidate_opportunity_label", "spread_reversion") or "spread_reversion"), paper_bot_id=str(payload.get("paper_bot_id", "tt_conservative") or "tt_conservative"),
        paper_cohort=str(payload.get("paper_cohort", "legacy") or "legacy"), paper_entry_mode=str(payload.get("paper_entry_mode", "") or ""), paper_exit_mode=str(payload.get("paper_exit_mode", "") or ""),
        paper_maker_leg=str(payload.get("paper_maker_leg", "") or ""), paper_hedge_delay_ms=int(payload.get("paper_hedge_delay_ms", 0) or 0), paper_control_group=paper_control_group,
        paper_fill_assumption=str(payload.get("paper_fill_assumption", "") or ""), finalist_rank=int(payload.get("finalist_rank", 0) or 0), registered_at_ms=int(payload.get("registered_at_ms", 0) or 0), entry_eligible_at_ms=int(payload.get("entry_eligible_at_ms", 0) or 0),
        entry_notional_quote=float(payload.get("paper_entry_notional_quote", 0.0) or 0.0), long_leg=long_leg, short_leg=short_leg,
        candidate_snapshot=dict(payload.get("candidate_snapshot", {}) or {}), entry_market_snapshot=dict(payload.get("entry_market_snapshot", {}) or {}), due_horizons=horizons,
        requested_base_qty=float(payload.get("requested_base_qty", max(long_leg.requested_qty, short_leg.requested_qty)) or 0.0), filled_base_qty=float(payload.get("filled_base_qty", min(long_leg.qty, short_leg.qty)) or 0.0),
        residual_base_qty=float(payload.get("residual_base_qty", max(long_leg.residual_qty, short_leg.residual_qty)) or 0.0),
        delta_exposure_base_qty=float(payload.get("delta_exposure_base_qty", long_leg.qty - short_leg.qty) or 0.0),
        maker_fill_observed_at_ms=int(payload.get("maker_fill_observed_at_ms", 0) or 0), model_epoch=str(payload.get("model_epoch", "v1_legacy") or "v1_legacy"),
        official_pnl=official_pnl,
        research_manifest_version=str(payload.get("research_manifest_version", "legacy") or "legacy"),
        research_manifest_digest=str(payload.get("research_manifest_digest", "") or ""),
        research_hypothesis=str(payload.get("research_hypothesis", "") or ""),
        acceptance_eligible=acceptance_eligible,
        account_fee_evidence_complete=account_fee_evidence_complete,
        account_fee_evidence_observed_at_ms=int(
            payload.get("account_fee_evidence_observed_at_ms", 0) or 0
        ),
        account_fee_evidence_source=str(
            payload.get("account_fee_evidence_source", "") or ""
        ),
        account_fee_evidence_fingerprint=str(
            payload.get("account_fee_evidence_fingerprint", "") or ""
        ),
        account_fee_evidence_provenance=_provenance_payload(
            payload.get("account_fee_evidence_provenance")
        ),
        research_sample_split=str(
            payload.get("research_sample_split", "in_sample") or "in_sample"
        ),
        volatility_regime=str(
            payload.get("volatility_regime", "unknown") or "unknown"
        ),
    )
    except (TypeError, ValueError, OverflowError):
        return None
    return position if _paper_position_numerics_valid(position) else None


def _provenance_payload(value: object) -> list[dict[str, object]]:
    """Accept only JSON-object provenance rows at the restart boundary."""
    if not isinstance(value, list) or not value:
        return []
    if any(not isinstance(item, dict) for item in value):
        return []
    return [dict(item) for item in value]


def _valid_restored_horizons(value: object) -> list[dict] | None:
    if not isinstance(value, list) or not value:
        return None
    horizons: list[dict] = []
    kinds: set[str] = set()
    terminal_count = 0
    for raw in value:
        if not isinstance(raw, dict):
            return None
        kind = str(raw.get("kind", "") or "")
        try:
            due_at_ms = int(raw.get("due_at_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return None
        if not kind or kind in kinds or due_at_ms <= 0:
            return None
        terminal = _strict_json_bool(raw.get("terminal"))
        if terminal is None:
            return None
        terminal_count += int(terminal)
        horizons.append({"kind": kind, "due_at_ms": due_at_ms, "terminal": terminal})
        kinds.add(kind)
    if terminal_count != 1:
        return None
    return sorted(horizons, key=lambda item: (int(item["due_at_ms"]), str(item["kind"])))


def _paper_position_numerics_valid(position: SpreadPaperPosition) -> bool:
    numbers = (
        position.entry_notional_quote,
        position.requested_base_qty,
        position.filled_base_qty,
        position.residual_base_qty,
        position.delta_exposure_base_qty,
    )
    if not all(isfinite(float(value)) for value in numbers):
        return False
    if any(value < 0.0 for value in numbers[:4]):
        return False
    return bool(
        position.candidate_id
        and position.symbol
        and position.long_venue
        and position.short_venue
        and position.long_venue.lower() != position.short_venue.lower()
        and position.registered_at_ms > 0
        and position.entry_eligible_at_ms > position.registered_at_ms
    )


def _restorable_current_v3_record(payload: dict, config: SpreadPaperConfig) -> bool:
    try:
        journal_schema_version = int(payload.get("journal_schema_version", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    if journal_schema_version != SPREAD_PAPER_JOURNAL_SCHEMA_VERSION:
        return False
    if str(payload.get("calculation_version", "") or "") != "spread_paper_v3":
        return False
    if str(payload.get("model_epoch", "") or "") != config.model_epoch:
        return False
    if (
        str(payload.get("research_manifest_digest", "") or "").lower()
        != config.research_manifest.digest
    ):
        return False
    candidate = payload.get("candidate_snapshot")
    if not isinstance(candidate, dict):
        return False
    # Journal v6 cannot infer a missing exit cost, account-evidence identity,
    # or manifest digest from today's schedule.  A partial row would otherwise
    # silently become a zero-fee record or altered research contract on
    # restore, which is exactly the economic rewrite this schema prevents.
    frozen_leg_fields = {
        "entry_fee_bps",
        "exit_fee_bps",
        "entry_fee_quote",
        "entry_slippage_quote",
        "entry_latency_buffer_quote",
        "entry_execution_source",
    }
    for leg_name in ("long_leg", "short_leg"):
        leg = payload.get(leg_name)
        if not isinstance(leg, dict) or not frozen_leg_fields.issubset(leg):
            return False
    account_evidence_matches = (
        not config.require_account_fee_evidence
        or _restored_account_fee_evidence_matches_candidate(payload, candidate)
    )
    return (
        candidate.get("economics_complete") is True
        and candidate.get("fee_evidence_complete") is True
        and str(candidate.get("contract_normalization_status", "") or "").lower()
        == "complete"
        and str(candidate.get("calculation_version", "") or "")
        == _candidate_calculation_version(config)
        and str(candidate.get("model_epoch", "") or "") == config.model_epoch
        and account_evidence_matches
    )


def _restored_account_fee_evidence_matches_candidate(
    payload: dict, candidate: dict
) -> bool:
    """Keep the position-level fee receipt bound to its admission snapshot.

    A schema-v5 row freezes both locations of this evidence.  Accepting either
    one in isolation would allow a malformed restart record to carry the
    candidate economics from one account-fee observation and the reported
    provenance from another.
    """
    provenance = candidate.get("account_fee_evidence_provenance")
    if (
        candidate.get("account_fee_evidence_complete") is not True
        or not isinstance(provenance, list)
        or not provenance
        or any(not isinstance(row, dict) for row in provenance)
        or not str(candidate.get("account_fee_evidence_fingerprint") or "")
        or not str(candidate.get("account_fee_evidence_source") or "")
    ):
        return False
    try:
        observed_at_ms = int(candidate.get("account_fee_evidence_observed_at_ms") or 0)
        payload_observed_at_ms = int(
            payload.get("account_fee_evidence_observed_at_ms") or 0
        )
    except (TypeError, ValueError, OverflowError):
        return False
    return bool(
        observed_at_ms > 0
        and payload.get("account_fee_evidence_complete") is True
        and payload_observed_at_ms == observed_at_ms
        and str(payload.get("account_fee_evidence_source") or "")
        == str(candidate.get("account_fee_evidence_source") or "")
        and str(payload.get("account_fee_evidence_fingerprint") or "")
        == str(candidate.get("account_fee_evidence_fingerprint") or "")
        and payload.get("account_fee_evidence_provenance") == provenance
    )


def _restored_position_has_fee_evidence(
    position: SpreadPaperPosition,
    config: SpreadPaperConfig,
) -> bool:
    # A schema-v5 record is intentionally self-contained: the four pricing
    # terms were frozen at entry, so a later service refresh must not either
    # rewrite its economics or make a valid in-flight position un-restorable.
    # Strict account-fee mode still requires the persisted admission proof.
    return _frozen_fee_terms_valid(position) and _position_account_fee_evidence_complete(
        position, config
    )


def _restored_position_is_official_eligible(
    position: SpreadPaperPosition,
    config: SpreadPaperConfig,
) -> bool:
    # The runtime gate above is deliberately repeated during recovery.  A
    # journal row produced by an earlier diagnostic configuration cannot be
    # promoted merely because its boolean flag survived serialization.
    if (
        config.require_l2_vwap is not True
        or config.require_account_fee_evidence is not True
    ):
        return False
    baseline_entry_sources = {"l2_vwap"}
    return bool(
        position.official_pnl is True
        and position.paper_entry_mode == "long_taker:short_taker"
        and position.paper_exit_mode == "long_taker:short_taker"
        and not position.paper_maker_leg
        and position.paper_control_group is False
        and position.acceptance_eligible is True
        and _position_account_fee_evidence_complete(position, config)
        and not position.long_leg.entry_pending
        and not position.short_leg.entry_pending
        # The mode strings are reporting fields; on restart they cannot be
        # trusted as execution evidence.  Re-derive baseline eligibility from
        # both persisted legs so a forged maker/unknown cohort cannot be
        # promoted into the taker/taker acceptance sample.
        and position.long_leg.entry_liquidity_role == "taker"
        and position.short_leg.entry_liquidity_role == "taker"
        and position.long_leg.exit_liquidity_role == "taker"
        and position.short_leg.exit_liquidity_role == "taker"
        and position.long_leg.entry_execution_source in baseline_entry_sources
        and position.short_leg.entry_execution_source in baseline_entry_sources
        and position.long_leg.entry_filled_at_ms > 0
        and position.short_leg.entry_filled_at_ms > 0
        and position.long_leg.order_state == PaperOrderState.FILLED.value
        and position.short_leg.order_state == PaperOrderState.FILLED.value
        and position.residual_base_qty <= 1e-12
        and abs(position.delta_exposure_base_qty) <= 1e-12
        and abs(position.long_leg.qty - position.short_leg.qty) <= 1e-12
    )


def _leg_from_payload(value: object) -> SpreadPaperLeg | None:
    if not isinstance(value, dict) or str(value.get("side", "")) not in {"long", "short"}:
        return None
    entry_pending = _strict_json_bool(value.get("entry_pending"))
    funding_settlement_conflict = _strict_json_bool(
        value.get("funding_settlement_conflict")
    )
    if entry_pending is None or funding_settlement_conflict is None:
        return None
    settlements, restored_settlement_conflict = _settlements_from_payload(
        value.get("funding_settlements")
    )
    try:
        leg = SpreadPaperLeg(
            venue=str(value.get("venue", "") or ""), side=str(value["side"]), entry_liquidity_role=str(value.get("entry_liquidity_role", "taker") or "taker"), exit_liquidity_role=str(value.get("exit_liquidity_role", "taker") or "taker"), entry_pending=entry_pending,
            entry_bid=float(value.get("entry_bid", 0.0) or 0.0), entry_ask=float(value.get("entry_ask", 0.0) or 0.0), entry_bid_size=float(value.get("entry_bid_size", 0.0) or 0.0), entry_ask_size=float(value.get("entry_ask_size", 0.0) or 0.0), entry_observed_at_ms=int(value.get("entry_observed_at_ms", 0) or 0),
            mark_price=float(value.get("mark_price", 0.0) or 0.0), index_price=float(value.get("index_price", 0.0) or 0.0), volume_24h_quote=float(value.get("volume_24h_quote", 0.0) or 0.0), open_interest=float(value.get("open_interest", 0.0) or 0.0),
            entry_raw_price=_optional_float(value.get("entry_raw_price")), entry_price=_optional_float(value.get("entry_price")), qty=float(value.get("qty", 0.0) or 0.0), entry_notional_quote=float(value.get("entry_notional_quote", 0.0) or 0.0),
            entry_fee_bps=float(value.get("entry_fee_bps", 0.0) or 0.0), entry_fee_quote=float(value.get("entry_fee_quote", 0.0) or 0.0), entry_slippage_quote=float(value.get("entry_slippage_quote", 0.0) or 0.0), exit_fee_bps=float(value.get("exit_fee_bps", 0.0) or 0.0), entry_latency_buffer_quote=float(value.get("entry_latency_buffer_quote", 0.0) or 0.0), funding_rate_bps=float(value.get("funding_rate_bps", 0.0) or 0.0), funding_timestamp_ms=int(value.get("funding_timestamp_ms", 0) or 0), funding_interval_ms=int(value.get("funding_interval_ms", 0) or 0), entry_filled_at_ms=int(value.get("entry_filled_at_ms", 0) or 0),
            order_state=str(value.get("order_state", PaperOrderState.FILLED.value) or PaperOrderState.FILLED.value), requested_qty=float(value.get("requested_qty", value.get("qty", 0.0)) or 0.0), residual_qty=float(value.get("residual_qty", 0.0) or 0.0),
            funding_settlements=settlements, funding_settlement_conflict=(
                funding_settlement_conflict or restored_settlement_conflict
            ),
            entry_execution_source=str(value.get("entry_execution_source", "top_book_only") or "top_book_only"),
        )
    except (TypeError, ValueError, OverflowError):
        return None
    return leg if _paper_leg_numerics_valid(leg) else None


def _strict_json_bool(value: object) -> bool | None:
    """Accept only an actual JSON boolean at the persisted paper boundary."""
    return value if isinstance(value, bool) else None


def _paper_leg_numerics_valid(leg: SpreadPaperLeg) -> bool:
    """Reject corrupt persisted paper positions before they can emit PnL.

    Journal JSON is an input boundary after a restart.  ``NaN`` and infinity
    compare unusually in Python and can otherwise turn a malformed historical
    record into a seemingly official, but non-numeric, paper result.
    """
    numbers = (
        leg.entry_bid,
        leg.entry_ask,
        leg.entry_bid_size,
        leg.entry_ask_size,
        leg.mark_price,
        leg.index_price,
        leg.volume_24h_quote,
        leg.open_interest,
        leg.qty,
        leg.entry_notional_quote,
        leg.entry_fee_bps,
        leg.exit_fee_bps,
        leg.entry_fee_quote,
        leg.entry_slippage_quote,
        leg.entry_latency_buffer_quote,
        leg.funding_rate_bps,
        leg.requested_qty,
        leg.residual_qty,
    )
    # A signed maker rebate can legitimately make fee bps/quote negative;
    # a taker leg cannot claim that credit.  Notional, quantity and execution
    # buffers may never be negative.
    return (
        all(isfinite(float(number)) for number in numbers)
        and all(
            value >= 0.0
            for value in (
                leg.qty,
                leg.entry_notional_quote,
                leg.requested_qty,
                leg.residual_qty,
                leg.entry_slippage_quote,
                leg.entry_latency_buffer_quote,
            )
        )
        and (
            _liquidity_role(leg.entry_liquidity_role) == "maker"
            or (leg.entry_fee_bps >= 0.0 and leg.entry_fee_quote >= 0.0)
        )
        and (
            _liquidity_role(leg.exit_liquidity_role) == "maker"
            or leg.exit_fee_bps >= 0.0
        )
    )


def _frozen_fee_terms_valid(position: SpreadPaperPosition) -> bool:
    """Check immutable four-leg pricing terms stored in a v5 journal row."""
    return _paper_leg_numerics_valid(position.long_leg) and _paper_leg_numerics_valid(
        position.short_leg
    )


def _candidate_snapshot(candidate: SpreadReversionCandidate) -> dict:
    """Freeze every admission and economics fact needed for paper replay."""
    return {
        "canonical_venue_a": candidate.canonical_venue_a,
        "canonical_venue_b": candidate.canonical_venue_b,
        "current_signed_mid_spread_bps": candidate.current_signed_mid_spread_bps,
        "current_executable_entry_spread_bps": (
            candidate.current_executable_entry_spread_bps
        ),
        "equilibrium_spread_bps": candidate.equilibrium_spread_bps,
        "target_exit_spread_bps": candidate.target_exit_spread_bps,
        "gross_reversion_edge_bps": candidate.gross_reversion_edge_bps,
        "expected_net_edge_bps": candidate.expected_net_edge_bps,
        "worst_case_edge_bps": candidate.worst_case_edge_bps,
        "gross_signal_edge_bps": candidate.gross_signal_edge_bps,
        "funding_edge_bps": candidate.funding_edge_bps,
        "entry_cross_bps": candidate.entry_cross_bps,
        "expected_exit_cross_bps": candidate.expected_exit_cross_bps,
        "entry_fee_bps": candidate.entry_fee_bps,
        "exit_fee_bps": candidate.exit_fee_bps,
        "entry_slippage_bps": candidate.entry_slippage_bps,
        "exit_slippage_bps": candidate.exit_slippage_bps,
        "adverse_selection_bps": candidate.adverse_selection_bps,
        "capital_buffer_bps": candidate.capital_buffer_bps,
        "execution_buffer_bps": candidate.execution_buffer_bps,
        "venue_risk_haircut_bps": candidate.venue_risk_haircut_bps,
        "rolling_mean_bps": candidate.rolling_mean_bps,
        "rolling_std_bps": candidate.rolling_std_bps,
        "z_score": candidate.z_score,
        "calculation_version": candidate.calculation_version,
        "model_epoch": candidate.model_epoch,
        "economics_complete": candidate.economics_complete,
        "fee_evidence_complete": candidate.fee_evidence_complete,
        "account_fee_evidence_complete": candidate.account_fee_evidence_complete,
        "account_fee_evidence_observed_at_ms": (
            candidate.account_fee_evidence_observed_at_ms
        ),
        "account_fee_evidence_source": candidate.account_fee_evidence_source,
        "account_fee_evidence_fingerprint": candidate.account_fee_evidence_fingerprint,
        "account_fee_evidence_provenance": list(
            candidate.account_fee_evidence_provenance
        ),
        "research_sample_split": candidate.research_sample_split,
        "volatility_regime": candidate.volatility_regime,
        "net_edge_per_capital_hour_bps": candidate.net_edge_per_capital_hour_bps,
        "risk_adjusted_edge_per_capital_hour_bps": (
            candidate.risk_adjusted_edge_per_capital_hour_bps
        ),
        "hold_time_confidence": candidate.hold_time_confidence,
        "dynamic_min_gross_edge_bps": candidate.dynamic_min_gross_edge_bps,
        "contract_normalization_status": candidate.contract_normalization_status,
        "contract_normalization_reason": candidate.contract_normalization_reason,
    }


def _market_snapshot_payload(long_quote: QuoteSnapshot | None, short_quote: QuoteSnapshot | None) -> dict:
    return {"long_quote": _quote_payload(long_quote), "short_quote": _quote_payload(short_quote)}


def _quote_payload(quote: QuoteSnapshot | None) -> dict | None:
    if quote is None:
        return None
    # Persist only depth provenance/counts, not repeated full ladders in every
    # journal event.  The corresponding snapshot is the raw market evidence.
    return {"venue": quote.venue, "symbol": quote.symbol, "source": quote.source, "bid": quote.bid, "ask": quote.ask, "bid_size": quote.bid_size, "ask_size": quote.ask_size, "bid_depth_levels": len(quote.bid_depth), "ask_depth_levels": len(quote.ask_depth), "observed_at_ms": quote.observed_at_ms, "funding_rate_bps": quote.funding_rate_bps, "funding_timestamp_ms": quote.funding_timestamp_ms, "funding_interval_ms": quote.funding_interval_ms}


def _leg_payload(leg: SpreadPaperLeg, exit_raw_price: float | None = None, exit_price: float | None = None, exit_fee_quote: float = 0.0, gross_quote: float | None = None, exit_execution_source: str = "") -> dict:
    return {
        "venue": leg.venue,
        "side": leg.side,
        "entry_liquidity_role": leg.entry_liquidity_role,
        "exit_liquidity_role": leg.exit_liquidity_role,
        "entry_execution_source": leg.entry_execution_source,
        "exit_execution_source": exit_execution_source,
        "entry_pending": leg.entry_pending,
        "entry_bid": leg.entry_bid,
        "entry_ask": leg.entry_ask,
        "entry_bid_size": leg.entry_bid_size,
        "entry_ask_size": leg.entry_ask_size,
        "entry_observed_at_ms": leg.entry_observed_at_ms,
        "mark_price": leg.mark_price,
        "index_price": leg.index_price,
        "volume_24h_quote": leg.volume_24h_quote,
        "open_interest": leg.open_interest,
        "entry_raw_price": leg.entry_raw_price,
        "entry_price": leg.entry_price,
        "exit_raw_price": exit_raw_price,
        "exit_price": exit_price,
        "qty": leg.qty,
        "requested_qty": leg.requested_qty,
        "residual_qty": leg.residual_qty,
        "order_state": leg.order_state,
        "entry_notional_quote": leg.entry_notional_quote,
        "entry_fee_bps": leg.entry_fee_bps,
        "exit_fee_bps": leg.exit_fee_bps,
        "entry_fee_quote": leg.entry_fee_quote,
        "exit_fee_quote": exit_fee_quote,
        "entry_slippage_quote": leg.entry_slippage_quote,
        "entry_latency_buffer_quote": leg.entry_latency_buffer_quote,
        "gross_quote": gross_quote,
        "funding_rate_bps": leg.funding_rate_bps,
        "funding_timestamp_ms": leg.funding_timestamp_ms,
        "funding_interval_ms": leg.funding_interval_ms,
        "entry_filled_at_ms": leg.entry_filled_at_ms,
        "funding_settlements": [
            _funding_settlement_payload(item)
            for item in leg.funding_settlements
        ],
        "funding_settlement_conflict": leg.funding_settlement_conflict,
    }


def _entry_capacity_source(position: SpreadPaperPosition) -> str:
    return _sources_summary(
        position.long_leg.entry_execution_source,
        position.short_leg.entry_execution_source,
    )


def _sources_summary(*sources: str) -> str:
    normalized = tuple(sorted({str(source or "top_book_only") for source in sources}))
    if normalized == ("l2_vwap",):
        return "l2_vwap"
    if normalized == ("top_book_only",):
        return "top_book_only"
    return "mixed:" + "+".join(normalized)


def _exit_capacity_failure_reason(
    long_execution: _ExecutionEstimate | None,
    short_execution: _ExecutionEstimate | None,
) -> str:
    sources = tuple(
        estimate.source
        for estimate in (long_execution, short_execution)
        if estimate is not None
    )
    if sources and all(source == "top_book_only" for source in sources):
        return "exit_top_book_capacity_insufficient"
    return "exit_l2_capacity_insufficient"


def _position_order_state(position: SpreadPaperPosition) -> str:
    if _position_has_pending_entry(position):
        return PaperOrderState.WORKING.value
    if position.paper_maker_leg:
        return PaperOrderState.UNKNOWN.value
    if position.residual_base_qty > 1e-12:
        return PaperOrderState.PARTIAL.value
    return PaperOrderState.FILLED.value


def _paper_id(candidate: SpreadReversionCandidate, bot_id: str) -> str:
    base = f"spread:{candidate.candidate_id}:{int(candidate.signal_ts_ms or 0)}"
    return base if bot_id == "tt_conservative" else f"{base}:bot:{bot_id}"


def _quote_for(quotes: dict[str, QuoteSnapshot], venue: str, symbol: str) -> QuoteSnapshot | None:
    target = f"{str(venue).lower()}:{str(symbol).upper()}"
    if target in quotes:
        return quotes[target]
    return next((quote for quote in quotes.values() if str(quote.venue).lower() == str(venue).lower() and str(quote.symbol).upper() == str(symbol).upper()), None)


def _entry_raw_price(quote: QuoteSnapshot, side: str, role: str) -> float:
    if side == "long":
        return float((quote.bid if _liquidity_role(role) == "maker" else quote.ask) or 0.0)
    return float((quote.ask if _liquidity_role(role) == "maker" else quote.bid) or 0.0)


def _exit_raw_price(quote: QuoteSnapshot, side: str, role: str) -> float:
    if side == "long":
        return float((quote.ask if _liquidity_role(role) == "maker" else quote.bid) or 0.0)
    return float((quote.bid if _liquidity_role(role) == "maker" else quote.ask) or 0.0)


def _liquidity_role(role: str) -> str:
    return "maker" if str(role or "").lower() == "maker" else "taker"


def _fee_bps(config: SpreadPaperConfig, venue: str, role: str) -> float:
    fees = (
        config.maker_fee_bps_by_venue
        if _liquidity_role(role) == "maker"
        else config.taker_fee_bps_by_venue
    )
    try:
        fee_bps = float(
            fees.get(
                str(venue).lower(),
                config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0),
            )
            or 0.0
        )
    except (TypeError, ValueError):
        return 0.0
    if not isfinite(fee_bps):
        return 0.0
    if _liquidity_role(role) != "maker":
        return fee_bps if fee_bps >= 0.0 else 0.0
    if fee_bps >= 0.0:
        return fee_bps
    if _verified_maker_rebate(config, venue, fee_bps):
        return fee_bps
    # A negative static value cannot improve a paper result. Price the maker
    # leg at its explicit taker fallback instead of inventing zero cost.
    try:
        fallback = float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0))
    except (TypeError, ValueError):
        return 0.0
    return fallback if isfinite(fallback) and fallback >= 0.0 else 0.0


def _candidate_calculation_version(config: SpreadPaperConfig) -> str:
    return (
        "spread_v3_cost_normalized_reversion"
        if str(config.model_epoch or "").startswith("v3_")
        else "spread_v2_signed_reversion"
    )


def _verified_maker_rebate(
    config: SpreadPaperConfig, venue: str, fee_bps: float
) -> bool:
    evidence = config.account_fee_evidence
    if (
        config.allow_verified_maker_rebates is not True
        or evidence is None
        or evidence.integrity_verified is not True
    ):
        return False
    schedule = evidence.schedule_for(venue)
    return bool(
        schedule is not None
        and schedule.maker_fee_bps < 0.0
        and abs(schedule.maker_fee_bps - fee_bps) <= 1e-12
    )


def _executions_have_l2(*executions: _ExecutionEstimate | None) -> bool:
    return all(
        execution is not None and execution.source == "l2_vwap"
        for execution in executions
    )


def _candidate_account_fee_evidence_complete(
    config: SpreadPaperConfig,
    candidate: SpreadReversionCandidate,
) -> bool:
    """Require same-epoch account-fee proof for strict paper admission."""
    if not config.require_account_fee_evidence:
        return True
    evidence = config.account_fee_evidence
    if (
        evidence is None
        or not evidence.complete_for(candidate.long_venue, candidate.short_venue)
        or evidence.integrity_verified is not True
        or candidate.account_fee_evidence_complete is not True
    ):
        return False
    if _v3_fee_identity_binding_required(config) and not (
        evidence.schema_version == FEE_EVIDENCE_SCHEMA_VERSION
        and evidence.integrity_key_id == TRUSTED_FEE_EVIDENCE_KEY_ID
        and evidence.identity_matches(
            config.fee_evidence_account_identity_hashes,
            candidate.long_venue,
            candidate.short_venue,
        )
    ):
        return False
    return (
        int(candidate.account_fee_evidence_observed_at_ms or 0)
        == evidence.observed_at_ms_for(candidate.long_venue, candidate.short_venue)
        and str(candidate.account_fee_evidence_source or "")
        == evidence.source_for(candidate.long_venue, candidate.short_venue)
        and str(candidate.account_fee_evidence_fingerprint or "")
        == evidence.fingerprint_for(candidate.long_venue, candidate.short_venue)
        and candidate.account_fee_evidence_provenance
        == evidence.provenance_for(candidate.long_venue, candidate.short_venue)
    )


def _position_account_fee_evidence_complete(
    position: SpreadPaperPosition,
    config: SpreadPaperConfig,
) -> bool:
    if not config.require_account_fee_evidence:
        return True
    # The entry position retains its timestamp/source even when a later fee
    # refresh changes the service's active schedule.  The schedule is already
    # frozen in each leg, so this proves the admission state rather than
    # restating it using a future account tier.
    base_complete = bool(
        position.account_fee_evidence_complete is True
        and position.account_fee_evidence_observed_at_ms > 0
        and position.account_fee_evidence_source
        and position.account_fee_evidence_fingerprint
        and position.account_fee_evidence_provenance
    )
    if not base_complete:
        return False
    if not _v3_fee_identity_binding_required(config):
        return True
    return _persisted_v3_fee_identity_binding_matches(
        position.account_fee_evidence_provenance,
        config.fee_evidence_account_identity_hashes,
        position.long_venue,
        position.short_venue,
    )


def _v3_fee_identity_binding_required(config: SpreadPaperConfig) -> bool:
    return str(config.model_epoch or "").startswith("v3_")


def _persisted_v3_fee_identity_binding_matches(
    provenance: object,
    expected_by_venue: dict[str, str],
    *venues: str,
) -> bool:
    """Validate the immutable account binding retained in a v3 journal row."""
    if not isinstance(provenance, list):
        return False
    rows_by_venue: dict[str, dict[str, object]] = {}
    for row in provenance:
        if not isinstance(row, dict):
            return False
        venue = str(row.get("venue") or "").strip().lower()
        if not venue or venue in rows_by_venue:
            return False
        rows_by_venue[venue] = row
    for venue in venues:
        venue_key = str(venue or "").strip().lower()
        expected_identity = str(expected_by_venue.get(venue_key) or "").strip().lower()
        row = rows_by_venue.get(venue_key)
        if (
            row is None
            or not _is_sha256_hex(expected_identity)
            or str(row.get("account_identity_hash") or "").lower()
            != expected_identity
            or not _is_sha256_hex(str(row.get("document_sha256") or "").lower())
            or row.get("integrity_verified") is not True
            or str(row.get("integrity_key_id") or "")
            != TRUSTED_FEE_EVIDENCE_KEY_ID
        ):
            return False
    return True


def _is_sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _research_sample_split(
    candidate: SpreadReversionCandidate,
    config: SpreadPaperConfig,
    registered_at_ms: int,
) -> str:
    cutoff = max(int(config.oos_start_ms or 0), 0)
    if cutoff > 0:
        return "out_of_sample" if registered_at_ms >= cutoff else "in_sample"
    declared = str(candidate.research_sample_split or "").strip().lower()
    return declared if declared in {"in_sample", "out_of_sample"} else "in_sample"


def _paper_fee_evidence_complete(
    config: SpreadPaperConfig,
    candidate: SpreadReversionCandidate,
    bot: SpreadPaperBotSpec,
) -> bool:
    """Verify all four simulated legs have an explicit fee source."""
    return _execution_fee_evidence_complete(
        config,
        long_venue=candidate.long_venue,
        short_venue=candidate.short_venue,
        entry_long_role=bot.entry_long_role,
        entry_short_role=bot.entry_short_role,
        exit_long_role=bot.exit_long_role,
        exit_short_role=bot.exit_short_role,
    )


def _execution_fee_evidence_complete(
    config: SpreadPaperConfig,
    *,
    long_venue: str,
    short_venue: str,
    entry_long_role: str,
    entry_short_role: str,
    exit_long_role: str,
    exit_short_role: str,
) -> bool:
    """Verify all four simulated legs have an explicit fee source.

    Maker roles may use an explicit maker fee or the explicitly configured
    taker fallback, exactly as ``_fee_bps`` prices them.  This keeps control
    cohorts observable without letting a missing map turn into zero-cost PnL.
    """
    legs = (
        (long_venue, entry_long_role),
        (short_venue, entry_short_role),
        (long_venue, exit_long_role),
        (short_venue, exit_short_role),
    )
    for venue, role in legs:
        key = str(venue or "").lower()
        source = (
            config.maker_fee_bps_by_venue
            if _liquidity_role(role) == "maker" and key in config.maker_fee_bps_by_venue
            else config.taker_fee_bps_by_venue
        )
        if key not in source:
            return False
        try:
            fee_bps = float(source[key])
        except (TypeError, ValueError):
            return False
        if not isfinite(fee_bps):
            return False
        if fee_bps < 0.0 and (
            _liquidity_role(role) != "maker"
            or not _verified_maker_rebate(config, key, fee_bps)
        ):
            return False
    return True


def _apply_slippage(raw_price: float, *, bps: float, action: str) -> float:
    factor = max(float(bps or 0.0), 0.0) / 10_000.0
    return raw_price * (1.0 + factor if action == "buy" else 1.0 - factor)


def _settlement_funding_quote(position: SpreadPaperPosition, now_ms: int) -> float:
    """Return only actual, position-allocated settlement cash flows.

    The entry funding quote remains an indicative forecast in
    ``accrued_funding_estimate_quote``. Treating it as settled PnL was a
    fictitious accounting entry.
    """
    return sum(
        _settled_leg_funding(leg, position.registered_at_ms, now_ms)
        for leg in (position.long_leg, position.short_leg)
    )


def _settled_leg_funding(leg: SpreadPaperLeg, opened_at_ms: int, now_ms: int) -> float:
    opened_at_ms = int(leg.entry_filled_at_ms or opened_at_ms)
    return sum(
        float(settlement.amount_quote)
        for settlement in leg.funding_settlements
        if (
            opened_at_ms < int(settlement.settlement_timestamp_ms) <= int(now_ms)
            and int(settlement.observed_at_ms) <= int(now_ms)
        )
    )


def _public_settled_funding_for_position(
    position: SpreadPaperPosition,
    *,
    now_ms: int,
    quotes: dict[str, QuoteSnapshot],
    config: SpreadPaperConfig,
) -> list[FundingSettlement]:
    """Build only settlement facts directly evidenced by the public snapshot.

    ``QuoteSnapshot.settled_funding_rate_bps`` is a source-level statement
    that the venue has published the previous interval's settled rate.  It is
    not interchangeable with ``funding_rate_bps`` (the current/quoted rate)
    or a forecast.  The timestamp is the immediately preceding point in the
    quote's *current* known schedule; schedule continuity is still checked by
    the ledger validator before the record can become official.
    """
    records: list[FundingSettlement] = []
    for leg_side, leg in (
        ("long", position.long_leg),
        ("short", position.short_leg),
    ):
        quote = _quote_for(quotes, leg.venue, position.symbol)
        settlement = _public_settled_funding_for_leg(
            position,
            leg_side=leg_side,
            leg=leg,
            quote=quote,
            now_ms=now_ms,
            config=config,
        )
        if settlement is not None:
            records.append(settlement)
    return records


def _public_settled_funding_for_leg(
    position: SpreadPaperPosition,
    *,
    leg_side: str,
    leg: SpreadPaperLeg,
    quote: QuoteSnapshot | None,
    now_ms: int,
    config: SpreadPaperConfig,
) -> FundingSettlement | None:
    if leg.entry_pending or leg.qty <= 0.0 or quote is None:
        return None
    if not _funding_schedule_matches_entry(leg, quote, now_ms):
        return None
    try:
        interval_ms = int(quote.funding_interval_ms or 0)
        next_timestamp_ms = int(quote.funding_timestamp_ms or 0)
        observed_at_ms = int(quote.observed_at_ms or 0)
        settled_rate_bps = float(quote.settled_funding_rate_bps)
        mark_price = float(quote.mark_price or 0.0)
        quantity = float(leg.qty)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        interval_ms <= 0
        or next_timestamp_ms <= interval_ms
        or observed_at_ms <= 0
        or not isfinite(settled_rate_bps)
        or not isfinite(mark_price)
        or mark_price <= 0.0
        or not isfinite(quantity)
        or quantity <= 0.0
    ):
        return None
    settlement_timestamp_ms = next_timestamp_ms - interval_ms
    opened_at_ms = int(leg.entry_filled_at_ms or position.registered_at_ms)
    terminal_due_at_ms = _terminal_due_at_ms(position)
    if (
        settlement_timestamp_ms <= opened_at_ms
        or settlement_timestamp_ms > now_ms
        or terminal_due_at_ms <= 0
        or settlement_timestamp_ms > terminal_due_at_ms
        or observed_at_ms < settlement_timestamp_ms
        or observed_at_ms > now_ms
    ):
        return None
    # A current quote received long after settlement cannot supply the
    # settlement mark.  Keeping this bounded turns restart/refresh gaps into
    # missing evidence instead of a back-filled and potentially fictitious
    # cash flow.
    max_observation_lag_ms = max(int(config.quote_ttl_ms or 0), 1)
    if observed_at_ms - settlement_timestamp_ms > max_observation_lag_ms:
        return None
    side_sign = -1.0 if leg_side == "long" else 1.0
    amount_quote = side_sign * settled_rate_bps * quantity * mark_price / 10_000.0
    if not isfinite(amount_quote):
        return None
    return FundingSettlement(
        paper_id=position.paper_id,
        leg_side=leg_side,
        settlement_timestamp_ms=settlement_timestamp_ms,
        amount_quote=amount_quote,
        observed_at_ms=observed_at_ms,
        source=(
            "public_settled_rate_position_allocation:"
            f"{str(leg.venue or '').lower()}"
        ),
    )


def _funding_settlement_evidence_complete(
    position: SpreadPaperPosition,
    now_ms: int,
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> bool:
    """Require an actual record for every known settlement crossed.

    A forecast can never prove a settled amount. An unknown interval also
    prevents us from proving that another settlement has not happened, so an
    evaluation after the first crossing remains diagnostic-only in that case.
    """
    return all(
        _leg_funding_evidence_complete(
            leg,
            position.registered_at_ms,
            now_ms,
            quote,
        )
        for leg, quote in (
            (position.long_leg, long_quote),
            (position.short_leg, short_quote),
        )
        if leg.qty > 0.0 and not leg.entry_pending
    )


def _leg_funding_evidence_complete(
    leg: SpreadPaperLeg,
    registered_at_ms: int,
    now_ms: int,
    current_quote: QuoteSnapshot | None,
) -> bool:
    if leg.funding_settlement_conflict:
        return False
    # An entry snapshot describes the initial settlement schedule, not a
    # permanent exchange contract.  If the venue changes either the interval
    # *or the next settlement point* while a paper position is open, a missing
    # intermediate ledger row could be a real settlement rather than a zero.
    # Without a schedule-history source, official PnL must fail closed instead
    # of extrapolating the old cadence.
    if not _funding_schedule_matches_entry(leg, current_quote, now_ms):
        return False
    opened_at_ms = int(leg.entry_filled_at_ms or registered_at_ms)
    first_settlement = int(leg.funding_timestamp_ms or 0)
    if first_settlement <= 0:
        return False
    # Before the first known timestamp after the position opened, funding is
    # demonstrably zero.  A maker fill (or a delayed paper admission) can
    # happen after the snapshot's next funding timestamp.  That earlier cash
    # event belongs to a position that existed before this one and must never
    # be required as evidence for this paper position.
    if first_settlement > opened_at_ms:
        first_required_settlement = first_settlement
    else:
        interval = int(leg.funding_interval_ms or 0)
        if interval <= 0:
            return False
        elapsed_intervals = (opened_at_ms - first_settlement) // interval + 1
        first_required_settlement = first_settlement + elapsed_intervals * interval
    if now_ms < first_required_settlement:
        return True
    interval = int(leg.funding_interval_ms or 0)
    if interval <= 0:
        return False
    required_timestamps = range(
        first_required_settlement,
        int(now_ms) + 1,
        interval,
    )
    observed_timestamps = {
        int(settlement.settlement_timestamp_ms)
        for settlement in leg.funding_settlements
        if int(settlement.observed_at_ms) <= int(now_ms)
    }
    return all(timestamp in observed_timestamps for timestamp in required_timestamps)


def _settlement_funding_evidence(
    position: SpreadPaperPosition,
    now_ms: int,
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> str:
    if any(
        leg.funding_settlement_conflict
        for leg in (position.long_leg, position.short_leg)
    ):
        return "conflicting_actual_funding_ledger"
    if not _funding_schedules_unchanged(
        position,
        now_ms,
        long_quote,
        short_quote,
    ):
        return "funding_schedule_changed"
    if _funding_settlement_evidence_complete(position, now_ms, long_quote, short_quote):
        return "actual_position_allocated_funding_ledger"
    if any(
        settlement.settlement_timestamp_ms <= now_ms
        and settlement.observed_at_ms <= now_ms
        for leg in (position.long_leg, position.short_leg)
        for settlement in leg.funding_settlements
    ):
        return "partial_actual_funding_ledger"
    return "missing_actual_funding_ledger"


def _funding_schedules_unchanged(
    position: SpreadPaperPosition,
    now_ms: int,
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> bool:
    return all(
        _funding_schedule_matches_entry(leg, quote, now_ms)
        for leg, quote in (
            (position.long_leg, long_quote),
            (position.short_leg, short_quote),
        )
        if leg.qty > 0.0 and not leg.entry_pending
    )


def _funding_schedule_matches_entry(
    leg: SpreadPaperLeg,
    current_quote: QuoteSnapshot | None,
    now_ms: int,
) -> bool:
    """Return whether the current next-funding time preserves entry cadence.

    ``funding_timestamp_ms`` is the venue's next future settlement time.  An
    unchanged interval alone is insufficient: an exchange can move the
    settlement clock while retaining the same cadence.  Since no schedule
    history is available to paper execution, accepting such a quote would let
    an unknown cash event disappear from official PnL.
    """
    if current_quote is None:
        return False
    interval_ms = int(leg.funding_interval_ms or 0)
    first_settlement_ms = int(leg.funding_timestamp_ms or 0)
    if interval_ms <= 0 or first_settlement_ms <= 0:
        return False
    if int(current_quote.funding_interval_ms or 0) != interval_ms:
        return False
    next_settlement_ms = int(current_quote.funding_timestamp_ms or 0)
    if next_settlement_ms <= int(now_ms):
        return False
    expected_next_ms = first_settlement_ms
    if expected_next_ms <= int(now_ms):
        elapsed_intervals = (int(now_ms) - expected_next_ms) // interval_ms + 1
        expected_next_ms += elapsed_intervals * interval_ms
    return next_settlement_ms == expected_next_ms


def _accrued_funding_quote(position: SpreadPaperPosition, now_ms: int) -> float:
    """Indicative carry only; unknown settlement intervals contribute nothing."""
    return _accrued_leg_funding(position.long_leg, position.registered_at_ms, now_ms) + _accrued_leg_funding(position.short_leg, position.registered_at_ms, now_ms)


def _accrued_leg_funding(leg: SpreadPaperLeg, registered_at_ms: int, now_ms: int) -> float:
    if leg.entry_pending or leg.funding_interval_ms <= 0:
        return 0.0
    opened_at_ms = int(leg.entry_filled_at_ms or registered_at_ms)
    ratio = max(now_ms - opened_at_ms, 0) / leg.funding_interval_ms
    value = leg.entry_notional_quote * leg.funding_rate_bps / 10_000.0 * ratio
    return -value if leg.side == "long" else value


def _residual_gross_quote(
    position: SpreadPaperPosition,
    long_gross: float,
    short_gross: float,
) -> float:
    """Return PnL attributable to any remaining directional base exposure."""
    matched_qty = min(position.long_leg.qty, position.short_leg.qty)
    if matched_qty <= 0.0:
        return long_gross + short_gross
    long_per_unit = 0.0 if position.long_leg.qty <= 0.0 else long_gross / position.long_leg.qty
    short_per_unit = 0.0 if position.short_leg.qty <= 0.0 else short_gross / position.short_leg.qty
    matched_gross = matched_qty * (long_per_unit + short_per_unit)
    return long_gross + short_gross - matched_gross


def _hedge_delay_quote(position: SpreadPaperPosition) -> float:
    """Isolate price impact caused by waiting after a maker-fill observation."""
    if position.maker_fill_observed_at_ms <= 0 or not position.paper_maker_leg:
        return 0.0
    hedge_leg = position.short_leg if position.paper_maker_leg == "long" else position.long_leg
    if hedge_leg.entry_filled_at_ms <= position.maker_fill_observed_at_ms:
        return 0.0
    initial_raw = _initial_entry_raw_price(position, hedge_leg)
    if initial_raw is None or hedge_leg.entry_raw_price is None:
        return 0.0
    if hedge_leg.side == "long":
        return hedge_leg.qty * (initial_raw - hedge_leg.entry_raw_price)
    return hedge_leg.qty * (hedge_leg.entry_raw_price - initial_raw)


def _initial_entry_raw_price(position: SpreadPaperPosition, leg: SpreadPaperLeg) -> float | None:
    entry_side = "long_quote" if leg.side == "long" else "short_quote"
    snapshot = position.entry_market_snapshot.get(entry_side)
    if not isinstance(snapshot, dict):
        return None
    if leg.side == "long":
        raw = snapshot.get("bid") if leg.entry_liquidity_role == "maker" else snapshot.get("ask")
    else:
        raw = snapshot.get("ask") if leg.entry_liquidity_role == "maker" else snapshot.get("bid")
    value = _optional_float(raw)
    return value if value is not None and value > 0.0 else None


def _position_signed_spread_bps(
    position: SpreadPaperPosition,
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> float | None:
    if long_quote is None or short_quote is None:
        return None
    long_mid = (float(long_quote.bid or 0.0) + float(long_quote.ask or 0.0)) / 2.0
    short_mid = (float(short_quote.bid or 0.0) + float(short_quote.ask or 0.0)) / 2.0
    reference = (long_mid + short_mid) / 2.0
    if reference <= 0.0:
        return None
    canonical_a = str(
        position.candidate_snapshot.get("canonical_venue_a", "") or min(position.long_venue, position.short_venue)
    ).lower()
    canonical_b = str(
        position.candidate_snapshot.get("canonical_venue_b", "") or max(position.long_venue, position.short_venue)
    ).lower()
    mids = {
        str(position.long_venue).lower(): long_mid,
        str(position.short_venue).lower(): short_mid,
    }
    if canonical_a not in mids or canonical_b not in mids:
        return None
    return (mids[canonical_a] - mids[canonical_b]) / reference * 10_000.0


def _position_exit_z_score(position: SpreadPaperPosition, spread: float | None) -> float | None:
    if spread is None:
        return None
    std = float(position.candidate_snapshot.get("rolling_std_bps", 0.0) or 0.0)
    return None if std <= 0.0 else (spread - float(position.candidate_snapshot.get("rolling_mean_bps", 0.0) or 0.0)) / std


def _excluded_symbols(config: SpreadPaperConfig) -> set[str]:
    return {str(symbol).upper() for symbol in config.excluded_symbols if str(symbol).strip()}


def _allowed_labels(config: SpreadPaperConfig) -> set[str]:
    return {str(label) for label in config.allowed_opportunity_labels if str(label).strip()}


def _fill_assumption(bot: SpreadPaperBotSpec) -> str:
    # The source may carry a coherent L2 ladder, but BBO-only snapshots remain
    # a supported conservative fallback.  The position/journal records which
    # source was actually used for each leg.
    return "taker_l2_vwap_or_top_book_size_only" if not bot.maker_leg else "maker_cross_control_not_official"


def _episode_key(candidate: SpreadReversionCandidate, bot_id: str) -> tuple[str, str, str, str]:
    return candidate.symbol.upper(), candidate.canonical_venue_a or min(candidate.long_venue, candidate.short_venue), candidate.canonical_venue_b or max(candidate.long_venue, candidate.short_venue), bot_id


def _episode_key_from_position(position: SpreadPaperPosition) -> tuple[str, str, str, str]:
    return position.symbol.upper(), min(position.long_venue, position.short_venue), max(position.long_venue, position.short_venue), position.paper_bot_id


def _optional_float(value: object) -> float | None:
    try:
        numeric = None if value is None else float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric is None or isfinite(numeric) else None


def _funding_settlement_payload(settlement: FundingSettlement) -> dict:
    return {
        "paper_id": settlement.paper_id,
        "leg_side": settlement.leg_side,
        "settlement_timestamp_ms": settlement.settlement_timestamp_ms,
        "amount_quote": settlement.amount_quote,
        "observed_at_ms": settlement.observed_at_ms,
        "source": settlement.source,
    }


def _settlements_from_payload(
    value: object,
) -> tuple[tuple[FundingSettlement, ...], bool]:
    """Restore idempotent settlement facts without double-crediting a journal.

    A leg has exactly one cash settlement for a timestamp.  Journal replay can
    legitimately duplicate an identical event, while conflicting amounts are
    an irreconcilable evidence failure.  Return the deduplicated facts and a
    conflict marker so official PnL remains fail-closed without inflating the
    diagnostic PnL by summing a duplicate record.
    """
    if not isinstance(value, list):
        return (), False
    settlements_by_timestamp: dict[int, FundingSettlement] = {}
    conflict = False
    for raw in value:
        if not isinstance(raw, dict):
            continue
        try:
            settlement = FundingSettlement(
                paper_id=str(raw.get("paper_id", "") or ""),
                leg_side=str(raw.get("leg_side", "") or ""),
                settlement_timestamp_ms=int(raw.get("settlement_timestamp_ms", 0) or 0),
                amount_quote=float(raw.get("amount_quote", 0.0) or 0.0),
                observed_at_ms=int(raw.get("observed_at_ms", 0) or 0),
                source=str(raw.get("source", "") or ""),
            )
        except (TypeError, ValueError):
            continue
        if (
            settlement.paper_id
            and settlement.leg_side in {"long", "short"}
            and settlement.settlement_timestamp_ms > 0
            and settlement.observed_at_ms >= settlement.settlement_timestamp_ms
            and settlement.source
            and isfinite(float(settlement.amount_quote))
        ):
            timestamp = int(settlement.settlement_timestamp_ms)
            existing = settlements_by_timestamp.get(timestamp)
            if existing is None:
                settlements_by_timestamp[timestamp] = settlement
            elif float(existing.amount_quote) != float(settlement.amount_quote):
                conflict = True
            elif int(settlement.observed_at_ms) < int(existing.observed_at_ms):
                # Same cash fact, earlier observation is the conservative
                # evidence timestamp for a later replay of the same event.
                settlements_by_timestamp[timestamp] = settlement
    return (
        tuple(
            settlements_by_timestamp[timestamp]
            for timestamp in sorted(settlements_by_timestamp)
        ),
        conflict,
    )


def _valid_funding_settlement(
    position: SpreadPaperPosition,
    settlement: FundingSettlement,
) -> bool:
    if settlement.leg_side not in {"long", "short"}:
        return False
    if settlement.paper_id != position.paper_id or not str(settlement.source or ""):
        return False
    try:
        amount_quote = float(settlement.amount_quote)
    except (TypeError, ValueError):
        return False
    if not isfinite(amount_quote):
        return False
    if (
        int(settlement.settlement_timestamp_ms or 0) <= 0
        or int(settlement.observed_at_ms or 0) < int(settlement.settlement_timestamp_ms)
    ):
        return False
    leg = position.long_leg if settlement.leg_side == "long" else position.short_leg
    opened_at_ms = int(leg.entry_filled_at_ms or position.registered_at_ms)
    if leg.entry_pending or leg.qty <= 0.0 or settlement.settlement_timestamp_ms <= opened_at_ms:
        return False
    first = int(leg.funding_timestamp_ms or 0)
    interval = int(leg.funding_interval_ms or 0)
    if first <= 0 or interval <= 0 or settlement.settlement_timestamp_ms < first:
        return False
    if (settlement.settlement_timestamp_ms - first) % interval != 0:
        return False
    # The terminal horizon is the simulator's deterministic close boundary.
    # A delayed sidecar refresh may receive a later account-ledger row before
    # it evaluates that horizon; accepting it would credit funding earned only
    # after the simulated position had already closed.  The position can only
    # have held through a settlement at or before this boundary.
    terminal_due_at_ms = _terminal_due_at_ms(position)
    if terminal_due_at_ms <= 0 or settlement.settlement_timestamp_ms > terminal_due_at_ms:
        return False
    return True


def _terminal_due_at_ms(position: SpreadPaperPosition) -> int:
    """Return the one strict terminal close boundary, or zero if malformed."""
    terminal_due: list[int] = []
    for horizon in position.due_horizons:
        if not isinstance(horizon, dict) or horizon.get("terminal") is not True:
            continue
        try:
            due_at_ms = int(horizon.get("due_at_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return 0
        if due_at_ms <= 0:
            return 0
        terminal_due.append(due_at_ms)
    return terminal_due[0] if len(terminal_due) == 1 else 0


def _with_funding_settlement(
    leg: SpreadPaperLeg,
    settlement: FundingSettlement,
) -> SpreadPaperLeg:
    records = {
        int(item.settlement_timestamp_ms): item
        for item in leg.funding_settlements
    }
    timestamp = int(settlement.settlement_timestamp_ms)
    existing = records.get(timestamp)
    if existing is not None:
        # Funding is an immutable cash fact. A revised amount for the same
        # position/leg/settlement cannot be silently accepted into official
        # paper PnL: retain the first observation and make the ledger fail
        # closed for this episode.
        if float(existing.amount_quote) != float(settlement.amount_quote):
            return replace(leg, funding_settlement_conflict=True)
        return leg
    records[timestamp] = settlement
    return replace(
        leg,
        funding_settlements=tuple(
            records[timestamp] for timestamp in sorted(records)
        ),
    )
