"""Same-symbol funding pair construction with V1 economics compatibility."""

from __future__ import annotations

from math import isfinite

from lightfee.config.schema import StrategyConfig
from lightfee.engine.entry_local_l2 import make_candidate_pair_id
from lightfee.sidecar.snapshot import (
    CandidateInput,
    QuoteSnapshot,
    funding_rate_evidence_reason,
)
from lightfee.strategy.economics import FundingForecast, build_edge_breakdown
from lightfee.strategy.candidate_identity import (
    candidate_revision_id as build_candidate_revision_id,
    opportunity_lease_id as build_opportunity_lease_id,
)
from lightfee.strategy.fee_evidence import FeeEvidenceBook
from lightfee.strategy.funding_canary_policy import canary_notional_cap_for_tier
from lightfee.strategy.risk_allocator import StrategyRiskAllocator


_INTERVAL_ALIGNED_THRESHOLD_MS = 60_000


def _record_rejection(counts: dict[str, int] | None, reason: str) -> None:
    if counts is not None:
        counts[reason] = int(counts.get(reason, 0)) + 1


class FundingCandidateService:
    """Reusable funding-shortlist builder owned by the sidecar service.

    The public ``build_same_symbol_pairs`` function remains a compatibility
    boundary for tests and tooling.  A running sidecar instead keeps the
    typed strategy, venue-fee evidence and risk allocator together, so a
    refresh does not rebuild configuration-derived maps.
    """

    def __init__(
        self,
        *,
        strategy: StrategyConfig,
        venue_fee_bps: dict[str, float],
        venue_maker_fee_bps: dict[str, float],
        venue_notional_caps: dict[str, float],
        passive_execution_enabled: bool,
        fee_evidence: FeeEvidenceBook | None = None,
        expected_fee_identity_hashes: dict[str, str] | None = None,
    ) -> None:
        self._strategy = strategy
        self._fee_by_venue = _normalise_venue_bps(venue_fee_bps)
        self._fee_evidence = fee_evidence
        self._expected_fee_identity_hashes = expected_fee_identity_hashes
        self._maker_fee_by_venue = _normalise_venue_bps(
            venue_maker_fee_bps,
            allow_negative=bool(
                fee_evidence is not None and fee_evidence.integrity_verified is True
            ),
        )
        self._caps_by_venue = _normalise_venue_bps(venue_notional_caps)
        self._passive_execution_enabled = passive_execution_enabled is True
        self._allocator = StrategyRiskAllocator()

    def build(
        self,
        quotes: dict[str, QuoteSnapshot],
        symbols: list[str],
        *,
        observed_at_ms: int = 0,
        diagnostics: dict[str, object] | None = None,
        max_candidates: int | None = None,
    ) -> list[CandidateInput]:
        return _build_same_symbol_pairs(
            quotes,
            symbols,
            config=self._strategy,
            fee_by_venue=self._fee_by_venue,
            maker_fee_by_venue=self._maker_fee_by_venue,
            caps_by_venue=self._caps_by_venue,
            allocator=self._allocator,
            passive_execution_enabled=self._passive_execution_enabled,
            fee_evidence=self._fee_evidence,
            expected_fee_identity_hashes=self._expected_fee_identity_hashes,
            observed_at_ms=observed_at_ms,
            diagnostics=diagnostics,
            max_candidates=max_candidates,
        )


def build_same_symbol_pairs(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
    *,
    strategy: StrategyConfig | None = None,
    venue_fee_bps: dict[str, float] | None = None,
    venue_maker_fee_bps: dict[str, float] | None = None,
    venue_notional_caps: dict[str, float] | None = None,
    passive_execution_enabled: bool = False,
    fee_evidence: FeeEvidenceBook | None = None,
    expected_fee_identity_hashes: dict[str, str] | None = None,
    observed_at_ms: int = 0,
    diagnostics: dict[str, object] | None = None,
    max_candidates: int | None = None,
) -> list[CandidateInput]:
    """Build directed funding pairs using a common base quantity.

    This is the conservative sidecar shortlist only.  The live entry path must
    revalidate the same contract against current L2 immediately before it
    submits the first leg.  Direction consistency is diagnostic evidence, not
    an alpha gate: executable cross is part of the economics formula.
    """

    return _build_same_symbol_pairs(
        quotes,
        symbols,
        config=strategy if strategy is not None else StrategyConfig(),
        fee_by_venue=_normalise_venue_bps(venue_fee_bps or {}),
        maker_fee_by_venue=_normalise_venue_bps(
            venue_maker_fee_bps or {},
            allow_negative=bool(
                fee_evidence is not None and fee_evidence.integrity_verified is True
            ),
        ),
        caps_by_venue=_normalise_venue_bps(venue_notional_caps or {}),
        allocator=StrategyRiskAllocator(),
        passive_execution_enabled=passive_execution_enabled is True,
        fee_evidence=fee_evidence,
        expected_fee_identity_hashes=expected_fee_identity_hashes,
        observed_at_ms=observed_at_ms,
        diagnostics=diagnostics,
        max_candidates=max_candidates,
    )


def _pair_fee_assurance(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    *,
    config: StrategyConfig,
    fee_by_venue: dict[str, float],
    fee_evidence: FeeEvidenceBook | None,
    expected_fee_identity_hashes: dict[str, str] | None,
) -> tuple[str, bool, bool, bool, str]:
    """Resolve one fee contract shared by frontier and exact evaluation.

    The conservative canary tier deliberately accepts a symbol that was not
    covered by the latest private-account schedule, provided both configured
    taker fees are explicit.  It remains conservative because the caller adds
    the per-unverified-leg buffer, removes that leg's maker discount and
    applies the smaller notional cap.
    """

    taker_complete = _taker_fee_evidence_complete(
        fee_by_venue,
        long_q.venue,
        short_q.venue,
    )
    long_authoritative = bool(
        fee_evidence is not None
        and fee_evidence.account_authoritative_for(
            expected_fee_identity_hashes or {},
            long_q.venue,
            symbol=long_q.symbol,
        )
    )
    short_authoritative = bool(
        fee_evidence is not None
        and fee_evidence.account_authoritative_for(
            expected_fee_identity_hashes or {},
            short_q.venue,
            symbol=short_q.symbol,
        )
    )
    if long_authoritative and short_authoritative:
        return "account", taker_complete, True, True, ""

    unavailable_reason = "account_fee_evidence_unavailable"
    if (
        fee_evidence is not None
        and fee_evidence.complete_for(long_q.venue, short_q.venue)
        and expected_fee_identity_hashes is not None
    ):
        schedules = (
            fee_evidence.schedule_for(long_q.venue),
            fee_evidence.schedule_for(short_q.venue),
        )
        symbol = str(long_q.symbol).strip().upper()
        if any(
            schedule is not None
            and schedule.covered_symbols
            and symbol not in schedule.covered_symbols
            for schedule in schedules
        ):
            unavailable_reason = "account_fee_symbol_coverage_missing"
        else:
            unavailable_reason = "account_fee_account_identity_mismatch"

    if (
        config.funding_canary_require_account_fee_evidence is not True
        and taker_complete
    ):
        return (
            "conservative",
            taker_complete,
            long_authoritative,
            short_authoritative,
            unavailable_reason,
        )
    return (
        "unavailable",
        taker_complete,
        long_authoritative,
        short_authoritative,
        unavailable_reason,
    )


def _optimistic_candidate_rank_upper(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    *,
    config: StrategyConfig,
    fee_by_venue: dict[str, float],
    maker_fee_by_venue: dict[str, float],
    caps_by_venue: dict[str, float],
    allocator: StrategyRiskAllocator,
    passive_execution_enabled: bool,
    fee_evidence: FeeEvidenceBook | None,
    expected_fee_identity_hashes: dict[str, str] | None,
) -> float:
    """Cheap mathematical upper bound for the exact worst-case rank.

    The bound deliberately computes every cheap, already-known signed term:
    first-settlement funding, executable cross, a lower bound on route fees,
    and fixed risk buffers.  Only depth-dependent slippage remains at its
    mathematical lower bound of zero.  This keeps branch-and-bound both safe
    and useful on the full seven-venue universe.
    """

    def _finite(value: object, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return parsed if isfinite(parsed) else default

    long_ask = _finite(long_q.ask)
    short_bid = _finite(short_q.bid)
    reference_mid = (long_ask + short_bid) / 2.0
    if long_ask <= 0.0 or short_bid <= 0.0 or reference_mid <= 0.0:
        return float("inf")
    raw_cross = ((short_bid - long_ask) / reference_mid) * 10_000.0
    configured_uncertainty = max(
        _finite(config.funding_forecast_uncertainty_haircut_bps),
        0.0,
    )

    def _enhanced_live_component(
        quote: QuoteSnapshot,
        *,
        long_leg: bool,
    ) -> float:
        quoted = _finite(quote.funding_rate_bps)
        predicted = _finite(
            quote.predicted_funding_rate_bps,
            quoted,
        )
        uncertainty = max(
            configured_uncertainty,
            _finite_nonnegative(quote.funding_forecast_uncertainty_bps),
        )
        # Ranking uses the conservative signed forecast, not the magnitude of
        # each leg independently. A long pays the upper forecast bound while a
        # short receives the lower bound. This is the exact enhanced-live
        # funding component for an admissible seed and therefore remains a safe
        # upper bound without making equal positive rates look like 2x alpha.
        return -(predicted + uncertainty) if long_leg else predicted - uncertainty

    long_ts = max(int(_finite(long_q.funding_timestamp_ms)), 0)
    short_ts = max(int(_finite(short_q.funding_timestamp_ms)), 0)
    interval_aligned = bool(
        long_ts > 0
        and short_ts > 0
        and abs(long_ts - short_ts) <= _INTERVAL_ALIGNED_THRESHOLD_MS
    )
    economics_mode = str(config.funding_economics_mode or "v1_exact").lower()
    if economics_mode == "enhanced_live":
        # `_seed_is_potentially_live` admits only calibrated, stable forecasts
        # to exact evaluation. Non-admissible rows are sorted behind that set
        # and never rely on this signed bound for publication.
        long_component_upper = _enhanced_live_component(long_q, long_leg=True)
        short_component_upper = _enhanced_live_component(short_q, long_leg=False)
    else:
        # V1/shadow uses the signed quoted funding contract exactly.
        long_component_upper = -_finite(long_q.funding_rate_bps)
        short_component_upper = _finite(short_q.funding_rate_bps)
    if interval_aligned:
        first_stage_funding_upper = (
            long_component_upper + short_component_upper
        )
    elif long_ts > 0 and short_ts > 0 and long_ts <= short_ts:
        first_stage_funding_upper = long_component_upper
    elif long_ts > 0 and short_ts > 0:
        first_stage_funding_upper = short_component_upper
    else:
        first_stage_funding_upper = 0.0

    long_venue = str(long_q.venue).lower()
    short_venue = str(short_q.venue).lower()
    long_fee = _finite(fee_by_venue.get(long_venue, 0.0))
    short_fee = _finite(fee_by_venue.get(short_venue, 0.0))
    long_maker_fee = _finite(
        maker_fee_by_venue.get(long_venue, long_fee),
        long_fee,
    )
    short_maker_fee = _finite(
        maker_fee_by_venue.get(short_venue, short_fee),
        short_fee,
    )
    (
        assurance_tier,
        _taker_fee_complete,
        long_authoritative,
        short_authoritative,
        _fee_unavailable_reason,
    ) = _pair_fee_assurance(
        long_q,
        short_q,
        config=config,
        fee_by_venue=fee_by_venue,
        fee_evidence=fee_evidence,
        expected_fee_identity_hashes=expected_fee_identity_hashes,
    )
    if (
        config.funding_canary_enabled is True
        and assurance_tier == "conservative"
    ):
        conservative_buffer = max(
            _finite(config.funding_canary_conservative_fee_buffer_bps),
            0.0,
        )
        if not long_authoritative:
            long_fee += conservative_buffer
            long_maker_fee = long_fee
        if not short_authoritative:
            short_fee += conservative_buffer
            short_maker_fee = short_fee
    canary_cap = (
        canary_notional_cap_for_tier(assurance_tier, config)
        if config.funding_canary_enabled is True
        and assurance_tier != "unavailable"
        else 0.0
    )
    configured_cap = min(
        max(_finite(config.entry_notional_cap_quote), 0.0),
        max(_finite(config.live_entry_notional_cap_quote), 0.0),
    )
    if canary_cap > 0.0:
        configured_cap = min(configured_cap, canary_cap)
    conservative_quantity = (
        configured_cap / reference_mid if configured_cap > 0.0 else 0.0
    )
    long_depth = max(_finite(long_q.ask_size), 0.0) * max(
        _finite(config.max_top_book_usage_ratio),
        0.0,
    )
    short_depth = max(_finite(short_q.bid_size), 0.0) * max(
        _finite(config.max_top_book_usage_ratio),
        0.0,
    )
    long_max_quantity = long_depth if long_depth > 0.0 else conservative_quantity
    short_max_quantity = short_depth if short_depth > 0.0 else conservative_quantity
    if canary_cap > 0.0:
        canary_quantity = canary_cap / max(long_ask, short_bid)
        long_max_quantity = min(long_max_quantity, canary_quantity)
        short_max_quantity = min(short_max_quantity, canary_quantity)
    venue_cap = _minimum_positive(
        caps_by_venue.get(long_venue, 0.0),
        caps_by_venue.get(short_venue, 0.0),
        config.max_single_venue_exposure_quote,
    )
    global_reference_cap = _minimum_positive(
        (
            _finite(config.funding_max_global_gross_exposure_quote) / 2.0
            if _finite(config.funding_max_global_gross_exposure_quote) > 0.0
            else 0.0
        ),
        config.funding_max_correlation_group_exposure_quote,
        (
            _finite(config.funding_expected_shortfall_budget_quote)
            * 10_000.0
            / _finite(config.funding_expected_shortfall_bps)
            if _finite(config.funding_expected_shortfall_bps) > 0.0
            else 0.0
        ),
    )
    allocation = allocator.allocate(
        long_entry_price=long_ask,
        short_entry_price=short_bid,
        long_max_quantity=long_max_quantity,
        short_max_quantity=short_max_quantity,
        configured_notional_cap_quote=configured_cap,
        venue_notional_cap_quote=venue_cap,
        symbol_risk_budget_quote=_finite(config.max_symbol_exposure_quote),
        venue_pair_risk_budget_quote=_finite(
            config.funding_max_venue_pair_exposure_quote
        ),
        global_risk_budget_quote=global_reference_cap,
        fallback_notional_quote=_finite(
            config.funding_missing_margin_fallback_notional_quote
        ),
        health_buffer_ratio=_finite(config.funding_risk_health_buffer_ratio),
    )
    quantity = max(float(allocation.base_quantity or 0.0), 0.0)
    long_entry_slippage = _heuristic_slippage_bps(
        long_q,
        quantity,
        taking_ask=True,
    )
    short_entry_slippage = _heuristic_slippage_bps(
        short_q,
        quantity,
        taking_ask=False,
    )
    long_exit_slippage = _heuristic_slippage_bps(
        long_q,
        quantity,
        taking_ask=False,
    )
    short_exit_slippage = _heuristic_slippage_bps(
        short_q,
        quantity,
        taking_ask=True,
    )
    entry_maker_leg = _select_maker_leg(
        long_entry_slippage,
        short_entry_slippage,
    )
    exit_maker_leg = _select_maker_leg(
        long_exit_slippage,
        short_exit_slippage,
    )
    fee_floor = _effective_fee_bps(
        long_taker_fee_bps=long_fee,
        long_maker_fee_bps=long_maker_fee,
        short_taker_fee_bps=short_fee,
        short_maker_fee_bps=short_maker_fee,
        maker_leg=entry_maker_leg,
        passive_execution_enabled=passive_execution_enabled,
    ) + _effective_fee_bps(
        long_taker_fee_bps=long_fee,
        long_maker_fee_bps=long_maker_fee,
        short_taker_fee_bps=short_fee,
        short_maker_fee_bps=short_maker_fee,
        maker_leg=exit_maker_leg,
        passive_execution_enabled=passive_execution_enabled,
    )
    slippage_floor = _effective_slippage_bps(
        long_entry_slippage,
        short_entry_slippage,
        entry_maker_leg,
        passive_execution_enabled,
    ) + _effective_slippage_bps(
        long_exit_slippage,
        short_exit_slippage,
        exit_maker_leg,
        passive_execution_enabled,
    )

    venue_haircut = max(
        _finite(
            config.funding_venue_risk_haircut_bps_by_venue.get(long_venue, 0.0)
        ),
        0.0,
    ) + max(
        _finite(
            config.funding_venue_risk_haircut_bps_by_venue.get(short_venue, 0.0)
        ),
        0.0,
    )
    fixed_costs = (
        max(_finite(config.entry_exit_reserve_bps), 0.0)
        + max(_finite(config.capital_buffer_bps), 0.0)
        + max(_finite(config.execution_buffer_bps), 0.0)
        + venue_haircut
    )
    upper = (
        raw_cross
        + first_stage_funding_upper
        - fee_floor
        - slippage_floor
        - fixed_costs
    )
    if passive_execution_enabled:
        upper += _pair_spread_bps(
            long_q,
            short_q,
            reference_mid,
            entry_maker_leg,
        )
    return upper


def _seed_is_potentially_live(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    *,
    config: StrategyConfig,
    fee_by_venue: dict[str, float],
    fee_evidence: FeeEvidenceBook | None,
    expected_fee_identity_hashes: dict[str, str] | None,
    observed_at_ms: int,
) -> bool:
    """Cheap fail-closed classification before the exact Top-K contract.

    A bounded frontier must not be filled by rows already known to be
    structurally untradeable while a lower-edge executable row sits just
    outside the cap. Keep this predicate limited to deterministic blockers
    whose inputs are already available without allocation or slippage work.
    """

    if _funding_contract_block_reasons(long_q, short_q):
        return False
    funding_decision_at_ms = max(int(observed_at_ms or 0), 0)
    for quote in (long_q, short_q):
        if funding_rate_evidence_reason(
            venue=str(getattr(quote, "venue", "") or ""),
            symbol=str(getattr(quote, "symbol", "") or ""),
            rate_bps=getattr(quote, "funding_rate_bps", None),
            funding_timestamp_ms=getattr(quote, "funding_timestamp_ms", 0),
            observed_at_ms=getattr(quote, "funding_rate_observed_at_ms", 0),
            event_at_ms=getattr(quote, "funding_rate_event_at_ms", 0),
            received_at_ms=getattr(quote, "funding_rate_received_at_ms", 0),
            source=getattr(quote, "funding_rate_source", ""),
            sample_id=getattr(quote, "funding_rate_sample_id", ""),
            decision_at_ms=funding_decision_at_ms,
        ):
            return False
    if str(config.funding_economics_mode or "v1_exact").lower() == "enhanced_live":
        if not (
            long_q.funding_forecast_distribution_stable is True
            and short_q.funding_forecast_distribution_stable is True
        ):
            return False
        # Use the same decision wall clock as the exact constructor.  Using
        # the quote timestamp here could classify a forecast as live while
        # the exact pass rejects the same row for insufficient shadow age.
        now_ms = max(int(observed_at_ms or 0), 0)
        uncertainty = max(
            float(config.funding_forecast_uncertainty_haircut_bps or 0.0),
            0.0,
        )
        long_forecast = FundingForecast.from_quote(
            venue=long_q.venue,
            symbol=long_q.symbol,
            quoted_rate_bps=long_q.funding_rate_bps,
            predicted_settled_rate_bps=long_q.predicted_funding_rate_bps,
            next_funding_timestamp_ms=int(long_q.funding_timestamp_ms or 0),
            funding_interval_ms=long_q.funding_interval_ms,
            observed_at_ms=now_ms,
            uncertainty_haircut_bps=max(
                uncertainty,
                float(long_q.funding_forecast_uncertainty_bps or 0.0),
            ),
            sample_count=long_q.funding_forecast_sample_count,
            min_samples=config.funding_forecast_min_samples,
            source=long_q.funding_forecast_source,
        )
        short_forecast = FundingForecast.from_quote(
            venue=short_q.venue,
            symbol=short_q.symbol,
            quoted_rate_bps=short_q.funding_rate_bps,
            predicted_settled_rate_bps=short_q.predicted_funding_rate_bps,
            next_funding_timestamp_ms=int(short_q.funding_timestamp_ms or 0),
            funding_interval_ms=short_q.funding_interval_ms,
            observed_at_ms=now_ms,
            uncertainty_haircut_bps=max(
                uncertainty,
                float(short_q.funding_forecast_uncertainty_bps or 0.0),
            ),
            sample_count=short_q.funding_forecast_sample_count,
            min_samples=config.funding_forecast_min_samples,
            source=short_q.funding_forecast_source,
        )
        required_shadow_age_ms = (
            max(int(config.funding_forecast_shadow_min_days or 0), 0)
            * 24
            * 60
            * 60
            * 1_000
        )
        if not (
            long_forecast.confidence > 0.0
            and short_forecast.confidence > 0.0
            and min(
                _forecast_shadow_age_ms(long_q, now_ms),
                _forecast_shadow_age_ms(short_q, now_ms),
            )
            >= required_shadow_age_ms
        ):
            return False
    (
        assurance_tier,
        taker_fee_complete,
        long_authoritative,
        short_authoritative,
        fee_unavailable_reason,
    ) = _pair_fee_assurance(
        long_q,
        short_q,
        config=config,
        fee_by_venue=fee_by_venue,
        fee_evidence=fee_evidence,
        expected_fee_identity_hashes=expected_fee_identity_hashes,
    )
    if not taker_fee_complete:
        return False
    if (
        bool(config.funding_canary_require_account_fee_evidence)
        and assurance_tier == "unavailable"
        and fee_unavailable_reason != "account_fee_evidence_unavailable"
    ):
        return False
    if config.funding_canary_enabled is True and assurance_tier == "unavailable":
        return False
    if config.funding_canary_enabled is True:
        canary_cap = (
            canary_notional_cap_for_tier(assurance_tier, config)
            if assurance_tier != "unavailable"
            else 0.0
        )
        if canary_cap > 0.0:
            canary_quantity = canary_cap / max(
                float(long_q.ask),
                float(short_q.bid),
            )
            if any(
                canary_quantity + 1e-12 < float(quote.min_quantity_base or 0.0)
                or canary_quantity * price + 1e-9
                < float(quote.min_notional_quote or 0.0)
                for quote, price in (
                    (long_q, float(long_q.ask)),
                    (short_q, float(short_q.bid)),
                )
            ):
                return False
    return True


def _build_same_symbol_pairs(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
    *,
    config: StrategyConfig,
    fee_by_venue: dict[str, float],
    maker_fee_by_venue: dict[str, float],
    caps_by_venue: dict[str, float],
    allocator: StrategyRiskAllocator,
    passive_execution_enabled: bool,
    fee_evidence: FeeEvidenceBook | None,
    expected_fee_identity_hashes: dict[str, str] | None,
    observed_at_ms: int,
    diagnostics: dict[str, object] | None,
    max_candidates: int | None,
) -> list[CandidateInput]:
    candidates: list[CandidateInput] = []
    rejection_counts: dict[str, int] = {}
    directional_pair_count = 0
    future_input_quote_count = sum(
        1
        for quote in quotes.values()
        if observed_at_ms > 0 and _quote_observed_after(quote, observed_at_ms)
    )

    quotes_by_symbol: dict[str, list[QuoteSnapshot]] = {}
    seen_quote_identities: set[tuple[str, str]] = set()
    duplicate_quote_identities: set[tuple[str, str]] = set()
    seed_pairs: list[tuple[bool, float, QuoteSnapshot, QuoteSnapshot]] = []
    for quote in quotes.values():
        identity = (
            str(quote.venue).strip().lower(),
            str(quote.symbol).strip().upper(),
        )
        if identity in seen_quote_identities:
            duplicate_quote_identities.add(identity)
            continue
        seen_quote_identities.add(identity)
        quotes_by_symbol.setdefault(identity[1], []).append(quote)
    if duplicate_quote_identities:
        quotes_by_symbol = {
            symbol: [
                quote
                for quote in venue_quotes
                if (
                    str(quote.venue).strip().lower(),
                    str(quote.symbol).strip().upper(),
                )
                not in duplicate_quote_identities
            ]
            for symbol, venue_quotes in quotes_by_symbol.items()
        }

    canonical_symbols = list(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        )
    )
    for symbol in canonical_symbols:
        venue_quotes = quotes_by_symbol.get(str(symbol).upper(), [])
        if len(venue_quotes) < 2:
            continue
        economics_mode = str(config.funding_economics_mode or "v1_exact").lower()
        for long_q in venue_quotes:
            for short_q in venue_quotes:
                if long_q is short_q or str(long_q.venue).strip().lower() == str(
                    short_q.venue
                ).strip().lower():
                    continue
                # Validate both records before any arithmetic or ordering.
                # A malformed funding scalar must reject only this directed
                # pair, never abort publication of unrelated venue/symbols.
                if not _valid_trade_quote(long_q) or not _valid_trade_quote(short_q):
                    directional_pair_count += 1
                    _record_rejection(rejection_counts, "invalid_trade_quote")
                    continue
                # V1 and the shadow comparison must preserve the legacy
                # quoted-rate discovery universe exactly.  Once calibrated
                # enhanced-live is explicitly selected, however, the
                # prediction may reverse the quoted ordering.  Enumerate both
                # directions there so the actual forecast gate can evaluate
                # the positive direction instead of silently omitting it.
                if (
                    economics_mode != "enhanced_live"
                    and short_q.funding_rate_bps <= long_q.funding_rate_bps
                ):
                    continue
                directional_pair_count += 1
                if max_candidates is not None:
                    seed_pairs.append(
                        (
                            _seed_is_potentially_live(
                                long_q,
                                short_q,
                                config=config,
                                fee_by_venue=fee_by_venue,
                                fee_evidence=fee_evidence,
                                expected_fee_identity_hashes=(
                                    expected_fee_identity_hashes
                                ),
                                observed_at_ms=observed_at_ms,
                            ),
                            _optimistic_candidate_rank_upper(
                                long_q,
                                short_q,
                                config=config,
                                fee_by_venue=fee_by_venue,
                                maker_fee_by_venue=maker_fee_by_venue,
                                caps_by_venue=caps_by_venue,
                                allocator=allocator,
                                passive_execution_enabled=passive_execution_enabled,
                                fee_evidence=fee_evidence,
                                expected_fee_identity_hashes=(
                                    expected_fee_identity_hashes
                                ),
                            ),
                            long_q,
                            short_q,
                        )
                    )
                    continue
                candidate = _candidate_for_pair(
                    long_q=long_q,
                    short_q=short_q,
                    config=config,
                    fee_by_venue=fee_by_venue,
                    maker_fee_by_venue=maker_fee_by_venue,
                    caps_by_venue=caps_by_venue,
                    allocator=allocator,
                    passive_execution_enabled=passive_execution_enabled,
                    fee_evidence=fee_evidence,
                    expected_fee_identity_hashes=expected_fee_identity_hashes,
                    observed_at_ms=observed_at_ms,
                    rejection_counts=rejection_counts,
                )
                if candidate is not None:
                    candidates.append(candidate)

    if max_candidates is not None:
        limit = max(int(max_candidates), 1)
        # The live entry artifact is a latency-bounded seed frontier, not a
        # second full-market audit build. Two exact rows per output slot is a
        # hard work budget. If the remaining optimistic bounds cannot be proven
        # dominated inside that budget, fail this generation closed instead of
        # publishing an approximate Top-K or moving a full audit onto the live
        # refresh path.
        exact_frontier_limit = min(len(seed_pairs), limit * 2)
        evaluated_seed_count = 0
        bound_violation = False
        frontier_complete = not seed_pairs
        frontier_stop_reason = "empty_seed_set" if not seed_pairs else ""
        ordered_seeds = sorted(
            seed_pairs,
            # Potentially executable seeds always consume the bounded exact
            # budget before deterministic blocked diagnostics. Edge remains
            # the order within each class.
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )
        for index, (
            _potentially_live,
            seed_upper,
            long_q,
            short_q,
        ) in enumerate(ordered_seeds):
            # The sort groups deterministic cheap blockers after every seed
            # that can still enter the live Top-K.  Their exact diagnostics
            # belong to the background audit artifact, not the latency-bound
            # entry data plane.
            if not _potentially_live:
                frontier_complete = True
                frontier_stop_reason = "potentially_live_seed_set_exhausted"
                break
            candidate = _candidate_for_pair(
                long_q=long_q,
                short_q=short_q,
                config=config,
                fee_by_venue=fee_by_venue,
                maker_fee_by_venue=maker_fee_by_venue,
                caps_by_venue=caps_by_venue,
                allocator=allocator,
                passive_execution_enabled=passive_execution_enabled,
                fee_evidence=fee_evidence,
                expected_fee_identity_hashes=expected_fee_identity_hashes,
                observed_at_ms=observed_at_ms,
                rejection_counts=rejection_counts,
            )
            evaluated_seed_count += 1
            if candidate is not None:
                candidates.append(candidate)
                if candidate.ranking_edge_bps > seed_upper + 1e-7:
                    # A future economics component must never silently make
                    # the optimistic bound unsafe.  Finishing the whole market
                    # here would put an unbounded audit calculation back on the
                    # live publication path, so fail this generation closed.
                    bound_violation = True
                    frontier_complete = False
                    frontier_stop_reason = "optimistic_upper_bound_violated"
                    break
            viable = sorted(
                (
                    row
                    for row in candidates
                    if not row.blocked and row.economics_complete
                ),
                key=lambda row: row.ranking_edge_bps,
                reverse=True,
            )
            if (
                len(viable) >= limit
                and index + 1 < len(ordered_seeds)
                and ordered_seeds[index + 1][1]
                <= viable[limit - 1].ranking_edge_bps + 1e-12
            ):
                frontier_complete = True
                frontier_stop_reason = "remaining_upper_bounds_dominated"
                break
            if evaluated_seed_count >= exact_frontier_limit:
                next_index = index + 1
                if next_index >= len(ordered_seeds):
                    frontier_complete = True
                    frontier_stop_reason = "seed_set_exhausted"
                elif not ordered_seeds[next_index][0]:
                    frontier_complete = True
                    frontier_stop_reason = "potentially_live_seed_set_exhausted"
                else:
                    frontier_complete = False
                    frontier_stop_reason = "exact_frontier_limit_reached"
                break
        else:
            frontier_complete = True
            frontier_stop_reason = "seed_set_exhausted"

        # Never publish a live ranking whose optimistic bound was disproved or
        # could not be proven inside the hard 2x exact-work budget.
        if not frontier_complete:
            candidates.clear()

    if max_candidates is not None:
        viable = sorted(
            (row for row in candidates if not row.blocked and row.economics_complete),
            key=lambda row: row.ranking_edge_bps,
            reverse=True,
        )
        blocked = sorted(
            (row for row in candidates if row.blocked or not row.economics_complete),
            key=lambda row: row.ranking_edge_bps,
            reverse=True,
        )
        ordered = (viable + blocked)[: max(int(max_candidates), 1)]
    else:
        ordered = sorted(candidates, key=lambda c: c.ranking_edge_bps, reverse=True)
    if diagnostics is not None:
        requested_symbols = sorted(canonical_symbols)
        diagnostics.clear()
        diagnostics.update(
            {
                "input_quote_count": len(quotes),
                "requested_symbol_count": len(requested_symbols),
                "requested_symbols": requested_symbols,
                "directional_pair_count": directional_pair_count,
                "output_candidate_count": len(ordered),
                "seed_frontier_count": (
                    evaluated_seed_count
                    if max_candidates is not None
                    else directional_pair_count
                ),
                "seed_pair_count": len(seed_pairs),
                "seed_potentially_live_count": sum(
                    1 for potentially_live, *_rest in seed_pairs
                    if potentially_live
                ),
                "seed_cheap_blocked_count": sum(
                    1 for potentially_live, *_rest in seed_pairs
                    if not potentially_live
                ),
                "seed_frontier_limit": (
                    exact_frontier_limit
                    if max_candidates is not None
                    else directional_pair_count
                ),
                "seed_frontier_truncated": bool(
                    max_candidates is not None
                    and evaluated_seed_count < len(seed_pairs)
                ),
                "seed_bound_violation": (
                    bound_violation if max_candidates is not None else False
                ),
                "seed_frontier_complete": (
                    frontier_complete if max_candidates is not None else True
                ),
                "seed_frontier_stop_reason": (
                    frontier_stop_reason
                    if max_candidates is not None
                    else "unbounded_audit_build"
                ),
                "future_input_quote_count": future_input_quote_count,
                "duplicate_input_quote_count": len(duplicate_quote_identities),
                "rejection_counts": dict(sorted(rejection_counts.items())),
            }
        )
    return ordered


def _candidate_for_pair(
    *,
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    config: StrategyConfig,
    fee_by_venue: dict[str, float],
    maker_fee_by_venue: dict[str, float],
    caps_by_venue: dict[str, float],
    allocator: StrategyRiskAllocator,
    passive_execution_enabled: bool,
    fee_evidence: FeeEvidenceBook | None,
    expected_fee_identity_hashes: dict[str, str] | None,
    observed_at_ms: int,
    rejection_counts: dict[str, int] | None = None,
) -> CandidateInput | None:
    # A snapshot is only a coherent cross-venue observation when no source
    # quote claims to have arrived after the refresh that is building it.
    # Keep zero/absent timestamps compatible with schema-1/V1 fixtures, but
    # fail closed on a future source timestamp rather than treating negative
    # age as a fresh executable market.
    now_ms = max(int(observed_at_ms or 0), 0)
    if now_ms > 0 and (
        _quote_observed_after(long_q, now_ms) or _quote_observed_after(short_q, now_ms)
    ):
        _record_rejection(rejection_counts, "quote_after_candidate_watermark")
        return None
    if not _valid_trade_quote(long_q) or not _valid_trade_quote(short_q):
        _record_rejection(rejection_counts, "invalid_trade_quote")
        return None
    long_mid = _mid(long_q)
    short_mid = _mid(short_q)
    if long_mid <= 0.0 or short_mid <= 0.0 or long_q.ask <= 0.0 or short_q.bid <= 0.0:
        _record_rejection(rejection_counts, "invalid_reference_price")
        return None
    contract_block_reasons = _funding_contract_block_reasons(long_q, short_q)
    # V1 prices a directed entry against the actual executable long ask and
    # short bid, rather than an unrelated midpoint.  This is the denominator
    # used by its cross, sizing and passive-spread recovery terms.
    reference_mid = (float(long_q.ask) + float(short_q.bid)) / 2.0
    raw_entry_cross_bps = ((short_q.bid - long_q.ask) / reference_mid) * 10_000.0
    long_ts = int(long_q.funding_timestamp_ms or 0)
    short_ts = int(short_q.funding_timestamp_ms or 0)
    interval_aligned = (
        abs(long_ts - short_ts) <= _INTERVAL_ALIGNED_THRESHOLD_MS
        if long_ts > 0 and short_ts > 0
        else False
    )
    first_ts = min(long_ts, short_ts) if long_ts > 0 and short_ts > 0 else 0
    second_ts = max(long_ts, short_ts) if long_ts > 0 and short_ts > 0 else 0
    first_leg = "long" if long_ts <= short_ts else "short" if long_ts and short_ts else ""
    now_ms = int(observed_at_ms or long_q.observed_at_ms or short_q.observed_at_ms or 0)

    configured_uncertainty = max(float(config.funding_forecast_uncertainty_haircut_bps or 0.0), 0.0)
    long_forecast = FundingForecast.from_quote(
        venue=long_q.venue,
        symbol=long_q.symbol,
        quoted_rate_bps=long_q.funding_rate_bps,
        predicted_settled_rate_bps=long_q.predicted_funding_rate_bps,
        next_funding_timestamp_ms=long_ts,
        funding_interval_ms=long_q.funding_interval_ms,
        observed_at_ms=now_ms,
        uncertainty_haircut_bps=max(
            configured_uncertainty,
            float(long_q.funding_forecast_uncertainty_bps or 0.0),
        ),
        sample_count=long_q.funding_forecast_sample_count,
        min_samples=config.funding_forecast_min_samples,
        source=long_q.funding_forecast_source,
    )
    short_forecast = FundingForecast.from_quote(
        venue=short_q.venue,
        symbol=short_q.symbol,
        quoted_rate_bps=short_q.funding_rate_bps,
        predicted_settled_rate_bps=short_q.predicted_funding_rate_bps,
        next_funding_timestamp_ms=short_ts,
        funding_interval_ms=short_q.funding_interval_ms,
        observed_at_ms=now_ms,
        uncertainty_haircut_bps=max(
            configured_uncertainty,
            float(short_q.funding_forecast_uncertainty_bps or 0.0),
        ),
        sample_count=short_q.funding_forecast_sample_count,
        min_samples=config.funding_forecast_min_samples,
        source=short_q.funding_forecast_source,
    )
    # Keep the two legs explicit.  For staggered timestamps the first
    # settlement contains only one of these components; crediting the total
    # carry at stage one is an economically invalid look-ahead.
    quoted_long_component = -float(long_q.funding_rate_bps)
    quoted_short_component = float(short_q.funding_rate_bps)
    forecast_long_component = -float(long_forecast.predicted_settled_rate_bps)
    forecast_short_component = float(short_forecast.predicted_settled_rate_bps)
    worst_long_component = -float(long_forecast.upper_bound_bps)
    worst_short_component = float(short_forecast.lower_bound_bps)
    required_shadow_age_ms = (
        max(int(config.funding_forecast_shadow_min_days or 0), 0) * 24 * 60 * 60 * 1000
    )
    long_shadow_age_ms = _forecast_shadow_age_ms(long_q, now_ms)
    short_shadow_age_ms = _forecast_shadow_age_ms(short_q, now_ms)
    forecast_shadow_age_ms = min(long_shadow_age_ms, short_shadow_age_ms)
    forecast_ready = (
        long_forecast.confidence > 0.0
        and short_forecast.confidence > 0.0
        and forecast_shadow_age_ms >= required_shadow_age_ms
    )
    forecast_distribution_stable = (
        long_q.funding_forecast_distribution_stable is True
        and short_q.funding_forecast_distribution_stable is True
    )
    forecast_stability_reason = (
        "|".join(
            f"{leg}:{str(getattr(quote, 'funding_forecast_stability_reason', '') or 'unknown')}"
            for leg, quote in (("long", long_q), ("short", short_q))
            if getattr(quote, "funding_forecast_distribution_stable", False) is not True
        )
        or "stable"
    )
    forecast_median_drift_bps = max(
        _finite_nonnegative(long_q.funding_forecast_median_drift_bps),
        _finite_nonnegative(short_q.funding_forecast_median_drift_bps),
    )
    forecast_p90_drift_bps = max(
        _finite_nonnegative(long_q.funding_forecast_p90_drift_bps),
        _finite_nonnegative(short_q.funding_forecast_p90_drift_bps),
    )
    economics_mode = str(config.funding_economics_mode or "v1_exact").lower()
    use_forecast_for_live_gate = (
        economics_mode == "enhanced_live" and forecast_ready and forecast_distribution_stable
    )
    # V1 exact and enhanced shadow keep the same entry gate.  Shadow records
    # the calibrated forecast without allowing an unproven model to alter a
    # live decision.  Enhanced live fails closed below when calibration is not
    # ready instead of silently using an optimistic prediction.
    if use_forecast_for_live_gate:
        gate_long_component = forecast_long_component
        gate_short_component = forecast_short_component
        gate_worst_long_component = worst_long_component
        gate_worst_short_component = worst_short_component
    else:
        # `enhanced_shadow` records the forecast but deliberately retains the
        # V1 quoted-rate entry contract until calibration has been proven.
        gate_long_component = quoted_long_component
        gate_short_component = quoted_short_component
        gate_worst_long_component = quoted_long_component
        gate_worst_short_component = quoted_short_component
    gate_funding = gate_long_component + gate_short_component
    gate_worst_funding = gate_worst_long_component + gate_worst_short_component
    calculation_version = economics_mode
    long_fee = fee_by_venue.get(str(long_q.venue).lower(), 0.0)
    short_fee = fee_by_venue.get(str(short_q.venue).lower(), 0.0)
    (
        canary_assurance_tier,
        taker_fee_evidence_complete,
        long_account_fee_authoritative,
        short_account_fee_authoritative,
        account_fee_unavailable_reason,
    ) = _pair_fee_assurance(
        long_q,
        short_q,
        config=config,
        fee_by_venue=fee_by_venue,
        fee_evidence=fee_evidence,
        expected_fee_identity_hashes=expected_fee_identity_hashes,
    )
    account_fee_evidence_base_complete = bool(
        fee_evidence is not None
        and fee_evidence.complete_for(long_q.venue, short_q.venue)
    )
    account_fee_identity_matches = (
        long_account_fee_authoritative and short_account_fee_authoritative
    )
    account_fee_evidence_complete = account_fee_identity_matches
    long_maker_fee = maker_fee_by_venue.get(str(long_q.venue).lower(), long_fee)
    short_maker_fee = maker_fee_by_venue.get(str(short_q.venue).lower(), short_fee)
    if (
        config.funding_canary_enabled is True
        and canary_assurance_tier == "conservative"
    ):
        # The fallback tier is a property of this symbol/venue leg, not of the
        # venue as a whole.  A daily-universe symbol that was not covered by
        # the latest private fee response must not inherit another symbol's
        # authority or maker discount.  Keep an authoritative sibling leg at
        # its measured fee and price only the unverified leg conservatively.
        conservative_buffer_bps = max(
            float(config.funding_canary_conservative_fee_buffer_bps or 0.0),
            0.0,
        )
        if not long_account_fee_authoritative:
            long_fee += conservative_buffer_bps
            long_maker_fee = long_fee
        if not short_account_fee_authoritative:
            short_fee += conservative_buffer_bps
            short_maker_fee = short_fee
    venue_haircut = float(
        config.funding_venue_risk_haircut_bps_by_venue.get(str(long_q.venue).lower(), 0.0) or 0.0
    ) + float(
        config.funding_venue_risk_haircut_bps_by_venue.get(str(short_q.venue).lower(), 0.0) or 0.0
    )
    opportunity_type = "aligned" if interval_aligned else "staggered"
    stagger_gap_ms = max(second_ts - first_ts, 0) if first_ts > 0 else 0
    if opportunity_type == "aligned":
        first_stage_funding = gate_funding
        first_stage_worst_funding = gate_worst_funding
        second_stage_funding = 0.0
        second_stage_worst_funding = 0.0
    elif first_leg == "long":
        first_stage_funding = gate_long_component
        first_stage_worst_funding = gate_worst_long_component
        second_stage_funding = gate_short_component
        second_stage_worst_funding = gate_worst_short_component
    elif first_leg == "short":
        first_stage_funding = gate_short_component
        first_stage_worst_funding = gate_worst_short_component
        second_stage_funding = gate_long_component
        second_stage_worst_funding = gate_worst_long_component
    else:
        # Timestamp-less candidates are incomplete and must not manufacture a
        # stage value that could later be mistaken for an executable edge.
        first_stage_funding = 0.0
        first_stage_worst_funding = 0.0
        second_stage_funding = 0.0
        second_stage_worst_funding = 0.0

    # Admission and first-stage stop/hold economics use only the carry that
    # settles before the planned first exit.  The total remains separately
    # attributable for a later evaluate-second-stage decision.
    configured_cap = min(
        max(float(config.entry_notional_cap_quote or 0.0), 0.0),
        max(float(config.live_entry_notional_cap_quote or 0.0), 0.0),
    )
    canary_hard_cap = (
        canary_notional_cap_for_tier(canary_assurance_tier, config)
        if config.funding_canary_enabled is True
        and canary_assurance_tier != "unavailable"
        else 0.0
    )
    pre_canary_configured_cap = configured_cap
    if canary_hard_cap > 0.0:
        configured_cap = min(configured_cap, canary_hard_cap)
    conservative_depth_quantity = configured_cap / reference_mid if configured_cap > 0.0 else 0.0
    pre_canary_conservative_depth_quantity = (
        pre_canary_configured_cap / reference_mid
        if pre_canary_configured_cap > 0.0
        else 0.0
    )
    long_depth = float(long_q.ask_size or 0.0) * max(
        float(config.max_top_book_usage_ratio or 0.0), 0.0
    )
    short_depth = float(short_q.bid_size or 0.0) * max(
        float(config.max_top_book_usage_ratio or 0.0), 0.0
    )
    long_allocation_quantity = long_depth if long_depth > 0.0 else conservative_depth_quantity
    short_allocation_quantity = short_depth if short_depth > 0.0 else conservative_depth_quantity
    pre_canary_long_allocation_quantity = (
        long_depth if long_depth > 0.0 else pre_canary_conservative_depth_quantity
    )
    pre_canary_short_allocation_quantity = (
        short_depth if short_depth > 0.0 else pre_canary_conservative_depth_quantity
    )
    if canary_hard_cap > 0.0:
        canary_quantity = canary_hard_cap / max(
            float(long_q.ask),
            float(short_q.bid),
        )
        long_allocation_quantity = min(long_allocation_quantity, canary_quantity)
        short_allocation_quantity = min(short_allocation_quantity, canary_quantity)
    venue_cap = _minimum_positive(
        caps_by_venue.get(str(long_q.venue).lower(), 0.0),
        caps_by_venue.get(str(short_q.venue).lower(), 0.0),
        config.max_single_venue_exposure_quote,
    )
    global_reference_cap = _minimum_positive(
        (
            float(config.funding_max_global_gross_exposure_quote or 0.0) / 2.0
            if float(config.funding_max_global_gross_exposure_quote or 0.0) > 0.0
            else 0.0
        ),
        config.funding_max_correlation_group_exposure_quote,
        (
            float(config.funding_expected_shortfall_budget_quote or 0.0)
            * 10_000.0
            / float(config.funding_expected_shortfall_bps or 0.0)
            if float(config.funding_expected_shortfall_bps or 0.0) > 0.0
            else 0.0
        ),
    )
    pre_canary_allocation = allocator.allocate(
        long_entry_price=long_q.ask,
        short_entry_price=short_q.bid,
        long_max_quantity=pre_canary_long_allocation_quantity,
        short_max_quantity=pre_canary_short_allocation_quantity,
        configured_notional_cap_quote=pre_canary_configured_cap,
        venue_notional_cap_quote=venue_cap,
        symbol_risk_budget_quote=float(config.max_symbol_exposure_quote or 0.0),
        venue_pair_risk_budget_quote=float(
            config.funding_max_venue_pair_exposure_quote or 0.0
        ),
        global_risk_budget_quote=global_reference_cap,
        fallback_notional_quote=float(
            config.funding_missing_margin_fallback_notional_quote or 0.0
        ),
        health_buffer_ratio=float(config.funding_risk_health_buffer_ratio or 0.0),
    )
    allocation = allocator.allocate(
        long_entry_price=long_q.ask,
        short_entry_price=short_q.bid,
        long_max_quantity=long_allocation_quantity,
        short_max_quantity=short_allocation_quantity,
        configured_notional_cap_quote=configured_cap,
        venue_notional_cap_quote=venue_cap,
        symbol_risk_budget_quote=float(config.max_symbol_exposure_quote or 0.0),
        venue_pair_risk_budget_quote=float(config.funding_max_venue_pair_exposure_quote or 0.0),
        global_risk_budget_quote=global_reference_cap,
        fallback_notional_quote=float(config.funding_missing_margin_fallback_notional_quote or 0.0),
        health_buffer_ratio=float(config.funding_risk_health_buffer_ratio or 0.0),
    )
    candidate_block_reasons = list(contract_block_reasons)
    if not account_fee_identity_matches:
        conservative_canary = bool(
            config.funding_canary_enabled is True
            and canary_assurance_tier == "conservative"
        )
        legacy_account_mismatch = bool(
            config.funding_canary_enabled is not True
            and account_fee_evidence_base_complete
            and expected_fee_identity_hashes is not None
        )
        unavailable_canary = bool(
            config.funding_canary_enabled is True
            and canary_assurance_tier == "unavailable"
            and taker_fee_evidence_complete
        )
        if not conservative_canary and (legacy_account_mismatch or unavailable_canary):
            candidate_block_reasons.append(account_fee_unavailable_reason)
    if not taker_fee_evidence_complete:
        # A zero fee can be a valid explicitly configured VIP tier, but an
        # omitted, non-finite, or negative taker fee is not evidence.  The
        # shortlist may remain observable for diagnostics, never complete
        # enough for a live first-leg decision.
        candidate_block_reasons.append("missing_taker_fee_evidence")
    if economics_mode == "enhanced_live" and not forecast_ready:
        candidate_block_reasons.append("funding_forecast_not_ready")
    if economics_mode == "enhanced_live" and not forecast_distribution_stable:
        candidate_block_reasons.append("funding_forecast_distribution_unstable")
    funding_decision_at_ms = int(observed_at_ms or 0)
    for leg_name, quote in (("long", long_q), ("short", short_q)):
        funding_reason = funding_rate_evidence_reason(
            venue=str(getattr(quote, "venue", "") or ""),
            symbol=str(getattr(quote, "symbol", "") or ""),
            rate_bps=getattr(quote, "funding_rate_bps", None),
            funding_timestamp_ms=getattr(quote, "funding_timestamp_ms", 0),
            observed_at_ms=getattr(quote, "funding_rate_observed_at_ms", 0),
            event_at_ms=getattr(quote, "funding_rate_event_at_ms", 0),
            received_at_ms=getattr(quote, "funding_rate_received_at_ms", 0),
            source=getattr(quote, "funding_rate_source", ""),
            sample_id=getattr(quote, "funding_rate_sample_id", ""),
            decision_at_ms=funding_decision_at_ms,
        )
        if funding_reason:
            candidate_block_reasons.append(
                f"{leg_name}_{funding_reason}"
            )
    required_economics_observations = (
        int(getattr(long_q, "observed_at_ms", 0) or 0),
        int(getattr(short_q, "observed_at_ms", 0) or 0),
        int(getattr(long_q, "funding_rate_observed_at_ms", 0) or 0),
        int(getattr(short_q, "funding_rate_observed_at_ms", 0) or 0),
    )
    if any(observed_at_ms <= 0 for observed_at_ms in required_economics_observations):
        candidate_block_reasons.append("funding_economics_observation_missing")
    candidate_blocked = bool(candidate_block_reasons)
    # Preserve the V1 discovery economics precisely.  Candidate construction
    # has only BBO size, so it uses V1's deliberately conservative depth
    # heuristic; live admission replaces the entry part with current L2 VWAP
    # immediately before leg one.  Crucially, passive mode gives the maker
    # leg both its spread recovery and maker fee, while it removes that leg's
    # taker-impact estimate.  This cannot be reconstructed from aggregate
    # four-leg fee fields after the fact.
    quantity = float(allocation.base_quantity or 0.0)
    canary_size_constrained = bool(
        config.funding_canary_enabled is True
        and canary_hard_cap > 0.0
        and quantity + 1e-12
        < float(pre_canary_allocation.base_quantity or 0.0)
    )
    if config.funding_canary_enabled is True and not contract_block_reasons:
        def _below_pair_minimum(target_quantity: float) -> bool:
            return any(
                target_quantity + 1e-12
                < float(quote.min_quantity_base or 0.0)
                or target_quantity * price + 1e-9
                < float(quote.min_notional_quote or 0.0)
                for quote, price in (
                    (long_q, float(long_q.ask)),
                    (short_q, float(short_q.bid)),
                )
            )

        if _below_pair_minimum(quantity):
            pre_canary_below_pair_minimum = _below_pair_minimum(
                float(pre_canary_allocation.base_quantity or 0.0)
            )
            candidate_block_reasons.append(
                "funding_canary_cap_below_pair_minimum"
                if canary_size_constrained
                and not pre_canary_below_pair_minimum
                else "entry_pair_minimum_not_met"
            )
            candidate_blocked = True
    long_entry_slippage_bps = _heuristic_slippage_bps(long_q, quantity, taking_ask=True)
    short_entry_slippage_bps = _heuristic_slippage_bps(short_q, quantity, taking_ask=False)
    entry_maker_leg = _select_maker_leg(long_entry_slippage_bps, short_entry_slippage_bps)
    entry_cross_bps = raw_entry_cross_bps
    if passive_execution_enabled:
        entry_cross_bps += _pair_spread_bps(long_q, short_q, reference_mid, entry_maker_leg)
    entry_fee_bps = _effective_fee_bps(
        long_taker_fee_bps=long_fee,
        long_maker_fee_bps=long_maker_fee,
        short_taker_fee_bps=short_fee,
        short_maker_fee_bps=short_maker_fee,
        maker_leg=entry_maker_leg,
        passive_execution_enabled=passive_execution_enabled,
    )
    entry_slippage_bps = _effective_slippage_bps(
        long_entry_slippage_bps,
        short_entry_slippage_bps,
        entry_maker_leg,
        passive_execution_enabled,
    )
    long_exit_slippage_bps = _heuristic_slippage_bps(long_q, quantity, taking_ask=False)
    short_exit_slippage_bps = _heuristic_slippage_bps(short_q, quantity, taking_ask=True)
    exit_maker_leg = _select_maker_leg(long_exit_slippage_bps, short_exit_slippage_bps)
    exit_fee_bps = _effective_fee_bps(
        long_taker_fee_bps=long_fee,
        long_maker_fee_bps=long_maker_fee,
        short_taker_fee_bps=short_fee,
        short_maker_fee_bps=short_maker_fee,
        maker_leg=exit_maker_leg,
        passive_execution_enabled=passive_execution_enabled,
    )
    exit_slippage_bps = _effective_slippage_bps(
        long_exit_slippage_bps,
        short_exit_slippage_bps,
        exit_maker_leg,
        passive_execution_enabled,
    )
    economics_observed_at_ms = (
        min(required_economics_observations)
        if all(
            observed_at_ms > 0
            for observed_at_ms in required_economics_observations
        )
        else 0
    )
    expected = build_edge_breakdown(
        funding_edge_bps=first_stage_funding,
        worst_case_funding_edge_bps=first_stage_worst_funding,
        entry_cross_bps=entry_cross_bps,
        entry_fee_bps=entry_fee_bps,
        exit_fee_bps=exit_fee_bps,
        entry_slippage_bps=entry_slippage_bps,
        exit_slippage_bps=exit_slippage_bps,
        adverse_selection_bps=float(config.entry_exit_reserve_bps or 0.0),
        capital_buffer_bps=float(config.capital_buffer_bps or 0.0),
        execution_buffer_bps=float(config.execution_buffer_bps or 0.0),
        venue_risk_haircut_bps=venue_haircut,
        calculation_version=calculation_version,
        model_epoch=calculation_version,
        observed_at_ms=economics_observed_at_ms,
        economics_complete=(quantity > 0.0 and bool(first_ts) and not candidate_block_reasons),
    )
    # A common-base hedge cannot be claimed for an unnormalised, inverse,
    # quanto, mismatched-underlying, or otherwise incompatible pair.  This is
    # a contract-safety fact, not a model preference: V1-compatible scoring
    # may remain visible in the sidecar, but no mode may label the candidate
    # complete or send it to live admission.
    pair_id = make_candidate_pair_id(long_q.symbol, long_q.venue, short_q.venue)
    fee_fingerprint = (
        fee_evidence.fingerprint_for(
            long_q.venue,
            short_q.venue,
            symbol=long_q.symbol,
        )
        if account_fee_evidence_complete and fee_evidence is not None
        else canary_assurance_tier
    )
    candidate_revision_id = build_candidate_revision_id(
        pair_id=pair_id,
        long_quote=long_q,
        short_quote=short_q,
        settlement_timestamps_ms=(first_ts, long_ts, short_ts, second_ts),
        entry_route=(entry_maker_leg if passive_execution_enabled else "taker_both"),
        exit_route=(exit_maker_leg if passive_execution_enabled else "taker_both"),
        fee_evidence_fingerprint=fee_fingerprint,
        fee_assurance_tier=canary_assurance_tier,
        model_epoch=calculation_version,
        economics={
            "long_funding_rate_bps": long_q.funding_rate_bps,
            "short_funding_rate_bps": short_q.funding_rate_bps,
            "long_forecast_rate_bps": long_forecast.predicted_settled_rate_bps,
            "short_forecast_rate_bps": short_forecast.predicted_settled_rate_bps,
            "entry_target_quantity": quantity,
            "long_taker_fee_bps": long_fee,
            "short_taker_fee_bps": short_fee,
            "long_maker_fee_bps": long_maker_fee,
            "short_maker_fee_bps": short_maker_fee,
            "entry_cross_bps": expected.entry_cross_bps,
            "entry_fee_bps": expected.entry_fee_bps,
            "exit_fee_bps": expected.exit_fee_bps,
            "entry_slippage_bps": expected.entry_slippage_bps,
            "exit_slippage_bps": expected.exit_slippage_bps,
            "expected_net_edge_bps": expected.expected_net_edge_bps,
            "worst_case_edge_bps": expected.worst_case_edge_bps,
            "ranking_edge_bps": expected.ranking_edge_bps,
        },
    )
    opportunity_lease_id = build_opportunity_lease_id(
        pair_id=pair_id,
        long_quote=long_q,
        short_quote=short_q,
        first_funding_timestamp_ms=first_ts,
        second_funding_timestamp_ms=second_ts,
        entry_route=(entry_maker_leg if passive_execution_enabled else "taker_both"),
        exit_route=(exit_maker_leg if passive_execution_enabled else "taker_both"),
        model_epoch=calculation_version,
    )
    return CandidateInput(
        long_venue=long_q.venue,
        short_venue=short_q.venue,
        symbol=str(long_q.symbol).upper(),
        funding_diff_bps=gate_funding,
        funding_edge_bps=first_stage_funding,
        expected_edge_bps=expected.expected_net_edge_bps,
        worst_case_edge_bps=expected.worst_case_edge_bps,
        ranking_edge_bps=expected.ranking_edge_bps,
        total_funding_edge_bps=gate_funding,
        first_stage_funding_edge_bps=first_stage_funding,
        first_stage_expected_edge_bps=expected.expected_net_edge_bps,
        first_stage_worst_case_edge_bps=expected.worst_case_edge_bps,
        second_stage_incremental_funding_edge_bps=second_stage_funding,
        second_stage_worst_case_funding_edge_bps=second_stage_worst_funding,
        stagger_gap_ms=stagger_gap_ms,
        entry_cross_bps=entry_cross_bps,
        fee_bps=entry_fee_bps + exit_fee_bps,
        entry_slippage_bps=entry_slippage_bps,
        long_entry_slippage_bps=long_entry_slippage_bps,
        short_entry_slippage_bps=short_entry_slippage_bps,
        long_exit_slippage_bps=long_exit_slippage_bps,
        short_exit_slippage_bps=short_exit_slippage_bps,
        long_taker_fee_bps=long_fee,
        short_taker_fee_bps=short_fee,
        taker_fee_evidence_complete=taker_fee_evidence_complete,
        account_fee_evidence_complete=account_fee_evidence_complete,
        account_fee_evidence_observed_at_ms=(
            fee_evidence.observed_at_ms_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and fee_evidence is not None
            else 0
        ),
        account_fee_evidence_source=(
            fee_evidence.source_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and fee_evidence is not None
            else ""
        ),
        account_fee_evidence_fingerprint=(
            fee_evidence.fingerprint_for(
                long_q.venue, short_q.venue, symbol=long_q.symbol
            )
            if account_fee_evidence_complete and fee_evidence is not None
            else ""
        ),
        account_fee_evidence_provenance=(
            fee_evidence.provenance_for(
                long_q.venue, short_q.venue, symbol=long_q.symbol
            )
            if account_fee_evidence_complete and fee_evidence is not None
            else []
        ),
        pair_id=pair_id,
        funding_timestamp_ms=first_ts,
        first_funding_timestamp_ms=first_ts,
        long_funding_timestamp_ms=long_ts,
        short_funding_timestamp_ms=short_ts,
        second_funding_timestamp_ms=second_ts,
        first_funding_leg=first_leg,
        entry_maker_leg=entry_maker_leg if passive_execution_enabled else "",
        exit_maker_leg=exit_maker_leg if passive_execution_enabled else "",
        direction_consistent=gate_funding > 0.0 and short_mid >= long_mid,
        interval_aligned=interval_aligned,
        opportunity_type=opportunity_type,
        entry_notional_quote=allocation.reference_notional_quote,
        entry_max_leg_notional_quote=max(
            allocation.long_leg_notional_quote,
            allocation.short_leg_notional_quote,
        ),
        funding_canary_fee_assurance_tier=canary_assurance_tier,
        funding_canary_hard_max_entry_notional_quote=canary_hard_cap,
        funding_canary_size_constrained=canary_size_constrained,
        funding_canary_requested_quantity=float(
            pre_canary_allocation.base_quantity or 0.0
        ),
        funding_canary_requested_max_leg_notional_quote=(
            float(pre_canary_allocation.base_quantity or 0.0)
            * max(float(long_q.ask), float(short_q.bid))
        ),
        contract_price_consistency_ratio=(
            max(float(long_q.ask), float(short_q.bid))
            / min(float(long_q.ask), float(short_q.bid))
        ),
        contract_price_consistency_long_price=float(long_q.ask),
        contract_price_consistency_short_price=float(short_q.bid),
        candidate_revision_id=candidate_revision_id,
        opportunity_lease_id=opportunity_lease_id,
        candidate_built_at_ms=now_ms,
        entry_target_quantity=allocation.base_quantity,
        long_max_executable_quantity=long_depth,
        short_max_executable_quantity=short_depth,
        entry_max_executable_quantity=min(long_depth, short_depth)
        if long_depth and short_depth
        else allocation.base_quantity,
        entry_max_executable_notional_quote=allocation.reference_notional_quote,
        entry_capacity_constrained=bool(long_depth > 0.0 or short_depth > 0.0),
        entry_depth_capped_at_entry=bool(long_depth > 0.0 or short_depth > 0.0),
        gross_signal_edge_bps=expected.gross_signal_edge_bps,
        expected_exit_cross_bps=expected.expected_exit_cross_bps,
        entry_fee_bps=expected.entry_fee_bps,
        exit_fee_bps=expected.exit_fee_bps,
        exit_slippage_bps=expected.exit_slippage_bps,
        adverse_selection_bps=expected.adverse_selection_bps,
        capital_buffer_bps=expected.capital_buffer_bps,
        execution_buffer_bps=expected.execution_buffer_bps,
        venue_risk_haircut_bps=expected.venue_risk_haircut_bps,
        transfer_or_inventory_bias_bps=expected.transfer_or_inventory_bias_bps,
        expected_net_edge_bps=expected.expected_net_edge_bps,
        expected_profit_quote=(
            allocation.reference_notional_quote
            * expected.expected_net_edge_bps
            / 10_000.0
        ),
        worst_case_profit_quote=(
            allocation.reference_notional_quote
            * expected.worst_case_edge_bps
            / 10_000.0
        ),
        economics_observed_at_ms=expected.observed_at_ms,
        economics_complete=(
            expected.economics_complete
            and allocation.base_quantity > 0.0
            and (
                economics_mode != "enhanced_live"
                or (forecast_ready and forecast_distribution_stable)
            )
            and not contract_block_reasons
        ),
        calculation_version=expected.calculation_version,
        model_epoch=calculation_version,
        forecast_long_rate_bps=long_forecast.predicted_settled_rate_bps,
        forecast_short_rate_bps=short_forecast.predicted_settled_rate_bps,
        # The final first-leg revalidator must use the same lifecycle horizon
        # as admission.  A staggered candidate cannot substitute total carry
        # (which includes the later settlement) for a first-stage worst case.
        forecast_worst_funding_edge_bps=first_stage_worst_funding,
        forecast_confidence=min(long_forecast.confidence, short_forecast.confidence),
        forecast_sample_count=min(long_forecast.sample_count, short_forecast.sample_count),
        forecast_shadow_age_ms=forecast_shadow_age_ms,
        forecast_ready=forecast_ready,
        forecast_distribution_stable=forecast_distribution_stable,
        forecast_stability_reason=forecast_stability_reason,
        forecast_median_drift_bps=forecast_median_drift_bps,
        forecast_p90_drift_bps=forecast_p90_drift_bps,
        forecast_source=f"{long_forecast.source}|{short_forecast.source}",
        advisories=[
            f"first_stage_funding_bps={first_stage_funding:.6f}",
            f"first_stage_worst_funding_bps={first_stage_worst_funding:.6f}",
            f"second_stage_funding_bps={second_stage_funding:.6f}",
            f"second_stage_worst_funding_bps={second_stage_worst_funding:.6f}",
            f"forecast_stability={forecast_stability_reason}",
            f"forecast_median_drift_bps={forecast_median_drift_bps:.6f}",
            f"forecast_p90_drift_bps={forecast_p90_drift_bps:.6f}",
            *[f"contract_validation_blocked:{reason}" for reason in contract_block_reasons],
            *([] if allocation.evidence_complete else ["sizing_missing_margin_fallback"]),
        ],
        blocked=candidate_blocked,
        blocked_reasons=candidate_block_reasons,
    )


def check_stale_snapshot(snapshot_published_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    """Return True unless the snapshot timestamp is within the usable range."""
    published_at_ms = int(snapshot_published_at_ms or 0)
    now_ms = int(now_ms or 0)
    if published_at_ms <= 0 or published_at_ms > now_ms:
        return True
    return (now_ms - published_at_ms) > max(int(max_age_ms or 0), 0)


def reference_mid_valid(long_q: QuoteSnapshot, short_q: QuoteSnapshot) -> bool:
    try:
        long_ask = float(long_q.ask)
        short_bid = float(short_q.bid)
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        isfinite(long_ask)
        and isfinite(short_bid)
        and long_ask > 0
        and short_bid > 0
    )


def _mid(quote: QuoteSnapshot) -> float:
    bid = float(quote.bid or 0.0)
    ask = float(quote.ask or 0.0)
    return (bid + ask) / 2.0 if isfinite(bid) and isfinite(ask) and bid > 0.0 and ask > 0.0 else 0.0


def _valid_trade_quote(quote: QuoteSnapshot) -> bool:
    """Reject corrupted BBO/funding values before they enter ranking.

    ``nan`` has false ordering semantics, which otherwise lets a malformed
    source bypass simple positive-value guards and contaminate the shortlist.
    Top-book size intentionally remains optional here: the V1 conservative
    slippage branch prices missing depth as a large penalty rather than
    pretending the pair has no price.
    """
    raw_values = (quote.bid, quote.ask, quote.funding_rate_bps)
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in raw_values
    ):
        return False
    optional_numeric_values = (
        quote.predicted_funding_rate_bps,
        quote.funding_forecast_uncertainty_bps,
        quote.bid_size,
        quote.ask_size,
    )
    if any(
        value is not None
        and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(float(value))
        )
        for value in optional_numeric_values
    ):
        return False
    for value in (quote.funding_timestamp_ms, quote.funding_interval_ms):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
        ):
            return False
    try:
        bid = float(quote.bid)
        ask = float(quote.ask)
        funding = float(quote.funding_rate_bps)
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        all(isfinite(value) for value in (bid, ask, funding))
        and bid > 0.0
        and ask > 0.0
        and bid <= ask
    )


def _quote_observed_after(quote: QuoteSnapshot, now_ms: int) -> bool:
    """Whether a non-legacy source timestamp would be from the future."""
    raw_observed_at_ms = getattr(quote, "observed_at_ms", 0)
    if raw_observed_at_ms in (None, "", 0):
        return False
    try:
        observed_at_ms = int(raw_observed_at_ms)
    except (TypeError, ValueError, OverflowError):
        return True
    return observed_at_ms > now_ms


def _minimum_positive(*values: float) -> float:
    positive = [float(value) for value in values if float(value or 0.0) > 0.0]
    return min(positive) if positive else 0.0


def _finite_nonnegative(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return parsed if isfinite(parsed) and parsed >= 0.0 else 0.0


def _quote_spread_bps(quote: QuoteSnapshot, reference_mid: float) -> float:
    if reference_mid <= 0.0:
        return 0.0
    return max(float(quote.ask) - float(quote.bid), 0.0) / reference_mid * 10_000.0


def _pair_spread_bps(
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
    reference_mid: float,
    maker_leg: str,
) -> float:
    quote = long_quote if maker_leg == "long" else short_quote
    return _quote_spread_bps(quote, reference_mid)


def _heuristic_slippage_bps(
    quote: QuoteSnapshot,
    quantity: float,
    *,
    taking_ask: bool,
) -> float:
    """V1 BBO-only impact heuristic for a specific taker leg."""
    top_size = float((quote.ask_size if taking_ask else quote.bid_size) or 0.0)
    if top_size <= 0.0 or quantity <= 0.0:
        return 500.0
    mid = _mid(quote)
    if mid <= 0.0:
        return 500.0
    half_spread_bps = max(
        max(float(quote.ask) - float(quote.bid), 0.0) / mid * 10_000.0 / 2.0,
        0.1,
    )
    depth_ratio = quantity / top_size
    if depth_ratio <= 1.0:
        return depth_ratio * half_spread_bps
    return half_spread_bps + (depth_ratio - 1.0) * half_spread_bps * 2.0


def _select_maker_leg(long_slippage_bps: float, short_slippage_bps: float) -> str:
    """Match V1 tie breaking: the long leg wins equal estimated impact."""
    return "long" if long_slippage_bps >= short_slippage_bps - 1e-9 else "short"


def _effective_fee_bps(
    *,
    long_taker_fee_bps: float,
    long_maker_fee_bps: float,
    short_taker_fee_bps: float,
    short_maker_fee_bps: float,
    maker_leg: str,
    passive_execution_enabled: bool,
) -> float:
    taker_floor = float(long_taker_fee_bps) + float(short_taker_fee_bps)
    if not passive_execution_enabled:
        return taker_floor
    if maker_leg == "long":
        asserted = float(long_maker_fee_bps) + float(short_taker_fee_bps)
    else:
        asserted = float(long_taker_fee_bps) + float(short_maker_fee_bps)
    # Maker schedules are useful operational evidence, but their serialized
    # provenance cannot authenticate itself.  Never let a discount/rebate
    # create shortlist alpha; the local live boundary may only improve this
    # after reloading the signed account evidence.
    return max(asserted, taker_floor)


def _taker_fee_evidence_complete(
    fee_by_venue: dict[str, float],
    long_venue: object,
    short_venue: object,
) -> bool:
    """Require explicit, finite, non-negative taker fees for both legs."""
    for venue in (long_venue, short_venue):
        key = str(venue or "").lower()
        if key not in fee_by_venue:
            return False
        try:
            fee_bps = float(fee_by_venue[key])
        except (TypeError, ValueError):
            return False
        if not isfinite(fee_bps) or fee_bps < 0.0:
            return False
    return True


def _effective_slippage_bps(
    long_slippage_bps: float,
    short_slippage_bps: float,
    maker_leg: str,
    passive_execution_enabled: bool,
) -> float:
    if not passive_execution_enabled:
        return max(float(long_slippage_bps), 0.0) + max(float(short_slippage_bps), 0.0)
    return max(
        float(short_slippage_bps) if maker_leg == "long" else float(long_slippage_bps),
        0.0,
    )


def _funding_contract_block_reasons(
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
) -> tuple[str, ...]:
    """Return proof gaps that make a common-base funding hedge unsafe.

    Every funding mode requires this proof before a common-base hedge may be
    admitted: a pair cannot use one quantity if contract units, underlying,
    quote currency or price/quantity precision are unknown or incompatible.
    """
    reasons: list[str] = []
    if long_quote.contract_normalization_complete is not True:
        reasons.append("long_contract_normalization_incomplete")
    if short_quote.contract_normalization_complete is not True:
        reasons.append("short_contract_normalization_incomplete")
    if not _same_text(long_quote.underlying, short_quote.underlying):
        reasons.append("underlying_mismatch")
    if not _same_text(long_quote.quote_currency, short_quote.quote_currency):
        reasons.append("quote_currency_mismatch")
    if str(long_quote.contract_type or "").lower() != "linear":
        reasons.append("long_contract_type_incompatible")
    if str(short_quote.contract_type or "").lower() != "linear":
        reasons.append("short_contract_type_incompatible")
    long_multiplier = _positive_finite_number(long_quote.contract_multiplier)
    short_multiplier = _positive_finite_number(short_quote.contract_multiplier)
    if (
        long_multiplier is None
        or short_multiplier is None
        or abs(long_multiplier - short_multiplier) > 1e-12
    ):
        reasons.append("contract_multiplier_mismatch")
    long_price = _positive_finite_number(long_quote.ask)
    short_price = _positive_finite_number(short_quote.bid)
    if long_price is not None and short_price is not None:
        normalized_price_ratio = max(long_price, short_price) / min(
            long_price,
            short_price,
        )
        # Deliberately wide: this is a contract-binding guard, not a basis
        # alpha filter. It catches 1000x/multiplier mapping errors while
        # preserving any plausible cross-venue dislocation.
        if normalized_price_ratio > 2.0:
            reasons.append("cross_venue_price_normalization_mismatch")
    if not str(long_quote.mark_index_source or "").strip():
        reasons.append("long_mark_index_source_missing")
    if not str(short_quote.mark_index_source or "").strip():
        reasons.append("short_mark_index_source_missing")
    if not _nonnegative_literal_int(long_quote.price_precision) or not _nonnegative_literal_int(
        long_quote.quantity_precision
    ):
        reasons.append("long_precision_missing")
    if not _nonnegative_literal_int(short_quote.price_precision) or not _nonnegative_literal_int(
        short_quote.quantity_precision
    ):
        reasons.append("short_precision_missing")
    for leg_name, quote in (("long", long_quote), ("short", short_quote)):
        for field_name in (
            "price_tick",
            "quantity_step_base",
            "min_quantity_base",
        ):
            if _positive_finite_number(getattr(quote, field_name, 0.0)) is None:
                reasons.append(f"{leg_name}_{field_name}_missing")
        min_notional = getattr(quote, "min_notional_quote", None)
        if (
            quote.min_notional_evidence_complete is not True
            or isinstance(min_notional, bool)
            or not isinstance(min_notional, (int, float))
            or not isfinite(float(min_notional))
            or float(min_notional) < 0.0
        ):
            reasons.append(f"{leg_name}_min_notional_evidence_missing")
    if str(long_quote.venue_status or "").lower() != "active":
        reasons.append("long_venue_inactive")
    if str(short_quote.venue_status or "").lower() != "active":
        reasons.append("short_venue_inactive")
    if not _positive_literal_int(long_quote.funding_interval_ms):
        reasons.append("long_funding_interval_unknown")
    if not _positive_literal_int(short_quote.funding_interval_ms):
        reasons.append("short_funding_interval_unknown")
    if not _positive_literal_int(long_quote.funding_timestamp_ms):
        reasons.append("long_funding_timestamp_unknown")
    if not _positive_literal_int(short_quote.funding_timestamp_ms):
        reasons.append("short_funding_timestamp_unknown")
    return tuple(reasons)


def _positive_finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if isfinite(parsed) and parsed > 0.0 else None


def _positive_literal_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _nonnegative_literal_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _normalise_venue_bps(
    values: dict[str, float], *, allow_negative: bool = False
) -> dict[str, float]:
    """Normalise static venue evidence once at the service boundary.

    JSON booleans are integers in Python.  Treat them as malformed evidence,
    not as a 0/1 bps fee or cap, so a config/parser artefact cannot manufacture
    complete live economics.
    """
    normalized: dict[str, float] = {}
    for key, value in values.items():
        if isinstance(value, bool):
            normalized[str(key).lower()] = float("nan")
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            parsed = float("nan")
        normalized[str(key).lower()] = (
            parsed if allow_negative or not isfinite(parsed) or parsed >= 0.0 else float("nan")
        )
    return normalized


def _same_text(left: object, right: object) -> bool:
    first = str(left or "").strip().upper()
    second = str(right or "").strip().upper()
    return bool(first and second and first == second)


def _forecast_shadow_age_ms(quote: QuoteSnapshot, now_ms: int) -> int:
    started_at_ms = int(getattr(quote, "funding_forecast_started_at_ms", 0) or 0)
    if started_at_ms <= 0:
        return 0
    return max(int(now_ms or 0) - started_at_ms, 0)
