"""Runtime boundary for durable, public-data funding basis-risk evidence."""

from __future__ import annotations

from itertools import combinations
from typing import Any

from lightfee.strategy.funding_basis_risk import (
    FundingBasisExpectedShortfallEstimate,
    FundingBasisExpectedShortfallModel,
    publish_funding_basis_risk_checkpoint,
    restore_funding_basis_risk_checkpoint,
)


class FundingRiskRuntime:
    """Own the dynamic ES lifecycle without adding market-data requests.

    The live runtime only hands this service an already fresh sidecar snapshot.
    It never runs on last-good/degraded data, never invents quote timestamps,
    and makes checkpoint failures an entry-admission failure rather than a
    reason to fall back to a static volatility guess.
    """

    def __init__(self, ctx: Any) -> None:
        self.ctx = ctx
        strategy = ctx.config.strategy
        self.model = FundingBasisExpectedShortfallModel(
            window_ms=strategy.funding_dynamic_expected_shortfall_window_ms,
            max_samples=strategy.funding_dynamic_expected_shortfall_max_samples,
            max_pairs=strategy.funding_dynamic_expected_shortfall_max_pairs,
            horizon_ms=strategy.funding_dynamic_expected_shortfall_horizon_ms,
            min_samples=strategy.funding_dynamic_expected_shortfall_min_samples,
            min_history_ms=strategy.funding_dynamic_expected_shortfall_min_history_ms,
            confidence=strategy.funding_dynamic_expected_shortfall_confidence,
            quote_skew_ms=strategy.funding_dynamic_expected_shortfall_quote_skew_ms,
        )
        self.checkpoint_path = ctx.config.runtime.funding_basis_risk_checkpoint_path
        self.checkpoint_max_age_ms = (
            strategy.funding_dynamic_expected_shortfall_checkpoint_max_age_ms
        )
        self.checkpoint_publish_interval_ms = (
            strategy.funding_dynamic_expected_shortfall_checkpoint_publish_interval_ms
        )
        self._checkpoint_loaded = False
        self._checkpoint_restored = False
        self._checkpoint_healthy = False
        self._last_checkpoint_publish_ms = 0
        self._last_checkpoint_error = ""

    def observe_fresh_snapshot(self, snapshot: Any, *, now_ms: int) -> dict[str, object]:
        """Consume one fresh snapshot and publish a bounded checkpoint.

        The batch is committed before entry selection, but the model excludes
        that exact batch from every same-tick estimate.  Thus no live candidate
        can use the sidecar observation which selected it to reduce ES.
        """

        self._restore_once(now_ms)
        quotes = self._unique_quotes_by_venue_symbol(getattr(snapshot, "quotes", {}))
        batch_id = self.model.begin_observation_batch()
        observed_pairs = 0
        rejected_pairs = 0
        by_symbol: dict[str, list[Any]] = {}
        max_quote_age_ms = int(
            getattr(self.ctx.config.runtime, "max_market_age_ms", 0) or 0
        )
        for (venue, symbol), quote in quotes.items():
            if quote is None or not self._quote_is_fresh(
                quote,
                now_ms=now_ms,
                max_age_ms=max_quote_age_ms,
            ):
                continue
            by_symbol.setdefault(symbol, []).append(quote)
        for symbol, symbol_quotes in by_symbol.items():
            for first, second in combinations(symbol_quotes, 2):
                if self.model.observe_pair(
                    symbol=symbol,
                    venue_a=getattr(first, "venue", ""),
                    venue_b=getattr(second, "venue", ""),
                    bid_a=getattr(first, "bid", 0.0),
                    ask_a=getattr(first, "ask", 0.0),
                    observed_a_ms=getattr(first, "observed_at_ms", 0),
                    bid_b=getattr(second, "bid", 0.0),
                    ask_b=getattr(second, "ask", 0.0),
                    observed_b_ms=getattr(second, "observed_at_ms", 0),
                    now_ms=now_ms,
                    batch_id=batch_id,
                ):
                    observed_pairs += 1
                else:
                    rejected_pairs += 1
        self._maybe_publish_checkpoint(now_ms)
        return {
            "model_version": "funding_basis_es_v1",
            "checkpoint_restored": self._checkpoint_restored,
            "checkpoint_healthy": self._checkpoint_healthy,
            "checkpoint_error": self._last_checkpoint_error or None,
            "observed_pair_count": observed_pairs,
            "rejected_pair_count": rejected_pairs,
            "tracked_pair_count": self.model.state_count,
            "current_batch_id": batch_id,
            "source": "fresh_sidecar_snapshot",
        }

    def estimate_candidate(
        self,
        candidate: Any,
        *,
        long_venue: object,
        short_venue: object,
        now_ms: int,
    ) -> FundingBasisExpectedShortfallEstimate:
        strategy = self.ctx.config.strategy
        if strategy.funding_dynamic_expected_shortfall_enabled is not True:
            return FundingBasisExpectedShortfallEstimate(
                0.0, 0, 0, 0, self.model.confidence, False,
                "dynamic_expected_shortfall_disabled",
            )
        if not self._checkpoint_healthy:
            return FundingBasisExpectedShortfallEstimate(
                0.0, 0, 0, 0, self.model.confidence, False,
                "basis_risk_checkpoint_not_durable",
            )
        return self.model.estimate(
            symbol=str(getattr(candidate, "symbol", "") or ""),
            long_venue=str(getattr(long_venue, "value", long_venue) or ""),
            short_venue=str(getattr(short_venue, "value", short_venue) or ""),
            now_ms=now_ms,
        )

    def mark_unhealthy(self, reason: str) -> None:
        """Force later entry admission to fail closed after observation errors."""

        self._checkpoint_healthy = False
        self._last_checkpoint_error = str(reason or "risk_runtime_error")

    def _restore_once(self, now_ms: int) -> None:
        if self._checkpoint_loaded:
            return
        self._checkpoint_loaded = True
        self._checkpoint_restored = restore_funding_basis_risk_checkpoint(
            self.model,
            self.checkpoint_path,
            now_ms=now_ms,
            max_age_ms=self.checkpoint_max_age_ms,
        )
        self._checkpoint_healthy = self._checkpoint_restored

    def _maybe_publish_checkpoint(self, now_ms: int) -> None:
        if self.model.state_count <= 0:
            return
        due = (
            not self._checkpoint_healthy
            or self._last_checkpoint_publish_ms <= 0
            or now_ms - self._last_checkpoint_publish_ms
            >= self.checkpoint_publish_interval_ms
        )
        if not due:
            return
        try:
            publish_funding_basis_risk_checkpoint(
                self.model,
                self.checkpoint_path,
                now_ms=now_ms,
            )
        except OSError as exc:
            self._checkpoint_healthy = False
            self._last_checkpoint_error = type(exc).__name__
            return
        self._checkpoint_healthy = True
        self._last_checkpoint_error = ""
        self._last_checkpoint_publish_ms = now_ms

    @staticmethod
    def _unique_quotes_by_venue_symbol(raw_quotes: Any) -> dict[tuple[str, str], Any | None]:
        """Use explicit quote identities, refusing duplicate evidence."""

        items = raw_quotes.values() if hasattr(raw_quotes, "values") else raw_quotes or ()
        result: dict[tuple[str, str], Any | None] = {}
        for quote in items:
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            if not venue or not symbol:
                continue
            key = venue, symbol
            if key in result:
                result[key] = None
            else:
                result[key] = quote
        return result

    @staticmethod
    def _quote_is_fresh(quote: Any, *, now_ms: int, max_age_ms: int) -> bool:
        if max_age_ms <= 0:
            return False
        try:
            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return False
        return observed_at_ms > 0 and observed_at_ms <= now_ms and now_ms - observed_at_ms <= max_age_ms
