"""Spread-reversion sidecar service."""

from __future__ import annotations

import asyncio
import logging
import math
from pathlib import Path
import time

from lightfee.config.schema import AppConfig
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadSnapshot
from lightfee.spread.paper_runtime import (
    SpreadPaperConfig,
    SpreadPaperJournal,
    SpreadPaperTracker,
    load_paper_checkpoint,
    publish_paper_checkpoint,
)
from lightfee.spread.publisher import publish_spread_snapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadSignalEngine,
    SpreadStatsTracker,
)
from lightfee.spread.stats_checkpoint import (
    publish_spread_stats_checkpoint,
    restore_spread_stats_checkpoint,
)
from lightfee.spread.universe import (
    resolve_spread_sampling_symbols,
    spread_sampling_selection_required,
)


logger = logging.getLogger("lightfee.spread.service")

_STATS_CHECKPOINT_MIN_INTERVAL_MS = 30_000


def _paper_quote_key(venue: str, symbol: str) -> str:
    return f"{str(venue).lower()}:{str(symbol).upper()}"


def _quote_is_valid_for_spread_sidecar(
    quote: QuoteSnapshot,
    *,
    observed_ms: int,
    max_age_ms: int,
) -> bool:
    venue = str(getattr(quote, "venue", "") or "").strip()
    symbol = str(getattr(quote, "symbol", "") or "").strip()
    if not venue or not symbol:
        return False
    try:
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        bid_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
        quote_ts_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    if not all(math.isfinite(value) for value in (bid, ask, bid_size, ask_size)):
        return False
    if bid <= 0.0 or ask <= 0.0 or bid > ask:
        return False
    if bid_size < 0.0 or ask_size < 0.0:
        return False
    if quote_ts_ms <= 0 or quote_ts_ms > observed_ms:
        return False
    return max_age_ms <= 0 or observed_ms - quote_ts_ms <= max_age_ms


class SpreadSidecarService:
    """Public-data signal process for spread reversion.

    It has no private credentials and no order submission path.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.snapshot_path = config.runtime.spread_sidecar_snapshot_path
        self.sidecar_snapshot_path = config.runtime.sidecar_snapshot_path
        self._spread_sampling_selection_required = spread_sampling_selection_required(
            config
        )
        self._spread_sampling_symbols = (
            () if self._spread_sampling_selection_required else tuple(config.symbols)
        )
        self.signal_config = SpreadReversionConfig.from_app_config(config)
        self.stats = SpreadStatsTracker()
        self.signal_engine = SpreadSignalEngine(
            tracker=self.stats,
            config=self.signal_config,
        )
        self.stats_checkpoint_path = config.runtime.spread_stats_checkpoint_path
        self._stats_checkpoint_loaded = False
        self._stats_checkpoint_restored = False
        self._stats_checkpoint_persisted_revision = self.stats.revision
        self._stats_checkpoint_last_attempt_ms = 0
        self._stats_checkpoint_task: asyncio.Task[None] | None = None
        self._paper_journal: SpreadPaperJournal | None = None
        self._paper_tracker = SpreadPaperTracker(self._paper_config(config))
        self._paper_checkpoint_path = Path(
            f"{config.persistence.spread_paper_event_log_path}.checkpoint.json"
        )
        if self._paper_tracker.enabled:
            persistence = config.persistence
            self._paper_journal = SpreadPaperJournal(
                persistence.spread_paper_event_log_path,
                max_bytes=int(persistence.spread_paper_event_log_hard_max_bytes),
            )
            self._paper_journal.open()
            checkpoint = load_paper_checkpoint(self._paper_checkpoint_path)
            if checkpoint is not None and not self._paper_tracker.restore_checkpoint(
                checkpoint
            ):
                logger.error(
                    "spread paper checkpoint is invalid; paper admission disabled"
                )

    async def close(self) -> None:
        close_cancelled = False
        if hasattr(self, "stats"):
            checkpoint_task = getattr(self, "_stats_checkpoint_task", None)
            if checkpoint_task is not None:
                try:
                    await asyncio.shield(checkpoint_task)
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    close_cancelled = close_cancelled or bool(
                        current_task is not None and current_task.cancelling()
                    )
            self._checkpoint_stats_if_due(int(time.time() * 1000), force=True)
            checkpoint_task = getattr(self, "_stats_checkpoint_task", None)
            if checkpoint_task is not None:
                try:
                    await asyncio.shield(checkpoint_task)
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    close_cancelled = close_cancelled or bool(
                        current_task is not None and current_task.cancelling()
                    )
        if self._paper_journal is not None:
            try:
                publish_paper_checkpoint(
                    self._paper_checkpoint_path,
                    self._paper_tracker.checkpoint(),
                )
                self._paper_journal.close()
            except Exception:
                logger.exception("spread sidecar journal close failed; continuing resource cleanup")
            finally:
                self._paper_journal = None
        if close_cancelled:
            raise asyncio.CancelledError

    async def refresh_once(self, *, now_ms: int | None = None) -> SpreadSnapshot:
        requested_decision_at_ms = int(now_ms) if now_ms is not None else None
        (
            quotes,
            degraded_venues,
            source_mode,
            input_quote_count,
            market_observed_at_ms,
            degraded_symbols,
            decision_at_ms,
        ) = await self._fetch_quotes(requested_decision_at_ms)
        self._restore_stats_checkpoint_once(decision_at_ms)
        rejection_counts: dict[str, int] = {}
        evaluation_diagnostics: dict[str, int] = {}
        candidates = self.signal_engine.build(
            quotes,
            list(self._spread_sampling_symbols or self.config.symbols),
            now_ms=decision_at_ms,
            rejection_counts=rejection_counts,
            diagnostics=evaluation_diagnostics,
        )
        self._checkpoint_stats_if_due(decision_at_ms)
        paper_result = await self._refresh_paper(
            candidates,
            quotes,
            decision_at_ms,
        )
        published_at_ms = (
            decision_at_ms if now_ms is not None else max(int(time.time() * 1000), decision_at_ms)
        )
        snapshot = SpreadSnapshot(
            decision_at_ms=decision_at_ms,
            published_at_ms=published_at_ms,
            market_observed_at_ms=market_observed_at_ms,
            snapshot_path=str(self.snapshot_path),
            source_mode=source_mode,
            degraded_venues=sorted(degraded_venues),
            degraded_symbols=degraded_symbols,
            input_quote_count=input_quote_count,
            valid_quote_count=len(quotes),
            evaluated_pair_count=int(evaluation_diagnostics.get("evaluated_pair_count", 0)),
            accepted_pair_count=int(evaluation_diagnostics.get("accepted_pair_count", 0)),
            paper_configured_enabled=(self.config.strategy.spread_paper_enabled is True),
            paper_admission_enabled=self._paper_tracker.enabled,
            paper_tracked_count=self._paper_tracker.tracked_count,
            paper_refresh_status=str(paper_result["status"]),
            paper_event_count=int(paper_result["event_count"]),
            paper_last_success_at_ms=int(paper_result["last_success_at_ms"]),
            rejection_counts=rejection_counts,
            paper_admission_rejection_counts=dict(
                paper_result["admission_rejection_counts"]
            ),
            candidates=candidates,
        )
        publish_spread_snapshot(snapshot, self.snapshot_path)
        return snapshot

    def _restore_stats_checkpoint_once(self, now_ms: int) -> None:
        if self._stats_checkpoint_loaded:
            return
        if self._spread_sampling_selection_required and not self._spread_sampling_symbols:
            return
        self._stats_checkpoint_loaded = True
        self._stats_checkpoint_restored = restore_spread_stats_checkpoint(
            self.stats,
            self.stats_checkpoint_path,
            model_epoch=self.signal_config.model_epoch,
            now_ms=now_ms,
            allowed_symbols=(
                set(self._spread_sampling_symbols)
                if self._spread_sampling_selection_required
                else None
            ),
        )
        # Restoration establishes the durable baseline.  A new market sample
        # marks the tracker dirty through its monotonic revision; unchanged
        # state must never trigger a multi-megabyte rewrite on every quote
        # refresh.
        self._stats_checkpoint_persisted_revision = self.stats.revision
        self._stats_checkpoint_last_attempt_ms = max(int(now_ms or 0), 0)

    def _checkpoint_stats_if_due(self, now_ms: int, *, force: bool = False) -> bool:
        active_task = getattr(self, "_stats_checkpoint_task", None)
        if active_task is not None and not active_task.done():
            return False
        revision = self.stats.revision
        if revision == self._stats_checkpoint_persisted_revision:
            return False
        observed_ms = max(int(now_ms or 0), 0)
        if (
            not force
            and self._stats_checkpoint_last_attempt_ms > 0
            and observed_ms - self._stats_checkpoint_last_attempt_ms
            < _STATS_CHECKPOINT_MIN_INTERVAL_MS
        ):
            return False
        # Rate-limit failures as well as successes.  A full or temporarily
        # unwritable disk must not turn the 250 ms signal loop into a tight
        # checkpoint retry loop.
        self._stats_checkpoint_last_attempt_ms = observed_ms
        self._stats_checkpoint_task = asyncio.create_task(
            self._publish_stats_checkpoint_in_background(
                revision=revision,
                observed_ms=observed_ms,
            )
        )
        return True

    async def _publish_stats_checkpoint_in_background(
        self,
        *,
        revision: int,
        observed_ms: int,
    ) -> None:
        """Persist a large rolling window without blocking signal publication."""

        try:
            await asyncio.to_thread(
                publish_spread_stats_checkpoint,
                self.stats,
                self.stats_checkpoint_path,
                model_epoch=self.signal_config.model_epoch,
                now_ms=observed_ms,
            )
        except Exception:
            # The next process safely cold-starts if the durable snapshot is
            # unavailable. Keep the live signal loop available and retry only
            # after the normal checkpoint interval.
            logger.exception(
                "spread stats checkpoint publish failed; next restart will cold-start",
                extra={"checkpoint_path": self.stats_checkpoint_path},
            )
        else:
            # Mark only the revision that was scheduled. Evidence accepted
            # while the thread was writing remains dirty for the next pass.
            self._stats_checkpoint_persisted_revision = max(
                self._stats_checkpoint_persisted_revision,
                revision,
            )
        finally:
            if self._stats_checkpoint_task is asyncio.current_task():
                self._stats_checkpoint_task = None

    async def _refresh_paper(
        self,
        candidates: list,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> dict[str, int | str | dict[str, int]]:
        if self.config.strategy.spread_paper_enabled is not True:
            return {
                "status": "disabled",
                "event_count": 0,
                "last_success_at_ms": 0,
                "admission_rejection_counts": {},
            }
        if self._paper_journal is None:
            return {
                "status": "journal_unavailable",
                "event_count": 0,
                "last_success_at_ms": 0,
                "admission_rejection_counts": {"paper_journal_unavailable": 1},
            }
        if not self._paper_tracker.enabled:
            return {
                "status": "admission_disabled",
                "event_count": 0,
                "last_success_at_ms": 0,
                "admission_rejection_counts": {"paper_admission_disabled": 1},
            }
        try:
            paper_events: list[tuple[str, dict]] = []
            admission_rejection_counts: dict[str, int] = {}
            finalist_limit = self._paper_tracker.config.finalist_limit
            if len(candidates) > finalist_limit:
                admission_rejection_counts["paper_finalist_limit"] = (
                    len(candidates) - finalist_limit
                )
            for rank, candidate in enumerate(candidates):
                if rank >= finalist_limit:
                    break
                for registered_event in self._paper_tracker.register_many(
                    candidate,
                    quotes,
                    finalist_rank=rank,
                    decision_at_ms=observed_ms,
                    rejection_counts=admission_rejection_counts,
                ):
                    paper_events.append(
                        (
                            str(registered_event["kind"]),
                            dict(registered_event["payload"]),
                        )
                    )
            for settlement_event in self._paper_tracker.record_observed_public_funding_settlements(
                observed_ms,
                quotes,
            ):
                paper_events.append(
                    (
                        str(settlement_event["kind"]),
                        dict(settlement_event["payload"]),
                    )
                )
            for event in self._paper_tracker.evaluate_due(observed_ms, quotes):
                paper_events.append((str(event["kind"]), dict(event["payload"])))
            if paper_events:
                self._paper_journal.append_many(
                    paper_events,
                    ts_ms=observed_ms,
                )
                publish_paper_checkpoint(
                    self._paper_checkpoint_path,
                    self._paper_tracker.checkpoint(),
                )
        except Exception:
            self._paper_tracker.invalidate_replay()
            raise
        return {
            "status": "success",
            "event_count": len(paper_events),
            "last_success_at_ms": observed_ms,
            "admission_rejection_counts": admission_rejection_counts,
        }

    async def _fetch_quotes(
        self,
        observed_ms: int | None,
    ) -> tuple[
        dict[str, QuoteSnapshot],
        set[str],
        str,
        int,
        int,
        dict[str, list[str]],
        int,
    ]:
        """Read and filter the funding sidecar snapshot inside this process."""

        snapshot = load_snapshot(self.sidecar_snapshot_path)
        decision_at_ms = (
            int(observed_ms)
            if observed_ms is not None
            else int(time.time() * 1000)
        )
        configured_venues = {
            str(venue.venue or "").strip().lower()
            for venue in self.config.venues
            if str(venue.venue or "").strip()
        }
        if snapshot is None:
            return (
                {},
                configured_venues,
                "sidecar_snapshot_unavailable",
                0,
                0,
                {},
                decision_at_ms,
            )

        input_quote_count = len(snapshot.quotes)
        max_age_ms = min(
            int(self.config.runtime.sidecar_snapshot_max_age_ms),
            max(int(self.config.strategy.spread_signal_ttl_ms or 0), 1),
        )
        published_at_ms = int(snapshot.published_at_ms or 0)
        if (
            published_at_ms <= 0
            or published_at_ms > decision_at_ms
            or decision_at_ms - published_at_ms > max_age_ms
        ):
            return (
                {},
                configured_venues,
                "sidecar_snapshot_stale",
                input_quote_count,
                0,
                {},
                decision_at_ms,
            )

        if self._spread_sampling_selection_required:
            sampling_symbols = resolve_spread_sampling_symbols(
                self.config,
                snapshot.quotes,
                quote_eligible=lambda quote: bool(
                    quote.contract_normalization_complete is True
                    and _quote_is_valid_for_spread_sidecar(
                        quote,
                        observed_ms=decision_at_ms,
                        max_age_ms=max_age_ms,
                    )
                ),
            )
            if not sampling_symbols:
                return (
                    {},
                    configured_venues,
                    "sidecar_snapshot_universe_unavailable",
                    input_quote_count,
                    0,
                    {},
                    decision_at_ms,
                )
            if sampling_symbols != self._spread_sampling_symbols:
                self._spread_sampling_symbols = tuple(sampling_symbols)
                self.stats.retain_symbols(set(sampling_symbols))
        allowed_symbols = set(self._spread_sampling_symbols or self.config.symbols)

        degraded_venues = {
            str(venue).strip().lower()
            for venue in snapshot.degraded_venues
            if str(venue).strip()
        }
        degraded_symbol_sets = {
            str(venue).strip().lower(): {
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip()
            }
            for venue, symbols in snapshot.degraded_symbols.items()
            if isinstance(symbols, list)
        }
        quotes: dict[str, QuoteSnapshot] = {}
        for key, quote in snapshot.quotes.items():
            venue = str(quote.venue or "").strip().lower()
            symbol = str(quote.symbol or "").strip().upper()
            if symbol not in allowed_symbols:
                continue
            if (
                venue in degraded_venues
                or symbol in degraded_symbol_sets.get(venue, set())
            ):
                continue
            if not _quote_is_valid_for_spread_sidecar(
                quote,
                observed_ms=decision_at_ms,
                max_age_ms=max_age_ms,
            ):
                if venue and symbol:
                    degraded_symbol_sets.setdefault(venue, set()).add(symbol)
                continue
            quotes[str(key)] = quote

        # Re-apply symbol degradation after validation so insertion order
        # cannot leave one leg of a degraded venue/symbol pair executable.
        quotes = {
            key: quote
            for key, quote in quotes.items()
            if str(quote.symbol or "").strip().upper()
            not in degraded_symbol_sets.get(
                str(quote.venue or "").strip().lower(),
                set(),
            )
        }
        degraded_symbols = {
            venue: sorted(symbols)
            for venue, symbols in degraded_symbol_sets.items()
            if symbols
        }
        market_observed_at_ms = max(
            (int(quote.observed_at_ms or 0) for quote in quotes.values()),
            default=0,
        )
        source_mode = (
            "sidecar_snapshot"
            if len(quotes) == input_quote_count
            else "sidecar_snapshot_partial"
        )
        return (
            quotes,
            degraded_venues,
            source_mode,
            input_quote_count,
            market_observed_at_ms,
            degraded_symbols,
            decision_at_ms,
        )

    def _paper_config(
        self,
        config: AppConfig,
    ) -> SpreadPaperConfig:
        strategy = config.strategy
        slippage_bps = float(strategy.spread_paper_slippage_buffer_bps)
        if slippage_bps <= 0.0:
            slippage_bps = float(strategy.spread_slippage_reserve_bps)
        taker_fees = {
            str(venue.venue or "").lower(): max(float(venue.taker_fee_bps), 0.0)
            for venue in config.venues
            if str(venue.venue or "").strip()
        }
        return SpreadPaperConfig(
            enabled=strategy.spread_paper_enabled,
            finalist_limit=int(strategy.spread_paper_finalist_limit),
            min_decision_latency_ms=int(strategy.spread_paper_min_decision_latency_ms),
            markout_secs=list(strategy.spread_paper_markout_secs),
            terminal_secs=int(strategy.spread_paper_terminal_secs),
            exit_z=float(strategy.spread_exit_z),
            stop_z=float(strategy.spread_stop_z),
            max_hold_ms=int(strategy.spread_max_hold_ms),
            taker_fee_bps_by_venue=taker_fees,
            slippage_buffer_bps=slippage_bps,
            excluded_symbols=list(strategy.spread_paper_excluded_symbols),
            allowed_opportunity_labels=list(strategy.spread_paper_allowed_opportunity_labels),
            episode_cooldown_ms=int(strategy.spread_paper_episode_cooldown_ms),
            quote_ttl_ms=strategy.spread_signal_ttl_ms,
            quote_skew_ms=strategy.spread_quote_skew_ms,
            latency_buffer_bps=float(strategy.spread_paper_latency_buffer_bps),
            require_l2_vwap=strategy.spread_paper_require_l2_vwap,
        )
