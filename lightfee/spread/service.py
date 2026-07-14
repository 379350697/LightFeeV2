"""Spread-reversion sidecar service."""

from __future__ import annotations

import logging
import math
import time

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.persistence.journal import Journal
from lightfee.spread.models import SpreadSnapshot
from lightfee.spread.paper import SpreadPaperConfig, SpreadPaperTracker
from lightfee.spread.publisher import publish_spread_snapshot
from lightfee.spread.research_manifest import (
    DEFAULT_SPREAD_RESEARCH_MANIFEST,
    load_spread_research_manifest,
)
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadSignalEngine,
    SpreadStatsTracker,
)
from lightfee.spread.stats_checkpoint import (
    publish_spread_stats_checkpoint,
    restore_spread_stats_checkpoint,
)
from lightfee.strategy.fee_evidence import (
    FeeEvidenceBook,
    effective_fee_maps,
    load_fee_evidence,
)


logger = logging.getLogger("lightfee.spread.service")


def _paper_quote_key(venue: str, symbol: str) -> str:
    return f"{str(venue).lower()}:{str(symbol).upper()}"


def _venue_maker_fee_bps(venue_config: VenueConfig) -> float:
    maker_fee = venue_config.maker_fee_bps
    if maker_fee is None:
        maker_fee = venue_config.taker_fee_bps
    return float(maker_fee or 0.0)


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
        source_mode = str(config.runtime.spread_sidecar_source_mode).lower()
        if source_mode != "sidecar_snapshot" or config.runtime.spread_sidecar_direct_fetch_enabled:
            raise ValueError(
                "spread sidecar must use runtime.sidecar_snapshot_path; "
                "direct public market fetching is not supported"
            )
        self._fee_evidence = self._load_fee_evidence(int(time.time() * 1000))
        self.signal_config = SpreadReversionConfig.from_app_config(
            config,
            fee_evidence=self._fee_evidence,
        )
        self.stats = SpreadStatsTracker()
        self.signal_engine = SpreadSignalEngine(
            tracker=self.stats,
            config=self.signal_config,
        )
        self.stats_checkpoint_path = config.runtime.spread_stats_checkpoint_path
        self._stats_checkpoint_loaded = False
        self._stats_checkpoint_restored = False
        self._paper_journal: Journal | None = None
        self._paper_tracker = SpreadPaperTracker(
            self._paper_config(config, fee_evidence=self._fee_evidence)
        )
        if self._paper_tracker.enabled:
            persistence = config.persistence
            # A paper registration can remain stateful until its terminal
            # horizon.  Generic log rotation can prune that registration while
            # keeping a later fill/close row, making safe state reconstruction
            # mathematically impossible.  This dedicated journal therefore
            # never rotates; normal log compaction settings remain for the
            # non-stateful event logs.
            self._paper_journal = Journal(
                persistence.spread_paper_event_log_path,
            )
            self._paper_journal.open()
            paper_records, paper_journal_intact = (
                self._paper_journal.read_all_with_integrity()
            )
            if paper_journal_intact and not self._paper_journal.has_archives():
                self._paper_tracker.restore_from_records(
                    paper_records,
                    require_journal_envelope=True,
                )
            else:
                # A missing close/fill row is indistinguishable from an active
                # paper entry after restart.  Rotated segments are equally
                # unsafe: their oldest predecessor may already be pruned.
                # Feed an explicit invalid replay boundary to disable paper
                # admission instead of reviving a partial episode.
                self._paper_tracker.invalidate_replay()

    async def close(self) -> None:
        if self._paper_journal is not None:
            try:
                self._paper_journal.close()
            except Exception:
                logger.exception(
                    "spread sidecar journal close failed; continuing resource cleanup"
                )
            finally:
                self._paper_journal = None

    async def refresh_once(self, *, now_ms: int | None = None) -> SpreadSnapshot:
        observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        self._refresh_fee_evidence(observed_ms)
        self._restore_stats_checkpoint_once(observed_ms)
        quotes, degraded_venues, source_mode = await self._fetch_quotes(observed_ms)
        rejection_counts: dict[str, int] = {}
        candidates = self.signal_engine.build(
            quotes,
            list(self.config.symbols),
            now_ms=observed_ms,
            rejection_counts=rejection_counts,
        )
        snapshot = SpreadSnapshot(
            published_at_ms=observed_ms,
            market_observed_at_ms=observed_ms,
            snapshot_path=str(self.snapshot_path),
            source_mode=source_mode,
            degraded_venues=sorted(degraded_venues),
            rejection_counts=rejection_counts,
            candidates=candidates,
        )
        publish_spread_snapshot(snapshot, self.snapshot_path)
        try:
            publish_spread_stats_checkpoint(
                self.stats,
                self.stats_checkpoint_path,
                model_epoch=self.signal_config.model_epoch,
                now_ms=observed_ms,
            )
        except OSError:
            # The next process starts cold if this fails, which is safe because
            # `SpreadStatsTracker` refuses entries until it has fresh samples.
            logger.exception(
                "spread stats checkpoint publish failed; next restart will cold-start",
                extra={"checkpoint_path": self.stats_checkpoint_path},
            )
        await self._refresh_paper(candidates, quotes, observed_ms)
        return snapshot

    def _restore_stats_checkpoint_once(self, now_ms: int) -> None:
        if self._stats_checkpoint_loaded:
            return
        self._stats_checkpoint_loaded = True
        self._stats_checkpoint_restored = restore_spread_stats_checkpoint(
            self.stats,
            self.stats_checkpoint_path,
            model_epoch=self.signal_config.model_epoch,
            now_ms=now_ms,
        )

    async def _refresh_paper(
        self,
        candidates: list,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> None:
        if self._paper_journal is None or not self._paper_tracker.enabled:
            return
        paper_events: list[tuple[str, dict]] = []
        for rank, candidate in enumerate(candidates):
            if rank >= self._paper_tracker.config.finalist_limit:
                break
            for registered_event in self._paper_tracker.register_many(
                candidate,
                quotes,
                finalist_rank=rank,
                decision_at_ms=observed_ms,
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
        self._paper_journal.append_many(paper_events, ts_ms=observed_ms)

    async def _fetch_quotes(
        self,
        observed_ms: int,
    ) -> tuple[dict[str, QuoteSnapshot], set[str], str]:
        snapshot = load_snapshot(self.sidecar_snapshot_path)
        configured_venues = {
            str(vc.venue or "").lower()
            for vc in self.config.venues
            if str(vc.venue or "").strip()
        }
        if snapshot is None:
            return {}, configured_venues, "sidecar_snapshot_unavailable"

        max_age_ms = int(self.config.runtime.sidecar_snapshot_max_age_ms)
        published_at_ms = int(snapshot.published_at_ms or 0)
        market_observed_at_ms = int(snapshot.market_observed_at_ms or 0)
        if (
            published_at_ms <= 0
            or published_at_ms > observed_ms
            or observed_ms - published_at_ms > max_age_ms
            or market_observed_at_ms <= 0
            or market_observed_at_ms > observed_ms
            or observed_ms - market_observed_at_ms > max_age_ms
        ):
            return {}, configured_venues, "sidecar_snapshot_stale"

        quotes: dict[str, QuoteSnapshot] = {}
        degraded_venues = {
            str(venue).lower()
            for venue in snapshot.degraded_venues
            if str(venue)
        }
        dropped_count = 0
        for key, quote in snapshot.quotes.items():
            if _quote_is_valid_for_spread_sidecar(
                quote,
                observed_ms=observed_ms,
                max_age_ms=max_age_ms,
            ):
                quotes[str(key)] = quote
                continue
            dropped_count += 1
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            if venue:
                degraded_venues.add(venue)

        if dropped_count and not quotes:
            if not degraded_venues:
                degraded_venues.update(configured_venues)
            return {}, degraded_venues, "sidecar_snapshot_quotes_stale"

        source_mode = "sidecar_snapshot_partial" if dropped_count else "sidecar_snapshot"
        return quotes, degraded_venues, source_mode

    def _load_fee_evidence(self, now_ms: int) -> FeeEvidenceBook:
        return load_fee_evidence(
            self.config.runtime.fee_evidence_path,
            now_ms=now_ms,
            max_age_ms=int(self.config.runtime.fee_evidence_max_age_ms),
        )

    def _refresh_fee_evidence(self, now_ms: int) -> None:
        evidence = self._load_fee_evidence(now_ms)
        self._fee_evidence = evidence
        self.signal_config = SpreadReversionConfig.from_app_config(
            self.config,
            fee_evidence=evidence,
        )
        self.signal_engine.reconfigure(self.signal_config)
        # Open paper positions retain their per-leg fee snapshots; only later
        # registrations consume a refreshed account fee schedule.
        self._paper_tracker.config = self._paper_config(
            self.config,
            fee_evidence=evidence,
        )

    def _paper_config(
        self,
        config: AppConfig,
        *,
        fee_evidence: FeeEvidenceBook | None = None,
    ) -> SpreadPaperConfig:
        strategy = config.strategy
        manifest = DEFAULT_SPREAD_RESEARCH_MANIFEST
        if strategy.spread_paper_enabled:
            manifest = load_spread_research_manifest(
                strategy.spread_paper_research_manifest_path
            )
            if manifest.model_epoch != strategy.spread_paper_model_epoch:
                raise ValueError(
                    "spread research manifest model_epoch must match "
                    "strategy.spread_paper_model_epoch"
                )
        slippage_bps = float(strategy.spread_paper_slippage_buffer_bps)
        if slippage_bps <= 0.0:
            slippage_bps = float(strategy.spread_slippage_reserve_bps)
        configured_taker = {
            str(venue.venue or "").lower(): float(venue.taker_fee_bps)
            for venue in config.venues
            if str(venue.venue or "").strip()
        }
        configured_maker = {
            str(venue.venue or "").lower(): _venue_maker_fee_bps(venue)
            for venue in config.venues
            if str(venue.venue or "").strip()
        }
        taker_fees, maker_fees = effective_fee_maps(
            configured_taker,
            configured_maker,
            fee_evidence,
            allow_verified_maker_rebates=(
                strategy.spread_allow_verified_maker_rebates is True
            ),
        )
        return SpreadPaperConfig(
            enabled=strategy.spread_paper_enabled,
            finalist_limit=int(strategy.spread_paper_finalist_limit),
            min_decision_latency_ms=int(
                strategy.spread_paper_min_decision_latency_ms
            ),
            markout_secs=list(strategy.spread_paper_markout_secs),
            terminal_secs=int(strategy.spread_paper_terminal_secs),
            active_exit_enabled=True,
            exit_z=float(strategy.spread_exit_z),
            stop_z=float(strategy.spread_stop_z),
            max_hold_ms=int(strategy.spread_max_hold_ms),
            taker_fee_bps_by_venue=taker_fees,
            maker_fee_bps_by_venue=maker_fees,
            slippage_buffer_bps=slippage_bps,
            excluded_symbols=list(strategy.spread_paper_excluded_symbols),
            allowed_opportunity_labels=list(strategy.spread_paper_allowed_opportunity_labels),
            episode_cooldown_ms=int(strategy.spread_paper_episode_cooldown_ms),
            paper_bot_ids=list(strategy.spread_paper_bot_ids),
            model_epoch=strategy.spread_paper_model_epoch,
            primary_fill_model=strategy.spread_paper_primary_fill_model,
            require_taker_taker=strategy.spread_paper_require_taker_taker,
            quote_ttl_ms=strategy.spread_signal_ttl_ms,
            quote_skew_ms=strategy.spread_quote_skew_ms,
            latency_buffer_bps=float(strategy.spread_paper_latency_buffer_bps),
            require_l2_vwap=strategy.spread_paper_require_l2_vwap,
            require_account_fee_evidence=(
                strategy.spread_paper_require_account_fee_evidence
            ),
            account_fee_evidence=fee_evidence,
            fee_evidence_account_identity_hashes=dict(
                config.runtime.fee_evidence_account_identity_hashes
            ),
            allow_verified_maker_rebates=(
                strategy.spread_allow_verified_maker_rebates is True
            ),
            oos_start_ms=int(strategy.spread_paper_oos_start_ms),
            require_out_of_sample=strategy.spread_paper_require_out_of_sample,
            research_manifest=manifest,
        )
