"""Spread-reversion sidecar service."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import logging
import math
import os
from pathlib import Path
import tempfile
import time

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.persistence.journal import Journal
from lightfee.spread.models import SpreadSnapshot
from lightfee.spread.metadata_cache import (
    SpreadMetadataSnapshotCache,
    quote_cache_contract_eligible,
)
from lightfee.spread.quote_snapshot import (
    FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    HOT_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS,
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    load_spread_quote_snapshot,
    spread_quote_snapshot_path,
)
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
from lightfee.spread.universe import (
    SPREAD_SAMPLING_MAX_PAIR_COUNT,
    resolve_spread_sampling_symbols,
    spread_sampling_pair_bound,
    spread_sampling_selection_required,
)
from lightfee.strategy.fee_evidence import (
    FeeEvidenceBook,
    effective_fee_maps,
    load_fee_evidence,
)


logger = logging.getLogger("lightfee.spread.service")

_PAPER_JOURNAL_HEAD_VERSION = 1
_PAPER_JOURNAL_EPOCH_VERSION = 2
_PAPER_JOURNAL_GENESIS_PURPOSE = "spread_paper_genesis"
_PAPER_JOURNAL_EVENT_PURPOSE = "spread_paper_events"
_PAPER_JOURNAL_MIN_HARD_MAX_BYTES = 16_384
_STATS_CHECKPOINT_MIN_INTERVAL_MS = 30_000


def _paper_journal_head_path(journal_path: str | Path) -> Path:
    path = Path(journal_path)
    return path.with_name(f"{path.name}.head")


def _paper_rollback_anchor_path(
    journal_path: str | Path,
    configured_path: str | Path,
) -> Path | None:
    """Return a syntactically independent rollback anchor or fail closed.

    The operating environment is responsible for placing this absolute path
    on an independently retained volume.  Requiring a distinct directory
    prevents the prior implementation from silently keeping all three rollback
    witnesses beside the journal.
    """

    raw = str(configured_path or "").strip()
    if not raw:
        return None
    anchor = Path(raw)
    if not anchor.is_absolute():
        return None
    try:
        journal_parent = Path(journal_path).expanduser().resolve(strict=False).parent
        anchor_parent = anchor.expanduser().resolve(strict=False).parent
    except OSError:
        return None
    if anchor_parent == journal_parent:
        return None
    return anchor


def _load_paper_journal_head(path: Path) -> tuple[dict[str, object] | None, bool]:
    if not path.exists():
        return None, True
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, False
    if (
        not isinstance(raw, dict)
        or raw.get("head_version") != _PAPER_JOURNAL_HEAD_VERSION
        or not isinstance(raw.get("committed_batch"), dict)
    ):
        return None, False
    return dict(raw["committed_batch"]), True


def _publish_durable_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with open(temporary, "w", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
        directory_fd = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _publish_paper_journal_head(path: Path, envelope: dict[str, object]) -> None:
    _publish_durable_json(
        path,
        {
            "head_version": _PAPER_JOURNAL_HEAD_VERSION,
            "committed_batch": envelope,
        },
    )


def _load_paper_journal_epoch(path: Path) -> tuple[dict[str, object] | None, bool]:
    if not path.exists():
        return None, True
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, False
    if (
        not isinstance(raw, dict)
        or raw.get("epoch_version") != _PAPER_JOURNAL_EPOCH_VERSION
        or not isinstance(raw.get("genesis_batch"), dict)
        or not isinstance(raw.get("latest_committed_batch"), dict)
        or not isinstance(raw.get("generation"), int)
        or isinstance(raw.get("generation"), bool)
        or int(raw["generation"]) < 0
    ):
        return None, False
    return dict(raw), True


def _publish_paper_journal_epoch(
    path: Path,
    *,
    genesis: dict[str, object],
    latest: dict[str, object],
    generation: int,
) -> None:
    _publish_durable_json(
        path,
        {
            "epoch_version": _PAPER_JOURNAL_EPOCH_VERSION,
            "genesis_batch": genesis,
            "latest_committed_batch": latest,
            "generation": int(generation),
        },
    )


def _publish_paper_journal_checkpoint(
    path: Path,
    envelopes: list[dict[str, object]],
) -> None:
    if not envelopes:
        raise ValueError("paper journal checkpoint requires a genesis envelope")
    _publish_paper_journal_epoch(
        path,
        genesis=envelopes[0],
        latest=envelopes[-1],
        generation=len(envelopes) - 1,
    )


def _synchronize_paper_journal_head(
    journal: Journal,
    head_path: Path,
    epoch_path: Path,
    *,
    now_ms: int,
    has_legacy_state_records: bool,
) -> bool:
    head, head_valid = _load_paper_journal_head(head_path)
    epoch, epoch_valid = _load_paper_journal_epoch(epoch_path)
    if not head_valid or not epoch_valid or has_legacy_state_records:
        return False
    envelopes = journal.committed_batch_envelopes
    if epoch is None:
        if not envelopes and head is None:
            journal.append_committed_batch(
                [],
                ts_ms=now_ms,
                purpose=_PAPER_JOURNAL_GENESIS_PURPOSE,
            )
            envelope = journal.last_committed_batch_envelope
            assert envelope is not None
            _publish_paper_journal_head(head_path, envelope)
            _publish_paper_journal_checkpoint(
                epoch_path,
                journal.committed_batch_envelopes,
            )
            return True
        # Even a lone genesis can be a journal+head rollback after previously
        # committed paper state.  Missing independent state is therefore never
        # auto-promoted.  An initialization crash fails closed and requires an
        # explicit operator recovery instead of weakening rollback detection.
        return False
    epoch_genesis = epoch["genesis_batch"]
    epoch_latest = epoch["latest_committed_batch"]
    epoch_generation = int(epoch["generation"])
    if (
        not envelopes
        or envelopes[0] != epoch_genesis
        or epoch_generation >= len(envelopes)
        or envelopes[epoch_generation] != epoch_latest
    ):
        return False
    if (
        envelopes[0].get("purpose") != _PAPER_JOURNAL_GENESIS_PURPOSE
        or int(envelopes[0].get("event_count", -1)) != 0
        or any(
            envelope.get("purpose") != _PAPER_JOURNAL_EVENT_PURPOSE
            for envelope in envelopes[1:]
        )
    ):
        return False
    if head is None or head not in envelopes:
        return False
    head_index = envelopes.index(head)
    latest_index = len(envelopes) - 1
    if latest_index == epoch_generation and head_index == latest_index:
        return True
    if latest_index != epoch_generation + 1:
        return False
    latest = envelopes[-1]
    if head_index == epoch_generation:
        # Crash after the journal commit but before the external head.
        _publish_paper_journal_head(head_path, latest)
    elif head_index != latest_index:
        return False
    # Crash after the journal and head commit but before advancing the
    # independent rollback checkpoint.  Only one generation may be repaired.
    _publish_paper_journal_checkpoint(epoch_path, envelopes)
    return True


def _paper_journal_has_capacity(
    journal: Journal,
    events: list[tuple[str, dict]],
    *,
    hard_max_bytes: int,
) -> bool:
    maximum = max(int(hard_max_bytes or 0), 1)
    reserve = min(1_048_576, max(maximum // 10, 4_096))
    usable_limit = max(maximum - reserve, 1)
    try:
        current_size = journal.path.stat().st_size if journal.path.exists() else 0
    except OSError:
        return False
    estimated_batch_bytes = 4_096 + sum(
        len(
            json.dumps(
                {"kind": kind, "payload": payload},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        )
        + 512
        for kind, payload in events
    )
    return current_size + estimated_batch_bytes <= usable_limit


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
        self.spread_quote_snapshot_path = spread_quote_snapshot_path(
            self.sidecar_snapshot_path
        )
        self._spread_metadata_cache = SpreadMetadataSnapshotCache(
            self.sidecar_snapshot_path,
            max_age_ms=config.runtime.live_scan_last_good_max_age_ms,
        )
        self._spread_metadata_refresh_task: asyncio.Task[bool] | None = None
        self._spread_sampling_selection_required = spread_sampling_selection_required(
            config
        )
        self._spread_sampling_symbols = (
            ()
            if self._spread_sampling_selection_required
            else resolve_spread_sampling_symbols(
                config,
                self._spread_metadata_cache.quotes,
                quote_eligible=self._spread_metadata_cache.quote_eligible,
            )
        )
        self._spread_sampling_producer_generation_id = ""
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
        self._stats_checkpoint_persisted_revision = self.stats.revision
        self._stats_checkpoint_last_attempt_ms = 0
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
            # never rotates.  It is instead hard-bounded and fails paper
            # admission closed before the bound; normal log compaction settings
            # remain for non-stateful event logs.
            self._paper_journal = Journal(
                persistence.spread_paper_event_log_path,
            )
            self._paper_journal_head_path = _paper_journal_head_path(
                persistence.spread_paper_event_log_path
            )
            self._paper_journal_epoch_path = _paper_rollback_anchor_path(
                persistence.spread_paper_event_log_path,
                persistence.spread_paper_rollback_anchor_path,
            )
            self._paper_journal.open()
            paper_hard_max_bytes = int(
                persistence.spread_paper_event_log_hard_max_bytes
            )
            if self._paper_journal_epoch_path is None:
                logger.error(
                    "spread paper rollback anchor must be an absolute path in "
                    "a directory distinct from the journal; admission disabled"
                )
                paper_records, paper_journal_intact = [], False
            elif paper_hard_max_bytes < _PAPER_JOURNAL_MIN_HARD_MAX_BYTES:
                logger.error(
                    "spread paper journal hard capacity is below safe minimum; "
                    "admission disabled"
                )
                paper_records, paper_journal_intact = [], False
            else:
                paper_records, paper_journal_intact = (
                    self._paper_journal.read_committed_batches_with_integrity(
                        max_bytes=paper_hard_max_bytes,
                    )
                )
            if paper_journal_intact and not self._paper_journal.has_archives():
                try:
                    paper_journal_intact = _synchronize_paper_journal_head(
                        self._paper_journal,
                        self._paper_journal_head_path,
                        self._paper_journal_epoch_path,
                        now_ms=int(time.time() * 1000),
                        has_legacy_state_records=any(
                            str(record.get("kind", "") or "").startswith("opportunity.paper_")
                            for record in self._paper_journal.legacy_records
                        ),
                    )
                except OSError:
                    logger.exception("spread paper journal head synchronization failed")
                    paper_journal_intact = False
            if paper_journal_intact and not _paper_journal_has_capacity(
                self._paper_journal,
                [],
                hard_max_bytes=(
                    persistence.spread_paper_event_log_hard_max_bytes
                ),
            ):
                logger.error(
                    "spread paper journal capacity limit reached; admission disabled"
                )
                paper_journal_intact = False
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
        metadata_refresh_task = getattr(self, "_spread_metadata_refresh_task", None)
        close_cancelled = False
        if metadata_refresh_task is not None:
            try:
                await metadata_refresh_task
            except asyncio.CancelledError:
                current_task = asyncio.current_task()
                close_cancelled = bool(
                    current_task is not None and current_task.cancelling()
                )
            except Exception:
                logger.exception("final spread metadata refresh failed")
            finally:
                self._spread_metadata_refresh_task = None
        if hasattr(self, "stats"):
            try:
                self._checkpoint_stats_if_due(int(time.time() * 1000), force=True)
            except OSError:
                logger.exception("final spread stats checkpoint publish failed")
        if self._paper_journal is not None:
            try:
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
        self._refresh_fee_evidence(decision_at_ms)
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
        try:
            self._checkpoint_stats_if_due(decision_at_ms)
        except OSError:
            # The next process starts cold if this fails, which is safe because
            # `SpreadStatsTracker` refuses entries until it has fresh samples.
            logger.exception(
                "spread stats checkpoint publish failed; next restart will cold-start",
                extra={"checkpoint_path": self.stats_checkpoint_path},
            )
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
        publish_spread_stats_checkpoint(
            self.stats,
            self.stats_checkpoint_path,
            model_epoch=self.signal_config.model_epoch,
            now_ms=observed_ms,
        )
        self._stats_checkpoint_persisted_revision = revision
        return True

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
                if not _paper_journal_has_capacity(
                    self._paper_journal,
                    paper_events,
                    hard_max_bytes=(
                        self.config.persistence.spread_paper_event_log_hard_max_bytes
                    ),
                ):
                    raise OSError(
                        "spread paper journal hard capacity limit reached"
                    )
                self._paper_journal.append_committed_batch(
                    paper_events,
                    ts_ms=observed_ms,
                    purpose=_PAPER_JOURNAL_EVENT_PURPOSE,
                )
                envelope = self._paper_journal.last_committed_batch_envelope
                assert envelope is not None
                _publish_paper_journal_head(
                    self._paper_journal_head_path,
                    envelope,
                )
                _publish_paper_journal_checkpoint(
                    self._paper_journal_epoch_path,
                    self._paper_journal.committed_batch_envelopes,
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
        compact_path = Path(self.spread_quote_snapshot_path)
        using_compact_snapshot = compact_path.exists()
        if using_compact_snapshot:
            # Once the producer advertises the compact contract, a malformed
            # file is an integrity failure.  Do not silently bypass it with
            # the slower full snapshot.
            snapshot = load_spread_quote_snapshot(compact_path)
        else:
            # Rolling-deploy and direct-test compatibility only.  Production
            # naturally switches on the first compact Sidecar publication.
            snapshot = load_snapshot(self.sidecar_snapshot_path)
        snapshot_input_quote_count = len(snapshot.quotes) if snapshot is not None else 0
        if (
            using_compact_snapshot
            and snapshot is not None
            and snapshot.schema_version in HOT_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS
        ):
            # A full metadata generation contains the global funding universe
            # and takes hundreds of milliseconds to validate on the production
            # VM.  It is an independent slow lane: awaiting it here ages the
            # already-received BBO before the decision clock is even captured.
            # The cache constructor establishes the initial last-good value;
            # later generations refresh in one non-overlapping background task
            # and become visible atomically on a subsequent hot cycle.
            self._schedule_spread_metadata_refresh()
        decision_at_ms = (
            int(observed_ms)
            if observed_ms is not None
            else int(time.time() * 1000)
        )
        pending_sampling_symbols = self._spread_sampling_symbols
        pending_producer_generation = self._spread_sampling_producer_generation_id
        if snapshot is not None and snapshot.schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
            configured_symbols = {
                str(symbol).strip().upper()
                for symbol in self.config.symbols
                if str(symbol).strip()
            }
            producer_symbols = tuple(snapshot.sampling_symbols)
            producer_generation = str(snapshot.producer_generation_id or "")
            producer_pair_bound = spread_sampling_pair_bound(
                producer_symbols,
                (venue.venue for venue in self.config.venues),
            )
            producer_universe_valid = bool(
                producer_symbols
                and set(producer_symbols) <= configured_symbols
                and producer_generation
                and producer_pair_bound <= SPREAD_SAMPLING_MAX_PAIR_COUNT
            )
            if not producer_universe_valid:
                snapshot = None
            elif self._spread_sampling_selection_required:
                prior_generation = self._spread_sampling_producer_generation_id
                if (
                    pending_sampling_symbols
                    and producer_symbols != pending_sampling_symbols
                    and prior_generation == producer_generation
                ):
                    # One producer generation promises an immutable universe.
                    # A contradictory snapshot is a corrupted process boundary.
                    snapshot = None
                else:
                    pending_sampling_symbols = producer_symbols
                    pending_producer_generation = producer_generation
            elif producer_symbols != pending_sampling_symbols:
                # When the configured universe already fits the hard budget,
                # both processes must carry it exactly. A rolling/config
                # mismatch is not safe evidence for partial sampling.
                snapshot = None
            else:
                pending_producer_generation = producer_generation
        if self._spread_sampling_selection_required and not pending_sampling_symbols:
            if snapshot is None or snapshot.schema_version != SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
                # Schema-v4/full-snapshot rolling compatibility has no
                # producer-declared universe. Resolve locally and remain
                # fail-closed if metadata is absent or stale.
                if (
                    snapshot is not None
                    and snapshot.schema_version == FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION
                ):
                    metadata_quotes = snapshot.quotes

                    def metadata_quote_eligible(quote: QuoteSnapshot) -> bool:
                        observed_at_ms = int(quote.observed_at_ms or 0)
                        return bool(
                            quote_cache_contract_eligible(quote)
                            and 0 < observed_at_ms <= decision_at_ms
                            and decision_at_ms - observed_at_ms
                            <= self.config.runtime.live_scan_last_good_max_age_ms
                        )
                else:
                    metadata_generation = self._spread_metadata_cache.generation
                    metadata_quotes = metadata_generation.quotes

                    def metadata_quote_eligible(quote: QuoteSnapshot) -> bool:
                        return self._spread_metadata_cache.quote_eligible(
                            quote,
                            now_ms=decision_at_ms,
                            generation=metadata_generation,
                        )
                pending_sampling_symbols = resolve_spread_sampling_symbols(
                    self.config,
                    metadata_quotes,
                    quote_eligible=metadata_quote_eligible,
                )
        if self._spread_sampling_selection_required:
            allowed_symbols = set(pending_sampling_symbols)
            if not allowed_symbols:
                snapshot = None
            elif snapshot is not None:
                snapshot = replace(
                    snapshot,
                    quotes={
                        key: quote
                        for key, quote in snapshot.quotes.items()
                        if str(quote.symbol or "").strip().upper() in allowed_symbols
                    },
                    degraded_symbols={
                        venue: [
                            symbol
                            for symbol in symbols
                            if str(symbol).strip().upper() in allowed_symbols
                        ]
                        for venue, symbols in snapshot.degraded_symbols.items()
                        if isinstance(symbols, list)
                    },
                )
            snapshot_input_quote_count = len(snapshot.quotes) if snapshot is not None else 0
        if (
            using_compact_snapshot
            and snapshot is not None
            and snapshot.schema_version in HOT_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSIONS
        ):
            merged_quotes, metadata_unavailable = self._spread_metadata_cache.overlay_hot_quotes(
                snapshot.quotes,
                now_ms=decision_at_ms,
            )
            merged_degraded_symbols = {
                str(venue).lower(): set(symbols)
                for venue, symbols in snapshot.degraded_symbols.items()
            }
            for venue, symbols in metadata_unavailable.items():
                merged_degraded_symbols.setdefault(venue, set()).update(symbols)
            snapshot = replace(
                snapshot,
                quotes=merged_quotes,
                degraded_symbols={
                    venue: sorted(symbols)
                    for venue, symbols in merged_degraded_symbols.items()
                    if symbols
                },
            )
        # In production the decision watermark is taken after the immutable
        # snapshot has been read and validated.  Otherwise a concurrent
        # sidecar publish can be newer than a timestamp captured before I/O,
        # making current evidence look future/stale.  Explicit test/replay
        # clocks remain authoritative and never advance from wall time.
        configured_venues = {
            str(vc.venue or "").lower() for vc in self.config.venues if str(vc.venue or "").strip()
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
                snapshot_input_quote_count,
                0,
                {},
                decision_at_ms,
            )

        quotes: dict[str, QuoteSnapshot] = {}
        degraded_venues = {str(venue).lower() for venue in snapshot.degraded_venues if str(venue)}
        degraded_symbol_sets: dict[str, set[str]] = {
            str(venue).lower(): {str(symbol).upper() for symbol in symbols if str(symbol)}
            for venue, symbols in snapshot.degraded_symbols.items()
            if isinstance(symbols, list)
        }
        source_declared_degradation = bool(snapshot.degraded_venues or degraded_symbol_sets)
        invalid_quote_keys: set[str] = set()
        for key, quote in snapshot.quotes.items():
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            if venue in degraded_venues or symbol in degraded_symbol_sets.get(venue, set()):
                invalid_quote_keys.add(str(key))
                continue
            if not _quote_is_valid_for_spread_sidecar(
                quote,
                observed_ms=decision_at_ms,
                max_age_ms=max_age_ms,
            ):
                invalid_quote_keys.add(str(key))
                if venue and symbol:
                    degraded_symbol_sets.setdefault(venue, set()).add(symbol)

        # Apply the final degradation set in a second pass.  A one-pass filter
        # is insertion-order dependent: a fresh quote encountered before a
        # stale sibling can survive even after its (venue, symbol) is marked
        # degraded.  Per-symbol degradation also avoids discarding unrelated
        # fresh instruments from the same otherwise healthy venue.
        for key, quote in snapshot.quotes.items():
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            if (
                str(key) in invalid_quote_keys
                or venue in degraded_venues
                or symbol in degraded_symbol_sets.get(venue, set())
            ):
                continue
            quotes[str(key)] = quote

        degraded_symbols = {
            venue: sorted(symbols) for venue, symbols in degraded_symbol_sets.items() if symbols
        }
        dropped_count = snapshot_input_quote_count - len(quotes)

        if dropped_count and not quotes:
            if not degraded_venues:
                degraded_venues.update(configured_venues)
            source_mode = (
                "sidecar_snapshot_degraded"
                if source_declared_degradation
                else "sidecar_snapshot_quotes_stale"
            )
            return (
                {},
                degraded_venues,
                source_mode,
                snapshot_input_quote_count,
                0,
                degraded_symbols,
                decision_at_ms,
            )

        source_mode = "sidecar_snapshot_partial" if dropped_count else "sidecar_snapshot"
        market_observed_at_ms = max(
            (int(quote.observed_at_ms or 0) for quote in quotes.values()),
            default=0,
        )
        if self._spread_sampling_selection_required:
            if pending_sampling_symbols != self._spread_sampling_symbols:
                self._spread_sampling_symbols = pending_sampling_symbols
                self.stats.retain_symbols(set(pending_sampling_symbols))
            self._spread_sampling_producer_generation_id = pending_producer_generation
        elif pending_producer_generation:
            self._spread_sampling_producer_generation_id = pending_producer_generation
        return (
            quotes,
            degraded_venues,
            source_mode,
            snapshot_input_quote_count,
            market_observed_at_ms,
            degraded_symbols,
            decision_at_ms,
        )

    def _schedule_spread_metadata_refresh(self) -> None:
        """Refresh slow metadata without putting full validation on the BBO clock."""

        task = self._spread_metadata_refresh_task
        if task is not None:
            if not task.done():
                return
            try:
                task.result()
            except asyncio.CancelledError:
                # Cancellation belongs to the retired metadata task, not the
                # caller's quote-decision task.  Drop it and schedule a fresh
                # attempt without poisoning the hot path.
                pass
            except Exception:
                # Keep the already validated last-good generation. Eligibility
                # still expires against its own evidence watermark.
                logger.exception("spread metadata background refresh failed")
            self._spread_metadata_refresh_task = None
        self._spread_metadata_refresh_task = asyncio.create_task(
            asyncio.to_thread(self._spread_metadata_cache.refresh)
        )

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
            manifest = load_spread_research_manifest(strategy.spread_paper_research_manifest_path)
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
            allow_verified_maker_rebates=(strategy.spread_allow_verified_maker_rebates is True),
        )
        return SpreadPaperConfig(
            enabled=strategy.spread_paper_enabled,
            finalist_limit=int(strategy.spread_paper_finalist_limit),
            min_decision_latency_ms=int(strategy.spread_paper_min_decision_latency_ms),
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
            require_account_fee_evidence=(strategy.spread_paper_require_account_fee_evidence),
            account_fee_evidence=fee_evidence,
            fee_evidence_account_identity_hashes=dict(
                config.runtime.fee_evidence_account_identity_hashes
            ),
            allow_verified_maker_rebates=(strategy.spread_allow_verified_maker_rebates is True),
            oos_start_ms=int(strategy.spread_paper_oos_start_ms),
            require_out_of_sample=strategy.spread_paper_require_out_of_sample,
            research_manifest=manifest,
        )
