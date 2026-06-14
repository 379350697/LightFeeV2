"""Entry gate runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change entry selection, admission, or no-entry diagnostic semantics while extracting it.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import AccountBalanceSnapshot, Venue
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.runtime_context import EntryGateRuntimeContext
from lightfee.risk.modes import EngineLifecycle


class EntryGateRuntime:
    def __init__(self, ctx: EntryGateRuntimeContext) -> None:
        self.ctx = ctx

    @property
    def recovery_ledger(self):
        return getattr(self.ctx, "recovery_ledger", None)

    @property
    def _tracked_primary_pair_ids(self) -> set[str]:
        return self.ctx._tracked_primary_pair_ids

    @_tracked_primary_pair_ids.setter
    def _tracked_primary_pair_ids(self, value: set[str]) -> None:
        self.ctx._tracked_primary_pair_ids = value

    @property
    def _symbol_admission_blocked_until_ms(self) -> dict[tuple[str, str], int]:
        return self.ctx._symbol_admission_blocked_until_ms

    @property
    def _venue_cooldown_until_ms(self):
        return self.ctx._venue_cooldown_until_ms

    @property
    def _zero_fill_cooldown_until_ms(self):
        return self.ctx._zero_fill_cooldown_until_ms

    @property
    def _post_only_reject_cooldown_until_ms(self):
        return self.ctx._post_only_reject_cooldown_until_ms

    @property
    def _last_candidate_catalog_filter_blockers(self):
        return self.ctx._last_candidate_catalog_filter_blockers

    @_last_candidate_catalog_filter_blockers.setter
    def _last_candidate_catalog_filter_blockers(self, value) -> None:
        self.ctx._last_candidate_catalog_filter_blockers = value

    @property
    def _last_candidate_catalog_filter_samples(self):
        return self.ctx._last_candidate_catalog_filter_samples

    @_last_candidate_catalog_filter_samples.setter
    def _last_candidate_catalog_filter_samples(self, value) -> None:
        self.ctx._last_candidate_catalog_filter_samples = value

    @property
    def _last_entry_admission_filter_blockers(self):
        return self.ctx._last_entry_admission_filter_blockers

    @_last_entry_admission_filter_blockers.setter
    def _last_entry_admission_filter_blockers(self, value) -> None:
        self.ctx._last_entry_admission_filter_blockers = value

    @property
    def _last_entry_admission_filter_samples(self):
        return self.ctx._last_entry_admission_filter_samples

    @_last_entry_admission_filter_samples.setter
    def _last_entry_admission_filter_samples(self, value) -> None:
        self.ctx._last_entry_admission_filter_samples = value

    @property
    def _last_snapshot_freshness_filter_blockers(self):
        return self.ctx._last_snapshot_freshness_filter_blockers

    @_last_snapshot_freshness_filter_blockers.setter
    def _last_snapshot_freshness_filter_blockers(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_blockers = value

    @property
    def _last_snapshot_freshness_filter_samples(self):
        return self.ctx._last_snapshot_freshness_filter_samples

    @_last_snapshot_freshness_filter_samples.setter
    def _last_snapshot_freshness_filter_samples(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_samples = value

    @property
    def _last_no_entry_diag_fingerprint(self) -> str:
        return self.ctx._last_no_entry_diag_fingerprint

    @_last_no_entry_diag_fingerprint.setter
    def _last_no_entry_diag_fingerprint(self, value: str) -> None:
        self.ctx._last_no_entry_diag_fingerprint = value

    @property
    def _last_no_entry_diag_ts_ms(self) -> int:
        return self.ctx._last_no_entry_diag_ts_ms

    @_last_no_entry_diag_ts_ms.setter
    def _last_no_entry_diag_ts_ms(self, value: int) -> None:
        self.ctx._last_no_entry_diag_ts_ms = value

    @property
    def _last_no_entry_full_diag_ts_ms(self) -> int:
        return self.ctx._last_no_entry_full_diag_ts_ms

    @_last_no_entry_full_diag_ts_ms.setter
    def _last_no_entry_full_diag_ts_ms(self, value: int) -> None:
        self.ctx._last_no_entry_full_diag_ts_ms = value

    @property
    def _last_no_entry_full_diag_reason(self) -> str:
        return self.ctx._last_no_entry_full_diag_reason

    @_last_no_entry_full_diag_reason.setter
    def _last_no_entry_full_diag_reason(self, value: str) -> None:
        self.ctx._last_no_entry_full_diag_reason = value

    @property
    def _last_no_entry_summary_fingerprint(self) -> str:
        return self.ctx._last_no_entry_summary_fingerprint

    @_last_no_entry_summary_fingerprint.setter
    def _last_no_entry_summary_fingerprint(self, value: str) -> None:
        self.ctx._last_no_entry_summary_fingerprint = value

    @property
    def _no_entry_suppressed_full_payload_count(self) -> int:
        return self.ctx._no_entry_suppressed_full_payload_count

    @_no_entry_suppressed_full_payload_count.setter
    def _no_entry_suppressed_full_payload_count(self, value: int) -> None:
        self.ctx._no_entry_suppressed_full_payload_count = value

    @property
    def _last_no_entry_diagnostics(self):
        return self.ctx._last_no_entry_diagnostics

    @_last_no_entry_diagnostics.setter
    def _last_no_entry_diagnostics(self, value) -> None:
        self.ctx._last_no_entry_diagnostics = value

    @property
    def _SYMBOL_ADMISSION_BLOCK_TTL_MS(self) -> int:
        return self.ctx._SYMBOL_ADMISSION_BLOCK_TTL_MS

    @property
    def _ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS(self) -> int:
        return self.ctx._ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS

    @property
    def _NO_ENTRY_DIAGNOSTICS_COMPACT_INTERVAL_MS(self) -> int:
        return self.ctx._NO_ENTRY_DIAGNOSTICS_COMPACT_INTERVAL_MS

    @property
    def _NO_ENTRY_DIAGNOSTICS_FULL_INTERVAL_MS(self) -> int:
        return self.ctx._NO_ENTRY_DIAGNOSTICS_FULL_INTERVAL_MS

    @property
    def _ENTRY_BLOCKED_LOCAL_L2_SELECTION_LOG_INTERVAL_MS(self) -> int:
        return self.ctx._ENTRY_BLOCKED_LOCAL_L2_SELECTION_LOG_INTERVAL_MS

    @property
    def _CANDIDATE_SYMBOL_SKIPPED_LOG_INTERVAL_MS(self) -> int:
        return self.ctx._CANDIDATE_SYMBOL_SKIPPED_LOG_INTERVAL_MS

    @property
    def _V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS(self):
        return self.ctx._V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None:
        return self.ctx.get_venue_adapter(venue)

    def _append_runtime_diagnostic_event(self, *args: Any, **kwargs: Any):
        return self.ctx._append_runtime_diagnostic_event(*args, **kwargs)

    async def _filter_symbols_supported_by_venue(self, *args: Any, **kwargs: Any):
        return await self.ctx._filter_symbols_supported_by_venue(*args, **kwargs)

    def _candidate_pair_id(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_pair_id(*args, **kwargs)

    def _candidate_uses_venue(self, *args: Any, **kwargs: Any):
        return self.ctx._candidate_uses_venue(*args, **kwargs)

    def _cached_entry_balance_snapshot(self, *args: Any, **kwargs: Any):
        return self.ctx._cached_entry_balance_snapshot(*args, **kwargs)

    def _store_entry_balance_snapshot(self, *args: Any, **kwargs: Any) -> None:
        return self.ctx._store_entry_balance_snapshot(*args, **kwargs)

    def _entry_admission_evidence(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_admission_evidence(*args, **kwargs)

    def _entry_admission_margin_buffer_bps(self) -> float:
        return self.ctx._entry_admission_margin_buffer_bps()

    def _hyperliquid_entry_required_initial_margin_quote(self, *args: Any, **kwargs: Any):
        return self.ctx._hyperliquid_entry_required_initial_margin_quote(*args, **kwargs)

    def _record_symbol_admission_block(self, *args: Any, **kwargs: Any):
        return self.ctx._record_symbol_admission_block(*args, **kwargs)

    def _v1_lifecycle_entry_gate_decision(self):
        return self.ctx._v1_lifecycle_entry_gate_decision()

    def _v1_lifecycle_closure_blocks_candidate(self, *args: Any, **kwargs: Any):
        return self.ctx._v1_lifecycle_closure_blocks_candidate(*args, **kwargs)

    def _v1_lifecycle_runtime_gate_reason(self, *args: Any, **kwargs: Any):
        return self.ctx._v1_lifecycle_runtime_gate_reason(*args, **kwargs)

    def _snapshot_quote_observed_at_ms(self, *args: Any, **kwargs: Any):
        return self.ctx._snapshot_quote_observed_at_ms(*args, **kwargs)

    def _snapshot_domain_budget_ms(self, *args: Any, **kwargs: Any):
        return self.ctx._snapshot_domain_budget_ms(*args, **kwargs)

    def _entry_l2_readiness_diagnostics_payload(self):
        return self.ctx._entry_l2_readiness_diagnostics_payload()

    def _entry_readiness_provider_uses_local_l2(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_local_l2()

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_ws_bbo()

    def _local_l2_effective_enabled(self) -> bool:
        return self.ctx._local_l2_effective_enabled()

    def _entry_readiness_provider_name(self) -> str:
        return self.ctx._entry_readiness_provider_name()

    def _entry_ws_bbo_subscription_blocker(self, *args: Any, **kwargs: Any):
        return self.ctx._entry_ws_bbo_subscription_blocker(*args, **kwargs)

    def _ws_bbo_selection_blocker_family(self, *args: Any, **kwargs: Any):
        return self.ctx._ws_bbo_selection_blocker_family(*args, **kwargs)

    def _market_quote_lookup(self, *args: Any, **kwargs: Any):
        return self.ctx._market_quote_lookup(*args, **kwargs)

    def _v1_tradeable_no_entry_reason(self, *args: Any, **kwargs: Any):
        return self.ctx._v1_tradeable_no_entry_reason(*args, **kwargs)

    def _no_tradeable_reason_from_candidate_blockers(self, *args: Any, **kwargs: Any):
        return self.ctx._no_tradeable_reason_from_candidate_blockers(*args, **kwargs)

    def _payload_fingerprint(self, *args: Any, **kwargs: Any):
        return self.ctx._payload_fingerprint(*args, **kwargs)

    def _entry_local_l2_stale_after_ms(self) -> int:
        return self.ctx._entry_local_l2_stale_after_ms()

    async def _fetch_hyperliquid_entry_balance_snapshot(
        self,
        now_ms: int,
    ) -> tuple[AccountBalanceSnapshot | None, str | None]:
        adapter = self.get_venue_adapter(Venue.HYPERLIQUID)
        if adapter is None:
            return None, "hyperliquid_adapter_unavailable"

        cached_result, was_cached = self._cached_entry_balance_snapshot(
            Venue.HYPERLIQUID,
            now_ms,
        )
        if was_cached:
            ok, value = cached_result
            if ok:
                return value, None
            return None, str(value or "hyperliquid_account_balance_unavailable")

        try:
            snapshot = await adapter.fetch_account_balance_snapshot()
            self._store_entry_balance_snapshot(
                Venue.HYPERLIQUID,
                now_ms,
                (True, snapshot),
            )
            if snapshot is None:
                return None, "hyperliquid_account_balance_unavailable"
            return snapshot, None
        except Exception as e:
            error = str(e) or e.__class__.__name__
            self._store_entry_balance_snapshot(
                Venue.HYPERLIQUID,
                now_ms,
                (False, error),
            )
            return None, error

    def _hyperliquid_balance_block_sample(
        self,
        *,
        candidate: Any | None,
        reason: str,
        now_ms: int,
        stage: str,
        source: str,
        available_balance_quote: float | None,
        required_initial_margin_quote: float,
        entry_notional_quote: float,
        raw_error: str = "",
        balance_classification: str = "",
        user_abstraction: str = "",
        spot_usdc_available: float | None = None,
    ) -> dict:
        evidence = self._entry_admission_evidence(reason)
        try:
            live_target_leverage = float(
                getattr(self.ctx.config.strategy, "live_target_leverage", 1.0) or 1.0
            )
        except (TypeError, ValueError):
            live_target_leverage = 1.0
        candidate_pair_id = (
            self._candidate_pair_id(candidate)
            if candidate is not None
            else "hyperliquid:*"
        )
        symbol = "*"
        long_venue = ""
        short_venue = ""
        if candidate is not None:
            symbol = str(getattr(candidate, "symbol", "*") or "*")
            long_venue = str(getattr(candidate, "long_venue", "") or "")
            short_venue = str(getattr(candidate, "short_venue", "") or "")
        payload = {
            "candidate_pair_id": candidate_pair_id,
            "pair_id": candidate_pair_id,
            "symbol": symbol,
            "long_venue": long_venue,
            "short_venue": short_venue,
            "venue": Venue.HYPERLIQUID.value,
            "reason": reason,
            "block_scope": "venue",
            "source": source,
            "official_doc_url": str(evidence.get("official_doc_url") or ""),
            "evidence_gap": bool(evidence.get("evidence_gap", True)),
            "stage": stage,
            "available_balance_quote": available_balance_quote,
            "required_initial_margin_quote": required_initial_margin_quote,
            "entry_notional_quote": entry_notional_quote,
            "live_target_leverage": live_target_leverage,
            "margin_buffer_bps": self._entry_admission_margin_buffer_bps(),
            "ts_ms": now_ms,
        }
        if raw_error:
            payload["raw_error"] = raw_error[:500]
        if balance_classification:
            payload["balance_classification"] = balance_classification
        if user_abstraction:
            payload["user_abstraction"] = user_abstraction
        if spot_usdc_available is not None:
            payload["spot_usdc_available"] = spot_usdc_available
        return payload

    def _append_hyperliquid_balance_unavailable_event(
        self,
        *,
        now_ms: int,
        stage: str,
        source: str,
        raw_error: str,
        candidate_count: int,
        blocked_count: int,
        allowed_count: int,
        samples: list[dict],
    ) -> None:
        reason = "hyperliquid_account_balance_unavailable"
        evidence = self._entry_admission_evidence(reason)
        self._append_runtime_diagnostic_event(
            "runtime.entry_admission_venue_degraded",
            {
                "venue": Venue.HYPERLIQUID.value,
                "reason": reason,
                "block_scope": "venue",
                "source": source,
                "official_doc_url": evidence["official_doc_url"],
                "evidence_gap": True,
                "stage": stage,
                "candidate_count": candidate_count,
                "blocked_count": blocked_count,
                "allowed_count": allowed_count,
                "blocked_reason_counts": {reason: blocked_count},
                "samples": samples[:10],
                "suppressed_count": max(blocked_count - len(samples), 0),
                "raw_error": raw_error[:500],
                "ts_ms": now_ms,
            },
            now_ms=now_ms,
            key_parts=(stage, Venue.HYPERLIQUID.value, reason, source),
            interval_ms=self._ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS,
        )

    async def _refresh_hyperliquid_entry_balance_admission(self, now_ms: int) -> bool:
        adapter = self.get_venue_adapter(Venue.HYPERLIQUID)
        if adapter is None:
            return True

        try:
            entry_notional = float(
                getattr(
                    self.ctx.config.strategy,
                    "fixed_live_entry_notional_quote",
                    0.0,
                ) or 0.0
            )
        except (TypeError, ValueError):
            entry_notional = 0.0
        required_margin = self._hyperliquid_entry_required_initial_margin_quote(
            entry_notional
        )
        snapshot, error = await self._fetch_hyperliquid_entry_balance_snapshot(now_ms)
        if snapshot is None:
            reason = "hyperliquid_account_balance_unavailable"
            sample = self._hyperliquid_balance_block_sample(
                candidate=None,
                reason=reason,
                now_ms=now_ms,
                stage="scan_start",
                source="scan_start_balance_prefilter",
                available_balance_quote=None,
                required_initial_margin_quote=required_margin,
                entry_notional_quote=entry_notional,
                raw_error=error or reason,
            )
            self._append_hyperliquid_balance_unavailable_event(
                now_ms=now_ms,
                stage="scan_start",
                source="scan_start_balance_prefilter",
                raw_error=error or reason,
                candidate_count=1,
                blocked_count=1,
                allowed_count=0,
                samples=[sample],
            )
            return False

        available = max(float(snapshot.free or 0.0), 0.0)
        if available + 1e-9 >= required_margin:
            return True

        reason = "insufficient_margin_admission_prefiltered"
        evidence = self._entry_admission_evidence(reason)
        extra = self._hyperliquid_balance_block_sample(
            candidate=None,
            reason=reason,
            now_ms=now_ms,
            stage="scan_start",
            source="scan_start_balance_prefilter",
            available_balance_quote=available,
            required_initial_margin_quote=required_margin,
            entry_notional_quote=entry_notional,
            balance_classification=str(
                getattr(snapshot, "balance_classification", "") or ""
            ),
            user_abstraction=str(getattr(snapshot, "user_abstraction", "") or ""),
            spot_usdc_available=getattr(snapshot, "spot_usdc_available", None),
        )
        self._record_symbol_admission_block(
            venue=Venue.HYPERLIQUID,
            symbol="*",
            reason=reason,
            raw_error="hyperliquid available balance below entry initial margin",
            now_ms=now_ms,
            evidence=evidence,
            source="scan_start_balance_prefilter",
            candidate_pair_id="hyperliquid:*",
            extra_payload=extra,
        )
        return False

    async def _filter_candidates_by_entry_balance_admission(
        self,
        candidates: list,
        *,
        now_ms: int,
        stage: str,
    ) -> list:
        if not candidates:
            return []
        if not any(
            self._candidate_uses_venue(candidate, Venue.HYPERLIQUID)
            for candidate in candidates
        ):
            return candidates
        if self.get_venue_adapter(Venue.HYPERLIQUID) is None:
            return candidates

        snapshot, error = await self._fetch_hyperliquid_entry_balance_snapshot(now_ms)
        allowed: list = []
        blocked_samples: list[dict] = []
        blocked_reason_counts: Counter[str] = Counter()
        source = "candidate_balance_prefilter"

        if snapshot is None:
            reason = "hyperliquid_account_balance_unavailable"
            for candidate in candidates:
                if not self._candidate_uses_venue(candidate, Venue.HYPERLIQUID):
                    allowed.append(candidate)
                    continue
                try:
                    entry_notional = float(
                        getattr(candidate, "entry_notional_quote", 0.0) or 0.0
                    )
                except (TypeError, ValueError):
                    entry_notional = 0.0
                required_margin = self._hyperliquid_entry_required_initial_margin_quote(
                    entry_notional
                )
                blocked_reason_counts[reason] += 1
                if len(blocked_samples) < 24:
                    blocked_samples.append(
                        self._hyperliquid_balance_block_sample(
                            candidate=candidate,
                            reason=reason,
                            now_ms=now_ms,
                            stage=stage,
                            source=source,
                            available_balance_quote=None,
                            required_initial_margin_quote=required_margin,
                            entry_notional_quote=entry_notional,
                            raw_error=error or reason,
                        )
                    )
            self._last_entry_admission_filter_blockers.update(blocked_reason_counts)
            self._last_entry_admission_filter_samples.extend(blocked_samples)
            self._append_hyperliquid_balance_unavailable_event(
                now_ms=now_ms,
                stage=stage,
                source=source,
                raw_error=error or reason,
                candidate_count=len(candidates),
                blocked_count=sum(blocked_reason_counts.values()),
                allowed_count=len(allowed),
                samples=blocked_samples,
            )
            return allowed

        available = max(float(snapshot.free or 0.0), 0.0)
        for candidate in candidates:
            if not self._candidate_uses_venue(candidate, Venue.HYPERLIQUID):
                allowed.append(candidate)
                continue
            try:
                entry_notional = float(
                    getattr(candidate, "entry_notional_quote", 0.0) or 0.0
                )
            except (TypeError, ValueError):
                entry_notional = 0.0
            required_margin = self._hyperliquid_entry_required_initial_margin_quote(
                entry_notional
            )
            if available + 1e-9 >= required_margin:
                allowed.append(candidate)
                continue

            reason = "insufficient_margin_admission_prefiltered"
            blocked_reason_counts[reason] += 1
            if len(blocked_samples) < 24:
                blocked_samples.append(
                    self._hyperliquid_balance_block_sample(
                        candidate=candidate,
                        reason=reason,
                        now_ms=now_ms,
                        stage=stage,
                        source=source,
                        available_balance_quote=available,
                        required_initial_margin_quote=required_margin,
                        entry_notional_quote=entry_notional,
                        balance_classification=str(
                            getattr(snapshot, "balance_classification", "") or ""
                        ),
                        user_abstraction=str(
                            getattr(snapshot, "user_abstraction", "") or ""
                        ),
                        spot_usdc_available=getattr(
                            snapshot,
                            "spot_usdc_available",
                            None,
                        ),
                    )
                )

        blocked_count = sum(blocked_reason_counts.values())
        if blocked_count <= 0:
            return allowed

        self._last_entry_admission_filter_blockers.update(blocked_reason_counts)
        self._last_entry_admission_filter_samples.extend(blocked_samples)
        sorted_reasons = sorted(blocked_reason_counts)
        reason = (
            sorted_reasons[0]
            if len(sorted_reasons) == 1
            else "multiple_entry_balance_admission_blocks"
        )
        evidence = self._entry_admission_evidence(reason)
        self._append_runtime_diagnostic_event(
            "runtime.entry_admission_venue_degraded",
            {
                "venue": Venue.HYPERLIQUID.value,
                "reason": reason,
                "block_scope": "venue",
                "source": source,
                "official_doc_url": evidence.get("official_doc_url", ""),
                "evidence_gap": bool(evidence.get("evidence_gap", True)),
                "stage": stage,
                "candidate_count": len(candidates),
                "blocked_count": blocked_count,
                "allowed_count": len(allowed),
                "blocked_reason_counts": dict(sorted(blocked_reason_counts.items())),
                "samples": blocked_samples[:10],
                "suppressed_count": max(blocked_count - len(blocked_samples), 0),
                "ts_ms": now_ms,
            },
            now_ms=now_ms,
            key_parts=(stage, Venue.HYPERLIQUID.value, reason, source),
            interval_ms=self._ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS,
        )
        return allowed

    def _candidate_admission_block(self, candidate, now_ms: int) -> dict | None:
        symbol = str(getattr(candidate, "symbol", "") or "")
        candidate_pair_id = self._candidate_pair_id(candidate)
        for raw_venue in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        ):
            try:
                venue = Venue.from_str(str(raw_venue))
            except ValueError:
                continue
            key = (venue.value, symbol)
            payload = {}
            payload_state_key = ""
            state_until_ms = 0
            for state_key in (f"{venue.value}:{symbol}", f"{venue.value}:*"):
                candidate_payload = dict(
                    self.ctx.state.venue_entry_cooldowns.get(state_key, {}) or {}
                )
                try:
                    candidate_until_ms = int(
                        candidate_payload.get("blocked_until_ms", 0) or 0
                    )
                except (TypeError, ValueError):
                    candidate_until_ms = 0
                if candidate_until_ms > state_until_ms:
                    state_until_ms = candidate_until_ms
                    payload = candidate_payload
                    payload_state_key = state_key
            until_ms = max(
                self._symbol_admission_blocked_until_ms.get(key, 0),
                state_until_ms,
            )
            if until_ms > now_ms:
                self._symbol_admission_blocked_until_ms[key] = until_ms
                if payload:
                    payload.setdefault("venue", venue.value)
                    if payload.get("block_scope") == "venue":
                        payload["blocked_symbol"] = (
                            payload.get("blocked_symbol")
                            or payload.get("symbol")
                            or "*"
                        )
                        payload["symbol"] = symbol
                    else:
                        payload.setdefault("symbol", symbol)
                    payload.setdefault(
                        "block_scope",
                        "venue" if payload_state_key.endswith(":*") else "symbol",
                    )
                    payload.setdefault("reason", "symbol_admission_blocked")
                    payload["blocked_until_ms"] = until_ms
                    payload.setdefault("ttl_ms", self._SYMBOL_ADMISSION_BLOCK_TTL_MS)
                    payload.setdefault("raw_error", "")
                    payload.setdefault("official_doc_url", "")
                    payload.setdefault("evidence_gap", True)
                    payload.setdefault("candidate_pair_id", candidate_pair_id)
                    payload.setdefault("pair_id", candidate_pair_id)
                    return payload
                return {
                    "venue": venue.value,
                    "symbol": symbol,
                    "reason": "symbol_admission_blocked",
                    "blocked_until_ms": until_ms,
                    "ttl_ms": self._SYMBOL_ADMISSION_BLOCK_TTL_MS,
                    "raw_error": "",
                    "official_doc_url": "",
                    "evidence_gap": True,
                    "candidate_pair_id": candidate_pair_id,
                    "pair_id": candidate_pair_id,
                }
        return None

    def _filter_candidates_by_entry_admission(
        self,
        candidates: list,
        *,
        now_ms: int,
        stage: str,
    ) -> list:
        """V1-style pre-shortlist entry admission gate for venue-scope cooldowns."""
        self._last_entry_admission_filter_blockers = Counter()
        self._last_entry_admission_filter_samples = []
        if not candidates:
            return []

        allowed: list = []
        blocked_samples: list[dict] = []
        blocked_until_ms = 0
        blocked_venues: set[str] = set()
        blocked_sources: set[str] = set()
        official_doc_urls: set[str] = set()
        evidence_gap_values: set[bool] = set()
        for candidate in candidates:
            block = self._candidate_admission_block(candidate, now_ms)
            if not block or block.get("block_scope") != "venue":
                allowed.append(candidate)
                continue

            reason = str(block.get("reason") or "venue_admission_blocked")
            venue = str(block.get("venue") or "")
            try:
                candidate_blocked_until_ms = int(block.get("blocked_until_ms", 0) or 0)
            except (TypeError, ValueError):
                candidate_blocked_until_ms = 0
            blocked_until_ms = max(blocked_until_ms, candidate_blocked_until_ms)
            if venue:
                blocked_venues.add(venue)
            source = str(block.get("source") or "entry_admission_cooldown")
            if source:
                blocked_sources.add(source)
            doc_url = str(block.get("official_doc_url") or "")
            if doc_url:
                official_doc_urls.add(doc_url)
            evidence_gap_values.add(bool(block.get("evidence_gap", True)))
            self._last_entry_admission_filter_blockers[reason] += 1
            if len(blocked_samples) < 24:
                blocked_samples.append({
                    "candidate_pair_id": self._candidate_pair_id(candidate),
                    "pair_id": self._candidate_pair_id(candidate),
                    "symbol": str(getattr(candidate, "symbol", "") or ""),
                    "long_venue": str(getattr(candidate, "long_venue", "") or ""),
                    "short_venue": str(getattr(candidate, "short_venue", "") or ""),
                    "venue": venue,
                    "reason": reason,
                    "block_scope": "venue",
                    "blocked_until_ms": candidate_blocked_until_ms,
                    "blocked_symbol": str(block.get("blocked_symbol") or ""),
                    "source": source,
                    "official_doc_url": doc_url,
                    "evidence_gap": bool(block.get("evidence_gap", True)),
                    "stage": stage,
                })

        self._last_entry_admission_filter_samples = blocked_samples
        blocked_count = sum(self._last_entry_admission_filter_blockers.values())
        if blocked_count <= 0:
            return allowed

        sorted_reasons = sorted(self._last_entry_admission_filter_blockers)
        venue = next(iter(blocked_venues)) if len(blocked_venues) == 1 else "multiple"
        reason = sorted_reasons[0] if len(sorted_reasons) == 1 else "multiple_entry_admission_blocks"
        source = next(iter(blocked_sources)) if len(blocked_sources) == 1 else "multiple"
        official_doc_url = (
            next(iter(official_doc_urls)) if len(official_doc_urls) == 1 else ""
        )
        evidence_gap = (
            next(iter(evidence_gap_values))
            if len(evidence_gap_values) == 1
            else True
        )
        self._append_runtime_diagnostic_event(
            "runtime.entry_admission_venue_degraded",
            {
                "venue": venue,
                "reason": reason,
                "block_scope": "venue",
                "blocked_until_ms": blocked_until_ms,
                "source": source,
                "official_doc_url": official_doc_url,
                "evidence_gap": evidence_gap,
                "stage": stage,
                "candidate_count": len(candidates),
                "blocked_count": blocked_count,
                "allowed_count": len(allowed),
                "blocked_reason_counts": dict(
                    sorted(self._last_entry_admission_filter_blockers.items())
                ),
                "samples": blocked_samples[:10],
                "suppressed_count": 0,
                "ts_ms": now_ms,
            },
            now_ms=now_ms,
            key_parts=(stage, venue, reason, source),
            interval_ms=self._ENTRY_ADMISSION_VENUE_DEGRADED_LOG_INTERVAL_MS,
        )
        return allowed

    async def _filter_candidates_supported_by_venue_catalog(
        self,
        candidates: list,
        *,
        skip_event_kind: str = "runtime.candidate_symbol_skipped",
    ) -> list:
        """Filter live candidates through both venues' trading catalogs.

        V1 build_scan_symbol_cache only admits symbols supported by both venues
        in a directed pair. V2 sidecar snapshots can still contain public quote
        rows for symbols that are not orderable on one venue, so runtime applies
        the same catalog gate before shortlist/tracking/entry selection.
        """
        self._last_candidate_catalog_filter_blockers = Counter()
        self._last_candidate_catalog_filter_samples = []
        if self.ctx.config.runtime.mode == "paper":
            return list(candidates)

        venue_symbols: dict[Venue, set[str]] = {}
        candidate_venues: list[tuple[object, Venue | None, Venue | None]] = []
        for candidate in candidates:
            try:
                long_venue = Venue.from_str(str(getattr(candidate, "long_venue", "")))
            except ValueError:
                long_venue = None
            try:
                short_venue = Venue.from_str(str(getattr(candidate, "short_venue", "")))
            except ValueError:
                short_venue = None
            candidate_venues.append((candidate, long_venue, short_venue))
            symbol = str(getattr(candidate, "symbol", "") or "")
            if not symbol:
                continue
            for venue in (long_venue, short_venue):
                if venue is not None:
                    venue_symbols.setdefault(venue, set()).add(symbol)

        supported_by_venue: dict[Venue, set[str] | None] = {}
        for venue, symbols in venue_symbols.items():
            adapter = self.get_venue_adapter(venue)
            if adapter is None:
                supported_by_venue[venue] = None
                continue
            filtered = await self._filter_symbols_supported_by_venue(
                venue,
                adapter,
                sorted(symbols),
                skip_event_kind="",
            )
            supported_by_venue[venue] = set(filtered)

        filtered_candidates: list = []
        skipped = 0
        for candidate, long_venue, short_venue in candidate_venues:
            symbol = str(getattr(candidate, "symbol", "") or "")

            def venue_supports(venue: Venue | None) -> bool:
                if venue is None:
                    return True
                supported = supported_by_venue.get(venue)
                return supported is None or symbol in supported

            long_supported = venue_supports(long_venue)
            short_supported = venue_supports(short_venue)
            if long_supported and short_supported:
                filtered_candidates.append(candidate)
                continue

            skipped += 1
            self._last_candidate_catalog_filter_blockers["unsupported_symbol"] += 1
            sample_payload = {
                "symbol": symbol,
                "candidate_pair_id": self._candidate_pair_id(candidate),
                "pair_id": self._candidate_pair_id(candidate),
                "long_venue": (
                    long_venue.value
                    if long_venue
                    else str(getattr(candidate, "long_venue", ""))
                ),
                "short_venue": (
                    short_venue.value
                    if short_venue
                    else str(getattr(candidate, "short_venue", ""))
                ),
                "long_supported": long_supported,
                "short_supported": short_supported,
                "reason": "unsupported_symbol",
            }
            if len(self._last_candidate_catalog_filter_samples) < 24:
                self._last_candidate_catalog_filter_samples.append(sample_payload)
            if getattr(self.ctx.journal, "_file", None) is not None:
                self._append_runtime_diagnostic_event(
                    skip_event_kind,
                    sample_payload,
                    now_ms=wall_clock_now_ms(),
                    key_parts=(
                        symbol,
                        self._candidate_pair_id(candidate),
                        "unsupported_symbol",
                        str(long_supported),
                        str(short_supported),
                    ),
                    interval_ms=self._CANDIDATE_SYMBOL_SKIPPED_LOG_INTERVAL_MS,
                )

        if skipped > 0 and getattr(self.ctx.journal, "_file", None) is not None:
            self.ctx.journal.append(
                "runtime.tradeable_candidates_catalog_filtered",
                {
                    "input_count": len(candidates),
                    "output_count": len(filtered_candidates),
                    "skipped_count": skipped,
                    "blocked_reason_counts": dict(
                        sorted(self._last_candidate_catalog_filter_blockers.items())
                    ),
                    "samples": self._last_candidate_catalog_filter_samples[:10],
                },
            )
        return filtered_candidates

    def _gate_pending_close_reconciliation(self, candidate) -> tuple[bool, str]:
        """Block entry if a pending close reconciliation exists for same symbol+venues."""
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        self.ctx.state.set_pending_close_reconciliations(
            getattr(self.ctx.state, "pending_close_reconciliations", [])
        )
        for rec in self.ctx.state.pending_close_reconciliations:
            if not isinstance(rec, dict):
                continue
            snapshot = rec.get("position_snapshot", {})
            if not isinstance(snapshot, dict):
                snapshot = {}
            if (rec.get("symbol") or snapshot.get("symbol") or "") != sym:
                continue
            pc_long = rec.get("long_venue") or snapshot.get("long_venue")
            pc_short = rec.get("short_venue") or snapshot.get("short_venue")
            pc_long_s = pc_long.value if hasattr(pc_long, "value") else str(pc_long)
            pc_short_s = pc_short.value if hasattr(pc_short, "value") else str(pc_short)
            if not pc_long_s or not pc_short_s:
                return False, "pending_close_reconciliation_invalid"
            if (pc_long_s == long_v and pc_short_s == short_v) or \
               (pc_long_s == short_v and pc_short_s == long_v):
                return False, "pending_close_reconciliation_conflict"
        return True, ""

    def _gate_passive_close_pending(self, candidate) -> tuple[bool, str]:
        """Block entry if a passive close is in-flight for the same symbol pair."""
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        for pos_id in list(self.ctx.state.pending_passive_closes.keys()):
            pos = self.ctx.state.open_positions.get(pos_id)
            if pos is None:
                continue
            if getattr(pos, 'symbol', '') != sym:
                continue
            pos_long = getattr(pos, 'long_venue', None)
            pos_short = getattr(pos, 'short_venue', None)
            pos_long_s = pos_long.value if hasattr(pos_long, 'value') else str(pos_long)
            pos_short_s = pos_short.value if hasattr(pos_short, 'value') else str(pos_short)
            if (pos_long_s == long_v and pos_short_s == short_v) or \
               (pos_long_s == short_v and pos_short_s == long_v):
                return False, "passive_close_in_flight"
        return True, ""

    def _gate_reduce_only(self, candidate) -> tuple[bool, str]:
        """Block new entry when lifecycle/risk mode is reduce-only or fail-closed."""
        if self.ctx.state.lifecycle == EngineLifecycle.RISK_ONLY:
            return False, f"lifecycle_{self.ctx.state.lifecycle.value}"
        if self.ctx.state.risk_mode.value in ("reduce_only", "fail_closed"):
            return False, f"risk_mode_{self.ctx.state.risk_mode.value}"
        return True, ""

    def _gate_venue_cooldown(self, candidate, now_ms: int) -> tuple[bool, str]:
        """Block entry if either venue is in cooldown."""
        for ven_str in (getattr(candidate, 'long_venue', ''), getattr(candidate, 'short_venue', '')):
            if not ven_str:
                continue
            until = self._venue_cooldown_until_ms.get(ven_str, 0)
            if until > 0 and now_ms < until:
                return False, f"venue_cooldown_{ven_str}"
        return True, ""

    def _gate_zero_fill_cooldown(self, candidate, now_ms: int) -> tuple[bool, str]:
        """Block entry if a zero-fill terminal event is in cooldown for the same pair.

        Zero-fill means a recent entry attempt on this pair produced no fills,
        indicating the venue may be rejecting orders or the spread is too wide.
        """
        pair_key = (getattr(candidate, 'symbol', ''), getattr(candidate, 'long_venue', ''), getattr(candidate, 'short_venue', ''))
        until = self._zero_fill_cooldown_until_ms.get(pair_key, 0)
        if until > 0 and now_ms < until:
            return False, "zero_fill_cooldown"
        symbol = getattr(candidate, "symbol", "")
        for venue in (getattr(candidate, "long_venue", ""), getattr(candidate, "short_venue", "")):
            if not venue:
                continue
            until = self._post_only_reject_cooldown_until_ms.get((symbol, venue), 0)
            if until > 0 and now_ms < until:
                return False, f"post_only_reject_cooldown_{venue}"
        return True, ""

    def _gate_pending_entry_dedup(self, candidate) -> tuple[bool, str]:
        """Block entry if a pending entry already exists for same symbol+venue pair."""
        from lightfee.engine.recovery import has_pending_entry_for_symbol
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        if has_pending_entry_for_symbol(self.ctx.state, sym, long_v, short_v):
            return False, "pending_entry_protection"
        return True, ""

    def _gate_recovery_ledger(self, candidate) -> tuple[bool, str]:
        allowed, reason = self._v1_lifecycle_entry_gate_decision()
        if not allowed:
            if not self._v1_lifecycle_closure_blocks_candidate(
                candidate,
                reason=reason,
            ):
                return True, ""
            return False, self._v1_lifecycle_runtime_gate_reason(reason)
        return True, ""

    def _gate_entry_sizing(self, candidate) -> tuple[bool, str]:
        """Block entry if notional quote is zero or negative."""
        if getattr(candidate, 'entry_notional_quote', 0.0) <= 0:
            return False, "entry_notional_zero_or_negative"
        return True, ""

    def _entry_liquidity_qualification_state(self):
        from lightfee.engine.entry_liquidity_qualification import (
            EntryLiquidityQualificationState,
        )

        return EntryLiquidityQualificationState.from_records(
            getattr(self.ctx.state, "entry_liquidity_qualification_records", []) or []
        )

    def _entry_liquidity_volume_floor_quote(self, venue: str) -> float:
        from lightfee.config.schema import V1_ENTRY_VOLUME_FLOOR_DEFAULT_QUOTE

        getter = getattr(self.ctx.config.strategy, "entry_volume_floor_quote", None)
        if callable(getter):
            return float(getter(venue))
        return float(V1_ENTRY_VOLUME_FLOOR_DEFAULT_QUOTE)

    def _entry_liquidity_open_interest_floor_quote(self, venue: str) -> float:
        from lightfee.config.schema import V1_ENTRY_OPEN_INTEREST_FLOOR_DEFAULT_QUOTE

        getter = getattr(self.ctx.config.strategy, "entry_open_interest_floor_quote", None)
        if callable(getter):
            return float(getter(venue))
        return float(V1_ENTRY_OPEN_INTEREST_FLOOR_DEFAULT_QUOTE)

    def _entry_liquidity_decision_payload(
        self,
        *,
        venue: str,
        symbol: str,
        quote,
        snapshot,
        now_ms: int,
        fallback_source: str,
        reason: str,
        decision: str,
        event_kind: str,
        eligibility_class: str,
        observed_volume_24h_quote: float,
        min_volume_24h_quote: float,
        observed_open_interest_quote: float,
        min_open_interest_quote: float,
        state_record: dict | None = None,
        open_interest_evidence_status: str = "available",
    ) -> dict:
        observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        payload = {
            "venue": venue,
            "symbol": symbol,
            "domain": "liquidity",
            "source_domain": "perp_liquidity",
            "source": "sidecar_perp_liquidity",
            "endpoint": "sidecar_perp_liquidity",
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": self._snapshot_domain_budget_ms("liquidity"),
            "decision": decision,
            "fallback_source": fallback_source,
            "reason": reason,
            "event_kind": event_kind,
            "blocking": decision == "skip_entry",
            "observed_volume_24h_quote": observed_volume_24h_quote,
            "min_volume_24h_quote": min_volume_24h_quote,
            "observed_open_interest_quote": observed_open_interest_quote,
            "min_open_interest_quote": min_open_interest_quote,
            "open_interest_evidence_status": open_interest_evidence_status,
            "eligibility_class": eligibility_class,
            "floor": min_open_interest_quote,
            "current_value": observed_open_interest_quote,
            "targeted_revalidate_required": reason in {
                "perp_open_interest_structural",
                "oi_evidence_unavailable",
            },
            "targeted_revalidate_scope": "entry_candidate",
        }
        if state_record:
            payload.update({
                "consecutive_failures": int(
                    state_record.get("consecutive_failures", 0) or 0
                ),
                "suppress_until_ms": state_record.get("suppress_until_ms"),
                "last_failure_at_ms": state_record.get("last_failure_at_ms"),
                "last_structural_probe_at_ms": state_record.get(
                    "last_structural_probe_at_ms"
                ),
            })
        return payload

    def _entry_liquidity_qualification_decisions(
        self,
        candidate,
        *,
        snapshot,
        quote_lookup: dict,
        now_ms: int,
        fallback_source: str,
        record_result: bool = False,
    ) -> list[dict]:
        if str(getattr(self.ctx.config.runtime, "mode", "") or "").lower() != "live":
            return []
        if not bool(getattr(self.ctx.config.strategy, "execution_liquidity_enabled", True)):
            return []

        from lightfee.engine.entry_liquidity_qualification import (
            ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS,
            EntryLiquidityEligibilityClass,
        )

        state = self._entry_liquidity_qualification_state()
        decisions: list[dict] = []
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        if not symbol:
            return decisions

        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            if not venue:
                continue
            quote = quote_lookup.get((venue, symbol))
            if quote is None:
                continue
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            if bid <= 0.0 or ask <= 0.0:
                continue

            observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
            volume_24h_quote = float(getattr(quote, "volume_24h_quote", 0.0) or 0.0)
            open_interest_quote = float(getattr(quote, "open_interest", 0.0) or 0.0)
            open_interest_evidence_status = str(
                getattr(quote, "open_interest_evidence_status", "available")
                or "available"
            ).lower()
            volume_floor = self._entry_liquidity_volume_floor_quote(venue)
            open_interest_floor = self._entry_liquidity_open_interest_floor_quote(venue)
            current_class = state.current_class(venue, symbol, now_ms=now_ms)

            if record_result and open_interest_evidence_status == "available":
                state.note_open_interest_observation(
                    venue,
                    symbol,
                    open_interest_quote,
                    observed_at_ms=observed_at_ms,
                )

            if (
                open_interest_floor > 0.0
                and open_interest_evidence_status != "available"
            ):
                decisions.append(
                    self._entry_liquidity_decision_payload(
                        venue=venue,
                        symbol=symbol,
                        quote=quote,
                        snapshot=snapshot,
                        now_ms=now_ms,
                        fallback_source=fallback_source,
                        reason="oi_evidence_unavailable",
                        decision="skip_entry",
                        event_kind="execution.entry_liquidity_blocked",
                        eligibility_class=(
                            EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR.value
                        ),
                        observed_volume_24h_quote=volume_24h_quote,
                        min_volume_24h_quote=volume_floor,
                        observed_open_interest_quote=open_interest_quote,
                        min_open_interest_quote=open_interest_floor,
                        open_interest_evidence_status=open_interest_evidence_status,
                    )
                )
                continue

            if current_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY:
                if not record_result or not state.should_probe_structural(
                    venue,
                    symbol,
                    now_ms=now_ms,
                    probe_interval_ms=ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS,
                ):
                    state_record = next(
                        (
                            record for record in state.to_records()
                            if record["venue"] == venue and record["symbol"] == symbol
                        ),
                        None,
                    )
                    decisions.append(
                        self._entry_liquidity_decision_payload(
                            venue=venue,
                            symbol=symbol,
                            quote=quote,
                            snapshot=snapshot,
                            now_ms=now_ms,
                            fallback_source=fallback_source,
                            reason="perp_open_interest_structural",
                            decision="skip_entry",
                            event_kind="execution.entry_liquidity_blocked",
                            eligibility_class=(
                                EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY.value
                            ),
                            observed_volume_24h_quote=volume_24h_quote,
                            min_volume_24h_quote=volume_floor,
                            observed_open_interest_quote=open_interest_quote,
                            min_open_interest_quote=open_interest_floor,
                            state_record=state_record,
                            open_interest_evidence_status=open_interest_evidence_status,
                        )
                    )
                    continue

            if volume_floor > 0.0 and volume_24h_quote < volume_floor:
                decisions.append(
                    self._entry_liquidity_decision_payload(
                        venue=venue,
                        symbol=symbol,
                        quote=quote,
                        snapshot=snapshot,
                        now_ms=now_ms,
                        fallback_source=fallback_source,
                        reason="perp_volume_below_floor_advisory",
                        decision="continue",
                        event_kind="execution.entry_liquidity_advisory",
                        eligibility_class=(
                            EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR.value
                        ),
                        observed_volume_24h_quote=volume_24h_quote,
                        min_volume_24h_quote=volume_floor,
                        observed_open_interest_quote=open_interest_quote,
                        min_open_interest_quote=open_interest_floor,
                        open_interest_evidence_status=open_interest_evidence_status,
                    )
                )

            if open_interest_floor > 0.0 and open_interest_quote < open_interest_floor:
                if record_result:
                    result_class = state.record_result(
                        venue,
                        symbol,
                        EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR,
                        now_ms=now_ms,
                    )
                else:
                    result_class = (
                        EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
                        if current_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
                        else EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR
                    )
                state_record = next(
                    (
                        record for record in state.to_records()
                        if record["venue"] == venue and record["symbol"] == symbol
                    ),
                    None,
                )
                reason = (
                    "perp_open_interest_structural"
                    if result_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
                    else "perp_open_interest_below_floor"
                )
                decisions.append(
                    self._entry_liquidity_decision_payload(
                        venue=venue,
                        symbol=symbol,
                        quote=quote,
                        snapshot=snapshot,
                        now_ms=now_ms,
                        fallback_source=fallback_source,
                        reason=reason,
                        decision="skip_entry",
                        event_kind="execution.entry_liquidity_blocked",
                        eligibility_class=result_class.value,
                        observed_volume_24h_quote=volume_24h_quote,
                        min_volume_24h_quote=volume_floor,
                        observed_open_interest_quote=open_interest_quote,
                        min_open_interest_quote=open_interest_floor,
                        state_record=state_record,
                        open_interest_evidence_status=open_interest_evidence_status,
                    )
                )
                continue

            if record_result:
                state.record_result(
                    venue,
                    symbol,
                    EntryLiquidityEligibilityClass.ELIGIBLE,
                    now_ms=now_ms,
                )

        if record_result:
            self.ctx.state.entry_liquidity_qualification_records = state.to_records()
        return decisions

    def _compact_scan_no_entry_diagnostics_payload(
        self,
        payload: dict,
        *,
        suppressed_full_payload_count: int,
    ) -> dict:
        compact_keys = (
            "reason",
            "generic_reason",
            "candidate_count",
            "tradeable_count",
            "selected_candidate_count",
            "dispatched_candidate_count",
            "max_concurrent_positions",
            "open_position_count",
            "remaining_slots",
            "capacity_blocked",
            "blocked_reason_counts",
            "entry_candidate_blocked_counts",
            "unsupported_symbol_blocked_counts",
            "entry_admission_venue_degraded_counts",
            "snapshot_freshness_blocked_counts",
            "execution_liquidity_blocked_counts",
            "entry_final_gate_blocked_counts",
            "tradeable_selection_blocker_counts",
            "entry_ws_bbo_blocker_counts",
            "entry_admission_blocker_counts",
            "quote_truth_must_resolve_count",
            "quote_truth_resolved_count",
            "quote_truth_failed_count",
            "quote_truth_ws_resolved_count",
            "quote_truth_rest_resolved_count",
            "budget_excluded_without_rest_count",
            "quote_revalidate_sources",
            "top_quote_blocker_buckets",
            "selection_bucket_counts",
            "candidate_stage_blocked_counts",
            "entry_local_l2_primary_ready_filter_active",
            "entry_local_l2_primary_not_ready_reason_counts",
            "entry_local_l2_primary_not_ready_reason_totals",
            "ts_ms",
        )
        compact = {key: payload[key] for key in compact_keys if key in payload}
        compact["compact"] = True
        compact["suppressed_full_payload_count"] = suppressed_full_payload_count
        return compact

    def _emit_scan_no_entry_diagnostics(
        self,
        *,
        reason: str,
        snapshot,
        tradeable: list,
        selected_candidate_count: int,
        dispatched_candidate_count: int,
        remaining_slots: int,
        tradeable_selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        now_ms: int,
        admission_blocker_counts: Counter | None = None,
    ) -> None:
        if getattr(self.ctx.journal, "_file", None) is None:
            return
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        blocked_reason_counts: Counter[str] = Counter()
        for candidate in getattr(snapshot, "candidates", []) or []:
            for blocked_reason in getattr(candidate, "blocked_reasons", []) or []:
                blocked_reason_counts[str(blocked_reason)] += 1
        catalog_filter_blockers = Counter(
            getattr(self, "_last_candidate_catalog_filter_blockers", Counter())
        )
        snapshot_freshness_blockers = Counter(
            getattr(self, "_last_snapshot_freshness_filter_blockers", Counter())
        )
        entry_admission_filter_blockers = Counter(
            getattr(self, "_last_entry_admission_filter_blockers", Counter())
        )
        if reason == "no_tradeable_candidates":
            if (
                entry_admission_filter_blockers
                and not blocked_reason_counts
                and not catalog_filter_blockers
                and not snapshot_freshness_blockers
            ):
                reason = (
                    self._v1_tradeable_no_entry_reason(
                        Counter(),
                        admission_blocker_counts=entry_admission_filter_blockers,
                    )
                    or "tradeable_candidates_blocked_by_entry_admission"
                )
            elif (
                catalog_filter_blockers
                and not blocked_reason_counts
                and not entry_admission_filter_blockers
                and not snapshot_freshness_blockers
            ):
                reason = "tradeable_candidates_blocked_by_unsupported_symbol"
            else:
                reason = self._no_tradeable_reason_from_candidate_blockers(
                    blocked_reason_counts,
                    snapshot_freshness_blockers,
                )

        readiness = self._entry_l2_readiness_diagnostics_payload()
        local_l2_provider_active = self._entry_readiness_provider_uses_local_l2()
        ws_bbo_provider_active = self._entry_readiness_provider_uses_ws_bbo()
        candidate_samples = []
        for rank, candidate in enumerate(list(tradeable)[:24], start=1):
            pair_id = getattr(candidate, "pair_id", "")
            if not pair_id:
                pair_id = make_candidate_pair_id(
                    str(getattr(candidate, "symbol", "")),
                    str(getattr(candidate, "long_venue", "")),
                    str(getattr(candidate, "short_venue", "")),
                )
            first_funding_ms = int(getattr(candidate, "first_funding_timestamp_ms", 0) or 0)
            candidate_samples.append({
                "rank": rank,
                "pair_id": pair_id,
                "symbol": str(getattr(candidate, "symbol", "")),
                "long_venue": str(getattr(candidate, "long_venue", "")),
                "short_venue": str(getattr(candidate, "short_venue", "")),
                "remaining_ms": first_funding_ms - now_ms if first_funding_ms > 0 else 0,
                "primary_tracked": pair_id in self._tracked_primary_pair_ids,
                "ranking_edge_bps": float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0),
                "blocked_reasons": list(getattr(candidate, "blocked_reasons", []) or [])[:8],
                "selection_blocker": candidate_blockers.get(pair_id, ""),
            })

        execution_liquidity_blocked_counts: Counter[str] = Counter()
        for reason_key, count in blocked_reason_counts.items():
            if "liquidity" in reason_key or reason_key.startswith("execution_"):
                execution_liquidity_blocked_counts[str(reason_key)] += int(count)

        admission_counts = admission_blocker_counts if admission_blocker_counts is not None else {}
        not_primary_tracked = int(
            admission_counts.get("entry_local_l2_waiting_for_primary_tracking", 0)
        )
        lifecycle_selection_blocked = sum(
            int(v) for k, v in tradeable_selection_blocker_counts.items()
            if str(k) in self._V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS
        )
        ws_bbo_selection_blocked = sum(
            int(v) for k, v in tradeable_selection_blocker_counts.items()
            if str(k).startswith("entry_ws_bbo_quote_lease_")
        )
        entry_admission_blocked = sum(
            int(v) for k, v in admission_counts.items()
            if (
                str(k).endswith("_admission_blocked")
                or str(k) in {
                    "bybit_trading_terms_required",
                    "insufficient_margin_admission_prefiltered",
                    "hyperliquid_account_balance_unavailable",
                }
            )
        )
        primary_tracked_not_ready = sum(
            int(v) for k, v in tradeable_selection_blocker_counts.items()
            if (
                k not in {"entry_local_l2_waiting_for_primary_tracking"}
                and str(k) not in self._V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS
                and not str(k).startswith("entry_ws_bbo_quote_lease_")
            )
        )
        selection_bucket_counts = {
            "not_primary_tracked": not_primary_tracked,
            "primary_tracked_not_ready": primary_tracked_not_ready,
        }
        if lifecycle_selection_blocked > 0:
            selection_bucket_counts[
                "lifecycle_selection_blocked"
            ] = lifecycle_selection_blocked
        if ws_bbo_selection_blocked > 0:
            selection_bucket_counts["ws_bbo_not_ready"] = ws_bbo_selection_blocked
        if entry_admission_blocked > 0:
            selection_bucket_counts["entry_admission_blocked"] = entry_admission_blocked

        entry_ws_bbo_blocker_counts = {
            str(k): int(v)
            for k, v in tradeable_selection_blocker_counts.items()
            if str(k).startswith("entry_ws_bbo_quote_lease_") and int(v) > 0
        }
        entry_admission_blocker_counts = {
            str(k): int(v)
            for k, v in admission_counts.items()
            if (
                int(v) > 0
                and (
                    str(k).endswith("_admission_blocked")
                    or str(k) in {
                        "bybit_trading_terms_required",
                        "insufficient_margin_admission_prefiltered",
                        "hyperliquid_account_balance_unavailable",
                    }
                )
            )
        }
        entry_ws_bbo_blocker_samples = [
            sample
            for sample in candidate_samples
            if str(sample.get("selection_blocker", "")).startswith(
                "entry_ws_bbo_quote_lease_"
            )
        ][:24]

        candidate_stage_blocked_counts = {
            "candidate_universe": sum(int(v) for v in blocked_reason_counts.values()),
            "unsupported_symbol": sum(
                int(v) for v in catalog_filter_blockers.values()
            ),
            "entry_admission_venue_degraded": sum(
                int(v) for v in entry_admission_filter_blockers.values()
            ),
            "entry_admission": entry_admission_blocked,
            "snapshot_quote_or_freshness": sum(
                int(v) for v in snapshot_freshness_blockers.values()
            ),
            "execution_liquidity": sum(
                int(v) for v in execution_liquidity_blocked_counts.values()
            ),
            "entry_selection": sum(
                int(v) for v in tradeable_selection_blocker_counts.values()
            ),
        }
        max_concurrent_positions = max(
            int(getattr(self.ctx.config.strategy, "max_concurrent_positions", 0) or 0),
            1,
        )
        open_position_count = len(self.ctx.state.open_positions)
        normalized_remaining_slots = max(int(remaining_slots), 0)
        last_scan = self.ctx.state.last_scan if isinstance(self.ctx.state.last_scan, dict) else {}
        quote_truth_payload = {
            "quote_truth_must_resolve_count": int(
                last_scan.get("quote_truth_must_resolve_count", 0) or 0
            ),
            "quote_truth_resolved_count": int(
                last_scan.get("quote_truth_resolved_count", 0) or 0
            ),
            "quote_truth_failed_count": int(
                last_scan.get("quote_truth_failed_count", 0) or 0
            ),
            "quote_truth_ws_resolved_count": int(
                last_scan.get("quote_truth_ws_resolved_count", 0) or 0
            ),
            "quote_truth_rest_resolved_count": int(
                last_scan.get("quote_truth_rest_resolved_count", 0) or 0
            ),
            "budget_excluded_without_rest_count": int(
                last_scan.get("budget_excluded_without_rest_count", 0) or 0
            ),
            "quote_revalidate_sources": dict(
                last_scan.get("quote_revalidate_sources", {}) or {}
            ),
            "top_quote_blocker_buckets": dict(
                last_scan.get("top_quote_blocker_buckets", {}) or {}
            ),
        }
        pipeline_counts = {
            "raw_candidates": int(
                last_scan.get(
                    "raw_candidate_count",
                    len(getattr(snapshot, "candidates", []) or []),
                )
                or 0
            ),
            "strategy_passed": int(last_scan.get("strategy_tradeable_count", 0) or 0),
            "catalog_admission_balance_passed": int(
                last_scan.get(
                    "catalog_admission_balance_passed_count",
                    last_scan.get(
                        "snapshot_freshness_all_candidate_count",
                        len(tradeable),
                    ),
                )
                or 0
            ),
            "v1_primary_shadow_tracked": int(
                last_scan.get("snapshot_freshness_candidate_count", 0) or 0
            ),
            "quote_oi_truth_must_resolve": int(
                last_scan.get("quote_truth_must_resolve_count", 0) or 0
            ),
            "quote_oi_truth_resolved": int(
                last_scan.get("quote_truth_resolved_count", 0) or 0
            ),
            "quote_oi_truth_failed": int(
                last_scan.get("quote_truth_failed_count", 0) or 0
            ),
            "selected": int(selected_candidate_count),
            "dispatched": int(dispatched_candidate_count),
        }

        payload = {
            "reason": reason,
            "generic_reason": (
                "no_tradeable_candidates"
                if reason in {
                    "candidate_snapshot_domain_stale",
                    "candidate_edge_insufficient",
                    "candidate_window_mismatch",
                }
                else reason
            ),
            "candidate_count": len(getattr(snapshot, "candidates", []) or []),
            "tradeable_count": len(tradeable),
            "selected_candidate_count": selected_candidate_count,
            "dispatched_candidate_count": dispatched_candidate_count,
            "pipeline_counts": pipeline_counts,
            "max_concurrent_positions": max_concurrent_positions,
            "open_position_count": open_position_count,
            "remaining_slots": normalized_remaining_slots,
            "capacity_blocked": open_position_count >= max_concurrent_positions
            and normalized_remaining_slots <= 0,
            "blocked_reason_counts": dict(sorted(blocked_reason_counts.items())),
            "entry_candidate_blocked_counts": dict(sorted(blocked_reason_counts.items())),
            "unsupported_symbol_blocked_counts": dict(
                sorted(catalog_filter_blockers.items())
            ),
            "unsupported_symbol_blocked_samples": list(
                getattr(self, "_last_candidate_catalog_filter_samples", []) or []
            )[:24],
            "entry_admission_venue_degraded_counts": dict(
                sorted(entry_admission_filter_blockers.items())
            ),
            "entry_admission_venue_degraded_samples": list(
                getattr(self, "_last_entry_admission_filter_samples", []) or []
            )[:24],
            "snapshot_freshness_blocked_counts": dict(
                sorted(snapshot_freshness_blockers.items())
            ),
            "snapshot_freshness_blocked_samples": list(
                getattr(self, "_last_snapshot_freshness_filter_samples", []) or []
            )[:24],
            "execution_liquidity_blocked_counts": dict(
                sorted(execution_liquidity_blocked_counts.items())
            ),
            "entry_final_gate_blocked_counts": dict(
                sorted((str(k), int(v)) for k, v in tradeable_selection_blocker_counts.items())
            ),
            "tradeable_selection_blocker_counts": dict(
                sorted((str(k), int(v)) for k, v in tradeable_selection_blocker_counts.items())
            ),
            "entry_ws_bbo_blocker_counts": dict(
                sorted(entry_ws_bbo_blocker_counts.items())
            ),
            "entry_admission_blocker_counts": dict(
                sorted(entry_admission_blocker_counts.items())
            ),
            "entry_ws_bbo_blocker_samples": entry_ws_bbo_blocker_samples,
            **quote_truth_payload,
            "selection_bucket_counts": selection_bucket_counts,
            "candidate_stage_blocked_counts": {
                key: value
                for key, value in candidate_stage_blocked_counts.items()
                if value > 0
            },
            "candidates": candidate_samples,
            "ts_ms": now_ms,
        }
        if local_l2_provider_active:
            payload.update({
                "entry_local_l2_primary_ready_filter_active": bool(
                    self._local_l2_effective_enabled() and self._tracked_primary_pair_ids
                ),
                "entry_local_l2_primary_not_ready_reason_counts": readiness["reason_counts"],
                "entry_local_l2_primary_not_ready_reason_totals": readiness["reason_totals"],
                "entry_local_l2_primary_not_ready_detail_samples": readiness["not_ready"][:24],
            })
        elif ws_bbo_provider_active:
            payload["entry_readiness_provider"] = "ws_bbo_quote_lease"
        fingerprint = self._payload_fingerprint({
            "reason": payload["reason"],
            "candidate_count": payload["candidate_count"],
            "tradeable_count": payload["tradeable_count"],
            "selected_candidate_count": payload["selected_candidate_count"],
            "dispatched_candidate_count": payload["dispatched_candidate_count"],
            "max_concurrent_positions": payload["max_concurrent_positions"],
            "open_position_count": payload["open_position_count"],
            "remaining_slots": payload["remaining_slots"],
            "tradeable_selection_blocker_counts": payload["tradeable_selection_blocker_counts"],
            "entry_local_l2_primary_not_ready_reason_totals": payload.get(
                "entry_local_l2_primary_not_ready_reason_totals", {},
            ),
            "entry_ws_bbo_blocker_counts": payload.get(
                "entry_ws_bbo_blocker_counts", {},
            ),
            "entry_admission_blocker_counts": payload.get(
                "entry_admission_blocker_counts", {},
            ),
            "candidates": [
                {
                    "pair_id": c["pair_id"],
                    "selection_blocker": c["selection_blocker"],
                }
                for c in payload["candidates"]
            ],
        })
        summary_fingerprint = self._payload_fingerprint({
            "reason": payload["reason"],
            "generic_reason": payload["generic_reason"],
            "max_concurrent_positions": payload["max_concurrent_positions"],
            "open_position_count": payload["open_position_count"],
            "remaining_slots": payload["remaining_slots"],
            "blocked_reason_keys": sorted(payload["blocked_reason_counts"].keys()),
            "unsupported_symbol_blocked_keys": sorted(
                payload["unsupported_symbol_blocked_counts"].keys()
            ),
            "snapshot_freshness_blocker_keys": sorted(
                payload["snapshot_freshness_blocked_counts"].keys()
            ),
            "entry_admission_venue_degraded_keys": sorted(
                payload["entry_admission_venue_degraded_counts"].keys()
            ),
            "tradeable_selection_blocker_keys": sorted(
                payload["tradeable_selection_blocker_counts"].keys()
            ),
            "entry_local_l2_primary_not_ready_reason_keys": sorted(
                payload.get("entry_local_l2_primary_not_ready_reason_totals", {}).keys()
            ),
            "entry_ws_bbo_blocker_keys": sorted(
                payload.get("entry_ws_bbo_blocker_counts", {}).keys()
            ),
            "entry_admission_blocker_keys": sorted(
                payload.get("entry_admission_blocker_counts", {}).keys()
            ),
        })
        full_due = (
            self._last_no_entry_full_diag_reason != payload["reason"]
            or self._last_no_entry_full_diag_ts_ms <= 0
            or now_ms - self._last_no_entry_full_diag_ts_ms
            >= self._NO_ENTRY_DIAGNOSTICS_FULL_INTERVAL_MS
            or summary_fingerprint != self._last_no_entry_summary_fingerprint
        )
        if full_due:
            self._last_no_entry_full_diag_reason = str(payload["reason"])
            self._last_no_entry_full_diag_ts_ms = now_ms
            self._last_no_entry_summary_fingerprint = summary_fingerprint
            self._last_no_entry_diag_fingerprint = fingerprint
            self._last_no_entry_diag_ts_ms = now_ms
            self._no_entry_suppressed_full_payload_count = 0
            self._last_no_entry_diagnostics = payload
            if self.ctx.state.last_scan is not None:
                self.ctx.state.last_scan.update({
                    "no_entry_reason": payload["reason"],
                    "max_concurrent_positions": payload["max_concurrent_positions"],
                    "open_position_count": payload["open_position_count"],
                    "remaining_slots": payload["remaining_slots"],
                    "capacity_blocked": payload["capacity_blocked"],
                    "selection_bucket_counts": payload["selection_bucket_counts"],
                    "tradeable_selection_blocker_counts": payload[
                        "tradeable_selection_blocker_counts"
                    ],
                    "candidate_stage_blocked_counts": payload[
                        "candidate_stage_blocked_counts"
                    ],
                })
            self.ctx.journal.append("scan.no_entry_diagnostics", payload)
            return

        self._no_entry_suppressed_full_payload_count += 1
        if now_ms - self._last_no_entry_diag_ts_ms < self._NO_ENTRY_DIAGNOSTICS_COMPACT_INTERVAL_MS:
            return

        self._last_no_entry_diag_fingerprint = summary_fingerprint
        self._last_no_entry_diag_ts_ms = now_ms
        self._last_no_entry_diagnostics = payload
        if self.ctx.state.last_scan is not None:
            self.ctx.state.last_scan.update({
                "no_entry_reason": payload["reason"],
                "max_concurrent_positions": payload["max_concurrent_positions"],
                "open_position_count": payload["open_position_count"],
                "remaining_slots": payload["remaining_slots"],
                "capacity_blocked": payload["capacity_blocked"],
                "selection_bucket_counts": payload["selection_bucket_counts"],
                "tradeable_selection_blocker_counts": payload[
                    "tradeable_selection_blocker_counts"
                ],
                "candidate_stage_blocked_counts": payload[
                    "candidate_stage_blocked_counts"
                ],
            })
        compact_payload = self._compact_scan_no_entry_diagnostics_payload(
            payload,
            suppressed_full_payload_count=self._no_entry_suppressed_full_payload_count,
        )
        self._no_entry_suppressed_full_payload_count = 0
        self.ctx.journal.append("scan.no_entry_diagnostics", compact_payload)

    def _entry_selection_target(self, remaining_slots: int) -> int:
        """V1 selection buffer: remaining slots, expanded up to eight candidates."""
        if remaining_slots <= 0:
            return 0
        return min(max(remaining_slots, remaining_slots * 4), 8)

    def _candidate_is_tradeable_for_selection(self, candidate) -> bool:
        if bool(getattr(candidate, "blocked", False)):
            return False
        if list(getattr(candidate, "blocked_reasons", []) or []):
            return False
        if float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0) <= 0:
            return False
        for venue_raw in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        ):
            try:
                venue = Venue.from_str(str(venue_raw)) if venue_raw else None
            except Exception:
                venue = None
            if venue is None:
                continue
            adapter = self.get_venue_adapter(venue)
            transport = getattr(adapter, "_transport", adapter)
            trusted = getattr(transport, "trading_capability_trusted", True)
            if trusted is False:
                return False
        return True

    def _candidate_quote(
        self,
        quote_lookup: dict[tuple[str, str], object],
        venue: str,
        symbol: str,
    ):
        return quote_lookup.get((str(venue).lower(), str(symbol).upper()))

    def _entry_leg_depth_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object],
        *,
        venue: str,
        side: str,
    ) -> float:
        quote = self._candidate_quote(quote_lookup, venue, str(getattr(candidate, "symbol", "")))
        if quote is None:
            return 10.0
        if side == "buy":
            price = float(getattr(quote, "ask", 0.0) or 0.0)
            top_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
        else:
            price = float(getattr(quote, "bid", 0.0) or 0.0)
            top_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
        if price <= 0.0 or top_size <= 0.0:
            return 10.0
        quantity = float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0) / price
        if quantity <= 0.0:
            return 10.0
        return quantity / top_size

    def _runtime_candidate_risk_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object],
    ) -> float:
        explicit_risk = getattr(candidate, "runtime_risk_score", None)
        if explicit_risk is not None:
            return max(float(explicit_risk or 0.0), 0.0)

        long_depth = self._entry_leg_depth_score(
            candidate,
            quote_lookup,
            venue=str(getattr(candidate, "long_venue", "")),
            side="buy",
        )
        short_depth = self._entry_leg_depth_score(
            candidate,
            quote_lookup,
            venue=str(getattr(candidate, "short_venue", "")),
            side="sell",
        )
        depth_risk = max(long_depth, short_depth, 0.0)
        selection_risk = float(getattr(candidate, "selection_risk_score", 0.0) or 0.0)
        return max(depth_risk, selection_risk, 0.0)

    def _runtime_candidate_selection_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object] | None = None,
    ) -> float:
        ranking_edge = float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0)
        risk_score = self._runtime_candidate_risk_score(candidate, quote_lookup or {})
        return ranking_edge / (1.0 + max(risk_score, 0.0))

    def _candidate_final_selection_sort_key(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object] | None = None,
    ) -> tuple[float, float, float, str]:
        return (
            -self._runtime_candidate_selection_score(candidate, quote_lookup),
            -float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0),
            -float(getattr(candidate, "worst_case_edge_bps", 0.0) or 0.0),
            self._candidate_pair_id(candidate),
        )

    def _has_pending_residual_pair(self, pair_id: str) -> bool:
        for task in self.ctx.state.pending_residual_repairs:
            if isinstance(task, dict):
                task_pair_id = task.get("pair_id", "")
            else:
                task_pair_id = getattr(task, "pair_id", "")
            if str(task_pair_id) == pair_id:
                return True
        return False

    def _select_entry_candidates(
        self,
        tradeable: list,
        *,
        now_ms: int,
        remaining_slots: int,
        selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        market_quotes=None,
        admission_blocker_counts: Counter | None = None,
    ) -> list:
        """V1 select_entry_candidates_from_refs parity for the final entry list."""
        from lightfee.engine.v1_lifecycle import V1TradingLifecycle

        target = self._entry_selection_target(remaining_slots)
        if target <= 0:
            return []

        admission_reasons = {
            "entry_local_l2_waiting_for_primary_tracking",
            "bybit_trading_terms_required",
            "insufficient_balance_admission_blocked",
            "insufficient_margin_admission_blocked",
            "leverage_admission_blocked",
            "max_notional_admission_blocked",
        }
        exchange_admission_reasons = admission_reasons - {
            "entry_local_l2_waiting_for_primary_tracking",
        }

        active_symbols = {
            str(getattr(position, "symbol", ""))
            for position in self.ctx.state.open_positions.values()
        }
        active_symbols.update(
            str(getattr(pending, "symbol", ""))
            for pending in self.ctx.state.pending_entries.values()
        )
        selected_symbols: set[str] = set()
        ranked: list = []
        selected: list = []

        for candidate in tradeable:
            if not self._candidate_is_tradeable_for_selection(candidate):
                continue
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._candidate_pair_id(candidate)
            readiness_evidence: dict = {}
            lifecycle_evidence: dict = {}
            blocker = None
            admission_block = self._candidate_admission_block(candidate, now_ms)
            if admission_block:
                blocker = str(admission_block.get("reason") or "symbol_admission_blocked")
                readiness_evidence = dict(admission_block)
                if readiness_evidence.get("source"):
                    readiness_evidence["cooldown_source"] = readiness_evidence["source"]
                readiness_evidence["source"] = "initial_entry"
                readiness_evidence["candidate_pair_id"] = pair_id
                readiness_evidence["pair_id"] = pair_id
                self.ctx.journal.append(
                    "runtime.entry_admission_blocked",
                    {
                        **readiness_evidence,
                        "long_venue": getattr(candidate, "long_venue", ""),
                        "short_venue": getattr(candidate, "short_venue", ""),
                        "ts_ms": now_ms,
                    },
                )
            decision = None
            if not blocker:
                decision = V1TradingLifecycle.entry_admissibility(
                    candidate,
                    now_ms=now_ms,
                    strategy=self.ctx.config.strategy,
                    recovery_ledger=getattr(self, "recovery_ledger", None),
                    source="selection",
                )
            if decision is not None and not decision.allowed:
                lifecycle_evidence = dict(getattr(decision, "evidence", {}) or {})
                blocker = decision.reason
            first_funding_ts = getattr(candidate, "first_funding_timestamp_ms", 0)
            if not blocker:
                blocker = (
                    self._entry_finalization_window_blocker(first_funding_ts, now_ms)
                    if first_funding_ts > 0
                    else None
                )
            if not blocker:
                blocker, readiness_evidence = (
                    self._entry_ws_bbo_subscription_blocker(candidate)
                )
                if not blocker:
                    readiness = self.ctx.entry_readiness_provider.decide(
                        candidate,
                        now_ms,
                        market_quotes=market_quotes,
                    )
                    readiness_evidence = dict(getattr(readiness, "evidence", {}) or {})
                    blocker = None if readiness.allowed else (
                        readiness.reason or "entry_readiness_provider_denied"
                    )
            if blocker:
                blocker_str = str(blocker)
                ws_bbo_blocker = blocker_str.startswith("entry_ws_bbo_quote_lease_")
                admission_selection_blocker = blocker_str in exchange_admission_reasons
                # Admission buckets (not primary tracked) vs readiness failures
                if blocker_str in admission_reasons:
                    if admission_blocker_counts is not None:
                        admission_blocker_counts[blocker_str] += 1
                else:
                    selection_blocker_counts[blocker_str] += 1
                candidate_blockers[pair_id] = blocker_str
                if blocker_str not in {
                    "entry_waiting_for_finalization_window_too_early",
                    "entry_finalization_window_expired",
                }:
                    diagnostic_payload = {
                        "symbol": symbol,
                        "pair_id": pair_id,
                        "reason": blocker_str,
                        "ts_ms": now_ms,
                    }
                    if lifecycle_evidence:
                        diagnostic_payload["lifecycle_evidence"] = lifecycle_evidence
                    if readiness_evidence:
                        if admission_selection_blocker:
                            provider_name = self._entry_readiness_provider_name()
                            readiness_evidence.setdefault("provider", provider_name)
                            readiness_evidence.setdefault("source", "entry_admission")
                            readiness_evidence.setdefault("domain", "entry_admission")
                            readiness_evidence.setdefault(
                                "blocker_family",
                                "exchange_admission",
                            )
                            diagnostic_payload.update({
                                "provider": provider_name,
                                "source": "entry_admission",
                                "domain": "entry_admission",
                                "blocker_family": "exchange_admission",
                            })
                        elif ws_bbo_blocker:
                            readiness_evidence.setdefault("provider", "ws_bbo_quote_lease")
                            readiness_evidence.setdefault("source", "ws_bbo_quote_lease")
                            domain = (
                                "ws_bbo_subscription"
                                if blocker_str in {
                                    "entry_ws_bbo_quote_lease_waiting_for_subscription",
                                    "entry_ws_bbo_quote_lease_budget_exhausted",
                                }
                                else "ws_bbo_cache"
                            )
                            readiness_evidence.setdefault("domain", domain)
                            readiness_evidence.setdefault(
                                "blocker_family",
                                self._ws_bbo_selection_blocker_family(blocker_str),
                            )
                            diagnostic_payload.update({
                                "provider": "ws_bbo_quote_lease",
                                "source": "ws_bbo_quote_lease",
                                "domain": readiness_evidence["domain"],
                                "blocker_family": readiness_evidence["blocker_family"],
                            })
                        diagnostic_payload["readiness_evidence"] = readiness_evidence
                    if blocker_str in self._V1_ENTRY_LIFECYCLE_SELECTION_BLOCKERS:
                        event_kind = "runtime.entry_blocked_lifecycle_selection"
                    elif admission_selection_blocker:
                        event_kind = "runtime.entry_blocked_admission_selection"
                    elif ws_bbo_blocker:
                        event_kind = "runtime.entry_blocked_ws_bbo_selection"
                    elif self._entry_readiness_provider_uses_local_l2():
                        event_kind = "runtime.entry_blocked_local_l2_selection"
                    else:
                        event_kind = "runtime.entry_blocked_ws_bbo_selection"
                    self._append_runtime_diagnostic_event(
                        event_kind,
                        diagnostic_payload,
                        now_ms=now_ms,
                        key_parts=(symbol, pair_id, blocker_str),
                        interval_ms=self._ENTRY_BLOCKED_LOCAL_L2_SELECTION_LOG_INTERVAL_MS,
                    )
                continue
            ranked.append(candidate)

        quote_lookup = self._market_quote_lookup(market_quotes)
        ranked.sort(
            key=lambda candidate: self._candidate_final_selection_sort_key(
                candidate,
                quote_lookup,
            )
        )

        for candidate in ranked:
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._candidate_pair_id(candidate)
            if symbol in active_symbols or symbol in selected_symbols:
                continue
            if self._has_pending_residual_pair(pair_id):
                continue
            selected.append(candidate)
            selected_symbols.add(symbol)
            if len(selected) >= target:
                break
        return selected

    def _entry_finalization_window_blocker(
        self,
        first_funding_timestamp_ms: int,
        now_ms: int,
    ) -> str | None:
        """V1 final entry window: entries are allowed in [min_before, entry_window]."""
        remaining_ms = first_funding_timestamp_ms - max(now_ms, 0)
        min_before_ms = self.ctx.config.strategy.min_scan_minutes_before_funding * 60_000
        entry_window_ms = self.ctx.config.strategy.entry_window_secs * 1000

        if remaining_ms <= 0 or (min_before_ms > 0 and remaining_ms < min_before_ms):
            return "entry_finalization_window_expired"
        if entry_window_ms > 0 and remaining_ms > entry_window_ms:
            return "entry_waiting_for_finalization_window_too_early"
        return None

    def _entry_local_l2_selection_blocker(self, candidate, now_ms: int) -> str | None:
        """V1 entry local L2 selection gate: check prewarm, primary tracking, dual-ready.

        Returns a reason string if blocked, or None if ready to proceed.

        V1 (Rust: market_data.rs:1518-1526, final_gate.rs entry_final_gate_result_from_candidate_local_l2):
        - Live + local_l2_enabled → gate applies
        - Candidate must be in primary tracked set
        - Session must exist for pair_id
        - Both legs must be ready (dual-ready)
        - V1 prewarm: remaining_ms = first_funding_timestamp_ms - now_ms;
          remaining_ms > 0 && remaining_ms <= prewarm_window_secs * 1000

        Blocker reasons (V1 stable labels):
        - entry_waiting_for_finalization_window_too_early
        - entry_finalization_window_expired
        - entry_local_l2_waiting_for_prewarm_window
        - entry_local_l2_waiting_for_primary_tracking
        - entry_local_l2_waiting_for_dual_ready
        """
        if self.ctx.config.runtime.mode != "live":
            return None

        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        symbol = getattr(candidate, "symbol", "")
        long_ven = str(getattr(candidate, "long_venue", ""))
        short_ven = str(getattr(candidate, "short_venue", ""))
        pair_id = getattr(candidate, "pair_id", None)
        if not pair_id:
            pair_id = make_candidate_pair_id(symbol, long_ven, short_ven)

        # V1 prewarm: remaining_ms = first_funding_timestamp_ms - now_ms
        first_funding_ts = getattr(candidate, "first_funding_timestamp_ms", 0)
        if first_funding_ts <= 0:
            if not self._local_l2_effective_enabled():
                return None
            return "entry_local_l2_waiting_for_prewarm_window"
        remaining_ms = first_funding_ts - max(now_ms, 0)
        finalization_blocker = self._entry_finalization_window_blocker(
            first_funding_ts,
            now_ms,
        )
        if finalization_blocker:
            return finalization_blocker
        if not self._local_l2_effective_enabled():
            return None
        prewarm_window_ms = self.ctx.config.strategy.entry_local_l2_prewarm_window_secs * 1000
        if remaining_ms <= 0 or remaining_ms > prewarm_window_ms:
            return "entry_local_l2_waiting_for_prewarm_window"

        # Primary tracking: candidate must be in primary tracked set
        if pair_id not in self._tracked_primary_pair_ids:
            return "entry_local_l2_waiting_for_primary_tracking"

        # Session dual-ready check
        session = self.ctx.entry_l2_sessions.sessions.get(pair_id)
        if session is None:
            return "entry_local_l2_waiting_for_dual_ready"

        if not session.both_legs_ready(now_ms, stale_after_ms=self._entry_local_l2_stale_after_ms()):
            return "entry_local_l2_waiting_for_dual_ready"

        return None
