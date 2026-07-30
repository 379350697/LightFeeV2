"""V1 semantic parity tests for sidecar opportunity input: freshness, health domains, scan discovery.

Coverage targets from v1_semantic_contract_catalog.md:
  OPP-001: sidecar snapshot freshness states
  OPP-002: health domain diagnostics
  OPP-003: sidecar scan mode discovery
  OPP-004 / DEV-001: Chillybot removal as approved deviation
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

import pytest

from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
    TransferLifecycle,
    evaluate_snapshot_freshness,
    decide_snapshot_freshness,
    has_usable_funding_payload,
)


def test_funding_payload_accepts_staggered_candidate_but_not_single_leg_quote():
    staggered = CandidateInput(
        long_venue="binance",
        short_venue="okx",
        symbol="BTCUSDT",
        funding_diff_bps=3.0,
        funding_edge_bps=3.0,
        expected_edge_bps=2.0,
        worst_case_edge_bps=1.0,
        ranking_edge_bps=1.0,
        funding_timestamp_ms=2_000,
        first_funding_timestamp_ms=2_000,
        long_funding_timestamp_ms=2_000,
        short_funding_timestamp_ms=3_000,
        opportunity_type="staggered",
        interval_aligned=False,
    )
    assert has_usable_funding_payload(
        SidecarSnapshot(acquisition_mode="fresh_sidecar", candidates=[staggered])
    )

    single_leg = SidecarSnapshot(
        acquisition_mode="degraded_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                funding_rate_bps=1.0,
                funding_timestamp_ms=2_000,
                funding_interval_ms=28_800_000,
            )
        },
    )
    assert not has_usable_funding_payload(single_leg)


def test_funding_fallback_decision_selects_healthy_last_good_not_bad_current():
    current = SidecarSnapshot(
        published_at_ms=1_000,
        market_observed_at_ms=1_000,
        acquisition_mode="degraded_sidecar",
        quotes={},
    )
    last_good = SidecarSnapshot(
        published_at_ms=1_900,
        market_observed_at_ms=1_900,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"{venue}:BTCUSDT": QuoteSnapshot(
                venue=venue,
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                funding_rate_bps=rate,
                funding_timestamp_ms=3_000,
                funding_interval_ms=28_800_000,
            )
            for venue, rate in (("binance", 1.0), ("okx", 2.0))
        },
    )

    decision = decide_snapshot_freshness(
        current,
        max_age_ms=100,
        now_ms=2_000,
        last_good=last_good,
        last_good_max_age_ms=5_000,
        usable_payload=has_usable_funding_payload,
    )

    assert decision.freshness == SnapshotFreshness.LAST_GOOD_FALLBACK
    assert decision.snapshot is last_good
from lightfee.sidecar.lifecycle import (
    DomainStatus,
    DomainLifecycle,
    SidecarLifecycleState,
    create_domain_lifecycle,
)
from lightfee.config.compatibility import (
    CHILLYBOT_FIELDS,
    REMOVED_FIELD_MESSAGES,
)
from lightfee.config.validation import check_raw_toml_for_chillybot


# ── OPP-001: Sidecar Snapshot Freshness States ──────────────────────────────


def _usable_snapshot(*, published_at_ms: int, **kwargs) -> SidecarSnapshot:
    market_observed_at_ms = kwargs.pop("market_observed_at_ms", published_at_ms)
    candidate_build_observed_at_ms = kwargs.pop(
        "candidate_build_observed_at_ms", published_at_ms
    )
    return SidecarSnapshot(
        published_at_ms=published_at_ms,
        market_observed_at_ms=market_observed_at_ms,
        candidate_build_observed_at_ms=candidate_build_observed_at_ms,
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=market_observed_at_ms,
            )
        },
        **kwargs,
    )


class TestSnapshotFreshnessStates:
    """OPP-001: Five freshness states: fresh, last-good fallback, stale, missing, degraded."""

    def test_fresh_snapshot(self):
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(published_at_ms=now_ms - 1000)
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.FRESH

    @pytest.mark.parametrize("publish_age_ms", (9_000, 15_000))
    def test_normal_slow_publication_remains_available_below_30_second_bound(
        self, publish_age_ms
    ):
        """The 10-second producer SLO is not a last-good/entry boundary."""
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - publish_age_ms,
            market_observed_at_ms=now_ms - publish_age_ms,
        )

        decision = decide_snapshot_freshness(
            snapshot,
            max_age_ms=30_000,
            market_max_age_ms=30_000,
            now_ms=now_ms,
            last_good_max_age_ms=600_000,
        )

        assert decision.freshness == SnapshotFreshness.FRESH
        assert decision.snapshot is snapshot

    def test_publication_enters_v1_last_good_only_after_30_second_availability_bound(
        self,
    ):
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - 30_001,
            market_observed_at_ms=now_ms - 30_001,
        )

        decision = decide_snapshot_freshness(
            snapshot,
            max_age_ms=30_000,
            market_max_age_ms=30_000,
            now_ms=now_ms,
            last_good_max_age_ms=600_000,
        )

        assert decision.freshness == SnapshotFreshness.LAST_GOOD_FALLBACK
        assert decision.snapshot is snapshot

    def test_stale_snapshot(self):
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(published_at_ms=now_ms - 30000)
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.STALE

    def test_missing_snapshot(self):
        now_ms = int(time.time() * 1000)
        freshness = evaluate_snapshot_freshness(None, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.MISSING

    def test_last_good_fallback(self):
        """When current is stale but a recent valid (last good) snapshot exists."""
        now_ms = int(time.time() * 1000)
        current = _usable_snapshot(published_at_ms=now_ms - 30000)
        last_good = _usable_snapshot(published_at_ms=now_ms - 5000)
        freshness = evaluate_snapshot_freshness(
            current, max_age_ms=10000, now_ms=now_ms, last_good=last_good
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_current_snapshot_inside_last_good_window_falls_back(self):
        """V1: stale publish age can remain usable as last-good until last_good_max_age."""
        now_ms = int(time.time() * 1000)
        current = _usable_snapshot(published_at_ms=now_ms - 30000)
        freshness = evaluate_snapshot_freshness(
            current,
            max_age_ms=10000,
            now_ms=now_ms,
            last_good_max_age_ms=600000,
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_missing_snapshot_uses_live_scan_last_good_max_age(self):
        """V1: cached last-good age is governed by live_scan_last_good_max_age_ms."""
        now_ms = int(time.time() * 1000)
        last_good = _usable_snapshot(published_at_ms=now_ms - 500000)
        freshness = evaluate_snapshot_freshness(
            None,
            max_age_ms=10000,
            now_ms=now_ms,
            last_good=last_good,
            last_good_max_age_ms=600000,
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_stale_market_observation_is_not_global_last_good_fallback(self):
        """A slow broad scan must not replace current candidates with global last-good."""
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - 1000,
            market_observed_at_ms=now_ms - 30000,
        )
        freshness = evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=10000,
            now_ms=now_ms,
            last_good_max_age_ms=600000,
        )
        assert freshness == SnapshotFreshness.STALE

    def test_market_max_age_can_be_stricter_than_snapshot_publish_age(self):
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - 1000,
            market_observed_at_ms=now_ms - 30000,
        )
        freshness = evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=600000,
            now_ms=now_ms,
            last_good_max_age_ms=600000,
            market_max_age_ms=5000,
        )
        assert freshness == SnapshotFreshness.STALE

    def test_degraded_snapshot_with_degraded_venues(self):
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - 1000,
            degraded_venues=["binance"],
        )
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.DEGRADED

    def test_degraded_snapshot_with_missing_health_domains(self):
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(
            published_at_ms=now_ms - 1000,
            degraded_domains=["market"],
        )
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.DEGRADED

    def test_fresh_has_highest_priority(self):
        """Fresh takes priority over degraded — no degraded venues or domains means fresh."""
        now_ms = int(time.time() * 1000)
        snapshot = _usable_snapshot(published_at_ms=now_ms - 100)
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.FRESH

    def test_stale_overrides_missing(self):
        """A stale current with no last good is still STALE, not MISSING."""
        now_ms = int(time.time() * 1000)
        current = SidecarSnapshot(published_at_ms=now_ms - 30000)
        freshness = evaluate_snapshot_freshness(current, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.STALE

    @pytest.mark.parametrize(
        "mutation",
        [
            {"published_at_ms": 20_001},
            {"market_observed_at_ms": 20_001},
            {"candidate_build_observed_at_ms": 20_001},
        ],
    )
    def test_future_snapshot_watermarks_are_never_fresh(self, mutation):
        values = {"published_at_ms": 20_000, **mutation}
        snapshot = SidecarSnapshot(**values)
        last_good = _usable_snapshot(
            published_at_ms=19_000,
            market_observed_at_ms=19_000,
            candidate_build_observed_at_ms=19_000,
        )

        freshness = evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=10_000,
            now_ms=20_000,
            last_good=last_good,
        )

        assert freshness == SnapshotFreshness.STALE

    def test_unavailable_current_never_uses_last_good_window(self):
        snapshot = SidecarSnapshot(
            published_at_ms=1_000,
            market_observed_at_ms=1_000,
            candidate_build_observed_at_ms=1_000,
            acquisition_mode="unavailable",
            degraded_venues=["binance"],
        )

        assert evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=100,
            now_ms=2_000,
            last_good_max_age_ms=5_000,
        ) == SnapshotFreshness.DEGRADED

    def test_fresh_age_unavailable_is_still_degraded(self):
        snapshot = SidecarSnapshot(
            published_at_ms=1_950,
            market_observed_at_ms=1_950,
            candidate_build_observed_at_ms=1_950,
            acquisition_mode="unavailable",
            degraded_venues=["binance"],
        )

        assert evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=100,
            now_ms=2_000,
        ) == SnapshotFreshness.DEGRADED

    def test_unavailable_snapshot_is_never_accepted_as_last_good(self):
        unavailable = SidecarSnapshot(
            published_at_ms=1_000,
            acquisition_mode="unavailable",
            degraded_venues=["binance"],
        )

        assert evaluate_snapshot_freshness(
            None,
            max_age_ms=100,
            now_ms=2_000,
            last_good=unavailable,
            last_good_max_age_ms=5_000,
        ) == SnapshotFreshness.MISSING


# ── OPP-002: Health Domain Diagnostics ─────────────────────────────────────


class TestHealthDomainDiagnostics:
    """OPP-002: Market, transfer, hint, and perp-liquidity health domains survive into runtime diagnostics."""

    def test_all_domains_independent(self):
        lifecycle = SidecarLifecycleState()
        lifecycle.domains["market"] = create_domain_lifecycle("market")
        lifecycle.domains["transfer"] = create_domain_lifecycle("transfer")
        lifecycle.domains["hint"] = create_domain_lifecycle("hint")
        lifecycle.domains["perp_liquidity"] = create_domain_lifecycle("perp_liquidity")

        assert "market" in lifecycle.domains
        assert "transfer" in lifecycle.domains
        assert "hint" in lifecycle.domains
        assert "perp_liquidity" in lifecycle.domains

    def test_domain_fresh_when_within_max_age(self):
        now_ms = int(time.time() * 1000)
        domain = DomainLifecycle(domain="market", observed_at_ms=now_ms - 1000, venue_count=3)
        status = domain.evaluate(now_ms, max_age_ms=10000)
        assert status == DomainStatus.FRESH

    def test_domain_stale_when_exceeds_max_age(self):
        now_ms = int(time.time() * 1000)
        domain = DomainLifecycle(domain="market", observed_at_ms=now_ms - 30000, venue_count=3)
        status = domain.evaluate(now_ms, max_age_ms=10000)
        assert status == DomainStatus.STALE

    def test_domain_unknown_when_no_observation(self):
        now_ms = int(time.time() * 1000)
        domain = DomainLifecycle(domain="hint", observed_at_ms=0, venue_count=0)
        status = domain.evaluate(now_ms, max_age_ms=10000)
        assert status == DomainStatus.UNKNOWN

    def test_degraded_domain_tracked_separately(self):
        now_ms = int(time.time() * 1000)
        lifecycle = SidecarLifecycleState(
            domains={
                "market": DomainLifecycle(domain="market", observed_at_ms=now_ms, venue_count=3),
                "transfer": DomainLifecycle(
                    domain="transfer", observed_at_ms=now_ms - 30000, venue_count=2
                ),
            },
            degraded_venues=["binance"],
        )
        fresh = lifecycle.fresh_domains(now_ms, max_age_ms=10000)
        stale = lifecycle.stale_domains(now_ms, max_age_ms=10000)
        assert "market" in fresh
        assert "transfer" in stale
        assert lifecycle.any_degraded()

    def test_health_domains_appear_in_diagnostics_snapshot(self):
        """Health domain state must be serializable for current-state export."""
        lifecycle = SidecarLifecycleState(
            domains={
                "market": DomainLifecycle(domain="market", observed_at_ms=1000000, venue_count=3),
                "transfer": DomainLifecycle(
                    domain="transfer", observed_at_ms=1000000, venue_count=2
                ),
                "hint": DomainLifecycle(domain="hint", observed_at_ms=1000000, venue_count=1),
                "perp_liquidity": DomainLifecycle(
                    domain="perp_liquidity", observed_at_ms=1000000, venue_count=2
                ),
            },
        )
        diag = lifecycle.to_dict()
        assert "domains" in diag
        assert "market" in diag["domains"]
        assert "transfer" in diag["domains"]
        assert "hint" in diag["domains"]


# ── OPP-003: Sidecar Scan Mode Discovery ───────────────────────────────────


class TestSidecarScanDiscovery:
    """Funding opportunities have one production input: the sidecar snapshot."""

    def test_runtime_has_no_compatibility_input_mode(self):
        from lightfee.config.schema import RuntimeConfig

        config = RuntimeConfig()

        assert "opportunity_input_mode" not in config.__dataclass_fields__


# ── DEV-001: Chillybot Removal is Explicit ─────────────────────────────────


class TestChillybotRemovalIsExplicit:
    """OPP-004 / DEV-001: Chillybot-era inputs intentionally removed, not silently omitted."""

    def test_chillybot_fields_are_enumerated(self):
        """All Chillybot fields are catalogued for explicit rejection."""
        assert len(CHILLYBOT_FIELDS) > 0

    def test_chillybot_api_base_rejected(self):
        raw = {"runtime": {"chillybot_api_base": "https://api.chillybot.xyz"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_chillybot_timeout_rejected(self):
        raw = {"runtime": {"chillybot_timeout_ms": 2000}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_opportunity_source_chillybot_rejected(self):
        raw = {"runtime": {"opportunity_source": "chillybot_via_feedgrab"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_each_chillybot_field_has_message(self):
        """Every removed Chillybot field must have a migration message."""
        for field_name in ("chillybot_api_base", "chillybot_timeout_ms", "sidecar_chillybot_mode"):
            # These specific fields must appear in messages
            assert field_name in REMOVED_FIELD_MESSAGES or any(
                field_name in k for k in REMOVED_FIELD_MESSAGES
            ), f"Missing migration message for {field_name}"

    def test_chillybot_removal_deviation_exists(self):
        """DEV-001 must be referenced in the approved deviations."""
        deviations_path = (
            Path(__file__).resolve().parent.parent.parent
            / "docs"
            / "parity"
            / "approved_deviations.md"
        )
        text = deviations_path.read_text()
        assert "DEV-001" in text
        assert "Chillybot" in text


# ── Snapshot load/publish round-trip ───────────────────────────────────────


class TestSnapshotPersistence:
    """Snapshot serialization preserves freshness and health domain data."""

    def test_snapshot_roundtrip_preserves_freshness_fields(self):
        from lightfee.sidecar.publisher import publish_snapshot, load_snapshot

        now_ms = int(time.time() * 1000)
        original = SidecarSnapshot(
            published_at_ms=now_ms,
            market_observed_at_ms=now_ms - 100,
            candidate_build_observed_at_ms=now_ms - 50,
            candidate_build_diagnostics={
                "input_quote_count": 0,
                "requested_symbol_count": 1,
                "requested_symbols": ["BTCUSDT"],
                "requested_venues": ["gate", "okx"],
                "directional_pair_count": 0,
                "output_candidate_count": 0,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            degraded_venues=["gate", "okx"],
            degraded_domains=["transfer"],
            source_mode="direct_market",
            acquisition_mode="degraded_sidecar",
            funding_lifecycle=[
                FundingLifecycle(
                    venue=venue,
                    observed_at_ms=now_ms - 100,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason="funding unavailable",
                )
                for venue in ("gate", "okx")
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue=venue,
                    observed_at_ms=now_ms - 100,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason="market unavailable",
                )
                for venue in ("gate", "okx")
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue=venue,
                    observed_at_ms=now_ms - 100,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason="liquidity unavailable",
                )
                for venue in ("gate", "okx")
            ],
            transfer_lifecycle=[
                TransferLifecycle(
                    from_venue="gate",
                    to_venue="okx",
                    observed_at_ms=now_ms - 100,
                    coverage_usable=0,
                    degraded_reason="transfer unavailable",
                )
            ],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "snapshot.json")
            publish_snapshot(original, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.published_at_ms == now_ms
            assert loaded.candidate_build_observed_at_ms == now_ms - 50
            assert loaded.candidate_build_diagnostics == original.candidate_build_diagnostics
            assert loaded.degraded_venues == ["gate", "okx"]
            assert loaded.degraded_domains == ["transfer"]

    def test_loader_rejects_unknown_schema_and_missing_v4_proof(self, tmp_path):
        from lightfee.sidecar.publisher import load_snapshot

        unknown = tmp_path / "unknown.json"
        unknown.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
        missing_proof = tmp_path / "missing-proof.json"
        missing_proof.write_text(
            json.dumps(
                {
                    "schema_version": 4,
                    "published_at_ms": 20_000,
                    "market_observed_at_ms": 20_000,
                    "quotes": {
                        "venue:BTCUSDT": {
                            "venue": "venue",
                            "symbol": "BTCUSDT",
                            "bid": 100.0,
                            "ask": 101.0,
                            "observed_at_ms": 20_000,
                        }
                    },
                    "candidates": [],
                }
            ),
            encoding="utf-8",
        )
        legacy_v3 = tmp_path / "legacy-v3.json"
        legacy_v3.write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "published_at_ms": 20_000,
                    "market_observed_at_ms": 20_000,
                    "quotes": {
                        "venue:BTCUSDT": {
                            "venue": "venue",
                            "symbol": "BTCUSDT",
                            "bid": 100.0,
                            "ask": 101.0,
                            "observed_at_ms": 20_000,
                        }
                    },
                    "candidates": [],
                }
            ),
            encoding="utf-8",
        )

        assert load_snapshot(unknown) is None
        assert load_snapshot(missing_proof) is None
        loaded_legacy = load_snapshot(legacy_v3)
        assert loaded_legacy is not None
        assert loaded_legacy.schema_version == 3
        assert (
            loaded_legacy.quotes["venue:BTCUSDT"].min_notional_evidence_complete
            is False
        )
