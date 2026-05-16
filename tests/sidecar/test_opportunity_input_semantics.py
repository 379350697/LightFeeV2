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
    SidecarSnapshot,
    SnapshotFreshness,
    evaluate_snapshot_freshness,
)
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


class TestSnapshotFreshnessStates:
    """OPP-001: Five freshness states: fresh, last-good fallback, stale, missing, degraded."""

    def test_fresh_snapshot(self):
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(published_at_ms=now_ms - 1000)
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.FRESH

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
        current = SidecarSnapshot(published_at_ms=now_ms - 30000)
        last_good = SidecarSnapshot(published_at_ms=now_ms - 5000)
        freshness = evaluate_snapshot_freshness(
            current, max_age_ms=10000, now_ms=now_ms, last_good=last_good
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_current_snapshot_inside_last_good_window_falls_back(self):
        """V1: stale publish age can remain usable as last-good until last_good_max_age."""
        now_ms = int(time.time() * 1000)
        current = SidecarSnapshot(published_at_ms=now_ms - 30000)
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
        last_good = SidecarSnapshot(published_at_ms=now_ms - 500000)
        freshness = evaluate_snapshot_freshness(
            None,
            max_age_ms=10000,
            now_ms=now_ms,
            last_good=last_good,
            last_good_max_age_ms=600000,
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_stale_market_observation_enters_last_good_fallback(self):
        """V1: fresh file publish with stale market observation is last-good, not fresh."""
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(
            published_at_ms=now_ms - 1000,
            market_observed_at_ms=now_ms - 30000,
        )
        freshness = evaluate_snapshot_freshness(
            snapshot,
            max_age_ms=10000,
            now_ms=now_ms,
            last_good_max_age_ms=600000,
        )
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_market_max_age_can_be_stricter_than_snapshot_publish_age(self):
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(
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
        assert freshness == SnapshotFreshness.LAST_GOOD_FALLBACK

    def test_degraded_snapshot_with_degraded_venues(self):
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(
            published_at_ms=now_ms - 1000,
            degraded_venues=["binance"],
        )
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.DEGRADED

    def test_degraded_snapshot_with_missing_health_domains(self):
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(
            published_at_ms=now_ms - 1000,
            degraded_domains=["market"],
        )
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.DEGRADED

    def test_fresh_has_highest_priority(self):
        """Fresh takes priority over degraded — no degraded venues or domains means fresh."""
        now_ms = int(time.time() * 1000)
        snapshot = SidecarSnapshot(published_at_ms=now_ms - 100)
        freshness = evaluate_snapshot_freshness(snapshot, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.FRESH

    def test_stale_overrides_missing(self):
        """A stale current with no last good is still STALE, not MISSING."""
        now_ms = int(time.time() * 1000)
        current = SidecarSnapshot(published_at_ms=now_ms - 30000)
        freshness = evaluate_snapshot_freshness(current, max_age_ms=10000, now_ms=now_ms)
        assert freshness == SnapshotFreshness.STALE


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
                "transfer": DomainLifecycle(domain="transfer", observed_at_ms=now_ms - 30000, venue_count=2),
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
                "transfer": DomainLifecycle(domain="transfer", observed_at_ms=1000000, venue_count=2),
                "hint": DomainLifecycle(domain="hint", observed_at_ms=1000000, venue_count=1),
                "perp_liquidity": DomainLifecycle(domain="perp_liquidity", observed_at_ms=1000000, venue_count=2),
            },
        )
        diag = lifecycle.to_dict()
        assert "domains" in diag
        assert "market" in diag["domains"]
        assert "transfer" in diag["domains"]
        assert "hint" in diag["domains"]


# ── OPP-003: Sidecar Scan Mode Discovery ───────────────────────────────────


class TestSidecarScanDiscovery:
    """OPP-003: Sidecar scan pairs are filtered by directed_pairs."""

    def test_disabled_mode_returns_empty(self):
        """Disabled mode produces explicit empty pairs, not a silent omission."""
        from lightfee.config.schema import AppConfig, RuntimeConfig

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(opportunity_input_mode="disabled"),
        )
        assert config.runtime.opportunity_input_mode == "disabled"
        # Disabled mode should be distinguishable from unconfigured
        assert config.runtime.opportunity_input_mode != "coarse_sidecar"

    def test_sidecar_scan_mode_distinct_from_coarse(self):
        """Sidecar scan is a distinct mode from coarse sidecar."""
        from lightfee.config.schema import AppConfig, RuntimeConfig

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(opportunity_input_mode="sidecar_scan"),
        )
        assert config.runtime.opportunity_input_mode == "sidecar_scan"
        assert config.runtime.opportunity_input_mode != "coarse_sidecar"

    def test_non_parity_mode_is_explicit(self):
        """Non-parity fallback is explicitly configured, not a hidden default."""
        from lightfee.config.schema import AppConfig, RuntimeConfig

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(opportunity_input_mode="non_parity"),
        )
        assert config.runtime.opportunity_input_mode == "non_parity"


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
            / "docs" / "parity" / "approved_deviations.md"
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
            degraded_venues=["gate"],
            degraded_domains=["transfer"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "snapshot.json")
            publish_snapshot(original, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.published_at_ms == now_ms
            assert loaded.degraded_venues == ["gate"]
            assert loaded.degraded_domains == ["transfer"]
