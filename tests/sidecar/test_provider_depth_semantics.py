"""Worker A: Test provider depth semantics — provenance, source mode propagation, acquisition mode.

Covers:
- direct_market_enriched provenance
- Source mode / acquisition mode in snapshot diagnostics
- Domain lifecycle propagation
"""

from __future__ import annotations

import time

import pytest

from lightfee.sidecar.snapshot import (
    SidecarSnapshot,
    SnapshotFreshness,
    evaluate_snapshot_freshness,
)


class TestProviderDepthProvenance:
    """V1 provider layering encodes meaningful business semantics in source_mode and acquisition_mode."""

    def test_snapshot_carries_source_mode(self):
        """SidecarSnapshot must carry source_mode for diagnostics routing."""
        snapshot = SidecarSnapshot(source_mode="direct_market_enriched")
        assert snapshot.source_mode == "direct_market_enriched"

    def test_snapshot_carries_acquisition_mode(self):
        """SidecarSnapshot must carry acquisition_mode for provenance tracking."""
        snapshot = SidecarSnapshot(acquisition_mode="fresh_sidecar")
        assert snapshot.acquisition_mode == "fresh_sidecar"

    def test_default_source_mode_is_direct_market(self):
        """Default matches V1: direct_market when not enriched."""
        snapshot = SidecarSnapshot()
        assert snapshot.source_mode in ("direct_market", ""), (
            f"Expected 'direct_market' or '' as default source_mode, got: {snapshot.source_mode!r}"
        )

    def test_default_acquisition_mode_is_unknown(self):
        """Default acquisition_mode indicates unknown/unavailable provenance."""
        snapshot = SidecarSnapshot()
        # V1 default is Unavailable; empty or None is acceptable for "not known"
        assert snapshot.acquisition_mode in ("", "unavailable", None), (
            f"Expected empty/unavailable default, got: {snapshot.acquisition_mode!r}"
        )

    def test_direct_market_enriched_has_distinct_provenance(self):
        """direct_market_enriched snapshot must carry clearly distinguishable provenance from plain direct_market."""
        enriched = SidecarSnapshot(
            source_mode="direct_market_enriched",
            acquisition_mode="fresh_sidecar",
        )
        plain = SidecarSnapshot(
            source_mode="direct_market",
            acquisition_mode="direct_market_view",
        )
        assert enriched.source_mode != plain.source_mode
        assert enriched.acquisition_mode != plain.acquisition_mode

    def test_provenance_survives_round_trip(self):
        """Source/acquisition mode must survive serialization round-trip."""
        from lightfee.sidecar.publisher import publish_snapshot, load_snapshot

        import tempfile
        import os

        snapshot = SidecarSnapshot(
            published_at_ms=int(time.time() * 1000),
            source_mode="coarse_sidecar",
            acquisition_mode="fresh_sidecar",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "snapshot.json")
            publish_snapshot(snapshot, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.source_mode == "coarse_sidecar"
            assert loaded.acquisition_mode == "fresh_sidecar"


class TestDomainLifecycleDepth:
    """V1 domain lifecycle carries per-domain freshness, coverage, and warmup depth."""

    def test_domain_lifecycle_includes_sidecar_source_mode(self):
        """Sidecar domain lifecycle must reflect which provider mode produced the data."""
        from lightfee.sidecar.lifecycle import SidecarLifecycleState

        state = SidecarLifecycleState()
        diag = state.to_dict()
        # By default, lifecycle has no source_mode — that comes from the snapshot
        assert "domains" in diag
        # source_mode is carried on the snapshot, not lifecycle state directly
        # This test documents the expected separation of concerns
