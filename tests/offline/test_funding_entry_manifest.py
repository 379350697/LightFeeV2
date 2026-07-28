"""Offline-only reader coverage for retired funding-entry manifests."""

from __future__ import annotations

import json
from hashlib import sha256

from lightfee.offline.funding_entry_manifest import (
    legacy_manifest_path,
    read_legacy_funding_entry_manifest,
)


def _write_v7_fixture(snapshot_path) -> tuple[dict[str, object], object]:
    generation_id = "a" * 64
    page = {"schema_version": 5, "candidates": [], "quotes": {}}
    raw_page = json.dumps(page, separators=(",", ":")).encode()
    base = snapshot_path.with_name(f"{snapshot_path.name}.funding-entry-v7.json")
    page_name = f"{base.name}.{generation_id}.page-00000.json"
    page_path = base.with_name(page_name)
    page_path.write_bytes(raw_page)
    manifest: dict[str, object] = {
        "schema_version": 7,
        "generation_id": generation_id,
        "page_count": 1,
        "pages": [
            {
                "page_index": 0,
                "payload_path": page_name,
                "payload_sha256": sha256(raw_page).hexdigest(),
            }
        ],
    }
    legacy_manifest_path(snapshot_path).write_text(json.dumps(manifest))
    return manifest, page_path


def test_offline_reader_retains_v7_manifest_analysis_without_live_publisher(tmp_path) -> None:
    snapshot_path = tmp_path / "sidecar.json"
    expected_manifest, _page_path = _write_v7_fixture(snapshot_path)

    result = read_legacy_funding_entry_manifest(snapshot_path)

    assert result is not None
    assert result["manifest"] == expected_manifest
    assert result["pages"] == [{"schema_version": 5, "candidates": [], "quotes": {}}]


def test_offline_reader_rejects_tampered_v7_page_without_v6_downgrade(tmp_path) -> None:
    snapshot_path = tmp_path / "sidecar.json"
    _manifest, page_path = _write_v7_fixture(snapshot_path)
    page_path.write_text("{}")
    legacy_manifest_path(snapshot_path, 6).write_text("{}")

    assert read_legacy_funding_entry_manifest(snapshot_path) is None
