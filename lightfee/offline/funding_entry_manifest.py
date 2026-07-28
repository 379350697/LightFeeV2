"""Read retired funding-entry manifests for offline incident analysis only.

The live sidecar publishes one atomically replaced V5 snapshot.  These helpers
recognise the historical V6/V7 manifest files without importing the sidecar
publisher or constructing live admission objects.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any


_SUPPORTED_SCHEMA_VERSIONS = frozenset({6, 7})


def _base_path(snapshot_path: str | Path, schema_version: int) -> Path:
    target = Path(snapshot_path)
    return target.with_name(f"{target.name}.funding-entry-v{schema_version}.json")


def legacy_manifest_path(snapshot_path: str | Path, schema_version: int = 7) -> Path:
    """Return the historical manifest location for a V6 or V7 snapshot."""
    if schema_version not in _SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported legacy manifest schema: {schema_version}")
    base = _base_path(snapshot_path, schema_version)
    return base.with_name(f"{base.name}.manifest.json")


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _generation_id(manifest: dict[str, Any]) -> str | None:
    value = manifest.get("generation_id")
    if not isinstance(value, str) or len(value) != 64:
        return None
    return value if all(character in "0123456789abcdef" for character in value) else None


def _page_names(
    snapshot_path: str | Path,
    manifest: dict[str, Any],
) -> list[str] | None:
    schema_version = manifest.get("schema_version")
    if schema_version not in _SUPPORTED_SCHEMA_VERSIONS:
        return None
    generation_id = _generation_id(manifest)
    if generation_id is None:
        return None
    base = _base_path(snapshot_path, int(schema_version))
    if schema_version == 6:
        payload_name = manifest.get("payload_path")
        expected = f"{base.name}.{generation_id}.json"
        return [expected] if payload_name == expected else None
    pages = manifest.get("pages")
    if not isinstance(pages, list) or not pages:
        return None
    names: list[str] = []
    for index, page in enumerate(pages):
        if not isinstance(page, dict) or page.get("page_index") != index:
            return None
        expected = f"{base.name}.{generation_id}.page-{index:05d}.json"
        if page.get("payload_path") != expected:
            return None
        names.append(expected)
    return names


def read_legacy_funding_entry_manifest(snapshot_path: str | Path) -> dict[str, Any] | None:
    """Return a verified historical manifest and pages, or ``None``.

    A malformed V7 manifest remains authoritative: the reader deliberately
    does not fall back to V6 once a V7 manifest path exists.  This mirrors the
    old on-disk safety property while keeping the result offline-only.
    """
    modern_path = legacy_manifest_path(snapshot_path, 7)
    selected_path = modern_path if modern_path.exists() else legacy_manifest_path(snapshot_path, 6)
    manifest = _read_object(selected_path)
    if manifest is None:
        return None
    names = _page_names(snapshot_path, manifest)
    if names is None:
        return None
    base = _base_path(snapshot_path, int(manifest["schema_version"]))
    descriptors = manifest.get("pages") if manifest["schema_version"] == 7 else [manifest]
    pages: list[dict[str, Any]] = []
    for name, descriptor in zip(names, descriptors):
        assert isinstance(descriptor, dict)
        payload = base.with_name(name)
        try:
            raw = payload.read_bytes()
        except OSError:
            return None
        if descriptor.get("payload_sha256") != sha256(raw).hexdigest():
            return None
        page = _read_object(payload)
        if page is None:
            return None
        pages.append(page)
    return {"manifest": manifest, "pages": pages}
