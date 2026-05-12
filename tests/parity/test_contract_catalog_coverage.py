"""Gate 0: Contract catalog coverage guard.

Fails if:
- A required area is missing from the catalog.
- A catalog entry has neither a focused test path nor a deviation id.
- A deviation id referenced in the catalog does not exist in approved_deviations.md.
"""

from __future__ import annotations

import re

import pytest

# ── Area extraction from catalog ──────────────────────────────────────────

AREA_RE = re.compile(r"\*\*area:\*\*\s*(\S+)", re.IGNORECASE)


def test_catalog_exists(catalog_text: str) -> None:
    """Catalog file is present and non-empty."""
    assert len(catalog_text.strip()) > 0, "Catalog is empty"


def test_deviations_file_exists(deviations_text: str) -> None:
    """Approved deviations file is present and non-empty."""
    assert len(deviations_text.strip()) > 0, "Approved deviations file is empty"


def test_all_required_areas_present(
    catalog_text: str, required_areas: set[str]
) -> None:
    """Every required area has at least one contract entry in the catalog."""
    found_areas: set[str] = set()
    for m in AREA_RE.finditer(catalog_text):
        found_areas.add(m.group(1).lower())

    missing = required_areas - found_areas
    assert not missing, (
        f"Required areas missing from catalog: {sorted(missing)}\n"
        f"Areas found: {sorted(found_areas)}"
    )


def test_catalog_entries_have_test_or_deviation(
    catalog_text: str,
    catalog_contract_ids: set[str],
    catalog_deviation_ids: set[str],
) -> None:
    """Every catalog entry has a focused test path or a deviation id."""
    # Parse each contract entry block
    entries = _parse_catalog_entries(catalog_text)

    uncovered: list[str] = []
    for entry_id, fields in entries.items():
        test_path = fields.get("focused_test_path") or fields.get("test_path") or ""
        has_test = test_path and test_path != "n/a"
        deviation_id = fields.get("deviation_id") or ""
        has_deviation = deviation_id and deviation_id not in ("—", "-", "n/a")
        if not has_test and not has_deviation:
            uncovered.append(entry_id)

    assert not uncovered, (
        f"Catalog entries with neither test path nor deviation id: {uncovered}\n"
        f"Each entry needs 'focused test path' or 'deviation id'"
    )


def test_deviation_ids_are_defined(
    catalog_deviation_ids: set[str],
    ledger_deviation_ids: set[str],
) -> None:
    """Every deviation id referenced in the catalog exists in approved_deviations.md."""
    missing = catalog_deviation_ids - ledger_deviation_ids
    assert not missing, (
        f"Deviation IDs in catalog but not in approved_deviations.md: {sorted(missing)}"
    )


def test_deviation_ids_in_ledger_are_referenced(
    catalog_deviation_ids: set[str],
    ledger_deviation_ids: set[str],
) -> None:
    """Every deviation id in the ledger is referenced from at least one catalog entry.

    This prevents orphan deviations that accumulate without being tied to a contract.
    """
    orphan = ledger_deviation_ids - catalog_deviation_ids
    assert not orphan, (
        f"Deviation IDs in approved_deviations.md but not referenced in catalog: {sorted(orphan)}"
    )


def test_contract_ids_follow_naming_convention(catalog_contract_ids: set[str]) -> None:
    """All contract IDs follow AREA-NNN format with known area prefixes."""
    valid_prefixes = {
        "CONFIG", "STARTUP", "LANE", "OPP", "VENUE", "MD", "L2",
        "ENTRY", "CLOSE", "PCLOSE", "RISK", "STATE", "JRNL", "REC",
        "REPLAY", "ANAL", "EVOL", "OPS",
    }
    for cid in catalog_contract_ids:
        parts = cid.split("-")
        assert len(parts) == 2, f"Invalid contract ID format: {cid}"
        assert parts[0] in valid_prefixes, f"Unknown area prefix in {cid}: {parts[0]}"
        assert parts[1].isdigit() and len(parts[1]) == 3, (
            f"Invalid sequence number in {cid}: {parts[1]}"
        )


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_catalog_entries(text: str) -> dict[str, dict[str, str]]:
    """Parse the catalog markdown into a dict of entry_id -> field_dict."""
    entries: dict[str, dict[str, str]] = {}
    current_id: str | None = None
    current_fields: dict[str, str] = {}

    for line in text.splitlines():
        # Match "### PREFIX-NNN" headers
        m = re.match(r"^###\s+([A-Z0-9]+-\d{3})", line)
        if m:
            if current_id is not None:
                entries[current_id] = current_fields
            current_id = m.group(1)
            current_fields = {}
            continue

        if current_id is None:
            continue

        # Match field lines: "- **field:** value"
        fm = re.match(r"^- \*\*([^*]+):\*\*\s*(.*)", line)
        if fm:
            key = fm.group(1).strip().lower().replace(" ", "_")
            val = fm.group(2).strip()
            current_fields[key] = val

    if current_id is not None:
        entries[current_id] = current_fields

    return entries
