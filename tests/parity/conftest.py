"""Shared pytest fixtures for parity tests.

All workers (A-F) and the merge gates can use these fixtures. They provide
access to the contract catalog, approved deviations, and fixture builders.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


# ── Catalog and deviation access ──────────────────────────────────────────


@pytest.fixture(scope="session")
def parity_docs_dir() -> Path:
    """Path to docs/parity/."""
    return ROOT / "docs" / "parity"


@pytest.fixture(scope="session")
def catalog_path(parity_docs_dir: Path) -> Path:
    return parity_docs_dir / "v1_semantic_contract_catalog.md"


@pytest.fixture(scope="session")
def deviations_path(parity_docs_dir: Path) -> Path:
    return parity_docs_dir / "approved_deviations.md"


@pytest.fixture(scope="session")
def catalog_text(catalog_path: Path) -> str:
    """Raw text of the contract catalog."""
    if not catalog_path.exists():
        pytest.fail(f"Contract catalog not found: {catalog_path}")
    return catalog_path.read_text()


@pytest.fixture(scope="session")
def deviations_text(deviations_path: Path) -> str:
    """Raw text of the approved deviations."""
    if not deviations_path.exists():
        pytest.fail(f"Approved deviations not found: {deviations_path}")
    return deviations_path.read_text()


# ── Parsed catalog entries ────────────────────────────────────────────────

# Regex to extract contract IDs from catalog entries: CONFIG-001, ENTRY-002, etc.
CONTRACT_ID_RE = re.compile(r"^###\s+(CONFIG|STARTUP|LANE|OPP|VENUE|MD|L2|ENTRY|CLOSE|PCLOSE|RISK|STATE|JRNL|REC|REPLAY|ANAL|EVOL|OPS)-(\d{3})", re.MULTILINE)

# Regex to extract deviation IDs from catalog: DEV-001, DEV-002, etc.
CATALOG_DEV_RE = re.compile(r"\*\*deviation id:\*\*\s*(DEV-\d{3})", re.IGNORECASE)

# Regex to extract focused test paths from catalog
CATALOG_TEST_RE = re.compile(r"\*\*focused test path:\*\*\s*`([^`]+)`", re.IGNORECASE)

# Regex to extract deviation IDs from approved_deviations.md
DEVIATION_ID_RE = re.compile(r"^##\s+(DEV-\d{3})", re.MULTILINE)


@pytest.fixture(scope="session")
def catalog_contract_ids(catalog_text: str) -> set[str]:
    """Set of all contract IDs in the catalog (e.g. CONFIG-001, ENTRY-002)."""
    return {f"{m.group(1)}-{m.group(2)}" for m in CONTRACT_ID_RE.finditer(catalog_text)}


@pytest.fixture(scope="session")
def catalog_deviation_ids(catalog_text: str) -> set[str]:
    """Set of deviation IDs referenced in the catalog."""
    return {m.group(1).upper() for m in CATALOG_DEV_RE.finditer(catalog_text)}


@pytest.fixture(scope="session")
def ledger_deviation_ids(deviations_text: str) -> set[str]:
    """Set of deviation IDs defined in approved_deviations.md."""
    return {m.group(1) for m in DEVIATION_ID_RE.finditer(deviations_text)}


@pytest.fixture(scope="session")
def catalog_test_paths(catalog_text: str) -> set[str]:
    """Set of focused test paths referenced in the catalog."""
    paths: set[str] = set()
    for m in CATALOG_TEST_RE.finditer(catalog_text):
        path = m.group(1)
        if path and path != "n/a":
            paths.add(path)
    return paths


# ── Required areas ────────────────────────────────────────────────────────

REQUIRED_AREAS = {
    "config",
    "startup",
    "runtime-lanes",
    "opportunity-input",
    "venue-capabilities",
    "market-data",
    "local-l2",
    "entry",
    "close",
    "passive-close",
    "risk",
    "state",
    "journal",
    "recovery",
    "replay",
    "offline-analysis",
    "evolution",
    "ops",
}


@pytest.fixture(scope="session")
def required_areas() -> set[str]:
    return REQUIRED_AREAS
