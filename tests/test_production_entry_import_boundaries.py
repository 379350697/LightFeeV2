"""Production entrypoints must not regain retired admission data planes."""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


_ROOT = Path(__file__).resolve().parents[1]
_PRODUCTION_MODULES = (
    "lightfee/apps/sidecar.py",
    "lightfee/apps/live.py",
    "lightfee/apps/spread_sidecar.py",
    "lightfee/engine/runtime.py",
    "lightfee/sidecar/service.py",
    "lightfee/spread/service.py",
)
_RETIRED_MODULE_PREFIXES = (
    "lightfee.strategy.funding_canary",
    "lightfee.strategy.fee_evidence",
    "lightfee.strategy.cohort",
    "lightfee.spread.paper",
    "lightfee.spread.research_manifest",
)
_RETIRED_PUBLISHER_NAMES = {
    "funding_entry_snapshot_identity",
    "funding_entry_snapshot_manifest_path",
    "load_funding_entry_snapshot",
    "publish_funding_entry_snapshot",
}


def _imports(path: Path) -> tuple[set[str], set[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.add(node.module)
            imported_names.update(alias.name for alias in node.names)
    return modules, imported_names


def test_three_production_processes_do_not_import_retired_admission_planes() -> None:
    """Offline analysis may retain them; sidecar/live/spread production may not."""
    violations: list[str] = []
    for relative_path in _PRODUCTION_MODULES:
        modules, imported_names = _imports(_ROOT / relative_path)
        retired_modules = sorted(
            module
            for module in modules
            if module in _RETIRED_MODULE_PREFIXES
            or module.startswith(tuple(f"{prefix}." for prefix in _RETIRED_MODULE_PREFIXES))
        )
        retired_names = sorted(imported_names & _RETIRED_PUBLISHER_NAMES)
        if retired_modules or retired_names:
            violations.append(f"{relative_path}: modules={retired_modules}, names={retired_names}")

    assert not violations, "\n".join(violations)


def test_three_production_entrypoints_do_not_load_offline_manifest_or_v7_api() -> None:
    """The historical manifest reader is offline-only, not a live fallback."""
    script = """
import sys
from lightfee.apps import live, sidecar, spread_sidecar
from lightfee.sidecar import publisher

retired = (
    "funding_entry_snapshot_identity",
    "funding_entry_snapshot_manifest_path",
    "funding_entry_snapshot_path",
    "load_funding_entry_snapshot",
    "publish_funding_entry_snapshot",
)
loaded = "lightfee.offline.funding_entry_manifest" in sys.modules
exported = [name for name in retired if hasattr(publisher, name)]
if loaded or exported:
    raise SystemExit(f"offline_loaded={loaded}; v7_exports={exported}")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
