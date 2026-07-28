from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.sidecar.service import SidecarService


@pytest.mark.asyncio
async def test_funding_sidecar_has_no_embedded_spread_data_plane(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    service = SidecarService(config)
    try:
        assert not hasattr(service, "_spread_bbo_sources")
        assert not hasattr(service, "_spread_bbo_data_plane")
        assert not hasattr(service, "embedded_spread_bbo_enabled")
    finally:
        await service.close()


def test_standalone_spread_bbo_modules_are_retired() -> None:
    assert importlib.util.find_spec("lightfee.sidecar.spread_bbo") is None
    assert importlib.util.find_spec("lightfee.sidecar.spread_bbo_service") is None
    assert importlib.util.find_spec("lightfee.apps.spread_bbo") is None


def test_deployment_keeps_one_spread_sidecar_unit() -> None:
    systemd = Path(__file__).resolve().parents[2] / "deploy" / "systemd"
    assert (systemd / "lightfee-sidecar.service").exists()
    assert (systemd / "lightfee-live.service").exists()
    assert (systemd / "lightfee-spread-sidecar.service").exists()
    assert not (systemd / "lightfee-spread-bbo.service").exists()


def test_production_entrypoints_do_not_import_retired_evidence_planes() -> None:
    project_root = Path(__file__).resolve().parents[2]
    script = """
import sys
from lightfee.apps import live, sidecar, spread_sidecar
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime

retired_modules = (
    "lightfee.strategy.fee_evidence",
    "lightfee.persistence.open_interest_store",
    "lightfee.sidecar.spread_bbo",
    "lightfee.sidecar.spread_bbo_service",
    "lightfee.apps.spread_bbo",
)
loaded = [name for name in retired_modules if name in sys.modules]
retired_methods = [
    name
    for name in (
        "_funding_canary_admission_reason",
        "_funding_canary_submission_reason",
        "_funding_canary_clamp_quantity",
    )
    if hasattr(EntryDispatchRuntime, name)
]
if loaded or retired_methods:
    raise SystemExit(f"loaded={loaded}; methods={retired_methods}")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_offline_fee_evidence_reader_remains_available() -> None:
    assert importlib.util.find_spec("lightfee.strategy.fee_evidence") is not None
