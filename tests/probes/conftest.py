from __future__ import annotations

import os
from pathlib import Path

import pytest


PROBE_ROOT = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config, items):
    probe_items = [
        item
        for item in items
        if PROBE_ROOT in Path(str(item.path)).resolve().parents
    ]
    if not probe_items:
        return

    for item in probe_items:
        item.add_marker(pytest.mark.live_probe)

    if os.environ.get("LIGHTFEE_RUN_LIVE_PROBES") == "1":
        return
    skip = pytest.mark.skip(reason="LIGHTFEE_RUN_LIVE_PROBES=1 required")
    for item in probe_items:
        item.add_marker(skip)
