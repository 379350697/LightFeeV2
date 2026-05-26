from __future__ import annotations

import pytest


pytestmark = pytest.mark.live_probe


def test_live_probe_profile_guard_is_collectable():
    assert True
