from __future__ import annotations

import subprocess
import sys


SCRIPT = "scripts/validate_change.py"


def test_validate_change_lists_profiles():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--list"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "close" in result.stdout
    assert "venue-bybit" in result.stdout
    assert "full" in result.stdout


def test_validate_change_close_dry_run_shows_segmented_commands():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "close", "--dry-run"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "[dry-run]" in result.stdout
    assert "compileall" in result.stdout
    assert "git diff --check" in result.stdout
    assert "tests/test_passive_close.py" in result.stdout
    assert "timeout=" in result.stdout
