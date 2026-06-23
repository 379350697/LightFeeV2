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


def test_validate_change_live_harness_profile_is_independent():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "live-harness", "--dry-run"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tests/live_harness" in result.stdout
    assert "tests/probes" not in result.stdout


def test_validate_change_full_profile_excludes_live_probes():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "full", "--dry-run"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tests/probes" not in result.stdout


def test_validate_change_bug_docs_profile_checks_bug_ledger_governance():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "bug-docs", "--dry-run"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "scripts/check_bug_ledger.py" in result.stdout
    assert "tests/test_bug_docs_status.py" in result.stdout
