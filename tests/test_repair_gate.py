from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _repair_gate_module():
    path = Path("scripts/repair_gate.py")
    spec = importlib.util.spec_from_file_location("repair_gate", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_safety_release_gate_requires_test_change_for_protected_code():
    gate = _repair_gate_module()
    profile = gate.GateProfile(
        name="safety-release",
        description="",
        protected_paths=("engine/recovery.py",),
        test_paths=("tests/",),
        commands=(("{python}", "-m", "pytest"),),
        require_test_change=True,
    )

    plan = gate.build_plan(profile, ["engine/recovery.py"], force=False)

    assert plan.active is True
    assert plan.error is not None
    assert "contract test" in plan.error


def test_safety_release_gate_runs_only_when_protected_path_changes():
    gate = _repair_gate_module()
    profile = gate.GateProfile(
        name="safety-release",
        description="",
        protected_paths=("engine/recovery.py",),
        test_paths=("tests/",),
        commands=(("{python}", "-m", "pytest"),),
        require_test_change=True,
    )

    plan = gate.build_plan(profile, ["docs/bugs.md"], force=False)

    assert plan.active is False
    assert plan.error is None


def test_safety_release_gate_accepts_matching_contract_test_and_expands_placeholders():
    gate = _repair_gate_module()
    profile = gate.GateProfile(
        name="safety-release",
        description="",
        protected_paths=("engine/recovery.py",),
        test_paths=("tests/",),
        commands=(("{python}", "-m", "pytest"),),
        require_test_change=True,
    )

    plan = gate.build_plan(
        profile,
        ["engine/recovery.py", "tests/test_recovery_contract.py"],
        force=False,
    )

    assert plan.active is True
    assert plan.error is None
    expanded = gate.expand_command(
        ("{python}", "--repo", "{repo}", "--base", "{base}"),
        root=Path.cwd(),
        base="abc123",
    )

    assert expanded[0] == sys.executable
    assert expanded[2] == str(Path.cwd())
    assert expanded[4] == "abc123"
    assert gate.expand_command(
        ("git", "diff", "--check", "{base}...HEAD"),
        root=Path.cwd(),
        base="abc123",
    )[-1] == "abc123...HEAD"


def test_git_change_discovery_includes_untracked_protected_files(monkeypatch, tmp_path):
    gate = _repair_gate_module()
    calls: list[tuple[str, ...]] = []

    def fake_git_output(root, *args):
        calls.append(args)
        if args == ("ls-files", "--others", "--exclude-standard"):
            return ["lightfee/engine/new_recovery_boundary.py"]
        return []

    monkeypatch.setattr(gate, "_git_output", fake_git_output)

    changed = gate.changed_files_from_git(tmp_path, "HEAD")

    assert changed == ["lightfee/engine/new_recovery_boundary.py"]
    assert ("ls-files", "--others", "--exclude-standard") in calls


def test_safety_release_gate_protects_its_own_control_plane():
    gate = _repair_gate_module()
    profile = gate.load_profile(Path("repair-gate.toml"), "safety-release")

    for control_path in (
        "repair-gate.toml",
        "scripts/repair_gate.py",
        ".github/workflows/repair-gate.yml",
    ):
        plan = gate.build_plan(
            profile,
            [control_path, "tests/test_repair_gate.py"],
            force=False,
        )

        assert plan.active is True
        assert plan.error is None


def test_repair_gate_workflow_uses_base_revision_control_plane():
    workflow = Path(".github/workflows/repair-gate.yml").read_text(encoding="utf-8")

    assert 'git show "$REPAIR_GATE_BASE:scripts/repair_gate.py"' in workflow
    assert 'git show "$REPAIR_GATE_BASE:repair-gate.toml"' in workflow
