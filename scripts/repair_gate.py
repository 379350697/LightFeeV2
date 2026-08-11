#!/usr/bin/env python3
"""Run the small, executable closure gate for protected repair boundaries.

The gate intentionally has no service, database, or per-repair paperwork.  A
repository declares only the paths whose changes can release a protected state
and the tests that prove the corresponding invariant.  If such a path changes,
the same diff must change a test and every configured command must pass.
"""

from __future__ import annotations

import argparse
import fnmatch
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


CONTROL_PLANE_PATHS = frozenset(
    {
        "repair-gate.toml",
        "scripts/repair_gate.py",
        ".github/workflows/repair-gate.yml",
    }
)


class GateConfigError(ValueError):
    """Raised when a repository gate configuration is not executable."""


@dataclass(frozen=True)
class GateProfile:
    name: str
    description: str
    protected_paths: tuple[str, ...]
    test_paths: tuple[str, ...]
    commands: tuple[tuple[str, ...], ...]
    require_test_change: bool


@dataclass(frozen=True)
class GatePlan:
    active: bool
    changed_files: tuple[str, ...]
    error: str | None = None


def _string_list(value: Any, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise GateConfigError(f"{field} must be a non-empty list of strings")
    return tuple(value)


def _commands(value: Any, *, field: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list) or not value:
        raise GateConfigError(f"{field} must be a non-empty list of command arrays")
    commands: list[tuple[str, ...]] = []
    for index, command in enumerate(value):
        if not isinstance(command, list) or not command or not all(
            isinstance(part, str) and part for part in command
        ):
            raise GateConfigError(f"{field}[{index}] must be a non-empty command array")
        commands.append(tuple(command))
    return tuple(commands)


def load_profile(config_path: Path, profile_name: str) -> GateProfile:
    try:
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise GateConfigError(f"cannot read config {config_path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise GateConfigError(f"invalid TOML in {config_path}: {exc}") from exc

    profiles = data.get("profiles")
    if not isinstance(profiles, dict):
        raise GateConfigError("config must contain [profiles.<name>]")
    raw = profiles.get(profile_name)
    if not isinstance(raw, dict):
        raise GateConfigError(f"profile not found: {profile_name}")
    return GateProfile(
        name=profile_name,
        description=str(raw.get("description") or ""),
        protected_paths=_string_list(raw.get("protected_paths"), field="protected_paths"),
        test_paths=_string_list(raw.get("test_paths"), field="test_paths"),
        commands=_commands(raw.get("commands"), field="commands"),
        require_test_change=bool(raw.get("require_test_change", True)),
    )


def path_matches(path: str, pattern: str) -> bool:
    normalized_path = path.replace("\\", "/").lstrip("./")
    normalized_pattern = pattern.replace("\\", "/").lstrip("./")
    if normalized_pattern.endswith("/"):
        return normalized_path.startswith(normalized_pattern)
    return (
        normalized_path == normalized_pattern
        or fnmatch.fnmatchcase(normalized_path, normalized_pattern)
        or PurePosixPath(normalized_path).match(normalized_pattern)
    )


def build_plan(profile: GateProfile, changed_files: list[str], *, force: bool) -> GatePlan:
    changed = tuple(sorted({path.replace("\\", "/") for path in changed_files if path}))
    active = force or any(
        path_matches(path, pattern)
        for path in changed
        for pattern in profile.protected_paths
    ) or any(path in CONTROL_PLANE_PATHS for path in changed)
    if not active:
        return GatePlan(active=False, changed_files=changed)
    if profile.require_test_change and not any(
        path_matches(path, pattern)
        for path in changed
        for pattern in profile.test_paths
    ):
        return GatePlan(
            active=True,
            changed_files=changed,
            error=(
                f"profile {profile.name} protects this change; add or update a contract test "
                f"under one of: {', '.join(profile.test_paths)}"
            ),
        )
    return GatePlan(active=True, changed_files=changed)


def _git_output(root: Path, *args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit={result.returncode}"
        raise GateConfigError(f"git {' '.join(args)} failed: {detail}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def changed_files_from_git(root: Path, base: str) -> list[str]:
    changed = _git_output(root, "diff", "--name-only", f"{base}...HEAD")
    changed.extend(_git_output(root, "diff", "--name-only", "--cached"))
    changed.extend(_git_output(root, "diff", "--name-only"))
    changed.extend(_git_output(root, "ls-files", "--others", "--exclude-standard"))
    return changed


def expand_command(
    command: tuple[str, ...], *, root: Path, base: str
) -> tuple[str, ...]:
    replacements = {
        "{python}": sys.executable,
        "{repo}": str(root),
        "{base}": base,
    }
    return tuple(
        part.replace("{python}", replacements["{python}"])
        .replace("{repo}", replacements["{repo}"])
        .replace("{base}", replacements["{base}"])
        for part in command
    )


def run_commands(profile: GateProfile, *, root: Path, base: str, dry_run: bool) -> int:
    for command in profile.commands:
        expanded = expand_command(command, root=root, base=base)
        print("$ " + " ".join(expanded))
        if dry_run:
            continue
        result = subprocess.run(expanded, cwd=root, check=False)
        if result.returncode != 0:
            print(f"[failed] {profile.name}: exit={result.returncode}")
            return result.returncode or 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("verify", "explain"), help="gate operation")
    parser.add_argument("--config", default="repair-gate.toml", help="repository TOML config")
    parser.add_argument("--profile", required=True, help="configured profile name")
    parser.add_argument(
        "--base",
        default="HEAD",
        help="merge-base/commit for committed diff detection (default: HEAD)",
    )
    parser.add_argument("--force", action="store_true", help="run even without protected changes")
    parser.add_argument("--dry-run", action="store_true", help="print gate commands only")
    parser.add_argument(
        "--changed-file",
        action="append",
        default=[],
        help="override git discovery; intended for diagnostics and tests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path.cwd()
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = root / config_path
        profile = load_profile(config_path, args.profile)
        changed = args.changed_file or changed_files_from_git(root, args.base)
        plan = build_plan(profile, changed, force=args.force)
    except GateConfigError as exc:
        print(f"[repair-gate] configuration error: {exc}", file=sys.stderr)
        return 2

    print(f"[repair-gate] profile={profile.name} changed={len(plan.changed_files)}")
    if args.command == "explain":
        print(profile.description)
        print("active=" + str(plan.active).lower())
        for path in plan.changed_files:
            print(path)
        return 1 if plan.error else 0
    if plan.error:
        print(f"[repair-gate] {plan.error}", file=sys.stderr)
        return 1
    if not plan.active:
        print("[repair-gate] skipped: no protected path changed")
        return 0
    return run_commands(profile, root=root, base=args.base, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
