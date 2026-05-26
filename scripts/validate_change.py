#!/usr/bin/env python3
"""Profile-based validation runner for LightFeeV2 changes.

This keeps bug-fix validation focused by default, while still making the
larger suites observable when they are needed.
"""

from __future__ import annotations

import argparse
import os
import selectors
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


PY = sys.executable


@dataclass(frozen=True)
class Step:
    name: str
    cmd: tuple[str, ...]
    timeout_s: int


def py(*args: str) -> tuple[str, ...]:
    return (PY, *args)


def pytest(*args: str, timeout_s: int = 300) -> Step:
    label = "pytest " + " ".join(args)
    return Step(label, ("pytest", "-q", *args), timeout_s)


BASE_STEPS = (
    Step("compileall", py("-m", "compileall", "-q", "lightfee", "tests", "scripts"), 120),
    Step("git diff --check", ("git", "diff", "--check"), 60),
)


PROFILES: dict[str, tuple[Step, ...]] = {
    "smoke": BASE_STEPS,
    "close": (
        *BASE_STEPS,
        pytest(
            "tests/test_passive_close.py",
            "tests/test_venues_transport.py::TestPassivePreflight",
            "tests/test_diagnose_live.py",
            "tests/engine/test_close_semantic_parity.py",
            "tests/engine/test_passive_close_semantic_parity.py",
            "tests/test_close_execution.py",
            timeout_s=300,
        ),
    ),
    "venue-bybit": (
        *BASE_STEPS,
        pytest(
            "tests/test_venues_transport.py::TestPassivePreflight",
            "tests/test_passive_close.py",
            "-k",
            "Bybit or bybit or UBUSDT or ALTUSDT",
            timeout_s=240,
        ),
    ),
    "venue-okx": (
        *BASE_STEPS,
        pytest(
            "tests/test_venues_transport.py",
            "tests/test_live_entry_hedge_root_fix.py",
            "tests/test_runtime_entry_flow.py",
            "-k",
            "OKX or okx or residual_contract or contract_min",
            timeout_s=360,
        ),
    ),
    "venue-hyperliquid": (
        *BASE_STEPS,
        pytest(
            "tests/test_venues_transport.py",
            "tests/test_live_entry_hedge_root_fix.py",
            "tests/test_venues_contract.py::TestHyperliquidLiveOrderNowSupported",
            "tests/test_venues_contract.py::TestHyperliquidCapabilityConsistency",
            "-k",
            "Hyperliquid or hyperliquid or l2Book or cloid",
            timeout_s=300,
        ),
    ),
    "venue-aster": (
        *BASE_STEPS,
        pytest(
            "tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile",
            "tests/test_diagnose_live.py",
            "-k",
            "XCNUSDT or aster or Aster or position_id",
            timeout_s=240,
        ),
    ),
    "local-l2": (
        *BASE_STEPS,
        pytest(
            "tests/test_local_l2_runtime.py",
            "tests/test_local_l2_ws.py",
            "tests/test_local_l2_venue_rules.py",
            "tests/test_entry_local_l2.py",
            "tests/test_runtime_maker_event_local_l2.py",
            timeout_s=420,
        ),
    ),
    "live-harness": (
        *BASE_STEPS,
        pytest("tests/live_harness", timeout_s=300),
    ),
    "live-probe": (
        pytest("tests/probes", timeout_s=300),
    ),
    "full": (
        *BASE_STEPS,
        pytest("tests/test_passive_close.py", timeout_s=300),
        pytest("tests/test_venues_transport.py", timeout_s=480),
        pytest("tests/test_diagnose_live.py", timeout_s=180),
        pytest(
            "tests/engine/test_close_semantic_parity.py",
            "tests/engine/test_passive_close_semantic_parity.py",
            "tests/test_close_execution.py",
            timeout_s=240,
        ),
        pytest("tests/test_venues_contract.py", "-vv", "--durations=20", timeout_s=300),
        pytest("tests/test_live_entry_hedge_root_fix.py", timeout_s=600),
        pytest("tests/test_runtime_entry_flow.py", timeout_s=240),
        pytest(
            "tests/test_local_l2_runtime.py",
            "tests/test_local_l2_ws.py",
            "tests/test_entry_local_l2.py",
            timeout_s=480,
        ),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run focused validation profiles with timeout and heartbeat output."
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        action="append",
        default=[],
        help="Validation profile to run. May be repeated. Default: smoke.",
    )
    parser.add_argument("--list", action="store_true", help="List available profiles and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument(
        "--timeout-per-step",
        type=int,
        default=None,
        help="Override every step timeout in seconds.",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=30,
        help="Print a progress line when a step produces no output for this long.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue running later steps after a failure and summarize at the end.",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Directory for per-step logs. Default: /tmp/lightfee-validate-change-<timestamp>.",
    )
    return parser.parse_args()


def selected_steps(profile_names: list[str]) -> list[tuple[str, Step]]:
    names = profile_names or ["smoke"]
    steps: list[tuple[str, Step]] = []
    for profile in names:
        for step in PROFILES[profile]:
            steps.append((profile, step))
    return steps


def command_text(cmd: tuple[str, ...]) -> str:
    return shlex.join(cmd)


def slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in value).strip("-")[:80]


def terminate_process(proc: subprocess.Popen[str]) -> None:
    try:
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            if hasattr(os, "killpg"):
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except Exception:
            pass


def run_step(
    profile: str,
    step: Step,
    *,
    index: int,
    total: int,
    timeout_s: int,
    heartbeat_s: int,
    log_dir: Path,
) -> int:
    started = time.monotonic()
    last_output = started
    log_path = log_dir / f"{index:02d}-{profile}-{slug(step.name)}.log"
    print(f"\n[{index}/{total}] {profile}: {step.name}")
    print(f"$ {command_text(step.cmd)}")
    print(f"log: {log_path}")

    with log_path.open("w", encoding="utf-8") as log:
        kwargs: dict[str, object] = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "bufsize": 1,
        }
        if hasattr(os, "setsid"):
            kwargs["preexec_fn"] = os.setsid
        proc = subprocess.Popen(step.cmd, **kwargs)
        assert proc.stdout is not None

        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)
        timed_out = False

        while proc.poll() is None:
            now = time.monotonic()
            if now - started > timeout_s:
                timed_out = True
                message = f"\n[timeout] {step.name} exceeded {timeout_s}s\n"
                print(message, end="")
                log.write(message)
                terminate_process(proc)
                break

            if now - last_output >= heartbeat_s:
                elapsed = int(now - started)
                message = f"[heartbeat] {step.name} still running after {elapsed}s without output\n"
                print(message, end="")
                log.write(message)
                log.flush()
                last_output = now

            events = selector.select(timeout=1.0)
            for key, _ in events:
                line = key.fileobj.readline()
                if line:
                    print(line, end="")
                    log.write(line)
                    log.flush()
                    last_output = time.monotonic()

        for line in proc.stdout:
            print(line, end="")
            log.write(line)

    elapsed = time.monotonic() - started
    if timed_out:
        print(f"[failed] {step.name}: timeout after {elapsed:.1f}s")
        return 124
    code = proc.returncode or 0
    status = "passed" if code == 0 else "failed"
    print(f"[{status}] {step.name}: exit={code} elapsed={elapsed:.1f}s")
    return code


def main() -> int:
    args = parse_args()

    if args.list:
        for name in sorted(PROFILES):
            print(f"{name}: {len(PROFILES[name])} step(s)")
        return 0

    steps = selected_steps(args.profile)
    if args.dry_run:
        for profile, step in steps:
            timeout_s = args.timeout_per_step or step.timeout_s
            print(f"[dry-run] {profile}: {step.name} timeout={timeout_s}s")
            print(f"  {command_text(step.cmd)}")
        return 0

    log_dir = Path(
        args.log_dir
        or f"/tmp/lightfee-validate-change-{time.strftime('%Y%m%d-%H%M%S')}"
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"validation logs: {log_dir}")

    failures: list[tuple[str, str, int]] = []
    for index, (profile, step) in enumerate(steps, start=1):
        code = run_step(
            profile,
            step,
            index=index,
            total=len(steps),
            timeout_s=args.timeout_per_step or step.timeout_s,
            heartbeat_s=max(args.heartbeat_seconds, 1),
            log_dir=log_dir,
        )
        if code != 0:
            failures.append((profile, step.name, code))
            if not args.keep_going:
                break

    if failures:
        print("\nValidation failed:")
        for profile, name, code in failures:
            print(f"- {profile}: {name} exit={code}")
        return 1

    print("\nValidation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
