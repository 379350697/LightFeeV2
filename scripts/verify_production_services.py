#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

# Ensure repo root is on sys.path when invoked as a script (subprocess, cron, etc.)
# Python sets sys.path[0] to the script's directory, which is scripts/, not the
# repo root containing the lightfee package.
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from lightfee.ops.production_health import (
    HealthReport,
    analyze_current_state,
    analyze_resolver_config,
    analyze_runtime_resources,
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    deployment_acceptance_ok,
    summarize_reports,
)
from lightfee.engine.exchange_truth import normalize_exchange_truth_payload
from scripts.diagnose_live import _environment_file_scope

EXCHANGE_TRUTH_PROBE_TIMEOUT_S = 60.0
RUNTIME_RESOURCE_JOURNAL_WINDOW = "-1h"
_SOCKET_LINK_RE = re.compile(r"^socket:\[(?P<inode>\d+)\]$")
_PRIVATE_WS_STARTED_RE = re.compile(
    r"\b(?P<venue>aster|binance|bitget|bybit|gate|hyperliquid|okx)\s+"
    r"private\s+WS\s+worker\s+started\b",
    re.IGNORECASE,
)
_BINANCE_LISTEN_KEY_EVENT_RE = re.compile(
    r"\bbinance\.listen_key_(?:created|keepalive_ok)\b",
    re.IGNORECASE,
)
_BINANCE_LISTEN_KEY_FIELD_RE = re.compile(
    r"['\"](?P<name>last_listen_key_success_at|listen_key_created_at|"
    r"listen_key_expires_at)['\"]\s*:\s*(?P<value>\d+)"
)


def _read_json(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return data


def _run_readonly_command(command: list[str]) -> str:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            f"{' '.join(command[:2])} failed with {result.returncode}: {message[:300]}"
        )
    return result.stdout


def _systemd_main_pid(unit: str, command_runner) -> int:
    output = command_runner([
        "systemctl",
        "show",
        unit,
        "--property=MainPID",
        "--value",
    ])
    try:
        return int(output.strip().splitlines()[0])
    except (IndexError, ValueError):
        return 0


def _systemd_active_enter_timestamp_ms(unit: str, command_runner) -> int:
    output = command_runner([
        "systemctl",
        "show",
        unit,
        "--property=ActiveEnterTimestampUSec",
        "--value",
    ])
    try:
        return max(int(output.strip().splitlines()[0]) // 1_000, 0)
    except (IndexError, ValueError):
        return 0


def _process_socket_metrics(pid: int, *, proc_root: Path) -> dict[str, int]:
    process_root = proc_root / str(pid)
    fd_dir = process_root / "fd"
    socket_inodes: set[str] = set()
    fd_count = 0
    for fd_path in fd_dir.iterdir():
        fd_count += 1
        target = os.readlink(fd_path)
        match = _SOCKET_LINK_RE.match(target)
        if match is not None:
            socket_inodes.add(match.group("inode"))

    close_wait_count = 0
    for name in ("tcp", "tcp6"):
        table = (process_root / "net" / name).read_text()
        for raw in table.splitlines()[1:]:
            fields = raw.split()
            if len(fields) > 9 and fields[3] == "08" and fields[9] in socket_inodes:
                close_wait_count += 1
    return {
        "pid": pid,
        "fd_count": fd_count,
        "socket_count": len(socket_inodes),
        "close_wait_count": close_wait_count,
    }


def _live_journal_resource_evidence(lines: str) -> tuple[dict[str, int], dict[str, int]]:
    starts: dict[str, int] = {}
    listen_key: dict[str, int] = {}
    latest_success_at_ms = -1
    for raw in lines.splitlines():
        started = _PRIVATE_WS_STARTED_RE.search(raw)
        if started is not None:
            venue = started.group("venue").lower()
            starts[venue] = starts.get(venue, 0) + 1
        if _BINANCE_LISTEN_KEY_EVENT_RE.search(raw) is None:
            continue
        fields = {
            match.group("name"): int(match.group("value"))
            for match in _BINANCE_LISTEN_KEY_FIELD_RE.finditer(raw)
        }
        success_at_ms = (
            fields.get("last_listen_key_success_at")
            or fields.get("listen_key_created_at")
        )
        if success_at_ms is None or success_at_ms < latest_success_at_ms:
            continue
        latest_success_at_ms = success_at_ms
        listen_key = {
            "last_success_at_ms": success_at_ms,
            "expires_at_ms": fields.get("listen_key_expires_at", 0),
        }
    return starts, listen_key


def collect_runtime_resource_evidence(
    *,
    command_runner=None,
    proc_root: Path = Path("/proc"),
) -> dict:
    """Collect bounded, read-only OS and journal evidence for the health gate."""
    if command_runner is None:
        command_runner = _run_readonly_command
    evidence: dict = {
        "processes": {},
        "private_ws_worker_starts": {},
        "private_ws_window_ms": 60 * 60 * 1000,
        "private_ws_journal_since_ms": 0,
        "binance_listen_key": {},
        "collection_errors": [],
    }
    for name, unit in (
        ("sidecar", "lightfee-sidecar.service"),
        ("live", "lightfee-live.service"),
    ):
        try:
            pid = _systemd_main_pid(unit, command_runner)
            if pid <= 0:
                raise RuntimeError(f"{unit} has no active MainPID")
            evidence["processes"][name] = _process_socket_metrics(
                pid,
                proc_root=proc_root,
            )
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            evidence["collection_errors"].append(f"{name}:{str(exc)[:300]}")

    try:
        now_ms = int(time.time() * 1000)
        window_start_ms = now_ms - (60 * 60 * 1000)
        # One private worker per venue is expected after an intentional deploy;
        # only count churn from the current live-service lifecycle.
        active_entered_at_ms = _systemd_active_enter_timestamp_ms(
            "lightfee-live.service",
            command_runner,
        )
        if active_entered_at_ms > 0:
            window_start_ms = max(window_start_ms, min(active_entered_at_ms, now_ms))
        evidence["private_ws_journal_since_ms"] = window_start_ms
        lines = command_runner([
            "journalctl",
            "--unit=lightfee-live.service",
            f"--since=@{window_start_ms / 1000.0:.3f}",
            "--no-pager",
            "--output=cat",
        ])
        starts, listen_key = _live_journal_resource_evidence(lines)
        evidence["private_ws_worker_starts"] = starts
        evidence["binance_listen_key"] = listen_key
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        evidence["collection_errors"].append(f"live-journal:{str(exc)[:300]}")
    return evidence


def _environment_file_paths(unit_texts: dict[str, str]) -> list[Path]:
    paths: list[Path] = []
    for text in unit_texts.values():
        for raw in text.splitlines():
            line = raw.strip()
            if not line.startswith("EnvironmentFile="):
                continue
            value = line.split("=", 1)[1].strip()
            try:
                items = shlex.split(value)
            except ValueError:
                items = value.split()
            for item in items:
                path = item[1:] if item.startswith("-") else item
                if path:
                    paths.append(Path(path))
    return paths


def _exchange_truth_probe_timeout_s() -> float:
    raw = os.environ.get("LIGHTFEE_VERIFY_EXCHANGE_TRUTH_TIMEOUT_S")
    if raw is None:
        return EXCHANGE_TRUTH_PROBE_TIMEOUT_S
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return EXCHANGE_TRUTH_PROBE_TIMEOUT_S


def _call_exchange_truth_builder_with_timeout(
    exchange_truth_builder,
    runtime_dir: str,
) -> dict:
    timeout_s = _exchange_truth_probe_timeout_s()
    if timeout_s <= 0:
        return exchange_truth_builder(runtime_dir, [], None)

    try:
        import signal
    except Exception:
        return exchange_truth_builder(runtime_dir, [], None)

    if not hasattr(signal, "SIGALRM") or not hasattr(signal, "setitimer"):
        return exchange_truth_builder(runtime_dir, [], None)

    def _timeout(_signum, _frame):
        raise TimeoutError(
            "exchange truth probe timed out after {:.3g}s".format(timeout_s)
        )

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = (0.0, 0.0)
    try:
        signal.signal(signal.SIGALRM, _timeout)
        previous_timer = signal.setitimer(signal.ITIMER_REAL, timeout_s)
    except (AttributeError, ValueError):
        return exchange_truth_builder(runtime_dir, [], None)

    try:
        return exchange_truth_builder(runtime_dir, [], None)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(
                signal.ITIMER_REAL,
                previous_timer[0],
                previous_timer[1],
            )


def _attach_exchange_truth_if_missing(
    state: dict,
    *,
    current_state_path: Path,
    unit_texts: dict[str, str],
    exchange_truth_builder=None,
) -> dict:
    if isinstance(state.get("exchange_truth"), dict):
        return state

    env_files = _environment_file_paths(unit_texts)
    if exchange_truth_builder is None:
        from scripts.diagnose_live import _build_exchange_truth
        exchange_truth_builder = _build_exchange_truth

    runtime_dir = str(Path(current_state_path).resolve().parent)
    enriched = dict(state)
    with _environment_file_scope(env_files) as loaded_env_files:
        try:
            exchange_truth = normalize_exchange_truth_payload(
                _call_exchange_truth_builder_with_timeout(
                    exchange_truth_builder,
                    runtime_dir,
                )
            )
        except Exception as exc:
            exchange_truth = normalize_exchange_truth_payload(
                {
                    "available": False,
                    "confidence": "low",
                    "positions": {},
                    "open_orders": {},
                    "errors": [str(exc)[:500]],
                    "missing_evidence": ["exchange_truth_fetch_failed"],
                }
            )
    enriched["exchange_truth"] = exchange_truth
    enriched["exchange_truth_source"] = "verify_production_services_probe"
    enriched["exchange_truth_env_files_loaded"] = loaded_env_files
    return enriched


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify LightFee production service health")
    default_current_state = "/opt/lightfee-v2/runtime/live-state-current.json"
    parser.add_argument("--unit-dir", default="/etc/systemd/system")
    parser.add_argument("--snapshot", default="/opt/lightfee-v2/runtime/opportunity-input-snapshot.json")
    parser.add_argument("--current-state", default=default_current_state)
    parser.add_argument("--resolv-conf", default="/etc/resolv.conf")
    parser.add_argument("--now-ms", type=int, default=0)
    parser.add_argument("--snapshot-max-age-ms", type=int, default=60_000)
    parser.add_argument("--max-tick-age-ms", type=int, default=15_000)
    parser.add_argument(
        "--probe-exchange-truth",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Attach credentialed exchange truth when current-state lacks it. "
            "Default: enabled for the production default current-state path."
        ),
    )
    parser.add_argument(
        "--runtime-resources",
        action="store_true",
        help=(
            "Collect process-owned FD/CLOSE_WAIT and bounded private-stream "
            "journal evidence for a non-default current-state path. The "
            "production default path always collects it."
        ),
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--deployment-acceptance",
        action="store_true",
        help=(
            "Exit successfully only for a fully green report or the exact "
            "high-confidence-flat background close-accounting warning."
        ),
    )
    args = parser.parse_args()

    now_ms = args.now_ms or int(time.time() * 1000)
    reports = []
    unit_texts: dict[str, str] = {}
    unit_dir = Path(args.unit_dir)
    for name in ("lightfee-sidecar.service", "lightfee-live.service"):
        path = unit_dir / name
        if path.exists():
            unit_text = path.read_text()
            unit_texts[name] = unit_text
            reports.append(analyze_systemd_unit(name, unit_text))
        else:
            unit_texts[name] = ""
            reports.append(analyze_systemd_unit(name, ""))

    if Path(args.snapshot).exists():
        reports.append(analyze_sidecar_snapshot(_read_json(args.snapshot), now_ms=now_ms, max_age_ms=args.snapshot_max_age_ms))
    else:
        reports.append(HealthReport(
            name="sidecar_snapshot",
            ok=False,
            severity="critical",
            fingerprints=["snapshot_file_missing"],
            details={"path": args.snapshot},
        ))
    if Path(args.current_state).exists():
        current_state = _read_json(args.current_state)
        should_probe_exchange_truth = (
            args.probe_exchange_truth
            if args.probe_exchange_truth is not None
            else args.current_state == default_current_state
        )
        if should_probe_exchange_truth:
            current_state = _attach_exchange_truth_if_missing(
                current_state,
                current_state_path=Path(args.current_state),
                unit_texts=unit_texts,
            )
        reports.append(analyze_current_state(
            current_state,
            now_ms=now_ms,
            max_tick_age_ms=args.max_tick_age_ms,
            require_exchange_truth=True,
        ))
    else:
        reports.append(HealthReport(
            name="current_state",
            ok=False,
            severity="critical",
            fingerprints=["current_state_file_missing"],
            details={"path": args.current_state},
        ))
    if Path(args.resolv_conf).exists():
        reports.append(analyze_resolver_config(Path(args.resolv_conf).read_text()))
    else:
        reports.append(HealthReport(
            name="resolver_config",
            ok=False,
            severity="warning",
            fingerprints=["resolver_file_missing"],
            details={"path": args.resolv_conf},
        ))

    if args.runtime_resources or args.current_state == default_current_state:
        reports.append(analyze_runtime_resources(
            collect_runtime_resource_evidence(),
            now_ms=now_ms,
        ))

    summary = summarize_reports(reports)
    payload = asdict(summary)
    deployment_acceptable = deployment_acceptance_ok(summary)
    payload["deployment_acceptable"] = deployment_acceptable
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"ok={summary.ok} deployment_acceptable={deployment_acceptable} "
            f"critical={summary.critical_count} warning={summary.warning_count}"
        )
        for report in summary.reports:
            status = "PASS" if report.ok else report.severity.upper()
            print(f"{status} {report.name}: {','.join(report.fingerprints) or 'ok'}")
    accepted = deployment_acceptable if args.deployment_acceptance else summary.ok
    sys.exit(0 if accepted else 1)


if __name__ == "__main__":
    main()
