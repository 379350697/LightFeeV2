#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
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
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    deployment_acceptance_ok,
    summarize_reports,
)
from lightfee.engine.exchange_truth import normalize_exchange_truth_payload
from scripts.diagnose_live import _environment_file_scope

EXCHANGE_TRUTH_PROBE_TIMEOUT_S = 60.0


def _read_json(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return data


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
