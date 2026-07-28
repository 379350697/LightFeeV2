#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from collections import deque
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
    analyze_spread_snapshot,
    analyze_strategy_entry_policy,
    analyze_systemd_unit,
    summarize_reports,
)
from lightfee.engine.exchange_truth import normalize_exchange_truth_payload
from lightfee.ops.auto_fail_closed_events import build_auto_fail_closed_summary
from scripts.diagnose_live import _build_stale_risk_state_alignment_summary

EXCHANGE_TRUTH_PROBE_TIMEOUT_S = 60.0
AUTO_FAIL_CLOSED_RECENT_WINDOW_MS = 24 * 3600 * 1000
DEFAULT_SPREAD_SNAPSHOT_MAX_AGE_MS = 60_000
PRODUCTION_SERVICE_NAMES = (
    "lightfee-sidecar.service",
    "lightfee-spread-bbo.service",
    "lightfee-spread-sidecar.service",
    "lightfee-live.service",
)

# These conditions describe current market-data availability, not whether the
# process is safe to run.  Spread execution proves freshness independently for
# the two selected legs immediately before dispatch; requiring every configured
# venue to be fresh in one global verifier sample would make an unrelated slow
# venue a new admission gate.
_SPREAD_SNAPSHOT_MARKET_OBSERVATION_FINGERPRINTS = frozenset(
    {
        "spread_snapshot_stale",
        "spread_source_sidecar_snapshot_stale",
        "spread_source_sidecar_snapshot_quotes_stale",
        "spread_source_sidecar_snapshot_degraded",
        "spread_source_sidecar_snapshot_partial",
        "spread_source_direct_spread_bbo_partial",
        "spread_source_spread_bbo_snapshot_unavailable",
        "spread_source_spread_bbo_snapshot_stale",
        "spread_source_spread_bbo_snapshot_universe_unavailable",
        "spread_degraded_inputs",
        "spread_input_pipeline_stalled",
    }
)


def _read_json(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return data


def _sidecar_snapshot_report(
    path: str | Path,
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    """Check the one atomically replaced sidecar snapshot."""
    try:
        snapshot = _read_json(str(path))
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return HealthReport(
            name="sidecar_snapshot",
            ok=False,
            severity="critical",
            fingerprints=["funding_sidecar_snapshot_missing_or_invalid"],
            details={"path": str(path), "error": str(exc)[:500]},
        )
    return analyze_sidecar_snapshot(
        snapshot,
        now_ms=now_ms,
        max_age_ms=max_age_ms,
    )


def _resolve_spread_snapshot_max_age_ms(
    explicit_max_age_ms: int | None,
    app_config,
) -> int:
    if explicit_max_age_ms is not None:
        return max(int(explicit_max_age_ms), 0)
    # This is a service-liveness budget.  ``spread_signal_ttl_ms`` remains the
    # strict per-candidate execution gate and must not be repurposed as a
    # whole-data-plane availability requirement.
    configured = getattr(
        getattr(app_config, "runtime", None),
        "sidecar_snapshot_max_age_ms",
        None,
    )
    try:
        if configured is not None and int(configured) > 0:
            return int(configured)
    except (TypeError, ValueError):
        pass
    return DEFAULT_SPREAD_SNAPSHOT_MAX_AGE_MS


def _keep_pair_scoped_market_observations_nonblocking(
    report: HealthReport,
    *,
    market_observation_fingerprints: frozenset[str],
) -> HealthReport:
    """Keep degraded market evidence visible without making it a global gate.

    The runtime's order path owns pair-scoped admission and rejects stale or
    incomplete evidence for either selected leg.  This verifier must still
    fail on a broken producer, schema, generation, or sampling contract, but a
    temporary absence of quotes on unrelated venues is an operational
    observation rather than a process-wide readiness failure.
    """

    observations = sorted(
        set(report.fingerprints) & market_observation_fingerprints
    )
    if not observations:
        return report
    blocking = [
        fingerprint
        for fingerprint in report.fingerprints
        if fingerprint not in market_observation_fingerprints
    ]
    details = dict(report.details)
    details["nonblocking_market_observations"] = observations
    details["execution_admission_contract"] = (
        "selected_pair_two_leg_freshness_fail_closed"
    )
    details["data_plane_status"] = "degraded"
    return HealthReport(
        name=report.name,
        ok=not blocking,
        severity="critical" if blocking else "info",
        fingerprints=blocking,
        details=details,
    )


def _systemd_active_report(name: str) -> HealthReport:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", name],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return HealthReport(
            name=f"systemd_active:{name}",
            ok=False,
            severity="critical",
            fingerprints=["systemd_active_check_failed"],
            details={"error": str(exc)[:500]},
        )
    state = result.stdout.strip()
    ok = result.returncode == 0 and state == "active"
    return HealthReport(
        name=f"systemd_active:{name}",
        ok=ok,
        severity="critical" if not ok else "info",
        fingerprints=[] if ok else ["systemd_service_not_active"],
        details={"state": state or "unknown", "returncode": result.returncode},
    )


def _systemd_main_pid(name: str) -> int:
    result = subprocess.run(
        ["systemctl", "show", name, "--property=MainPID", "--value"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0:
        return 0
    try:
        return int(result.stdout.strip() or 0)
    except ValueError:
        return 0


def _process_started_at_ms(pid: int) -> int:
    """Resolve Linux process start wall time without locale-dependent parsing."""

    if pid <= 0:
        return 0
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text()
        stat_tail = stat_text.rsplit(")", 1)[1].strip().split()
        start_ticks = int(stat_tail[19])  # field 22; tail starts at field 3
        btime_line = next(
            line
            for line in Path("/proc/stat").read_text().splitlines()
            if line.startswith("btime ")
        )
        boot_epoch_s = int(btime_line.split()[1])
        ticks_per_s = int(os.sysconf("SC_CLK_TCK"))
    except (OSError, ValueError, IndexError, StopIteration):
        return 0
    if ticks_per_s <= 0:
        return 0
    return int((boot_epoch_s + start_ticks / ticks_per_s) * 1000)


def _spread_snapshot_runtime_report(
    path: str | Path,
    *,
    max_age_ms: int,
    now_ms: int | None,
) -> HealthReport:
    """Read first, then sample wall time so a concurrent publish is not future."""

    snapshot = _read_json(path)
    checked_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    report = analyze_spread_snapshot(
        snapshot,
        now_ms=checked_at_ms,
        max_age_ms=max_age_ms,
    )
    return _keep_pair_scoped_market_observations_nonblocking(
        report,
        market_observation_fingerprints=(
            _SPREAD_SNAPSHOT_MARKET_OBSERVATION_FINGERPRINTS
        ),
    )


def _read_jsonl_tail(path: Path, max_records: int = 1000) -> list[dict]:
    if not path.exists():
        return []
    records: deque[dict] = deque(maxlen=max_records)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                records.append(item)
    return list(records)


def _runtime_event_files(runtime_dir: Path) -> list[Path]:
    if not runtime_dir.exists():
        return []
    candidates: list[Path] = []
    for pattern in ("live-events*.jsonl", "events*.jsonl", "journal.jsonl"):
        for path in sorted(runtime_dir.glob(pattern)):
            if path not in candidates:
                candidates.append(path)
    return candidates


def _auto_fail_closed_since_ms(state: dict) -> int:
    window = state.get("window")
    if isinstance(window, dict):
        try:
            since_ms = int(window.get("since_ms") or 0)
        except (TypeError, ValueError):
            since_ms = 0
        if since_ms > 0:
            return since_ms

    for key in ("generated_at_ms", "ts_ms", "timestamp_ms", "last_tick_ms"):
        try:
            anchor_ms = int(state.get(key) or 0)
        except (TypeError, ValueError):
            anchor_ms = 0
        if anchor_ms > 0:
            return max(0, anchor_ms - AUTO_FAIL_CLOSED_RECENT_WINDOW_MS)
    return max(0, int(time.time() * 1000) - AUTO_FAIL_CLOSED_RECENT_WINDOW_MS)


def _attach_auto_fail_closed_summary_if_missing(
    state: dict,
    *,
    current_state_path: Path,
) -> dict:
    if isinstance(state.get("auto_fail_closed_summary"), dict):
        return state

    events: list[dict] = []
    for path in _runtime_event_files(Path(current_state_path).resolve().parent):
        events.extend(_read_jsonl_tail(path))
        if len(events) >= 1000:
            events = events[-1000:]
            break

    summary = build_auto_fail_closed_summary(
        events,
        since_ms=_auto_fail_closed_since_ms(state),
    )
    if not summary.get("recent_incident"):
        return state
    enriched = dict(state)
    enriched["auto_fail_closed_summary"] = summary
    return enriched


def _attach_stale_risk_state_alignment_summary_if_missing(
    state: dict,
    *,
    current_state_path: Path,
) -> dict:
    if isinstance(state.get("stale_risk_state_alignment_summary"), dict):
        return state

    events: list[dict] = []
    for path in _runtime_event_files(Path(current_state_path).resolve().parent):
        events.extend(_read_jsonl_tail(path))
        if len(events) >= 1000:
            events = events[-1000:]
            break

    summary = _build_stale_risk_state_alignment_summary(
        events,
        since_ms=_auto_fail_closed_since_ms(state),
    )
    if not summary.get("recent_incident"):
        return state
    enriched = dict(state)
    enriched["stale_risk_state_alignment_summary"] = summary
    return enriched


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


def _load_environment_files(paths: list[Path]) -> list[str]:
    loaded: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        loaded.append(str(path))
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or not key.replace("_", "").isalnum():
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ.setdefault(key, value)
    return loaded


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
        raise TimeoutError("exchange truth probe timed out after {:.3g}s".format(timeout_s))

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
    loaded_env_files = _load_environment_files(env_files)

    if exchange_truth_builder is None:
        from scripts.diagnose_live import _build_exchange_truth

        exchange_truth_builder = _build_exchange_truth

    runtime_dir = str(Path(current_state_path).resolve().parent)
    enriched = dict(state)
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
    default_config = "/opt/lightfee-v2/config/live.toml"
    default_spread_snapshot = "/opt/lightfee-v2/runtime/spread-opportunities-current.json"
    parser.add_argument("--unit-dir", default="/etc/systemd/system")
    parser.add_argument(
        "--snapshot", default="/opt/lightfee-v2/runtime/opportunity-input-snapshot.json"
    )
    parser.add_argument("--spread-snapshot", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--current-state", default=default_current_state)
    parser.add_argument("--resolv-conf", default="/etc/resolv.conf")
    parser.add_argument("--now-ms", type=int, default=0)
    parser.add_argument("--snapshot-max-age-ms", type=int, default=60_000)
    parser.add_argument("--spread-snapshot-max-age-ms", type=int, default=None)
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
        "--require-entry-enabled",
        action="store_true",
        help="Fail readiness when the live funding entry policy is disabled.",
    )
    parser.add_argument(
        "--require-active-services",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Require all four production systemd services and current BBO output.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    now_ms = args.now_ms or int(time.time() * 1000)
    reports = []
    unit_texts: dict[str, str] = {}
    unit_dir = Path(args.unit_dir)
    for name in PRODUCTION_SERVICE_NAMES:
        path = unit_dir / name
        if path.exists():
            unit_text = path.read_text()
            unit_texts[name] = unit_text
            reports.append(analyze_systemd_unit(name, unit_text))
        else:
            unit_texts[name] = ""
            reports.append(analyze_systemd_unit(name, ""))

    require_active_services = (
        args.require_active_services
        if args.require_active_services is not None
        else (
            unit_dir == Path("/etc/systemd/system") and args.current_state == default_current_state
        )
    )
    if require_active_services:
        reports.extend(_systemd_active_report(name) for name in PRODUCTION_SERVICE_NAMES)

    ownership_fingerprints: list[str] = []
    live_unit = unit_texts.get("lightfee-live.service", "")
    spread_sidecar_unit = unit_texts.get("lightfee-spread-sidecar.service", "")
    spread_bbo_unit = unit_texts.get("lightfee-spread-bbo.service", "")
    funding_sidecar_unit = unit_texts.get("lightfee-sidecar.service", "")
    if not any(
        entrypoint in funding_sidecar_unit
        for entrypoint in ("lightfee-sidecar", "-m lightfee.apps.sidecar")
    ):
        ownership_fingerprints.append("funding_sidecar_entrypoint_missing")
    if "lightfee-sidecar.service" not in spread_sidecar_unit:
        ownership_fingerprints.append("spread_consumer_dependency_missing")
    if not any(
        entrypoint in spread_bbo_unit
        for entrypoint in ("lightfee-spread-bbo", "-m lightfee.apps.spread_bbo")
    ):
        ownership_fingerprints.append("spread_bbo_entrypoint_missing")
    if "lightfee-spread-bbo.service" not in spread_sidecar_unit:
        ownership_fingerprints.append("spread_bbo_consumer_dependency_missing")
    if "lightfee-sidecar.service" in live_unit:
        ownership_fingerprints.append("live_funding_sidecar_dependency_present")
    reports.append(
        HealthReport(
            name="production_process_ownership",
            ok=not ownership_fingerprints,
            severity="critical" if ownership_fingerprints else "info",
            fingerprints=ownership_fingerprints,
            details={"process_model": "funding_sidecar+spread_bbo+spread_paper+live"},
        )
    )

    app_config = None
    config_path = args.config
    if config_path is None and args.current_state == default_current_state:
        config_path = default_config
    if config_path:
        try:
            from lightfee.config.loader import load_config

            app_config = load_config(config_path)
        except Exception as exc:
            reports.append(
                HealthReport(
                    name="strategy_entry_policy",
                    ok=False,
                    severity="critical",
                    fingerprints=["production_config_unreadable"],
                    details={"path": config_path, "error": str(exc)[:500]},
                )
            )
        else:
            reports.append(
                analyze_strategy_entry_policy(
                    app_config.strategy,
                    runtime_mode=app_config.runtime.mode,
                    require_entry_enabled=args.require_entry_enabled,
                )
            )

    sidecar_snapshot_path = Path(args.snapshot)
    if sidecar_snapshot_path.exists():
        reports.append(
            _sidecar_snapshot_report(
                args.snapshot,
                now_ms=now_ms,
                max_age_ms=args.snapshot_max_age_ms,
            )
        )
    else:
        reports.append(
            HealthReport(
                name="sidecar_snapshot",
                ok=False,
                severity="critical",
                fingerprints=["funding_sidecar_snapshot_missing"],
                details={
                    "status": "missing",
                    "path": args.snapshot,
                },
            )
        )

    spread_snapshot_path = args.spread_snapshot
    if spread_snapshot_path is None and args.current_state == default_current_state:
        spread_snapshot_path = default_spread_snapshot
    if spread_snapshot_path:
        if Path(spread_snapshot_path).exists():
            reports.append(
                _spread_snapshot_runtime_report(
                    spread_snapshot_path,
                    now_ms=args.now_ms or None,
                    max_age_ms=_resolve_spread_snapshot_max_age_ms(
                        args.spread_snapshot_max_age_ms,
                        app_config,
                    ),
                )
            )
        else:
            reports.append(
                HealthReport(
                    name="spread_snapshot",
                    ok=False,
                    severity="critical",
                    fingerprints=["spread_snapshot_file_missing"],
                    details={"path": spread_snapshot_path},
                )
            )

    if Path(args.current_state).exists():
        current_state = _read_json(args.current_state)
        current_state = _attach_auto_fail_closed_summary_if_missing(
            current_state,
            current_state_path=Path(args.current_state),
        )
        current_state = _attach_stale_risk_state_alignment_summary_if_missing(
            current_state,
            current_state_path=Path(args.current_state),
        )
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
        reports.append(
            analyze_current_state(
                current_state,
                now_ms=now_ms,
                max_tick_age_ms=args.max_tick_age_ms,
                require_exchange_truth=True,
            )
        )
    else:
        reports.append(
            HealthReport(
                name="current_state",
                ok=False,
                severity="critical",
                fingerprints=["current_state_file_missing"],
                details={"path": args.current_state},
            )
        )
    if Path(args.resolv_conf).exists():
        reports.append(analyze_resolver_config(Path(args.resolv_conf).read_text()))
    else:
        reports.append(
            HealthReport(
                name="resolver_config",
                ok=False,
                severity="warning",
                fingerprints=["resolver_file_missing"],
                details={"path": args.resolv_conf},
            )
        )

    summary = summarize_reports(reports)
    payload = asdict(summary)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"ok={summary.ok} critical={summary.critical_count} warning={summary.warning_count}")
        for report in summary.reports:
            observations = report.details.get("nonblocking_market_observations", [])
            if observations:
                status = "OBSERVE"
                labels = ",".join(str(item) for item in observations)
            else:
                status = "PASS" if report.ok else report.severity.upper()
                labels = ",".join(report.fingerprints) or "ok"
            print(f"{status} {report.name}: {labels}")
    sys.exit(0 if summary.ok else 1)


if __name__ == "__main__":
    main()
