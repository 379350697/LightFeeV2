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
from lightfee.sidecar.publisher import (
    FUNDING_ENTRY_SNAPSHOT_MAX_BYTES,
    funding_entry_snapshot_identity,
    funding_entry_snapshot_manifest_path,
    load_funding_entry_snapshot,
)
from lightfee.spread.quote_snapshot import (
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    load_spread_quote_snapshot,
    producer_generation_id,
    spread_quote_snapshot_path,
)
from lightfee.spread.universe import (
    SPREAD_SAMPLING_MAX_PAIR_COUNT,
    spread_sampling_pair_bound,
)
from scripts.diagnose_live import _build_stale_risk_state_alignment_summary

EXCHANGE_TRUTH_PROBE_TIMEOUT_S = 60.0
AUTO_FAIL_CLOSED_RECENT_WINDOW_MS = 24 * 3600 * 1000
DEFAULT_SPREAD_SNAPSHOT_MAX_AGE_MS = 60_000
FUNDING_ENTRY_MAX_FUTURE_SKEW_MS = 1_000
PRODUCTION_SERVICE_NAMES = (
    "lightfee-spread-bbo.service",
    "lightfee-spread-sidecar.service",
    "lightfee-live.service",
)


def _read_json(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return data


def _funding_entry_snapshot_report(
    path: str | Path,
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    """Verify the atomic complete live-entry generation, not the slow audit."""

    def _manifest_int(name: str, *, default: int = -1) -> int:
        value = manifest.get(name, default)
        if isinstance(value, bool):
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _diagnostic_int(name: str) -> int:
        value = candidate_diagnostics.get(name, 0)
        if isinstance(value, bool):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    fingerprints: list[str] = []
    manifest_path = funding_entry_snapshot_manifest_path(path)
    manifest: dict = {}
    identity = None
    snapshot = None
    for _attempt in range(2):
        try:
            candidate_manifest = _read_json(str(manifest_path))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
        identity_before = funding_entry_snapshot_identity(path, verify_digest=True)
        candidate_snapshot = load_funding_entry_snapshot(path)
        identity_after = funding_entry_snapshot_identity(path, verify_digest=True)
        if (
            identity_before is not None
            and identity_before == identity_after
            and candidate_snapshot is not None
            and str(candidate_manifest.get("generation_id", "") or "")
            == identity_before[0]
        ):
            manifest = candidate_manifest
            identity = identity_before
            snapshot = candidate_snapshot
            break
    if identity is None or snapshot is None or not manifest:
        fingerprints.append("funding_entry_generation_missing_or_invalid")

    ready_at_ms = int(getattr(snapshot, "ready_at_ms", 0) or 0)
    ready_age_ms = now_ms - ready_at_ms if ready_at_ms > 0 else None
    if (
        ready_age_ms is None
        or ready_age_ms < -FUNDING_ENTRY_MAX_FUTURE_SKEW_MS
        or ready_age_ms > max(int(max_age_ms), 0)
    ):
        fingerprints.append("funding_entry_generation_stale")

    payload_size = _manifest_int("payload_size_bytes")
    manifest_candidate_count = _manifest_int("candidate_count")
    manifest_quote_count = _manifest_int("quote_count")
    manifest_schema_version = _manifest_int("schema_version")
    page_count = _manifest_int("page_count")
    max_page_size_bytes = _manifest_int("max_page_size_bytes")
    candidates = list(getattr(snapshot, "candidates", []) or [])
    quotes = dict(getattr(snapshot, "quotes", {}) or {})
    candidate_diagnostics = dict(
        getattr(snapshot, "candidate_build_diagnostics", {}) or {}
    )
    source_data_ready = candidate_diagnostics.get("source_data_ready") is True
    eligible_frontier_complete = (
        candidate_diagnostics.get("eligible_frontier_complete") is True
    )
    entry_frontier_ready = source_data_ready and eligible_frontier_complete
    seed_pair_count = _diagnostic_int("seed_pair_count")
    pair_decision_count = _diagnostic_int("pair_decision_count")
    eligible_candidate_count = _diagnostic_int("eligible_candidate_count")
    omitted_eligible_count = _diagnostic_int("omitted_eligible_count")
    frontier_stop_reason = str(
        candidate_diagnostics.get("frontier_stop_reason", "") or ""
    )
    if payload_size < 0:
        fingerprints.append("funding_entry_payload_size_invalid")
    if (
        manifest_schema_version != 7
        or page_count <= 0
        or max_page_size_bytes <= 0
        or max_page_size_bytes > FUNDING_ENTRY_SNAPSHOT_MAX_BYTES
    ):
        fingerprints.append("funding_entry_page_manifest_invalid")
    if (
        manifest_candidate_count != len(candidates)
        or manifest_quote_count != len(quotes)
    ):
        fingerprints.append("funding_entry_manifest_count_mismatch")
    if (
        pair_decision_count != seed_pair_count
        or eligible_candidate_count != len(candidates)
        or omitted_eligible_count != 0
    ):
        fingerprints.append("funding_entry_opportunity_omitted")
    if any(
        bool(getattr(candidate, "blocked", True))
        or getattr(candidate, "economics_complete", False) is not True
        for candidate in candidates
    ):
        fingerprints.append("funding_entry_candidate_not_executable")
    candidate_venues = {
        str(venue or "").strip().lower()
        for candidate in candidates
        for venue in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        )
        if str(venue or "").strip()
    }
    degraded_venues = {
        str(venue or "").strip().lower()
        for venue in (getattr(snapshot, "degraded_venues", []) or [])
        if str(venue or "").strip()
    }
    degraded_symbols = getattr(snapshot, "degraded_symbols", {}) or {}
    symbol_degraded_venues = {
        str(venue or "").strip().lower()
        for venue, symbols in (
            degraded_symbols.items() if isinstance(degraded_symbols, dict) else []
        )
        if str(venue or "").strip() and symbols
    }
    scoped_degraded_venues = degraded_venues | symbol_degraded_venues
    unscoped_degradation = bool(
        not scoped_degraded_venues
        and (
            getattr(snapshot, "degraded_domains", [])
            or (degraded_symbols and not isinstance(degraded_symbols, dict))
        )
    )
    if candidates and (
        str(getattr(snapshot, "acquisition_mode", "") or "") != "fresh_sidecar"
        or bool(candidate_venues & scoped_degraded_venues)
        or unscoped_degradation
    ):
        fingerprints.append("funding_entry_candidate_evidence_degraded")
    if not candidates and not source_data_ready:
        fingerprints.append("funding_entry_source_data_unavailable")
    if not eligible_frontier_complete:
        fingerprints.append("funding_entry_frontier_incomplete")

    return HealthReport(
        name="sidecar_snapshot",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "data_plane": "funding_entry_v7",
            "manifest_path": str(manifest_path),
            "generation_id": identity[0] if identity is not None else "",
            "ready_at_ms": ready_at_ms,
            "ready_age_ms": ready_age_ms,
            "payload_size_bytes": payload_size,
            "page_max_bytes": FUNDING_ENTRY_SNAPSHOT_MAX_BYTES,
            "page_count": page_count,
            "max_page_size_bytes": max_page_size_bytes,
            "candidate_count": len(candidates),
            "quote_count": len(quotes),
            "diagnostics_only": not bool(candidates),
            "source_data_ready": source_data_ready,
            "eligible_frontier_complete": eligible_frontier_complete,
            "entry_frontier_ready": entry_frontier_ready,
            "seed_pair_count": seed_pair_count,
            "pair_decision_count": pair_decision_count,
            "eligible_candidate_count": eligible_candidate_count,
            "omitted_eligible_count": omitted_eligible_count,
            "frontier_stop_reason": frontier_stop_reason,
        },
    )


def _sidecar_audit_observation(report: HealthReport) -> HealthReport:
    """Expose background audit health without making it the live liveness gate."""

    source = report.details
    degraded_symbols = source.get("degraded_symbols", {})
    degraded_symbol_counts = (
        {
            str(venue): len(symbols)
            for venue, symbols in degraded_symbols.items()
            if isinstance(symbols, list)
        }
        if isinstance(degraded_symbols, dict)
        else {}
    )
    return HealthReport(
        name="sidecar_audit_snapshot",
        ok=True,
        severity="info",
        fingerprints=[],
        details={
            "data_plane": "background_full_audit",
            "audit_ok": report.ok,
            "audit_fingerprints": list(report.fingerprints),
            "observed_at_ms": source.get("observed_at_ms"),
            "age_ms": source.get("age_ms"),
            "quote_venues": source.get("quote_venues", []),
            "missing_venues": source.get("missing_venues", []),
            "degraded_venues": source.get("degraded_venues", []),
            "degraded_domains": source.get("degraded_domains", []),
            "degraded_symbol_counts": degraded_symbol_counts,
            "candidate_count": source.get("candidate_count", 0),
            "unblocked_candidate_count": source.get(
                "unblocked_candidate_count",
                0,
            ),
        },
    )


def _resolve_spread_snapshot_max_age_ms(
    explicit_max_age_ms: int | None,
    app_config,
) -> int:
    if explicit_max_age_ms is not None:
        return max(int(explicit_max_age_ms), 0)
    candidates: list[int] = []
    for owner, name in (
        (getattr(app_config, "runtime", None), "sidecar_snapshot_max_age_ms"),
        (getattr(app_config, "strategy", None), "spread_signal_ttl_ms"),
    ):
        configured = getattr(owner, name, None)
        try:
            if configured is not None and int(configured) > 0:
                candidates.append(int(configured))
        except (TypeError, ValueError):
            continue
    if candidates:
        return min(candidates)
    return DEFAULT_SPREAD_SNAPSHOT_MAX_AGE_MS


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


def _spread_bbo_runtime_report(
    path: str | Path,
    *,
    app_config,
    now_ms: int | None,
) -> HealthReport:
    fingerprints: list[str] = []
    details: dict[str, object] = {"path": str(path)}
    snapshot = load_spread_quote_snapshot(path)
    if snapshot is None:
        fingerprints.append("spread_bbo_snapshot_missing_or_invalid")
    else:
        checked_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        expected_venues = {
            str(venue.venue or "").strip().lower()
            for venue in getattr(app_config, "venues", [])
            if str(venue.venue or "").strip()
        }
        ttl_ms = max(
            int(getattr(getattr(app_config, "strategy", None), "spread_signal_ttl_ms", 0) or 0),
            1,
        )
        publish_age_ms = checked_at_ms - int(snapshot.published_at_ms or 0)
        quote_ages_by_key = {
            key: checked_at_ms - int(quote.observed_at_ms or 0)
            for key, quote in snapshot.quotes.items()
        }
        fresh_quotes = {
            key: quote
            for key, quote in snapshot.quotes.items()
            if 0 <= quote_ages_by_key[key] <= ttl_ms
        }
        observed_venues = {quote.venue for quote in fresh_quotes.values()}
        venues_by_symbol: dict[str, set[str]] = {}
        for quote in fresh_quotes.values():
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            if symbol and venue:
                venues_by_symbol.setdefault(symbol, set()).add(venue)
        observed_pair_count = sum(
            len(venues) * (len(venues) - 1) // 2
            for venues in venues_by_symbol.values()
        )
        sampling_symbols = list(getattr(snapshot, "sampling_symbols", []) or [])
        configured_venues = set(snapshot.configured_venues)
        evaluated_pair_bound = spread_sampling_pair_bound(
            sampling_symbols,
            configured_venues,
        )
        degraded_venues = sorted(
            {
                str(venue).strip().lower()
                for venue in getattr(snapshot, "degraded_venues", [])
                if str(venue).strip()
            }
        )
        degraded_symbols = {
            str(venue).strip().lower(): sorted(
                {
                    str(symbol).strip().upper()
                    for symbol in symbols
                    if str(symbol).strip()
                }
            )
            for venue, symbols in getattr(snapshot, "degraded_symbols", {}).items()
            if str(venue).strip() and symbols
        }
        quote_ages_ms = list(quote_ages_by_key.values())
        strict_symbol_completeness = bool(
            getattr(
                getattr(app_config, "strategy", None),
                "spread_live_enabled",
                False,
            )
        )
        details.update(
            {
                "published_at_ms": snapshot.published_at_ms,
                "publish_age_ms": publish_age_ms,
                "batch_started_at_ms": snapshot.batch_started_at_ms,
                "quote_count": len(snapshot.quotes),
                "fresh_quote_count": len(fresh_quotes),
                "stale_quote_count": len(snapshot.quotes) - len(fresh_quotes),
                "configured_venues": sorted(configured_venues),
                "observed_venues": sorted(observed_venues),
                "degraded_venues": degraded_venues,
                "degraded_symbols": degraded_symbols,
                "max_quote_age_ms": max(quote_ages_ms, default=None),
                "signal_ttl_ms": ttl_ms,
                "checked_at_ms": checked_at_ms,
                "producer_generation_id": snapshot.producer_generation_id,
                "schema_version": getattr(snapshot, "schema_version", 0),
                "sampling_symbol_count": len(sampling_symbols),
                "observed_pair_count": observed_pair_count,
                "evaluated_pair_bound": evaluated_pair_bound,
                "evaluated_pair_limit": SPREAD_SAMPLING_MAX_PAIR_COUNT,
                "strict_symbol_completeness": strict_symbol_completeness,
                "execution_contract": (
                    "live" if strict_symbol_completeness else "paper_only"
                ),
            }
        )
        if getattr(snapshot, "schema_version", 0) != SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION:
            fingerprints.append("spread_bbo_sampling_contract_unavailable")
        if not sampling_symbols:
            fingerprints.append("spread_bbo_sampling_universe_empty")
        if evaluated_pair_bound > SPREAD_SAMPLING_MAX_PAIR_COUNT:
            fingerprints.append("spread_bbo_sampling_pair_budget_exceeded")
        if configured_venues != expected_venues:
            fingerprints.append("spread_bbo_configured_venue_mismatch")
        if observed_venues != expected_venues:
            fingerprints.append("spread_bbo_venue_coverage_incomplete")
        if strict_symbol_completeness and degraded_venues:
            fingerprints.append("spread_bbo_venue_degraded")
        if strict_symbol_completeness and degraded_symbols:
            fingerprints.append("spread_bbo_symbol_degraded")
        if publish_age_ms < 0 or publish_age_ms > ttl_ms:
            fingerprints.append("spread_bbo_publication_stale")
        if strict_symbol_completeness and any(
            age < 0 or age > ttl_ms for age in quote_ages_ms
        ):
            fingerprints.append("spread_bbo_quote_stale")

        try:
            main_pid = _systemd_main_pid("lightfee-spread-bbo.service")
            process_started_at_ms = _process_started_at_ms(main_pid)
        except (OSError, subprocess.SubprocessError):
            main_pid = 0
            process_started_at_ms = 0
        details["main_pid"] = main_pid
        details["process_started_at_ms"] = process_started_at_ms
        expected_generation_id = producer_generation_id(main_pid) if main_pid > 0 else ""
        details["expected_generation_id"] = expected_generation_id
        if process_started_at_ms <= 0:
            fingerprints.append("spread_bbo_process_start_unavailable")
        if not expected_generation_id or (
            snapshot.producer_generation_id != expected_generation_id
        ):
            fingerprints.append("spread_bbo_snapshot_generation_mismatch")

    return HealthReport(
        name="spread_bbo_runtime",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details=details,
    )


def _spread_snapshot_runtime_report(
    path: str | Path,
    *,
    max_age_ms: int,
    now_ms: int | None,
) -> HealthReport:
    """Read first, then sample wall time so a concurrent publish is not future."""

    snapshot = _read_json(path)
    checked_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    return analyze_spread_snapshot(
        snapshot,
        now_ms=checked_at_ms,
        max_age_ms=max_age_ms,
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
    parser.add_argument("--spread-bbo-snapshot", default=None)
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
        help="Fail readiness when the live funding canary entry policy is disabled.",
    )
    parser.add_argument(
        "--require-active-services",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Require all three production systemd services and current BBO output.",
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
    spread_bbo_unit = unit_texts.get("lightfee-spread-bbo.service", "")
    spread_sidecar_unit = unit_texts.get("lightfee-spread-sidecar.service", "")
    if "-m lightfee.apps.spread_bbo" not in spread_bbo_unit:
        ownership_fingerprints.append("dedicated_spread_bbo_entrypoint_missing")
    if "lightfee-spread-bbo.service" not in spread_sidecar_unit:
        ownership_fingerprints.append("spread_consumer_dependency_missing")
    if "lightfee-sidecar.service" in live_unit:
        ownership_fingerprints.append("live_funding_sidecar_dependency_present")
    if "lightfee-sidecar.service" in spread_bbo_unit:
        ownership_fingerprints.append("spread_bbo_funding_sidecar_dependency_present")
    reports.append(
        HealthReport(
            name="spread_bbo_ownership",
            ok=not ownership_fingerprints,
            severity="critical" if ownership_fingerprints else "info",
            fingerprints=ownership_fingerprints,
            details={"writer_model": "dedicated_process"},
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

    entry_manifest_path = funding_entry_snapshot_manifest_path(args.snapshot)
    entry_snapshot_path = Path(args.snapshot)
    if (
        app_config is not None
        and app_config.runtime.opportunity_input_mode == "single_process_entry"
    ):
        # Funding-entry handoff is retired in this mode.  A previous deployment
        # can leave its V6 snapshot or manifest behind; it must not make the
        # live-service health gate fail after the runtime has stopped reading it.
        reports.append(
            HealthReport(
                name="funding_entry_handoff",
                ok=True,
                severity="info",
                fingerprints=[],
                details={
                    "mode": "single_process_entry",
                    "status": "not_required",
                    "path": args.snapshot,
                    "retired_snapshot_present": entry_snapshot_path.exists(),
                    "retired_manifest_present": entry_manifest_path.exists(),
                },
            )
        )
    elif entry_manifest_path.exists():
        reports.append(
            _funding_entry_snapshot_report(
                args.snapshot,
                now_ms=now_ms,
                max_age_ms=args.snapshot_max_age_ms,
            )
        )
        if entry_snapshot_path.exists():
            reports.append(
                _sidecar_audit_observation(
                    analyze_sidecar_snapshot(
                        _read_json(args.snapshot),
                        now_ms=now_ms,
                        max_age_ms=args.snapshot_max_age_ms,
                    )
                )
            )
        else:
            reports.append(
                HealthReport(
                    name="sidecar_audit_snapshot",
                    ok=True,
                    severity="info",
                    fingerprints=[],
                    details={
                        "data_plane": "background_full_audit",
                        "audit_ok": False,
                        "audit_fingerprints": ["snapshot_file_missing"],
                        "path": args.snapshot,
                    },
                )
            )
    elif entry_snapshot_path.exists():
        # Backward-compatible diagnostics for pre-V6 and explicit fixture paths.
        reports.append(
            analyze_sidecar_snapshot(
                _read_json(args.snapshot),
                now_ms=now_ms,
                max_age_ms=args.snapshot_max_age_ms,
            )
        )
    else:
        reports.append(
            HealthReport(
                name="funding_entry_handoff",
                ok=True,
                severity="info",
                fingerprints=[],
                details={
                    "mode": "single_process_entry",
                    "status": "not_required",
                    "path": args.snapshot,
                },
            )
        )

    if require_active_services:
        spread_bbo_snapshot = args.spread_bbo_snapshot or str(
            spread_quote_snapshot_path(args.snapshot)
        )
        if app_config is None:
            reports.append(
                HealthReport(
                    name="spread_bbo_runtime",
                    ok=False,
                    severity="critical",
                    fingerprints=["spread_bbo_runtime_config_unavailable"],
                    details={"path": spread_bbo_snapshot},
                )
            )
        else:
            reports.append(
                _spread_bbo_runtime_report(
                    spread_bbo_snapshot,
                    app_config=app_config,
                    now_ms=args.now_ms or None,
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
            status = "PASS" if report.ok else report.severity.upper()
            print(f"{status} {report.name}: {','.join(report.fingerprints) or 'ok'}")
    sys.exit(0 if summary.ok else 1)


if __name__ == "__main__":
    main()
