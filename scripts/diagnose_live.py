#!/usr/bin/env python3
"""Read-only production diagnostics for LightFeeV2 live.

Consumes structured journal events + live state + exchange snapshots
to produce a stable JSON diagnose artifact.  This artifact is consumed
by both local operators and the wlcodex Telegram cockpit — same facts,
same conclusion.

Usage:
  python scripts/diagnose_live.py --json                     # full diagnose
  python scripts/diagnose_live.py --compact-json             # field-focused diagnose
  python scripts/diagnose_live.py --json --symbol BTCUSDT     # filter
  python scripts/diagnose_live.py --json --runtime-dir ./tests/fixtures
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

from lightfee.marketdata.local_l2_incident_classification import (
    has_official_sequence_rebuild_evidence,
)
from lightfee.core.domain import Venue
from lightfee.config.paths import remember_hyperliquid_info_coordinator_directory
from lightfee.core.order_identity import normalize_order_identity
from lightfee.engine.exchange_truth import (
    build_venue_operation_request,
    normalize_exchange_truth_payload,
    request_venue_operation,
)
from lightfee.engine.business_contract import (
    classify_close_reconciliation_state,
    classify_noise_visibility,
    close_reconciliation_exchange_truth_clean,
    close_reconciliation_evidence_contract,
    close_order_error_resolution_contract,
    diagnose_issue_counts,
    entry_market_evidence_contract,
    passive_close_final_truth_contract,
    passive_close_has_terminal_truth as contract_passive_close_has_terminal_truth,
    normalize_close_reconciliation_record,
    quote_rewarm_handoff_contract,
    _payload_is_aster_reduce_only_no_order_reject,
)
from lightfee.engine.lifecycle_sla import (
    LifecyclePhaseBudget,
    classify_phase_age,
    phase_budgets_from_strategy,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex
from lightfee.engine.venue_private_health import (
    is_private_health_admission_reason,
    private_health_status_for_admission_reason,
)
from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table
from lightfee.lifecycle.exchange_truth_ledger import build_exchange_truth_lifecycle
from lightfee.offline.analysis.journal import (
    _entry_time_info,
    _select_quick_flat_entry_time,
    summarize_quick_flat_events,
)
from lightfee.ops.auto_fail_closed_events import build_auto_fail_closed_summary
from lightfee.ops.diagnostics.reporting import render_budgeted_json
from lightfee.ops.position_side_semantics import side_matches_business_leg
from lightfee.venues.symbol_eligibility import (
    PRIVATE_TRUTH_UNSUPPORTED_REASON,
    venue_symbol_eligibility,
)
from lightfee.venues.specs import VenueOperation

# Schema version — bump when output shape changes
SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# Default paths (production-correct)
# ---------------------------------------------------------------------------
DEFAULT_RUNTIME_DIR = "/opt/lightfee-v2/runtime"
PRODUCTION_STATE_FILE = "live-state-current.json"
FALLBACK_STATE_FILE = "state-current.json"
DEFAULT_DEPLOY_FILE = ".deploy_version"
DEFAULT_UNIT_DIR = "/etc/systemd/system"
DEFAULT_MAX_EVENTS = 50_000
SERVICE_NAMES = [
    "lightfee-live.service",
    "lightfee-spread-sidecar.service",
]
DEFAULT_EXCHANGE_TRUTH_VENUES = [
    "binance",
    "bybit",
    "aster",
    "okx",
    "bitget",
    "gate",
    "hyperliquid",
]
UNFILTERED_PROBE_KEY = "*"

ORDER_ERROR_KINDS = frozenset({
    "order.rejected",
    "order.uncertain",
    "exit.passive_close_maker_submit_error",
    "exit.passive_close_hedge_error",
})

L2_WARNING_KINDS = frozenset({
    "runtime.local_l2_sequence_gap",
    "runtime.local_l2_sync_failed",
    "runtime.snapshot_stale",
    "runtime.snapshot_degraded",
    "runtime.entry_blocked_local_l2_selection",
    "runtime.entry_local_l2_readiness_diagnostics",
    "runtime.close_price_evidence_rewarm_failed",
    "runtime.passive_close_readiness_blocked",
    "exit.passive_close_maker_terminal_no_fill",
    "scan.no_entry_diagnostics",
})

RUNTIME_WARNING_KINDS = frozenset({
    "runtime.lifecycle_changed",
    "runtime.risk_mode_changed",
    "runtime.fail_closed",
    "risk.warning_triggered",
    "risk.death_triggered",
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with open(p) as f:
        return json.load(f)


def _read_jsonl_tail(
    path: str | Path,
    max_records: int = DEFAULT_MAX_EVENTS,
    since_ms: int = 0,
) -> list[dict[str, Any]]:
    """Read JSONL from tail (most recent events first), respecting since_ms filter."""
    p = Path(path)
    if not p.exists():
        return []
    file_size = p.stat().st_size
    if file_size == 0:
        return []

    records: list[dict[str, Any]] = []
    chunk_size = min(file_size, max(200_000, max_records * 1000))
    with open(p, "rb") as f:
        if file_size > chunk_size:
            f.seek(file_size - chunk_size)
            f.readline()  # skip partial first line
        for line_bytes in f:
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = int(rec.get("ts_ms", 0) or 0)
                if since_ms and ts < since_ms:
                    continue
                records.append(rec)
            except json.JSONDecodeError:
                pass

    if len(records) < max_records and since_ms > 0:
        # Fallback to full scan for time-filtered window
        records = []
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    ts = int(rec.get("ts_ms", 0) or 0)
                    if since_ms and ts < since_ms:
                        continue
                    records.append(rec)
                    if len(records) >= max_records:
                        break
                except json.JSONDecodeError:
                    pass

    if len(records) > max_records:
        records = records[-max_records:]
    return records


def _read_jsonl_since(path: str | Path, since_ms: int = 0) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(p) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = int(rec.get("ts_ms", 0) or 0)
                if since_ms and ts < since_ms:
                    continue
                records.append(rec)
            except json.JSONDecodeError:
                pass
    return records


def _limit_since_deploy_events(
    events: list[dict[str, Any]],
    max_records: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if max_records <= 0 or len(events) <= max_records:
        return events, {
            "event_scan_truncated": False,
            "events_dropped_by_cap": 0,
            "events_before_cap": len(events),
        }

    materialized: list[dict[str, Any]] = []
    latest: list[dict[str, Any]] = []
    materialized_ids: set[int] = set()
    for rec in events:
        kind = str(rec.get("kind") or "")
        if _event_kind_contributes_exchange_truth_scope(kind):
            materialized.append(rec)
            materialized_ids.add(id(rec))

    remaining_slots = max(max_records - len(materialized), 0)
    if remaining_slots:
        for rec in events[-max_records:]:
            if id(rec) not in materialized_ids:
                latest.append(rec)
        latest = latest[-remaining_slots:]

    selected = sorted(
        materialized + latest,
        key=lambda rec: int(rec.get("ts_ms", 0) or 0),
    )
    return selected, {
        "event_scan_truncated": True,
        "events_dropped_by_cap": max(len(events) - len(selected), 0),
        "events_before_cap": len(events),
        "materialized_events_preserved": len(materialized),
    }


def _find_event_files(runtime_dir: str) -> list[Path]:
    base = Path(runtime_dir)
    if not base.exists():
        return []
    candidates: list[Path] = []
    for pattern in ["live-events*.jsonl", "events*.jsonl", "*.jsonl"]:
        for p in sorted(base.glob(pattern)):
            if p not in candidates:
                candidates.append(p)
    return candidates


def _resolve_state_path(runtime_dir: str, explicit_path: str = "") -> tuple[Path, str]:
    """Resolve state path with production-priority fallback.

    Priority: explicit path > live-state-current.json > state-current.json.
    Returns (path, source_label).
    """
    if explicit_path:
        return Path(explicit_path), "explicit"
    live_path = Path(runtime_dir) / PRODUCTION_STATE_FILE
    if live_path.exists():
        return live_path, "live-state-current.json"
    fallback_path = Path(runtime_dir) / FALLBACK_STATE_FILE
    if fallback_path.exists():
        return fallback_path, "state-current.json (fallback)"
    return fallback_path, "state-current.json (not found)"


def _try_read_unit(unit_dir: str, name: str) -> str:
    p = Path(unit_dir) / name
    try:
        return p.read_text()
    except (OSError, PermissionError):
        return ""


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
        try:
            lines = path.read_text().splitlines()
        except (OSError, PermissionError):
            continue
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or not key.replace("_", "").isalnum():
                continue
            value = value.strip()
            if (
                len(value) >= 2
                and value[0] == value[-1]
                and value[0] in {"'", '"'}
            ):
                value = value[1:-1]
            os.environ.setdefault(key, value)
    return loaded


def _load_systemd_environment_files(unit_dir: str) -> list[str]:
    unit_texts = {
        name: _try_read_unit(unit_dir, name)
        for name in SERVICE_NAMES
    }
    return _load_environment_files(_environment_file_paths(unit_texts))


def _git_head(project_dir: str = "/opt/lightfee-v2") -> str:
    try:
        result = subprocess.run(
            ["git", "-C", project_dir, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _git_commit_time(project_dir: str = "/opt/lightfee-v2") -> str:
    try:
        result = subprocess.run(
            ["git", "-C", project_dir, "log", "-1", "--format=%cI", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _systemd_active_since(service_name: str) -> int:
    try:
        result = subprocess.run(
            ["systemctl", "show", service_name, "--property=ActiveEnterTimestamp"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.strip().split("\n"):
            if line.startswith("ActiveEnterTimestamp="):
                ts_str = line.split("=", 1)[1].strip()
                if ts_str:
                    try:
                        from datetime import datetime
                        dt = datetime.strptime(ts_str, "%a %Y-%m-%d %H:%M:%S %Z")
                        return int(dt.timestamp() * 1000)
                    except ValueError:
                        pass
        return 0
    except Exception:
        return 0


def _runtime_started_at(events: list[dict[str, Any]]) -> int:
    earliest = 0
    for rec in events:
        ts = int(rec.get("ts_ms", 0) or 0)
        kind = str(rec.get("kind", ""))
        if kind == "runtime.lifecycle_changed":
            payload = rec.get("payload", {})
            if isinstance(payload, dict) and str(payload.get("to", "")) == "running":
                if earliest == 0 or ts < earliest:
                    earliest = ts
    return earliest


# ---------------------------------------------------------------------------
# deploy status
# ---------------------------------------------------------------------------

def _read_deploy_version(runtime_dir: str, project_dir: str = "/opt/lightfee-v2") -> str:
    p = Path(project_dir) / DEFAULT_DEPLOY_FILE
    try:
        if p.exists():
            return p.read_text().strip()
    except (OSError, PermissionError):
        pass
    p2 = Path(runtime_dir) / "deploy_version.txt"
    try:
        return p2.read_text().strip()
    except (OSError, PermissionError):
        return ""


def _deploy_versions_match(git_head: str, deploy_version: str) -> bool:
    git_head = str(git_head or "").strip()
    deploy_version = str(deploy_version or "").strip()
    if not git_head or not deploy_version:
        return False
    return git_head == deploy_version or deploy_version.startswith(git_head)


def _build_deploy_status(runtime_dir: str) -> dict[str, Any]:
    git_head = _git_head()
    commit_time = _git_commit_time()
    deploy_version = _read_deploy_version(runtime_dir)
    mismatch = bool(
        git_head
        and deploy_version
        and not _deploy_versions_match(git_head, deploy_version)
    )
    return {
        "git_head": git_head,
        "commit_time": commit_time,
        "deploy_version": deploy_version,
        "version_mismatch": mismatch,
    }


# ---------------------------------------------------------------------------
# service status
# ---------------------------------------------------------------------------

def _build_service_status(unit_dir: str) -> dict[str, Any]:
    status: dict[str, Any] = {}
    for name in SERVICE_NAMES:
        unit_text = _try_read_unit(unit_dir, name)
        active = "unknown"
        n_restarts = 0
        started_at = 0
        try:
            result = subprocess.run(
                ["systemctl", "is-active", name],
                capture_output=True, text=True, timeout=5,
            )
            active = result.stdout.strip()
        except Exception:
            pass
        if active == "active":
            try:
                nr = subprocess.run(
                    ["systemctl", "show", name, "--property=NRestarts"],
                    capture_output=True, text=True, timeout=5,
                )
                for line in nr.stdout.strip().split("\n"):
                    if line.startswith("NRestarts="):
                        try:
                            n_restarts = int(line.split("=", 1)[1])
                        except ValueError:
                            pass
            except Exception:
                pass
            started_at = _systemd_active_since(name)
        status[name.replace(".service", "")] = {
            "active": active,
            "unit_exists": bool(unit_text),
            "n_restarts": n_restarts,
            "started_at_ms": started_at,
        }
    return status


# ---------------------------------------------------------------------------
# Window calculation (since_deploy)
# ---------------------------------------------------------------------------

def _compute_window(
    since_deploy: bool,
    generated_at_ms: int,
    deploy_status: dict[str, Any],
    service_status: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    if not since_deploy:
        return {
            "mode": "all_available",
            "since_ms": 0,
            "until_ms": generated_at_ms,
            "source": "no time filter",
            "confidence": "high",
        }

    candidates: list[tuple[int, str]] = []
    for svc_name, svc in service_status.items():
        started = svc.get("started_at_ms", 0)
        if started > 0:
            candidates.append((started, f"service_{svc_name}_started_at"))

    commit_time = deploy_status.get("commit_time", "")
    if commit_time:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(commit_time)
            ts = int(dt.timestamp() * 1000)
            candidates.append((ts, "deploy_commit_time"))
        except (ValueError, OSError):
            pass

    runtime_start = _runtime_started_at(events)
    if runtime_start > 0:
        candidates.append((runtime_start, "runtime_lifecycle_changed_to_running"))

    if candidates:
        since_ms = max(c[0] for c in candidates)
        source = ", ".join(c[1] for c in candidates)
        return {
            "mode": "since_deploy",
            "since_ms": since_ms,
            "until_ms": generated_at_ms,
            "source": source,
            "confidence": "high",
        }

    return {
        "mode": "since_deploy_fallback_24h",
        "since_ms": generated_at_ms - 24 * 3600 * 1000,
        "until_ms": generated_at_ms,
        "source": "fallback_24h — no deploy/service time available",
        "confidence": "low",
        "missing_evidence": ["deploy_or_service_start_time"],
    }


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------

def _build_health(state: dict[str, Any], service_status: dict[str, Any]) -> dict[str, Any]:
    fingerprints: list[str] = []
    critical = 0
    warning = 0

    lifecycle = str(state.get("lifecycle", ""))
    risk_mode = str(state.get("risk_mode", ""))
    if lifecycle not in ("running", ""):
        fingerprints.append("lifecycle_{}".format(lifecycle))
        critical += 1
    if risk_mode in ("fail_closed", "risk_only"):
        fingerprints.append("risk_mode_{}".format(risk_mode))
        critical += 1
    if risk_mode == "warning":
        warning += 1

    for svc_name, svc in service_status.items():
        if svc.get("active") not in ("active", "unknown"):
            fingerprints.append("service_{}_{}".format(svc_name, svc.get("active")))
            critical += 1

    last_tick = int(state.get("last_tick_ms", 0) or 0)
    if last_tick and _now_ms() - last_tick > 300_000:
        fingerprints.append("tick_stale_5min")
        warning += 1

    return {
        "ok": critical == 0,
        "critical_count": critical,
        "warning_count": warning,
        "fingerprints": fingerprints,
    }


# ---------------------------------------------------------------------------
# local state
# ---------------------------------------------------------------------------

def _build_local_state(
    state: dict[str, Any], events: list[dict[str, Any]]
) -> dict[str, Any]:
    positions: list[dict[str, Any]] = []
    for pos in state.get("open_positions", []) or []:
        if isinstance(pos, dict):
            positions.append({
                "position_id": pos.get("position_id", ""),
                "symbol": pos.get("symbol", ""),
                "long_venue": pos.get("long_venue", ""),
                "short_venue": pos.get("short_venue", ""),
                "quantity": pos.get("quantity", 0),
                "matched_quantity": pos.get("matched_quantity", 0),
                "opened_at_ms": pos.get("opened_at_ms", 0),
            })

    return {
        "lifecycle": str(state.get("lifecycle", "unknown")),
        "risk_mode": str(state.get("risk_mode", "unknown")),
        "global_risk_mode": str(state.get("global_risk_mode", state.get("risk_mode", "unknown"))),
        "global_risk_reason": state.get("global_risk_reason"),
        "recovery_blocked_reason": state.get("recovery_blocked_reason"),
        "hyperliquid_trading_disabled_reason": state.get(
            "hyperliquid_trading_disabled_reason"
        ),
        "open_position_count": int(state.get("open_position_count", 0) or 0),
        "pending_entry_count": int(state.get("pending_entry_count", 0) or 0),
        "pending_entries": state.get("pending_entries", []),
        "pending_close_count": int(state.get("pending_close_count", 0) or 0),
        "open_positions": positions,
        "positions": positions,
        "last_tick_ms": int(state.get("last_tick_ms", 0) or 0),
        "runtime_progress": dict(state.get("runtime_progress") or {}),
        "runtime_market_data_config": dict(
            state.get("runtime_market_data_config") or {}
        ),
        "state_path": state.get("_state_path", ""),
        "state_path_source": state.get("_state_path_source", ""),
        "v1_lifecycle_closure": dict(state.get("v1_lifecycle_closure") or {}),
    }


# ---------------------------------------------------------------------------
# exchange truth — read-only position/order fetching
# ---------------------------------------------------------------------------

def _load_venue_credential(venue: str) -> Optional[Any]:
    prefix = "LIGHTFEE_{}_".format(venue.upper())
    api_key = os.environ.get(prefix + "API_KEY", "")
    api_secret = os.environ.get(prefix + "API_SECRET", "")
    wallet_private_key = (
        os.environ.get(prefix + "WALLET_PRIVATE_KEY", "")
        or os.environ.get(prefix + "PRIVATE_KEY", "")
    )
    account_address = os.environ.get(prefix + "ACCOUNT_ADDRESS", "")
    wallet_mode = os.environ.get(prefix + "WALLET_MODE", "")
    if not ((api_key and api_secret) or wallet_private_key or account_address):
        return None
    try:
        from lightfee.venues.transport import (
            LiveCredential,
            _normalize_hyperliquid_wallet_mode,
        )
        if venue.lower() == "hyperliquid":
            wallet_mode = _normalize_hyperliquid_wallet_mode(wallet_mode)
        return LiveCredential(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=os.environ.get(prefix + "API_PASSPHRASE", ""),
            wallet_private_key=wallet_private_key,
            account_address=account_address,
            wallet_mode=wallet_mode or "account_wallet",
        )
    except Exception:
        return None


def _create_readonly_rate_limiter() -> Any:
    from lightfee.venues.transport import EndpointRateLimiter

    return EndpointRateLimiter(initial_ms=1000, max_ms=8000, pacing_interval_ms=25)


def _install_readonly_exchange_truth_rate_limit_runtime() -> Any:
    from lightfee.rate_limit.config import RateLimitConfigManager
    from lightfee.rate_limit.engine import (
        RateLimitRuntime,
        global_rate_limit_runtime,
        install_global_rate_limit_runtime,
    )

    previous = global_rate_limit_runtime()
    install_global_rate_limit_runtime(
        RateLimitRuntime(config_manager=RateLimitConfigManager(config_path=None))
    )
    return previous


def _restore_readonly_exchange_truth_rate_limit_runtime(previous: Any) -> None:
    from lightfee.rate_limit.engine import install_global_rate_limit_runtime

    install_global_rate_limit_runtime(previous)


def _create_readonly_adapter(
    venue: str,
    credential: Any,
    *,
    rate_limiter: Any = None,
) -> Optional[Any]:
    try:
        if venue.lower() == "binance":
            from lightfee.venues.binance import BinanceAdapter
            return BinanceAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "bybit":
            from lightfee.venues.bybit import BybitAdapter
            return BybitAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "aster":
            from lightfee.venues.aster import AsterAdapter
            return AsterAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "okx":
            from lightfee.venues.okx import OkxAdapter
            return OkxAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "bitget":
            from lightfee.venues.bitget import BitgetAdapter
            return BitgetAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "gate":
            from lightfee.venues.gate import GateAdapter
            return GateAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
        elif venue.lower() == "hyperliquid":
            from lightfee.venues.hyperliquid import HyperliquidAdapter
            return HyperliquidAdapter(
                mode="live", credential=credential, rate_limiter=rate_limiter
            )
    except Exception:
        pass
    return None


def _mask_address(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 12:
        return text
    return "{}...{}".format(text[:6], text[-4:])


def _address_hash(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _hyperliquid_credential_identity(credential: Any) -> dict[str, Any]:
    try:
        from lightfee.venues.transport import (
            resolve_hyperliquid_credential_identity,
        )
    except Exception:
        return {
            "wallet_mode": str(getattr(credential, "wallet_mode", "") or ""),
            "account_address_present": bool(
                str(getattr(credential, "account_address", "") or "").strip()
            ),
            "signer_address_present": False,
            "account_matches_signer": False,
        }

    resolved = resolve_hyperliquid_credential_identity(credential)
    wallet_mode = str(resolved.get("wallet_mode") or "")
    account_address = str(resolved.get("account_address") or "")
    signer_address = str(resolved.get("signer_address") or "")
    account_matches_signer = bool(resolved.get("account_matches_signer"))
    identity: dict[str, Any] = {
        "wallet_mode": wallet_mode,
        "account_address_present": bool(resolved.get("account_address_present")),
        "signer_address_present": bool(resolved.get("signer_address_present")),
        "account_matches_signer": account_matches_signer,
        "signer_matches_account": account_matches_signer,
        "account_address_masked": _mask_address(account_address),
        "account_address_hash": _address_hash(account_address),
        "signer_address_masked": _mask_address(signer_address),
        "signer_address_hash": _address_hash(signer_address),
        "allow_api_wallet_authorization_probe": bool(
            resolved.get("allow_api_wallet_authorization_probe")
        ),
    }
    signer_error = str(resolved.get("signer_address_error") or "")
    if signer_error:
        identity["signer_address_error"] = signer_error
    return identity


def _build_hyperliquid_trading_authorization_summary(
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    credential_identity = exchange_truth.get("credential_identity", {})
    if not isinstance(credential_identity, dict):
        credential_identity = {}
    identity = credential_identity.get(Venue.HYPERLIQUID.value, {})
    if not isinstance(identity, dict):
        identity = {}

    fetch_status = exchange_truth.get("fetch_status", {})
    if not isinstance(fetch_status, dict):
        fetch_status = {}
    hyper_fetch_status = fetch_status.get(Venue.HYPERLIQUID.value, {})
    if not isinstance(hyper_fetch_status, dict):
        hyper_fetch_status = {}
    available_venues = {
        str(venue).lower()
        for venue in exchange_truth.get("available_venues", []) or []
    }

    disabled_reason = str(
        local_state.get("hyperliquid_trading_disabled_reason") or ""
    )
    wallet_mode = str(identity.get("wallet_mode") or "")
    signer_matches_account = bool(
        identity.get("signer_matches_account")
        or identity.get("account_matches_signer")
        or identity.get("wallet_matches_account")
    )
    account_state_readable = (
        Venue.HYPERLIQUID.value in available_venues
        or str(hyper_fetch_status.get("status") or "").lower() == "ok"
    )
    policy_block = (
        disabled_reason == "api_wallet_authorization_not_verified_strict_readonly"
    )
    authorization_probe_allowed = bool(
        identity.get("allow_api_wallet_authorization_probe")
    )
    api_wallet_authorization_verified = bool(
        identity.get("api_wallet_authorization_verified")
    )
    trading_authorization_trusted = (
        not disabled_reason
        and account_state_readable
        and (
            signer_matches_account
            or (
                wallet_mode == "api_wallet"
                and api_wallet_authorization_verified
            )
        )
    )
    if policy_block:
        next_action = "keep_hyperliquid_readonly_until_authorization_proven"
    elif trading_authorization_trusted:
        next_action = "hyperliquid_trading_authorization_trusted"
    elif wallet_mode == "api_wallet":
        next_action = "prove_api_wallet_authorization_or_keep_readonly"
    else:
        next_action = "resolve_hyperliquid_account_signer_identity"

    return {
        "wallet_mode": wallet_mode,
        "account_state_readable": bool(account_state_readable),
        "signer_matches_account": signer_matches_account,
        "trading_authorization_trusted": bool(trading_authorization_trusted),
        "policy_block": bool(policy_block),
        "trading_disabled_reason": disabled_reason,
        "next_action": next_action,
        "allow_api_wallet_authorization_probe": authorization_probe_allowed,
        "api_wallet_authorization_verified": api_wallet_authorization_verified,
        "account_address_present": bool(identity.get("account_address_present")),
        "signer_address_present": bool(identity.get("signer_address_present")),
        "account_address_masked": str(identity.get("account_address_masked") or ""),
        "account_address_hash": str(identity.get("account_address_hash") or ""),
        "signer_address_masked": str(identity.get("signer_address_masked") or ""),
        "signer_address_hash": str(identity.get("signer_address_hash") or ""),
    }


def _event_payload_mentions_hyperliquid(payload: dict[str, Any]) -> bool:
    try:
        text = json.dumps(payload, sort_keys=True, default=str).lower()
    except TypeError:
        text = str(payload).lower()
    return "hyperliquid" in text


def _is_hyperliquid_canonical_trade_event(
    kind: str,
    payload: dict[str, Any],
) -> bool:
    venue = str(payload.get("venue") or "").lower()
    if kind.startswith("order."):
        return venue == Venue.HYPERLIQUID.value
    if kind in {"entry.opened", "runtime.position_opened"}:
        return Venue.HYPERLIQUID.value in {
            venue,
            str(payload.get("long_venue") or "").lower(),
            str(payload.get("short_venue") or "").lower(),
        }
    return False


def _build_hyperliquid_historical_trade_evidence(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    ignored_quote_or_preflight_count = 0
    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        if _is_hyperliquid_canonical_trade_event(kind, payload):
            sample = {
                "kind": kind,
                "ts_ms": int(rec.get("ts_ms", 0) or 0),
                "symbol": str(payload.get("symbol") or ""),
                "venue": str(payload.get("venue") or ""),
                "long_venue": str(payload.get("long_venue") or ""),
                "short_venue": str(payload.get("short_venue") or ""),
            }
            for key in ("account_address_masked", "signer_address_masked"):
                if payload.get(key):
                    sample[key] = str(payload.get(key))
            samples.append(sample)
        elif _event_payload_mentions_hyperliquid(payload):
            ignored_quote_or_preflight_count += 1

    success_count = len(samples)
    return {
        "canonical_success_count": success_count,
        "has_successful_hyperliquid_order_evidence": success_count > 0,
        "conclusion": (
            "successful_hyperliquid_order_evidence_present"
            if success_count > 0
            else "no_successful_hyperliquid_order_evidence_in_window"
        ),
        "ignored_quote_or_preflight_count": ignored_quote_or_preflight_count,
        "samples": samples[:10],
    }


def _exchange_truth_venues_for_diagnose(
    *,
    explicit_venues: list[str] | None,
    position_venues: list[str],
    local_state: dict[str, Any],
) -> list[str] | None:
    if explicit_venues is not None:
        return explicit_venues
    # Production diagnosis is an account-level safety assertion, not an
    # optimisation of the current local/event scope. An empty local state (or
    # a quiet deployment window) must still prove every configured account is
    # flat and free of orders. V1's recovery truth is likewise unfiltered.
    del position_venues, local_state
    return list(DEFAULT_EXCHANGE_TRUTH_VENUES)


_EXCHANGE_TRUTH_SCOPE_EVENT_KINDS = frozenset({
    "entry.opened",
    "entry.aborted",
    "execution.dual_taker_armed",
    "execution.passive_cycle_zero_fill",
    "passive_maintenance.maker_progress",
    "recovery.flat",
    "reconciliation.entry_flat_unresolved_maker_retained",
    "reconciliation.entry_flatten_residual_submitted",
    "reconciliation.entry_flatten_residual_succeeded",
    "runtime.entry_dispatched",
    "runtime.normal_close_routing_passive",
    "runtime.pending_entry_registered",
    "runtime.position_drift_corrected",
    "runtime.position_drift_detected",
    "runtime.position_lifecycle_terminal",
    "runtime.position_opened",
})

_EXCHANGE_TRUTH_SCOPE_EVENT_PREFIXES = (
    "exit.",
    "order.",
    "pending_entry.",
)


def _event_kind_contributes_exchange_truth_scope(kind: str) -> bool:
    return (
        kind in _EXCHANGE_TRUTH_SCOPE_EVENT_KINDS
        or kind.startswith(_EXCHANGE_TRUTH_SCOPE_EVENT_PREFIXES)
    )


def _canonical_event_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or text in {"NONE", "NULL"}:
        return ""
    if text.endswith("-SWAP"):
        text = text[:-5]
    text = text.replace("-", "").replace("_", "").replace("/", "")
    if text.endswith(("USDT", "USDC", "USD")) and any(ch.isalpha() for ch in text):
        return text
    return ""


def _symbol_from_event_identity(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    normalized = text
    for sep in ("->", ":", "-", "/", "_"):
        normalized = normalized.replace(sep, " ")
    for token in normalized.split():
        symbol = _canonical_event_symbol(token)
        if symbol:
            return symbol
    return ""


def _append_unique_normalized(target: list[str], value: str) -> None:
    if value and value not in target:
        target.append(value)


def _canonical_event_venue(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text or text in {"none", "null"}:
        return ""
    return text


def _extend_exchange_truth_scope_from_payload(
    payload: dict[str, Any],
    symbols: list[str],
    venues: list[str],
) -> None:
    for key in ("symbol", "canonical_symbol", "venue_symbol"):
        _append_unique_normalized(symbols, _canonical_event_symbol(payload.get(key)))
    for key in ("symbols", "canonical_symbols"):
        values = payload.get(key)
        if isinstance(values, str):
            values = [values]
        if isinstance(values, list):
            for item in values:
                _append_unique_normalized(symbols, _canonical_event_symbol(item))
    for key in ("position_id", "entry_id", "pending_id", "owner_id", "recovery_key"):
        _append_unique_normalized(symbols, _symbol_from_event_identity(payload.get(key)))

    for key in (
        "venue",
        "long_venue",
        "short_venue",
        "maker_venue",
        "hedge_venue",
        "passive_venue",
        "taker_venue",
        "close_venue",
    ):
        _append_unique_normalized(venues, _canonical_event_venue(payload.get(key)))
    payload_venues = payload.get("venues")
    if isinstance(payload_venues, str):
        payload_venues = [payload_venues]
    if isinstance(payload_venues, dict):
        payload_venues = list(payload_venues.values())
    if isinstance(payload_venues, list):
        for item in payload_venues:
            _append_unique_normalized(venues, _canonical_event_venue(item))


def _exchange_truth_scope_from_events(
    events: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    symbols: list[str] = []
    venues: list[str] = []
    for rec in events:
        kind = str(rec.get("kind") or "")
        if not _event_kind_contributes_exchange_truth_scope(kind):
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        _extend_exchange_truth_scope_from_payload(payload, symbols, venues)
    return symbols, venues


def _annotate_exchange_truth_required_venues(
    exchange_truth: dict[str, Any],
    required_venues: list[str],
) -> dict[str, Any]:
    required = [venue for venue in required_venues if venue]
    available = {
        str(venue or "").strip().lower()
        for venue in exchange_truth.get("available_venues", []) or []
    }
    missing_required = [
        venue for venue in required
        if venue not in available
    ]
    annotated = dict(exchange_truth)
    annotated["required_venues"] = required
    annotated["missing_required_venues"] = missing_required
    if missing_required:
        missing_evidence = list(annotated.get("missing_evidence") or [])
        for venue in missing_required:
            key = f"exchange_truth_required_venue_missing_{venue}"
            if key not in missing_evidence:
                missing_evidence.append(key)
        annotated["missing_evidence"] = missing_evidence
        if str(annotated.get("confidence", "")).lower() == "high":
            annotated["confidence"] = "medium"
    return normalize_exchange_truth_payload(annotated)


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _hyperliquid_spot_balances(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    balances: list[dict[str, Any]] = []
    for item in raw.get("balances") or []:
        if not isinstance(item, dict):
            continue
        coin = str(item.get("coin", "") or "")
        total = _optional_float(item.get("total"))
        hold = _optional_float(item.get("hold"))
        entry_ntl = _optional_float(item.get("entryNtl"))
        if not coin:
            continue
        balances.append({
            "coin": coin,
            "total": total,
            "hold": hold,
            "entry_ntl": entry_ntl,
        })
    return balances


def _hyperliquid_balance_view_payload(
    *,
    account_address: str,
    perp_raw: Any,
    spot_raw: Any,
    user_abstraction: str = "",
) -> dict[str, Any]:
    perp = perp_raw if isinstance(perp_raw, dict) else {}
    spot_balances = _hyperliquid_spot_balances(spot_raw)
    usdc_total = sum(
        float(item.get("total") or 0.0)
        for item in spot_balances
        if str(item.get("coin", "") or "").upper() == "USDC"
    )
    usdc_available = sum(
        max(
            float(item.get("total") or 0.0) - float(item.get("hold") or 0.0),
            0.0,
        )
        for item in spot_balances
        if str(item.get("coin", "") or "").upper() == "USDC"
    )

    cross = perp.get("crossMarginSummary")
    margin = perp.get("marginSummary")
    if not isinstance(cross, dict):
        cross = {}
    if not isinstance(margin, dict):
        margin = {}
    account_value = (
        _optional_float(cross.get("accountValue"))
        if cross else None
    )
    if account_value is None:
        account_value = _optional_float(margin.get("accountValue"))
    total_margin_used = (
        _optional_float(cross.get("totalMarginUsed"))
        if cross else None
    )
    if total_margin_used is None:
        total_margin_used = _optional_float(margin.get("totalMarginUsed"))
    withdrawable = _optional_float(perp.get("withdrawable"))

    abstraction = str(user_abstraction or "")
    if (
        (withdrawable or 0.0) <= 1e-9
        and abstraction == "unifiedAccount"
        and usdc_available > 1e-9
    ):
        classification = "unified_collateral_available"
    elif (withdrawable or 0.0) <= 1e-9 and usdc_total > 1e-9:
        classification = "usdc_present_margin_view_zero"
    elif (withdrawable or 0.0) <= 1e-9:
        classification = "margin_view_zero"
    else:
        classification = "margin_view_available"

    return {
        "classification": classification,
        "user_abstraction": abstraction,
        "account_address_masked": _mask_address(account_address),
        "account_address_hash": _address_hash(account_address),
        "perp": {
            "withdrawable": withdrawable,
            "account_value": account_value,
            "total_margin_used": total_margin_used,
            "asset_position_count": len(perp.get("assetPositions") or []),
        },
        "spot": {
            "usdc_total": usdc_total,
            "usdc_available": usdc_available,
            "balances": spot_balances[:20],
        },
    }


async def _fetch_hyperliquid_balance_view(
    adapter: Any,
    credential: Any,
) -> dict[str, Any]:
    account_address = str(getattr(credential, "account_address", "") or "").strip()
    transport = getattr(adapter, "_transport", None)
    if not account_address:
        return {"classification": "account_address_unavailable"}
    if transport is None:
        return {"classification": "transport_unavailable"}
    try:
        agent_wallet_address = str(
            getattr(credential, "agent_wallet_address", "") or ""
        ).strip()
        perp_raw, _ = await request_venue_operation(
            transport,
            Venue.HYPERLIQUID,
            VenueOperation.POSITION,
            account_address=account_address,
            agent_wallet_address=agent_wallet_address,
        )
        user_abstraction = ""
        try:
            raw_abstraction, _ = await request_venue_operation(
                transport,
                Venue.HYPERLIQUID,
                VenueOperation.USER_ABSTRACTION,
                account_address=account_address,
                agent_wallet_address=agent_wallet_address,
            )
            user_abstraction = str(raw_abstraction or "")
        except Exception:
            user_abstraction = "unavailable"
        spot_raw, _ = await request_venue_operation(
            transport,
            Venue.HYPERLIQUID,
            VenueOperation.SPOT_CLEARINGHOUSE_STATE,
            account_address=account_address,
            agent_wallet_address=agent_wallet_address,
        )
        return _hyperliquid_balance_view_payload(
            account_address=account_address,
            perp_raw=perp_raw,
            spot_raw=spot_raw,
            user_abstraction=user_abstraction,
        )
    except Exception as exc:
        return {
            "classification": "balance_view_probe_failed",
            "account_address_masked": _mask_address(account_address),
            "account_address_hash": _address_hash(account_address),
            "error": str(exc)[:200],
        }


def _probe_venue_symbol(adapter: Any, symbol: str) -> str:
    transport = getattr(adapter, "_transport", None)
    convert = getattr(transport, "_venue_symbol", None)
    if callable(convert):
        try:
            return str(convert(symbol))
        except Exception:
            return symbol
    return symbol


def _venue_from_probe_text(venue: str) -> Venue | None:
    venue_lower = venue.lower()
    for item in Venue:
        if item.value in venue_lower:
            return item
    return None


def _unsupported_symbol_probe_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(
        marker in text
        for marker in (
            "invalid symbol",
            "symbol is invalid",
            "unknown symbol",
            "contract_metadata_missing_ct_val",
            "instrument id does not exist",
            "instrument_id does not exist",
            "instrument_missing",
            "instrument does not exist",
            "invalid instid",
            "instid does not exist",
            "symbol not found",
            "not listed",
            "-1121",
        )
    )


async def _private_symbol_eligibility_for_probe(adapter: Any, symbol: str):
    venue = _venue_from_probe_text(str(getattr(adapter, "venue", "")))
    if venue != Venue.ASTER:
        return None
    supported_provider = getattr(adapter, "supported_symbols", None)
    supported_symbols: list[str] = []
    if callable(supported_provider):
        try:
            supported_symbols = list(supported_provider() or [])
        except Exception:
            supported_symbols = []
    if not supported_symbols:
        ensure_catalog = getattr(adapter, "ensure_supported_symbols_loaded", None)
        if callable(ensure_catalog):
            try:
                result = ensure_catalog()
                if hasattr(result, "__await__"):
                    await result
                supported_symbols = (
                    list(supported_provider() or [])
                    if callable(supported_provider)
                    else []
                )
            except Exception:
                supported_symbols = []
    return venue_symbol_eligibility(
        venue,
        symbol,
        supported_symbols=supported_symbols,
        venue_symbol=_probe_venue_symbol(adapter, symbol),
    )


async def _fetch_venue_positions(
    adapter: Any, symbols: list[str],
) -> tuple[dict[str, Any], set[str], set[str], dict[str, Any]]:
    """Fetch positions. Returns positions, succeeded/failed symbols, evidence."""
    positions: dict[str, Any] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
    evidence: dict[str, Any] = {}
    if not symbols:
        fetch_all = getattr(adapter, "fetch_all_positions", None)
        if not callable(fetch_all):
            transport = getattr(adapter, "_transport", None)
            fetch_all = getattr(transport, "fetch_all_positions", None)
        if not callable(fetch_all):
            failed.add(UNFILTERED_PROBE_KEY)
            evidence[UNFILTERED_PROBE_KEY] = {
                "classification": "position_probe_unfiltered_failed",
                "error": "fetch_all_positions_unavailable",
            }
            return positions, succeeded, failed, evidence
        try:
            all_positions = await fetch_all()
            items = all_positions if isinstance(all_positions, (list, tuple)) else []
            for pos in items:
                qty = float(getattr(pos, "quantity", 0) or 0)
                if abs(qty) <= 1e-9:
                    continue
                sym = str(getattr(pos, "symbol", "") or "")
                if not sym:
                    sym = UNFILTERED_PROBE_KEY
                positions[sym] = {
                    "symbol": sym,
                    "quantity": qty,
                    "entry_price": float(getattr(pos, "entry_price", 0) or 0),
                    "side": str(getattr(pos, "side", "")),
                }
            succeeded.add(UNFILTERED_PROBE_KEY)
            evidence[UNFILTERED_PROBE_KEY] = {
                "classification": "position_probe_unfiltered_succeeded",
                "position_count": len(positions),
            }
        except Exception as exc:
            failed.add(UNFILTERED_PROBE_KEY)
            evidence[UNFILTERED_PROBE_KEY] = {
                "classification": "position_probe_unfiltered_failed",
                "error": str(exc)[:200],
            }
        return positions, succeeded, failed, evidence

    for sym in symbols:
        eligibility = await _private_symbol_eligibility_for_probe(adapter, sym)
        if eligibility is not None and eligibility.unsupported_before_http:
            succeeded.add(sym)
            evidence[sym] = {
                "classification": "unsupported_symbol_flat",
                "venue_symbol": eligibility.venue_symbol,
                "reason": PRIVATE_TRUTH_UNSUPPORTED_REASON,
                "catalog_loaded": eligibility.catalog_loaded,
                "supported_symbol_count": eligibility.supported_symbol_count,
            }
            continue
        try:
            pos = await adapter.fetch_position(sym)
            qty = float(getattr(pos, "quantity", 0) or 0)
            if abs(qty) > 1e-9:
                positions[sym] = {
                    "symbol": sym,
                    "quantity": qty,
                    "entry_price": float(getattr(pos, "entry_price", 0) or 0),
                    "side": str(getattr(pos, "side", "")),
                }
            succeeded.add(sym)
            evidence[sym] = {
                "classification": "position_probe_succeeded",
                "venue_symbol": _probe_venue_symbol(adapter, sym),
            }
        except Exception as exc:
            if _unsupported_symbol_probe_error(exc):
                succeeded.add(sym)
                evidence[sym] = {
                    "classification": "unsupported_symbol_flat",
                    "venue_symbol": _probe_venue_symbol(adapter, sym),
                    "error": str(exc)[:200],
                }
                continue
            positions[sym] = {"symbol": sym, "error": str(exc)[:200]}
            failed.add(sym)
            evidence[sym] = {
                "classification": "position_probe_failed",
                "venue_symbol": _probe_venue_symbol(adapter, sym),
                "error": str(exc)[:200],
            }
    return positions, succeeded, failed, evidence


def _extract_order_rows(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return []
    result = raw.get("result")
    if isinstance(result, dict) and isinstance(result.get("list"), list):
        return result["list"]
    data = raw.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("entrustedList", "orderList", "list", "orders"):
            rows = data.get(key)
            if isinstance(rows, list):
                return rows
    for key in ("list", "orders", "openOrders"):
        rows = raw.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _float_order_field(order: dict[str, Any], *keys: str) -> float:
    for key in keys:
        if key in order and order.get(key) not in (None, ""):
            try:
                return float(order.get(key) or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _bool_order_field(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").lower() in {"true", "1", "yes"}


def _summarize_open_order(order: dict[str, Any], fallback_symbol: str = "") -> dict[str, Any]:
    symbol = str(
        order.get("symbol")
        or order.get("instId")
        or order.get("contract")
        or order.get("coin")
        or fallback_symbol
        or ""
    )
    return {
        "order_id": str(
            order.get("orderId")
            or order.get("order_id")
            or order.get("ordId")
            or order.get("id")
            or order.get("orderLinkId")
            or order.get("clientOid")
            or order.get("clOrdId")
            or ""
        ),
        "symbol": symbol,
        "side": str(order.get("side", "")),
        "quantity": _float_order_field(order, "origQty", "qty", "size", "sz", "left", "amount"),
        "price": _float_order_field(order, "price", "px"),
        "reduce_only": _bool_order_field(order.get("reduceOnly", order.get("reduce_only"))),
    }


async def _fetch_unfiltered_open_orders(
    adapter: Any,
    transport: Any,
    venue: str,
) -> list[dict[str, Any]]:
    contract_venue = _venue_from_probe_text(venue)
    if contract_venue == Venue.ASTER:
        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        if callable(fetch_open_orders):
            rows = await fetch_open_orders(None)
            return [
                _summarize_open_order(row)
                for row in rows[:50]
                if isinstance(row, dict)
            ]
    if contract_venue is not None:
        credential = getattr(transport, "_credential", None)
        account = str(getattr(credential, "account_address", "") or "")
        agent_wallet = str(getattr(credential, "agent_wallet_address", "") or "")
        if contract_venue == Venue.BYBIT:
            request = build_venue_operation_request(
                contract_venue,
                VenueOperation.OPEN_ORDERS,
                account_address=account,
                agent_wallet_address=agent_wallet,
            )
            params = dict(request.params)
            params["limit"] = 50
            rows: list[Any] = []
            seen_cursors: set[str] = set()
            for _page in range(100):
                raw = await transport._request(
                    request.method,
                    request.path,
                    params=params,
                    private=request.private,
                )
                if not isinstance(raw, dict) or int(raw.get("retCode", 0) or 0) != 0:
                    raise RuntimeError("Bybit open-order pagination response invalid")
                rows.extend(_extract_order_rows(raw))
                result = raw.get("result")
                cursor = (
                    str(result.get("nextPageCursor") or "")
                    if isinstance(result, dict)
                    else ""
                )
                if not cursor:
                    return [
                        _summarize_open_order(row)
                        for row in rows
                        if isinstance(row, dict)
                    ]
                if cursor in seen_cursors:
                    raise RuntimeError("Bybit open-order pagination cursor loop")
                seen_cursors.add(cursor)
                params["cursor"] = cursor
            raise RuntimeError("Bybit open-order pagination page cap reached")
        raw, _ = await request_venue_operation(
            transport,
            contract_venue,
            VenueOperation.OPEN_ORDERS,
            account_address=account,
            agent_wallet_address=agent_wallet,
        )
    else:
        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        if callable(fetch_open_orders):
            rows = await fetch_open_orders(UNFILTERED_PROBE_KEY)
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []
    return [
        _summarize_open_order(row)
        for row in _extract_order_rows(raw)[:50]
        if isinstance(row, dict)
    ]


async def _fetch_venue_open_orders(
    adapter: Any, symbols: list[str],
) -> tuple[dict[str, Any], set[str], set[str], dict[str, Any]]:
    """Fetch open orders. Returns orders, succeeded/failed symbols, evidence."""
    orders: dict[str, Any] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
    evidence: dict[str, Any] = {}
    transport = getattr(adapter, "_transport", None)
    if transport is None:
        for sym in symbols:
            orders[sym] = {"error": "no transport available"}
            failed.add(sym)
            evidence[sym] = {
                "classification": "open_order_probe_failed",
                "venue_symbol": sym,
                "error": "no transport available",
            }
        return orders, succeeded, failed, evidence

    venue = str(getattr(adapter, "venue", ""))
    if not symbols:
        try:
            order_items = await _fetch_unfiltered_open_orders(adapter, transport, venue)
            orders[UNFILTERED_PROBE_KEY] = order_items
            succeeded.add(UNFILTERED_PROBE_KEY)
            evidence[UNFILTERED_PROBE_KEY] = {
                "classification": "open_order_probe_unfiltered_succeeded",
                "order_count": len(order_items),
            }
        except Exception as exc:
            orders[UNFILTERED_PROBE_KEY] = {"error": str(exc)[:200]}
            failed.add(UNFILTERED_PROBE_KEY)
            evidence[UNFILTERED_PROBE_KEY] = {
                "classification": "open_order_probe_unfiltered_failed",
                "error": str(exc)[:200],
            }
        return orders, succeeded, failed, evidence

    for sym in symbols:
        venue_symbol = _probe_venue_symbol(adapter, sym)
        eligibility = await _private_symbol_eligibility_for_probe(adapter, sym)
        if eligibility is not None and eligibility.unsupported_before_http:
            orders[sym] = []
            succeeded.add(sym)
            evidence[sym] = {
                "classification": "unsupported_symbol_no_open_orders",
                "venue_symbol": eligibility.venue_symbol,
                "reason": PRIVATE_TRUTH_UNSUPPORTED_REASON,
                "catalog_loaded": eligibility.catalog_loaded,
                "supported_symbol_count": eligibility.supported_symbol_count,
            }
            continue
        try:
            contract_venue = _venue_from_probe_text(venue)
            if contract_venue == Venue.ASTER:
                fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
                if callable(fetch_open_orders):
                    raw = await fetch_open_orders(venue_symbol)
                else:
                    credential = getattr(transport, "_credential", None)
                    account = str(getattr(credential, "account_address", "") or "")
                    agent_wallet = str(
                        getattr(credential, "agent_wallet_address", "") or ""
                    )
                    raw, _ = await request_venue_operation(
                        transport,
                        contract_venue,
                        VenueOperation.OPEN_ORDERS,
                        symbol=sym,
                        account_address=account,
                        agent_wallet_address=agent_wallet,
                    )
            elif contract_venue is not None:
                credential = getattr(transport, "_credential", None)
                account = str(getattr(credential, "account_address", "") or "")
                agent_wallet = str(getattr(credential, "agent_wallet_address", "") or "")
                raw, _ = await request_venue_operation(
                    transport,
                    contract_venue,
                    VenueOperation.OPEN_ORDERS,
                    symbol=sym,
                    account_address=account,
                    agent_wallet_address=agent_wallet,
                )
            else:
                succeeded.add(sym)
                orders[sym] = []
                evidence[sym] = {
                    "classification": "open_order_probe_unsupported_venue_assumed_empty",
                    "venue_symbol": venue_symbol,
                }
                continue

            order_list = _extract_order_rows(raw)
            if isinstance(order_list, list) and order_list:
                orders[sym] = [
                    _summarize_open_order(o, fallback_symbol=sym)
                    for o in order_list[:50]
                    if isinstance(o, dict)
                ]
            else:
                orders[sym] = []
            succeeded.add(sym)
            evidence[sym] = {
                "classification": "open_order_probe_succeeded",
                "venue_symbol": venue_symbol,
            }
        except Exception as exc:
            if _unsupported_symbol_probe_error(exc):
                orders[sym] = []
                succeeded.add(sym)
                evidence[sym] = {
                    "classification": "unsupported_symbol_no_open_orders",
                    "venue_symbol": venue_symbol,
                    "error": str(exc)[:200],
                }
                continue
            orders[sym] = {"error": str(exc)[:200]}
            failed.add(sym)
            evidence[sym] = {
                "classification": "open_order_probe_failed",
                "venue_symbol": venue_symbol,
                "error": str(exc)[:200],
            }
    return orders, succeeded, failed, evidence


async def _build_exchange_truth_async(
    runtime_dir: str, symbols: list[str],
    venues: list[str] | None = None,
) -> dict[str, Any]:
    previous_rate_limit_runtime = _install_readonly_exchange_truth_rate_limit_runtime()
    readonly_rate_limiter = _create_readonly_rate_limiter()
    try:
        return await _build_exchange_truth_async_inner(
            runtime_dir,
            symbols,
            venues,
            readonly_rate_limiter=readonly_rate_limiter,
        )
    finally:
        _restore_readonly_exchange_truth_rate_limit_runtime(previous_rate_limit_runtime)


async def _build_exchange_truth_async_inner(
    runtime_dir: str, symbols: list[str],
    venues: list[str] | None = None,
    *,
    readonly_rate_limiter: Any = None,
) -> dict[str, Any]:
    errors: list[str] = []
    all_positions: dict[str, dict[str, Any]] = {}
    all_open_orders: dict[str, dict[str, Any]] = {}
    all_position_probe_evidence: dict[str, dict[str, Any]] = {}
    all_open_order_probe_evidence: dict[str, dict[str, Any]] = {}
    credential_identity: dict[str, dict[str, Any]] = {}
    balance_views: dict[str, dict[str, Any]] = {}
    available_venues: list[str] = []
    fetch_status: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    target_symbols = symbols if symbols else []

    target_venues = venues or DEFAULT_EXCHANGE_TRUTH_VENUES
    for venue in target_venues:
        credential = _load_venue_credential(venue)
        if credential is None:
            all_positions[venue] = {"error": "no credentials available"}
            all_open_orders[venue] = {"error": "no credentials available"}
            all_position_probe_evidence[venue] = {}
            all_open_order_probe_evidence[venue] = {}
            if venue.lower() == "hyperliquid":
                credential_identity[venue] = {
                    "wallet_mode": "",
                    "account_address_present": False,
                    "signer_address_present": False,
                    "account_matches_signer": False,
                }
            fetch_status[venue] = {
                "status": "no_credentials",
                "positions_succeeded": [],
                "positions_failed": [],
                "orders_succeeded": [],
                "orders_failed": [],
            }
            missing.append("{}_credentials".format(venue))
            continue

        if venue.lower() == "hyperliquid":
            credential_identity[venue] = _hyperliquid_credential_identity(credential)

        adapter = _create_readonly_adapter(
            venue,
            credential,
            rate_limiter=readonly_rate_limiter,
        )
        if adapter is None:
            errors.append("failed to create {} adapter".format(venue))
            all_position_probe_evidence[venue] = {}
            all_open_order_probe_evidence[venue] = {}
            fetch_status[venue] = {
                "status": "adapter_creation_failed",
                "positions_succeeded": [],
                "positions_failed": list(target_symbols),
                "orders_succeeded": [],
                "orders_failed": list(target_symbols),
            }
            missing.append("{}_adapter".format(venue))
            continue

        # Fetch positions
        pos_succeeded: set[str] = set()
        pos_failed: set[str] = set()
        pos_evidence: dict[str, Any] = {}
        try:
            positions, pos_succeeded, pos_failed, pos_evidence = await _fetch_venue_positions(
                adapter, target_symbols,
            )
            all_positions[venue] = positions
        except Exception as exc:
            all_positions[venue] = {"error": str(exc)[:300]}
            pos_failed = set(target_symbols)
            pos_evidence = {}
        all_position_probe_evidence[venue] = pos_evidence

        # Fetch open orders
        ord_succeeded: set[str] = set()
        ord_failed: set[str] = set()
        ord_evidence: dict[str, Any] = {}
        try:
            orders, ord_succeeded, ord_failed, ord_evidence = await _fetch_venue_open_orders(
                adapter, target_symbols,
            )
            all_open_orders[venue] = orders
        except Exception as exc:
            all_open_orders[venue] = {"error": str(exc)[:300]}
            ord_failed = set(target_symbols)
            ord_evidence = {}
        all_open_order_probe_evidence[venue] = ord_evidence

        if venue.lower() == "hyperliquid":
            balance_views[venue] = await _fetch_hyperliquid_balance_view(
                adapter,
                credential,
            )

        try:
            await adapter.shutdown()
        except Exception:
            pass

        # Only count venue as available if at least one position OR order query succeeded
        any_success = bool(pos_succeeded) or bool(ord_succeeded)
        any_failure = bool(pos_failed) or bool(ord_failed)
        private_filtered_symbols = sorted(
            {
                sym
                for evidence_items in (pos_evidence, ord_evidence)
                for sym, item in evidence_items.items()
                if item.get("reason") == PRIVATE_TRUTH_UNSUPPORTED_REASON
            }
        )

        fetch_status[venue] = {
            "status": "partial" if (any_success and any_failure) else (
                "ok" if any_success else "all_failed"
            ),
            "positions_succeeded": sorted(pos_succeeded),
            "positions_failed": sorted(pos_failed),
            "orders_succeeded": sorted(ord_succeeded),
            "orders_failed": sorted(ord_failed),
            "positions_unsupported_symbols": sorted(
                sym for sym, item in pos_evidence.items()
                if item.get("classification") == "unsupported_symbol_flat"
            ),
            "orders_unsupported_symbols": sorted(
                sym for sym, item in ord_evidence.items()
                if item.get("classification") == "unsupported_symbol_no_open_orders"
            ),
            "private_truth_pre_http_filtered_symbols": private_filtered_symbols,
        }

        if any_success:
            available_venues.append(venue)

        if pos_failed:
            for sym in sorted(pos_failed):
                missing.append("{}_position_fetch_failed_{}".format(venue, sym))
        if ord_failed:
            for sym in sorted(ord_failed):
                missing.append("{}_open_order_fetch_failed_{}".format(venue, sym))

    # has_nonzero_position: True ONLY if at least one SUCCESSFULLY fetched symbol has qty > 0
    has_any_position = any(
        isinstance(v, dict) and "quantity" in v
        for vp in all_positions.values() if isinstance(vp, dict)
        for v in vp.values() if isinstance(v, dict)
    )
    has_any_open_order = any(
        isinstance(v, list) and len(v) > 0
        for vo in all_open_orders.values() if isinstance(vo, dict)
        for v in vo.values() if isinstance(v, list)
    )

    # Confidence: high only if we successfully queried at least one venue and all queries succeeded
    all_ok = bool(fetch_status) and all(
        fs.get("status") == "ok"
        for fs in fetch_status.values()
    )
    any_available = len(available_venues) > 0

    if not any_available:
        confidence = "low"
    elif not all_ok:
        confidence = "medium"
    else:
        confidence = "high"

    return normalize_exchange_truth_payload({
        "available": any_available,
        "available_venues": available_venues,
        "confidence": confidence,
        "positions": all_positions,
        "open_orders": all_open_orders,
        "credential_identity": credential_identity,
        "balance_views": balance_views,
        "position_probe_evidence": all_position_probe_evidence,
        "open_order_probe_evidence": all_open_order_probe_evidence,
        "has_nonzero_position": has_any_position,
        "has_open_order": has_any_open_order,
        "fetch_status": fetch_status,
        "private_truth_pre_http_filtered_count": sum(
            len(fs.get("private_truth_pre_http_filtered_symbols") or [])
            for fs in fetch_status.values()
            if isinstance(fs, dict)
        ),
        "errors": errors,
        "missing_evidence": missing,
    })


def _build_exchange_truth(
    runtime_dir: str, symbols: list[str],
    venues: list[str] | None = None,
) -> dict[str, Any]:
    import asyncio
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            return asyncio.run(_build_exchange_truth_async(runtime_dir, symbols, venues))
        except Exception as exc:
            return normalize_exchange_truth_payload({
                "available": False,
                "confidence": "low",
                "positions": {},
                "open_orders": {},
                "errors": [str(exc)[:500]],
                "missing_evidence": ["exchange_truth_fetch_failed"],
            })
    except Exception as exc:
        return normalize_exchange_truth_payload({
            "available": False,
            "confidence": "low",
            "positions": {},
            "open_orders": {},
            "errors": [str(exc)[:500]],
            "missing_evidence": ["exchange_truth_fetch_failed"],
        })
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run,
                _build_exchange_truth_async(runtime_dir, symbols, venues),
            )
            return future.result(timeout=30)
    except Exception as exc:
        return normalize_exchange_truth_payload({
            "available": False,
            "confidence": "low",
            "positions": {},
            "open_orders": {},
            "errors": [str(exc)[:500]],
            "missing_evidence": ["exchange_truth_fetch_failed"],
        })


# ---------------------------------------------------------------------------
# state consistency
# ---------------------------------------------------------------------------

def _safe_abs_quantity(value: Any) -> float:
    try:
        return abs(float(value or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _live_position_details(exchange_truth: dict[str, Any]) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for venue, positions in (exchange_truth.get("positions") or {}).items():
        if not isinstance(positions, dict):
            continue
        for symbol, pos in positions.items():
            if not isinstance(pos, dict):
                continue
            qty = _safe_abs_quantity(pos.get("quantity"))
            if qty <= 1e-9:
                continue
            live.append({
                "venue": str(pos.get("venue") or venue).lower(),
                "symbol": str(pos.get("symbol") or symbol).upper(),
                "side": str(pos.get("side") or "").lower(),
                "quantity": qty,
                "entry_price": pos.get("entry_price"),
            })
    return live


def _local_open_position_rows(local_state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = local_state.get("open_positions")
    if not rows:
        rows = local_state.get("positions")
    if isinstance(rows, dict):
        rows = list(rows.values())
    return [row for row in (rows or []) if isinstance(row, dict)]


def _active_owner_managed_scope(local_state: dict[str, Any]) -> dict[str, set[str]]:
    position_ids: set[str] = set()
    symbols: set[str] = set()
    venues: set[str] = set()
    for row in _local_open_position_rows(local_state):
        symbol = str(row.get("symbol") or "").upper()
        position_id = str(
            row.get("position_id")
            or row.get("internal_entry_id")
            or row.get("entry_id")
            or ""
        )
        if position_id.startswith("live-recovered:"):
            continue
        matched_qty = (
            _safe_abs_quantity(row.get("matched_quantity"))
            if "matched_quantity" in row
            else 0.0
        )
        quantity = _safe_abs_quantity(row.get("quantity"))
        qty = matched_qty if matched_qty > 1e-9 else quantity
        if qty <= 1e-9:
            continue
        if position_id:
            position_ids.add(position_id)
        if symbol:
            symbols.add(symbol)
        for venue_key in ("long_venue", "short_venue", "venue"):
            venue = str(row.get(venue_key) or "").lower()
            if venue:
                venues.add(venue)
    return {"position_ids": position_ids, "symbols": symbols, "venues": venues}


def _position_side_from_truth(row: dict[str, Any], quantity: float) -> str:
    side = str(row.get("side") or row.get("position_side") or "").lower()
    if side in {"long", "buy"}:
        return "long"
    if side in {"short", "sell"}:
        return "short"
    if quantity > 1e-9:
        return "long"
    if quantity < -1e-9:
        return "short"
    return ""


def _exchange_truth_has_balanced_symbol(
    exchange_truth: dict[str, Any],
    symbol: str,
) -> bool:
    target = str(symbol or "").upper()
    if not target or not exchange_truth.get("available"):
        return False
    sides: set[str] = set()
    quantities: list[float] = []
    for positions in (exchange_truth.get("positions") or {}).values():
        if not isinstance(positions, dict):
            continue
        for raw_symbol, row in positions.items():
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get("symbol") or raw_symbol or "").upper()
            if row_symbol != target:
                continue
            qty = _safe_float(row.get("quantity"))
            abs_qty = abs(qty)
            if abs_qty <= 1e-9:
                continue
            side = _position_side_from_truth(row, qty)
            if side:
                sides.add(side)
            quantities.append(abs_qty)
    if not {"long", "short"}.issubset(sides) or len(quantities) < 2:
        return False
    return max(quantities) - min(quantities) <= max(max(quantities) * 1e-9, 1e-9)


def _event_scoped_to_active_owner(
    payload: dict[str, Any],
    *,
    scope: dict[str, set[str]],
    exchange_truth: dict[str, Any],
) -> bool:
    position_id = str(
        payload.get("position_id")
        or payload.get("entry_id")
        or payload.get("internal_entry_id")
        or ""
    )
    symbol = str(payload.get("symbol") or "").upper()
    if position_id:
        scoped = position_id in scope["position_ids"]
    else:
        scoped = bool(symbol and symbol in scope["symbols"])
    return scoped and _exchange_truth_has_balanced_symbol(exchange_truth, symbol)


def _completed_residuals_scoped_to_active_owner(
    events: list[dict[str, Any]],
    *,
    scope: dict[str, set[str]],
    exchange_truth: dict[str, Any],
) -> bool:
    saw_residual = False
    completed_scopes: set[tuple[str, str]] = set()
    residual_scopes: set[tuple[str, str]] = set()
    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = _payload_dict(rec)
        reason = str(payload.get("reason") or "")
        if "residual" not in kind and "residual" not in reason:
            continue
        saw_residual = True
        if not _event_scoped_to_active_owner(
            payload,
            scope=scope,
            exchange_truth=exchange_truth,
        ):
            return False
        key = (
            str(
                payload.get("position_id")
                or payload.get("entry_id")
                or payload.get("internal_entry_id")
                or ""
            ),
            str(payload.get("symbol") or "").upper(),
        )
        residual_scopes.add(key)
        if _is_residual_completion(kind, payload):
            completed_scopes.add(key)
    return saw_residual and residual_scopes.issubset(completed_scopes)


def _overhedge_corrections_scoped_to_active_owner(
    events: list[dict[str, Any]],
    *,
    scope: dict[str, set[str]],
    exchange_truth: dict[str, Any],
) -> bool:
    saw_correction = False
    for rec in events:
        if str(rec.get("kind") or "") != "runtime.position_drift_corrected":
            continue
        payload = _payload_dict(rec)
        saw_correction = True
        if not _event_scoped_to_active_owner(
            payload,
            scope=scope,
            exchange_truth=exchange_truth,
        ):
            return False
    return saw_correction


def _position_event_key(payload: dict[str, Any]) -> tuple[str, str]:
    return (
        str(
            payload.get("position_id")
            or payload.get("entry_id")
            or payload.get("internal_entry_id")
            or ""
        ),
        str(payload.get("symbol") or "").upper(),
    )


def _terminal_overhedge_corrections_closed(events: list[dict[str, Any]]) -> bool:
    drift_keys: set[tuple[str, str]] = set()
    terminal_keys: set[tuple[str, str]] = set()
    flat_keys: set[tuple[str, str]] = set()
    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = _payload_dict(rec)
        key = _position_event_key(payload)
        if not key[0] and not key[1]:
            continue
        if kind == "runtime.position_drift_corrected":
            drift_keys.add(key)
        elif kind in {
            "runtime.position_lifecycle_terminal",
            "exit.passive_close_resolved",
            "exit.passive_close_fallback_terminal_flat",
            "exit.passive_close_recovery_probe_flat",
        }:
            terminal_keys.add(key)
        elif kind == "recovery.flat":
            flat_keys.add(key)
    return bool(drift_keys) and drift_keys.issubset(terminal_keys & flat_keys)


def _local_expected_legs(local_state: dict[str, Any]) -> list[dict[str, Any]]:
    legs: list[dict[str, Any]] = []
    for pos in local_state.get("positions", []) or []:
        if not isinstance(pos, dict):
            continue
        symbol = str(pos.get("symbol") or "").upper()
        qty = _safe_abs_quantity(pos.get("quantity") or pos.get("matched_quantity"))
        if not symbol or qty <= 1e-9:
            continue
        long_venue = str(pos.get("long_venue") or "").lower()
        short_venue = str(pos.get("short_venue") or "").lower()
        if long_venue:
            legs.append({
                "venue": long_venue,
                "symbol": symbol,
                "expected_side": "long",
                "expected_quantity": qty,
                "position_id": pos.get("position_id"),
            })
        if short_venue:
            legs.append({
                "venue": short_venue,
                "symbol": symbol,
                "expected_side": "short",
                "expected_quantity": qty,
                "position_id": pos.get("position_id"),
            })
    return legs


def _side_matches(actual: str, expected: str) -> bool:
    return side_matches_business_leg(actual, expected)


def _exchange_truth_position_mismatches(
    local_state: dict[str, Any], exchange_truth: dict[str, Any]
) -> list[dict[str, Any]]:
    live_positions = _live_position_details(exchange_truth)
    live_by_key = {
        (p["venue"], p["symbol"]): p
        for p in live_positions
    }
    expected_legs = _local_expected_legs(local_state)
    expected_keys = {(leg["venue"], leg["symbol"]) for leg in expected_legs}
    mismatches: list[dict[str, Any]] = []

    for leg in expected_legs:
        key = (leg["venue"], leg["symbol"])
        live = live_by_key.get(key)
        live_qty = float(live.get("quantity", 0.0)) if live else 0.0
        live_side = str(live.get("side", "")) if live else ""
        expected_qty = float(leg["expected_quantity"])
        if (
            abs(live_qty - expected_qty) > 1e-9
            or not _side_matches(live_side, str(leg["expected_side"]))
        ):
            mismatches.append({
                "check": "local_live_leg_missing_or_quantity_mismatch",
                "ok": False,
                **leg,
                "live_quantity": live_qty,
                "live_side": live_side,
            })

    if expected_legs:
        for live in live_positions:
            key = (live["venue"], live["symbol"])
            if key not in expected_keys:
                mismatches.append({
                    "check": "unexpected_live_position",
                    "ok": False,
                    **live,
                })

    return mismatches


def _runtime_progress_from_state(local_state: dict[str, Any]) -> dict[str, Any]:
    runtime_progress = local_state.get("runtime_progress")
    if isinstance(runtime_progress, dict):
        return dict(runtime_progress)
    return {}


def _runtime_market_data_config_from_state(
    local_state: dict[str, Any],
) -> dict[str, Any]:
    config = local_state.get("runtime_market_data_config")
    if isinstance(config, dict):
        return dict(config)
    return {}


def _build_state_consistency(
    local_state: dict[str, Any], exchange_truth: dict[str, Any]
) -> dict[str, Any]:
    """Compare local state against exchange truth.

    CRITICAL: Only declares local_open_exchange_flat=true when exchange truth
    confidence is "high" (all symbol queries succeeded). When queries partially
    or fully failed, the mismatch is reported as "unknown" (cannot verify), not
    as "flat" (confirmed no position).
    """
    details: list[dict[str, Any]] = []
    local_open = local_state.get("open_position_count", 0)
    et_available = exchange_truth.get("available", False)
    et_confidence = exchange_truth.get("confidence", "low")
    fetch_status = exchange_truth.get("fetch_status", {})
    runtime_progress = _runtime_progress_from_state(local_state)
    runtime_market_data_config = _runtime_market_data_config_from_state(local_state)

    # Collect local symbols for cross-reference
    local_symbols = [
        p.get("symbol", "")
        for p in local_state.get("positions", [])
        if isinstance(p, dict)
    ]

    # Build list of symbols where position fetch failed
    position_fetch_failed: list[str] = []
    for venue, fs in fetch_status.items():
        if isinstance(fs, dict):
            for sym in fs.get("positions_failed", []) or []:
                if sym not in position_fetch_failed:
                    position_fetch_failed.append(sym)

    if not et_available:
        details.append({
            "check": "exchange_truth_available",
            "ok": False,
            "detail": (
                "exchange truth not available — cannot verify consistency"
            ),
            "evidence_source": "exchange_truth.available=false",
        })
        return {
            "state_mismatch": False,
            "local_open_exchange_flat": False,
            "state_verdict": "unknown",
            "details": details,
            "confidence": "low",
            "missing_evidence": exchange_truth.get("missing_evidence", []),
            "runtime_progress": runtime_progress,
        }

    # Determine which local symbols were successfully checked on exchange
    local_with_failed_fetch = [
        s for s in local_symbols if s in position_fetch_failed
    ]
    local_with_success_fetch = [
        s for s in local_symbols if s not in position_fetch_failed
    ]

    if et_confidence not in ("high",):
        # Partial or full fetch failure — cannot reliably declare flat
        details.append({
            "check": "exchange_truth_confidence",
            "ok": False,
            "detail": (
                "exchange truth confidence={} — cannot confirm positions for all symbols. "
                "fetch_failed_symbols={}".format(
                    et_confidence, position_fetch_failed,
                )
            ),
            "evidence_source": "exchange_truth.confidence",
        })
        if local_symbols and local_with_failed_fetch:
            details.append({
                "check": "local_open_unverified",
                "ok": False,
                "detail": (
                    "local has {} open position(s) ({}) but exchange fetch failed for: {}".format(
                        local_open, ", ".join(local_symbols[:5]),
                        ", ".join(local_with_failed_fetch[:5]),
                    )
                ),
                "evidence_source": "exchange_truth.fetch_status",
            })
        return {
            "state_mismatch": False,
            "local_open_exchange_flat": False,
            "state_verdict": "unknown",
            "details": details,
            "confidence": "low",
            "missing_evidence": [
                "exchange_truth_fetch_partial_or_failed_for_{}".format(s)
                for s in local_with_failed_fetch[:5]
            ],
            "runtime_progress": runtime_progress,
        }

    # Confidence high: all queries succeeded — we can reliably compare
    exchange_has_positions = exchange_truth.get("has_nonzero_position", False)
    local_has_positions = local_open > 0
    leg_mismatches = _exchange_truth_position_mismatches(local_state, exchange_truth)
    state_mismatch = (local_has_positions != exchange_has_positions) or bool(leg_mismatches)
    local_open_exchange_flat = local_has_positions and not exchange_has_positions

    if local_open_exchange_flat:
        details.append({
            "check": "local_open_exchange_flat",
            "ok": False,
            "detail": (
                "CONFIRMED: local has {} open position(s) ({}) but exchange reports "
                "no positions (all {} symbol(s) successfully queried)".format(
                    local_open, ", ".join(local_symbols[:5]),
                    len(local_with_success_fetch),
                )
            ),
            "evidence_source": "local_state.open_positions vs exchange_truth.positions (confidence=high)",
            "local_symbols": local_symbols,
        })
    if leg_mismatches:
        details.extend(leg_mismatches)
    if state_mismatch and not local_open_exchange_flat:
        live_positions = _live_position_details(exchange_truth)
        details.append({
            "check": "nonzero_live_position" if not local_has_positions else "state_mismatch",
            "ok": False,
            "detail": "local and exchange state diverge (exchange has positions not in local)",
            "evidence_source": "local_state vs exchange_truth",
            "live_positions": live_positions,
        })

    if not details:
        details.append({
            "check": "consistency",
            "ok": True,
            "detail": "local and exchange state consistent",
            "evidence_source": "local_state + exchange_truth (confidence=high)",
        })

    state_verdict = (
        "local_open_exchange_flat" if local_open_exchange_flat
        else "consistent" if not state_mismatch
        else "exchange_truth_mismatch" if exchange_has_positions and not local_has_positions
        else "exchange_truth_mismatch" if leg_mismatches
        else "state_mismatch"
    )
    fingerprints = []
    if state_mismatch:
        fingerprints.append("exchange_truth_mismatch")
    if exchange_has_positions and not local_has_positions:
        fingerprints.append("nonzero_live_position")
    if leg_mismatches:
        fingerprints.append("local_exchange_position_mismatch")
    if any(m.get("check") == "unexpected_live_position" for m in leg_mismatches):
        fingerprints.append("nonzero_live_position")

    return {
        "state_mismatch": state_mismatch,
        "local_open_exchange_flat": local_open_exchange_flat,
        "state_verdict": state_verdict,
        "fingerprints": fingerprints,
        "details": details,
        "confidence": "high",
        "runtime_progress": runtime_progress,
        "runtime_market_data_config": runtime_market_data_config,
    }


# ---------------------------------------------------------------------------
# order error evidence
# ---------------------------------------------------------------------------

_ORDER_TRUTH_GAP_REGISTERED_KINDS = frozenset({
    "exit.accepted_order_truth_gap_registered",
})
_ORDER_TRUTH_GAP_RESOLUTION_KINDS = frozenset({
    "exit.accepted_order_truth_gap_resolved",
    "exit.passive_close_hedge_confirmed_after_ack",
    "exit.passive_close_hedge_reconciled_after_error",
    "exit.passive_close_hedge_duplicate_client_order_reconciled",
    "exit.passive_close_fallback_terminal_flat",
    "exit.passive_close_recovery_probe_flat",
    "runtime.position_lifecycle_terminal",
    "recovery.flat",
})
_EXIT_ACCEPTED_ORDER_TRUTH_GAP_COMPLETE_STATUSES = frozenset({
    "filled",
    "live_flat",
    "live_flat_after_single_leg_cleanup",
})


def _truth_gap_identity_values(payload: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    nested_payloads: list[dict[str, Any]] = [payload]
    exchange_error = payload.get("exchange_error")
    if isinstance(exchange_error, dict):
        nested_payloads.append(exchange_error)
        extra = exchange_error.get("extra")
        if isinstance(extra, dict):
            nested_payloads.append(extra)
        request_context = exchange_error.get("request_context")
        if isinstance(request_context, dict):
            nested_payloads.append(request_context)
    request_context = payload.get("request_context")
    if isinstance(request_context, dict):
        nested_payloads.append(request_context)

    for key in (
        "position_id",
        "entry_id",
        "pending_id",
        "source_entry_id",
        "internal_entry_id",
        "accepted_order_id",
        "accepted_client_order_id",
        "order_id",
        "client_order_id",
    ):
        for nested in nested_payloads:
            value = nested.get(key)
            if value:
                if key in {
                    "accepted_order_id",
                    "accepted_client_order_id",
                    "order_id",
                    "client_order_id",
                }:
                    normalized_value = normalize_order_identity(value)
                    if not normalized_value:
                        continue
                else:
                    normalized_value = str(value)
                normalized = normalized_value.lower()
                values.add(f"{key}:{normalized}")
                if key in {"accepted_order_id", "order_id"}:
                    values.add(f"order_ref:{normalized}")
                elif key in {"accepted_client_order_id", "client_order_id"}:
                    values.add(f"client_ref:{normalized}")
    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        for nested in nested_payloads:
            symbol = str(nested.get("symbol") or "").upper()
            if symbol:
                break
    if symbol:
        values.add(f"symbol:{symbol}")
    venue = str(payload.get("venue") or payload.get("hedge_venue") or "").lower()
    if not venue:
        for nested in nested_payloads:
            venue = str(nested.get("venue") or nested.get("hedge_venue") or "").lower()
            if venue:
                break
    if venue and symbol:
        values.add(f"venue_symbol:{venue}:{symbol}")
    return values


def _strong_order_identity_values(payload: dict[str, Any]) -> set[str]:
    return _strong_truth_gap_identity_values(_truth_gap_identity_values(payload))


def _strong_truth_gap_identity_values(values: set[str]) -> set[str]:
    return {
        value
        for value in values
        if not value.startswith(("symbol:", "venue_symbol:"))
    }


def _identity_values_with_bare_refs(values: set[str]) -> set[str]:
    normalized = {str(value).lower() for value in values if value}
    for value in list(normalized):
        if ":" not in value:
            continue
        _prefix, raw = value.split(":", 1)
        if raw:
            normalized.add(raw)
    return normalized


def _truth_gap_identity_sets_match(
    registered_identities: set[str],
    resolution_identities: set[str],
) -> bool:
    return bool(
        _strong_truth_gap_identity_values(registered_identities)
        & _strong_truth_gap_identity_values(resolution_identities)
    )


def _exchange_error_dict(payload: dict[str, Any]) -> dict[str, Any]:
    exchange_error = payload.get("exchange_error")
    return exchange_error if isinstance(exchange_error, dict) else {}


def _payload_is_ack_only_order_truth_gap(payload: dict[str, Any]) -> bool:
    exchange_error = _exchange_error_dict(payload)
    extra = exchange_error.get("extra")
    if not isinstance(extra, dict):
        extra = {}
    missing_evidence = payload.get("missing_evidence") or exchange_error.get("missing_evidence") or []
    if not isinstance(missing_evidence, list):
        missing_evidence = [missing_evidence]
    reason = str(payload.get("reason") or payload.get("error") or "").lower()
    exchange_code = str(
        payload.get("exchange_code") or exchange_error.get("exchange_code") or ""
    )
    return (
        payload.get("accepted_order_truth_gap") is True
        or payload.get("truth_required_by") == "accepted_order_truth_gap"
        or extra.get("order_ack_only") is True
        or (
            exchange_code == "0"
            and any(str(item) == "fill_confirmation" for item in missing_evidence)
        )
        or ("accepted" in reason and "fill" in reason and "confirm" in reason)
    )


def _payload_is_duplicate_client_id(payload: dict[str, Any]) -> bool:
    exchange_error = _exchange_error_dict(payload)
    exchange_code = str(
        payload.get("exchange_code") or exchange_error.get("exchange_code") or ""
    )
    exchange_msg = str(
        payload.get("exchange_msg") or exchange_error.get("exchange_msg") or ""
    ).lower()
    reason = str(payload.get("reason") or payload.get("error") or "").lower()
    return (
        exchange_code == "110072"
        or exchange_code == "40786"
        or ("110072" in reason)
        or ("40786" in reason)
        or ("orderlinkedid" in exchange_msg and "duplicate" in exchange_msg)
        or ("orderlinkedid" in reason and "duplicate" in reason)
        or ("clientoid" in exchange_msg and "duplicate" in exchange_msg)
        or ("clientoid" in reason and "duplicate" in reason)
    )


def _payload_is_bybit_duplicate_client_id(payload: dict[str, Any]) -> bool:
    return _payload_is_duplicate_client_id(payload)


def _payload_is_bybit_terminal_zero_qty_reduce_only(payload: dict[str, Any]) -> bool:
    exchange_error = _exchange_error_dict(payload)
    request_context = _payload_request_context(payload)
    exchange_code = str(
        payload.get("exchange_code") or exchange_error.get("exchange_code") or ""
    )
    exchange_msg = str(
        payload.get("exchange_msg") or exchange_error.get("exchange_msg") or ""
    ).lower()
    reason = str(payload.get("reason") or payload.get("error") or "").lower()
    venue = str(
        payload.get("venue") or exchange_error.get("venue") or request_context.get("venue") or ""
    ).lower()
    raw_body = str(exchange_error.get("raw_body") or "").lower()
    text = " ".join((exchange_msg, reason, raw_body))
    return (
        venue == "bybit"
        and exchange_code == "110017"
        and _boolish(request_context.get("reduce_only"))
        and (
            "current position is zero" in text
            or "cannot fix reduce-only order qty" in text
            or "orderqty will be truncated to zero" in text
        )
    )


def _payload_is_terminal_zero_qty_truth_probe_retained(
    payload: dict[str, Any],
) -> bool:
    reason = str(payload.get("reason") or payload.get("error") or "").lower()
    decision = str(payload.get("decision") or "").lower()
    return (
        reason == "terminal_zero_qty_live_truth_not_flat"
        and decision == "retain_pending"
    )


def _truth_gap_resolution_complete(kind: str, payload: dict[str, Any]) -> bool:
    if kind == "exit.accepted_order_truth_gap_resolved":
        return (
            str(payload.get("resolution_status") or "").lower()
            in _EXIT_ACCEPTED_ORDER_TRUTH_GAP_COMPLETE_STATUSES
        )
    if kind in {
        "exit.passive_close_hedge_confirmed_after_ack",
        "exit.passive_close_hedge_reconciled_after_error",
        "exit.passive_close_hedge_duplicate_client_order_reconciled",
    }:
        if not _payload_order_truth_is_confirmed(payload):
            return False
        try:
            residual = float(payload.get("residual", 0) or 0)
        except (TypeError, ValueError):
            residual = 0.0
        return residual <= 1e-9
    if kind == "runtime.position_lifecycle_terminal":
        return str(payload.get("terminal_state", "") or "").lower() == "flat"
    if kind == "order.reconcile_result":
        try:
            live_qty = float(payload.get("live_qty") or 0.0)
        except (TypeError, ValueError):
            live_qty = 0.0
        return (
            str(payload.get("reason", "") or "").lower() == "duplicate_client_id"
            and str(payload.get("next_action", "") or "").lower() == "clear_live_flat"
            and abs(live_qty) <= 1e-9
        )
    return True


def _payload_order_truth_is_confirmed(payload: dict[str, Any]) -> bool:
    if (
        "order_truth_fill_status" not in payload
        and "order_truth_evidence_status" not in payload
    ):
        return True
    return (
        str(payload.get("order_truth_fill_status") or "").lower()
        == "confirmed_fill"
        and str(payload.get("order_truth_evidence_status") or "").lower()
        == "available"
        and payload.get("terminal_without_truth") is not True
    )


def _build_resolved_order_truth_gap_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    if not (_exchange_truth_flat(exchange_truth) and _exchange_truth_no_open_orders(exchange_truth)):
        return {
            "count": 0,
            "explicit_resolved_count": 0,
            "legacy_inferred_count": 0,
            "ledger_closed_legacy_inferred_count": 0,
            "ledger_terminal_flat_legacy_inferred_count": 0,
            "resolved_identities": [],
            "explicit_resolved_identities": [],
            "legacy_inferred_identities": [],
            "ledger_closed_legacy_inferred_identities": [],
            "ledger_terminal_flat_legacy_inferred_identities": [],
            "legacy_inferred_positions": [],
            "ledger_closed_legacy_inferred_positions": [],
            "ledger_terminal_flat_legacy_inferred_positions": [],
            "ledger_closed_legacy_project_statuses": {},
            "ledger_terminal_flat_legacy_project_statuses": {},
            "unresolved_count": 0,
            "unresolved_identities": [],
            "current_exchange_truth_clean": False,
        }

    registered: list[tuple[str, set[str]]] = []
    explicit_resolved_identity_sets: list[set[str]] = []
    legacy_inferred_identity_sets: list[set[str]] = []
    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol", "") or "").upper()
        if symbol and event_symbol and event_symbol != symbol.upper():
            continue
        identities = _truth_gap_identity_values(payload)
        if not identities:
            continue
        position_id = _position_event_key(payload)[0]
        if kind in _ORDER_TRUTH_GAP_REGISTERED_KINDS or (
            kind == "order.uncertain" and _payload_is_ack_only_order_truth_gap(payload)
        ):
            registered.append((position_id, identities))
        elif (
            (kind in _ORDER_TRUTH_GAP_RESOLUTION_KINDS or kind == "order.reconcile_result")
            and _truth_gap_resolution_complete(kind, payload)
        ):
            if kind == "exit.accepted_order_truth_gap_resolved":
                explicit_resolved_identity_sets.append(identities)
            else:
                legacy_inferred_identity_sets.append(identities)

    resolved: set[str] = set()
    explicit_resolved: set[str] = set()
    legacy_inferred: set[str] = set()
    ledger_closed_legacy: set[str] = set()
    ledger_terminal_flat_legacy: set[str] = set()
    unresolved: set[str] = set()
    legacy_positions: set[str] = set()
    ledger_closed_legacy_positions: set[str] = set()
    ledger_terminal_flat_legacy_positions: set[str] = set()
    ledger_closed_legacy_project_statuses: dict[str, str] = {}
    ledger_terminal_flat_legacy_project_statuses: dict[str, str] = {}
    matched_count = 0
    explicit_matched_count = 0
    legacy_matched_count = 0
    legacy_candidate_position_ids = {
        position_id for position_id, _identities in registered if position_id
    }
    ledger_positions = _exchange_truth_lifecycle_positions(
        events,
        legacy_candidate_position_ids,
    )
    for position_id, registered_identities in registered:
        matched_resolution = next(
            (
                identities for identities in explicit_resolved_identity_sets
                if _truth_gap_identity_sets_match(registered_identities, identities)
            ),
            None,
        )
        matched_kind = "explicit"
        if matched_resolution is None:
            matched_resolution = next(
                (
                    identities for identities in legacy_inferred_identity_sets
                    if _truth_gap_identity_sets_match(registered_identities, identities)
                ),
                None,
            )
            matched_kind = "legacy"
        if matched_resolution is None:
            unresolved.update(registered_identities)
            continue
        matched_count += 1
        resolved.update(registered_identities)
        resolved.update(matched_resolution)
        if matched_kind == "explicit":
            explicit_matched_count += 1
            explicit_resolved.update(registered_identities)
            explicit_resolved.update(matched_resolution)
        else:
            ledger_truth = ledger_positions.get(position_id) if position_id else None
            if _lifecycle_truth_exchange_complete(ledger_truth):
                ledger_closed_legacy.update(registered_identities)
                ledger_closed_legacy.update(matched_resolution)
                ledger_closed_legacy_positions.add(position_id)
                project_status = str(
                    ledger_truth.get("project_record_status") or ""
                )
                if project_status:
                    ledger_closed_legacy_project_statuses[position_id] = (
                        project_status
                    )
            elif _lifecycle_truth_terminal_flat_evidence_gap(ledger_truth):
                ledger_terminal_flat_legacy.update(registered_identities)
                ledger_terminal_flat_legacy.update(matched_resolution)
                ledger_terminal_flat_legacy_positions.add(position_id)
                project_status = str(
                    ledger_truth.get("project_record_status") or ""
                )
                if project_status:
                    ledger_terminal_flat_legacy_project_statuses[position_id] = (
                        project_status
                    )
            else:
                legacy_matched_count += 1
                legacy_inferred.update(registered_identities)
                legacy_inferred.update(matched_resolution)
                if position_id:
                    legacy_positions.add(position_id)

    return {
        "count": matched_count,
        "explicit_resolved_count": explicit_matched_count,
        "legacy_inferred_count": legacy_matched_count,
        "ledger_closed_legacy_inferred_count": len(ledger_closed_legacy_positions),
        "ledger_terminal_flat_legacy_inferred_count": len(
            ledger_terminal_flat_legacy_positions
        ),
        "resolved_identities": sorted(resolved),
        "explicit_resolved_identities": sorted(explicit_resolved),
        "legacy_inferred_identities": sorted(legacy_inferred),
        "ledger_closed_legacy_inferred_identities": sorted(ledger_closed_legacy),
        "ledger_terminal_flat_legacy_inferred_identities": sorted(
            ledger_terminal_flat_legacy
        ),
        "legacy_inferred_positions": sorted(legacy_positions),
        "ledger_closed_legacy_inferred_positions": sorted(
            ledger_closed_legacy_positions
        ),
        "ledger_terminal_flat_legacy_inferred_positions": sorted(
            ledger_terminal_flat_legacy_positions
        ),
        "ledger_closed_legacy_project_statuses": dict(
            sorted(ledger_closed_legacy_project_statuses.items())
        ),
        "ledger_terminal_flat_legacy_project_statuses": dict(
            sorted(ledger_terminal_flat_legacy_project_statuses.items())
        ),
        "unresolved_count": len(unresolved),
        "unresolved_identities": sorted(unresolved),
        "current_exchange_truth_clean": True,
    }


def _exchange_truth_lifecycle_positions(
    events: list[dict[str, Any]],
    position_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not position_ids:
        return {}
    try:
        report = build_exchange_truth_lifecycle(events, position_ids=position_ids)
    except Exception:
        return {}
    positions = report.get("positions")
    if not isinstance(positions, dict):
        return {}
    return {
        str(position_id): row
        for position_id, row in positions.items()
        if isinstance(row, dict)
    }


def _lifecycle_truth_exchange_complete(truth: dict[str, Any] | None) -> bool:
    if not isinstance(truth, dict):
        return False
    if str(truth.get("classification") or "") != "exchange_lifecycle_complete":
        return False
    close_coverage = truth.get("close_coverage")
    if not isinstance(close_coverage, dict):
        return False
    return all(
        isinstance(close_coverage.get(leg), dict)
        and close_coverage[leg].get("covered") is True
        for leg in ("long", "short")
    )


def _lifecycle_truth_terminal_flat_evidence_gap(
    truth: dict[str, Any] | None,
) -> bool:
    if not isinstance(truth, dict):
        return False
    if str(truth.get("classification") or "") not in {
        "evidence_incomplete",
        "exchange_lifecycle_incomplete",
    }:
        return False
    terminal_flat_truth = truth.get("terminal_flat_truth")
    if not isinstance(terminal_flat_truth, dict):
        return False
    if terminal_flat_truth.get("available") is not True:
        return False
    if terminal_flat_truth.get("positions_flat") is not True:
        return False
    if terminal_flat_truth.get("open_orders_flat") is not True:
        return False
    return (
        str(truth.get("project_record_status") or "")
        == "terminal_flat_exchange_truth_accounting_gap"
    )


def _order_error_resolved_by_truth_gap(
    payload: dict[str, Any],
    resolved_truth_gap_summary: dict[str, Any] | None,
) -> bool:
    if not resolved_truth_gap_summary:
        return False
    if int(resolved_truth_gap_summary.get("count", 0) or 0) <= 0:
        return False
    if not (
        _payload_is_ack_only_order_truth_gap(payload)
        or _payload_is_duplicate_client_id(payload)
    ):
        return False
    resolved = _identity_values_with_bare_refs(
        {
            str(value)
            for value in (resolved_truth_gap_summary.get("resolved_identities", []) or [])
            if value
        }
    )
    payload_identities = _identity_values_with_bare_refs(
        _truth_gap_identity_values(payload)
    )
    return bool(payload_identities & resolved)


def _build_resolved_terminal_zero_qty_reduce_only_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    terminal_positions: set[str] = set()
    truth_probe_retain_pending_positions: set[str] = set()
    resolved_positions: set[str] = set()
    terminal_event_count = 0
    truth_probe_retain_pending_count = 0
    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol", "") or "").upper()
        if symbol and event_symbol and event_symbol != symbol.upper():
            continue
        position_id = str(payload.get("position_id") or "")
        if not position_id:
            continue
        if kind == "exit.passive_close_terminal_zero_qty_reduce_only_evidence":
            terminal_event_count += 1
            terminal_positions.add(position_id)
        elif (
            kind == "exit.passive_close_maker_submit_error"
            and _payload_is_terminal_zero_qty_truth_probe_retained(payload)
        ):
            truth_probe_retain_pending_count += 1
            truth_probe_retain_pending_positions.add(position_id)
        elif kind == "exit.passive_close_resolved":
            if payload.get("live_flat_terminal") is not False:
                resolved_positions.add(position_id)
        elif kind == "runtime.position_lifecycle_terminal":
            if str(payload.get("terminal_state", "") or "").lower() == "flat":
                resolved_positions.add(position_id)

    current_clean = (
        _exchange_truth_flat(exchange_truth)
        and _exchange_truth_no_open_orders(exchange_truth)
    )
    resolved_terminal_positions = terminal_positions & resolved_positions
    unresolved_positions = terminal_positions - resolved_terminal_positions
    resolved_truth_probe_retain_pending_positions = (
        truth_probe_retain_pending_positions & resolved_positions
    )
    unresolved_truth_probe_retain_pending_positions = (
        truth_probe_retain_pending_positions
        - resolved_truth_probe_retain_pending_positions
    )
    return {
        "count": terminal_event_count,
        "position_ids": sorted(terminal_positions),
        "resolved_count": len(resolved_terminal_positions),
        "resolved_position_ids": sorted(resolved_terminal_positions),
        "unresolved_count": len(unresolved_positions),
        "unresolved_position_ids": sorted(unresolved_positions),
        "truth_probe_retain_pending_count": truth_probe_retain_pending_count,
        "truth_probe_retain_pending_position_ids": (
            sorted(truth_probe_retain_pending_positions)
        ),
        "truth_probe_retain_pending_resolved_count": len(
            resolved_truth_probe_retain_pending_positions
        ),
        "truth_probe_retain_pending_resolved_position_ids": (
            sorted(resolved_truth_probe_retain_pending_positions)
        ),
        "truth_probe_retain_pending_unresolved_count": len(
            unresolved_truth_probe_retain_pending_positions
        ),
        "truth_probe_retain_pending_unresolved_position_ids": (
            sorted(unresolved_truth_probe_retain_pending_positions)
        ),
        "current_exchange_truth_clean": current_clean,
    }


def _order_error_resolved_by_terminal_zero_qty(
    payload: dict[str, Any],
    resolved_terminal_zero_qty_summary: dict[str, Any] | None,
) -> bool:
    if not resolved_terminal_zero_qty_summary:
        return False
    if not (
        _payload_is_bybit_terminal_zero_qty_reduce_only(payload)
        or _payload_is_terminal_zero_qty_truth_probe_retained(payload)
    ):
        return False
    position_id = str(payload.get("position_id") or "")
    resolved = set(
        resolved_terminal_zero_qty_summary.get("resolved_position_ids", []) or []
    )
    resolved.update(
        resolved_terminal_zero_qty_summary.get(
            "truth_probe_retain_pending_resolved_position_ids",
            [],
        )
        or []
    )
    return bool(position_id and position_id in resolved)


def _payload_request_context(payload: dict[str, Any]) -> dict[str, Any]:
    request_context = payload.get("request_context")
    if isinstance(request_context, dict):
        return request_context
    exchange_error = _exchange_error_dict(payload)
    request_context = exchange_error.get("request_context")
    return request_context if isinstance(request_context, dict) else {}


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _payload_is_binance_close_post_only_boundary_reject(payload: dict[str, Any]) -> bool:
    exchange_error = _exchange_error_dict(payload)
    request_context = _payload_request_context(payload)
    venue = str(
        payload.get("venue")
        or payload.get("maker_venue")
        or exchange_error.get("venue")
        or request_context.get("venue")
        or ""
    ).lower()
    if venue != "binance":
        return False
    code = str(
        payload.get("exchange_code")
        or exchange_error.get("exchange_code")
        or ""
    )
    reason_text = " ".join(
        str(part or "")
        for part in (
            payload.get("reason"),
            payload.get("error"),
            exchange_error.get("exchange_msg"),
            exchange_error.get("raw_body"),
        )
    ).lower()
    if code != "-5022" and not any(
        token in reason_text
        for token in (
            "-5022",
            "gtx_order_reject",
            "could not be executed as maker",
            "post only order will be rejected",
        )
    ):
        return False
    return (
        _boolish(request_context.get("post_only"))
        and _boolish(request_context.get("reduce_only"))
    )


def _payload_is_reduce_only_terminal_flat_reject(payload: dict[str, Any]) -> bool:
    request_context = _payload_request_context(payload)
    if not _boolish(request_context.get("reduce_only")):
        return False
    exchange_error = _exchange_error_dict(payload)
    code = str(
        payload.get("exchange_code")
        or exchange_error.get("exchange_code")
        or ""
    )
    reason_text = " ".join(
        str(part or "")
        for part in (
            payload.get("reason"),
            payload.get("error"),
            exchange_error.get("exchange_msg"),
            exchange_error.get("raw_body"),
        )
    ).lower()
    extra = exchange_error.get("extra")
    if not isinstance(extra, dict):
        extra = {}
    gate_label = str(extra.get("label") or code or "").strip().lower()
    gate_message = str(
        extra.get("message")
        or exchange_error.get("message")
        or exchange_error.get("exchange_msg")
        or ""
    ).lower()
    gate_text = " ".join((reason_text, gate_label, gate_message))
    gate_code_text = " ".join((code, gate_label, reason_text)).lower()
    gate_empty_position = (
        "reduce_exceeded" in gate_code_text
        and "empty position" in gate_text
        and "pending order" not in gate_text
    )
    return (
        code == "-2022"
        or _payload_is_bybit_terminal_zero_qty_reduce_only(payload)
        or gate_empty_position
        or "reduceonly order is rejected" in reason_text
        or "reduce only order is rejected" in reason_text
        or "reduce-only order is rejected" in reason_text
    )


def _payload_is_zero_fill_terminal_flat(payload: dict[str, Any]) -> bool:
    reason = str(payload.get("reason") or payload.get("error") or "").lower()
    return "zero fill" in reason


def _build_resolved_close_order_error_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    current_clean = (
        _exchange_truth_flat(exchange_truth)
        and _exchange_truth_no_open_orders(exchange_truth)
    )
    target_symbol = symbol.upper()
    terminal_positions: set[str] = set()
    terminal_identity_values: set[str] = set()
    post_only_resolved: list[dict[str, Any]] = []
    reduce_only_resolved: list[dict[str, Any]] = []
    zero_fill_resolved: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol") or "").upper()
        if target_symbol and event_symbol and event_symbol != target_symbol:
            continue
        position_id = str(payload.get("position_id") or "")
        if not position_id:
            continue
        terminal = False
        if kind == "exit.passive_close_resolved":
            terminal = (
                payload.get("live_flat_terminal") is not False
                and payload.get("problem") is not True
            )
        elif kind == "runtime.position_lifecycle_terminal":
            terminal = (
                str(payload.get("terminal_state") or "").lower() == "flat"
                and payload.get("problem") is not True
            )
        elif kind == "execution.residual_repair_completed":
            terminal = str(payload.get("result") or "").lower() in {
                "already_flat",
                "completed",
                "flat",
            }
        elif kind == "exit.accepted_order_truth_gap_resolved":
            terminal = (
                str(payload.get("resolution_status") or "").lower()
                in _EXIT_ACCEPTED_ORDER_TRUTH_GAP_COMPLETE_STATUSES
            )
        elif kind in {"order.filled", "exit.close_chunk_submitted", "exit.closed"}:
            terminal = True
        if terminal:
            terminal_positions.add(position_id)
            terminal_identity_values.update(_strong_order_identity_values(payload))

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        if kind not in ORDER_ERROR_KINDS:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol") or "").upper()
        if target_symbol and event_symbol and event_symbol != target_symbol:
            continue
        position_id = str(payload.get("position_id") or "")
        if not position_id or position_id not in terminal_positions:
            continue
        identities = _strong_order_identity_values(payload)
        position_terminal_match = (
            f"position_id:{position_id.lower()}" in terminal_identity_values
        )
        order_identities = {
            value for value in identities
            if not value.startswith("position_id:")
        }
        order_terminal_match = bool(order_identities & terminal_identity_values)
        resolution = close_order_error_resolution_contract(
            kind=kind,
            payload=payload,
            current_exchange_truth_clean=current_clean,
            position_terminal_match=position_terminal_match,
            order_terminal_match=order_terminal_match,
            has_order_identity=bool(order_identities),
            is_post_only_close_reject=(
                _payload_is_binance_close_post_only_boundary_reject(payload)
            ),
        )
        if not resolution.get("resolved"):
            continue
        resolution_bucket = str(resolution.get("resolution_bucket") or "")

        exchange_error = _exchange_error_dict(payload)
        sample = {
            "kind": kind,
            "position_id": position_id,
            "symbol": event_symbol or str(_payload_request_context(payload).get("symbol") or "").upper(),
            "venue": str(
                payload.get("venue")
                or exchange_error.get("venue")
                or _payload_request_context(payload).get("venue")
                or ""
            ).lower(),
            "exchange_code": str(
                payload.get("exchange_code")
                or exchange_error.get("exchange_code")
                or ""
            ),
            "reason": str(
                payload.get("reason")
                or payload.get("error")
                or exchange_error.get("exchange_msg")
                or ""
            )[:300],
            "ts_ms": rec.get("ts_ms", 0),
        }
        if resolution_bucket == "post_only_boundary_reject":
            post_only_resolved.append(sample)
        elif resolution_bucket == "reduce_only_terminal_flat":
            reduce_only_resolved.append(sample)
        elif resolution_bucket == "zero_fill_terminal_flat":
            zero_fill_resolved.append(sample)
        else:
            continue
        if len(samples) < 10:
            samples.append(sample)

    resolved_identities = set(terminal_identity_values)
    resolved_identities.update(f"position_id:{position_id.lower()}" for position_id in terminal_positions)
    resolved_positions = {
        sample["position_id"]
        for sample in post_only_resolved + reduce_only_resolved + zero_fill_resolved
        if sample.get("position_id")
    }
    return {
        "count": len(post_only_resolved) + len(reduce_only_resolved) + len(zero_fill_resolved),
        "post_only_boundary_reject_count": len(post_only_resolved),
        "reduce_only_terminal_flat_count": len(reduce_only_resolved),
        "zero_fill_terminal_flat_count": len(zero_fill_resolved),
        "position_ids": sorted(resolved_positions),
        "resolved_identities": sorted(resolved_identities),
        "current_exchange_truth_clean": current_clean,
        "samples": samples,
    }


def _order_error_resolved_by_close_terminal_truth(
    payload: dict[str, Any],
    resolved_close_summary: dict[str, Any] | None,
) -> bool:
    if not resolved_close_summary:
        return False
    if int(resolved_close_summary.get("count", 0) or 0) <= 0:
        return False
    if resolved_close_summary.get("current_exchange_truth_clean") is not True:
        return False
    resolved = set(resolved_close_summary.get("resolved_identities", []) or [])
    identities = _strong_order_identity_values(payload)
    position_id = str(payload.get("position_id") or "")
    position_terminal_match = bool(
        position_id
        and f"position_id:{position_id.lower()}" in resolved
    )
    order_identities = {
        value for value in identities
        if not value.startswith("position_id:")
    }
    order_terminal_match = bool(order_identities & resolved)
    if _payload_is_binance_close_post_only_boundary_reject(payload):
        return position_terminal_match
    if (
        _payload_is_reduce_only_terminal_flat_reject(payload)
        or _payload_is_zero_fill_terminal_flat(payload)
    ):
        if _payload_is_bybit_terminal_zero_qty_reduce_only(payload):
            return position_terminal_match
        if _payload_is_aster_reduce_only_no_order_reject(payload):
            return position_terminal_match and bool(order_identities)
        return bool(order_terminal_match)
    return False


def _payload_is_binance_post_only_boundary_reject(payload: dict[str, Any]) -> bool:
    venue = str(payload.get("venue") or payload.get("maker_venue") or "").lower()
    if venue != "binance":
        return False
    exchange_error = payload.get("exchange_error", {})
    if not isinstance(exchange_error, dict):
        exchange_error = {}
    code = str(exchange_error.get("exchange_code") or "")
    reason_text = " ".join(
        str(part or "")
        for part in (
            payload.get("reason"),
            payload.get("error"),
            exchange_error.get("exchange_msg"),
            exchange_error.get("raw_body"),
        )
    ).lower()
    if code != "-5022" and not any(
        token in reason_text
        for token in (
            "-5022",
            "gtx_order_reject",
            "could not be executed as maker",
            "post only order will be rejected",
        )
    ):
        return False
    request_context = payload.get("request_context", {})
    if not isinstance(request_context, dict):
        return False
    return (
        _boolish(request_context.get("post_only"))
        and not _boolish(request_context.get("reduce_only"))
    )


def _post_only_boundary_identity(payload: dict[str, Any]) -> tuple[str, str]:
    symbol = str(payload.get("symbol") or "").upper()
    venue = str(payload.get("venue") or payload.get("maker_venue") or "").lower()
    request_context = payload.get("request_context", {})
    if isinstance(request_context, dict):
        symbol = symbol or str(request_context.get("symbol") or "").upper()
        venue = venue or str(request_context.get("venue") or "").lower()
    return symbol, venue


def _build_resolved_post_only_reject_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    current_clean = (
        _exchange_truth_flat(exchange_truth)
        and _exchange_truth_no_open_orders(exchange_truth)
    )
    reject_keys: set[tuple[str, str]] = set()
    cooldown_keys: set[tuple[str, str]] = set()
    reprice_keys: set[tuple[str, str]] = set()
    samples: list[dict[str, Any]] = []
    target_symbol = symbol.upper()

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol") or "").upper()
        if target_symbol and event_symbol and event_symbol != target_symbol:
            continue
        if _payload_is_binance_post_only_boundary_reject(payload):
            key = _post_only_boundary_identity(payload)
            if not key[0] or not key[1]:
                continue
            reject_keys.add(key)
            if len(samples) < 10:
                exchange_error = payload.get("exchange_error", {})
                if not isinstance(exchange_error, dict):
                    exchange_error = {}
                samples.append(
                    {
                        "symbol": key[0],
                        "venue": key[1],
                        "exchange_code": str(
                            exchange_error.get("exchange_code") or ""
                        ),
                        "exchange_msg": str(
                            exchange_error.get("exchange_msg")
                            or payload.get("reason")
                            or ""
                        )[:300],
                        "ts_ms": rec.get("ts_ms", 0),
                    }
                )
            continue
        if kind in {
            "runtime.entry_post_only_reject_cooldown",
            "runtime.entry_post_only_bbo_repriced",
        }:
            event_venue = str(payload.get("venue") or payload.get("maker_venue") or "").lower()
            key = (event_symbol, event_venue)
            if not key[0] or not key[1]:
                continue
            if target_symbol and key[0] != target_symbol:
                continue
            reason = str(payload.get("reason") or payload.get("raw_error") or "").lower()
            if kind == "runtime.entry_post_only_reject_cooldown" and (
                "post_only_would_take" in reason
                or "gtx_order_reject" in reason
                or "-5022" in reason
                or "could not be executed as maker" in reason
            ):
                cooldown_keys.add(key)
            elif (
                kind == "runtime.entry_post_only_bbo_repriced"
                and "post_only_would_cross" in reason
            ):
                reprice_keys.add(key)

    resolved_keys = reject_keys & cooldown_keys if current_clean else set()
    unresolved_keys = reject_keys - resolved_keys
    return {
        "count": len(reject_keys),
        "resolved_count": len(resolved_keys),
        "unresolved_count": len(unresolved_keys),
        "resolved_event_kind": "order_error.resolved_post_only_reject",
        "resolved_events": [
            {
                "kind": "order_error.resolved_post_only_reject",
                "symbol": sym,
                "venue": venue,
                "reason": "post_only_reject_resolved_by_cooldown_and_clean_truth",
            }
            for sym, venue in sorted(resolved_keys)
        ],
        "symbols": sorted({key[0] for key in reject_keys}),
        "resolved_symbols": sorted({key[0] for key in resolved_keys}),
        "unresolved_symbols": sorted({key[0] for key in unresolved_keys}),
        "resolved_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(resolved_keys)
        ],
        "unresolved_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(unresolved_keys)
        ],
        "cooldown_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(cooldown_keys)
        ],
        "reprice_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(reprice_keys)
        ],
        "current_exchange_truth_clean": current_clean,
        "samples": samples,
    }


def _order_error_resolved_by_post_only_boundary_reject(
    payload: dict[str, Any],
    resolved_post_only_summary: dict[str, Any] | None,
) -> bool:
    if not resolved_post_only_summary:
        return False
    if int(resolved_post_only_summary.get("resolved_count", 0) or 0) <= 0:
        return False
    if not _payload_is_binance_post_only_boundary_reject(payload):
        return False
    key = _post_only_boundary_identity(payload)
    resolved = {
        (
            str(item.get("symbol") or "").upper(),
            str(item.get("venue") or "").lower(),
        )
        for item in (resolved_post_only_summary.get("resolved_identities", []) or [])
        if isinstance(item, dict)
    }
    return key in resolved


def _payload_is_entry_insufficient_balance_admission_reject(payload: dict[str, Any]) -> bool:
    exchange_error = _exchange_error_dict(payload)
    request_context = _payload_request_context(payload)
    venue = str(
        payload.get("venue")
        or exchange_error.get("venue")
        or request_context.get("venue")
        or ""
    ).lower()
    if venue != "bybit":
        return False
    if _boolish(request_context.get("reduce_only")):
        return False

    exchange_code = str(
        payload.get("exchange_code") or exchange_error.get("exchange_code") or ""
    )
    text = " ".join(
        str(value or "").lower()
        for value in (
            payload.get("reason"),
            payload.get("error"),
            payload.get("response_classification"),
            exchange_error.get("exchange_msg"),
            exchange_error.get("raw_body"),
        )
    )
    return (
        exchange_code == "110007"
        or "110007" in text
        or "insufficient_balance_admission_blocked" in text
        or "insufficient_margin_admission_blocked" in text
        or "not enough for new order" in text
        or "available balance is insufficient" in text
        or "insufficient available balance" in text
    )


def _payload_is_contained_entry_admission_reject(payload: dict[str, Any]) -> bool:
    if _payload_is_entry_insufficient_balance_admission_reject(payload):
        return True
    exchange_error = _exchange_error_dict(payload)
    exchange_code = str(exchange_error.get("exchange_code") or "")
    reason = str(payload.get("reason") or "")
    text = " ".join(
        str(value or "").lower()
        for value in (
            reason,
            payload.get("error"),
            payload.get("response_classification"),
            exchange_error.get("exchange_msg"),
            exchange_error.get("raw_body"),
        )
    )
    return (
        exchange_code == "-5018"
        or exchange_code == "33004"
        or "-5018" in text
        or "33004" in text
        or is_private_health_admission_reason(reason)
        or "venue_auth_invalid" in text
        or "venue_permission_denied" in text
        or "max_notional_admission_blocked" in text
        or "maximum notional value limit" in text
    )


def _contained_entry_admission_identity(payload: dict[str, Any]) -> tuple[str, str]:
    request_context = _payload_request_context(payload)
    exchange_error = _exchange_error_dict(payload)
    symbol = str(
        payload.get("symbol")
        or exchange_error.get("symbol")
        or request_context.get("symbol")
        or ""
    ).upper()
    venue = str(
        payload.get("venue")
        or exchange_error.get("venue")
        or request_context.get("venue")
        or ""
    ).lower()
    return symbol, venue


def _exchange_truth_identity_clean(
    exchange_truth: dict[str, Any],
    key: tuple[str, str],
) -> bool:
    symbol, venue = key
    if not symbol or not venue:
        return False
    if exchange_truth.get("available") is False:
        return False
    positions_by_venue = exchange_truth.get("positions") or {}
    venue_positions = positions_by_venue.get(venue) or {}
    if isinstance(venue_positions, dict):
        pos = venue_positions.get(symbol) or venue_positions.get(symbol.upper())
        if isinstance(pos, dict) and _safe_abs_quantity(pos.get("quantity")) > 1e-9:
            return False
    orders_by_venue = exchange_truth.get("open_orders") or {}
    venue_orders = orders_by_venue.get(venue) or {}
    order_items: list[Any]
    if isinstance(venue_orders, dict):
        maybe_symbol_orders = venue_orders.get(symbol) or venue_orders.get(symbol.upper())
        if maybe_symbol_orders is None:
            order_items = list(venue_orders.values())
        elif isinstance(maybe_symbol_orders, dict):
            if maybe_symbol_orders:
                return False
            order_items = []
        elif isinstance(maybe_symbol_orders, (list, tuple)):
            if maybe_symbol_orders:
                return False
            order_items = list(maybe_symbol_orders)
        else:
            order_items = [maybe_symbol_orders]
    elif isinstance(venue_orders, (list, tuple)):
        order_items = list(venue_orders)
    else:
        order_items = []
    for order in order_items:
        if not isinstance(order, dict):
            continue
        order_symbol = str(order.get("symbol") or "").upper()
        if order_symbol == symbol.upper():
            return False
    return True


def _build_resolved_contained_entry_admission_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    current_clean = (
        _exchange_truth_flat(exchange_truth)
        and _exchange_truth_no_open_orders(exchange_truth)
    )
    target_symbol = symbol.upper()
    reject_keys: set[tuple[str, str]] = set()
    block_keys: set[tuple[str, str]] = set()
    aster_max_notional_blocked = 0
    samples: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_symbol = str(payload.get("symbol") or "").upper()
        if target_symbol and event_symbol and event_symbol != target_symbol:
            continue

        if (
            kind in ORDER_ERROR_KINDS
            and _payload_is_contained_entry_admission_reject(payload)
        ):
            key = _contained_entry_admission_identity(payload)
            if key[0] and key[1]:
                reject_keys.add(key)
            continue

        if kind not in {
            "runtime.entry_admission_blocked",
            "runtime.entry_admission_venue_degraded",
        }:
            continue
        if payload.get("evidence_gap") is not False:
            continue
        reason_text = " ".join(
            str(value or "").lower()
            for value in (
                payload.get("reason"),
                payload.get("source"),
                payload.get("raw_error"),
            )
        )
        is_aster_max_notional = (
            str(payload.get("venue") or "").lower() == "aster"
            and "max_notional_admission_blocked" in reason_text
        )
        if not (
            "admission" in reason_text
            or "insufficient_balance" in reason_text
            or "insufficient_margin" in reason_text
            or "venue_auth_invalid" in reason_text
            or "venue_permission_denied" in reason_text
            or "110007" in reason_text
            or "33004" in reason_text
            or is_aster_max_notional
        ):
            continue
        if str(payload.get("block_scope") or "").lower() not in {
            "symbol",
            "venue",
            "symbol_and_venue",
        }:
            continue
        key = _contained_entry_admission_identity(payload)
        if not key[0] or not key[1]:
            continue
        block_keys.add(key)
        if is_aster_max_notional:
            aster_max_notional_blocked += 1
        if len(samples) < 10:
            samples.append(
                {
                    "symbol": key[0],
                    "venue": key[1],
                    "reason": str(payload.get("reason") or "")[:300],
                    "source": str(payload.get("source") or "")[:120],
                    "block_scope": str(payload.get("block_scope") or ""),
                    "ts_ms": rec.get("ts_ms", 0),
                }
            )

    resolved_keys = {
        key
        for key in reject_keys & block_keys
        if current_clean or _exchange_truth_identity_clean(exchange_truth, key)
    }
    unresolved_keys = reject_keys - resolved_keys
    return {
        "count": len(reject_keys),
        "resolved_count": len(resolved_keys),
        "unresolved_count": len(unresolved_keys),
        "symbols": sorted({key[0] for key in reject_keys}),
        "resolved_symbols": sorted({key[0] for key in resolved_keys}),
        "unresolved_symbols": sorted({key[0] for key in unresolved_keys}),
        "resolved_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(resolved_keys)
        ],
        "unresolved_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(unresolved_keys)
        ],
        "aster_max_notional_blocked": aster_max_notional_blocked,
        "block_identities": [
            {"symbol": sym, "venue": venue}
            for sym, venue in sorted(block_keys)
        ],
        "current_exchange_truth_clean": current_clean,
        "resolved_by_identity_exchange_truth_clean": not current_clean
        and bool(resolved_keys),
        "samples": samples,
    }


def _order_error_resolved_by_contained_entry_admission(
    payload: dict[str, Any],
    resolved_contained_admission_summary: dict[str, Any] | None,
) -> bool:
    if not resolved_contained_admission_summary:
        return False
    if int(resolved_contained_admission_summary.get("resolved_count", 0) or 0) <= 0:
        return False
    if not _payload_is_contained_entry_admission_reject(payload):
        return False
    key = _contained_entry_admission_identity(payload)
    resolved = {
        (
            str(item.get("symbol") or "").upper(),
            str(item.get("venue") or "").lower(),
        )
        for item in (
            resolved_contained_admission_summary.get("resolved_identities", [])
            or []
        )
        if isinstance(item, dict)
    }
    return key in resolved


def _build_order_error_evidence(
    events: list[dict[str, Any]],
    symbol: str = "",
    resolved_truth_gap_summary: dict[str, Any] | None = None,
    resolved_terminal_zero_qty_summary: dict[str, Any] | None = None,
    resolved_post_only_summary: dict[str, Any] | None = None,
    resolved_close_order_error_summary: dict[str, Any] | None = None,
    resolved_contained_admission_summary: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple, dict[str, Any]] = {}

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in ORDER_ERROR_KINDS:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue

        event_symbol = str(payload.get("symbol", ""))
        venue = str(payload.get("venue", payload.get("hedge_venue", "")))
        position_id = str(payload.get("position_id", ""))
        error_msg = str(payload.get("error", payload.get("reason", "")))

        if symbol and event_symbol and event_symbol != symbol:
            continue
        if _order_error_resolved_by_truth_gap(payload, resolved_truth_gap_summary):
            continue
        if _order_error_resolved_by_terminal_zero_qty(
            payload,
            resolved_terminal_zero_qty_summary,
        ):
            continue
        if _order_error_resolved_by_post_only_boundary_reject(
            payload,
            resolved_post_only_summary,
        ):
            continue
        if _order_error_resolved_by_close_terminal_truth(
            payload,
            resolved_close_order_error_summary,
        ):
            continue
        if _order_error_resolved_by_contained_entry_admission(
            payload,
            resolved_contained_admission_summary,
        ):
            continue

        exchange_error = payload.get("exchange_error", {})
        if isinstance(exchange_error, dict):
            ex_code = str(exchange_error.get("exchange_code", ""))
            ex_msg = str(exchange_error.get("exchange_msg", ""))
            http_status = int(exchange_error.get("http_status", 0))
            completeness = str(exchange_error.get("evidence_completeness", ""))
            confidence = str(exchange_error.get("confidence", ""))
            raw_body = str(exchange_error.get("raw_body", ""))
            raw_body_present = bool(raw_body)
            missing = exchange_error.get("missing_evidence", [])
            if not isinstance(missing, list):
                missing = []
        else:
            ex_code = ""
            ex_msg = ""
            http_status = 0
            completeness = ""
            confidence = ""
            raw_body_present = False
            missing = ["exchange_error_not_structured"]

        kind_key = kind if "passive_close" not in kind else (
            "exit.passive_close_maker_submit_error"
            if "maker" in kind
            else "exit.passive_close_hedge_error"
        )
        operation = "submit_passive_order" if "passive_close" in kind else "place_order"

        key = (kind_key, position_id, venue, event_symbol, ex_code, error_msg[:100])
        if key not in groups:
            groups[key] = {
                "kind": kind_key,
                "position_id": position_id,
                "symbol": event_symbol,
                "venue": venue,
                "operation": operation,
                "error": error_msg[:500],
                "exchange_error": exchange_error if isinstance(exchange_error, dict) else {
                    "http_status": http_status,
                    "exchange_code": ex_code,
                    "exchange_msg": ex_msg,
                },
                "request_context": payload.get("request_context", {}),
                "http_status": http_status,
                "exchange_code": ex_code,
                "exchange_msg": ex_msg,
                "evidence_completeness": completeness,
                "confidence": confidence,
                "raw_body_present": raw_body_present,
                "missing_evidence": missing,
                "count": 0,
                "first_ts_ms": rec.get("ts_ms", 0),
                "last_ts_ms": rec.get("ts_ms", 0),
            }
        g = groups[key]
        g["count"] += 1
        ts = rec.get("ts_ms", 0)
        if ts:
            g["first_ts_ms"] = min(g["first_ts_ms"], ts) if g["first_ts_ms"] else ts
            g["last_ts_ms"] = max(g["last_ts_ms"], ts)

    return sorted(groups.values(), key=lambda g: g["first_ts_ms"] or 0)


# ---------------------------------------------------------------------------
# L2 evidence
# ---------------------------------------------------------------------------

def _is_ws_bbo_entry_selection_event(kind: str, payload: dict[str, Any]) -> bool:
    readiness = payload.get("readiness_evidence", {})
    if not isinstance(readiness, dict):
        readiness = {}
    provider = str(payload.get("provider") or readiness.get("provider") or "")
    effective_provider = str(
        payload.get("entry_readiness_provider_effective")
        or readiness.get("entry_readiness_provider_effective")
        or ""
    )
    reason = str(payload.get("reason") or "")
    return (
        kind == "runtime.entry_blocked_ws_bbo_selection"
        or provider == "ws_bbo_quote_lease"
        or provider == "ws_bbo_l2_on_demand"
        or effective_provider == "ws_bbo_l2_on_demand"
        or reason.startswith("entry_ws_bbo_quote_lease_")
    )


def _build_l2_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    missing_l2_count = 0
    stale_rebuild_count = 0
    sequence_gap_count = 0
    close_readiness_blocked_count = 0
    close_readiness_rewarm_failed_count = 0
    close_readiness_missing_tick_count = 0
    close_readiness_would_take_count = 0
    passive_close_maker_terminal_no_fill_count = 0
    details: list[dict[str, Any]] = []
    close_readiness_samples: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in L2_WARNING_KINDS:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        if _is_ws_bbo_entry_selection_event(kind, payload):
            continue
        if kind == "runtime.local_l2_sequence_gap":
            sequence_gap_count += 1
        elif kind == "runtime.local_l2_sync_failed":
            stale_rebuild_count += 1
        elif kind in ("runtime.entry_blocked_local_l2_selection", "runtime.entry_local_l2_readiness_diagnostics"):
            not_ready = payload.get("not_ready", []) or []
            reason_totals = payload.get("reason_totals", {})
            if reason_totals:
                missing_l2_count += sum(
                    int(v) for v in reason_totals.values()
                    if isinstance(v, (int, float))
                )
            if not_ready:
                for item in not_ready[:3]:
                    if isinstance(item, dict):
                        details.append({
                            "kind": kind,
                            "pair_id": item.get("pair_id", ""),
                            "venue": item.get("venue", ""),
                            "symbol": item.get("symbol", ""),
                            "reason": item.get("reason", ""),
                            "ts_ms": rec.get("ts_ms", 0),
                        })
        elif kind == "scan.no_entry_diagnostics":
            reason_totals = payload.get("entry_local_l2_primary_not_ready_reason_totals", {})
            if reason_totals:
                missing_l2_count += sum(
                    int(v) for v in reason_totals.values()
                    if isinstance(v, (int, float))
                )
        elif kind == "runtime.close_price_evidence_rewarm_failed":
            close_readiness_rewarm_failed_count += 1
            if len(close_readiness_samples) < 20:
                close_readiness_samples.append({
                    "kind": kind,
                    "position_id": str(payload.get("position_id") or ""),
                    "venue": str(payload.get("venue") or ""),
                    "symbol": str(payload.get("symbol") or ""),
                    "reason": str(payload.get("outcome") or payload.get("reason") or ""),
                    "ts_ms": rec.get("ts_ms", 0),
                })
        elif kind == "runtime.passive_close_readiness_blocked":
            close_readiness_blocked_count += 1
            reasons = payload.get("reasons", []) or []
            if isinstance(reasons, str):
                reasons = [reasons]
            readiness_items = payload.get("readiness", []) or []
            saw_leg_missing_tick = False
            if isinstance(readiness_items, list):
                for item in readiness_items:
                    if not isinstance(item, dict):
                        continue
                    item_reasons = item.get("reasons", []) or []
                    if isinstance(item_reasons, str):
                        item_reasons = [item_reasons]
                    if "missing_tick_size" in {str(r) for r in item_reasons}:
                        close_readiness_missing_tick_count += 1
                        saw_leg_missing_tick = True
                    if item.get("would_take") is True or "post_only_would_take" in {
                        str(r) for r in item_reasons
                    }:
                        close_readiness_would_take_count += 1
            if not saw_leg_missing_tick and any(str(reason) == "missing_tick_size" for reason in reasons):
                close_readiness_missing_tick_count += 1
            if len(close_readiness_samples) < 20:
                close_readiness_samples.append({
                    "kind": kind,
                    "position_id": str(payload.get("position_id") or ""),
                    "venue": "",
                    "symbol": str(payload.get("symbol") or ""),
                    "reason": ",".join(str(reason) for reason in reasons),
                    "ts_ms": rec.get("ts_ms", 0),
                })
        elif kind == "exit.passive_close_maker_terminal_no_fill":
            passive_close_maker_terminal_no_fill_count += 1

    return {
        "missing_l2_or_tick_count": missing_l2_count,
        "stale_rebuild_count": stale_rebuild_count,
        "sequence_gap_count": sequence_gap_count,
        "details": details[:20],
        "close_readiness_blocked_count": close_readiness_blocked_count,
        "close_readiness_rewarm_failed_count": close_readiness_rewarm_failed_count,
        "close_readiness_missing_tick_count": close_readiness_missing_tick_count,
        "close_readiness_would_take_count": close_readiness_would_take_count,
        "passive_close_maker_terminal_no_fill_count": passive_close_maker_terminal_no_fill_count,
        "close_readiness_samples": close_readiness_samples[:20],
    }


def _snapshot_domains_from_payload(payload: dict[str, Any]) -> set[str]:
    domains: set[str] = set()
    for key in ("stale_degraded_domains", "degraded_domains"):
        values = payload.get(key, []) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            domain = str(value or "").strip()
            if domain:
                domains.add(domain)
    for item in payload.get("candidate_freshness_scope", []) or []:
        if not isinstance(item, dict):
            continue
        domain = str(item.get("domain", "") or "").strip()
        if domain:
            domains.add(domain)
    return domains


def _build_snapshot_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    stale_or_degraded_count = 0
    domain_counts: dict[str, int] = {}
    blocking_scope_count = 0
    details: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in ("runtime.snapshot_stale", "runtime.snapshot_degraded"):
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        stale_or_degraded_count += 1
        domains = _snapshot_domains_from_payload(payload) or {"unknown"}
        for domain in domains:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        scoped_blockers = [
            item for item in payload.get("candidate_freshness_scope", []) or []
            if isinstance(item, dict) and bool(item.get("blocked", False))
        ]
        blocking_scope_count += len(scoped_blockers)
        if len(details) < 20:
            details.append({
                "kind": kind,
                "ts_ms": rec.get("ts_ms", 0),
                "domains": sorted(domains),
                "blocking_scope_count": len(scoped_blockers),
            })

    return {
        "stale_or_degraded_count": stale_or_degraded_count,
        "domain_counts": dict(sorted(domain_counts.items())),
        "blocking_scope_count": blocking_scope_count,
        "details": details,
    }


# ---------------------------------------------------------------------------
# runtime warnings
# ---------------------------------------------------------------------------

def _build_runtime_warnings(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in RUNTIME_WARNING_KINDS:
            payload = rec.get("payload", {})
            if not isinstance(payload, dict):
                continue
            error_str = str(payload.get("error", payload.get("reason", "")))
            if "was never awaited" in error_str:
                key = (kind, error_str[:100])
                if key not in seen:
                    seen.add(key)
                    warnings.append({
                        "kind": kind,
                        "source": "never_awaited",
                        "error": error_str[:500],
                        "ts_ms": rec.get("ts_ms", 0),
                    })
            continue

        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        msg = str(payload.get("to", payload.get("reason", "")))
        key = (kind, msg[:100])
        if key not in seen:
            seen.add(key)
            warnings.append({
                "kind": kind,
                "ts_ms": rec.get("ts_ms", 0),
                "detail": msg[:500],
            })

    return sorted(warnings, key=lambda w: w.get("ts_ms", 0))


# ---------------------------------------------------------------------------
# top exchange errors
# ---------------------------------------------------------------------------

def _build_top_exchange_errors(
    order_errors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[tuple, dict[str, Any]] = {}
    for err in order_errors:
        key = (
            err.get("venue", ""),
            err.get("symbol", ""),
            err.get("http_status", 0),
            str(err.get("exchange_code", "")),
            str(err.get("exchange_msg", ""))[:80],
        )
        if key not in by_key:
            by_key[key] = {
                "venue": err.get("venue", ""),
                "symbol": err.get("symbol", ""),
                "http_status": err.get("http_status", 0),
                "exchange_code": str(err.get("exchange_code", "")),
                "exchange_msg": str(err.get("exchange_msg", ""))[:200],
                "evidence_completeness": err.get("evidence_completeness", ""),
                "raw_body_present": err.get("raw_body_present", False),
                "missing_evidence": err.get("missing_evidence", []),
                "count": 0,
                "last_ts_ms": 0,
            }
        g = by_key[key]
        g["count"] += err.get("count", 0)
        g["last_ts_ms"] = max(g["last_ts_ms"], err.get("last_ts_ms", 0))

    return sorted(by_key.values(), key=lambda g: g["count"], reverse=True)


def _build_order_reconcile_identifier_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    invalid_local_order_identifier_count = 0
    placeholder_order_id_blocked_count = 0
    binance_invalid_client_order_id_error_count = 0
    samples: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", ""))
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue

        if kind == "order.reconcile_query":
            classification = str(payload.get("response_classification", ""))
            subtype = str(payload.get("uncertain_subtype", ""))
            order_id = str(payload.get("order_id", "") or "")
            if (
                classification == "invalid_local_order_identifier"
                or subtype == "invalid_local_order_identifier"
            ):
                invalid_local_order_identifier_count += 1
                if "-recovery-" in order_id.lower() or len(order_id) > 36:
                    placeholder_order_id_blocked_count += 1
                if len(samples) < 5:
                    samples.append({
                        "ts_ms": rec.get("ts_ms", 0),
                        "venue": payload.get("venue", ""),
                        "symbol": payload.get("symbol", ""),
                        "order_id": order_id,
                        "client_order_id": payload.get("client_order_id", ""),
                        "response_classification": classification,
                        "next_action": payload.get("next_action", ""),
                    })

        if kind == "reconciliation.entry_reconcile_error":
            error = str(payload.get("error", ""))
            if "-4015" in error or "Client order id length" in error:
                binance_invalid_client_order_id_error_count += 1

    return {
        "invalid_local_order_identifier_count": invalid_local_order_identifier_count,
        "placeholder_order_id_blocked_count": placeholder_order_id_blocked_count,
        "binance_invalid_client_order_id_error_count": (
            binance_invalid_client_order_id_error_count
        ),
        "samples": samples,
    }


# ---------------------------------------------------------------------------
# production acceptance gate
# ---------------------------------------------------------------------------

_SNAPSHOT_FALLBACK_CURRENT_TTL_MS = 60_000

def _payload_dict(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _is_snapshot_fallback_blocking(payload: dict[str, Any]) -> bool:
    if _snapshot_fallback_blocking_scope(payload):
        if not _snapshot_fallback_payload_identity_keys(payload):
            return bool(payload.get("blocked") is True or payload.get("block_reason"))
        return True
    if payload.get("blocked") is True or payload.get("block_reason"):
        return True
    return False


def _snapshot_fallback_blocking_scope(payload: dict[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for item in payload.get("candidate_freshness_scope", []) or []:
        if isinstance(item, dict) and (
            item.get("blocked") is True or item.get("block_reason")
        ):
            blockers.append(item)
    return blockers


def _snapshot_fallback_has_scoped_blocking_evidence(payload: dict[str, Any]) -> bool:
    for item in _snapshot_fallback_blocking_scope(payload):
        if not (item.get("candidate_symbol") or item.get("candidate_pair_id")):
            continue
        if (
            item.get("domain")
            or item.get("venue")
            or item.get("source_age_ms") is not None
            or item.get("fallback_duration_ms") is not None
        ):
            return True
    return False


def _snapshot_fallback_identity_keys(payload: dict[str, Any]) -> set[str]:
    keys = _snapshot_fallback_payload_identity_keys(payload)
    for item in payload.get("candidate_freshness_scope", []) or []:
        if isinstance(item, dict):
            keys.update(_snapshot_fallback_payload_identity_keys(item))
    return keys


def _snapshot_fallback_payload_identity_keys(payload: dict[str, Any]) -> set[str]:
    keys: set[str] = set()

    for field in ("candidate_pair_id", "pair_id"):
        value = str(payload.get(field) or "").strip()
        if value:
            keys.add(f"pair:{value}")
    for field in ("candidate_symbol", "symbol"):
        value = str(payload.get(field) or "").strip().upper()
        if value:
            keys.add(f"symbol:{value}")
    return keys


def _snapshot_fallback_resolution_keys(events: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    resolved_kinds = {
        "runtime.entry_quote_revalidate_resolved",
        "runtime.entry_quote_evidence_resolved_by_ws_bbo",
    }
    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = _payload_dict(rec)
        if kind == "runtime.snapshot_freshness_decision":
            reason = str(payload.get("reason") or "")
            action = str(payload.get("action") or payload.get("decision") or "")
            if (
                reason != "quote_stale_resolved_by_entry_quote_truth"
                or action not in {"allow", "continue", "resolved"}
            ):
                continue
        elif kind not in resolved_kinds:
            continue
        keys.update(_snapshot_fallback_identity_keys(payload))
    return keys


def _snapshot_fallback_resolved_by_entry_quote_truth(
    payload: dict[str, Any],
    resolution_keys: set[str],
) -> bool:
    if not resolution_keys:
        return False
    direct_keys = _snapshot_fallback_payload_identity_keys(payload)
    if direct_keys:
        return bool(direct_keys & resolution_keys)
    scoped_keys = _snapshot_fallback_identity_keys(payload)
    if scoped_keys & resolution_keys:
        return True
    return False


def _snapshot_fallback_broad_scope_demoted(payload: dict[str, Any]) -> bool:
    if _snapshot_fallback_payload_identity_keys(payload):
        return False
    if payload.get("blocked") is True or payload.get("block_reason"):
        return False
    return bool(_snapshot_fallback_has_scoped_blocking_evidence(payload))


def _snapshot_fallback_is_current(
    record: dict[str, Any],
    now_ms: int,
) -> bool:
    try:
        ts_ms = int(record.get("ts_ms", 0) or 0)
    except (TypeError, ValueError):
        ts_ms = 0
    if ts_ms <= 0 or now_ms <= 0:
        return True
    return max(now_ms - ts_ms, 0) <= _SNAPSHOT_FALLBACK_CURRENT_TTL_MS


def _snapshot_fallback_exception_conclusion(payload: dict[str, Any]) -> str:
    if payload.get("v1_parity_evidence"):
        return "v1_parity"
    if _snapshot_fallback_has_scoped_blocking_evidence(payload):
        return "v1_parity"
    return "insufficient_evidence"


def _has_official_sequence_rebuild_evidence(payload: dict[str, Any]) -> bool:
    return has_official_sequence_rebuild_evidence(payload)


def _canonical_okx_symbol(value: Any) -> str:
    raw = str(value or "").upper()
    if raw.endswith("-USDT-SWAP"):
        return raw.replace("-USDT-SWAP", "USDT")
    if raw.endswith("-SWAP"):
        return raw.replace("-SWAP", "")
    return raw.replace("-", "")


def _okx_catalog_symbols(payload: dict[str, Any]) -> set[str]:
    symbols: set[str] = set()
    for row in payload.get("okx_catalog", []) or []:
        if not isinstance(row, dict):
            continue
        inst_id = row.get("instId") or row.get("inst_id") or row.get("symbol")
        if inst_id:
            symbols.add(_canonical_okx_symbol(inst_id))
    return symbols


def _payload_symbol_set(payload: dict[str, Any]) -> set[str]:
    symbols: set[str] = set()
    for key in ("probe_symbols", "requested_symbols"):
        values = payload.get(key, []) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            if value:
                symbols.add(_canonical_okx_symbol(value))
    return symbols


def _okx_instrument_missing_skipped_count(payload: dict[str, Any]) -> int:
    explicit_count = payload.get("unsupported_count")
    if isinstance(explicit_count, (int, float)) and int(explicit_count) > 0:
        return int(explicit_count)
    skipped = payload.get("skipped_by_catalog", []) or []
    if isinstance(skipped, list) and skipped:
        return len(skipped)
    if isinstance(skipped, str) and skipped:
        return 1

    probe_symbols = {
        _canonical_okx_symbol(symbol)
        for symbol in (payload.get("probe_symbols", []) or [])
        if symbol
    }
    catalog_symbols = _okx_catalog_symbols(payload)
    if probe_symbols and catalog_symbols:
        return len(probe_symbols - catalog_symbols)

    return 1 if payload.get("instrument_missing_error") else 0


def _event_recovery_key(payload: dict[str, Any]) -> str:
    for key in (
        "position_id",
        "entry_id",
        "pending_id",
        "source_entry_id",
        "internal_entry_id",
        "pair_id",
    ):
        value = payload.get(key)
        if value:
            return str(value)
    symbol = payload.get("symbol")
    return str(symbol or "").upper()


def _event_symbol(payload: dict[str, Any]) -> str:
    return str(payload.get("symbol") or "").upper()


def _is_flat_lifecycle_terminal(kind: str, payload: dict[str, Any]) -> bool:
    if kind == "runtime.position_lifecycle_terminal":
        return str(payload.get("terminal_state", "") or "").lower() == "flat"
    if kind == "exit.passive_close_resolved":
        if not payload.get("position_id"):
            return False
        long_closed = _optional_float(payload.get("long_closed_qty"))
        short_closed = _optional_float(payload.get("short_closed_qty"))
        qty_tolerance = 1e-9
        has_closed_qty_evidence = (
            long_closed is not None
            and short_closed is not None
            and long_closed > qty_tolerance
            and short_closed > qty_tolerance
            and abs(long_closed - short_closed)
            <= max(qty_tolerance, max(abs(long_closed), abs(short_closed)) * qty_tolerance)
        )
        closure_phase = str(payload.get("closure_phase", "") or "").upper()
        closure_decision_id = str(payload.get("closure_decision_id", "") or "")
        return (
            has_closed_qty_evidence
            or closure_phase == "PASSIVE_CLOSE"
            or bool(closure_decision_id)
        )
    if kind in {
        "exit.closed",
        "exit.passive_close_fallback_terminal_flat",
        "exit.passive_close_recovery_probe_flat",
        "recovery.flat",
    }:
        return True
    if kind == "runtime.position_drift_corrected":
        to_state = str(payload.get("to", payload.get("state", "")) or "").lower()
        return to_state in {"flat", "closed"}
    return False


def _is_residual_completion(kind: str, payload: dict[str, Any]) -> bool:
    if kind in {
        "execution.residual_repair_completed",
        "recovery.residual_repairs_complete",
    }:
        return True
    if kind == "execution.residual_repair_terminal":
        reason = str(payload.get("terminal_reason", "") or "").lower()
        return bool(reason)
    return False


def _build_recovery_lifecycle_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    opened_keys: set[str] = set()
    opened_key_symbols: dict[str, str] = {}
    terminal_keys: set[str] = set()
    terminal_symbols: set[str] = set()
    residual_keys: set[str] = set()
    residual_completed_keys: set[str] = set()
    all_residuals_completed = False

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = _payload_dict(rec)
        reason = str(payload.get("reason", "") or "")
        key = _event_recovery_key(payload)
        symbol = _event_symbol(payload)

        if kind in {"entry.opened", "runtime.position_opened"} and key:
            opened_keys.add(key)
            if symbol:
                opened_key_symbols[key] = symbol

        if key and _is_flat_lifecycle_terminal(kind, payload):
            terminal_keys.add(key)
            if symbol:
                terminal_symbols.add(symbol)

        if "residual" in kind or "residual" in reason:
            residual_key = symbol or key
            if residual_key:
                residual_keys.add(residual_key)
            elif kind == "recovery.residual_repairs_complete":
                all_residuals_completed = True

        if _is_residual_completion(kind, payload):
            residual_candidates = {value for value in (key, symbol) if value}
            if residual_candidates:
                residual_completed_keys.update(residual_candidates)
            else:
                all_residuals_completed = True

    symbol_closed_open_keys = {
        key for key in opened_keys
        if opened_key_symbols.get(key)
        and opened_key_symbols[key] in terminal_symbols
    }
    closed_open_keys = (opened_keys & terminal_keys) | symbol_closed_open_keys
    unclosed_open_keys = opened_keys - closed_open_keys
    if all_residuals_completed:
        unclosed_residual_keys: set[str] = set()
    else:
        unclosed_residual_keys = residual_keys - residual_completed_keys

    return {
        "opened_keys": sorted(opened_keys),
        "closed_open_keys": sorted(closed_open_keys),
        "unclosed_open_keys": sorted(unclosed_open_keys),
        "residual_keys": sorted(residual_keys),
        "closed_residual_keys": sorted(residual_completed_keys),
        "unclosed_residual_keys": sorted(unclosed_residual_keys),
        "closed_trade_lifecycle_count": len(closed_open_keys),
        "unclosed_trade_lifecycle_count": len(unclosed_open_keys),
        "closed_residual_lifecycle_count": (
            len(residual_keys)
            if all_residuals_completed
            else len(residual_keys & residual_completed_keys)
        ),
        "unclosed_residual_lifecycle_count": len(unclosed_residual_keys),
    }


def _exchange_truth_flat(exchange_truth: dict[str, Any]) -> bool:
    if not exchange_truth.get("available"):
        return False
    if exchange_truth.get("has_nonzero_position"):
        return False
    for venue_positions in (exchange_truth.get("positions") or {}).values():
        if not isinstance(venue_positions, dict):
            continue
        for position in venue_positions.values():
            if isinstance(position, dict) and abs(float(position.get("quantity", 0) or 0)) > 1e-9:
                return False
    return True


def _exchange_truth_no_open_orders(exchange_truth: dict[str, Any]) -> bool:
    if not exchange_truth.get("available"):
        return False
    if _exchange_truth_actual_open_orders_present(exchange_truth):
        return False
    return True


def _exchange_truth_actual_open_orders_present(exchange_truth: dict[str, Any]) -> bool:
    for venue_orders in (exchange_truth.get("open_orders") or {}).values():
        if not isinstance(venue_orders, dict):
            continue
        for orders in venue_orders.values():
            if isinstance(orders, list):
                if orders:
                    return True
                continue
            if isinstance(orders, dict):
                if orders.get("error"):
                    continue
                return True
            if orders not in (None, "", False):
                return True
    return bool(exchange_truth.get("has_open_order")) and not _exchange_truth_probe_gaps(
        exchange_truth
    )


def _exchange_truth_probe_gaps(
    exchange_truth: dict[str, Any],
    *,
    active_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    active = {str(symbol or "").upper() for symbol in active_symbols or set() if symbol}
    gaps: list[dict[str, Any]] = []
    for venue, venue_orders in (exchange_truth.get("open_orders") or {}).items():
        venue_name = str(venue or "").lower()
        if not isinstance(venue_orders, dict):
            continue
        if venue_orders.get("error"):
            kind = _classify_exchange_truth_probe_error(venue_orders)
            gaps.append({
                "venue": venue_name,
                "symbol": "",
                "kind": kind,
                "blocking": True,
            })
        for symbol, orders in venue_orders.items():
            if not isinstance(orders, dict) or not orders.get("error"):
                continue
            symbol_name = str(symbol or "").upper()
            kind = _classify_exchange_truth_probe_error(orders)
            blocking = not (
                kind in {"symbol_removed", "unsupported_symbol"}
                and symbol_name
                and symbol_name not in active
            )
            gaps.append({
                "venue": venue_name,
                "symbol": symbol_name,
                "kind": kind,
                "blocking": blocking,
            })
    return gaps


def _suppress_nonblocking_probe_gap_required_venues(
    exchange_truth: dict[str, Any],
    probe_gaps: list[dict[str, Any]],
    *,
    active_venues: set[str] | None = None,
) -> None:
    active = {
        str(venue or "").lower()
        for venue in active_venues or set()
        if str(venue or "")
    }
    blocking_gap_venues = {
        str(gap.get("venue") or "").lower()
        for gap in probe_gaps
        if gap.get("blocking") is True
    }
    nonblocking_gap_venues = {
        str(gap.get("venue") or "").lower()
        for gap in probe_gaps
        if gap.get("blocking") is False
        and gap.get("kind") in {"symbol_removed", "unsupported_symbol"}
        and str(gap.get("venue") or "").lower() not in active
        and str(gap.get("venue") or "").lower() not in blocking_gap_venues
    }
    if not nonblocking_gap_venues:
        return
    missing_required = [
        str(venue or "").lower()
        for venue in exchange_truth.get("missing_required_venues", []) or []
        if str(venue or "").lower() not in nonblocking_gap_venues
    ]
    exchange_truth["missing_required_venues"] = missing_required
    missing_evidence = [
        evidence
        for evidence in exchange_truth.get("missing_evidence", []) or []
        if not any(
            evidence == f"exchange_truth_required_venue_missing_{venue}"
            for venue in nonblocking_gap_venues
        )
    ]
    exchange_truth["missing_evidence"] = missing_evidence


def _classify_exchange_truth_probe_error(row: dict[str, Any]) -> str:
    code = str(row.get("code") or row.get("error_code") or "")
    error = str(row.get("error") or row.get("msg") or row.get("message") or "").lower()
    if code == "40309" or "symbol has been removed" in error or "has been removed" in error:
        return "symbol_removed"
    if (
        "unsupported" in error
        or "not supported" in error
        or "does not exist" in error
        or "invalid symbol" in error
        or "unknown symbol" in error
    ):
        return "unsupported_symbol"
    if "rate limit" in error or "too many requests" in error:
        return "rate_limited"
    if "auth" in error or "signature" in error or "permission" in error:
        return "auth_or_transport_error"
    if error:
        return "unknown"
    return "unknown"


def _build_entry_outcome_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    selected_entry_ids: set[str] = set()
    dispatched_entry_ids: set[str] = set()
    opened_entry_ids: set[str] = set()
    passive_unfilled_entry_ids: set[str] = set()
    zero_fill_lifecycle_guard_entry_ids: set[str] = set()
    entry_execution_viability_blocked_ids: set[str] = set()
    entry_execution_viability_zero_fill_reprice_ids: set[str] = set()
    reason_counts: dict[str, int] = {}
    zero_fill_lifecycle_guard_blocker_counts: dict[str, int] = {}
    zero_fill_lifecycle_guard_samples: list[dict[str, Any]] = []
    entry_execution_viability_blocker_counts: dict[str, int] = {}
    entry_execution_viability_samples: list[dict[str, Any]] = []
    quote_lease_failure_counts: dict[str, int] = {}
    quote_lease_failure_family_counts: dict[str, int] = {}
    oi_liquidity_evidence_counts: dict[str, int] = {}
    oi_liquidity_evidence_reason_counts: dict[str, int] = {}
    oi_liquidity_health_summary = {
        "cache_hit_count": 0,
        "cache_miss_count": 0,
        "refresh_attempt_count": 0,
        "deferred_count": 0,
        "timeout_count": 0,
        "public_oi_pre_http_filtered_count": 0,
        "max_refresh_cap": 0,
        "max_refresh_elapsed_ms": 0,
    }
    oi_targeted_refresh_summary = {
        "attempt_count": 0,
        "resolved_count": 0,
        "failed_count": 0,
        "timeout_count": 0,
        "unsupported_count": 0,
        "entry_blocked_after_targeted_refresh_count": 0,
        "max_elapsed_ms": 0,
        "status_counts": {},
        "previous_status_counts": {},
    }
    normal_close_trigger_position_ids: set[str] = set()
    normal_close_shadow_ids: set[str] = set()
    exit_shadow_recorded_position_ids: set[str] = set()
    exit_shadow_recorded_shadow_ids: set[str] = set()
    exit_shadow_strategy_decision_count = 0
    passive_close_retry_kind_counts: dict[str, int] = {}
    passive_close_retry_kinds = {
        "exit.accepted_order_truth_gap_retry_blocked",
        "exit.passive_close_dual_taker_drive",
        "exit.passive_close_hedge_ack_live_truth_pending",
        "exit.passive_close_hedge_ack_reconcile_in_progress",
    }

    def payload_blocked_reasons(payload: dict[str, Any]) -> list[str]:
        raw = payload.get("blocked_reasons", []) or []
        if not isinstance(raw, list):
            raw = [raw]
        return [str(reason) for reason in raw if str(reason)]

    def viability_owner_id(payload: dict[str, Any]) -> str:
        return str(
            payload.get("entry_id")
            or payload.get("position_id")
            or payload.get("internal_entry_id")
            or payload.get("candidate_pair_id")
            or payload.get("pair_id")
            or ""
        )

    def record_entry_execution_viability(
        payload: dict[str, Any],
        *,
        reason: str,
        blocked_reasons: list[str],
        source: str,
        decision: str,
    ) -> None:
        owner_id = viability_owner_id(payload)
        if not owner_id:
            return
        entry_execution_viability_blocked_ids.add(owner_id)
        if reason == "candidate_not_tradeable_after_zero_fill_reprice":
            entry_execution_viability_zero_fill_reprice_ids.add(owner_id)
        if source == "zero_fill_reprice":
            entry_execution_viability_zero_fill_reprice_ids.add(owner_id)
        for blocker in blocked_reasons:
            entry_execution_viability_blocker_counts[blocker] = (
                entry_execution_viability_blocker_counts.get(blocker, 0) + 1
            )
        if len(entry_execution_viability_samples) < 24:
            entry_execution_viability_samples.append({
                "entry_id": owner_id,
                "symbol": str(payload.get("symbol", "") or ""),
                "reason": reason,
                "blocked_reasons": blocked_reasons,
                "source": source,
                "decision": decision,
                "phase": str(payload.get("phase", "") or ""),
            })

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        entry_id = str(
            payload.get("entry_id")
            or payload.get("position_id")
            or payload.get("internal_entry_id")
            or ""
        )
        if kind == "execution.entry_selected" and entry_id:
            selected_entry_ids.add(entry_id)
        elif kind == "runtime.entry_dispatched" and entry_id:
            dispatched_entry_ids.add(entry_id)
        elif kind == "entry.opened" and entry_id:
            opened_entry_ids.add(entry_id)
        elif kind == "runtime.position_opened" and entry_id:
            opened_entry_ids.add(entry_id)
        elif kind == "entry.passive_unfilled" and entry_id:
            passive_unfilled_entry_ids.add(entry_id)
        elif kind in {
            "runtime.normal_close_routing_passive",
            "runtime.normal_close_routing_aggressive",
        }:
            if entry_id:
                normal_close_trigger_position_ids.add(entry_id)
            shadow_id = str(payload.get("exit_shadow_id") or "")
            if shadow_id:
                normal_close_shadow_ids.add(shadow_id)
        elif kind == "exit_shadow.strategy_decision":
            exit_shadow_strategy_decision_count += 1
            if entry_id:
                exit_shadow_recorded_position_ids.add(entry_id)
            shadow_id = str(payload.get("shadow_id") or "")
            if shadow_id:
                exit_shadow_recorded_shadow_ids.add(shadow_id)
        elif kind in passive_close_retry_kinds:
            passive_close_retry_kind_counts[kind] = (
                passive_close_retry_kind_counts.get(kind, 0) + 1
            )
        elif kind == "entry.dispatch_viability_blocked":
            reason = str(payload.get("reason", "") or "")
            blocked_reasons = payload_blocked_reasons(payload)
            record_entry_execution_viability(
                payload,
                reason=reason,
                blocked_reasons=blocked_reasons,
                source=str(payload.get("source") or "entry_dispatch"),
                decision=str(payload.get("decision") or "skip_dispatch"),
            )
        elif kind in {
            "runtime.entry_quote_revalidate_failed",
            "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
        }:
            bucket = str(
                payload.get("reason_bucket")
                or payload.get("outcome")
                or payload.get("reason")
                or "quote_revalidate_failed"
            )
            quote_lease_failure_counts[bucket] = (
                quote_lease_failure_counts.get(bucket, 0) + 1
            )
            family = str(payload.get("reason_family") or bucket)
            quote_lease_failure_family_counts[family] = (
                quote_lease_failure_family_counts.get(family, 0) + 1
            )
        elif kind == "execution.entry_liquidity_blocked":
            status = str(
                payload.get("open_interest_evidence_status")
                or payload.get("reason")
                or "unknown"
            )
            oi_liquidity_evidence_counts[status] = (
                oi_liquidity_evidence_counts.get(status, 0) + 1
            )
            if status == "symbol_not_listed_before_http":
                oi_liquidity_health_summary["public_oi_pre_http_filtered_count"] += 1
            reason = str(payload.get("open_interest_evidence_reason") or "unknown")
            oi_liquidity_evidence_reason_counts[reason] = (
                oi_liquidity_evidence_reason_counts.get(reason, 0) + 1
            )
            oi_liquidity_health_summary["cache_hit_count"] += int(
                payload.get("oi_cache_hit_count") or 0
            )
            oi_liquidity_health_summary["cache_miss_count"] += int(
                payload.get("oi_cache_miss_count") or 0
            )
            oi_liquidity_health_summary["refresh_attempt_count"] += int(
                payload.get("oi_refresh_attempt_count") or 0
            )
            oi_liquidity_health_summary["deferred_count"] += int(
                payload.get("oi_deferred_count") or 0
            )
            oi_liquidity_health_summary["timeout_count"] += int(
                payload.get("oi_timeout_count") or 0
            )
            oi_liquidity_health_summary["max_refresh_cap"] = max(
                oi_liquidity_health_summary["max_refresh_cap"],
                int(payload.get("oi_refresh_cap") or 0),
            )
            oi_liquidity_health_summary["max_refresh_elapsed_ms"] = max(
                oi_liquidity_health_summary["max_refresh_elapsed_ms"],
                int(payload.get("oi_refresh_elapsed_ms") or 0),
            )
        elif kind in {
            "runtime.entry_oi_targeted_refresh_resolved",
            "runtime.entry_oi_targeted_refresh_failed",
        }:
            oi_targeted_refresh_summary["attempt_count"] += 1
            status = str(payload.get("open_interest_evidence_status") or "unknown")
            previous_status = str(
                payload.get("previous_open_interest_evidence_status") or "unknown"
            )
            status_counts = oi_targeted_refresh_summary["status_counts"]
            status_counts[status] = status_counts.get(status, 0) + 1
            previous_status_counts = oi_targeted_refresh_summary[
                "previous_status_counts"
            ]
            previous_status_counts[previous_status] = (
                previous_status_counts.get(previous_status, 0) + 1
            )
            oi_targeted_refresh_summary["max_elapsed_ms"] = max(
                oi_targeted_refresh_summary["max_elapsed_ms"],
                int(payload.get("elapsed_ms") or 0),
            )
            if kind == "runtime.entry_oi_targeted_refresh_resolved":
                oi_targeted_refresh_summary["resolved_count"] += 1
            else:
                oi_targeted_refresh_summary["failed_count"] += 1
                oi_targeted_refresh_summary[
                    "entry_blocked_after_targeted_refresh_count"
                ] += 1
            if status == "timeout":
                oi_targeted_refresh_summary["timeout_count"] += 1
            if status == "unsupported":
                oi_targeted_refresh_summary["unsupported_count"] += 1
            if status == "symbol_not_listed_before_http":
                oi_liquidity_health_summary["public_oi_pre_http_filtered_count"] += 1
        elif kind == "execution.direction_drift_blocked":
            reason = str(payload.get("reason", "") or "")
            if reason:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            blocked_reasons = payload_blocked_reasons(payload)
            if (
                entry_id
                and reason == "candidate_not_tradeable_after_zero_fill_reprice"
                and (
                    "lifecycle_risk_only" in blocked_reasons
                    or "recovery_ledger_blocked" in blocked_reasons
                )
            ):
                zero_fill_lifecycle_guard_entry_ids.add(entry_id)
                record_entry_execution_viability(
                    payload,
                    reason=reason,
                    blocked_reasons=blocked_reasons,
                    source="zero_fill_reprice",
                    decision="terminalize_without_repost",
                )
                for blocker in blocked_reasons:
                    zero_fill_lifecycle_guard_blocker_counts[blocker] = (
                        zero_fill_lifecycle_guard_blocker_counts.get(blocker, 0) + 1
                    )
                if len(zero_fill_lifecycle_guard_samples) < 24:
                    zero_fill_lifecycle_guard_samples.append({
                        "entry_id": entry_id,
                        "symbol": str(payload.get("symbol", "") or ""),
                        "reason": reason,
                        "blocked_reasons": blocked_reasons,
                        "phase": str(payload.get("phase", "") or ""),
                    })

    opened_count = len(opened_entry_ids)
    dispatched_count = len(dispatched_entry_ids)
    quote_rewarm_after_rest_stale_summary = (
        _build_quote_rewarm_after_rest_stale_summary(events)
    )
    entry_market_evidence_summary = _build_entry_market_evidence_summary(events)
    artifact_duration_summary = _build_artifact_duration_summary(events)
    phase_duration_summary = _build_phase_duration_summary(events)
    passive_unfilled_avoidable_reprice_ids = (
        passive_unfilled_entry_ids & entry_execution_viability_zero_fill_reprice_ids
    )
    shadow_expected_position_ids = set(normal_close_trigger_position_ids)
    shadow_missing_position_ids = sorted(
        shadow_expected_position_ids - exit_shadow_recorded_position_ids
    )
    close_trigger_recording_summary = {
        "normal_close_trigger_count": len(normal_close_trigger_position_ids),
        "normal_close_shadow_id_count": len(normal_close_shadow_ids),
        "exit_shadow_expected_position_count": len(shadow_expected_position_ids),
        "exit_shadow_strategy_decision_count": exit_shadow_strategy_decision_count,
        "exit_shadow_recorded_position_count": len(exit_shadow_recorded_position_ids),
        "exit_shadow_recorded_shadow_id_count": len(exit_shadow_recorded_shadow_ids),
        "exit_shadow_missing_expected_count": len(shadow_missing_position_ids),
        "exit_shadow_missing_position_ids": shadow_missing_position_ids,
        "passive_close_retry_count": sum(passive_close_retry_kind_counts.values()),
        "passive_close_retry_kind_counts": dict(
            sorted(passive_close_retry_kind_counts.items())
        ),
    }
    return {
        "selected_count": len(selected_entry_ids),
        "dispatched_count": dispatched_count,
        "opened_count": opened_count,
        "selected_not_opened_count": max(dispatched_count - opened_count, 0),
        "passive_unfilled_count": len(passive_unfilled_entry_ids),
        "passive_unfilled_quality_summary": {
            "passive_unfilled_count": len(passive_unfilled_entry_ids),
            "avoidable_reprice_not_tradeable_count": len(
                passive_unfilled_avoidable_reprice_ids
            ),
            "avoidable_reprice_not_tradeable_entry_ids": sorted(
                passive_unfilled_avoidable_reprice_ids
            ),
            "legitimate_passive_no_fill_count": max(
                len(passive_unfilled_entry_ids)
                - len(passive_unfilled_avoidable_reprice_ids),
                0,
            ),
            "scope": "entry_passive_terminality",
            "next_action": (
                "move_lifecycle_recovery_gate_before_maker_repost"
                if passive_unfilled_avoidable_reprice_ids
                else "no_passive_unfilled_quality_action_required"
            ),
        },
        "entry_execution_viability_summary": {
            "blocked_count": len(entry_execution_viability_blocked_ids),
            "zero_fill_reprice_blocked_count": len(
                entry_execution_viability_zero_fill_reprice_ids
            ),
            "blocker_counts": dict(
                sorted(entry_execution_viability_blocker_counts.items())
            ),
            "entry_ids": sorted(entry_execution_viability_blocked_ids),
            "zero_fill_reprice_entry_ids": sorted(
                entry_execution_viability_zero_fill_reprice_ids
            ),
            "samples": entry_execution_viability_samples,
            "blocks_production_gate": False,
            "scope": "entry_execution_viability",
        },
        "zero_fill_lifecycle_guard_count": len(zero_fill_lifecycle_guard_entry_ids),
        "zero_fill_lifecycle_guard_blocker_counts": dict(
            sorted(zero_fill_lifecycle_guard_blocker_counts.items())
        ),
        "zero_fill_lifecycle_guard_entry_ids": sorted(
            zero_fill_lifecycle_guard_entry_ids
        ),
        "zero_fill_lifecycle_guard_samples": zero_fill_lifecycle_guard_samples,
        "quote_lease_failure_counts": dict(sorted(quote_lease_failure_counts.items())),
        "quote_lease_failure_family_counts": dict(
            sorted(quote_lease_failure_family_counts.items())
        ),
        "oi_liquidity_evidence_counts": dict(
            sorted(oi_liquidity_evidence_counts.items())
        ),
        "oi_liquidity_evidence_reason_counts": dict(
            sorted(oi_liquidity_evidence_reason_counts.items())
        ),
        "oi_liquidity_health_summary": oi_liquidity_health_summary,
        "oi_targeted_refresh_summary": {
            **oi_targeted_refresh_summary,
            "status_counts": dict(
                sorted(oi_targeted_refresh_summary["status_counts"].items())
            ),
            "previous_status_counts": dict(
                sorted(oi_targeted_refresh_summary["previous_status_counts"].items())
            ),
        },
        "entry_market_evidence_summary": entry_market_evidence_summary,
        "quote_rewarm_after_rest_stale_summary": quote_rewarm_after_rest_stale_summary,
        "artifact_duration_summary": artifact_duration_summary,
        "phase_duration_summary": phase_duration_summary,
        "close_trigger_recording_summary": close_trigger_recording_summary,
        "reason_counts": dict(sorted(reason_counts.items())),
    }


def _build_entry_market_evidence_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    def optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    action_counts: dict[str, int] = {}
    evidence_class_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []
    total_count = 0
    blocked_candidate_count = 0
    terminal_candidate_rewarm_count = 0
    diagnostic_recovered_overbudget_count = 0
    unresolved_blocker_count = 0
    oi_resolved_count = 0
    oi_failed_count = 0
    oi_blocked_candidate_count = 0
    oi_unavailable_count = 0
    oi_below_floor_count = 0
    oi_structural_count = 0
    oi_structural_suppressed_count = 0
    oi_structural_next_recheck_ms = 0
    quote_blocked_candidate_count = 0
    prewarm_extra_targeted_count = 0
    prewarm_extra_resolved_count = 0
    prewarm_extra_failed_count = 0
    prewarm_extra_suppressed_count = 0
    blocked_owner_stats: dict[str, dict[str, Any]] = {}

    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        else:
            payload = dict(payload)
            payload.setdefault("event_ts_ms", rec.get("ts_ms", 0))
        prewarm_only = (
            str(payload.get("evidence_role") or "").strip().lower() == "prewarm_only"
            or str(payload.get("candidate_scope") or "").strip().lower()
            == "prewarm_extra"
        )
        if prewarm_only:
            if kind in {
                "runtime.entry_quote_revalidate_targeted",
                "runtime.entry_oi_targeted_refresh_started",
            }:
                prewarm_extra_targeted_count += int(payload.get("target_count") or 1)
            elif kind in {
                "runtime.entry_quote_revalidate_resolved",
                "runtime.entry_oi_targeted_refresh_resolved",
            }:
                prewarm_extra_resolved_count += 1
            elif kind in {
                "runtime.entry_quote_revalidate_failed",
                "runtime.entry_oi_targeted_refresh_failed",
                "runtime.entry_quote_rewarm_terminal_stale",
            }:
                prewarm_extra_failed_count += 1
        contract = entry_market_evidence_contract(kind, payload)
        if not contract:
            continue
        total_count += 1
        action = str(contract.get("action") or "unknown")
        evidence_class = str(contract.get("evidence_class") or "unknown")
        action_counts[action] = action_counts.get(action, 0) + 1
        evidence_class_counts[evidence_class] = (
            evidence_class_counts.get(evidence_class, 0) + 1
        )
        blocks_entry = contract.get("blocks_entry") is True
        if blocks_entry:
            blocked_candidate_count += 1
            if evidence_class == "oi":
                oi_blocked_candidate_count += 1
                if action == "block_oi_unavailable":
                    oi_unavailable_count += 1
                elif action == "block_oi_below_floor":
                    oi_below_floor_count += 1
                elif action == "block_oi_structural":
                    oi_structural_count += 1
            elif evidence_class == "quote":
                quote_blocked_candidate_count += 1
            owner_id = str(contract.get("owner_id") or "")
            if owner_id:
                stat = blocked_owner_stats.setdefault(
                    owner_id,
                    {
                        "owner_id": owner_id,
                        "count": 0,
                        "actions": {},
                        "evidence_classes": {},
                        "reasons": {},
                    },
                )
                stat["count"] = int(stat.get("count") or 0) + 1
                actions = stat["actions"]
                if isinstance(actions, dict):
                    actions[action] = int(actions.get(action) or 0) + 1
                classes = stat["evidence_classes"]
                if isinstance(classes, dict):
                    classes[evidence_class] = int(
                        classes.get(evidence_class) or 0
                    ) + 1
                reasons = stat["reasons"]
                if isinstance(reasons, dict):
                    reason = str(contract.get("reason") or "unknown")
                    reasons[reason] = int(reasons.get(reason) or 0) + 1
                if action == "block_oi_structural":
                    suppress_until_ms = optional_int(payload.get("suppress_until_ms"))
                    if suppress_until_ms:
                        stat["next_structural_recheck_ms"] = max(
                            int(stat.get("next_structural_recheck_ms") or 0),
                            suppress_until_ms,
                        )
                        oi_structural_next_recheck_ms = max(
                            oi_structural_next_recheck_ms,
                            suppress_until_ms,
                        )
        elif action == "suppress_oi_structural":
            oi_structural_count += 1
            oi_structural_suppressed_count += 1
            suppress_until_ms = optional_int(
                payload.get("next_structural_recheck_ms")
                or payload.get("suppress_until_ms")
            )
            if suppress_until_ms:
                oi_structural_next_recheck_ms = max(
                    oi_structural_next_recheck_ms,
                    suppress_until_ms,
                )
            owner_id = str(contract.get("owner_id") or "")
            if owner_id:
                stat = blocked_owner_stats.setdefault(
                    owner_id,
                    {
                        "owner_id": owner_id,
                        "count": 0,
                        "actions": {},
                        "evidence_classes": {},
                        "reasons": {},
                    },
                )
                stat["count"] = int(stat.get("count") or 0) + 1
                actions = stat["actions"]
                if isinstance(actions, dict):
                    actions[action] = int(actions.get(action) or 0) + 1
                classes = stat["evidence_classes"]
                if isinstance(classes, dict):
                    classes[evidence_class] = int(
                        classes.get(evidence_class) or 0
                    ) + 1
                reasons = stat["reasons"]
                if isinstance(reasons, dict):
                    reason = str(contract.get("reason") or "unknown")
                    reasons[reason] = int(reasons.get(reason) or 0) + 1
                stat["structural_suppressed_count"] = int(
                    stat.get("structural_suppressed_count") or 0
                ) + 1
                if suppress_until_ms:
                    stat["next_structural_recheck_ms"] = max(
                        int(stat.get("next_structural_recheck_ms") or 0),
                        suppress_until_ms,
                    )
        if action == "terminal_candidate_rewarm":
            terminal_candidate_rewarm_count += 1
            unresolved_blocker_count += 1
        elif action == "diagnostic_recovered_overbudget":
            diagnostic_recovered_overbudget_count += 1
        elif kind == "runtime.entry_oi_targeted_refresh_failed" and blocks_entry:
            unresolved_blocker_count += 1
        if evidence_class == "oi" and action in {
            "allow_entry_evidence",
            "diagnostic_recovered_overbudget",
        }:
            oi_resolved_count += 1
        elif kind == "runtime.entry_oi_targeted_refresh_failed" and not prewarm_only:
            oi_failed_count += 1
        if prewarm_only and not blocks_entry and action == "refresh_evidence":
            prewarm_extra_suppressed_count += 1
        if len(samples) < 5 and action != "refresh_evidence":
            samples.append({
                "action": action,
                "blocks_entry": blocks_entry,
                "evidence_class": evidence_class,
                "owner_id": str(contract.get("owner_id") or ""),
                "reason": str(contract.get("reason") or ""),
                "scope": (
                    "entry_candidate_admission"
                    if blocks_entry
                    else "entry_market_evidence"
                ),
            })

    for stat in blocked_owner_stats.values():
        reasons = stat.get("reasons") if isinstance(stat.get("reasons"), dict) else {}
        actions = stat.get("actions") if isinstance(stat.get("actions"), dict) else {}
        structural_count = int((actions or {}).get("block_oi_structural") or 0)
        next_recheck = int(stat.get("next_structural_recheck_ms") or 0)
        existing_suppressed = int(stat.get("structural_suppressed_count") or 0)
        if structural_count > 1 and next_recheck:
            suppressed_count = structural_count - 1
            stat["structural_suppressed_count"] = (
                existing_suppressed + suppressed_count
            )
            oi_structural_suppressed_count += suppressed_count

    top_blocked_owner_ids: list[dict[str, Any]] = []
    for stat in sorted(
        blocked_owner_stats.values(),
        key=lambda item: (
            -int(item.get("count") or 0),
            str(item.get("owner_id") or ""),
        ),
    )[:12]:
        top_blocked_owner_ids.append({
            "owner_id": str(stat.get("owner_id") or ""),
            "count": int(stat.get("count") or 0),
            "actions": dict(sorted((stat.get("actions") or {}).items())),
            "evidence_classes": dict(
                sorted((stat.get("evidence_classes") or {}).items())
            ),
            "reasons": dict(sorted((stat.get("reasons") or {}).items())),
        })
        if stat.get("structural_suppressed_count"):
            top_blocked_owner_ids[-1]["structural_suppressed_count"] = int(
                stat.get("structural_suppressed_count") or 0
            )
        if stat.get("next_structural_recheck_ms"):
            top_blocked_owner_ids[-1]["next_structural_recheck_ms"] = int(
                stat.get("next_structural_recheck_ms") or 0
            )

    if oi_unavailable_count > 0 or quote_blocked_candidate_count > 0:
        next_action = "targeted_refresh_or_data_source_backfill"
    elif oi_below_floor_count > 0 or oi_structural_count > 0:
        next_action = "confirmed_oi_below_floor_no_data_backfill"
    else:
        next_action = "no_entry_market_evidence_action_required"
    prewarm_pending_count = max(
        prewarm_extra_targeted_count
        - prewarm_extra_resolved_count
        - prewarm_extra_failed_count,
        0,
    )
    readiness_buckets = {
        "quote_stale": quote_blocked_candidate_count,
        "oi_below_floor": oi_below_floor_count,
        "oi_unavailable": oi_unavailable_count,
        "oi_structural_suppressed": oi_structural_suppressed_count,
        "prewarm_pending": prewarm_pending_count,
    }

    return {
        "action_counts": dict(sorted(action_counts.items())),
        "blocked_candidate_count": blocked_candidate_count,
        "candidate_admission_noise_summary": {
            "blocks_production_gate": False,
            "current_abnormal_position_count": 0,
            "current_close_blocker_count": 0,
            "current_warning_count": 0,
            "current_scope": "entry_candidate_admission",
            "next_action": next_action,
            "raw_candidate_block_count": blocked_candidate_count,
            "structural_suppressed_count": oi_structural_suppressed_count,
            "next_structural_recheck_ms": oi_structural_next_recheck_ms or None,
            "top_blocked_owner_ids": top_blocked_owner_ids,
        },
        "market_data_readiness_summary": {
            "blocks_production_gate": False,
            "current_scope": "entry_candidate_admission",
            "readiness_buckets": readiness_buckets,
            "readiness_blocked_count": sum(readiness_buckets.values()),
            "raw_candidate_block_count": blocked_candidate_count,
            "dedupe_scope": "symbol_venue_ttl",
            "next_action": next_action,
            "top_blocked_owner_ids": top_blocked_owner_ids,
        },
        "diagnostic_recovered_overbudget_count": (
            diagnostic_recovered_overbudget_count
        ),
        "evidence_class_counts": dict(sorted(evidence_class_counts.items())),
        "oi": {
            "below_floor_count": oi_below_floor_count,
            "blocked_candidate_count": oi_blocked_candidate_count,
            "failed_count": oi_failed_count,
            "resolved_count": oi_resolved_count,
            "structural_count": oi_structural_count,
            "structural_suppressed_count": oi_structural_suppressed_count,
            "next_structural_recheck_ms": oi_structural_next_recheck_ms or None,
            "unavailable_count": oi_unavailable_count,
        },
        "quote": {
            "blocked_candidate_count": quote_blocked_candidate_count,
            "terminal_candidate_rewarm_count": terminal_candidate_rewarm_count,
        },
        "prewarm_extra": {
            "targeted_count": prewarm_extra_targeted_count,
            "resolved_count": prewarm_extra_resolved_count,
            "failed_count": prewarm_extra_failed_count,
            "suppressed_count": prewarm_extra_suppressed_count,
        },
        "blocker_scope": "entry_candidate_admission",
        "samples": samples,
        "terminal_candidate_rewarm_count": terminal_candidate_rewarm_count,
        "total_count": total_count,
        "unresolved_blocker_count": unresolved_blocker_count,
        "unresolved_blocker_scope": "entry_candidate_admission",
    }


def _event_venue_symbol_key(payload: dict[str, Any]) -> tuple[str, str]:
    return (
        str(payload.get("venue") or "").strip().lower(),
        str(payload.get("symbol") or "").strip().upper(),
    )


UNPAIRED_LIVE_POSITION_RECOVERY_EVENTS = {
    "recovery.unpaired_live_position_detected",
    "recovery.unpaired_live_position_owner_excluded",
    "recovery.unpaired_live_position_cleanup_skipped",
    "recovery.unpaired_live_position_cleanup_attempt",
    "recovery.unpaired_live_position_cleanup_submitted",
    "recovery.unpaired_live_position_cleanup_succeeded",
    "recovery.unpaired_live_position_cleanup_failed",
    "recovery.unpaired_live_position_terminal_flat",
}


STALE_RISK_STATE_ALIGNMENT_EVENTS = {
    "runtime.stale_risk_state_alignment_started",
    "runtime.stale_risk_state_aligned",
    "runtime.stale_risk_state_alignment_blocked",
}


def _build_stale_risk_state_alignment_summary(
    events: list[dict[str, Any]],
    *,
    since_ms: int = 0,
) -> dict[str, Any]:
    counts = {
        "started_count": 0,
        "aligned_count": 0,
        "blocked_count": 0,
    }
    latest: dict[str, Any] | None = None
    symbols: set[str] = set()
    venues: set[str] = set()
    for event in events:
        kind = str(event.get("kind") or "")
        if kind not in STALE_RISK_STATE_ALIGNMENT_EVENTS:
            continue
        ts_ms = int(event.get("ts_ms") or 0)
        if since_ms and ts_ms < since_ms:
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if kind == "runtime.stale_risk_state_alignment_started":
            counts["started_count"] += 1
        elif kind == "runtime.stale_risk_state_aligned":
            counts["aligned_count"] += 1
        elif kind == "runtime.stale_risk_state_alignment_blocked":
            counts["blocked_count"] += 1
        for symbol in payload.get("symbols") or []:
            if str(symbol):
                symbols.add(str(symbol))
        for venue in payload.get("venues") or []:
            if str(venue):
                venues.add(str(venue))
        terminalized_records_raw = payload.get("terminalized_records")
        if isinstance(terminalized_records_raw, list):
            terminalized_record_ids = [
                str(item)
                for item in terminalized_records_raw
                if str(item)
            ]
            terminalized_records = len(terminalized_record_ids)
        else:
            terminalized_record_ids = []
            try:
                terminalized_records = int(terminalized_records_raw or 0)
            except (TypeError, ValueError):
                terminalized_records = 0
        sample = {
            "kind": kind,
            "ts_ms": ts_ms,
            "source": str(payload.get("source") or ""),
            "reason": str(payload.get("reason") or ""),
            "symbols": list(payload.get("symbols") or []),
            "venues": list(payload.get("venues") or []),
            "previous_risk_mode": str(payload.get("previous_risk_mode") or ""),
            "new_risk_mode": str(payload.get("new_risk_mode") or ""),
            "previous_lifecycle": str(payload.get("previous_lifecycle") or ""),
            "new_lifecycle": str(payload.get("new_lifecycle") or ""),
            "terminalized_records": terminalized_records,
            "terminalized_record_ids": terminalized_record_ids,
            "residual_blockers": list(payload.get("residual_blockers") or []),
        }
        if latest is None or ts_ms >= int(latest.get("ts_ms") or 0):
            latest = sample
    total = sum(counts.values())
    return {
        **counts,
        "count": total,
        "recent_incident": total > 0,
        "symbols": sorted(symbols),
        "venues": sorted(venues),
        "latest_event": latest or {},
    }


def _diagnose_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _build_unpaired_live_position_recovery_summary(
    state: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_records = state.get("unpaired_live_position_recoveries", []) or []
    records = [dict(item) for item in raw_records if isinstance(item, dict)]
    event_counts: dict[str, int] = {}
    last_auto_enabled: bool | None = None
    latest_event_by_work: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in events:
        kind = str(event.get("kind") or "")
        if kind not in UNPAIRED_LIVE_POSITION_RECOVERY_EVENTS:
            continue
        event_counts[kind] = event_counts.get(kind, 0) + 1
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if "auto_enabled" in payload:
            last_auto_enabled = bool(payload.get("auto_enabled"))
        key = (
            str(payload.get("venue") or "").lower(),
            str(payload.get("symbol") or "").upper(),
            str(payload.get("side") or "").lower(),
        )
        latest_event_by_work[key] = {
            "kind": kind,
            "ts_ms": int(event.get("ts_ms") or 0),
            "reason": str(payload.get("reason") or payload.get("last_error") or ""),
            "current_risk_exposure": payload.get("current_risk_exposure"),
            "business_terminal": payload.get("business_terminal"),
            "diagnostic_severity": str(payload.get("diagnostic_severity") or ""),
            "next_action": str(payload.get("next_action") or ""),
        }

    details: list[dict[str, Any]] = []
    terminal_count = 0
    active_count = 0
    manual_required_count = 0
    current_risk_exposure_count = 0
    for record in records:
        terminal_status = str(record.get("terminal_status") or "")
        if terminal_status == "flat":
            terminal_count += 1
        else:
            active_count += 1
        current_risk_exposure = (
            terminal_status != "flat"
            and _diagnose_float(record.get("quantity")) > 1e-9
        )
        if current_risk_exposure:
            current_risk_exposure_count += 1
        if terminal_status == "manual_required":
            manual_required_count += 1
        key = (
            str(record.get("venue") or "").lower(),
            str(record.get("symbol") or "").upper(),
            str(record.get("side") or "").lower(),
        )
        latest_event = latest_event_by_work.get(key, {})
        details.append(
            {
                "venue": key[0],
                "symbol": key[1],
                "side": key[2],
                "quantity": _diagnose_float(record.get("quantity")),
                "notional_quote": _diagnose_float(record.get("notional_quote")),
                "first_seen_ms": int(record.get("first_seen_ms") or 0),
                "attempt_count": int(record.get("attempt_count") or 0),
                "next_attempt_ms": int(record.get("next_attempt_ms") or 0),
                "last_error": str(record.get("last_error") or ""),
                "terminal_status": terminal_status,
                "owner_excluded": bool(record.get("owner_excluded")),
                "open_order_truth_available": bool(
                    record.get("open_order_truth_available")
                ),
                "cap_quote": _diagnose_float(record.get("cap_quote")),
                "cap_ok": bool(record.get("cap_ok")),
                "current_risk_exposure": current_risk_exposure,
                "business_terminal": terminal_status == "flat",
                "latest_event": latest_event,
            }
        )

    return {
        "current_work_count": active_count,
        "active_work_count": active_count,
        "terminal_flat_count": terminal_count,
        "manual_required_count": manual_required_count,
        "current_risk_exposure_count": current_risk_exposure_count,
        "auto_enabled": last_auto_enabled,
        "event_counts": dict(sorted(event_counts.items())),
        "details": details,
    }


def _build_quote_rewarm_after_rest_stale_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    scheduled: list[tuple[int, tuple[str, str], dict[str, Any]]] = []
    followups: list[tuple[int, str, tuple[str, str], dict[str, Any]]] = []
    resolved_kinds = {
        "runtime.entry_quote_revalidate_resolved",
        "runtime.entry_ws_bbo_top_candidate_rewarm_succeeded",
    }
    failure_kinds = {
        "runtime.entry_quote_revalidate_failed",
        "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
    }
    for rec in events:
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        ts_ms = int(rec.get("ts_ms") or payload.get("ts_ms") or 0)
        kind = str(rec.get("kind") or "")
        key = _event_venue_symbol_key(payload)
        if not key[0] or not key[1]:
            continue
        if kind == "runtime.entry_quote_rewarm_scheduled_after_rest_stale":
            scheduled.append((ts_ms, key, payload))
        elif kind in resolved_kinds or kind in failure_kinds:
            followups.append((ts_ms, kind, key, payload))

    resolved_count = 0
    still_stale_count = 0
    timeout_count = 0
    still_stale_by_venue_symbol: dict[str, int] = {}
    timeout_by_venue_symbol: dict[str, int] = {}
    samples: list[dict[str, Any]] = []
    for scheduled_at_ms, key, _payload in scheduled:
        status = "timeout"
        matched_at_ms = 0
        matched_field = "timeout_at_ms"
        matched_payload: dict[str, Any] = {}
        for ts_ms, kind, event_key, payload in followups:
            if event_key != key or ts_ms < scheduled_at_ms:
                continue
            if kind in resolved_kinds:
                status = "resolved"
                matched_at_ms = ts_ms
                matched_field = "resolved_at_ms"
                matched_payload = payload
                break
            reason_bucket = str(payload.get("reason_bucket") or "")
            if kind in failure_kinds and reason_bucket == "rest_resolved_but_stale":
                status = "still_stale"
                matched_at_ms = ts_ms
                matched_field = "still_stale_at_ms"
                matched_payload = payload
                break
        if status == "resolved":
            resolved_count += 1
        elif status == "still_stale":
            still_stale_count += 1
            bucket = f"{key[0]}:{key[1]}"
            still_stale_by_venue_symbol[bucket] = (
                still_stale_by_venue_symbol.get(bucket, 0) + 1
            )
        else:
            timeout_count += 1
            bucket = f"{key[0]}:{key[1]}"
            timeout_by_venue_symbol[bucket] = (
                timeout_by_venue_symbol.get(bucket, 0) + 1
            )
        if len(samples) < 12:
            sample = {
                "venue": key[0],
                "symbol": key[1],
                "status": status,
                "scheduled_at_ms": scheduled_at_ms,
            }
            if matched_at_ms:
                sample[matched_field] = matched_at_ms
            for field in (
                "rest_quote_observed_at_ms",
                "rest_quote_received_at_ms",
                "rest_quote_exchange_event_at_ms",
            ):
                if field in matched_payload:
                    sample[field] = matched_payload.get(field)
            samples.append(sample)

    return {
        "scheduled_count": len(scheduled),
        "resolved_count": resolved_count,
        "still_stale_count": still_stale_count,
        "timeout_count": timeout_count,
        "still_stale_by_venue_symbol": dict(sorted(still_stale_by_venue_symbol.items())),
        "timeout_by_venue_symbol": dict(sorted(timeout_by_venue_symbol.items())),
        "samples": samples,
    }


def _entry_artifact_id(payload: dict[str, Any]) -> str:
    return str(
        payload.get("entry_id")
        or payload.get("pending_id")
        or payload.get("position_id")
        or payload.get("internal_entry_id")
        or ""
    )


def _build_artifact_duration_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    artifacts: dict[str, dict[str, Any]] = {}
    entry_times = _entry_time_info(events)

    def artifact_for(entry_id: str) -> dict[str, Any]:
        return artifacts.setdefault(
            entry_id,
            {
                "entry_id": entry_id,
                "symbol": "",
                "selected_at_ms": 0,
                "pending_created_at_ms": 0,
                "entry_started_at_ms": 0,
                "entered_at_ms": 0,
                "opened_at_ms": 0,
                "semantic_entry_at_ms": 0,
                "entry_time_source": "",
                "entry_timestamp_quality": "",
                "close_created_at_ms": 0,
                "terminal_at_ms": 0,
                "terminal_kind": "",
                "terminal_reason": "",
                "long_lived": False,
                "missing_l2_or_tick_count": 0,
            },
        )

    terminal_kinds = {
        "entry.aborted",
        "entry.passive_unfilled",
        "runtime.position_lifecycle_terminal",
        "exit.passive_close_resolved",
        "exit.reconciled",
    }
    for rec in events:
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        entry_id = _entry_artifact_id(payload)
        if not entry_id:
            continue
        ts_ms = int(rec.get("ts_ms") or payload.get("ts_ms") or 0)
        kind = str(rec.get("kind") or "")
        artifact = artifact_for(entry_id)
        symbol = str(payload.get("symbol") or "")
        if symbol and not artifact["symbol"]:
            artifact["symbol"] = symbol
        if kind == "execution.entry_selected":
            artifact["selected_at_ms"] = ts_ms
        elif kind == "runtime.pending_entry_registered":
            artifact["pending_created_at_ms"] = ts_ms
        elif kind == "entry.opened":
            artifact["opened_at_ms"] = int(payload.get("opened_at_ms") or ts_ms or 0)
            artifact["entered_at_ms"] = int(payload.get("entered_at_ms") or 0)
            artifact["entry_timestamp_quality"] = str(
                payload.get("entry_timestamp_quality") or ""
            )
        elif kind == "exit.passive_close_created":
            artifact["close_created_at_ms"] = ts_ms
        elif kind == "pending_entry.long_lived_pending_entry":
            artifact["long_lived"] = True
        elif kind == "exit.passive_close_missing_l2_or_tick":
            artifact["missing_l2_or_tick_count"] += 1

        if kind in terminal_kinds:
            artifact["terminal_at_ms"] = ts_ms
            artifact["terminal_kind"] = kind
            artifact["terminal_reason"] = str(payload.get("reason") or "")

    samples: list[dict[str, Any]] = []
    max_selected_to_terminal_ms = 0
    max_pending_created_to_terminal_ms = 0
    max_entered_to_terminal_ms = 0
    max_semantic_entry_to_terminal_ms = 0
    max_close_created_to_terminal_ms = 0
    long_lived_pending_entry_count = 0
    close_data_quality_warning_count = 0
    for artifact in artifacts.values():
        terminal_at_ms = int(artifact["terminal_at_ms"] or 0)
        selected_at_ms = int(artifact["selected_at_ms"] or 0)
        pending_created_at_ms = int(artifact["pending_created_at_ms"] or 0)
        entry_info = entry_times.get(str(artifact["entry_id"] or ""), {})
        semantic_entry_ms, entry_time_source, timestamp_quality = (
            _select_quick_flat_entry_time(entry_info, terminal_at_ms)
            if entry_info
            else (0, "", "")
        )
        entry_started_at_ms = int(
            entry_info.get("started_at_ms")
            or semantic_entry_ms
            or artifact["entered_at_ms"]
            or artifact["opened_at_ms"]
            or 0
        )
        if semantic_entry_ms > 0:
            artifact["semantic_entry_at_ms"] = semantic_entry_ms
            artifact["entry_time_source"] = entry_time_source
            artifact["entry_timestamp_quality"] = timestamp_quality
            artifact["entry_started_at_ms"] = entry_started_at_ms
        close_created_at_ms = int(artifact["close_created_at_ms"] or 0)
        selected_to_terminal_ms = (
            max(0, terminal_at_ms - selected_at_ms)
            if selected_at_ms > 0 and terminal_at_ms > 0
            else 0
        )
        pending_created_to_terminal_ms = (
            max(0, terminal_at_ms - pending_created_at_ms)
            if pending_created_at_ms > 0 and terminal_at_ms > 0
            else 0
        )
        entered_at_ms = int(artifact["entered_at_ms"] or 0)
        entered_to_terminal_ms = (
            max(0, terminal_at_ms - entered_at_ms)
            if entered_at_ms > 0 and terminal_at_ms > 0
            else 0
        )
        semantic_entry_at_ms = int(artifact["semantic_entry_at_ms"] or 0)
        semantic_entry_to_terminal_ms = (
            max(0, terminal_at_ms - semantic_entry_at_ms)
            if semantic_entry_at_ms > 0 and terminal_at_ms > 0
            else 0
        )
        close_created_to_terminal_ms = (
            max(0, terminal_at_ms - close_created_at_ms)
            if close_created_at_ms > 0 and terminal_at_ms > 0
            else 0
        )
        max_selected_to_terminal_ms = max(
            max_selected_to_terminal_ms, selected_to_terminal_ms
        )
        max_pending_created_to_terminal_ms = max(
            max_pending_created_to_terminal_ms, pending_created_to_terminal_ms
        )
        max_entered_to_terminal_ms = max(
            max_entered_to_terminal_ms, entered_to_terminal_ms
        )
        max_semantic_entry_to_terminal_ms = max(
            max_semantic_entry_to_terminal_ms, semantic_entry_to_terminal_ms
        )
        max_close_created_to_terminal_ms = max(
            max_close_created_to_terminal_ms, close_created_to_terminal_ms
        )
        long_lived = bool(artifact["long_lived"])
        close_warning = int(artifact["missing_l2_or_tick_count"] or 0) > 0
        if long_lived:
            long_lived_pending_entry_count += 1
        if close_warning:
            close_data_quality_warning_count += 1
        if selected_to_terminal_ms or pending_created_to_terminal_ms or semantic_entry_to_terminal_ms or entered_to_terminal_ms or close_created_to_terminal_ms or long_lived or close_warning:
            status = "terminal"
            if artifact["terminal_kind"] == "entry.aborted":
                status = "aborted"
            elif artifact["terminal_kind"] in {
                "runtime.position_lifecycle_terminal",
                "exit.passive_close_resolved",
                "exit.reconciled",
            }:
                status = "closed"
            samples.append({
                "entry_id": artifact["entry_id"],
                "symbol": artifact["symbol"],
                "status": status,
                "entry_started_at_ms": int(artifact["entry_started_at_ms"] or 0),
                "entered_at_ms": entered_at_ms,
                "opened_at_ms": int(artifact["opened_at_ms"] or 0),
                "semantic_entry_at_ms": semantic_entry_at_ms,
                "entry_time_source": str(artifact["entry_time_source"] or ""),
                "entry_timestamp_quality": str(
                    artifact["entry_timestamp_quality"] or ""
                ),
                "selected_to_terminal_ms": selected_to_terminal_ms,
                "pending_created_to_terminal_ms": pending_created_to_terminal_ms,
                "entered_to_terminal_ms": entered_to_terminal_ms,
                "semantic_entry_to_terminal_ms": semantic_entry_to_terminal_ms,
                "close_created_to_terminal_ms": close_created_to_terminal_ms,
                "long_lived": long_lived,
                "close_data_quality_warning": close_warning,
                "terminal_kind": artifact["terminal_kind"],
                "terminal_reason": artifact["terminal_reason"],
            })

    samples.sort(
        key=lambda sample: (
            not sample["long_lived"],
            -int(sample["selected_to_terminal_ms"] or 0),
            -int(sample["close_created_to_terminal_ms"] or 0),
            sample["entry_id"],
        )
    )
    return {
        "artifact_count": len(artifacts),
        "long_lived_pending_entry_count": long_lived_pending_entry_count,
        "close_data_quality_warning_count": close_data_quality_warning_count,
        "max_selected_to_terminal_ms": max_selected_to_terminal_ms,
        "max_pending_created_to_terminal_ms": max_pending_created_to_terminal_ms,
        "max_entered_to_terminal_ms": max_entered_to_terminal_ms,
        "max_semantic_entry_to_terminal_ms": max_semantic_entry_to_terminal_ms,
        "max_close_created_to_terminal_ms": max_close_created_to_terminal_ms,
        "samples": samples[:12],
    }


def _event_ts_ms(rec: dict[str, Any], payload: dict[str, Any]) -> int:
    return int(rec.get("ts_ms") or payload.get("ts_ms") or 0)


def _phase_artifact_id(payload: dict[str, Any]) -> str:
    return str(
        payload.get("entry_id")
        or payload.get("pending_id")
        or payload.get("position_id")
        or payload.get("internal_entry_id")
        or payload.get("candidate_id")
        or payload.get("candidate_pair_id")
        or payload.get("pair_id")
        or payload.get("recovery_id")
        or ""
    )


def _budget_defaults_payload(
    budgets: dict[str, LifecyclePhaseBudget],
) -> dict[str, dict[str, int]]:
    return {
        phase: {"soft_ms": budget.soft_ms, "hard_ms": budget.hard_ms}
        for phase, budget in sorted(budgets.items())
    }


def _build_phase_duration_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    budgets = phase_budgets_from_strategy()
    horizon_ms = 0
    artifacts: dict[str, dict[str, Any]] = {}
    # Candidate discovery is an observation before an entry has a durable
    # order/position identity.  Keep fallback pair-id observations in a
    # separate run-scoped map, otherwise the same pair in a later deployment
    # can overwrite an earlier deployment's lease policy in ``artifacts``.
    candidate_phase_artifacts: dict[str, dict[str, Any]] = {}
    active_candidate_phase_artifact_ids: dict[str, str] = {}
    quote_rewarm_scheduled: list[tuple[int, tuple[str, str], dict[str, Any]]] = []
    quote_rewarm_followups: list[tuple[int, str, tuple[str, str], dict[str, Any]]] = []
    # Logs written before the full-frontier fix have no policy marker and keep
    # their historical candidate-lease interpretation. New runtime starts mark
    # the old pre-evidence lease as retired, so repeated candidate revisions
    # are not misreported as a missing lifecycle handoff.
    candidate_discovery_lease_enforced = True
    candidate_policy_scope = "legacy"

    def artifact_for(artifact_id: str) -> dict[str, Any]:
        return artifacts.setdefault(
            artifact_id,
            {
                "artifact_id": artifact_id,
                "symbol": "",
                "venue": "",
                "selected_at_ms": 0,
                "submitted_at_ms": 0,
                "pending_created_at_ms": 0,
                "entry_terminal_at_ms": 0,
                "close_created_at_ms": 0,
                "close_terminal_at_ms": 0,
                "recovery_created_at_ms": 0,
                "recovery_terminal_at_ms": 0,
                "candidate_created_at_ms": 0,
                "candidate_terminal_at_ms": 0,
                "candidate_discovery_lease_enforced": True,
                "maker_resting": False,
                "observed_actions": {},
            },
        )

    def candidate_phase_artifact_for(
        artifact_id: str,
        *,
        base_artifact_id: str,
    ) -> dict[str, Any]:
        return candidate_phase_artifacts.setdefault(
            artifact_id,
            {
                # Keep the public diagnostic id familiar while the internal
                # map key adds the run scope needed for fallback pair ids.
                "artifact_id": base_artifact_id,
                "symbol": "",
                "venue": "",
                "candidate_created_at_ms": 0,
                "candidate_terminal_at_ms": 0,
                "candidate_discovery_lease_enforced": True,
                "observed_actions": {},
            },
        )

    def has_stable_candidate_artifact_id(payload: dict[str, Any]) -> bool:
        """Return whether the normal artifact id is unique across runs."""
        return any(
            payload.get(field)
            for field in (
                "entry_id",
                "pending_id",
                "position_id",
                "internal_entry_id",
                "candidate_id",
                "recovery_id",
            )
        )

    def candidate_phase_artifact(
        artifact: dict[str, Any],
        *,
        base_artifact_id: str,
        payload: dict[str, Any],
        create: bool,
    ) -> dict[str, Any] | None:
        """Resolve the candidate lifecycle without joining fallback pairs across runs."""
        if has_stable_candidate_artifact_id(payload):
            candidate_phase_artifacts.setdefault(
                f"stable_candidate_phase:{base_artifact_id}",
                artifact,
            )
            return artifact
        if create:
            scoped_id = f"candidate_phase:{candidate_policy_scope}:{base_artifact_id}"
            active_candidate_phase_artifact_ids[base_artifact_id] = scoped_id
        else:
            scoped_id = active_candidate_phase_artifact_ids.get(base_artifact_id)
        if scoped_id is None:
            return None
        return candidate_phase_artifact_for(
            scoped_id,
            base_artifact_id=base_artifact_id,
        )

    def note_observed_action(
        artifact: dict[str, Any],
        phase: str,
        kind: str,
    ) -> None:
        if phase not in budgets:
            return
        observed_actions = artifact.setdefault("observed_actions", {})
        if phase in observed_actions:
            return
        observed_actions[phase] = {
            "action_taken": budgets[phase].action,
            "action_evidence_kind": kind,
        }

    def default_action_evidence_kind(phase: str) -> str:
        return {
            "selected_pre_submit": "runtime.entry_selected_submit_deadline_exceeded",
            "entry_selected_terminal": "pending_entry.long_lived_pending_entry",
            "pending_entry": "pending_entry.long_lived_pending_entry",
            "maker_resting": "passive_maintenance.cancel_issued",
            "close_terminal": "runtime.passive_close_deadline_fallback_armed",
            "recovery_terminal": "recovery.blocked",
        }.get(phase, "")

    entry_terminal_kinds = {
        "entry.aborted",
        "entry.opened",
        "entry.passive_unfilled",
        "pending_entry.removed_by_v1_lifecycle_closure",
        "pending_entry.terminalized_after_single_leg_recovery",
        "runtime.position_opened",
    }
    close_terminal_kinds = {
        "runtime.position_lifecycle_terminal",
        "exit.passive_close_resolved",
        "exit.reconciled",
        "exit.closed",
    }
    submit_or_order_kinds = {
        "runtime.entry_dispatched",
        "runtime.pending_entry_registered",
        "execution.entry_order_submitted",
        "order.submitted",
    }
    candidate_start_kinds = {
        "review.candidate_shortlisted",
    }
    candidate_terminal_kinds = {
        "execution.entry_selected",
        "review.candidate_rejected",
        "runtime.entry_blocked_gate",
        "runtime.candidate_lease_expired",
        "runtime.candidate_symbol_skipped",
    }
    quote_rewarm_terminal_kinds = {
        "runtime.entry_quote_revalidate_resolved",
        "runtime.entry_quote_revalidate_failed",
        "runtime.entry_ws_bbo_top_candidate_rewarm_succeeded",
        "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
        "runtime.entry_quote_rewarm_terminal_stale",
    }
    recovery_start_kinds = {
        "recovery.blocked",
        "recovery.live_detected",
        "recovery.mismatch_detected",
        "recovery.required_position_truth_unavailable",
        "execution.residual_repair_queued",
        "exit.passive_close_residual_detected",
    }
    recovery_terminal_kinds = {
        "recovery.flat",
        "recovery.mismatch_flattened",
        "recovery.residual_repairs_complete",
        "execution.residual_repair_completed",
        "execution.residual_repair_terminal",
    }

    for rec in events:
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        ts_ms = _event_ts_ms(rec, payload)
        horizon_ms = max(horizon_ms, ts_ms)
        kind = str(rec.get("kind") or "")
        if kind == "startup.strategy_entry_policy":
            configured = payload.get("candidate_discovery_lease_enforced")
            if isinstance(configured, bool):
                candidate_discovery_lease_enforced = configured
                candidate_policy_scope = str(
                    payload.get("run_id") or f"startup:{ts_ms}"
                )
        venue_symbol_key = _event_venue_symbol_key(payload)
        if (
            kind == "runtime.entry_quote_rewarm_scheduled_after_rest_stale"
            and venue_symbol_key[0]
            and venue_symbol_key[1]
        ):
            quote_rewarm_scheduled.append((ts_ms, venue_symbol_key, payload))
        elif (
            kind in quote_rewarm_terminal_kinds
            and venue_symbol_key[0]
            and venue_symbol_key[1]
        ):
            quote_rewarm_followups.append((ts_ms, kind, venue_symbol_key, payload))
        artifact_id = _phase_artifact_id(payload)
        if not artifact_id:
            continue
        artifact = artifact_for(artifact_id)
        symbol = str(payload.get("symbol") or "").upper()
        if symbol and not artifact["symbol"]:
            artifact["symbol"] = symbol
        venue = str(
            payload.get("venue")
            or payload.get("maker_venue")
            or payload.get("hedge_venue")
            or ""
        ).lower()
        if venue and not artifact["venue"]:
            artifact["venue"] = venue

        if kind == "execution.entry_selected":
            artifact["selected_at_ms"] = ts_ms
        elif kind in submit_or_order_kinds:
            current = int(artifact["submitted_at_ms"] or 0)
            artifact["submitted_at_ms"] = min(current or ts_ms, ts_ms)
        if kind in candidate_start_kinds:
            candidate_artifact = candidate_phase_artifact(
                artifact,
                base_artifact_id=artifact_id,
                payload=payload,
                create=True,
            )
            assert candidate_artifact is not None
            if symbol and not candidate_artifact["symbol"]:
                candidate_artifact["symbol"] = symbol
            if venue and not candidate_artifact["venue"]:
                candidate_artifact["venue"] = venue
            current = int(candidate_artifact["candidate_created_at_ms"] or 0)
            candidate_artifact["candidate_created_at_ms"] = min(
                current or ts_ms,
                ts_ms,
            )
            candidate_artifact["candidate_discovery_lease_enforced"] = (
                candidate_discovery_lease_enforced
            )
        elif (
            kind in candidate_terminal_kinds
        ):
            candidate_artifact = candidate_phase_artifact(
                artifact,
                base_artifact_id=artifact_id,
                payload=payload,
                create=False,
            )
            if (
                candidate_artifact is not None
                and int(candidate_artifact["candidate_created_at_ms"] or 0)
            ):
                current = int(candidate_artifact["candidate_terminal_at_ms"] or 0)
                candidate_artifact["candidate_terminal_at_ms"] = min(
                    current or ts_ms,
                    ts_ms,
                )
        if kind == "runtime.pending_entry_registered":
            artifact["pending_created_at_ms"] = ts_ms
            outcome = str(payload.get("outcome") or "")
            if outcome == "maker_resting" or payload.get("maker_order_id"):
                artifact["maker_resting"] = True
        elif kind == "pending_entry.long_lived_pending_entry":
            artifact["maker_resting"] = True
        elif kind == "runtime.entry_selected_submit_deadline_exceeded":
            current = int(artifact["entry_terminal_at_ms"] or 0)
            artifact["entry_terminal_at_ms"] = min(current or ts_ms, ts_ms)
        elif kind in entry_terminal_kinds:
            current = int(artifact["entry_terminal_at_ms"] or 0)
            artifact["entry_terminal_at_ms"] = min(current or ts_ms, ts_ms)
        elif kind == "exit.passive_close_created":
            artifact["close_created_at_ms"] = ts_ms
        elif kind in close_terminal_kinds:
            current = int(artifact["close_terminal_at_ms"] or 0)
            artifact["close_terminal_at_ms"] = min(current or ts_ms, ts_ms)
        elif kind in recovery_start_kinds:
            current = int(artifact["recovery_created_at_ms"] or 0)
            artifact["recovery_created_at_ms"] = min(current or ts_ms, ts_ms)
        elif kind in recovery_terminal_kinds:
            current = int(artifact["recovery_terminal_at_ms"] or 0)
            artifact["recovery_terminal_at_ms"] = min(current or ts_ms, ts_ms)

        if kind == "pending_entry.long_lived_pending_entry":
            note_observed_action(artifact, "entry_selected_terminal", kind)
            note_observed_action(artifact, "pending_entry", kind)
            note_observed_action(artifact, "maker_resting", kind)
        if kind == "runtime.entry_selected_submit_deadline_exceeded":
            note_observed_action(artifact, "selected_pre_submit", kind)
        if kind == "runtime.candidate_lease_expired":
            candidate_artifact = candidate_phase_artifact(
                artifact,
                base_artifact_id=artifact_id,
                payload=payload,
                create=False,
            )
            if candidate_artifact is not None:
                note_observed_action(candidate_artifact, "candidate_lease", kind)
        if kind == "runtime.candidate_symbol_skipped":
            candidate_artifact = candidate_phase_artifact(
                artifact,
                base_artifact_id=artifact_id,
                payload=payload,
                create=False,
            )
            if candidate_artifact is not None:
                note_observed_action(candidate_artifact, "candidate_lease", kind)
        if kind in {
            "passive_maintenance.cancel_rest_timeout",
            "passive_maintenance.cancel_try_window",
            "passive_maintenance.cancel_issued",
        }:
            note_observed_action(artifact, "maker_resting", kind)
        if kind == "runtime.passive_close_deadline_fallback_armed":
            note_observed_action(artifact, "close_terminal", kind)
        if kind in {
            "recovery.blocked",
            "execution.residual_repair_paused",
            "execution.residual_repair_terminal",
            "execution.residual_repair_completed",
            "recovery.residual_repairs_complete",
        }:
            note_observed_action(artifact, "recovery_terminal", kind)

    records: list[dict[str, Any]] = []
    terminalized_record_ids: set[int] = set()

    def add_record(
        artifact: dict[str, Any],
        phase: str,
        start_ms: int,
        end_ms: int,
    ) -> None:
        if start_ms <= 0:
            return
        budget = budgets[phase]
        age_ms = max(0, (end_ms or horizon_ms) - start_ms)
        status = classify_phase_age(age_ms, budget)
        observed_action = (
            artifact.get("observed_actions", {}).get(phase, {})
            if isinstance(artifact.get("observed_actions", {}), dict)
            else {}
        )
        action_taken = str(observed_action.get("action_taken") or "")
        action_evidence_kind = str(observed_action.get("action_evidence_kind") or "")
        handoff = quote_rewarm_handoff_contract(
            phase=phase,
            status=status,
            configured_action=budget.action,
            terminal_kind=action_evidence_kind if action_taken else "",
        )
        if handoff and not action_taken:
            action_taken = handoff["action_taken"]
            action_evidence_kind = handoff["action_evidence_kind"]
        allow_configured_fallback = phase not in {"candidate_lease", "quote_rewarm"}
        if (
            not action_taken
            and allow_configured_fallback
            and status in {"hard_over_budget", "soft_over_budget"}
        ):
            action_taken = budget.action
            if status == "soft_over_budget":
                action_evidence_kind = f"runtime.{phase}_soft_budget_exceeded"
            else:
                action_evidence_kind = default_action_evidence_kind(phase)
        record = {
            "phase": phase,
            "artifact_id": str(artifact["artifact_id"]),
            "symbol": str(artifact["symbol"]),
            "venue": str(artifact["venue"]),
            "age_ms": age_ms,
            "soft_ms": budget.soft_ms,
            "hard_ms": budget.hard_ms,
            "status": status,
            "configured_action": budget.action,
            "action_taken": action_taken,
            "action_evidence_kind": action_evidence_kind,
            "truth_source": budget.truth_source,
        }
        records.append(record)
        if end_ms > 0:
            terminalized_record_ids.add(id(record))

    for artifact in artifacts.values():
        selected_at_ms = int(artifact["selected_at_ms"] or 0)
        submitted_at_ms = int(artifact["submitted_at_ms"] or 0)
        pending_created_at_ms = int(artifact["pending_created_at_ms"] or 0)
        entry_terminal_at_ms = int(artifact["entry_terminal_at_ms"] or 0)
        close_created_at_ms = int(artifact["close_created_at_ms"] or 0)
        close_terminal_at_ms = int(artifact["close_terminal_at_ms"] or 0)
        recovery_created_at_ms = int(artifact["recovery_created_at_ms"] or 0)
        recovery_terminal_at_ms = int(artifact["recovery_terminal_at_ms"] or 0)
        selected_pre_submit_end_ms = min(
            (
                value
                for value in [
                    submitted_at_ms,
                    pending_created_at_ms,
                    entry_terminal_at_ms,
                ]
                if value > 0
            ),
            default=0,
        )
        add_record(
            artifact,
            "selected_pre_submit",
            selected_at_ms,
            selected_pre_submit_end_ms,
        )
        add_record(
            artifact,
            "entry_selected_terminal",
            selected_at_ms,
            entry_terminal_at_ms,
        )
        add_record(
            artifact,
            "pending_entry",
            pending_created_at_ms,
            entry_terminal_at_ms,
        )
        if bool(artifact["maker_resting"]):
            add_record(
                artifact,
                "maker_resting",
                pending_created_at_ms,
                entry_terminal_at_ms,
            )
        add_record(
            artifact,
            "close_terminal",
            close_created_at_ms,
            close_terminal_at_ms,
        )
        add_record(
            artifact,
            "recovery_terminal",
            recovery_created_at_ms,
            recovery_terminal_at_ms,
        )

    for artifact in candidate_phase_artifacts.values():
        if bool(artifact.get("candidate_discovery_lease_enforced", True)):
            add_record(
                artifact,
                "candidate_lease",
                int(artifact["candidate_created_at_ms"] or 0),
                int(artifact["candidate_terminal_at_ms"] or 0),
            )

    for scheduled_at_ms, key, _payload in quote_rewarm_scheduled:
        terminal_at_ms = 0
        terminal_kind = ""
        for ts_ms, _kind, event_key, _followup_payload in quote_rewarm_followups:
            if event_key == key and ts_ms >= scheduled_at_ms:
                terminal_at_ms = ts_ms
                terminal_kind = _kind
                break
        artifact = {
            "artifact_id": f"quote_rewarm:{key[0]}:{key[1]}:{scheduled_at_ms}",
            "symbol": key[1],
            "venue": key[0],
        }
        if terminal_kind:
            artifact["observed_actions"] = {
                "quote_rewarm": {
                    "action_taken": budgets["quote_rewarm"].action,
                    "action_evidence_kind": terminal_kind,
                }
            }
        add_record(
            artifact,
            "quote_rewarm",
            scheduled_at_ms,
            terminal_at_ms,
        )

    over_budget_records = [
        record for record in records if record["status"] != "ok"
    ]
    hard_over_budget_records = [
        record for record in records if record["status"] == "hard_over_budget"
    ]

    def is_terminalized_record(record: dict[str, Any]) -> bool:
        return id(record) in terminalized_record_ids

    def unique_artifact_ids(items: list[dict[str, Any]]) -> set[str]:
        seen: set[str] = set()
        for item in items:
            artifact_id = str(item.get("artifact_id") or "")
            if artifact_id:
                seen.add(artifact_id)
        return seen

    def unique_artifact_samples(
        items: list[dict[str, Any]],
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        seen: set[str] = set()
        result: list[dict[str, Any]] = []
        for item in items:
            artifact_id = str(item.get("artifact_id") or "")
            if not artifact_id or artifact_id in seen:
                continue
            seen.add(artifact_id)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    current_hard_over_budget_records = [
        record for record in hard_over_budget_records
        if not is_terminalized_record(record)
    ]
    historical_terminalized_hard_over_budget_records = [
        record for record in hard_over_budget_records
        if is_terminalized_record(record)
    ]
    current_hard_over_budget_samples = unique_artifact_samples(
        current_hard_over_budget_records
    )
    historical_terminalized_hard_over_budget_samples = unique_artifact_samples(
        historical_terminalized_hard_over_budget_records
    )
    current_hard_over_budget_count = len(
        unique_artifact_ids(current_hard_over_budget_records)
    )
    historical_terminalized_hard_over_budget_count = len(
        unique_artifact_ids(historical_terminalized_hard_over_budget_records)
    )
    blank_action_count = sum(
        1
        for record in over_budget_records
        if record.get("phase") != "candidate_lease"
        and not str(record.get("action_taken") or "")
    )
    terminalized_candidate_lease_count = sum(
        1
        for record in over_budget_records
        if record.get("phase") == "candidate_lease"
        and str(record.get("action_taken") or "")
    )
    terminalized_quote_rewarm_count = sum(
        1
        for record in over_budget_records
        if record.get("phase") == "quote_rewarm"
        and str(record.get("action_taken") or "")
    )
    hard_terminalized_candidate_lease_count = sum(
        1
        for record in hard_over_budget_records
        if record.get("phase") == "candidate_lease"
        and str(record.get("action_taken") or "")
    )
    hard_terminalized_quote_rewarm_count = sum(
        1
        for record in hard_over_budget_records
        if record.get("phase") == "quote_rewarm"
        and str(record.get("action_taken") or "")
    )
    max_age_by_phase: dict[str, int] = {}
    for record in records:
        phase = str(record["phase"])
        max_age_by_phase[phase] = max(
            max_age_by_phase.get(phase, 0), int(record["age_ms"] or 0)
        )

    over_budget_records.sort(
        key=lambda record: (
            0 if record["status"] == "hard_over_budget" else 1,
            -int(record["age_ms"] or 0),
            str(record["phase"]),
            str(record["artifact_id"]),
        )
    )
    handoff_phases = ("candidate_lease", "quote_rewarm")
    phase_counts = {
        phase: {
            "over_budget_count": 0,
            "takeover_count": 0,
            "missing_takeover_count": 0,
        }
        for phase in handoff_phases
    }
    missing_handoff_samples: list[dict[str, Any]] = []
    for record in over_budget_records:
        phase = str(record.get("phase") or "")
        if phase not in phase_counts:
            continue
        phase_counts[phase]["over_budget_count"] += 1
        if str(record.get("action_taken") or ""):
            phase_counts[phase]["takeover_count"] += 1
            continue
        phase_counts[phase]["missing_takeover_count"] += 1
        if len(missing_handoff_samples) < 12:
            missing_handoff_samples.append(
                {
                    "phase": phase,
                    "artifact_id": str(record.get("artifact_id") or ""),
                    "symbol": str(record.get("symbol") or ""),
                    "venue": str(record.get("venue") or ""),
                    "age_ms": int(record.get("age_ms") or 0),
                    "hard_ms": int(record.get("hard_ms") or 0),
                    "configured_action": str(record.get("configured_action") or ""),
                    "truth_source": str(record.get("truth_source") or ""),
                }
            )
    missing_handoff_count = sum(
        counts["missing_takeover_count"] for counts in phase_counts.values()
    )
    return {
        "budget_defaults_ms": _budget_defaults_payload(budgets),
        "artifact_count": len(artifacts),
        "phase_record_count": len(records),
        "over_budget_count": len(over_budget_records),
        "hard_over_budget_count": len(hard_over_budget_records),
        "current_hard_over_budget_count": current_hard_over_budget_count,
        "historical_terminalized_hard_over_budget_count": (
            historical_terminalized_hard_over_budget_count
        ),
        "current_hard_over_budget_samples": current_hard_over_budget_samples,
        "historical_terminalized_hard_over_budget_samples": (
            historical_terminalized_hard_over_budget_samples
        ),
        "blank_action_count": blank_action_count,
        "terminalized_candidate_lease_count": terminalized_candidate_lease_count,
        "terminalized_quote_rewarm_count": terminalized_quote_rewarm_count,
        "hard_terminalized_candidate_lease_count": hard_terminalized_candidate_lease_count,
        "hard_terminalized_quote_rewarm_count": hard_terminalized_quote_rewarm_count,
        "phase_handoff_quality": {
            "severity": "production_issue" if missing_handoff_count else "ok",
            "missing_takeover_count": missing_handoff_count,
            "phase_counts": phase_counts,
            "samples": missing_handoff_samples,
        },
        "max_age_by_phase": dict(sorted(max_age_by_phase.items())),
        "samples": over_budget_records[:24],
    }


def _build_duplicate_close_leg_suppressed_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    total = 0
    positions: set[str] = set()
    samples: list[dict[str, Any]] = []
    for rec in events:
        if str(rec.get("kind") or "") != "exit.reconciled":
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        count = int(payload.get("duplicate_close_leg_suppressed_count") or 0)
        if count <= 0:
            continue
        total += count
        position_id = str(payload.get("position_id") or "")
        if position_id:
            positions.add(position_id)
        for sample in payload.get("duplicate_close_leg_suppressed_samples") or []:
            if not isinstance(sample, dict):
                continue
            if len(samples) >= 12:
                break
            samples.append({
                "position_id": position_id,
                "symbol": str(payload.get("symbol") or ""),
                "leg": str(sample.get("leg") or ""),
                "venue": str(sample.get("venue") or ""),
                "order_id": str(sample.get("order_id") or ""),
                "client_order_id": str(sample.get("client_order_id") or ""),
                "quantity": _safe_float(sample.get("quantity")),
                "average_price": _safe_float(sample.get("average_price")),
                "filled_at_ms": int(sample.get("filled_at_ms") or 0),
            })
    return {
        "duplicate_close_leg_suppressed_count": total,
        "position_ids": sorted(positions),
        "samples": samples,
    }


def _build_entry_admission_cooldown_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    def optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    def first_value(rows: list[dict[str, Any]], key: str) -> Any:
        for row in rows:
            value = row.get(key)
            if value is not None and value != "":
                return value
        return None

    def normalized_rollup_payload(payload: dict[str, Any]) -> dict[str, Any]:
        samples_raw = payload.get("samples") or []
        sample_rows = [
            item for item in samples_raw
            if isinstance(item, dict)
        ]
        normalized = dict(payload)
        venue = str(normalized.get("venue") or first_value(sample_rows, "venue") or "").lower()
        block_scope = str(
            normalized.get("cooldown_scope")
            or normalized.get("block_scope")
            or ""
        ).lower()
        root_symbol = str(
            normalized.get("blocked_symbol")
            or (
                first_value(sample_rows, "blocked_symbol")
                if block_scope == "venue"
                else None
            )
            or normalized.get("symbol")
            or first_value(sample_rows, "symbol")
            or ""
        ).upper()
        normalized["venue"] = venue
        normalized["symbol"] = root_symbol
        normalized["blocked_symbol"] = root_symbol
        affected_candidates = sorted({
            str(item.get("symbol") or "").upper()
            for item in sample_rows
            if str(item.get("symbol") or "").upper()
            and str(item.get("symbol") or "").upper() != root_symbol
        })
        if affected_candidates:
            normalized["affected_candidates"] = affected_candidates
        for key in (
            "requested_notional",
            "remaining_openable_notional",
            "remaining_openable_endpoint_value",
            "notional_gap",
            "leverage",
            "headroom_source",
            "headroom_error",
            "headroom_truth_source",
            "required_margin_estimate",
            "available_balance_quote",
            "position_flat",
            "open_orders_empty",
            "account_margin_sufficient",
            "account_truth_submit_allowed",
        ):
            if normalized.get(key) is None:
                value = first_value(sample_rows, key)
                if value is not None:
                    normalized[key] = value
        requested = optional_float(normalized.get("requested_notional"))
        remaining = optional_float(normalized.get("remaining_openable_notional"))
        if normalized.get("notional_gap") is None and requested is not None and remaining is not None:
            normalized["notional_gap"] = max(requested - remaining, 0.0)
        return normalized

    reason_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}
    venue_symbol_counts: dict[str, int] = {}
    advice_counts: dict[str, int] = {}
    symbol_rollups: dict[str, dict[str, Any]] = {}
    samples: list[dict[str, Any]] = []
    total = 0
    for rec in events:
        kind = str(rec.get("kind") or "")
        if kind not in {
            "runtime.entry_admission_blocked",
            "runtime.entry_admission_headroom_advisory",
            "runtime.entry_admission_venue_degraded",
            "runtime.venue_cooldown_started",
        }:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        rollup_payload = normalized_rollup_payload(payload)
        total += 1
        reason = str(payload.get("reason") or "unknown")
        source = str(payload.get("source") or "unknown")
        scope = str(
            payload.get("cooldown_scope")
            or payload.get("block_scope")
            or "unknown"
        )
        venue = str(rollup_payload.get("venue") or "").lower()
        symbol = str(
            rollup_payload.get("symbol")
            or rollup_payload.get("blocked_symbol")
            or ""
        ).upper()
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        source_counts[source] = source_counts.get(source, 0) + 1
        scope_counts[scope] = scope_counts.get(scope, 0) + 1
        if venue or symbol:
            key = f"{venue}:{symbol}" if symbol else f"{venue}:*"
            venue_symbol_counts[key] = venue_symbol_counts.get(key, 0) + 1
            rollup = symbol_rollups.setdefault(
                key,
                {
                    "venue": venue,
                    "symbol": symbol,
                    "count": 0,
                    "requested_notional": 0.0,
                    "remaining_openable_notional": None,
                    "notional_gap": 0.0,
                    "reasons": {},
                },
            )
            rollup["count"] += 1
            rollup["reasons"][reason] = rollup["reasons"].get(reason, 0) + 1
            requested_notional = optional_float(rollup_payload.get("requested_notional"))
            remaining_notional = optional_float(
                rollup_payload.get("remaining_openable_notional")
            )
            notional_gap = optional_float(rollup_payload.get("notional_gap"))
            if requested_notional is not None:
                rollup["requested_notional"] += requested_notional
            if remaining_notional is not None:
                current_remaining = rollup.get("remaining_openable_notional")
                rollup["remaining_openable_notional"] = (
                    remaining_notional
                    if current_remaining is None
                    else min(float(current_remaining), remaining_notional)
                )
            if notional_gap is None and requested_notional is not None and remaining_notional is not None:
                notional_gap = max(requested_notional - remaining_notional, 0.0)
            if notional_gap is not None:
                rollup["notional_gap"] += notional_gap
            leverage = rollup_payload.get("leverage")
            if leverage is not None and rollup.get("leverage") is None:
                rollup["leverage"] = leverage
            headroom_source = str(rollup_payload.get("headroom_source") or "")
            if headroom_source and not rollup.get("headroom_source"):
                rollup["headroom_source"] = headroom_source
            headroom_error = str(rollup_payload.get("headroom_error") or "")
            if headroom_error and not rollup.get("headroom_error"):
                rollup["headroom_error"] = headroom_error
            for key in (
                "remaining_openable_endpoint_value",
                "headroom_truth_source",
                "required_margin_estimate",
                "available_balance_quote",
                "position_flat",
                "open_orders_empty",
                "account_margin_sufficient",
                "account_truth_submit_allowed",
            ):
                value = rollup_payload.get(key)
                if value is not None and rollup.get(key) is None:
                    rollup[key] = value
            affected_candidates = rollup_payload.get("affected_candidates") or []
            if affected_candidates:
                existing = set(rollup.get("affected_candidates") or [])
                existing.update(str(item) for item in affected_candidates if str(item))
                rollup["affected_candidates"] = sorted(existing)
        if venue == "aster" and reason == "max_notional_admission_blocked":
            advice = "aster_real_exchange_max_notional_reject_symbol_scoped"
            advice_counts[advice] = advice_counts.get(advice, 0) + 1
        if venue == "aster" and reason == "aster_headroom_advisory_zero":
            advice = "aster_endpoint_zero_advisory_account_truth_submit_allowed"
            advice_counts[advice] = advice_counts.get(advice, 0) + 1
        if len(samples) < 12:
            samples.append({
                "ts_ms": rec.get("ts_ms", 0),
                "kind": kind,
                "venue": venue,
                "symbol": symbol,
                "reason": reason,
                "source": source,
                "cooldown_scope": scope,
                "blocked_until_ms": int(payload.get("blocked_until_ms") or 0),
                "evidence_gap": payload.get("evidence_gap"),
                "requested_notional": rollup_payload.get("requested_notional"),
                "remaining_openable_notional": rollup_payload.get("remaining_openable_notional"),
                "remaining_openable_endpoint_value": rollup_payload.get("remaining_openable_endpoint_value"),
                "notional_gap": rollup_payload.get("notional_gap"),
                "leverage": rollup_payload.get("leverage"),
                "required_margin_estimate": rollup_payload.get("required_margin_estimate"),
                "available_balance_quote": rollup_payload.get("available_balance_quote"),
                "headroom_truth_source": rollup_payload.get("headroom_truth_source"),
            })
    top_blocked_symbols = sorted(
        symbol_rollups.values(),
        key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("venue") or ""), str(item.get("symbol") or "")),
    )[:12]
    return {
        "count": total,
        "reason_counts": dict(sorted(reason_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "scope_counts": dict(sorted(scope_counts.items())),
        "venue_symbol_counts": dict(sorted(venue_symbol_counts.items())),
        "top_blocked_symbols": top_blocked_symbols,
        "advice_counts": dict(sorted(advice_counts.items())),
        "samples": samples,
    }


def _build_venue_private_health_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    venue_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []
    event_count = 0
    incident_keys: set[tuple[str, ...]] = set()
    for rec in events:
        kind = str(rec.get("kind") or "")
        if kind not in {
            "runtime.entry_admission_blocked",
            "runtime.venue_cooldown_started",
            "cleanup_blocked_by_venue_auth_invalid",
        }:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        reason = str(payload.get("reason") or "")
        status = str(
            payload.get("venue_private_health_status")
            or private_health_status_for_admission_reason(reason)
            or ""
        )
        if not status and "33004" in str(payload.get("raw_error") or ""):
            status = "auth_invalid"
        if not status:
            continue
        event_count += 1
        venue = str(payload.get("venue") or "").lower()
        reason = reason or "venue_private_health_degraded"
        symbol = str(
            payload.get("symbol")
            or payload.get("blocked_symbol")
            or ""
        ).upper()
        incident_id = str(
            payload.get("candidate_pair_id")
            or payload.get("pair_id")
            or payload.get("entry_id")
            or payload.get("position_id")
            or ""
        )
        incident_key = (
            incident_id,
            venue,
            symbol,
            reason,
            str(payload.get("source") or ""),
            str(payload.get("blocked_until_ms") or ""),
            str(rec.get("ts_ms", 0) or ""),
        )
        if incident_key not in incident_keys:
            incident_keys.add(incident_key)
            status_counts[status] = status_counts.get(status, 0) + 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if venue:
                venue_counts[venue] = venue_counts.get(venue, 0) + 1
        if len(samples) < 12:
            samples.append(
                {
                    "ts_ms": rec.get("ts_ms", 0),
                    "kind": kind,
                    "venue": venue,
                    "symbol": symbol,
                    "reason": reason,
                    "status": status,
                    "source": str(payload.get("source") or "")[:120],
                    "cooldown_scope": str(
                        payload.get("cooldown_scope")
                        or payload.get("block_scope")
                        or ""
                    ),
                    "reduce_only": payload.get("reduce_only"),
                    "action": str(payload.get("action") or ""),
                }
            )
    return {
        "count": len(incident_keys),
        "event_count": event_count,
        "status_counts": dict(sorted(status_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "venue_counts": dict(sorted(venue_counts.items())),
        "samples": samples,
    }


def _build_single_leg_exposure_recovery_summary(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    kind_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    venue_counts: dict[str, int] = {}
    entry_ids: set[str] = set()
    terminal_entry_ids: set[str] = set()
    recovery_entry_ids: set[str] = set()
    samples: list[dict[str, Any]] = []
    target_kinds = {
        "pending_entry.release_maker_cancel_failed",
        "pending_entry.release_maker_cancel_requested",
        "pending_entry.release_maker_open_order_truth_unavailable",
        "pending_entry.release_maker_order_reference_unavailable",
        "pending_entry.release_maker_order_truth_unavailable",
        "pending_entry.release_maker_positive_fill_truth_retained",
        "pending_entry.release_maker_terminal_no_open_order",
        "pending_entry.release_retained_maker_open_order",
        "pending_entry.single_leg_exposure_recovery_started",
        "pending_entry.single_leg_flatten_submitted",
        "pending_entry.single_leg_flatten_succeeded",
        "pending_entry.single_leg_flatten_failed",
        "pending_entry.terminalized_after_single_leg_recovery",
    }
    for rec in events:
        kind = str(rec.get("kind") or "")
        if kind not in target_kinds:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
        reason = str(payload.get("reason") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        venue = str(
            payload.get("cleanup_venue")
            or payload.get("venue")
            or ""
        ).lower()
        if venue:
            venue_counts[venue] = venue_counts.get(venue, 0) + 1
        entry_id = str(payload.get("entry_id") or "")
        if entry_id:
            entry_ids.add(entry_id)
            if kind == "pending_entry.terminalized_after_single_leg_recovery":
                terminal_entry_ids.add(entry_id)
            elif kind in {
                "pending_entry.release_maker_cancel_failed",
                "pending_entry.release_maker_cancel_requested",
                "pending_entry.release_maker_open_order_truth_unavailable",
                "pending_entry.release_maker_order_reference_unavailable",
                "pending_entry.release_maker_order_truth_unavailable",
                "pending_entry.release_maker_positive_fill_truth_retained",
                "pending_entry.release_maker_terminal_no_open_order",
                "pending_entry.release_retained_maker_open_order",
                "pending_entry.single_leg_exposure_recovery_started",
                "pending_entry.single_leg_flatten_submitted",
                "pending_entry.single_leg_flatten_succeeded",
                "pending_entry.single_leg_flatten_failed",
            }:
                recovery_entry_ids.add(entry_id)
        if len(samples) < 12:
            samples.append(
                {
                    "ts_ms": rec.get("ts_ms", 0),
                    "kind": kind,
                    "entry_id": entry_id,
                    "symbol": str(payload.get("symbol") or "").upper(),
                    "venue": venue,
                    "failed_hedge_venue": str(
                        payload.get("failed_hedge_venue") or ""
                    ).lower(),
                    "reason": reason,
                }
            )
    failed = int(kind_counts.get("pending_entry.single_leg_flatten_failed", 0) or 0)
    terminalized = int(
        kind_counts.get("pending_entry.terminalized_after_single_leg_recovery", 0)
        or 0
    )
    unresolved_entry_ids = sorted(recovery_entry_ids - terminal_entry_ids)
    return {
        "count": sum(kind_counts.values()),
        "entry_count": len(entry_ids),
        "started_count": int(
            kind_counts.get("pending_entry.single_leg_exposure_recovery_started", 0)
            or 0
        ),
        "submitted_count": int(
            kind_counts.get("pending_entry.single_leg_flatten_submitted", 0) or 0
        ),
        "succeeded_count": int(
            kind_counts.get("pending_entry.single_leg_flatten_succeeded", 0) or 0
        ),
        "failed_count": failed,
        "terminalized_count": terminalized,
        "unresolved_count": len(unresolved_entry_ids),
        "unresolved_entry_ids": unresolved_entry_ids[:50],
        "kind_counts": dict(sorted(kind_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "venue_counts": dict(sorted(venue_counts.items())),
        "entry_ids": sorted(entry_ids)[:50],
        "samples": samples,
    }


def _build_business_progression_quality_summary(
    events: list[dict[str, Any]],
    production_acceptance_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    phase_duration_summary = _build_phase_duration_summary(events)
    phase_handoff_quality = phase_duration_summary.get(
        "phase_handoff_quality",
        {
            "severity": "ok",
            "missing_takeover_count": 0,
            "phase_counts": {},
            "samples": [],
        },
    )
    pre_submit_blocked = 0
    single_leg_created = 0
    single_leg_cleanup = 0
    recovered_but_counted_entries: set[str] = set()
    zero_fill_cancel_entries: set[str] = set()
    admission_blocked_entries: set[str] = set()
    cleanup_entries: set[str] = set()
    active_cooldowns: dict[tuple[str, str], dict[str, Any]] = {}
    entry_routes: dict[str, dict[str, str]] = {}
    repeated_submit_samples: list[dict[str, Any]] = []
    ownerless_open_order_count = 0
    owned_pending_passive_close_count = 0
    adopted_reduce_only_order_count = 0
    duplicate_reduce_only_submit_blocked_count = 0
    deterministic_reject_after_submit_count = 0
    entry_quantity_contract_blocked_count = 0
    close_reconciliation_evidence_gap_count = 0
    cleanup_release_truth_blocked_count = 0
    cleanup_release_truth_blocked_samples: list[dict[str, Any]] = []
    close_reconciliation_evidence_gap_summary = {
        "count": 0,
        "terminal_flat_accounting_gap_count": 0,
        "unresolved_close_accounting_gap_count": 0,
        "blocking_count": 0,
        "samples": [],
    }
    admission_degraded_suppressed_count = 0
    passive_close_resolved_without_terminal_truth_entries: set[str] = set()
    passive_close_terminal_truth_entries: set[str] = set()
    passive_close_actionable_single_leg_wait_count = 0
    passive_close_final_truth_actions: dict[str, int] = {}
    risk_only_live_single_leg_exposure_count = 0
    passive_close_active_recovery_pending_count = 0
    passive_close_identity_sanitized_count = 0
    passive_close_owned_ioc_handoff_count = 0
    passive_close_owned_ioc_blocked_count = 0
    close_cost_entries: dict[str, dict[str, Any]] = {}

    def close_cost_entry(payload: dict[str, Any]) -> dict[str, Any] | None:
        cost_position_id = entry_id(payload)
        if not cost_position_id:
            return None
        entry = close_cost_entries.setdefault(
            cost_position_id,
            {
                "position_id": cost_position_id,
                "symbol": str(payload.get("symbol") or ""),
                "maker_no_fill_count": 0,
                "zero_fill_cycle_count": 0,
                "fallback_after_zero_fill_count": 0,
                "maker_viability_rejected_count": 0,
                "early_fallback_after_viability_rejected_count": 0,
                "venues": set(),
                "maker_legs": set(),
                "maker_viability_reasons": set(),
                "terminal_resolved": False,
            },
        )
        if payload.get("symbol") and not entry.get("symbol"):
            entry["symbol"] = str(payload.get("symbol") or "")
        for key in ("venue", "maker_venue", "hedge_venue"):
            venue = venue_value(payload, key)
            if venue:
                entry["venues"].add(venue)
        maker_leg = str(payload.get("maker_leg") or payload.get("leg") or "")
        if maker_leg:
            entry["maker_legs"].add(maker_leg)
        return entry

    def gate_currently_green() -> bool:
        if not isinstance(production_acceptance_gate, dict):
            return False
        if production_acceptance_gate.get("gate_passed") is not True:
            return False
        if production_acceptance_gate.get("exchange_truth_flat") is not True:
            return False
        if production_acceptance_gate.get("exchange_truth_no_open_orders") is not True:
            return False
        if production_acceptance_gate.get("blocking_reasons"):
            return False
        lifecycle = production_acceptance_gate.get("v1_lifecycle_summary", {})
        if isinstance(lifecycle, dict):
            try:
                if int(lifecycle.get("blocking_row_count") or 0) > 0:
                    return False
            except (TypeError, ValueError):
                return False
        return True

    def current_exchange_truth_clean() -> bool:
        if not isinstance(production_acceptance_gate, dict):
            return False
        return (
            production_acceptance_gate.get("exchange_truth_flat") is True
            and production_acceptance_gate.get("exchange_truth_no_open_orders") is True
        )

    def event_ts(rec: dict[str, Any]) -> int:
        payload = rec.get("payload", {})
        payload_ts = payload.get("ts_ms") if isinstance(payload, dict) else 0
        return int(rec.get("ts_ms") or payload_ts or 0)

    def entry_id(payload: dict[str, Any]) -> str:
        return str(
            payload.get("entry_id")
            or payload.get("position_id")
            or payload.get("internal_entry_id")
            or ""
        )

    def cooldown_until(payload: dict[str, Any]) -> int:
        return int(
            payload.get("blocked_until_ms")
            or payload.get("cooldown_until_ms")
            or 0
        )

    def venue_value(payload: dict[str, Any], key: str) -> str:
        return str(payload.get(key) or "").lower()

    def passive_close_has_terminal_truth(payload: dict[str, Any]) -> bool:
        return contract_passive_close_has_terminal_truth(payload)

    def route_venues(payload: dict[str, Any]) -> list[str]:
        venues: list[str] = []
        for key in (
            "venue",
            "long_venue",
            "short_venue",
            "maker_venue",
            "hedge_venue",
        ):
            venue = venue_value(payload, key)
            if venue and venue not in venues:
                venues.append(venue)
        route = entry_routes.get(entry_id(payload), {})
        for key in ("long_venue", "short_venue", "maker_venue", "hedge_venue"):
            venue = str(route.get(key) or "").lower()
            if venue and venue not in venues:
                venues.append(venue)
        return venues

    archived_close_accounting_entries: set[str] = set()
    backfilled_close_accounting_entries: set[str] = set()
    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        close_entry_id = entry_id(payload)
        if not close_entry_id:
            continue
        if kind == "reconciliation.pending_close_backfill_archived":
            archived_close_accounting_entries.add(close_entry_id)
        elif kind in {
            "reconciliation.pending_close_backfill_completed",
            "reconciliation.pending_close_statement_backfilled",
        }:
            backfilled_close_accounting_entries.add(close_entry_id)

    for rec in sorted(events, key=event_ts):
        ts_ms = event_ts(rec)
        active_cooldowns = {
            key: value
            for key, value in active_cooldowns.items()
            if int(value.get("blocked_until_ms") or 0) > ts_ms
        }
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        issue_counts = diagnose_issue_counts(payload, kind)
        if (
            kind
            in {
                "recovery.unpaired_live_position_cleanup_skipped",
                "recovery.unpaired_live_position_cleanup_failed",
            }
            and payload.get("current_risk_exposure") is True
        ):
            risk_only_live_single_leg_exposure_count += 1
        entry_quantity_contract_blocked_count += int(
            issue_counts.get("entry_quantity_contract_blocked_count", 0) or 0
        )
        close_reconciliation_evidence_gap_count += int(
            issue_counts.get("close_reconciliation_evidence_gap_count", 0) or 0
        )
        if kind == "exit.reconciled" and payload.get("evidence_gap") is True:
            reconciliation_contract = close_reconciliation_evidence_contract(
                payload,
                current_exchange_truth_clean=current_exchange_truth_clean(),
            )
            if reconciliation_contract:
                action = str(reconciliation_contract.get("action") or "")
                close_reconciliation_evidence_gap_summary["count"] += 1
                if action == "terminal_flat_accounting_gap":
                    close_reconciliation_evidence_gap_summary[
                        "terminal_flat_accounting_gap_count"
                    ] += 1
                elif action == "unresolved_close_accounting_gap":
                    close_reconciliation_evidence_gap_summary[
                        "unresolved_close_accounting_gap_count"
                    ] += 1
                if reconciliation_contract.get("blocks_business_terminal") is True:
                    close_reconciliation_evidence_gap_summary[
                        "blocking_count"
                    ] += 1
                samples = close_reconciliation_evidence_gap_summary["samples"]
                if isinstance(samples, list) and len(samples) < 12:
                    terminal_flat_gap = action == "terminal_flat_accounting_gap"
                    reconciliation_owner_id = str(
                        reconciliation_contract.get("owner_id") or ""
                    )
                    if (
                        terminal_flat_gap
                        and reconciliation_owner_id
                        in backfilled_close_accounting_entries
                    ):
                        audit_status = "terminal_flat_backfilled"
                        next_action = "none_backfill_recorded"
                    elif (
                        terminal_flat_gap
                        and reconciliation_owner_id
                        in archived_close_accounting_entries
                    ):
                        audit_status = "terminal_flat_archived"
                        next_action = "none_archive_recorded"
                    else:
                        audit_status = (
                            "terminal_flat_backfill_required"
                            if terminal_flat_gap
                            else "blocking_reconciliation_required"
                        )
                        next_action = (
                            "backfill_trade_statement_or_archive_gap"
                            if terminal_flat_gap
                            else "reconcile_missing_trade_statement_before_terminal"
                        )
                    samples.append({
                        "action": action,
                        "audit_status": audit_status,
                        "blocks_business_terminal": bool(
                            reconciliation_contract.get(
                                "blocks_business_terminal"
                            )
                        ),
                        "next_action": next_action,
                        "owner_id": reconciliation_owner_id,
                        "reason": str(reconciliation_contract.get("reason") or ""),
                        "statement_probe_status": str(
                            reconciliation_contract.get("statement_probe_status")
                            or ""
                        ),
                        "symbol": str(reconciliation_contract.get("symbol") or ""),
                        "truth_source": (
                            "exchange_flat_no_open_orders"
                            if terminal_flat_gap
                            else "local_reconciliation_incomplete"
                        ),
                    })
        admission_degraded_suppressed_count += int(
            issue_counts.get("admission_degraded_suppressed_count", 0) or 0
        )

        if kind == "runtime.entry_blocked_admission_selection":
            pre_submit_blocked += int(payload.get("blocked_count") or 1)
        elif kind == "runtime.entry_blocked_pre_submit_hedgeability":
            pre_submit_blocked += int(payload.get("blocked_count") or 1)
        elif kind == "entry.cleanup_leg_exposure_truth_blocked":
            cleanup_release_truth_blocked_count += 1
            if len(cleanup_release_truth_blocked_samples) < 12:
                cleanup_release_truth_blocked_samples.append(
                    {
                        "entry_id": entry_id(payload),
                        "venue": venue_value(payload, "venue"),
                        "symbol": str(payload.get("symbol") or ""),
                        "reason": str(payload.get("reason") or ""),
                        "live_quantity": float(payload.get("live_quantity") or 0.0),
                        "target_qty": float(payload.get("target_qty") or 0.0),
                        "cleanup_client_order_id": str(
                            payload.get("cleanup_client_order_id")
                            or payload.get("client_order_id")
                            or ""
                        ),
                    }
                )
        elif kind == "execution.entry_quantity_plan":
            plan_entry_id = entry_id(payload)
            if plan_entry_id:
                long_venue = venue_value(payload, "long_venue")
                short_venue = venue_value(payload, "short_venue")
                maker_leg = str(payload.get("maker_leg") or "").lower()
                route: dict[str, str] = {
                    "long_venue": long_venue,
                    "short_venue": short_venue,
                }
                if maker_leg == "long":
                    route["maker_venue"] = long_venue
                    route["hedge_venue"] = short_venue
                elif maker_leg == "short":
                    route["maker_venue"] = short_venue
                    route["hedge_venue"] = long_venue
                entry_routes[plan_entry_id] = route
        elif kind == "pending_entry.missing_hedge_detected":
            single_leg_created += 1
        elif kind == "pending_entry.single_leg_exposure_recovery_started":
            single_leg_created += 1
        elif kind in {
            "entry.cleanup_leg_exposure",
            "pending_entry.single_leg_flatten_submitted",
        }:
            single_leg_cleanup += 1
            cleanup_entry_id = entry_id(payload)
            if cleanup_entry_id:
                cleanup_entries.add(cleanup_entry_id)
        elif kind == "pending_entry.hedge_admission_blocked":
            blocked_entry_id = entry_id(payload)
            if blocked_entry_id:
                admission_blocked_entries.add(blocked_entry_id)
        elif kind == "passive_maintenance.cancel_issued":
            cancel_entry_id = entry_id(payload)
            reason = str(payload.get("reason") or "")
            fill_ratio = payload.get("fill_ratio")
            try:
                fill_ratio_value = float(fill_ratio)
            except (TypeError, ValueError):
                fill_ratio_value = 0.0
            if (
                cancel_entry_id
                and reason == "maker_try_window_fill_ratio_below_threshold"
                and fill_ratio_value <= 0.0
            ):
                zero_fill_cancel_entries.add(cancel_entry_id)
        elif kind in {"entry.aborted", "entry.passive_unfilled"}:
            terminal_entry_id = entry_id(payload)
            if terminal_entry_id and terminal_entry_id in zero_fill_cancel_entries:
                recovered_but_counted_entries.add(terminal_entry_id)
        elif kind == "exit.passive_close_open_order_ownerless_blocked":
            ownerless_open_order_count += 1
        elif kind == "exit.passive_close_existing_reduce_only_order_adopted":
            adopted_reduce_only_order_count += 1
            owned_pending_passive_close_count += 1
        elif kind == "exit.passive_close_reduce_only_quantity_covered_by_open_order":
            duplicate_reduce_only_submit_blocked_count += 1
            owned_pending_passive_close_count += 1
        elif kind == "recovery.identity_sanitized":
            passive_close_identity_sanitized_count += 1
            passive_close_active_recovery_pending_count += 1
        elif kind == "exit.passive_close_owned_one_sided_close_order_cancelled_for_ioc":
            passive_close_owned_ioc_handoff_count += 1
            passive_close_active_recovery_pending_count += 1
        elif kind == "exit.passive_close_owned_one_sided_ioc_blocked":
            passive_close_owned_ioc_blocked_count += 1
            passive_close_active_recovery_pending_count += 1
        elif kind == "order.rejected":
            code = str(
                payload.get("exchange_code")
                or payload.get("code")
                or ""
            )
            text = json.dumps(payload, sort_keys=True).lower()
            if code == "110007" or "110007" in text or "ab not enough" in text:
                deterministic_reject_after_submit_count += 1
        elif kind == "exit.passive_close_resolved":
            resolved_position_id = entry_id(payload)
            if resolved_position_id:
                if passive_close_has_terminal_truth(payload):
                    passive_close_terminal_truth_entries.add(resolved_position_id)
                else:
                    passive_close_resolved_without_terminal_truth_entries.add(
                        resolved_position_id
                    )
        elif kind in {
            "exit.passive_close_fallback_terminal_flat",
            "runtime.position_lifecycle_terminal",
            "exit.reconciled",
        }:
            terminal_position_id = entry_id(payload)
            if terminal_position_id and passive_close_has_terminal_truth(payload):
                passive_close_terminal_truth_entries.add(terminal_position_id)
        elif kind == "exit.passive_close_waiting_exchange_flat_truth":
            final_truth_contract = passive_close_final_truth_contract(
                payload.get("exchange_truth_attempt", {}),
                long_venue=payload.get("long_venue", ""),
                short_venue=payload.get("short_venue", ""),
            )
            final_truth_action = str(final_truth_contract.get("action") or "")
            if final_truth_action:
                passive_close_final_truth_actions[final_truth_action] = (
                    passive_close_final_truth_actions.get(final_truth_action, 0) + 1
                )
            if final_truth_action == "flatten_remaining_live_leg":
                passive_close_actionable_single_leg_wait_count += 1
        elif kind == "exit.passive_close_maker_terminal_no_fill":
            entry = close_cost_entry(payload)
            if entry is not None:
                entry["maker_no_fill_count"] += 1
        elif kind == "execution.passive_cycle_zero_fill":
            entry = close_cost_entry(payload)
            if entry is not None:
                entry["zero_fill_cycle_count"] += 1
        elif kind == "exit.passive_close_maker_viability_rejected":
            entry = close_cost_entry(payload)
            if entry is not None:
                entry["maker_viability_rejected_count"] += 1
                reason = str(payload.get("reason") or "")
                if reason:
                    entry["maker_viability_reasons"].add(reason)
        elif kind in {
            "exit.passive_close_dual_taker_drive",
            "exit.passive_close_fallback_terminal_flat",
            "exit.passive_close_live_matched_close",
            "execution.dual_taker_armed",
        }:
            entry = close_cost_entry(payload)
            if entry is not None and int(entry.get("zero_fill_cycle_count") or 0) > 0:
                entry["fallback_after_zero_fill_count"] += 1
            if (
                entry is not None
                and int(entry.get("maker_viability_rejected_count") or 0) > 0
            ):
                entry["early_fallback_after_viability_rejected_count"] += 1
        if kind in {
            "exit.passive_close_resolved",
            "exit.passive_close_fallback_terminal_flat",
            "runtime.position_lifecycle_terminal",
        }:
            entry = close_cost_entry(payload)
            if entry is not None and passive_close_has_terminal_truth(payload):
                entry["terminal_resolved"] = True

        if kind in {
            "runtime.entry_admission_blocked",
            "runtime.entry_admission_symbol_cooldown_armed",
            "runtime.venue_cooldown_started",
        }:
            venue = str(payload.get("venue") or "").lower()
            symbol = str(
                payload.get("symbol")
                or payload.get("blocked_symbol")
                or ""
            ).upper()
            until_ms = cooldown_until(payload)
            if venue and until_ms > ts_ms:
                if symbol:
                    active_cooldowns[(venue, symbol)] = {
                        "reason": str(payload.get("reason") or ""),
                        "blocked_until_ms": until_ms,
                        "ts_ms": ts_ms,
                    }
                scope = str(
                    payload.get("cooldown_scope")
                    or payload.get("block_scope")
                    or ""
                )
                if scope == "venue":
                    active_cooldowns[(venue, "*")] = {
                        "reason": str(payload.get("reason") or ""),
                        "blocked_until_ms": until_ms,
                        "ts_ms": ts_ms,
                    }

        if kind == "order.passive_submitted":
            submitted_venue = str(payload.get("venue") or "").lower()
            symbol = str(payload.get("symbol") or "").upper()
            if not submitted_venue or not symbol:
                continue
            cooldown = None
            cooldown_venue = ""
            cooldown_symbol = symbol
            for route_venue in route_venues(payload):
                cooldown = active_cooldowns.get(
                    (route_venue, symbol)
                ) or active_cooldowns.get((route_venue, "*"))
                if cooldown is not None:
                    cooldown_venue = route_venue
                    if (route_venue, "*") in active_cooldowns and (
                        route_venue,
                        symbol,
                    ) not in active_cooldowns:
                        cooldown_symbol = "*"
                    break
            if cooldown is not None and len(repeated_submit_samples) < 12:
                repeated_submit_samples.append(
                    {
                        "ts_ms": ts_ms,
                        "venue_symbol": f"{cooldown_venue}:{cooldown_symbol}",
                        "submitted_venue_symbol": f"{submitted_venue}:{symbol}",
                        "position_id": entry_id(payload),
                        "leg": str(payload.get("leg") or ""),
                        "cooldown_reason": str(cooldown.get("reason") or ""),
                        "cooldown_started_ts_ms": int(cooldown.get("ts_ms") or 0),
                        "blocked_until_ms": int(
                            cooldown.get("blocked_until_ms") or 0
                        ),
                    }
                )

    cleanup_after_admission_block = len(cleanup_entries & admission_blocked_entries)
    violation_count = len(repeated_submit_samples)
    phase_counts = phase_handoff_quality.get("phase_counts", {})
    candidate_takeover_count = int(
        (phase_counts.get("candidate_lease", {}) or {}).get("takeover_count", 0)
        or 0
    )
    quote_rewarm_terminalized_count = int(
        phase_duration_summary.get("terminalized_quote_rewarm_count", 0)
        or 0
    )
    passive_close_truth_lag_resolved_entries = (
        passive_close_resolved_without_terminal_truth_entries
        & passive_close_terminal_truth_entries
    )
    recovered_but_counted_entries.update(passive_close_truth_lag_resolved_entries)
    historical_hard_over_budget_recovered_count = 0
    if gate_currently_green():
        historical_hard_over_budget_recovered_count = int(
            phase_duration_summary.get("hard_over_budget_count", 0) or 0
        )
        for sample in phase_duration_summary.get("samples", []) or []:
            if not isinstance(sample, dict):
                continue
            if str(sample.get("status") or "") != "hard_over_budget":
                continue
            recovered_id = str(
                sample.get("entry_id")
                or sample.get("position_id")
                or sample.get("artifact_id")
                or ""
            )
            if recovered_id:
                recovered_but_counted_entries.add(recovered_id)
    if gate_currently_green():
        active_stuck_count = 0
    else:
        active_stuck_count = max(
            int(phase_duration_summary.get("hard_over_budget_count", 0) or 0)
            - int(
                phase_duration_summary.get("hard_terminalized_candidate_lease_count", 0)
                or 0
            )
            - int(
                phase_duration_summary.get("hard_terminalized_quote_rewarm_count", 0)
                or 0
            )
            - len(recovered_but_counted_entries),
            0,
        )
    close_cost_samples: list[dict[str, Any]] = []
    close_cost_count = 0
    close_cost_blocking_count = 0
    close_cost_maker_no_fill_count = 0
    close_cost_zero_fill_cycle_count = 0
    close_cost_fallback_after_zero_fill_count = 0
    close_cost_maker_viability_rejected_count = 0
    close_cost_early_fallback_after_viability_rejected_count = 0
    close_cost_normal_maker_poll_zero_fill_count = 0
    close_cost_resolved_after_zero_fill_count = 0
    close_cost_unproductive_zero_fill_count = 0
    for entry in close_cost_entries.values():
        maker_no_fill_count = int(entry.get("maker_no_fill_count") or 0)
        zero_fill_cycle_count = int(entry.get("zero_fill_cycle_count") or 0)
        fallback_after_zero_fill_count = int(
            entry.get("fallback_after_zero_fill_count") or 0
        )
        maker_viability_rejected_count = int(
            entry.get("maker_viability_rejected_count") or 0
        )
        early_fallback_after_viability_rejected_count = int(
            entry.get("early_fallback_after_viability_rejected_count") or 0
        )
        if (
            maker_no_fill_count <= 0
            and zero_fill_cycle_count <= 0
            and maker_viability_rejected_count <= 0
        ):
            continue
        close_cost_count += 1
        close_cost_maker_no_fill_count += maker_no_fill_count
        close_cost_zero_fill_cycle_count += zero_fill_cycle_count
        close_cost_fallback_after_zero_fill_count += fallback_after_zero_fill_count
        close_cost_maker_viability_rejected_count += maker_viability_rejected_count
        close_cost_early_fallback_after_viability_rejected_count += (
            early_fallback_after_viability_rejected_count
        )
        terminal_resolved = bool(entry.get("terminal_resolved"))
        if not terminal_resolved and not gate_currently_green():
            close_cost_blocking_count += 1
        if maker_viability_rejected_count > 0:
            classification = "maker_viability_rejected"
        elif (
            zero_fill_cycle_count > 0
            and terminal_resolved
            and maker_no_fill_count <= 0
            and fallback_after_zero_fill_count <= 0
        ):
            classification = "normal_maker_poll_zero_fill"
            close_cost_normal_maker_poll_zero_fill_count += zero_fill_cycle_count
            close_cost_resolved_after_zero_fill_count += 1
        elif zero_fill_cycle_count > 0:
            classification = (
                "resolved_unproductive_zero_fill"
                if terminal_resolved
                else "unresolved_unproductive_zero_fill"
            )
            close_cost_unproductive_zero_fill_count += zero_fill_cycle_count
            if terminal_resolved:
                close_cost_resolved_after_zero_fill_count += 1
        elif maker_no_fill_count > 0:
            classification = "maker_terminal_no_fill"
        else:
            classification = "close_cost_trace"
        if len(close_cost_samples) < 12:
            close_cost_samples.append(
                {
                    "position_id": str(entry.get("position_id") or ""),
                    "symbol": str(entry.get("symbol") or ""),
                    "status": (
                        "terminal_resolved"
                        if terminal_resolved
                        else "unresolved_cost_trace"
                    ),
                    "maker_no_fill_count": maker_no_fill_count,
                    "zero_fill_cycle_count": zero_fill_cycle_count,
                    "fallback_after_zero_fill_count": (
                        fallback_after_zero_fill_count
                    ),
                    "maker_viability_rejected_count": (
                        maker_viability_rejected_count
                    ),
                    "early_fallback_after_viability_rejected_count": (
                        early_fallback_after_viability_rejected_count
                    ),
                    "classification": classification,
                    "maker_viability_reasons": sorted(
                        entry.get("maker_viability_reasons") or []
                    ),
                    "venues": sorted(entry.get("venues") or []),
                    "maker_legs": sorted(entry.get("maker_legs") or []),
                    "next_action": (
                        "monitor_early_fallback_cost"
                        if maker_viability_rejected_count > 0
                        else (
                            "none_terminal_resolved"
                            if classification == "normal_maker_poll_zero_fill"
                            else (
                                "tighten_close_maker_viability_source_gate"
                                if terminal_resolved
                                else "verify_exchange_truth_before_classifying_noise"
                            )
                        )
                    ),
                }
            )
    close_cost_inefficiency_summary = {
        "count": close_cost_count,
        "blocking_count": close_cost_blocking_count,
        "maker_no_fill_count": close_cost_maker_no_fill_count,
        "zero_fill_cycle_count": close_cost_zero_fill_cycle_count,
        "normal_maker_poll_zero_fill_count": (
            close_cost_normal_maker_poll_zero_fill_count
        ),
        "resolved_after_zero_fill_count": close_cost_resolved_after_zero_fill_count,
        "unproductive_zero_fill_count": close_cost_unproductive_zero_fill_count,
        "fallback_after_zero_fill_count": close_cost_fallback_after_zero_fill_count,
        "maker_viability_rejected_count": close_cost_maker_viability_rejected_count,
        "early_fallback_after_viability_rejected_count": (
            close_cost_early_fallback_after_viability_rejected_count
        ),
        "samples": close_cost_samples,
    }
    return {
        "pre_submit_blocked": pre_submit_blocked,
        "single_leg_created": single_leg_created,
        "single_leg_cleanup": single_leg_cleanup,
        "cleanup_after_admission_block": cleanup_after_admission_block,
        "candidate_takeover_count": candidate_takeover_count,
        "quote_rewarm_terminalized_count": quote_rewarm_terminalized_count,
        "recovered_but_counted_issue_count": len(recovered_but_counted_entries),
        "historical_hard_over_budget_recovered_count": (
            historical_hard_over_budget_recovered_count
        ),
        "active_stuck_count": active_stuck_count,
        "ownerless_open_order_count": ownerless_open_order_count,
        "owned_pending_passive_close_count": owned_pending_passive_close_count,
        "adopted_reduce_only_order_count": adopted_reduce_only_order_count,
        "duplicate_reduce_only_submit_blocked_count": (
            duplicate_reduce_only_submit_blocked_count
        ),
        "deterministic_reject_after_submit_count": (
            deterministic_reject_after_submit_count
        ),
        "entry_quantity_contract_blocked_count": (
            entry_quantity_contract_blocked_count
        ),
        "close_reconciliation_evidence_gap_count": (
            close_reconciliation_evidence_gap_count
        ),
        "close_reconciliation_evidence_gap_summary": (
            close_reconciliation_evidence_gap_summary
        ),
        "cleanup_release_truth_blocked_count": cleanup_release_truth_blocked_count,
        "cleanup_release_truth_blocked_samples": cleanup_release_truth_blocked_samples,
        "admission_degraded_suppressed_count": admission_degraded_suppressed_count,
        "passive_close_resolved_without_terminal_truth_count": len(
            passive_close_resolved_without_terminal_truth_entries
        ),
        "passive_close_truth_lag_resolved_count": len(
            passive_close_truth_lag_resolved_entries
        ),
        "passive_close_actionable_single_leg_wait_count": (
            passive_close_actionable_single_leg_wait_count
        ),
        "risk_only_live_single_leg_exposure_count": (
            risk_only_live_single_leg_exposure_count
        ),
        "passive_close_active_recovery_pending_count": (
            passive_close_active_recovery_pending_count
        ),
        "passive_close_identity_sanitized_count": (
            passive_close_identity_sanitized_count
        ),
        "passive_close_owned_ioc_handoff_count": (
            passive_close_owned_ioc_handoff_count
        ),
        "passive_close_owned_ioc_blocked_count": (
            passive_close_owned_ioc_blocked_count
        ),
        "passive_close_final_truth_actions": dict(
            sorted(passive_close_final_truth_actions.items())
        ),
        "close_cost_inefficiency_summary": close_cost_inefficiency_summary,
        "repeated_single_leg_guarded": {
            "violation_count": violation_count,
            "severity": "production_issue" if violation_count else "ok",
            "samples": repeated_submit_samples,
        },
        "phase_handoff_quality": phase_handoff_quality,
    }


def _build_diagnostic_noise_summary(
    events: list[dict[str, Any]],
    *,
    production_acceptance_gate: dict[str, Any] | None,
    business_progression_quality_summary: dict[str, Any],
    resolved_truth_gap_summary: dict[str, Any] | None = None,
    resolved_close_order_error_summary: dict[str, Any] | None = None,
    resolved_terminal_zero_qty_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gate = production_acceptance_gate if isinstance(production_acceptance_gate, dict) else {}
    business = (
        business_progression_quality_summary
        if isinstance(business_progression_quality_summary, dict)
        else {}
    )
    resolved_close_errors = (
        resolved_close_order_error_summary
        if isinstance(resolved_close_order_error_summary, dict)
        else {}
    )
    resolved_terminal_zero_qty = (
        resolved_terminal_zero_qty_summary
        if isinstance(resolved_terminal_zero_qty_summary, dict)
        else {}
    )
    resolved_truth_gaps = (
        resolved_truth_gap_summary
        if isinstance(resolved_truth_gap_summary, dict)
        else {}
    )
    raw_resolved_close_artifact_count = int(
        gate.get("resolved_order_truth_gap_count") or 0
    )
    raw_resolved_close_artifact_count += int(
        resolved_close_errors.get("post_only_boundary_reject_count") or 0
    )
    raw_resolved_close_artifact_count += int(
        resolved_close_errors.get("reduce_only_terminal_flat_count") or 0
    )
    raw_resolved_close_artifact_count += int(
        resolved_close_errors.get("zero_fill_terminal_flat_count") or 0
    )
    current_exchange_truth_clean = (
        gate.get("exchange_truth_flat") is True
        and gate.get("exchange_truth_no_open_orders") is True
    )
    resolved_close_summary_truth_clean = (
        resolved_close_errors.get("current_exchange_truth_clean") is not False
    )
    resolved_close_artifact_count = (
        raw_resolved_close_artifact_count
        if current_exchange_truth_clean and resolved_close_summary_truth_clean
        else 0
    )
    untrusted_close_artifact_count = (
        raw_resolved_close_artifact_count - resolved_close_artifact_count
    )
    visibility_counts: dict[str, int] = {}
    visibility_reason_counts: dict[str, dict[str, int]] = {}
    samples: list[dict[str, Any]] = []
    raw_current_risk_only_count = 0

    def bump_reason(visibility: str, reason: str, count: int = 1) -> None:
        if not visibility or not reason or count <= 0:
            return
        reason_counts = visibility_reason_counts.setdefault(visibility, {})
        reason_counts[reason] = reason_counts.get(reason, 0) + count

    def note(item: dict[str, Any], sample: dict[str, Any] | None = None) -> None:
        visibility = str(item.get("visibility") or "")
        if not visibility or visibility == "aggregated_diagnostic":
            return
        visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1
        bump_reason(visibility, str(item.get("reason") or ""))
        if sample is not None and len(samples) < 12:
            samples.append(sample)

    def apply_derived_fields(result: dict[str, Any]) -> None:
        result["visibility_counts"] = dict(sorted(visibility_counts.items()))
        result["catalog_filtered_probe_count"] = int(
            visibility_counts.get("catalog_diagnostic", 0)
        )
        result["admission_blocker_reason_counts"] = dict(
            sorted(
                visibility_reason_counts.get(
                    "current_admission_blocker", {}
                ).items()
            )
        )
        result["historical_terminal_reason_counts"] = dict(
            sorted(
                visibility_reason_counts.get(
                    "historical_terminal_evidence", {}
                ).items()
            )
        )
        result["current_admission_blocker_count"] = int(
            visibility_counts.get("current_admission_blocker", 0)
        )
        result["current_blocker_count"] = int(
            visibility_counts.get("current_blocker", 0)
        )

    for rec in events:
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        else:
            payload = dict(payload)
            payload.setdefault("event_ts_ms", rec.get("ts_ms", 0))
        contract = classify_noise_visibility(
            kind,
            payload,
            current_exchange_truth_clean=current_exchange_truth_clean,
        )
        visibility = str(contract.get("visibility") or "")
        if visibility == "aggregated_diagnostic":
            continue
        sample = {
            "kind": kind,
            "visibility": visibility,
            "reason": str(contract.get("reason") or ""),
            "position_id": str(payload.get("position_id") or ""),
            "entry_id": str(payload.get("entry_id") or ""),
            "symbol": str(payload.get("symbol") or "").upper(),
            "scope": str(contract.get("scope") or ""),
        }
        if (
            kind in ORDER_ERROR_KINDS
            and visibility == "historical_terminal_evidence"
            and sample["reason"] == "resolved_close_artifact_after_terminal_truth"
        ):
            trusted_resolution = (
                _order_error_resolved_by_truth_gap(payload, resolved_truth_gaps)
                or _order_error_resolved_by_terminal_zero_qty(
                    payload,
                    resolved_terminal_zero_qty,
                )
                or _order_error_resolved_by_close_terminal_truth(
                    payload,
                    resolved_close_errors,
                )
            )
            if not trusted_resolution:
                contract = {
                    **contract,
                    "visibility": "current_blocker",
                    "reason": "unresolved_close_artifact",
                    "blocks_gate": True,
                    "requires_operator_action": True,
                }
                visibility = "current_blocker"
                sample["visibility"] = visibility
                sample["reason"] = "unresolved_close_artifact"
        if sample["reason"] == "current_single_leg_or_risk_only_exposure":
            raw_current_risk_only_count += 1
        if (
            resolved_close_artifact_count > 0
            and visibility == "historical_terminal_evidence"
            and sample["reason"] == "resolved_close_artifact_after_terminal_truth"
        ):
            if len(samples) < 12:
                samples.append(sample)
            continue
        note(contract, sample)

    risk_only_live_single_leg_exposure_count = int(
        business.get("risk_only_live_single_leg_exposure_count") or 0
    )
    risk_only_summary_delta_count = max(
        0,
        risk_only_live_single_leg_exposure_count - raw_current_risk_only_count,
    )
    if risk_only_summary_delta_count > 0:
        visibility_counts["current_blocker"] = (
            visibility_counts.get("current_blocker", 0)
            + risk_only_summary_delta_count
        )
        bump_reason(
            "current_blocker",
            "current_risk_only_live_single_leg_exposure",
            risk_only_summary_delta_count,
        )
        if len(samples) < 12:
            samples.append(
                {
                    "kind": "business_progression.risk_only_live_single_leg_exposure",
                    "visibility": "current_blocker",
                    "reason": "current_risk_only_live_single_leg_exposure",
                    "scope": "current_live_exposure",
                    "count": risk_only_summary_delta_count,
                }
            )

    historical_over_budget_count = int(
        business.get("historical_hard_over_budget_recovered_count") or 0
    )
    phase_summary = (
        gate.get("entry_outcome_summary", {}).get("phase_duration_summary", {})
        if isinstance(gate.get("entry_outcome_summary", {}), dict)
        else {}
    )
    if historical_over_budget_count > 0:
        visibility_counts["historical_terminal_evidence"] = (
            visibility_counts.get("historical_terminal_evidence", 0)
            + historical_over_budget_count
        )
        bump_reason(
            "historical_terminal_evidence",
            "terminalized_over_budget_after_clean_truth",
            historical_over_budget_count,
        )
        for sample in phase_summary.get("samples", []) or []:
            if not isinstance(sample, dict):
                continue
            if str(sample.get("status") or "") != "hard_over_budget":
                continue
            if len(samples) >= 12:
                break
            samples.append(
                {
                    "kind": "phase_duration.hard_over_budget",
                    "visibility": "historical_terminal_evidence",
                    "reason": "terminalized_over_budget_after_clean_truth",
                    "artifact_id": str(sample.get("artifact_id") or ""),
                    "symbol": str(sample.get("symbol") or "").upper(),
                    "phase": str(sample.get("phase") or ""),
                    "scope": "",
                }
            )

    if resolved_close_artifact_count > 0:
        visibility_counts["historical_terminal_evidence"] = (
            visibility_counts.get("historical_terminal_evidence", 0)
            + resolved_close_artifact_count
        )
        bump_reason(
            "historical_terminal_evidence",
            "resolved_close_artifact_after_terminal_truth",
            resolved_close_artifact_count,
        )
    if untrusted_close_artifact_count > 0:
        visibility_counts["current_blocker"] = (
            visibility_counts.get("current_blocker", 0)
            + untrusted_close_artifact_count
        )
        bump_reason(
            "current_blocker",
            "unresolved_close_artifact",
            untrusted_close_artifact_count,
        )
        if len(samples) < 12:
            samples.append(
                {
                    "kind": "resolved_close_order_error_summary.untrusted",
                    "visibility": "current_blocker",
                    "reason": "unresolved_close_artifact",
                    "scope": "current_exchange_truth",
                    "count": untrusted_close_artifact_count,
                }
            )

    result = {
        "visibility_counts": {},
        "historical_terminalized_over_budget_count": historical_over_budget_count,
        "resolved_close_artifact_count": resolved_close_artifact_count,
        "catalog_filtered_probe_count": 0,
        "current_admission_blocker_count": 0,
        "current_blocker_count": 0,
        "admission_blocker_reason_counts": {},
        "historical_terminal_reason_counts": {},
        "samples": samples,
    }
    apply_derived_fields(result)
    truth_probe_count = int(
        resolved_terminal_zero_qty.get("truth_probe_retain_pending_count") or 0
    )
    truth_probe_resolved_count = int(
        resolved_terminal_zero_qty.get(
            "truth_probe_retain_pending_resolved_count"
        )
        or 0
    )
    if truth_probe_count > 0:
        visibility_counts["historical_terminal_evidence"] = (
            visibility_counts.get("historical_terminal_evidence", 0)
            + truth_probe_resolved_count
        )
        bump_reason(
            "historical_terminal_evidence",
            "terminal_zero_qty_truth_probe_resolved",
            truth_probe_resolved_count,
        )
        result["terminal_zero_qty_truth_probe_count"] = truth_probe_count
        result["terminal_zero_qty_truth_probe_resolved_count"] = (
            truth_probe_resolved_count
        )
        result["terminal_zero_qty_truth_probe_position_ids"] = list(
            resolved_terminal_zero_qty.get(
                "truth_probe_retain_pending_position_ids",
                [],
            )
            or []
        )
        apply_derived_fields(result)
    return result


def _build_cleanup_blocker_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    venue_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []
    total = 0
    critical_count = 0
    for rec in events:
        kind = str(rec.get("kind") or "")
        if kind not in {
            "cleanup_blocked_by_venue_auth_invalid",
            "recovery_action_blocked",
        }:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        total += 1
        if str(payload.get("severity") or "").lower() == "critical":
            critical_count += 1
        reason = str(payload.get("reason") or kind)
        venue = str(payload.get("venue") or "").lower()
        action = str(payload.get("action") or "").lower()
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if venue:
            venue_counts[venue] = venue_counts.get(venue, 0) + 1
        if action:
            action_counts[action] = action_counts.get(action, 0) + 1
        if len(samples) < 12:
            samples.append(
                {
                    "ts_ms": rec.get("ts_ms", 0),
                    "kind": kind,
                    "severity": str(payload.get("severity") or ""),
                    "entry_id": str(payload.get("entry_id") or ""),
                    "symbol": str(payload.get("symbol") or "").upper(),
                    "venue": venue,
                    "reason": reason,
                    "action": action,
                    "reduce_only": payload.get("reduce_only"),
                }
            )
    return {
        "count": total,
        "critical_count": critical_count,
        "reason_counts": dict(sorted(reason_counts.items())),
        "venue_counts": dict(sorted(venue_counts.items())),
        "action_counts": dict(sorted(action_counts.items())),
        "samples": samples,
    }


def _build_production_acceptance_gate(
    events: list[dict[str, Any]],
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any],
    state_consistency: dict[str, Any] | None = None,
    now_ms: int = 0,
) -> dict[str, Any]:
    fill_ratios: list[float] = []
    passive_maker_zero_fill_count = 0
    abort_fail_closed_count = 0
    okx_recovery_probe_rate_limited_count = 0
    okx_instrument_missing_skipped_count = 0
    local_l2_official_rebuild_count = 0
    snapshot_fallback_blocking_count = 0
    snapshot_fallback_unresolved_current_blocker_count = 0
    snapshot_fallback_resolved_by_entry_quote_truth_count = 0
    snapshot_fallback_broad_scope_demoted_count = 0
    bulk_health_diagnostic_count = 0
    contained_admission_count = 0
    hyperliquid_margin_view_zero_count = 0
    hyperliquid_unified_collateral_available_count = 0
    hyperliquid_balance_view_advice: list[str] = []
    hyperliquid_balance_view_details: list[dict[str, Any]] = []
    required_position_truth_unavailable_count = 0
    entry_opened_count = 0
    position_opened_count = 0
    residual_count = 0
    exception_conclusions: dict[str, str] = {}
    runtime_progress = _runtime_progress_from_state(local_state)
    runtime_market_data_config = _runtime_market_data_config_from_state(local_state)
    legacy_ws_bbo_without_l2_mode = (
        str(
            runtime_market_data_config.get("entry_readiness_provider_effective", "")
            or ""
        )
        == "ws_bbo_quote_lease"
        and runtime_market_data_config.get("local_l2_effective_enabled") is False
    )
    local_l2_residual_runtime_enabled_count = 0
    snapshot_fallback_quote_resolution_keys = _snapshot_fallback_resolution_keys(events)
    diagnostic_now_ms = now_ms or _now_ms()

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = _payload_dict(rec)
        reason = str(payload.get("reason", "") or "")
        venue = str(payload.get("venue", "") or "").lower()
        classification = str(payload.get("classification", "") or "")

        if kind == "passive_maintenance.cancel_try_window":
            try:
                fill_ratio = float(payload.get("fill_ratio", 0) or 0)
            except (TypeError, ValueError):
                fill_ratio = 0.0
            fill_ratios.append(fill_ratio)
            if fill_ratio <= 0:
                passive_maker_zero_fill_count += 1
                exception_conclusions["passive_maker_zero_fill"] = "v1_parity"
        elif kind == "entry.aborted" and (
            "fail_closed" in reason or payload.get("fail_closed") is True
        ):
            abort_fail_closed_count += 1
            exception_conclusions["abort_fail_closed"] = "insufficient_evidence"
        elif kind == "runtime.fail_closed":
            abort_fail_closed_count += 1
            exception_conclusions["abort_fail_closed"] = "insufficient_evidence"
        elif kind == "recovery.live_position_probe_venue_cooldown" and venue == "okx" and classification == "rate_limited":
            okx_recovery_probe_rate_limited_count += 1
            exception_conclusions["okx_recovery_probe_rate_limited"] = "official_doc"
        elif kind == "recovery.live_position_probe_unsupported_symbols" and venue == "okx":
            okx_instrument_missing_skipped_count += _okx_instrument_missing_skipped_count(payload)
            if okx_instrument_missing_skipped_count:
                exception_conclusions["okx_instrument_missing_skipped"] = "official_doc"
        elif kind == "okx_recovery_probe_noise":
            if payload.get("rate_limit_error"):
                okx_recovery_probe_rate_limited_count += 1
                exception_conclusions["okx_recovery_probe_rate_limited"] = "official_doc"
            if payload.get("instrument_missing_error"):
                okx_instrument_missing_skipped_count += _okx_instrument_missing_skipped_count(payload)
                exception_conclusions["okx_instrument_missing_skipped"] = "official_doc"

        if kind == "recovery.live_position_bulk_diagnostic_error":
            truth_required_by = payload.get("truth_required_by") or []
            if not truth_required_by and payload.get("blocking") is not True:
                bulk_health_diagnostic_count += 1
                exception_conclusions["nonblocking_health_diagnostic"] = (
                    "nonblocking_health_diagnostic"
                )

        if kind == "recovery.required_position_truth_unavailable":
            required_position_truth_unavailable_count += 1
            if payload.get("blocking") is True:
                exception_conclusions["blocking_required_truth"] = (
                    "blocking_required_truth"
                )

        if kind in {
            "runtime.entry_admission_blocked",
            "runtime.entry_admission_venue_degraded",
        }:
            reason_text = f"{reason} {payload.get('source', '')}".lower()
            if (
                str(payload.get("block_scope") or "").lower()
                in {"symbol", "venue", "symbol_and_venue"}
                and payload.get("evidence_gap") is False
                and (
                    "admission" in reason_text
                    or "insufficient_balance" in reason_text
                    or "insufficient_margin" in reason_text
                    or "110007" in reason_text
                    or "max_notional_admission_blocked" in reason_text
                )
            ):
                contained_admission_count += 1
                exception_conclusions["contained_admission"] = "contained_admission"
                if venue == "hyperliquid" and "insufficient_margin" in reason_text:
                    balance_views = exchange_truth.get("balance_views") or {}
                    hyperliquid_balance = balance_views.get("hyperliquid") or {}
                    balance_classification = str(
                        hyperliquid_balance.get("classification", "") or ""
                    )
                    available_balance = _optional_float(
                        payload.get("available_balance_quote")
                    )
                    required_margin = _optional_float(
                        payload.get("required_initial_margin_quote")
                    )
                    if (
                        balance_classification
                        or (
                            available_balance is not None
                            and available_balance <= 1e-9
                            and (required_margin or 0.0) > 0.0
                        )
                    ):
                        conclusion = (
                            balance_classification
                            or "margin_view_zero"
                        )
                        spot = hyperliquid_balance.get("spot") or {}
                        perp = hyperliquid_balance.get("perp") or {}
                        detail = {
                            "classification": conclusion,
                            "available_balance_quote": available_balance,
                            "required_initial_margin_quote": required_margin,
                            "entry_notional_quote": _optional_float(
                                payload.get("entry_notional_quote")
                            ),
                            "live_target_leverage": _optional_float(
                                payload.get("live_target_leverage")
                            ),
                            "margin_buffer_bps": _optional_float(
                                payload.get("margin_buffer_bps")
                            ),
                            "spot_usdc_total": _optional_float(
                                spot.get("usdc_total")
                            ),
                            "spot_usdc_available": _optional_float(
                                spot.get("usdc_available")
                            ),
                            "user_abstraction": str(
                                hyperliquid_balance.get("user_abstraction", "") or ""
                            ),
                            "perp_withdrawable": _optional_float(
                                perp.get("withdrawable")
                            ),
                            "perp_account_value": _optional_float(
                                perp.get("account_value")
                            ),
                        }
                        hyperliquid_balance_view_details.append(detail)
                        if conclusion == "unified_collateral_available":
                            hyperliquid_unified_collateral_available_count += 1
                            exception_conclusions[
                                "hyperliquid_unified_collateral_available"
                            ] = conclusion
                            if not hyperliquid_balance_view_advice:
                                hyperliquid_balance_view_advice.append(
                                    "Hyperliquid unified collateral is available "
                                    "from spot USDC. If entries are still "
                                    "blocked, check trading preflight, candidate "
                                    "freshness, and exchange reject truth."
                                )
                        else:
                            hyperliquid_margin_view_zero_count += 1
                            exception_conclusions[
                                "hyperliquid_margin_view_zero"
                            ] = conclusion
                            if (
                                conclusion == "usdc_present_margin_view_zero"
                                and not hyperliquid_balance_view_advice
                            ):
                                hyperliquid_balance_view_advice.append(
                                    "Hyperliquid USDC is present, but the "
                                    "admission margin view reads zero available "
                                    "margin. Check the configured account "
                                    "address, API wallet parent account, "
                                    "collateral eligibility, and the balance "
                                    "source used by the admission prefilter."
                                )

        if kind in ("runtime.local_l2_sequence_gap_rebuild", "runtime.local_l2_snapshot_error"):
            if _has_official_sequence_rebuild_evidence(payload):
                local_l2_official_rebuild_count += 1
                exception_conclusions["local_l2_official_rebuild"] = "official_doc"
            else:
                exception_conclusions.setdefault("local_l2_official_rebuild", "insufficient_evidence")
        if legacy_ws_bbo_without_l2_mode and (
            kind.startswith("runtime.local_l2_")
            or kind
            in {
                "runtime.entry_blocked_local_l2_selection",
                "runtime.entry_local_l2_readiness_diagnostics",
            }
        ):
            local_l2_residual_runtime_enabled_count += 1

        if (
            kind == "runtime.snapshot_fallback_last_good"
            and _snapshot_fallback_is_current(rec, diagnostic_now_ms)
        ):
            if _snapshot_fallback_resolved_by_entry_quote_truth(
                payload,
                snapshot_fallback_quote_resolution_keys,
            ):
                snapshot_fallback_resolved_by_entry_quote_truth_count += 1
            elif _is_snapshot_fallback_blocking(payload):
                snapshot_fallback_blocking_count += 1
                snapshot_fallback_unresolved_current_blocker_count += 1
                exception_conclusions["snapshot_fallback_blocking"] = (
                    _snapshot_fallback_exception_conclusion(payload)
                )
            elif _snapshot_fallback_broad_scope_demoted(payload):
                snapshot_fallback_broad_scope_demoted_count += 1

        if kind == "entry.opened":
            entry_opened_count += 1
            exception_conclusions["entry_opened"] = "insufficient_evidence"
        elif kind == "runtime.position_opened":
            position_opened_count += 1
            exception_conclusions["position_opened"] = "insufficient_evidence"

        if "residual" in kind or "residual" in reason:
            residual_count += 1

    passive_maker_fill_rate = (
        sum(fill_ratios) / len(fill_ratios)
        if fill_ratios else 0.0
    )
    quick_flat_summary = summarize_quick_flat_events(events)
    quick_flat_count = int(quick_flat_summary.get("quick_flat_count", 0) or 0)
    entry_overhedge_drift_corrected_count = int(
        quick_flat_summary.get("entry_overhedge_drift_corrected_count", 0)
        or 0
    )
    recovery_lifecycle = _build_recovery_lifecycle_summary(events)
    open_position_count = int(local_state.get("open_position_count", 0) or 0)
    max_concurrent_positions = int(
        local_state.get("max_concurrent_positions")
        or (local_state.get("last_scan") or {}).get("max_concurrent_positions")
        or 8
    )
    remaining_position_slots = max(max_concurrent_positions - open_position_count, 0)
    pending_entry_count = int(local_state.get("pending_entry_count", 0) or 0)
    pending_close_count = int(local_state.get("pending_close_count", 0) or 0)
    pending_residual_repair_count = int(
        local_state.get(
            "pending_residual_repair_count",
            len(local_state.get("pending_residual_repairs", []) or []),
        )
        or 0
    )
    pending_close_reconciliation_items = _state_collection_or_count(
        local_state,
        "pending_close_reconciliations",
        "pending_close_reconciliation_count",
    )
    pending_close_reconciliation_count = int(
        local_state.get("pending_close_reconciliation_count")
        or len(pending_close_reconciliation_items)
        or 0
    )
    def pending_close_reconciliation_contract(item: Any) -> dict[str, Any]:
        if not isinstance(item, dict):
            return {}
        if (
            item.get("archived") is True
            and str(item.get("archive_reason") or "")
            == "terminal_flat_accounting_gap"
        ):
            return {
                "state": "terminal_flat_accounting_gap",
                "blocks_entry": False,
            }
        normalized = normalize_close_reconciliation_record(item)
        return classify_close_reconciliation_state(
            normalized,
            current_exchange_truth_clean=(
                close_reconciliation_exchange_truth_clean(normalized)
                or (
                    _exchange_truth_flat(exchange_truth)
                    and _exchange_truth_no_open_orders(exchange_truth)
                )
            ),
        )

    if "pending_close_reconciliation_blocking_count" in local_state:
        pending_close_reconciliation_blocking_count = int(
            local_state.get("pending_close_reconciliation_blocking_count") or 0
        )
    else:
        pending_close_reconciliation_blocking_count = sum(
            1
            for item in pending_close_reconciliation_items
            if pending_close_reconciliation_contract(item).get("blocks_entry") is True
        )
    pending_close_reconciliation_accounting_only_count = sum(
        1
        for item in pending_close_reconciliation_items
        if (
            isinstance(item, dict)
            and item.get("accounting_only_backfill") is True
            and item.get("blocking_trading") is False
        )
    )
    if "pending_close_reconciliation_terminal_flat_count" in local_state:
        pending_close_reconciliation_terminal_flat_count = int(
            local_state.get("pending_close_reconciliation_terminal_flat_count")
            or 0
        )
    else:
        pending_close_reconciliation_terminal_flat_count = sum(
            1
            for item in pending_close_reconciliation_items
            if pending_close_reconciliation_contract(item).get("state")
            == "terminal_flat_accounting_gap"
        )
    pending_close_reconciliation_symbols = list(
        local_state.get("pending_close_reconciliation_symbols") or []
    )
    exchange_truth_flat = _exchange_truth_flat(exchange_truth)
    exchange_truth_actual_open_orders_present = (
        _exchange_truth_actual_open_orders_present(exchange_truth)
    )
    exchange_truth_no_open_orders = _exchange_truth_no_open_orders(exchange_truth)
    exchange_truth_required_venues = list(
        exchange_truth.get("required_venues") or []
    )
    exchange_truth_missing_required_venues = list(
        exchange_truth.get("missing_required_venues") or []
    )
    pending_live_conflicts = _build_pending_entry_live_conflict_summary(
        local_state,
        exchange_truth,
    )
    pending_entry_order_truth_gap_summary = (
        _build_pending_entry_order_truth_gap_summary(local_state)
    )
    pending_entry_order_truth_gap_count = int(
        pending_entry_order_truth_gap_summary.get("count", 0) or 0
    )
    if pending_entry_order_truth_gap_count:
        exception_conclusions["pending_entry_order_truth_gap"] = (
            "order_truth_gap_unresolved"
        )
    recovery_decision = _recovery_decision_payload(local_state, exchange_truth)
    v1_lifecycle_closure = _v1_lifecycle_closure_payload(
        local_state,
        exchange_truth,
        events,
    )
    v1_rows = [
        row for row in v1_lifecycle_closure.get("rows", []) or []
        if isinstance(row, dict)
    ]
    v1_summary = dict(v1_lifecycle_closure.get("summary") or {})
    effective_lifecycle = str(local_state.get("lifecycle", "") or "").lower()
    if (
        v1_summary.get("entry_allowed") is True
        and not v1_summary.get("recovery_block_reason")
        and str(local_state.get("risk_mode", "") or "").lower() == "running"
        and pending_entry_count == 0
        and pending_close_count == 0
        and pending_residual_repair_count == 0
    ):
        effective_lifecycle = "running"
    closure_owned_pending_passive_close_count = sum(
        1
        for row in v1_rows
        if str(row.get("evidence_class") or "")
        == "owned_pending_passive_close"
    )
    closure_ownerless_open_order_count = sum(
        1
        for row in v1_rows
        if str(row.get("terminality") or "")
        in {"orphan_maker_order", "orphan_reduce_only_order"}
    )
    local_recovery_clean = (
        open_position_count == 0
        and pending_entry_count == 0
        and pending_close_count == 0
        and pending_residual_repair_count == 0
        and pending_close_reconciliation_blocking_count == 0
    )
    exchange_recovery_clean = exchange_truth_flat and exchange_truth_no_open_orders
    current_core_clean = (
        local_recovery_clean
        and exchange_recovery_clean
        and str(local_state.get("risk_mode", "") or "").lower() == "running"
        and recovery_decision.get("entry_allowed") is True
        and not recovery_decision.get("block_reason")
        and recovery_decision.get("kind")
        in {"RUNNING_CLEAN", "RUNNING_WITH_EVIDENCE_GAP"}
    )
    current_terminal_truth_clean = (
        local_recovery_clean
        and exchange_recovery_clean
        and recovery_decision.get("entry_allowed") is True
        and not recovery_decision.get("block_reason")
        and recovery_decision.get("kind")
        in {"RUNNING_CLEAN", "RUNNING_WITH_EVIDENCE_GAP"}
    )
    fingerprints: list[str] = []
    if pending_close_reconciliation_blocking_count:
        fingerprints.append("pending_close_reconciliations_active")
    if (
        current_core_clean
        and str(local_state.get("lifecycle", "") or "").lower() == "risk_only"
    ):
        fingerprints.append("lifecycle_release_not_applied")
    if local_l2_residual_runtime_enabled_count:
        fingerprints.append("local_l2_residual_runtime_enabled")
        exception_conclusions["local_l2_residual_runtime_enabled"] = "regression"
    if (
        required_position_truth_unavailable_count
        and exception_conclusions.get("blocking_required_truth")
        == "blocking_required_truth"
        and current_core_clean
    ):
        exception_conclusions["blocking_required_truth"] = (
            "historical_required_truth_resolved_by_current_core"
        )
    resolved_order_truth_gap_summary = _build_resolved_order_truth_gap_summary(
        events,
        exchange_truth,
    )
    resolved_order_truth_gap_count = int(
        resolved_order_truth_gap_summary.get("count", 0) or 0
    )
    unresolved_order_truth_gap_count = int(
        resolved_order_truth_gap_summary.get("unresolved_count", 0) or 0
    )
    if resolved_order_truth_gap_count and current_terminal_truth_clean:
        exception_conclusions["resolved_order_truth_gap"] = (
            "closed_by_current_exchange_truth"
        )
    legacy_order_truth_gap_count = int(
        resolved_order_truth_gap_summary.get("legacy_inferred_count", 0) or 0
    )
    if legacy_order_truth_gap_count:
        fingerprints.append("close_truth_gap_legacy_inferred")
        exception_conclusions["close_truth_gap_legacy_inferred"] = (
            "nonblocking_explicit_lifecycle_missing"
        )
    ledger_closed_legacy_order_truth_gap_count = int(
        resolved_order_truth_gap_summary.get(
            "ledger_closed_legacy_inferred_count",
            0,
        )
        or 0
    )
    if ledger_closed_legacy_order_truth_gap_count:
        exception_conclusions["ledger_closed_legacy_truth_gap"] = (
            "closed_by_exchange_truth_ledger"
        )
    ledger_terminal_flat_legacy_order_truth_gap_count = int(
        resolved_order_truth_gap_summary.get(
            "ledger_terminal_flat_legacy_inferred_count",
            0,
        )
        or 0
    )
    if ledger_terminal_flat_legacy_order_truth_gap_count:
        exception_conclusions["ledger_terminal_flat_legacy_truth_gap"] = (
            "terminal_flat_exchange_truth_ledger_evidence_gap"
        )
    if unresolved_order_truth_gap_count:
        exception_conclusions["unresolved_order_truth_gap"] = (
            "order_truth_gap_unresolved"
        )
    residual_lifecycle_closed_by_recovery = (
        residual_count > 0
        and local_recovery_clean
        and exchange_recovery_clean
        and recovery_lifecycle["unclosed_residual_lifecycle_count"] == 0
    )
    residual_lifecycle_complete = (
        residual_count > 0
        and pending_residual_repair_count == 0
        and recovery_lifecycle["unclosed_residual_lifecycle_count"] == 0
    )
    if (
        (entry_opened_count or position_opened_count)
        and current_core_clean
        and (
            residual_lifecycle_closed_by_recovery
            or recovery_lifecycle["closed_trade_lifecycle_count"] > 0
        )
    ):
        opened_keys = set(recovery_lifecycle.get("opened_keys", []) or [])
        closed_keys = set(recovery_lifecycle.get("closed_open_keys", []) or [])
        exchange_truth_closed_open_keys = opened_keys - closed_keys
        if exchange_truth_closed_open_keys:
            closed_keys |= exchange_truth_closed_open_keys
            recovery_lifecycle = dict(recovery_lifecycle)
            recovery_lifecycle["closed_open_keys"] = sorted(closed_keys)
            recovery_lifecycle["unclosed_open_keys"] = []
            recovery_lifecycle["exchange_truth_closed_open_keys"] = sorted(
                exchange_truth_closed_open_keys
            )
            recovery_lifecycle["closed_trade_lifecycle_count"] = len(closed_keys)
            recovery_lifecycle["unclosed_trade_lifecycle_count"] = 0
    residual_lifecycle_closed = (
        residual_count == 0
        or residual_lifecycle_closed_by_recovery
        or residual_lifecycle_complete
    )
    trade_lifecycle_closed = (
        not (entry_opened_count or position_opened_count)
        or (
            local_recovery_clean
            and exchange_recovery_clean
            and recovery_lifecycle["unclosed_trade_lifecycle_count"] == 0
        )
    )
    if (entry_opened_count or position_opened_count) and trade_lifecycle_closed:
        if current_core_clean:
            if entry_opened_count:
                exception_conclusions["entry_opened"] = (
                    "closed_by_current_exchange_truth"
                )
            if position_opened_count:
                exception_conclusions["position_opened"] = (
                    "closed_by_current_exchange_truth"
                )

    state_consistency = state_consistency or {}
    state_consistent = not bool(state_consistency.get("state_mismatch"))
    active_positions_with_capacity = (
        open_position_count > 0
        and open_position_count <= max_concurrent_positions
        and pending_entry_count == 0
        and pending_close_count == 0
        and pending_residual_repair_count == 0
        and pending_close_reconciliation_blocking_count == 0
        and exchange_truth.get("available")
        and not exchange_truth_flat
        and exchange_truth_no_open_orders
        and effective_lifecycle == "running"
        and str(local_state.get("risk_mode", "") or "").lower() == "running"
        and state_consistent
    )
    if active_positions_with_capacity:
        fingerprints.append("active_positions_with_capacity")
        fingerprints.append("acceptance_pending_open_lifecycle")
        if position_opened_count or entry_opened_count:
            exception_conclusions["position_opened"] = "active_lifecycle_in_progress"
            if entry_opened_count:
                exception_conclusions["entry_opened"] = "active_lifecycle_in_progress"
    active_owner_scope = _active_owner_managed_scope(local_state)
    exchange_truth_probe_gaps = _exchange_truth_probe_gaps(
        exchange_truth,
        active_symbols=active_owner_scope.get("symbols", set()),
    )
    if exchange_truth_probe_gaps:
        exchange_truth["probe_gaps"] = [dict(gap) for gap in exchange_truth_probe_gaps]
        _suppress_nonblocking_probe_gap_required_venues(
            exchange_truth,
            exchange_truth_probe_gaps,
            active_venues=active_owner_scope.get("venues", set()),
        )
        exchange_truth_missing_required_venues = list(
            exchange_truth.get("missing_required_venues") or []
        )
    exchange_truth_probe_gap_count = len(exchange_truth_probe_gaps)
    exchange_truth_stale_symbol_probe_gap_count = sum(
        1
        for gap in exchange_truth_probe_gaps
        if gap.get("kind") in {"symbol_removed", "unsupported_symbol"}
    )
    exchange_truth_blocking_probe_gap_count = sum(
        1 for gap in exchange_truth_probe_gaps if gap.get("blocking") is True
    )
    active_residual_lifecycle_closed = (
        active_positions_with_capacity
        and recovery_lifecycle["unclosed_residual_lifecycle_count"] == 0
        and _completed_residuals_scoped_to_active_owner(
            events,
            scope=active_owner_scope,
            exchange_truth=exchange_truth,
        )
    )
    if active_residual_lifecycle_closed:
        residual_lifecycle_closed = True
        fingerprints.append("active_owner_residual_resolved")
    if residual_lifecycle_complete:
        exception_conclusions["residual_lifecycle_complete"] = (
            "closed_by_residual_lifecycle_terminality"
        )
    active_overhedge_correction_resolved = (
        active_positions_with_capacity
        and _overhedge_corrections_scoped_to_active_owner(
            events,
            scope=active_owner_scope,
            exchange_truth=exchange_truth,
        )
    )
    if active_overhedge_correction_resolved:
        fingerprints.append("active_owner_overhedge_corrected")
    terminal_overhedge_corrections_closed = (
        active_positions_with_capacity
        and _terminal_overhedge_corrections_closed(events)
    )
    if terminal_overhedge_corrections_closed:
        fingerprints.append("terminal_overhedge_corrections_closed")

    blocking_reasons: list[str] = []
    if open_position_count > max_concurrent_positions:
        blocking_reasons.append("open_positions_exceed_configured_max")
    elif open_position_count and not active_positions_with_capacity:
        blocking_reasons.append("local_open_positions_present")
    if pending_entry_count or pending_close_count:
        blocking_reasons.append("local_pending_entries_or_closes_present")
    if pending_close_reconciliation_blocking_count:
        blocking_reasons.append("pending_close_reconciliations_active")
    if pending_residual_repair_count:
        blocking_reasons.append("local_pending_residual_repairs_present")
    if residual_count and not residual_lifecycle_closed:
        blocking_reasons.append("residual_events_present")
    if quick_flat_count:
        blocking_reasons.append("quick_flat_events_present")
    if (
        entry_overhedge_drift_corrected_count
        and not current_terminal_truth_clean
        and not active_overhedge_correction_resolved
        and not terminal_overhedge_corrections_closed
    ):
        blocking_reasons.append("entry_overhedge_drift_corrected_present")
    if not exchange_truth.get("available"):
        blocking_reasons.append("exchange_truth_unavailable")
    else:
        if exchange_truth_missing_required_venues:
            blocking_reasons.append("exchange_truth_required_venues_missing")
        if not exchange_truth_flat and not active_positions_with_capacity:
            blocking_reasons.append("exchange_truth_nonzero_position")
        if not exchange_truth_no_open_orders:
            blocking_reasons.append("exchange_truth_open_orders_present")
        if exchange_truth_blocking_probe_gap_count:
            blocking_reasons.append("exchange_truth_missing_required_evidence")
    if (
        (entry_opened_count or position_opened_count)
        and not trade_lifecycle_closed
        and not active_positions_with_capacity
    ):
        blocking_reasons.append("entry_or_position_opened_without_fixture_finalized_evidence")
    if required_position_truth_unavailable_count and exception_conclusions.get(
        "blocking_required_truth"
    ) == "blocking_required_truth":
        blocking_reasons.append("blocking_required_truth")
    if local_l2_residual_runtime_enabled_count:
        blocking_reasons.append("local_l2_residual_runtime_enabled")
    if unresolved_order_truth_gap_count:
        blocking_reasons.append("order_truth_gap_unresolved")
    if pending_entry_order_truth_gap_count:
        blocking_reasons.append("pending_entry_order_truth_gap_unresolved")

    diagnostic_counts = {
        "passive_maker_zero_fill": passive_maker_zero_fill_count,
        "abort_fail_closed": abort_fail_closed_count,
        "okx_recovery_probe_rate_limited": okx_recovery_probe_rate_limited_count,
        "okx_instrument_missing_skipped": okx_instrument_missing_skipped_count,
        "local_l2_official_rebuild": local_l2_official_rebuild_count,
        "local_l2_residual_runtime_enabled": (
            local_l2_residual_runtime_enabled_count
        ),
        "snapshot_fallback_blocking": snapshot_fallback_blocking_count,
        "nonblocking_health_diagnostic": bulk_health_diagnostic_count,
        "contained_admission": contained_admission_count,
        "hyperliquid_margin_view_zero": hyperliquid_margin_view_zero_count,
        "hyperliquid_unified_collateral_available": (
            hyperliquid_unified_collateral_available_count
        ),
        "resolved_order_truth_gap": resolved_order_truth_gap_count,
        "unresolved_order_truth_gap": unresolved_order_truth_gap_count,
        "pending_entry_order_truth_gap": pending_entry_order_truth_gap_count,
        "blocking_required_truth": (
            required_position_truth_unavailable_count
            if exception_conclusions.get("blocking_required_truth")
            == "blocking_required_truth"
            else 0
        ),
    }
    entry_quantity_terminal_summary = _build_entry_quantity_terminal_summary(
        events,
        {
            "exchange_truth_flat": exchange_truth_flat,
            "exchange_truth_no_open_orders": exchange_truth_no_open_orders,
        },
    )
    entry_outcome_summary = _build_entry_outcome_summary(events)
    passive_zero_fill_exhausted_then_recovered_count = _count_passive_zero_fill_exhausted_then_recovered(
        events
    )
    short_window_warning_details = {
        "entry_quantity_mismatch": int(
            entry_quantity_terminal_summary.get(
                "common_quantity_mismatch_warning_count", 0
            )
            or 0
        ),
        "hedge_quantity_undercut": int(
            entry_quantity_terminal_summary.get(
                "hedge_quantity_undercut_warning_count", 0
            )
            or 0
        ),
        "passive_close_truth_gap": unresolved_order_truth_gap_count,
        "passive_zero_fill_exhausted_then_recovered": (
            passive_zero_fill_exhausted_then_recovered_count
        ),
    }
    short_window_warning_families = [
        family
        for family, count in short_window_warning_details.items()
        if count
    ]
    short_window_warning_count = sum(short_window_warning_details.values())
    unclassified_exceptions = [
        name for name, count in diagnostic_counts.items()
        if count and name not in exception_conclusions
    ]
    insufficient_evidence_exceptions = [
        name for name, conclusion in exception_conclusions.items()
        if conclusion == "insufficient_evidence"
        and name not in {"entry_opened", "position_opened"}
    ]
    if unclassified_exceptions:
        blocking_reasons.append("diagnostic_exception_unclassified")
    if insufficient_evidence_exceptions:
        blocking_reasons.append("diagnostic_exception_insufficient_evidence")

    return {
        "passive_maker_zero_fill_count": passive_maker_zero_fill_count,
        "passive_maker_fill_rate": passive_maker_fill_rate,
        "abort_fail_closed_count": abort_fail_closed_count,
        "okx_recovery_probe_rate_limited_count": okx_recovery_probe_rate_limited_count,
        "okx_instrument_missing_skipped_count": okx_instrument_missing_skipped_count,
        "local_l2_official_rebuild_count": local_l2_official_rebuild_count,
        "local_l2_residual_runtime_enabled_count": (
            local_l2_residual_runtime_enabled_count
        ),
        "snapshot_fallback_blocking_count": snapshot_fallback_blocking_count,
        "snapshot_fallback_unresolved_current_blocker_count": (
            snapshot_fallback_unresolved_current_blocker_count
        ),
        "snapshot_fallback_resolved_by_entry_quote_truth_count": (
            snapshot_fallback_resolved_by_entry_quote_truth_count
        ),
        "snapshot_fallback_broad_scope_demoted_count": (
            snapshot_fallback_broad_scope_demoted_count
        ),
        "bulk_health_diagnostic_count": bulk_health_diagnostic_count,
        "contained_admission_count": contained_admission_count,
        "hyperliquid_margin_view_zero_count": hyperliquid_margin_view_zero_count,
        "hyperliquid_unified_collateral_available_count": (
            hyperliquid_unified_collateral_available_count
        ),
        "hyperliquid_balance_view_details": hyperliquid_balance_view_details[:10],
        "hyperliquid_balance_view_advice": hyperliquid_balance_view_advice,
        "resolved_order_truth_gap_count": resolved_order_truth_gap_count,
        "unresolved_order_truth_gap_count": unresolved_order_truth_gap_count,
        "resolved_order_truth_gap_summary": resolved_order_truth_gap_summary,
        "required_position_truth_unavailable_count": (
            required_position_truth_unavailable_count
        ),
        "entry_opened_count": entry_opened_count,
        "position_opened_count": position_opened_count,
        "entry_outcome_summary": entry_outcome_summary,
        "open_position_count": open_position_count,
        "max_concurrent_positions": max_concurrent_positions,
        "remaining_position_slots": remaining_position_slots,
        "active_positions_with_capacity": active_positions_with_capacity,
        "pending_entry_count": pending_entry_count,
        "pending_entry_order_truth_gap_count": (
            pending_entry_order_truth_gap_count
        ),
        "pending_entry_order_truth_gap_summary": (
            pending_entry_order_truth_gap_summary
        ),
        "pending_close_count": pending_close_count,
        "pending_close_reconciliation_count": pending_close_reconciliation_count,
        "pending_close_reconciliation_blocking_count": (
            pending_close_reconciliation_blocking_count
        ),
        "pending_close_reconciliation_terminal_flat_count": (
            pending_close_reconciliation_terminal_flat_count
        ),
        "pending_close_reconciliation_accounting_only_count": (
            pending_close_reconciliation_accounting_only_count
        ),
        "pending_close_reconciliation_symbols": (
            pending_close_reconciliation_symbols
        ),
        "pending_residual_repair_count": pending_residual_repair_count,
        "residual_count": residual_count,
        "quick_flat_count": quick_flat_count,
        "entry_overhedge_drift_corrected_count": (
            entry_overhedge_drift_corrected_count
        ),
        "quick_flat_duplicate_event_count": int(
            quick_flat_summary.get("duplicate_event_count", 0) or 0
        ),
        "quick_flat_summary": quick_flat_summary,
        "closed_trade_lifecycle_count": recovery_lifecycle["closed_trade_lifecycle_count"],
        "unclosed_trade_lifecycle_count": recovery_lifecycle["unclosed_trade_lifecycle_count"],
        "closed_residual_lifecycle_count": recovery_lifecycle["closed_residual_lifecycle_count"],
        "unclosed_residual_lifecycle_count": recovery_lifecycle["unclosed_residual_lifecycle_count"],
        "residual_lifecycle_complete": residual_lifecycle_complete,
        "recovery_lifecycle": recovery_lifecycle,
        "exchange_truth_flat": exchange_truth_flat,
        "exchange_truth_no_open_orders": exchange_truth_no_open_orders,
        "exchange_truth_actual_open_orders_present": (
            exchange_truth_actual_open_orders_present
        ),
        "exchange_truth_probe_gap_count": exchange_truth_probe_gap_count,
        "exchange_truth_stale_symbol_probe_gap_count": (
            exchange_truth_stale_symbol_probe_gap_count
        ),
        "exchange_truth_blocking_probe_gap_count": (
            exchange_truth_blocking_probe_gap_count
        ),
        "exchange_truth_probe_gaps": exchange_truth_probe_gaps,
        "exchange_truth_required_venues": exchange_truth_required_venues,
        "exchange_truth_missing_required_venues": (
            exchange_truth_missing_required_venues
        ),
        "pending_entry_live_conflicts": pending_live_conflicts,
        "recovery_decision": recovery_decision,
        "v1_lifecycle_closure": v1_lifecycle_closure,
        "ownerless_open_order_count": closure_ownerless_open_order_count,
        "owned_pending_passive_close_count": (
            closure_owned_pending_passive_close_count
        ),
        "runtime_progress": runtime_progress,
        "runtime_market_data_config": runtime_market_data_config,
        "fingerprints": fingerprints,
        "exception_conclusions": exception_conclusions,
        "unclassified_exceptions": unclassified_exceptions,
        "insufficient_evidence_exceptions": insufficient_evidence_exceptions,
        "short_window_warning_count": short_window_warning_count,
        "short_window_warning_families": short_window_warning_families,
        "short_window_warning_details": short_window_warning_details,
        "blocking_reasons": blocking_reasons,
        "gate_passed": not blocking_reasons,
    }


def _build_pending_entry_live_conflict_summary(
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any] | None,
) -> dict[str, Any]:
    exchange_truth = exchange_truth or {}
    live_positions = _live_position_index(exchange_truth)
    open_orders = _open_order_index(exchange_truth)
    details: list[dict[str, Any]] = []
    for pending in _state_collection_or_count(
        local_state,
        "pending_entries",
        "pending_entry_count",
    ):
        if not isinstance(pending, dict):
            continue
        symbol = str(pending.get("symbol") or "").upper()
        if not symbol:
            continue
        maker_fill = _safe_float(pending.get("maker_leg_filled"))
        hedge_fill = _safe_float(pending.get("hedge_leg_filled"))
        if maker_fill <= 1e-9 and hedge_fill <= 1e-9:
            continue
        expected_legs = _pending_expected_legs(pending, maker_fill, hedge_fill)
        leg_details: list[dict[str, Any]] = []
        conflict_reasons: list[str] = []
        for leg in expected_legs:
            venue = leg["venue"]
            key = (venue, symbol)
            live = live_positions.get(key, {})
            live_qty = _safe_float(live.get("quantity"))
            expected_qty = _safe_float(leg.get("expected_quantity"))
            live_side = str(live.get("side") or "").lower()
            expected_side = str(leg.get("expected_side") or "").lower()
            live_matches = (
                live_qty > 1e-9
                and abs(live_qty - expected_qty) <= 1e-9
                and _side_matches(live_side, expected_side)
            )
            if live_qty <= 1e-9:
                conflict_reasons.append(
                    f"{venue} fill evidence conflicts with {venue} live flat"
                )
            elif not live_matches:
                conflict_reasons.append(
                    f"{venue} fill evidence conflicts with {venue} live mismatch"
                )
            leg_details.append(
                {
                    **leg,
                    "live_quantity": live_qty,
                    "live_side": live_side,
                    "live_position_confirmed": live_matches,
                    "open_orders": open_orders.get(key, []),
                    "owner": "pending_entry",
                }
            )
        if any(leg["live_quantity"] > 1e-9 for leg in leg_details):
            conflict_reasons.append("live position owned by pending conflict")
        details.append(
            {
                "pending_id": str(
                    pending.get("pending_id")
                    or pending.get("position_id")
                    or symbol
                ),
                "symbol": symbol,
                "maker_leg": str(pending.get("maker_leg") or ""),
                "maker_leg_filled": maker_fill,
                "hedge_leg_filled": hedge_fill,
                "maker_order_id": str(pending.get("maker_order_id") or ""),
                "maker_client_order_id": str(
                    pending.get("maker_client_order_id") or ""
                ),
                "hedge_order_id": str(pending.get("hedge_order_id") or ""),
                "hedge_client_order_id": str(
                    pending.get("hedge_client_order_id") or ""
                ),
                "legs": leg_details,
                "conflict_reasons": sorted(set(conflict_reasons)),
                "next_action": "owned_pending_entry_live_conflict_cleanup",
            }
        )
    return {
        "count": len(details),
        "details": details,
    }


def _pending_expected_legs(
    pending: dict[str, Any],
    maker_fill: float,
    hedge_fill: float,
) -> list[dict[str, Any]]:
    symbol = str(pending.get("symbol") or "").upper()
    long_venue = str(pending.get("long_venue") or "").lower()
    short_venue = str(pending.get("short_venue") or "").lower()
    maker_leg = _maker_leg_text(pending.get("maker_leg") or pending.get("maker_side"))
    if maker_leg not in {"long", "short"}:
        return []
    long_qty = maker_fill if maker_leg == "long" else hedge_fill
    short_qty = hedge_fill if maker_leg == "long" else maker_fill
    legs: list[dict[str, Any]] = []
    if long_venue and long_qty > 1e-9:
        legs.append(
            {
                "venue": long_venue,
                "symbol": symbol,
                "expected_side": "long",
                "expected_quantity": long_qty,
                "source_fill_layer": "maker" if maker_leg == "long" else "hedge",
            }
        )
    if short_venue and short_qty > 1e-9:
        legs.append(
            {
                "venue": short_venue,
                "symbol": symbol,
                "expected_side": "short",
                "expected_quantity": short_qty,
                "source_fill_layer": "hedge" if maker_leg == "long" else "maker",
            }
        )
    return legs


def _build_pending_entry_order_truth_gap_summary(
    local_state: dict[str, Any],
) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for pending in _state_collection_or_count(
        local_state,
        "pending_entries",
        "pending_entry_count",
    ):
        if not isinstance(pending, dict):
            continue
        metadata = pending.get("metadata")
        if not isinstance(metadata, dict):
            continue
        gap = metadata.get("hedge_accepted_order_truth_gap")
        if not isinstance(gap, dict) or gap.get("accepted_order_truth_gap") is not True:
            continue
        samples.append(
            {
                "entry_id": str(
                    gap.get("entry_id")
                    or pending.get("pending_id")
                    or pending.get("entry_id")
                    or ""
                ),
                "symbol": str(gap.get("symbol") or pending.get("symbol") or ""),
                "venue": str(gap.get("venue") or ""),
                "accepted_order_id": str(gap.get("accepted_order_id") or ""),
                "accepted_client_order_id": str(
                    gap.get("accepted_client_order_id") or ""
                ),
                "order_truth_state": str(gap.get("order_truth_state") or ""),
                "next_action": str(gap.get("next_action") or ""),
                "last_status": str(gap.get("last_status") or ""),
            }
        )
    return {
        "count": len(samples),
        "samples": samples[:10],
    }


def _maker_leg_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    text = str(value or "").lower()
    if text == "buy":
        return "long"
    if text == "sell":
        return "short"
    return text


def _live_position_index(
    exchange_truth: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for venue, positions in (exchange_truth.get("positions") or {}).items():
        if not isinstance(positions, dict):
            continue
        for symbol, row in positions.items():
            if not isinstance(row, dict):
                continue
            indexed[
                (
                    str(row.get("venue") or venue).lower(),
                    str(row.get("symbol") or symbol).upper(),
                )
            ] = row
    return indexed


def _open_order_index(
    exchange_truth: dict[str, Any],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    indexed: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for venue, orders_by_symbol in (exchange_truth.get("open_orders") or {}).items():
        if not isinstance(orders_by_symbol, dict):
            continue
        for symbol, rows in orders_by_symbol.items():
            if not isinstance(rows, list):
                continue
            indexed[
                (
                    str(venue or "").lower(),
                    str(symbol or "").upper(),
                )
            ] = [row for row in rows if isinstance(row, dict)]
    return indexed


def _v1_lifecycle_closure_payload(
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any] | None,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    existing = local_state.get("v1_lifecycle_closure")
    if (
        isinstance(existing, dict)
        and existing.get("version")
        and not _exchange_truth_high_confidence(exchange_truth)
    ):
        return dict(existing)
    owner_index = RecoveryOwnerIndex.from_state_and_journal(local_state, events)
    return build_v1_lifecycle_closure_table(
        local_state=local_state,
        exchange_truth=exchange_truth,
        events=events,
        owner_index=owner_index,
    ).to_dict()


def _exchange_truth_high_confidence(exchange_truth: dict[str, Any] | None) -> bool:
    if not isinstance(exchange_truth, dict):
        return False
    available = bool(exchange_truth.get("available", exchange_truth.get("truth_available")))
    confidence = str(exchange_truth.get("confidence") or "").lower()
    return available and confidence == "high"


def _recovery_decision_payload(
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any] | None,
) -> dict[str, Any]:
    decision = V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(
            local_open_positions=_state_collection_or_count(
                local_state, "open_positions", "open_position_count"
            ),
            pending_entries=_state_collection_or_count(
                local_state, "pending_entries", "pending_entry_count"
            ),
            residual_repairs=_state_collection_or_count(
                local_state, "pending_residual_repairs", "pending_residual_repair_count"
            ),
            passive_closes=_state_collection_or_count(
                local_state, "pending_passive_closes", "pending_close_count"
            ),
            exchange_truth=exchange_truth,
            prior_recovery_block_reason=local_state.get("recovery_blocked_reason"),
        )
    )
    return {
        "kind": decision.kind.value,
        "evidence_quality": decision.evidence_quality,
        "entry_allowed": decision.entry_allowed,
        "block_reason": decision.block_reason,
        "clear_reason": decision.clear_reason,
        "diagnostic_severity": decision.diagnostic_severity,
    }


def _count_passive_zero_fill_exhausted_then_recovered(
    events: list[dict[str, Any]],
) -> int:
    exhausted_positions: set[str] = set()
    recovered_positions: set[str] = set()
    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        position_id = str(payload.get("position_id") or payload.get("entry_id") or "")
        if not position_id:
            continue
        if kind == "exit.passive_close_fallback_zero_fill_no_pending":
            exhausted_positions.add(position_id)
        elif kind == "execution.passive_cycle_zero_fill":
            try:
                zero_fill_cycles = int(payload.get("zero_fill_cycles", 0) or 0)
            except (TypeError, ValueError):
                zero_fill_cycles = 0
            try:
                max_zero_fill_cycles = int(
                    payload.get("max_zero_fill_cycles", 0) or 0
                )
            except (TypeError, ValueError):
                max_zero_fill_cycles = 0
            if max_zero_fill_cycles > 0 and zero_fill_cycles >= max_zero_fill_cycles:
                exhausted_positions.add(position_id)
        elif kind in {
            "exit.passive_close_resolved",
            "exit.passive_close_recovery_probe_flat",
            "exit.passive_close_fallback_terminal_flat",
        }:
            recovered_positions.add(position_id)
    return len(exhausted_positions & recovered_positions)


def _state_collection_or_count(
    state: dict[str, Any],
    collection_key: str,
    count_key: str,
) -> tuple[Any, ...]:
    collection = state.get(collection_key)
    if isinstance(collection, dict):
        return tuple(collection.values())
    if isinstance(collection, (list, tuple, set)):
        return tuple(collection)
    count = int(state.get(count_key) or 0)
    return tuple({"source": count_key} for _ in range(max(count, 0)))


# ---------------------------------------------------------------------------
# evidence completeness
# ---------------------------------------------------------------------------

def _build_evidence_completeness(
    order_errors: list[dict[str, Any]],
    state_consistency: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    missing: list[str] = []
    completions: set[str] = set()

    for err in order_errors:
        comp = str(err.get("evidence_completeness", ""))
        if comp:
            completions.add(comp)
        for m in err.get("missing_evidence", []) or []:
            if m and m not in missing:
                missing.append(m)

    if not exchange_truth.get("available", False):
        missing.append("exchange_truth_unavailable")
        for m2 in exchange_truth.get("missing_evidence", []) or []:
            if m2 and m2 not in missing:
                missing.append(m2)

    if state_consistency.get("local_open_exchange_flat"):
        missing.append("state_consistency_breach")

    if not completions:
        truth_confidence = str(exchange_truth.get("confidence", "low"))
        if (
            exchange_truth.get("available", False)
            and truth_confidence == "high"
            and not state_consistency.get("state_mismatch")
            and not missing
        ):
            overall = "complete"
            confidence = "high"
        elif exchange_truth.get("available", False) and not missing:
            overall = "partial"
            confidence = "medium"
        else:
            overall = "missing"
            confidence = "low"
    elif "transport_only" in completions or any(
        c in completions for c in ("missing_exchange_body",)
    ):
        overall = "missing"
        confidence = "low"
    elif "unparsed_exchange_body" in completions or "missing_exchange_code_or_msg" in completions:
        overall = "partial"
        confidence = "medium"
    elif "missing_body" in completions or "partial" in completions:
        overall = "partial"
        confidence = "medium"
    else:
        overall = "complete"
        confidence = "high"

    if missing and overall == "complete":
        overall = "partial"
        confidence = "medium"

    evidence_sources: list[str] = []
    if exchange_truth.get("available"):
        evidence_sources.append("exchange_truth")
    else:
        evidence_sources.append("exchange_truth=unavailable")
    if order_errors:
        evidence_sources.append("order_error_evidence ({} groups)".format(len(order_errors)))
    if state_consistency.get("state_mismatch"):
        evidence_sources.append("state_consistency=mismatch")

    return {
        "overall": overall,
        "confidence": confidence,
        "completions_seen": sorted(completions),
        "missing_evidence": missing,
        "evidence_sources": evidence_sources,
    }


# ---------------------------------------------------------------------------
# conclusion
# ---------------------------------------------------------------------------

def _order_error_is_current_blocker(error: dict[str, Any]) -> bool:
    if error.get("current_blocker") is False:
        return False
    visibility = str(error.get("visibility") or "")
    if visibility in {
        "historical_terminal_artifact",
        "historical_resolved_artifact",
        "legacy_terminal_inferred",
    }:
        return False
    status = str(error.get("status") or "")
    if status in {
        "resolved",
        "terminal_resolved",
        "historical_resolved",
        "historical_terminal_artifact",
    }:
        return False
    resolution_status = str(error.get("resolution_status") or "")
    if error.get("resolved") is True and resolution_status in {
        "terminal_flat",
        "live_flat",
        "live_flat_after_single_leg_cleanup",
        "position_terminal_flat",
        "resolved_by_terminal_truth",
        "reduce_only_terminal_flat",
    }:
        return False
    if error.get("blocking") is False:
        return False
    return True


def _build_conclusion(
    health: dict[str, Any],
    state_consistency: dict[str, Any],
    evidence_completeness: dict[str, Any],
    order_errors: list[dict[str, Any]],
    l2_evidence: dict[str, Any],
    snapshot_evidence: dict[str, Any],
    exchange_truth: dict[str, Any],
    production_acceptance_gate: dict[str, Any] | None = None,
    event_coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_coverage = event_coverage if isinstance(event_coverage, dict) else {}
    event_coverage_complete = bool(event_coverage.get("complete", True))
    gate_failed = (
        production_acceptance_gate is not None
        and production_acceptance_gate.get("gate_passed") is False
    )
    gate_blockers = (
        list(production_acceptance_gate.get("blocking_reasons", []) or [])
        if production_acceptance_gate is not None
        else []
    )
    gate_passed = (
        production_acceptance_gate is not None
        and production_acceptance_gate.get("gate_passed") is True
    )
    current_order_errors = [
        error
        for error in order_errors
        if _order_error_is_current_blocker(error) or not gate_passed
    ]
    resolved_order_error_count = len(order_errors) - len(current_order_errors)

    if gate_failed:
        status = "unhealthy"
        risk = "high"
    elif (
        health["ok"]
        and not state_consistency["state_mismatch"]
        and not current_order_errors
    ):
        status = (
            "healthy"
            if event_coverage_complete
            else "healthy_with_incomplete_event_coverage"
        )
        risk = "low" if event_coverage_complete else "medium"
    elif health["critical_count"] > 0:
        status = "unhealthy"
        risk = "high"
    elif state_consistency["local_open_exchange_flat"]:
        status = "critical"
        risk = "high"
    elif state_consistency["state_mismatch"]:
        status = "degraded"
        risk = "high"
    elif current_order_errors:
        status = "degraded"
        has_rejected = any(
            e.get("kind") == "order.rejected" for e in current_order_errors
        )
        risk = "medium" if has_rejected else "low"
    else:
        status = "degraded"
        risk = "medium"

    summary_parts: list[str] = []
    if health.get("fingerprints"):
        summary_parts.append("health issues: {}".format(", ".join(health["fingerprints"][:3])))
    if state_consistency.get("local_open_exchange_flat"):
        summary_parts.append("CRITICAL: local open position(s) but exchange flat")
    elif state_consistency.get("state_mismatch"):
        summary_parts.append("state mismatch detected")
    if not exchange_truth.get("available"):
        summary_parts.append("exchange truth unavailable — cannot verify consistency")
    if l2_evidence["missing_l2_or_tick_count"] > 0:
        summary_parts.append("L2/tick gaps: {}".format(l2_evidence["missing_l2_or_tick_count"]))
    if current_order_errors:
        total_errs = sum(e.get("count", 0) for e in current_order_errors)
        summary_parts.append("{} order error groups ({} total)".format(
            len(current_order_errors), total_errs,
        ))
    if resolved_order_error_count > 0:
        summary_parts.append(
            "{} historical resolved order error groups".format(
                resolved_order_error_count
            )
        )
    if evidence_completeness["overall"] != "complete":
        summary_parts.append("evidence: {}".format(evidence_completeness["overall"]))
    if not event_coverage_complete:
        summary_parts.append("event coverage incomplete; logs cannot prove no anomaly")
    if gate_failed:
        summary_parts.append(
            "production acceptance gate failed: {}".format(
                ", ".join(gate_blockers[:5]) if gate_blockers else "unknown"
            )
        )
    balance_view_advice = (
        list(production_acceptance_gate.get("hyperliquid_balance_view_advice", []) or [])
        if production_acceptance_gate is not None
        else []
    )
    unified_collateral_count = (
        int(
            production_acceptance_gate.get(
                "hyperliquid_unified_collateral_available_count",
                0,
            )
            or 0
        )
        if production_acceptance_gate is not None
        else 0
    )
    if unified_collateral_count > 0:
        summary_parts.append(
            "Hyperliquid unified collateral available; check trading preflight, "
            "candidate freshness, and exchange reject truth"
        )
    elif balance_view_advice:
        summary_parts.append(
            "Hyperliquid USDC present but admission margin view reads zero"
        )

    summary = (
        "; ".join(summary_parts)
        if summary_parts
        else "no issues detected"
    )

    next_actions: list[str] = []
    if state_consistency.get("local_open_exchange_flat"):
        for p in state_consistency.get("details", []):
            local_syms = p.get("local_symbols", [])
            if local_syms:
                next_actions.append(
                    "verify positions on exchange — local reports open {} but exchange flat".format(
                        ", ".join(local_syms[:3]),
                    )
                )
            else:
                next_actions.append("verify position on exchange — local reports open but exchange flat")
    if not exchange_truth.get("available"):
        missing_creds = exchange_truth.get("missing_evidence", [])
        if any("credentials" in m for m in missing_creds):
            next_actions.append(
                "set LIGHTFEE_BINANCE_API_KEY/LIGHTFEE_BINANCE_API_SECRET or "
                "LIGHTFEE_BYBIT_API_KEY/LIGHTFEE_BYBIT_API_SECRET env vars for exchange truth"
            )
        else:
            next_actions.append("exchange truth not available — check network/credentials")
    if evidence_completeness["overall"] in ("partial", "missing"):
        next_actions.append("collect full exchange error bodies (raw_body, exchange_code)")
    if not event_coverage_complete:
        next_actions.append("increase event coverage before declaring the live path verified")
    if current_order_errors:
        top = _build_top_exchange_errors(current_order_errors)
        for t in top[:3]:
            if t["http_status"] > 0 and not t["raw_body_present"]:
                next_actions.append(
                    "missing exchange body for HTTP {} {} code={}".format(
                        t["http_status"], t["venue"], t["exchange_code"],
                    )
                )
        next_actions.append("review order_error_evidence for root cause")
    if l2_evidence["stale_rebuild_count"] > 0:
        next_actions.append("investigate L2 stale/rebuild ({} rebuilds, {} gaps)".format(
            l2_evidence["stale_rebuild_count"], l2_evidence["sequence_gap_count"],
        ))
    if (
        snapshot_evidence.get("stale_or_degraded_count", 0) > 0
        and l2_evidence["stale_rebuild_count"] <= 0
    ):
        next_actions.append("review snapshot stale/degraded evidence; not classified as Local L2")
    if gate_failed:
        next_actions.append(
            "resolve production acceptance gate blockers: {}".format(
                ", ".join(gate_blockers[:5]) if gate_blockers else "unknown"
            )
        )
    for advice in balance_view_advice:
        if advice not in next_actions:
            next_actions.append(advice)
    if not next_actions:
        next_actions.append("no immediate action required")

    return {
        "status": status,
        "summary": summary,
        "risk": risk,
        "next_actions": next_actions,
    }


# ---------------------------------------------------------------------------
# Main diagnose
# ---------------------------------------------------------------------------

def _build_spread_sidecar_summary(
    runtime_dir: str,
    *,
    now_ms: int = 0,
    sidecar_snapshot_max_age_ms: int = 10_000,
) -> dict[str, Any]:
    path = Path(runtime_dir) / "spread-opportunities-current.json"
    try:
        raw_snapshot = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw_snapshot, dict):
            raw_snapshot = None
        from lightfee.spread.publisher import load_spread_snapshot

        snapshot = load_spread_snapshot(path)
    except Exception:
        raw_snapshot = None
        snapshot = None
    if snapshot is None:
        return {
            "available": False,
            "path": str(path),
            "spread_sidecar_source": "missing_or_malformed",
            "diagnostic_contract_status": "missing_or_malformed",
            "candidate_count": 0,
            "degraded_venues": [],
            "input_quote_count": 0,
            "valid_quote_count": 0,
            "evaluated_pair_count": 0,
            "accepted_pair_count": 0,
            "rejection_counts": {},
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "paper_admission_rejection_counts": {},
            "top_paper_admission_rejection": None,
            "blocking_reasons": ["spread_snapshot_missing_or_malformed"],
            "next_actions": ["restore a valid spread sidecar snapshot"],
        }

    def nonnegative_int(field: str) -> int:
        value = getattr(snapshot, field, 0)
        return value if type(value) is int and value >= 0 else 0

    def stable_counts(field: str) -> dict[str, int]:
        value = getattr(snapshot, field, {})
        if not isinstance(value, dict):
            return {}
        return {
            str(reason): count
            for reason, count in value.items()
            if str(reason).strip() and type(count) is int and count >= 0
        }

    from lightfee.ops.production_health import analyze_spread_snapshot

    observed_now_ms = int(now_ms or time.time() * 1000)
    spread_health = analyze_spread_snapshot(
        raw_snapshot,
        now_ms=observed_now_ms,
        max_age_ms=sidecar_snapshot_max_age_ms,
    )
    health_fingerprints = list(spread_health.fingerprints)
    contract_fingerprints = {
        "spread_diagnostics_contract_missing",
        "spread_diagnostics_contract_invalid",
        "spread_diagnostics_count_invariant_invalid",
        "spread_publication_watermark_invalid",
    }
    contract_valid = not any(
        fingerprint in contract_fingerprints for fingerprint in health_fingerprints
    )
    input_quote_count = nonnegative_int("input_quote_count")
    valid_quote_count = nonnegative_int("valid_quote_count")
    evaluated_pair_count = nonnegative_int("evaluated_pair_count")
    accepted_pair_count = nonnegative_int("accepted_pair_count")
    paper_tracked_count = nonnegative_int("paper_tracked_count")
    paper_event_count = nonnegative_int("paper_event_count")
    paper_last_success_at_ms = nonnegative_int("paper_last_success_at_ms")
    rejection_counts = stable_counts("rejection_counts")
    paper_rejection_counts = stable_counts("paper_admission_rejection_counts")
    paper_configured_enabled = getattr(snapshot, "paper_configured_enabled", None) is True
    paper_admission_enabled = getattr(snapshot, "paper_admission_enabled", None) is True
    paper_refresh_status = str(getattr(snapshot, "paper_refresh_status", "") or "")
    top_paper_rejection = (
        max(paper_rejection_counts.items(), key=lambda item: (item[1], item[0]))
        if paper_rejection_counts
        else None
    )
    blocking_reasons: list[str] = []
    next_actions: list[str] = []
    if not contract_valid:
        blocking_reasons.append("spread_diagnostics_contract_invalid")
        next_actions.append("repair the spread snapshot diagnostics contract")
    if not paper_configured_enabled:
        blocking_reasons.append("spread_paper_not_configured")
        next_actions.append("enable spread_paper_enabled only after paper prerequisites are ready")
    elif not paper_admission_enabled:
        blocking_reasons.append("spread_paper_admission_disabled")
        next_actions.append("restore paper journal, rollback anchor, and admission prerequisites")
    elif accepted_pair_count > 0 and paper_tracked_count == 0:
        if top_paper_rejection is not None:
            blocking_reasons.append(top_paper_rejection[0])
            next_actions.append(
                f"resolve top paper admission blocker: {top_paper_rejection[0]}"
            )
        else:
            blocking_reasons.append("spread_paper_admission_unexplained_zero")
            next_actions.append("inspect paper admission because accepted signals produced no attribution")
    source = str(getattr(snapshot, "source_mode", "") or "unknown")
    source_state = "current_ok"
    current_degraded = False
    main_age_ms: int | None = None
    if source in {
        "sidecar_snapshot_stale",
        "sidecar_snapshot_unavailable",
        "sidecar_snapshot_quotes_stale",
        "sidecar_snapshot_degraded",
        "spread_bbo_snapshot_unavailable",
        "spread_bbo_snapshot_stale",
        "spread_bbo_snapshot_universe_unavailable",
        "missing_or_malformed",
    }:
        source_state = "current_source_degraded"
        current_degraded = True
    if source == "sidecar_snapshot_stale":
        try:
            from lightfee.sidecar.publisher import load_snapshot

            main_snapshot = load_snapshot(
                Path(runtime_dir) / "opportunity-input-snapshot.json"
            )
        except Exception:
            main_snapshot = None
        observed_now_ms = int(now_ms or time.time() * 1000)
        main_published_ms = int(getattr(main_snapshot, "published_at_ms", 0) or 0)
        if main_published_ms > 0:
            main_age_ms = observed_now_ms - main_published_ms
        if main_age_ms is not None and 0 <= main_age_ms <= sidecar_snapshot_max_age_ms:
            source_state = "transient_stale_recovered"
            current_degraded = False
    return {
        "available": True,
        "path": str(path),
        "spread_sidecar_source": source,
        "spread_sidecar_source_state": source_state,
        "spread_sidecar_current_degraded": current_degraded,
        "main_sidecar_snapshot_age_ms": main_age_ms,
        "diagnostic_contract_status": "complete" if contract_valid else "invalid",
        "health_fingerprints": health_fingerprints,
        "candidate_count": len(getattr(snapshot, "candidates", []) or []),
        "degraded_venues": list(getattr(snapshot, "degraded_venues", []) or []),
        "input_quote_count": input_quote_count,
        "valid_quote_count": valid_quote_count,
        "evaluated_pair_count": evaluated_pair_count,
        "accepted_pair_count": accepted_pair_count,
        "rejection_counts": rejection_counts,
        "paper_configured_enabled": paper_configured_enabled,
        "paper_admission_enabled": paper_admission_enabled,
        "paper_tracked_count": paper_tracked_count,
        "paper_refresh_status": paper_refresh_status,
        "paper_event_count": paper_event_count,
        "paper_last_success_at_ms": paper_last_success_at_ms,
        "paper_admission_rejection_counts": paper_rejection_counts,
        "top_paper_admission_rejection": (
            {
                "reason": top_paper_rejection[0],
                "count": top_paper_rejection[1],
            }
            if top_paper_rejection is not None
            else None
        ),
        "blocking_reasons": blocking_reasons,
        "next_actions": next_actions,
    }

def run_diagnose(
    *,
    runtime_dir: str = DEFAULT_RUNTIME_DIR,
    unit_dir: str = DEFAULT_UNIT_DIR,
    current_state_path: str = "",
    event_paths: list[str] | None = None,
    snapshot_path: str = "",
    symbol: str = "",
    max_events: int = DEFAULT_MAX_EVENTS,
    now_ms: int = 0,
    since_deploy: bool = False,
    venues: list[str] | None = None,
    code_side_blockers: bool = False,
    exclude_strategy: bool = False,
    exclude_liquidity: bool = False,
    hyperliquid_info_coordinator_dir: str = "",
) -> dict[str, Any]:
    generated_at_ms = now_ms or _now_ms()

    # This diagnostic constructs read-only Hyper adapters directly.  Bind them
    # to the service runtime's coordinator before any adapter can issue
    # ``POST /info``; never let a standalone invocation create a CWD-scoped
    # pacing namespace.
    remember_hyperliquid_info_coordinator_directory(
        hyperliquid_info_coordinator_dir
        or (Path(runtime_dir) / "hyperliquid-info-coordinator")
    )

    resolved_state_path, state_source = _resolve_state_path(
        runtime_dir, current_state_path,
    )
    state = _read_json(resolved_state_path)
    state["_state_path"] = str(resolved_state_path)
    state["_state_path_source"] = state_source

    deploy_status = _build_deploy_status(runtime_dir)
    service_status = _build_service_status(unit_dir)
    exchange_truth_env_files_loaded = _load_systemd_environment_files(unit_dir)

    if event_paths:
        event_files = [Path(p) for p in event_paths]
    else:
        event_files = _find_event_files(runtime_dir)

    initial_events: list[dict[str, Any]] = []
    for ef in event_files:
        initial_events.extend(_read_jsonl_tail(ef, max_events))
        if len(initial_events) >= max_events:
            break

    window = _compute_window(
        since_deploy, generated_at_ms, deploy_status, service_status, initial_events,
    )

    event_scan_meta: dict[str, Any] = {
        "event_scan_truncated": False,
        "events_dropped_by_cap": 0,
        "events_before_cap": len(initial_events),
        "since_deploy_time_filtered": False,
    }
    since_ms = int(window.get("since_ms", 0) or 0)
    if since_deploy and since_ms > 0:
        window_events: list[dict[str, Any]] = []
        for ef in event_files:
            window_events.extend(_read_jsonl_since(ef, since_ms))
        if symbol or venues:
            window_events = [
                e for e in window_events
                if _event_matches_scope(e, symbol, venues)
            ]
        all_events, limit_meta = _limit_since_deploy_events(
            window_events,
            max_events,
        )
        event_scan_meta.update(limit_meta)
        event_scan_meta["since_deploy_time_filtered"] = True
        recent_events = list(initial_events)
        if symbol or venues:
            recent_events = [
                e for e in recent_events
                if _event_matches_scope(e, symbol, venues)
            ]
    else:
        all_events = list(initial_events)
        recent_events = list(all_events)
        if symbol or venues:
            all_events = [
                e for e in all_events
                if _event_matches_scope(e, symbol, venues)
            ]
            recent_events = [
                e for e in recent_events
                if _event_matches_scope(e, symbol, venues)
            ]

    health = _build_health(state, service_status)
    local_state = _build_local_state(state, all_events)

    pos_symbols: list[str] = []
    pos_venues: list[str] = []
    for pos in local_state.get("positions", []):
        sym = pos.get("symbol", "")
        if sym and sym not in pos_symbols:
            pos_symbols.append(sym)
        for venue_key in ("long_venue", "short_venue"):
            venue = str(pos.get(venue_key, "") or "").lower()
            if venue and venue not in pos_venues:
                pos_venues.append(venue)
    _event_symbols, event_venues = _exchange_truth_scope_from_events(all_events)
    for venue in event_venues:
        if venue not in pos_venues:
            pos_venues.append(venue)
    if symbol and symbol not in pos_symbols:
        pos_symbols.append(symbol)

    exchange_truth_venues = _exchange_truth_venues_for_diagnose(
        explicit_venues=venues,
        position_venues=pos_venues,
        local_state=local_state,
    )
    required_truth_venues = list(
        exchange_truth_venues or DEFAULT_EXCHANGE_TRUTH_VENUES
    )
    exchange_truth = _build_exchange_truth(
        runtime_dir,
        [],
        exchange_truth_venues,
    )
    exchange_truth = _annotate_exchange_truth_required_venues(
        exchange_truth,
        required_truth_venues,
    )
    hyperliquid_trading_authorization = (
        _build_hyperliquid_trading_authorization_summary(
            local_state,
            exchange_truth,
        )
    )
    hyperliquid_historical_trade_evidence = (
        _build_hyperliquid_historical_trade_evidence(all_events)
    )

    state_consistency = _build_state_consistency(local_state, exchange_truth)
    resolved_order_truth_gap_summary = _build_resolved_order_truth_gap_summary(
        all_events,
        exchange_truth,
        symbol,
    )
    resolved_terminal_zero_qty_summary = _build_resolved_terminal_zero_qty_reduce_only_summary(
        all_events,
        exchange_truth,
        symbol,
    )
    resolved_post_only_reject_summary = _build_resolved_post_only_reject_summary(
        all_events,
        exchange_truth,
        symbol,
    )
    resolved_close_order_error_summary = _build_resolved_close_order_error_summary(
        all_events,
        exchange_truth,
        symbol,
    )
    resolved_contained_entry_admission_summary = (
        _build_resolved_contained_entry_admission_summary(
            all_events,
            exchange_truth,
            symbol,
        )
    )
    duplicate_close_leg_suppressed_summary = (
        _build_duplicate_close_leg_suppressed_summary(all_events)
    )
    entry_admission_cooldown_summary = _build_entry_admission_cooldown_summary(
        all_events
    )
    venue_private_health_summary = _build_venue_private_health_summary(all_events)
    single_leg_exposure_recovery_summary = (
        _build_single_leg_exposure_recovery_summary(all_events)
    )
    cleanup_blocker_summary = _build_cleanup_blocker_summary(all_events)
    order_errors = _build_order_error_evidence(
        all_events,
        symbol,
        resolved_order_truth_gap_summary,
        resolved_terminal_zero_qty_summary,
        resolved_post_only_reject_summary,
        resolved_close_order_error_summary,
        resolved_contained_entry_admission_summary,
    )
    top_exchange_errors = _build_top_exchange_errors(order_errors)
    order_reconcile_identifier_summary = (
        _build_order_reconcile_identifier_summary(all_events)
    )
    l2_evidence = _build_l2_evidence(all_events)
    snapshot_evidence = _build_snapshot_evidence(all_events)
    runtime_warnings = _build_runtime_warnings(all_events)
    spread_sidecar_summary = _build_spread_sidecar_summary(runtime_dir, now_ms=now_ms)
    production_acceptance_gate = _build_production_acceptance_gate(
        all_events,
        local_state,
        exchange_truth,
        state_consistency,
        generated_at_ms,
    )
    business_progression_quality_summary = (
        _build_business_progression_quality_summary(
            all_events,
            production_acceptance_gate=production_acceptance_gate,
        )
    )
    repeated_single_leg_guarded = business_progression_quality_summary.get(
        "repeated_single_leg_guarded",
        {},
    )
    if int(repeated_single_leg_guarded.get("violation_count", 0) or 0) > 0:
        fingerprint = "repeated_single_leg_fee_drag_after_cooldown"
        if fingerprint not in health.get("fingerprints", []):
            health.setdefault("fingerprints", []).append(fingerprint)
        health["critical_count"] = int(health.get("critical_count", 0) or 0) + 1
        health["ok"] = False
    for fingerprint in production_acceptance_gate.get("fingerprints", []) or []:
        if fingerprint not in health.get("fingerprints", []):
            health.setdefault("fingerprints", []).append(fingerprint)
    evidence_completeness = _build_evidence_completeness(
        order_errors, state_consistency, exchange_truth,
    )
    event_coverage = {
        "complete": not bool(event_scan_meta.get("event_scan_truncated")),
        "event_scan_truncated": bool(event_scan_meta.get("event_scan_truncated")),
        "events_before_cap": int(event_scan_meta.get("events_before_cap", 0) or 0),
        "events_dropped_by_cap": int(
            event_scan_meta.get("events_dropped_by_cap", 0) or 0
        ),
    }
    conclusion = _build_conclusion(
        health, state_consistency, evidence_completeness, order_errors,
        l2_evidence, snapshot_evidence, exchange_truth, production_acceptance_gate,
        event_coverage,
    )

    event_counts: dict[str, int] = {}
    for rec in all_events:
        kind = str(rec.get("kind", ""))
        event_counts[kind] = event_counts.get(kind, 0) + 1
    quick_flat_summary = summarize_quick_flat_events(all_events)
    passive_close_terminal_summary = _build_passive_close_terminal_summary(all_events)
    auto_fail_closed_window_summary = build_auto_fail_closed_summary(all_events)
    auto_fail_closed_summary = build_auto_fail_closed_summary(
        recent_events,
        since_ms=max(0, generated_at_ms - 24 * 3600 * 1000),
    )
    stale_risk_state_alignment_summary = _build_stale_risk_state_alignment_summary(
        recent_events,
        since_ms=max(0, generated_at_ms - 24 * 3600 * 1000),
    )
    entry_quantity_terminal_summary = _build_entry_quantity_terminal_summary(
        all_events,
        production_acceptance_gate,
    )
    unpaired_live_position_recovery_summary = (
        _build_unpaired_live_position_recovery_summary(state, all_events)
    )
    business_progression_quality_summary[
        "risk_only_live_single_leg_exposure_count"
    ] = int(
        unpaired_live_position_recovery_summary.get(
            "current_risk_exposure_count",
            0,
        )
        or 0
    )
    resolved_quantity_adjustment_summary = entry_quantity_terminal_summary.get(
        "resolved_quantity_adjustment_summary",
        {},
    )
    quote_rewarm_after_rest_stale_summary = (
        production_acceptance_gate.get("entry_outcome_summary", {}).get(
            "quote_rewarm_after_rest_stale_summary",
            {},
        )
    )
    phase_duration_summary = (
        production_acceptance_gate.get("entry_outcome_summary", {}).get(
            "phase_duration_summary",
            {},
        )
    )
    entry_market_evidence_summary = (
        production_acceptance_gate.get("entry_outcome_summary", {}).get(
            "entry_market_evidence_summary",
            {},
        )
    )
    close_reconciliation_evidence_gap_summary = (
        business_progression_quality_summary.get(
            "close_reconciliation_evidence_gap_summary",
            {},
        )
    )
    diagnostic_noise_summary = _build_diagnostic_noise_summary(
        all_events,
        production_acceptance_gate=production_acceptance_gate,
        business_progression_quality_summary=business_progression_quality_summary,
        resolved_truth_gap_summary=resolved_order_truth_gap_summary,
        resolved_close_order_error_summary=resolved_close_order_error_summary,
        resolved_terminal_zero_qty_summary=resolved_terminal_zero_qty_summary,
    )
    if code_side_blockers or exclude_strategy or exclude_liquidity:
        from scripts.analyze_production_blockers import build_code_side_blocker_view

        code_side_blocker_view = build_code_side_blocker_view(
            all_events,
            exclude_strategy=exclude_strategy,
            exclude_liquidity=exclude_liquidity,
            enabled=True,
        )
    else:
        code_side_blocker_view = {
            "enabled": False,
            "excluded_filters": [],
            "category_counts": {},
            "reason_counts": {},
            "resolution_counts": {},
            "filtered_out_counts": {},
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ms": generated_at_ms,
        "scope": {
            "symbol": symbol or "*",
            "venues": venues or [],
            "since_deploy": since_deploy,
            "max_events": max_events,
            "event_files": [str(ef) for ef in event_files],
            "events_parsed": len(all_events),
            "event_scan_truncated": bool(
                event_scan_meta.get("event_scan_truncated")
            ),
            "events_dropped_by_cap": int(
                event_scan_meta.get("events_dropped_by_cap", 0) or 0
            ),
            "event_coverage": event_coverage,
            "since_deploy_time_filtered": bool(
                event_scan_meta.get("since_deploy_time_filtered")
            ),
            "state_path": str(resolved_state_path),
            "state_path_source": state_source,
        },
        "deploy_status": deploy_status,
        "service_status": service_status,
        "window": window,
        "health": health,
        "lifecycle": str(state.get("lifecycle", "unknown")),
        "risk_mode": str(state.get("risk_mode", "unknown")),
        "local_state": local_state,
        "exchange_truth": exchange_truth,
        "hyperliquid_trading_authorization": hyperliquid_trading_authorization,
        "hyperliquid_historical_trade_evidence": (
            hyperliquid_historical_trade_evidence
        ),
        "exchange_truth_env_files_loaded": exchange_truth_env_files_loaded,
        "state_consistency": state_consistency,
        "resolved_order_truth_gap_summary": resolved_order_truth_gap_summary,
        "resolved_terminal_zero_qty_reduce_only_summary": resolved_terminal_zero_qty_summary,
        "resolved_post_only_reject_summary": resolved_post_only_reject_summary,
        "resolved_close_order_error_summary": resolved_close_order_error_summary,
        "resolved_contained_entry_admission_summary": (
            resolved_contained_entry_admission_summary
        ),
        "entry_admission_cooldown_summary": entry_admission_cooldown_summary,
        "venue_private_health_summary": venue_private_health_summary,
        "single_leg_exposure_recovery_summary": (
            single_leg_exposure_recovery_summary
        ),
        "business_progression_quality_summary": (
            business_progression_quality_summary
        ),
        "cleanup_blocker_summary": cleanup_blocker_summary,
        "duplicate_close_leg_suppressed_summary": duplicate_close_leg_suppressed_summary,
        "order_error_evidence": order_errors,
        "top_exchange_errors": top_exchange_errors,
        "order_reconcile_identifier_summary": order_reconcile_identifier_summary,
        "event_counts": event_counts,
        "quick_flat_summary": quick_flat_summary,
        "passive_close_terminal_summary": passive_close_terminal_summary,
        "auto_fail_closed_summary": auto_fail_closed_summary,
        "auto_fail_closed_window_summary": auto_fail_closed_window_summary,
        "stale_risk_state_alignment_summary": (
            stale_risk_state_alignment_summary
        ),
        "entry_quantity_terminal_summary": entry_quantity_terminal_summary,
        "unpaired_live_position_recovery_summary": (
            unpaired_live_position_recovery_summary
        ),
        "resolved_quantity_adjustment_summary": resolved_quantity_adjustment_summary,
        "entry_market_evidence_summary": entry_market_evidence_summary,
        "close_reconciliation_evidence_gap_summary": (
            close_reconciliation_evidence_gap_summary
        ),
        "diagnostic_noise_summary": diagnostic_noise_summary,
        "quote_rewarm_after_rest_stale_summary": quote_rewarm_after_rest_stale_summary,
        "phase_duration_summary": phase_duration_summary,
        "code_side_blocker_view": code_side_blocker_view,
        "l2_evidence": l2_evidence,
        "snapshot_evidence": snapshot_evidence,
        "runtime_warnings": runtime_warnings,
        "spread_sidecar_summary": spread_sidecar_summary,
        "production_acceptance_gate": production_acceptance_gate,
        "evidence_quality": evidence_completeness,
        "conclusion": conclusion,
    }


def _build_entry_quantity_terminal_summary(
    events: list[dict[str, Any]],
    production_acceptance_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry_residual_dust_tolerated_count = 0
    hedge_quantity_undercut_count = 0
    common_quantity_mismatch_count = 0
    dust_tolerated_positions: set[str] = set()
    tolerated_dust_entry_ids: set[str] = set()
    hedge_undercut_entries: set[str] = set()
    quantity_mismatch_entries: set[str] = set()
    hedge_undercut_warning_entries: set[str] = set()
    quantity_mismatch_warning_entries: set[str] = set()
    quantity_warning_reason_counts: dict[str, int] = {}
    quantity_warning_samples: list[dict[str, Any]] = []
    balanced_opened_entries: set[str] = set()
    terminal_flat_entries: set[str] = set()
    terminal_flat_accounting_gap_entries: set[str] = set()
    residual_repair_completed_entries: set[str] = set()
    residual_repair_completed_symbols: set[str] = set()
    unopened_terminal_entries: set[str] = set()
    resolved_quantity_adjustment_entries: set[str] = set()
    resolved_quantity_adjustment_samples: list[dict[str, Any]] = []
    resolved_planner_quantity_adjustment_count = 0
    resolved_entry_quantity_exchange_step_rounding_count = 0
    resolved_hedge_exchange_step_rounding_count = 0
    current_exchange_truth_clean = bool(
        production_acceptance_gate
        and production_acceptance_gate.get("exchange_truth_flat") is True
        and production_acceptance_gate.get("exchange_truth_no_open_orders") is True
    )

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        entry_id = str(payload.get("entry_id") or payload.get("position_id") or "")
        if kind == "execution.residual_repair_completed":
            if entry_id:
                residual_repair_completed_entries.add(entry_id)
            symbol = str(payload.get("symbol") or "").upper()
            if symbol:
                residual_repair_completed_symbols.add(symbol)
            continue
        if not entry_id:
            continue
        if kind in {"entry.opened", "runtime.position_opened"}:
            long_qty = _safe_float(payload.get("long_quantity"))
            short_qty = _safe_float(payload.get("short_quantity"))
            matched_qty = _safe_float(
                payload.get("matched_quantity") or payload.get("quantity")
            )
            if (
                long_qty > 0.0
                and short_qty > 0.0
                and abs(long_qty - short_qty) <= 1e-9
                and (matched_qty <= 0.0 or abs(matched_qty - long_qty) <= 1e-9)
            ):
                balanced_opened_entries.add(entry_id)
        elif kind == "exit.reconciled":
            long_closed = _safe_float(payload.get("long_closed_qty"))
            short_closed = _safe_float(payload.get("short_closed_qty"))
            if (
                long_closed > 0.0
                and short_closed > 0.0
                and abs(long_closed - short_closed) <= 1e-9
            ):
                terminal_flat_entries.add(entry_id)
            elif contract_passive_close_has_terminal_truth(payload):
                terminal_flat_entries.add(entry_id)
                if payload.get("evidence_gap") is True:
                    terminal_flat_accounting_gap_entries.add(entry_id)
        elif kind == "runtime.position_lifecycle_terminal":
            terminal_state = str(payload.get("terminal_state") or "").lower()
            reason = str(payload.get("reason") or "").lower()
            if "flat" in terminal_state or "flat" in reason:
                terminal_flat_entries.add(entry_id)
        elif kind in {
            "exit.passive_close_resolved",
            "exit.passive_close_fallback_terminal_flat",
            "exit.passive_close_recovery_probe_flat",
        }:
            terminal_flat_entries.add(entry_id)
        elif kind in {
            "entry.aborted",
            "entry.passive_unfilled",
            "reconciliation.entry_abandoned_flat",
            "runtime.entry_admission_blocked",
            "pending_entry.hedge_admission_blocked",
        }:
            unopened_terminal_entries.add(entry_id)

    def add_warning_reason(kind_name: str, payload: dict[str, Any]) -> None:
        reason_family = str(
            payload.get("reason_family")
            or payload.get("quantity_plan_reason")
            or "unknown"
        )
        key = f"{kind_name}:{reason_family}"
        quantity_warning_reason_counts[key] = (
            quantity_warning_reason_counts.get(key, 0) + 1
        )
        entry_id = str(payload.get("entry_id") or payload.get("position_id") or "")
        sample: dict[str, Any] = {
            "entry_id": entry_id,
            "kind": kind_name,
            "reason_family": reason_family,
            "symbol": str(payload.get("symbol") or ""),
        }
        if kind_name == "hedge_quantity_undercut":
            missing = _safe_float(payload.get("missing_hedge_quantity"))
            normalized = _safe_float(payload.get("normalized_quantity"))
            undercut = _safe_float(payload.get("undercut_quantity"))
            if undercut <= 0.0:
                undercut = max(missing - normalized, 0.0)
            sample.update(
                {
                    "missing_hedge_quantity": missing,
                    "normalized_quantity": normalized,
                    "undercut_quantity": undercut,
                }
            )
        else:
            sample.update(
                {
                    "common_quantity": _safe_float(payload.get("common_quantity")),
                    "full_target_quantity": _safe_float(
                        payload.get("full_target_quantity")
                    ),
                }
            )
        quantity_warning_samples.append(sample)

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        if kind != "execution.entry_residual_dust_tolerated":
            continue
        try:
            residual_ratio = float(payload.get("residual_ratio", 0.0) or 0.0)
        except (TypeError, ValueError):
            residual_ratio = 0.0
        terminal_reason = str(payload.get("terminal_reason", "") or "")
        position_id = str(payload.get("position_id") or payload.get("entry_id") or "")
        if (
            position_id
            and residual_ratio <= 0.02 + 1e-12
            and terminal_reason in {
                "exchange_min_quantity_dust",
                "exchange_min_notional_dust",
            }
        ):
            tolerated_dust_entry_ids.add(position_id)

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        if kind == "execution.entry_residual_dust_tolerated":
            entry_residual_dust_tolerated_count += 1
            position_id = str(payload.get("position_id") or payload.get("entry_id") or "")
            if position_id:
                dust_tolerated_positions.add(position_id)
        elif kind == "pending_entry.hedge_quantity_undercut":
            hedge_quantity_undercut_count += 1
            entry_id = str(payload.get("entry_id") or payload.get("position_id") or "")
            if entry_id:
                hedge_undercut_entries.add(entry_id)
                reason_family = str(payload.get("reason_family") or "unknown")
                resolved_rounding = (
                    reason_family == "exchange_step_rounding"
                    and (
                        entry_id in terminal_flat_entries
                        or entry_id in residual_repair_completed_entries
                        or str(payload.get("symbol") or "").upper()
                        in residual_repair_completed_symbols
                    )
                )
                if resolved_rounding:
                    resolved_hedge_exchange_step_rounding_count += 1
                    resolved_quantity_adjustment_entries.add(entry_id)
                    if len(resolved_quantity_adjustment_samples) < 12:
                        resolved_quantity_adjustment_samples.append({
                            "entry_id": entry_id,
                            "kind": "hedge_quantity_undercut",
                            "reason_family": reason_family,
                            "symbol": str(payload.get("symbol") or ""),
                        })
                elif entry_id not in tolerated_dust_entry_ids:
                    hedge_undercut_warning_entries.add(entry_id)
                    add_warning_reason("hedge_quantity_undercut", payload)
        elif kind == "execution.entry_quantity_plan":
            try:
                common_quantity = float(payload.get("common_quantity", 0.0) or 0.0)
                full_target_quantity = float(
                    payload.get("full_target_quantity", 0.0) or 0.0
                )
            except (TypeError, ValueError):
                continue
            if abs(common_quantity - full_target_quantity) > 1e-9:
                common_quantity_mismatch_count += 1
                entry_id = str(payload.get("entry_id") or "")
                if entry_id:
                    quantity_mismatch_entries.add(entry_id)
                    reason_family = str(
                        payload.get("quantity_plan_reason")
                        or payload.get("reason_family")
                        or "unknown"
                    )
                    resolvable_common_adjustment = reason_family in {
                        "planner_quantity_adjustment",
                        "exchange_step_rounding",
                    }
                    opened_terminal_adjustment = (
                        entry_id in balanced_opened_entries
                        and (
                            entry_id in terminal_flat_entries
                            or entry_id in terminal_flat_accounting_gap_entries
                            or entry_id in residual_repair_completed_entries
                            or current_exchange_truth_clean
                        )
                    )
                    unopened_terminal_adjustment = (
                        resolvable_common_adjustment
                        and entry_id in unopened_terminal_entries
                        and entry_id not in balanced_opened_entries
                    )
                    if resolvable_common_adjustment and (
                        opened_terminal_adjustment
                        or unopened_terminal_adjustment
                        or current_exchange_truth_clean
                    ):
                        if reason_family == "planner_quantity_adjustment":
                            resolved_planner_quantity_adjustment_count += 1
                        elif reason_family == "exchange_step_rounding":
                            resolved_entry_quantity_exchange_step_rounding_count += 1
                        resolved_quantity_adjustment_entries.add(entry_id)
                        if len(resolved_quantity_adjustment_samples) < 12:
                            sample = {
                                "entry_id": entry_id,
                                "kind": "common_quantity_mismatch",
                                "reason_family": reason_family,
                                "symbol": str(payload.get("symbol") or ""),
                            }
                            if entry_id in terminal_flat_accounting_gap_entries:
                                sample["terminality"] = "terminal_flat_accounting_gap"
                            elif unopened_terminal_adjustment:
                                sample["terminality"] = "unopened_candidate_terminal"
                            elif entry_id in residual_repair_completed_entries:
                                sample["terminality"] = "residual_repair_completed"
                            elif current_exchange_truth_clean:
                                sample["terminality"] = "current_exchange_truth_clean"
                            resolved_quantity_adjustment_samples.append(sample)
                    elif entry_id not in tolerated_dust_entry_ids:
                        quantity_mismatch_warning_entries.add(entry_id)
                        add_warning_reason("common_quantity_mismatch", payload)

    fingerprints = []
    if production_acceptance_gate:
        fingerprints = list(production_acceptance_gate.get("fingerprints", []) or [])
    lifecycle_release_not_applied_count = (
        1 if "lifecycle_release_not_applied" in fingerprints else 0
    )

    return {
        "entry_residual_dust_tolerated_count": entry_residual_dust_tolerated_count,
        "hedge_quantity_undercut_count": hedge_quantity_undercut_count,
        "hedge_quantity_undercut_warning_count": len(hedge_undercut_warning_entries),
        "common_quantity_mismatch_count": common_quantity_mismatch_count,
        "common_quantity_mismatch_warning_count": len(quantity_mismatch_warning_entries),
        "lifecycle_release_not_applied_count": lifecycle_release_not_applied_count,
        "dust_tolerated_position_ids": sorted(dust_tolerated_positions),
        "hedge_undercut_entry_ids": sorted(hedge_undercut_entries),
        "hedge_quantity_undercut_warning_entry_ids": sorted(
            hedge_undercut_warning_entries
        ),
        "common_quantity_mismatch_entry_ids": sorted(quantity_mismatch_entries),
        "common_quantity_mismatch_warning_entry_ids": sorted(
            quantity_mismatch_warning_entries
        ),
        "quantity_warning_reason_counts": dict(
            sorted(quantity_warning_reason_counts.items())
        ),
        "quantity_warning_samples": sorted(
            quantity_warning_samples,
            key=lambda item: (
                str(item.get("kind") or ""),
                str(item.get("entry_id") or ""),
            ),
        )[:10],
        "resolved_quantity_adjustment_summary": {
            "planner_quantity_adjustment_count": (
                resolved_planner_quantity_adjustment_count
            ),
            "entry_quantity_exchange_step_rounding_count": (
                resolved_entry_quantity_exchange_step_rounding_count
            ),
            "hedge_exchange_step_rounding_count": (
                resolved_hedge_exchange_step_rounding_count
            ),
            "entry_ids": sorted(resolved_quantity_adjustment_entries),
            "samples": sorted(
                resolved_quantity_adjustment_samples,
                key=lambda item: (
                    str(item.get("kind") or ""),
                    str(item.get("entry_id") or ""),
                ),
            )[:10],
        },
    }


def _event_matches_symbol(event: dict[str, Any], symbol: str) -> bool:
    payload = event.get("payload", {})
    if not isinstance(payload, dict):
        return False
    event_symbol = str(payload.get("symbol", "")).upper()
    target = symbol.upper()
    if event_symbol == target:
        return True
    return target in json.dumps(payload, sort_keys=True).upper()


def _build_passive_close_terminal_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    resolved_count = 0
    problem_resolved_count = 0
    single_leg_fast_flatten_count = 0
    passive_owned_drift_blocked_count = 0
    stale_fail_closed_after_flat_count = 0
    terminal_zero_qty_reduce_only_count = 0
    resolved_positions: set[str] = set()
    problem_positions: set[str] = set()
    single_leg_fast_positions: set[str] = set()
    terminal_zero_positions: set[str] = set()

    for rec in events:
        kind = str(rec.get("kind", "") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        position_id = str(payload.get("position_id") or "")
        if kind == "exit.passive_close_resolved":
            resolved_count += 1
            if position_id:
                resolved_positions.add(position_id)
            if bool(payload.get("problem", False)):
                problem_resolved_count += 1
                if position_id:
                    problem_positions.add(position_id)
            if bool(payload.get("single_leg_fast_flatten", False)):
                single_leg_fast_flatten_count += 1
                if position_id:
                    single_leg_fast_positions.add(position_id)
        elif kind == "runtime.position_drift_skipped_passive_close_owner":
            passive_owned_drift_blocked_count += 1
        elif kind == "runtime.stale_fail_closed_cleared":
            stale_fail_closed_after_flat_count += 1
        elif kind == "exit.passive_close_terminal_zero_qty_reduce_only_evidence":
            terminal_zero_qty_reduce_only_count += 1
            if position_id:
                terminal_zero_positions.add(position_id)

    terminal_zero_resolved_positions = terminal_zero_positions & resolved_positions

    return {
        "passive_close_resolved_count": resolved_count,
        "problem_resolved_count": problem_resolved_count,
        "single_leg_fast_flatten_count": single_leg_fast_flatten_count,
        "passive_owned_drift_blocked_count": passive_owned_drift_blocked_count,
        "stale_fail_closed_after_flat_count": stale_fail_closed_after_flat_count,
        "terminal_zero_qty_reduce_only_count": terminal_zero_qty_reduce_only_count,
        "terminal_zero_qty_reduce_only_resolved_count": len(terminal_zero_resolved_positions),
        "resolved_position_ids": sorted(resolved_positions),
        "problem_position_ids": sorted(problem_positions),
        "single_leg_fast_flatten_position_ids": sorted(single_leg_fast_positions),
        "terminal_zero_qty_reduce_only_position_ids": sorted(terminal_zero_positions),
        "terminal_zero_qty_reduce_only_resolved_position_ids": sorted(
            terminal_zero_resolved_positions
        ),
    }


def _build_auto_fail_closed_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    return build_auto_fail_closed_summary(events)


def _event_scope_venues(payload: dict[str, Any]) -> set[str]:
    venues: set[str] = set()
    for key in ("venue", "maker_venue", "hedge_venue", "long_venue", "short_venue"):
        value = payload.get(key)
        if value:
            venues.add(str(value).lower())
    raw_venues = payload.get("venues", []) or []
    if isinstance(raw_venues, str):
        raw_venues = [raw_venues]
    for value in raw_venues:
        if value:
            venues.add(str(value).lower())
    return venues


def _event_matches_scope(
    event: dict[str, Any],
    symbol: str,
    venues: list[str] | None,
) -> bool:
    payload = event.get("payload", {})
    if not isinstance(payload, dict):
        return False

    requested_venues = {str(venue).lower() for venue in (venues or []) if venue}
    event_venues = _event_scope_venues(payload)

    if symbol and _event_matches_symbol(event, symbol):
        return True

    if requested_venues and event_venues and event_venues.isdisjoint(requested_venues):
        return False

    if not symbol:
        return True

    kind = str(event.get("kind", "") or "")
    if kind not in {
        "recovery.live_position_probe_venue_cooldown",
        "recovery.live_position_probe_unsupported_symbols",
        "okx_recovery_probe_noise",
    }:
        return False

    scoped_symbols = _payload_symbol_set(payload)
    if scoped_symbols:
        return symbol.upper() in scoped_symbols

    return bool(event_venues and (not requested_venues or not event_venues.isdisjoint(requested_venues)))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="LightFeeV2 read-only production diagnostics")
    parser.add_argument("--json", action="store_true", default=True,
                       help="Output JSON (default)")
    parser.add_argument("--profile", choices=["full", "operator", "agent", "gate"],
                       default="full",
                       help="Output profile: full, operator, agent, or gate")
    parser.add_argument("--compact-json", action="store_true", default=False,
                       help="Compatibility alias for --profile agent")
    parser.add_argument("--artifact-dir", type=str, default="",
                       help="Optional directory for full artifacts when profile output exceeds budget")
    parser.add_argument("--symbol", type=str, default="",
                       help="Filter by symbol")
    parser.add_argument("--venues", type=str, default="",
                       help="Comma-separated venues for exchange-truth checks")
    parser.add_argument("--since-deploy", action="store_true", default=False,
                       help="Limit to events since last deploy")
    parser.add_argument("--require-gate-pass", action="store_true", default=False,
                       help="Exit nonzero when the production acceptance gate does not pass")
    parser.add_argument("--runtime-dir", type=str, default=DEFAULT_RUNTIME_DIR,
                       help="Runtime directory (default: {})".format(DEFAULT_RUNTIME_DIR))
    parser.add_argument("--hyperliquid-info-coordinator-dir", type=str, default="",
                       help="Shared Hyperliquid /info coordination directory (defaults under --runtime-dir)")
    parser.add_argument("--current-state", type=str, default="",
                       help="Path to live-state-current.json (overrides default)")
    parser.add_argument("--events", type=str, nargs="*", default=None,
                       help="Specific event file(s); auto-discovered if omitted")
    parser.add_argument("--snapshot", type=str, default="",
                       help="Path to opportunity-input-snapshot.json")
    parser.add_argument("--unit-dir", type=str, default=DEFAULT_UNIT_DIR,
                       help="Systemd unit directory (default: {})".format(DEFAULT_UNIT_DIR))
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS,
                       help="Max events to parse (default: {})".format(DEFAULT_MAX_EVENTS))
    parser.add_argument("--now-ms", type=int, default=0,
                       help="Override current time in ms (testing)")
    parser.add_argument("--code-side-blockers", action="store_true",
                       help="Include a code-side blocker view in the JSON report")
    parser.add_argument("--exclude-strategy", action="store_true",
                       help="Filter strategy blockers out of the code-side blocker view")
    parser.add_argument("--exclude-liquidity", action="store_true",
                       help="Filter liquidity and OI blockers out of the code-side blocker view")
    args = parser.parse_args()
    venues = [
        venue.strip().lower()
        for venue in args.venues.split(",")
        if venue.strip()
    ] or None

    result = run_diagnose(
        runtime_dir=args.runtime_dir,
        unit_dir=args.unit_dir,
        current_state_path=args.current_state,
        event_paths=args.events,
        snapshot_path=args.snapshot,
        symbol=args.symbol,
        max_events=args.max_events,
        now_ms=args.now_ms,
        since_deploy=args.since_deploy,
        venues=venues,
        code_side_blockers=args.code_side_blockers,
        exclude_strategy=args.exclude_strategy,
        exclude_liquidity=args.exclude_liquidity,
        hyperliquid_info_coordinator_dir=args.hyperliquid_info_coordinator_dir,
    )

    profile = "agent" if args.compact_json else args.profile
    if profile != "full":
        sys.stdout.write(
            render_budgeted_json(
                result,
                profile=profile,
                artifact_dir=args.artifact_dir or None,
                artifact_name=f"diagnose-{profile}.json",
            )
        )
    elif args.json:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    else:
        _print_summary(result)
    if args.require_gate_pass and result.get("production_acceptance_gate", {}).get(
        "gate_passed"
    ) is not True:
        raise SystemExit(1)


def _print_summary(result: dict[str, Any]) -> None:
    c = result["conclusion"]
    h = result["health"]
    sc = result["state_consistency"]
    eq = result["evidence_quality"]
    ls = result["local_state"]
    w = result.get("window", {})
    et = result.get("exchange_truth", {})

    print("Status: {}  Risk: {}  Confidence: {}".format(c["status"], c["risk"], eq["confidence"]))
    print("Health: {}".format("OK" if h["ok"] else "CRITICAL={} WARN={}".format(h["critical_count"], h["warning_count"])))
    print("Local: {}/{} open={} pending_entry={} pending_close={}".format(
        ls["lifecycle"], ls["risk_mode"],
        ls["open_position_count"], ls["pending_entry_count"], ls["pending_close_count"],
    ))
    print("State path: {} ({})".format(ls.get("state_path", "?"), ls.get("state_path_source", "?")))
    print("Window: mode={} since_ms={} until_ms={} confidence={}".format(
        w.get("mode", "?"), w.get("since_ms", 0), w.get("until_ms", 0), w.get("confidence", "?"),
    ))

    if et.get("available"):
        print("Exchange truth: available venues={} has_position={}".format(
            et.get("available_venues", []), et.get("has_nonzero_position", False),
        ))
    else:
        print("Exchange truth: UNAVAILABLE ({})".format(
            (et.get("errors", ["unknown"]) or ["unknown"])[0][:100]
        ))

    if sc["local_open_exchange_flat"]:
        print("CRITICAL: local_open_exchange_flat")
    elif sc["state_mismatch"]:
        print("STATE MISMATCH: {}".format(sc.get("details", [])))

    if eq["missing_evidence"]:
        print("Missing evidence: {}".format(eq["missing_evidence"]))

    if result["order_error_evidence"]:
        print("Order errors: {} groups".format(len(result["order_error_evidence"])))
        for e in result["order_error_evidence"][:5]:
            has_body = e.get("raw_body_present", False)
            body_flag = " [body]" if has_body else " [no body]"
            print("  {} {} {} HTTP={}: {}{}".format(
                e["kind"], e["venue"], e["symbol"],
                e.get("http_status", "?"), e["error"][:100], body_flag,
            ))
            if e.get("exchange_code"):
                print("    code={} msg={} completeness={} confidence={}".format(
                    e["exchange_code"], e.get("exchange_msg", "")[:80],
                    e.get("evidence_completeness", "?"), e.get("confidence", "?"),
                ))

    if result.get("top_exchange_errors"):
        print("Top exchange errors:")
        for t in result["top_exchange_errors"][:5]:
            print("  {} {} HTTP={} code={}: {} (x{})".format(
                t["venue"], t["symbol"], t["http_status"],
                t["exchange_code"], t["exchange_msg"][:80], t["count"],
            ))

    print("Summary: {}".format(c["summary"]))
    if c["next_actions"]:
        for a in c["next_actions"]:
            print("  -> {}".format(a))


if __name__ == "__main__":
    main()
