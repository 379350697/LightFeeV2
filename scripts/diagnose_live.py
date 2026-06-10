#!/usr/bin/env python3
"""Read-only production diagnostics for LightFeeV2 live.

Consumes structured journal events + live state + exchange snapshots
to produce a stable JSON diagnose artifact.  This artifact is consumed
by both local operators and the wlcodex Telegram cockpit — same facts,
same conclusion.

Usage:
  python scripts/diagnose_live.py --json                     # full diagnose
  python scripts/diagnose_live.py --json --symbol BTCUSDT     # filter
  python scripts/diagnose_live.py --json --runtime-dir ./tests/fixtures
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
from lightfee.engine.exchange_truth import normalize_exchange_truth_payload
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.offline.analysis.journal import summarize_quick_flat_events

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
SERVICE_NAMES = ["lightfee-live.service", "lightfee-sidecar.service"]
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
        if earliest == 0 or (ts > 0 and ts < earliest):
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


def _build_deploy_status(runtime_dir: str) -> dict[str, Any]:
    git_head = _git_head()
    commit_time = _git_commit_time()
    deploy_version = _read_deploy_version(runtime_dir)
    mismatch = bool(git_head and deploy_version and git_head != deploy_version)
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
        "open_position_count": int(state.get("open_position_count", 0) or 0),
        "pending_entry_count": int(state.get("pending_entry_count", 0) or 0),
        "pending_close_count": int(state.get("pending_close_count", 0) or 0),
        "positions": positions,
        "last_tick_ms": int(state.get("last_tick_ms", 0) or 0),
        "state_path": state.get("_state_path", ""),
        "state_path_source": state.get("_state_path_source", ""),
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


def _create_readonly_adapter(venue: str, credential: Any) -> Optional[Any]:
    try:
        if venue.lower() == "binance":
            from lightfee.venues.binance import BinanceAdapter
            return BinanceAdapter(mode="live", credential=credential)
        elif venue.lower() == "bybit":
            from lightfee.venues.bybit import BybitAdapter
            return BybitAdapter(mode="live", credential=credential)
        elif venue.lower() == "aster":
            from lightfee.venues.aster import AsterAdapter
            return AsterAdapter(mode="live", credential=credential)
        elif venue.lower() == "okx":
            from lightfee.venues.okx import OkxAdapter
            return OkxAdapter(mode="live", credential=credential)
        elif venue.lower() == "bitget":
            from lightfee.venues.bitget import BitgetAdapter
            return BitgetAdapter(mode="live", credential=credential)
        elif venue.lower() == "gate":
            from lightfee.venues.gate import GateAdapter
            return GateAdapter(mode="live", credential=credential)
        elif venue.lower() == "hyperliquid":
            from lightfee.venues.hyperliquid import HyperliquidAdapter
            return HyperliquidAdapter(mode="live", credential=credential)
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
            _derive_hyperliquid_account_address,
            _normalize_hyperliquid_wallet_mode,
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

    wallet_mode = _normalize_hyperliquid_wallet_mode(
        str(getattr(credential, "wallet_mode", "") or "account_wallet")
    )
    account_address = str(getattr(credential, "account_address", "") or "").strip()
    wallet_private_key = str(getattr(credential, "wallet_private_key", "") or "").strip()
    signer_address = ""
    signer_error = ""
    if wallet_private_key:
        try:
            signer_address = _derive_hyperliquid_account_address(wallet_private_key)
        except Exception as exc:
            signer_error = str(exc)[:160]
    account_matches_signer = (
        bool(account_address)
        and bool(signer_address)
        and account_address.lower() == signer_address.lower()
    )
    identity: dict[str, Any] = {
        "wallet_mode": wallet_mode,
        "account_address_present": bool(account_address),
        "signer_address_present": bool(signer_address),
        "account_matches_signer": account_matches_signer,
        "account_address_masked": _mask_address(account_address),
        "account_address_hash": _address_hash(account_address),
        "signer_address_masked": _mask_address(signer_address),
        "signer_address_hash": _address_hash(signer_address),
    }
    if signer_error:
        identity["signer_address_error"] = signer_error
    return identity


def _probe_venue_symbol(adapter: Any, symbol: str) -> str:
    transport = getattr(adapter, "_transport", None)
    convert = getattr(transport, "_venue_symbol", None)
    if callable(convert):
        try:
            return str(convert(symbol))
        except Exception:
            return symbol
    return symbol


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
    venue_lower = venue.lower()
    if "binance" in venue_lower:
        raw = await transport._request("GET", "/fapi/v1/openOrders", params={}, private=True)
    elif "aster" in venue_lower:
        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        if callable(fetch_open_orders):
            rows = await fetch_open_orders(None)
            return [
                _summarize_open_order(row)
                for row in rows[:50]
                if isinstance(row, dict)
            ]
        raw = await transport._request("GET", "/fapi/v3/openOrders", params={}, private=True)
    elif "bybit" in venue_lower:
        raw = await transport._request(
            "GET", "/v5/order/realtime",
            params={"category": "linear", "settleCoin": "USDT"},
            private=True,
        )
    elif "okx" in venue_lower:
        raw = await transport._request(
            "GET", "/api/v5/trade/orders-pending",
            params={"instType": "SWAP"},
            private=True,
        )
    elif "bitget" in venue_lower:
        raw = await transport._request(
            "GET", "/api/v2/mix/order/orders-pending",
            params={"productType": "USDT-FUTURES"},
            private=True,
        )
    elif "gate" in venue_lower:
        raw = await transport._request(
            "GET", "/api/v4/futures/usdt/orders",
            params={"status": "open"},
            private=True,
        )
    elif "hyperliquid" in venue_lower:
        credential = getattr(transport, "_credential", None)
        account = str(getattr(credential, "account_address", "") or "")
        raw = await transport._request(
            "POST", "/info",
            body={"type": "openOrders", "user": account},
            private=False,
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
        try:
            if "binance" in venue.lower():
                raw = await transport._request(
                    "GET", "/fapi/v1/openOrders", params={"symbol": venue_symbol}, private=True,
                )
            elif "bybit" in venue.lower():
                raw = await transport._request(
                    "GET", "/v5/order/realtime",
                    params={"category": "linear", "symbol": venue_symbol, "settleCoin": "USDT"},
                    private=True,
                )
            elif "aster" in venue.lower():
                fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
                if callable(fetch_open_orders):
                    raw = await fetch_open_orders(venue_symbol)
                else:
                    raw = await transport._request(
                        "GET", "/fapi/v3/openOrders",
                        params={"symbol": venue_symbol},
                        private=True,
                    )
            elif "okx" in venue.lower():
                raw = await transport._request(
                    "GET", "/api/v5/trade/orders-pending",
                    params={"instId": venue_symbol},
                    private=True,
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
    errors: list[str] = []
    all_positions: dict[str, dict[str, Any]] = {}
    all_open_orders: dict[str, dict[str, Any]] = {}
    all_position_probe_evidence: dict[str, dict[str, Any]] = {}
    all_open_order_probe_evidence: dict[str, dict[str, Any]] = {}
    credential_identity: dict[str, dict[str, Any]] = {}
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

        adapter = _create_readonly_adapter(venue, credential)
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

        try:
            await adapter.shutdown()
        except Exception:
            pass

        # Only count venue as available if at least one position OR order query succeeded
        any_success = bool(pos_succeeded) or bool(ord_succeeded)
        any_failure = bool(pos_failed) or bool(ord_failed)

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
        "position_probe_evidence": all_position_probe_evidence,
        "open_order_probe_evidence": all_open_order_probe_evidence,
        "has_nonzero_position": has_any_position,
        "has_open_order": has_any_open_order,
        "fetch_status": fetch_status,
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
    actual = str(actual or "").lower()
    if expected == "long":
        return actual in ("buy", "long")
    if expected == "short":
        return actual in ("sell", "short")
    return False


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
    }


# ---------------------------------------------------------------------------
# order error evidence
# ---------------------------------------------------------------------------

_ORDER_TRUTH_GAP_REGISTERED_KINDS = frozenset({
    "exit.accepted_order_truth_gap_registered",
})
_ORDER_TRUTH_GAP_RESOLUTION_KINDS = frozenset({
    "exit.passive_close_hedge_reconciled_after_error",
    "exit.passive_close_hedge_duplicate_client_order_reconciled",
    "exit.passive_close_fallback_terminal_flat",
    "exit.passive_close_recovery_probe_flat",
    "runtime.position_lifecycle_terminal",
    "recovery.flat",
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
                normalized = str(value).lower()
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


def _payload_is_bybit_duplicate_client_id(payload: dict[str, Any]) -> bool:
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
        or ("110072" in reason)
        or ("orderlinkedid" in exchange_msg and "duplicate" in exchange_msg)
        or ("orderlinkedid" in reason and "duplicate" in reason)
    )


def _truth_gap_resolution_complete(kind: str, payload: dict[str, Any]) -> bool:
    if kind in {
        "exit.passive_close_hedge_reconciled_after_error",
        "exit.passive_close_hedge_duplicate_client_order_reconciled",
    }:
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


def _build_resolved_order_truth_gap_summary(
    events: list[dict[str, Any]],
    exchange_truth: dict[str, Any],
    symbol: str = "",
) -> dict[str, Any]:
    if not (_exchange_truth_flat(exchange_truth) and _exchange_truth_no_open_orders(exchange_truth)):
        return {
            "count": 0,
            "resolved_identities": [],
            "current_exchange_truth_clean": False,
        }

    registered: list[set[str]] = []
    resolved_identity_sets: list[set[str]] = []
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
        if kind in _ORDER_TRUTH_GAP_REGISTERED_KINDS or (
            kind == "order.uncertain" and _payload_is_ack_only_order_truth_gap(payload)
        ):
            registered.append(identities)
        elif (
            (kind in _ORDER_TRUTH_GAP_RESOLUTION_KINDS or kind == "order.reconcile_result")
            and _truth_gap_resolution_complete(kind, payload)
        ):
            resolved_identity_sets.append(identities)

    resolved: set[str] = set()
    matched_count = 0
    for registered_identities in registered:
        matched_resolution = next(
            (
                identities for identities in resolved_identity_sets
                if registered_identities & identities
            ),
            None,
        )
        if matched_resolution is None:
            continue
        matched_count += 1
        resolved.update(registered_identities)
        resolved.update(matched_resolution)

    return {
        "count": matched_count,
        "resolved_identities": sorted(resolved),
        "current_exchange_truth_clean": True,
    }


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
        or _payload_is_bybit_duplicate_client_id(payload)
    ):
        return False
    resolved = set(resolved_truth_gap_summary.get("resolved_identities", []) or [])
    return bool(_truth_gap_identity_values(payload) & resolved)


def _build_order_error_evidence(
    events: list[dict[str, Any]],
    symbol: str = "",
    resolved_truth_gap_summary: dict[str, Any] | None = None,
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
    reason = str(payload.get("reason") or "")
    return (
        kind == "runtime.entry_blocked_ws_bbo_selection"
        or provider == "ws_bbo_quote_lease"
        or reason.startswith("entry_ws_bbo_quote_lease_")
    )


def _build_l2_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    missing_l2_count = 0
    stale_rebuild_count = 0
    sequence_gap_count = 0
    details: list[dict[str, Any]] = []

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

    return {
        "missing_l2_or_tick_count": missing_l2_count,
        "stale_rebuild_count": stale_rebuild_count,
        "sequence_gap_count": sequence_gap_count,
        "details": details[:20],
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


# ---------------------------------------------------------------------------
# production acceptance gate
# ---------------------------------------------------------------------------

def _payload_dict(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _is_snapshot_fallback_blocking(payload: dict[str, Any]) -> bool:
    if _snapshot_fallback_blocking_scope(payload):
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
            residual_key = symbol or key
            if residual_key:
                residual_completed_keys.add(residual_key)
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
    if exchange_truth.get("has_open_order"):
        return False
    for venue_orders in (exchange_truth.get("open_orders") or {}).values():
        if not isinstance(venue_orders, dict):
            continue
        for orders in venue_orders.values():
            if isinstance(orders, list) and orders:
                return False
            if isinstance(orders, dict) and not orders.get("error"):
                return False
    return True


def _build_production_acceptance_gate(
    events: list[dict[str, Any]],
    local_state: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    fill_ratios: list[float] = []
    passive_maker_zero_fill_count = 0
    abort_fail_closed_count = 0
    okx_recovery_probe_rate_limited_count = 0
    okx_instrument_missing_skipped_count = 0
    local_l2_official_rebuild_count = 0
    snapshot_fallback_blocking_count = 0
    bulk_health_diagnostic_count = 0
    contained_admission_count = 0
    required_position_truth_unavailable_count = 0
    entry_opened_count = 0
    position_opened_count = 0
    residual_count = 0
    exception_conclusions: dict[str, str] = {}

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

        if kind == "runtime.entry_admission_venue_degraded":
            reason_text = f"{reason} {payload.get('source', '')}".lower()
            if (
                payload.get("block_scope") == "venue"
                and payload.get("evidence_gap") is False
                and (
                    "admission" in reason_text
                    or "insufficient_margin" in reason_text
                )
            ):
                contained_admission_count += 1
                exception_conclusions["contained_admission"] = "contained_admission"

        if kind in ("runtime.local_l2_sequence_gap_rebuild", "runtime.local_l2_snapshot_error"):
            if _has_official_sequence_rebuild_evidence(payload):
                local_l2_official_rebuild_count += 1
                exception_conclusions["local_l2_official_rebuild"] = "official_doc"
            else:
                exception_conclusions.setdefault("local_l2_official_rebuild", "insufficient_evidence")

        if kind == "runtime.snapshot_fallback_last_good" and _is_snapshot_fallback_blocking(payload):
            snapshot_fallback_blocking_count += 1
            exception_conclusions["snapshot_fallback_blocking"] = (
                _snapshot_fallback_exception_conclusion(payload)
            )

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
    recovery_lifecycle = _build_recovery_lifecycle_summary(events)
    open_position_count = int(local_state.get("open_position_count", 0) or 0)
    pending_entry_count = int(local_state.get("pending_entry_count", 0) or 0)
    pending_close_count = int(local_state.get("pending_close_count", 0) or 0)
    pending_residual_repair_count = int(
        local_state.get(
            "pending_residual_repair_count",
            len(local_state.get("pending_residual_repairs", []) or []),
        )
        or 0
    )
    exchange_truth_flat = _exchange_truth_flat(exchange_truth)
    exchange_truth_no_open_orders = _exchange_truth_no_open_orders(exchange_truth)
    recovery_decision = _recovery_decision_payload(local_state, exchange_truth)
    local_recovery_clean = (
        open_position_count == 0
        and pending_entry_count == 0
        and pending_close_count == 0
        and pending_residual_repair_count == 0
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
    fingerprints: list[str] = []
    if (
        current_core_clean
        and str(local_state.get("lifecycle", "") or "").lower() == "risk_only"
    ):
        fingerprints.append("lifecycle_release_not_applied")
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
    if resolved_order_truth_gap_count and current_core_clean:
        exception_conclusions["resolved_order_truth_gap"] = (
            "closed_by_current_exchange_truth"
        )
    residual_lifecycle_closed_by_recovery = (
        residual_count > 0
        and local_recovery_clean
        and exchange_recovery_clean
        and recovery_lifecycle["unclosed_residual_lifecycle_count"] == 0
    )
    if (
        (entry_opened_count or position_opened_count)
        and current_core_clean
        and residual_lifecycle_closed_by_recovery
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

    blocking_reasons: list[str] = []
    if open_position_count:
        blocking_reasons.append("local_open_positions_present")
    if pending_entry_count or pending_close_count:
        blocking_reasons.append("local_pending_entries_or_closes_present")
    if pending_residual_repair_count:
        blocking_reasons.append("local_pending_residual_repairs_present")
    if residual_count and not residual_lifecycle_closed:
        blocking_reasons.append("residual_events_present")
    if quick_flat_count:
        blocking_reasons.append("quick_flat_events_present")
    if not exchange_truth.get("available"):
        blocking_reasons.append("exchange_truth_unavailable")
    else:
        if not exchange_truth_flat:
            blocking_reasons.append("exchange_truth_nonzero_position")
        if not exchange_truth_no_open_orders:
            blocking_reasons.append("exchange_truth_open_orders_present")
    if (entry_opened_count or position_opened_count) and not trade_lifecycle_closed:
        blocking_reasons.append("entry_or_position_opened_without_fixture_finalized_evidence")
    if required_position_truth_unavailable_count and exception_conclusions.get(
        "blocking_required_truth"
    ) == "blocking_required_truth":
        blocking_reasons.append("blocking_required_truth")

    diagnostic_counts = {
        "passive_maker_zero_fill": passive_maker_zero_fill_count,
        "abort_fail_closed": abort_fail_closed_count,
        "okx_recovery_probe_rate_limited": okx_recovery_probe_rate_limited_count,
        "okx_instrument_missing_skipped": okx_instrument_missing_skipped_count,
        "local_l2_official_rebuild": local_l2_official_rebuild_count,
        "snapshot_fallback_blocking": snapshot_fallback_blocking_count,
        "nonblocking_health_diagnostic": bulk_health_diagnostic_count,
        "contained_admission": contained_admission_count,
        "resolved_order_truth_gap": resolved_order_truth_gap_count,
        "blocking_required_truth": (
            required_position_truth_unavailable_count
            if exception_conclusions.get("blocking_required_truth")
            == "blocking_required_truth"
            else 0
        ),
    }
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
        "snapshot_fallback_blocking_count": snapshot_fallback_blocking_count,
        "bulk_health_diagnostic_count": bulk_health_diagnostic_count,
        "contained_admission_count": contained_admission_count,
        "resolved_order_truth_gap_count": resolved_order_truth_gap_count,
        "resolved_order_truth_gap_summary": resolved_order_truth_gap_summary,
        "required_position_truth_unavailable_count": (
            required_position_truth_unavailable_count
        ),
        "entry_opened_count": entry_opened_count,
        "position_opened_count": position_opened_count,
        "open_position_count": open_position_count,
        "pending_entry_count": pending_entry_count,
        "pending_close_count": pending_close_count,
        "pending_residual_repair_count": pending_residual_repair_count,
        "residual_count": residual_count,
        "quick_flat_count": quick_flat_count,
        "quick_flat_duplicate_event_count": int(
            quick_flat_summary.get("duplicate_event_count", 0) or 0
        ),
        "closed_trade_lifecycle_count": recovery_lifecycle["closed_trade_lifecycle_count"],
        "unclosed_trade_lifecycle_count": recovery_lifecycle["unclosed_trade_lifecycle_count"],
        "closed_residual_lifecycle_count": recovery_lifecycle["closed_residual_lifecycle_count"],
        "unclosed_residual_lifecycle_count": recovery_lifecycle["unclosed_residual_lifecycle_count"],
        "recovery_lifecycle": recovery_lifecycle,
        "exchange_truth_flat": exchange_truth_flat,
        "exchange_truth_no_open_orders": exchange_truth_no_open_orders,
        "recovery_decision": recovery_decision,
        "fingerprints": fingerprints,
        "exception_conclusions": exception_conclusions,
        "unclassified_exceptions": unclassified_exceptions,
        "insufficient_evidence_exceptions": insufficient_evidence_exceptions,
        "blocking_reasons": blocking_reasons,
        "gate_passed": not blocking_reasons,
    }


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

def _build_conclusion(
    health: dict[str, Any],
    state_consistency: dict[str, Any],
    evidence_completeness: dict[str, Any],
    order_errors: list[dict[str, Any]],
    l2_evidence: dict[str, Any],
    snapshot_evidence: dict[str, Any],
    exchange_truth: dict[str, Any],
    production_acceptance_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gate_failed = (
        production_acceptance_gate is not None
        and production_acceptance_gate.get("gate_passed") is False
    )
    gate_blockers = (
        list(production_acceptance_gate.get("blocking_reasons", []) or [])
        if production_acceptance_gate is not None
        else []
    )

    if gate_failed:
        status = "unhealthy"
        risk = "high"
    elif health["ok"] and not state_consistency["state_mismatch"] and not order_errors:
        status = "healthy"
        risk = "low"
    elif health["critical_count"] > 0:
        status = "unhealthy"
        risk = "high"
    elif state_consistency["local_open_exchange_flat"]:
        status = "critical"
        risk = "high"
    elif state_consistency["state_mismatch"]:
        status = "degraded"
        risk = "high"
    elif order_errors:
        status = "degraded"
        has_rejected = any(e.get("kind") == "order.rejected" for e in order_errors)
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
    if order_errors:
        total_errs = sum(e.get("count", 0) for e in order_errors)
        summary_parts.append("{} order error groups ({} total)".format(
            len(order_errors), total_errs,
        ))
    if evidence_completeness["overall"] != "complete":
        summary_parts.append("evidence: {}".format(evidence_completeness["overall"]))
    if gate_failed:
        summary_parts.append(
            "production acceptance gate failed: {}".format(
                ", ".join(gate_blockers[:5]) if gate_blockers else "unknown"
            )
        )

    summary = "; ".join(summary_parts) if summary_parts else "no issues detected"

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
    if order_errors:
        top = _build_top_exchange_errors(order_errors)
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
) -> dict[str, Any]:
    generated_at_ms = now_ms or _now_ms()

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

    all_events: list[dict[str, Any]] = []
    for ef in event_files:
        all_events.extend(_read_jsonl_tail(ef, max_events))
        if len(all_events) >= max_events:
            break

    window = _compute_window(
        since_deploy, generated_at_ms, deploy_status, service_status, all_events,
    )

    since_ms = window.get("since_ms", 0)
    if since_ms > 0:
        all_events = [e for e in all_events if int(e.get("ts_ms", 0) or 0) >= since_ms]

    if symbol or venues:
        all_events = [e for e in all_events if _event_matches_scope(e, symbol, venues)]

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
    if symbol and symbol not in pos_symbols:
        pos_symbols.append(symbol)

    exchange_truth = _build_exchange_truth(
        runtime_dir,
        pos_symbols if pos_symbols else [],
        venues if venues is not None else (pos_venues if pos_venues else None),
    )

    state_consistency = _build_state_consistency(local_state, exchange_truth)
    resolved_order_truth_gap_summary = _build_resolved_order_truth_gap_summary(
        all_events,
        exchange_truth,
        symbol,
    )
    order_errors = _build_order_error_evidence(
        all_events,
        symbol,
        resolved_order_truth_gap_summary,
    )
    top_exchange_errors = _build_top_exchange_errors(order_errors)
    l2_evidence = _build_l2_evidence(all_events)
    snapshot_evidence = _build_snapshot_evidence(all_events)
    runtime_warnings = _build_runtime_warnings(all_events)
    production_acceptance_gate = _build_production_acceptance_gate(
        all_events, local_state, exchange_truth,
    )
    for fingerprint in production_acceptance_gate.get("fingerprints", []) or []:
        if fingerprint not in health.get("fingerprints", []):
            health.setdefault("fingerprints", []).append(fingerprint)
    evidence_completeness = _build_evidence_completeness(
        order_errors, state_consistency, exchange_truth,
    )
    conclusion = _build_conclusion(
        health, state_consistency, evidence_completeness, order_errors,
        l2_evidence, snapshot_evidence, exchange_truth, production_acceptance_gate,
    )

    event_counts: dict[str, int] = {}
    for rec in all_events:
        kind = str(rec.get("kind", ""))
        event_counts[kind] = event_counts.get(kind, 0) + 1
    quick_flat_summary = summarize_quick_flat_events(all_events)

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
        "exchange_truth_env_files_loaded": exchange_truth_env_files_loaded,
        "state_consistency": state_consistency,
        "resolved_order_truth_gap_summary": resolved_order_truth_gap_summary,
        "order_error_evidence": order_errors,
        "top_exchange_errors": top_exchange_errors,
        "event_counts": event_counts,
        "quick_flat_summary": quick_flat_summary,
        "l2_evidence": l2_evidence,
        "snapshot_evidence": snapshot_evidence,
        "runtime_warnings": runtime_warnings,
        "production_acceptance_gate": production_acceptance_gate,
        "evidence_quality": evidence_completeness,
        "conclusion": conclusion,
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
    parser.add_argument("--symbol", type=str, default="",
                       help="Filter by symbol")
    parser.add_argument("--venues", type=str, default="",
                       help="Comma-separated venues for exchange-truth checks")
    parser.add_argument("--since-deploy", action="store_true", default=False,
                       help="Limit to events since last deploy")
    parser.add_argument("--runtime-dir", type=str, default=DEFAULT_RUNTIME_DIR,
                       help="Runtime directory (default: {})".format(DEFAULT_RUNTIME_DIR))
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
    )

    if args.json:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    else:
        _print_summary(result)


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
