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
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

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
    if not api_key or not api_secret:
        return None
    try:
        from lightfee.venues.transport import LiveCredential
        return LiveCredential(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=os.environ.get(prefix + "API_PASSPHRASE", ""),
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
    except Exception:
        pass
    return None


async def _fetch_venue_positions(
    adapter: Any, symbols: list[str],
) -> tuple[dict[str, Any], set[str], set[str]]:
    """Fetch positions. Returns (positions, succeeded_symbols, failed_symbols)."""
    positions: dict[str, Any] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
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
        except Exception as exc:
            positions[sym] = {"symbol": sym, "error": str(exc)[:200]}
            failed.add(sym)
    return positions, succeeded, failed


async def _fetch_venue_open_orders(
    adapter: Any, symbols: list[str],
) -> tuple[dict[str, Any], set[str], set[str]]:
    """Fetch open orders. Returns (orders, succeeded_symbols, failed_symbols)."""
    orders: dict[str, Any] = {}
    succeeded: set[str] = set()
    failed: set[str] = set()
    transport = getattr(adapter, "_transport", None)
    if transport is None:
        for sym in symbols:
            orders[sym] = {"error": "no transport available"}
            failed.add(sym)
        return orders, succeeded, failed

    venue = str(getattr(adapter, "venue", ""))
    for sym in symbols:
        try:
            if "binance" in venue.lower():
                raw = await transport._request(
                    "GET", "/fapi/v1/openOrders", params={"symbol": sym},
                )
            elif "bybit" in venue.lower():
                raw = await transport._request(
                    "GET", "/v5/order/realtime",
                    params={"category": "linear", "symbol": sym, "settleCoin": "USDT"},
                    private=True,
                )
            elif "aster" in venue.lower():
                raw = await transport._request(
                    "GET", "/fapi/v1/openOrders",
                    params={"symbol": sym},
                    private=True,
                )
            elif "okx" in venue.lower():
                raw = await transport._request(
                    "GET", "/api/v5/trade/orders-pending",
                    params={"instId": sym},
                    private=True,
                )
            else:
                succeeded.add(sym)
                orders[sym] = []
                continue

            order_list = raw if isinstance(raw, list) else raw.get("result", {}).get("list", raw.get("data", []))
            if isinstance(order_list, list) and order_list:
                orders[sym] = []
                for o in order_list[:50]:
                    if isinstance(o, dict):
                        orders[sym].append({
                            "order_id": str(o.get("orderId", o.get("orderLinkId", ""))),
                            "symbol": sym,
                            "side": str(o.get("side", "")),
                            "quantity": float(o.get("origQty", o.get("qty", 0)) or 0),
                            "price": float(o.get("price", 0) or 0),
                            "reduce_only": bool(o.get("reduceOnly", False)),
                        })
            else:
                orders[sym] = []
            succeeded.add(sym)
        except Exception as exc:
            orders[sym] = {"error": str(exc)[:200]}
            failed.add(sym)
    return orders, succeeded, failed


async def _build_exchange_truth_async(
    runtime_dir: str, symbols: list[str],
    venues: list[str] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    all_positions: dict[str, dict[str, Any]] = {}
    all_open_orders: dict[str, dict[str, Any]] = {}
    available_venues: list[str] = []
    fetch_status: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    target_symbols = symbols if symbols else []

    target_venues = venues or ["binance", "bybit"]
    for venue in target_venues:
        credential = _load_venue_credential(venue)
        if credential is None:
            all_positions[venue] = {"error": "no credentials available"}
            all_open_orders[venue] = {"error": "no credentials available"}
            fetch_status[venue] = {
                "status": "no_credentials",
                "positions_succeeded": [],
                "positions_failed": [],
                "orders_succeeded": [],
                "orders_failed": [],
            }
            missing.append("{}_credentials".format(venue))
            continue

        adapter = _create_readonly_adapter(venue, credential)
        if adapter is None:
            errors.append("failed to create {} adapter".format(venue))
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
        try:
            positions, pos_succeeded, pos_failed = await _fetch_venue_positions(
                adapter, target_symbols,
            )
            all_positions[venue] = positions
        except Exception as exc:
            all_positions[venue] = {"error": str(exc)[:300]}
            pos_failed = set(target_symbols)

        # Fetch open orders
        ord_succeeded: set[str] = set()
        ord_failed: set[str] = set()
        try:
            orders, ord_succeeded, ord_failed = await _fetch_venue_open_orders(
                adapter, target_symbols,
            )
            all_open_orders[venue] = orders
        except Exception as exc:
            all_open_orders[venue] = {"error": str(exc)[:300]}
            ord_failed = set(target_symbols)

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

    return {
        "available": any_available,
        "available_venues": available_venues,
        "confidence": confidence,
        "positions": all_positions,
        "open_orders": all_open_orders,
        "has_nonzero_position": has_any_position,
        "has_open_order": has_any_open_order,
        "fetch_status": fetch_status,
        "errors": errors,
        "missing_evidence": missing,
    }


def _build_exchange_truth(
    runtime_dir: str, symbols: list[str],
    venues: list[str] | None = None,
) -> dict[str, Any]:
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    _build_exchange_truth_async(runtime_dir, symbols, venues),
                )
                return future.result(timeout=30)
        return loop.run_until_complete(
            _build_exchange_truth_async(runtime_dir, symbols, venues),
        )
    except RuntimeError:
        return asyncio.run(_build_exchange_truth_async(runtime_dir, symbols, venues))
    except Exception as exc:
        return {
            "available": False,
            "confidence": "low",
            "positions": {},
            "open_orders": {},
            "errors": [str(exc)[:500]],
            "missing_evidence": ["exchange_truth_fetch_failed"],
        }


# ---------------------------------------------------------------------------
# state consistency
# ---------------------------------------------------------------------------

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
    state_mismatch = local_has_positions != exchange_has_positions
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
    if state_mismatch and not local_open_exchange_flat:
        details.append({
            "check": "state_mismatch",
            "ok": False,
            "detail": "local and exchange state diverge (exchange has positions not in local)",
            "evidence_source": "local_state vs exchange_truth",
        })

    if not details:
        details.append({
            "check": "consistency",
            "ok": True,
            "detail": "local and exchange state consistent",
            "evidence_source": "local_state + exchange_truth (confidence=high)",
        })

    return {
        "state_mismatch": state_mismatch,
        "local_open_exchange_flat": local_open_exchange_flat,
        "state_verdict": (
            "local_open_exchange_flat" if local_open_exchange_flat
            else "consistent" if not state_mismatch
            else "state_mismatch"
        ),
        "details": details,
        "confidence": "high",
    }


# ---------------------------------------------------------------------------
# order error evidence
# ---------------------------------------------------------------------------

def _build_order_error_evidence(
    events: list[dict[str, Any]], symbol: str = "",
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
        if kind == "runtime.local_l2_sequence_gap":
            sequence_gap_count += 1
        elif kind == "runtime.local_l2_sync_failed":
            stale_rebuild_count += 1
        elif kind in ("runtime.snapshot_stale", "runtime.snapshot_degraded"):
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
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    if health["ok"] and not state_consistency["state_mismatch"] and not order_errors:
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

    if symbol:
        all_events = [e for e in all_events if _event_matches_symbol(e, symbol)]

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
        pos_venues if pos_venues else None,
    )

    state_consistency = _build_state_consistency(local_state, exchange_truth)
    order_errors = _build_order_error_evidence(all_events, symbol)
    top_exchange_errors = _build_top_exchange_errors(order_errors)
    l2_evidence = _build_l2_evidence(all_events)
    runtime_warnings = _build_runtime_warnings(all_events)
    evidence_completeness = _build_evidence_completeness(
        order_errors, state_consistency, exchange_truth,
    )
    conclusion = _build_conclusion(
        health, state_consistency, evidence_completeness, order_errors,
        l2_evidence, exchange_truth,
    )

    event_counts: dict[str, int] = {}
    for rec in all_events:
        kind = str(rec.get("kind", ""))
        event_counts[kind] = event_counts.get(kind, 0) + 1

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ms": generated_at_ms,
        "scope": {
            "symbol": symbol or "*",
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
        "state_consistency": state_consistency,
        "order_error_evidence": order_errors,
        "top_exchange_errors": top_exchange_errors,
        "event_counts": event_counts,
        "l2_evidence": l2_evidence,
        "runtime_warnings": runtime_warnings,
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="LightFeeV2 read-only production diagnostics")
    parser.add_argument("--json", action="store_true", default=True,
                       help="Output JSON (default)")
    parser.add_argument("--symbol", type=str, default="",
                       help="Filter by symbol")
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
