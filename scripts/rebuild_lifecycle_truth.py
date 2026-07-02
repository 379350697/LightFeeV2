#!/usr/bin/env python3
"""Rebuild canonical lifecycle truth from production-visible evidence."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lightfee.lifecycle.exchange_truth_ledger import build_exchange_truth_lifecycle  # noqa: E402


DEFAULT_RUNTIME_DIR = Path("runtime")
CORRECTION_DIR = Path("runtime/audits/lifecycle-truth-corrections")


def read_jsonl_events(paths: list[Path]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    events.append(event)
    return events


def discover_event_files(runtime_dir: Path, history: str) -> list[Path]:
    files = sorted(runtime_dir.glob("live-events*.jsonl*"))
    if history == "all":
        files.extend(sorted((runtime_dir / "archive").glob("live-events*.jsonl*")))
    return sorted(set(files), key=lambda path: str(path))


def read_position_ids(path: Path | None) -> list[str] | None:
    if path is None:
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item)]
    if isinstance(payload, dict):
        rows = payload.get("positions") or payload.get("position_ids") or payload.get("excluded_positions")
        if isinstance(rows, list):
            out: list[str] = []
            for row in rows:
                if isinstance(row, dict):
                    value = row.get("position_id") or row.get("entry_id")
                else:
                    value = row
                if value:
                    out.append(str(value))
            return out
    raise SystemExit(f"unsupported positions file shape: {path}")


def _identity_phase(identity: dict[str, Any]) -> str:
    explicit = str(identity.get("phase") or "").lower()
    if explicit in {"open", "close"}:
        return explicit
    text = " ".join(
        str(identity.get(key) or "")
        for key in ("source_kind", "source")
    ).lower()
    if any(token in text for token in ("close", "exit", "backfill", "probe", "truth_gap")):
        return "close"
    if "open" in text or "entry" in text:
        return "open"
    return ""


def _candidate_venue(truth: dict[str, Any], identity: dict[str, Any]) -> str:
    venue = str(identity.get("venue") or "").lower()
    if venue:
        return venue
    leg = str(identity.get("leg") or "").lower()
    if leg == "long":
        return str(truth.get("long_venue") or "").lower()
    if leg == "short":
        return str(truth.get("short_venue") or "").lower()
    return ""


def _iter_order_query_candidates(report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    candidates: list[dict[str, Any]] = []
    stats = {"skipped_already_covered": 0, "skipped_order_filled_source": 0}
    seen: set[tuple[str, str, str, str, str, str]] = set()
    positions = report.get("positions")
    if not isinstance(positions, dict):
        return candidates, stats
    for position_id, truth in sorted(positions.items()):
        if not isinstance(truth, dict):
            continue
        symbol = str(truth.get("symbol") or "").upper()
        identities = truth.get("order_identity_history")
        if not symbol or not isinstance(identities, list):
            continue
        for identity in identities:
            if not isinstance(identity, dict):
                continue
            if str(identity.get("source_kind") or "") == "order.filled":
                stats["skipped_order_filled_source"] += 1
                continue
            phase = _identity_phase(identity)
            if phase not in {"open", "close"}:
                continue
            leg = _candidate_leg(truth, identity)
            if leg not in {"long", "short"}:
                continue
            order_id = str(identity.get("order_id") or "")
            client_order_id = str(identity.get("client_order_id") or "")
            if not order_id and not client_order_id:
                continue
            venue = _candidate_venue(truth, identity)
            if not venue:
                continue
            identity_for_check = dict(identity)
            identity_for_check["leg"] = leg
            if _identity_already_covered(truth, identity_for_check, phase=phase):
                stats["skipped_already_covered"] += 1
                continue
            key = (
                str(position_id),
                phase,
                leg,
                venue,
                order_id,
                client_order_id,
            )
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "position_id": str(position_id),
                    "symbol": symbol,
                    "phase": phase,
                    "leg": leg,
                    "venue": venue,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "source_kind": str(identity.get("source_kind") or ""),
                    "source": str(identity.get("source") or ""),
                }
            )
    return candidates, stats


def _iter_close_query_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    candidates, _ = _iter_order_query_candidates(report)
    return [candidate for candidate in candidates if candidate.get("phase") == "close"]


def _candidate_leg(truth: dict[str, Any], identity: dict[str, Any]) -> str:
    leg = str(identity.get("leg") or "").lower()
    if leg in {"long", "short"}:
        return leg
    venue = str(identity.get("venue") or "").lower()
    if venue and venue == str(truth.get("long_venue") or "").lower():
        return "long"
    if venue and venue == str(truth.get("short_venue") or "").lower():
        return "short"
    return ""


def _identity_already_covered(
    truth: dict[str, Any],
    identity: dict[str, Any],
    *,
    phase: str,
) -> bool:
    leg = str(identity.get("leg") or "").lower()
    if leg not in {"long", "short"}:
        return False
    coverage = truth.get(f"{phase}_coverage")
    if not isinstance(coverage, dict):
        return False
    row = coverage.get(leg)
    if not isinstance(row, dict) or row.get("covered") is not True:
        return False
    order_id = str(identity.get("order_id") or "")
    client_order_id = str(identity.get("client_order_id") or "")
    order_ids = {str(item) for item in row.get("order_ids") or [] if str(item)}
    client_order_ids = {str(item) for item in row.get("client_order_ids") or [] if str(item)}
    return bool((order_id and order_id in order_ids) or (client_order_id and client_order_id in client_order_ids))


def _json_value(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    return value


def _fill_event_from_reconciliation(
    candidate: dict[str, Any],
    fill: Any,
) -> dict[str, Any]:
    metadata = getattr(fill, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    venue = str(_json_value(getattr(fill, "venue", None)) or candidate["venue"]).lower()
    side = str(_json_value(getattr(fill, "side", None)) or "").lower()
    order_id = str(getattr(fill, "order_id", "") or candidate.get("order_id") or "")
    client_order_id = str(
        getattr(fill, "client_order_id", "") or candidate.get("client_order_id") or ""
    )
    ts_ms = int(getattr(fill, "filled_at_ms", 0) or time.time() * 1000)
    phase = str(candidate.get("phase") or "close")
    return {
        "ts_ms": ts_ms,
        "kind": "order.filled",
        "payload": {
            "position_id": candidate["position_id"],
            "symbol": str(getattr(fill, "symbol", "") or candidate["symbol"]).upper(),
            "phase": phase,
            "leg": candidate.get("leg") or "",
            "venue": venue,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "side": side,
            "tradeSide": str(metadata.get("tradeSide") or metadata.get("trade_side") or phase),
            "quantity": getattr(fill, "quantity", 0.0) or 0.0,
            "average_price": getattr(fill, "average_price", 0.0) or 0.0,
            "fee_quote": getattr(fill, "fee_quote", 0.0) or 0.0,
            "filled_at_ms": ts_ms,
            "source": f"rebuild_lifecycle_truth_exchange_query_{phase}",
        },
    }


def _load_exchange_query_helpers() -> tuple[
    Callable[[str], Any],
    Callable[..., Any],
    Callable[[], Any],
    Callable[[], Any],
    Callable[[Any], None],
]:
    from scripts.diagnose_live import (  # noqa: PLC0415
        _create_readonly_adapter,
        _create_readonly_rate_limiter,
        _install_readonly_exchange_truth_rate_limit_runtime,
        _load_venue_credential,
        _restore_readonly_exchange_truth_rate_limit_runtime,
    )

    return (
        _load_venue_credential,
        _create_readonly_adapter,
        _create_readonly_rate_limiter,
        _install_readonly_exchange_truth_rate_limit_runtime,
        _restore_readonly_exchange_truth_rate_limit_runtime,
    )


def _load_exchange_truth_environment(unit_dir: str = "/etc/systemd/system") -> list[str]:
    try:
        from scripts.diagnose_live import _load_systemd_environment_files  # noqa: PLC0415

        return _load_systemd_environment_files(unit_dir)
    except Exception:
        return []


async def query_exchange_fill_events(
    report: dict[str, Any],
    *,
    credential_loader: Callable[[str], Any] | None = None,
    adapter_factory: Callable[..., Any] | None = None,
    rate_limiter_factory: Callable[[], Any] | None = None,
    install_runtime: Callable[[], Any] | None = None,
    restore_runtime: Callable[[Any], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Probe exchange read-only order truth for close identities in a report."""

    if (
        credential_loader is None
        or adapter_factory is None
        or rate_limiter_factory is None
        or install_runtime is None
        or restore_runtime is None
    ):
        (
            credential_loader,
            adapter_factory,
            rate_limiter_factory,
            install_runtime,
            restore_runtime,
        ) = _load_exchange_query_helpers()

    candidates, candidate_stats = _iter_order_query_candidates(report)
    summary: dict[str, Any] = {
        "enabled": True,
        "candidate_count": len(candidates),
        "attempted": 0,
        "filled": 0,
        "not_found": 0,
        "credential_missing": 0,
        "adapter_unavailable": 0,
        "reconciliation_unavailable": 0,
        **candidate_stats,
        "errors": [],
    }
    fill_events: list[dict[str, Any]] = []
    adapter_cache: dict[str, Any] = {}
    missing_credentials: set[str] = set()
    previous_runtime = install_runtime()
    try:
        for candidate in candidates:
            summary["attempted"] += 1
            venue = candidate["venue"]
            if venue in missing_credentials:
                summary["credential_missing"] += 1
                continue
            adapter = adapter_cache.get(venue)
            if adapter is None:
                credential = credential_loader(venue)
                if credential is None:
                    missing_credentials.add(venue)
                    summary["credential_missing"] += 1
                    continue
                adapter = adapter_factory(
                    venue,
                    credential,
                    rate_limiter=rate_limiter_factory(),
                )
                if adapter is None:
                    summary["adapter_unavailable"] += 1
                    continue
                adapter_cache[venue] = adapter
            fetch = getattr(adapter, "fetch_order_fill_reconciliation", None)
            if not callable(fetch):
                summary["reconciliation_unavailable"] += 1
                continue
            try:
                fill = await fetch(
                    candidate["symbol"],
                    candidate["order_id"],
                    candidate["client_order_id"],
                )
            except Exception as exc:  # pragma: no cover - defensive for live adapters.
                summary["errors"].append(
                    {
                        "position_id": candidate["position_id"],
                        "venue": venue,
                        "order_id": candidate["order_id"],
                        "client_order_id": candidate["client_order_id"],
                        "error": str(exc),
                    }
                )
                continue
            if fill is None or float(getattr(fill, "quantity", 0.0) or 0.0) <= 0.0:
                summary["not_found"] += 1
                continue
            fill_events.append(_fill_event_from_reconciliation(candidate, fill))
            summary["filled"] += 1
    finally:
        restore_runtime(previous_runtime)
    return fill_events, summary


def correction_event(position_id: str, truth: dict[str, Any]) -> dict[str, Any]:
    return {
        "seq": 0,
        "run_id": "manual-lifecycle-truth-{}".format(int(time.time() * 1000)),
        "ts_ms": int(time.time() * 1000),
        "kind": "accounting.lifecycle_truth_rebuilt",
        "payload": {
            "position_id": position_id,
            "source": "rebuild_lifecycle_truth",
            "classification": truth.get("classification", ""),
            "project_record_status": truth.get("project_record_status", ""),
            "truth": truth,
        },
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def append_events(runtime_dir: Path, events: list[dict[str, Any]]) -> Path:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    path = runtime_dir / "live-events.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
    return path


def _run_gate(cmd: list[str]) -> tuple[bool, str]:
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return proc.returncode == 0, proc.stdout.strip()[-4000:]


def assert_apply_gates() -> None:
    gates = [
        [sys.executable, "scripts/verify_deploy_manifest.py", "--check", "/opt/lightfee-v2"],
        [sys.executable, "scripts/check_process_singleton.py", "--strict"],
        [sys.executable, "scripts/verify_production_services.py", "--json"],
        [
            sys.executable,
            "scripts/diagnose_live.py",
            "--json",
            "--since-deploy",
            "--max-events",
            "200000",
        ],
    ]
    failures: list[dict[str, str]] = []
    for cmd in gates:
        ok, output = _run_gate(cmd)
        if not ok:
            failures.append({"cmd": " ".join(cmd), "output": output})
    if failures:
        raise SystemExit(json.dumps({"apply_allowed": False, "failures": failures}, indent=2))


def apply_report_blockers(
    report: dict[str, Any],
    *,
    position_ids: list[str] | None,
    expected_complete: int | None = None,
    expected_phantom_zero: int | None = None,
    expected_exchange_bad: int | None = None,
) -> list[str]:
    blockers: list[str] = []
    if position_ids is None:
        blockers.append("positions_file_required_for_apply")
    elif not position_ids:
        blockers.append("positions_file_empty")

    exchange_query = report.get("exchange_query")
    if isinstance(exchange_query, dict) and exchange_query.get("enabled", True):
        if exchange_query.get("errors"):
            blockers.append("exchange_query_errors_present")
        for key in ("credential_missing", "adapter_unavailable", "reconciliation_unavailable"):
            try:
                value = int(exchange_query.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                blockers.append(f"exchange_query_{key}:{value}")

    positions = report.get("positions")
    if isinstance(positions, dict):
        for position_id, truth in sorted(positions.items()):
            if not isinstance(truth, dict):
                continue
            classification = str(truth.get("classification") or "")
            if classification in {
                "exchange_lifecycle_incomplete",
                "evidence_incomplete",
            }:
                blockers.append(f"{classification}:{position_id}")
            pnl = truth.get("pnl") if isinstance(truth.get("pnl"), dict) else {}
            if (
                classification == "exchange_lifecycle_complete"
                and not list(pnl.get("evidence_refs") or [])
            ):
                blockers.append(f"missing_pnl_evidence_refs:{position_id}")

    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    if expected_complete is not None:
        actual = int(summary.get("exchange_lifecycle_complete") or 0)
        if actual != expected_complete:
            blockers.append(f"expected_complete_mismatch:{actual}!={expected_complete}")
    if expected_phantom_zero is not None:
        actual = int(summary.get("phantom_zero_qty_opened") or 0)
        if actual != expected_phantom_zero:
            blockers.append(f"expected_phantom_zero_mismatch:{actual}!={expected_phantom_zero}")
    if expected_exchange_bad is not None:
        actual = int(summary.get("exchange_lifecycle_incomplete") or 0) + int(
            summary.get("evidence_incomplete") or 0
        )
        if actual != expected_exchange_bad:
            blockers.append(f"expected_exchange_bad_mismatch:{actual}!={expected_exchange_bad}")
    return sorted(set(blockers))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    parser.add_argument("--events", type=Path, action="append", default=[])
    parser.add_argument("--positions-file", type=Path)
    parser.add_argument("--history", choices=["current", "all"], default="all")
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--expected-complete", type=int)
    parser.add_argument("--expected-phantom-zero", type=int)
    parser.add_argument("--expected-exchange-bad", type=int)
    parser.add_argument(
        "--query-exchange",
        dest="query_exchange",
        action="store_true",
        default=True,
        help="query read-only exchange order truth for close order identities (default)",
    )
    parser.add_argument(
        "--no-query-exchange",
        dest="query_exchange",
        action="store_false",
        help="rebuild from local event evidence only",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True)
    mode.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    event_files = list(args.events)
    if not event_files:
        event_files = discover_event_files(args.runtime_dir, args.history)
    position_ids = read_position_ids(args.positions_file)
    events = read_jsonl_events(event_files)
    queried_fill_events: list[dict[str, Any]] = []
    exchange_truth_env_files_loaded: list[str] = []
    report = build_exchange_truth_lifecycle(
        events,
        position_ids=set(position_ids or []),
    )
    if args.query_exchange:
        exchange_truth_env_files_loaded = _load_exchange_truth_environment()
        fill_events, exchange_query_summary = asyncio.run(query_exchange_fill_events(report))
        if fill_events:
            queried_fill_events = fill_events
            report = build_exchange_truth_lifecycle(
                events + fill_events,
                position_ids=set(position_ids or []),
            )
        exchange_query_summary["synthetic_fill_event_count"] = len(queried_fill_events)
        report["exchange_query"] = exchange_query_summary
    else:
        report["exchange_query"] = {"enabled": False}
    report["inputs"] = {
        "runtime_dir": str(args.runtime_dir),
        "event_files": [str(path) for path in event_files],
        "positions_file": str(args.positions_file) if args.positions_file else "",
        "position_ids": position_ids or [],
        "dry_run": not args.apply,
        "query_exchange": bool(args.query_exchange),
    }
    report["exchange_truth_env_files_loaded"] = exchange_truth_env_files_loaded
    report["apply_blockers"] = apply_report_blockers(
        report,
        position_ids=position_ids,
        expected_complete=args.expected_complete,
        expected_phantom_zero=args.expected_phantom_zero,
        expected_exchange_bad=args.expected_exchange_bad,
    )

    if args.apply:
        if report["apply_blockers"]:
            raise SystemExit(
                json.dumps(
                    {
                        "apply_allowed": False,
                        "blockers": report["apply_blockers"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
            )
        assert_apply_gates()
        correction_events = [
            correction_event(position_id, truth)
            for position_id, truth in sorted(report.get("positions", {}).items())
            if isinstance(truth, dict)
        ]
        append_items = [*queried_fill_events, *correction_events]
        correction_path = ROOT / CORRECTION_DIR / "{}.json".format(int(time.time() * 1000))
        write_json(correction_path, append_items)
        journal_path = append_events(args.runtime_dir, append_items)
        report["apply"] = {
            "correction_path": str(correction_path),
            "runtime_journal_path": str(journal_path),
            "event_count": len(append_items),
            "queried_fill_event_count": len(queried_fill_events),
            "correction_event_count": len(correction_events),
        }

    if args.output_json:
        write_json(args.output_json, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
