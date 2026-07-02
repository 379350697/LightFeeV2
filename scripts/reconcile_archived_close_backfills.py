#!/usr/bin/env python3
"""Read-only close statement backfill helper for archived passive-close gaps."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lightfee.core.domain import Venue  # noqa: E402
from scripts.diagnose_live import (  # noqa: E402
    _create_readonly_adapter,
    _create_readonly_rate_limiter,
    _install_readonly_exchange_truth_rate_limit_runtime,
    _load_venue_credential,
    _restore_readonly_exchange_truth_rate_limit_runtime,
)


DEFAULT_RUNTIME_DIR = Path("/opt/lightfee-v2/runtime")
CORRECTION_DIR = Path("runtime/audits/close-backfill-corrections")

KNOWN_BACKFILLS: dict[str, dict[str, Any]] = {
    "entry-1782849183829-LABUSDT": {
        "symbol": "LABUSDT",
        "long_venue": "bitget",
        "short_venue": "okx",
        "long_candidates": [
            {"order_id": "1455942280312156163", "source": "accepted_order_truth_gap"},
            {"order_id": "1455947295294648321", "source": "accepted_order_truth_gap"},
        ],
        "short_candidates": [
            {"order_id": "3702637960104878080", "source": "exit_reconciled"},
        ],
    },
    "entry-1782867317803-INUSDT": {
        "symbol": "INUSDT",
        "long_venue": "binance",
        "short_venue": "bybit",
        "long_candidates": [
            {"order_id": "1095052700", "source": "exit_reconciled"},
        ],
        "short_candidates": [
            {
                "order_id": "ba8d6524-3bac-4fa3-a3d8-91ff799bff6f",
                "source": "maker_submitted_statement_probe",
            },
        ],
    },
    "entry-1782982448575-LABUSDT": {
        "symbol": "LABUSDT",
        "long_venue": "bitget",
        "short_venue": "okx",
        "long_candidates": [
            {"order_id": "1456500944320229434", "source": "accepted_order_truth_gap"},
            {"order_id": "1456505976935575553", "source": "accepted_order_truth_gap"},
        ],
        "short_candidates": [
            {"order_id": "3707107089734017024", "source": "exit_reconciled"},
        ],
    },
}


def _json_default(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return str(value)


def _iter_event_files(runtime_dir: Path) -> list[Path]:
    if not runtime_dir.exists():
        return []
    return sorted(runtime_dir.glob("live-events*.jsonl"))


def _event_payload(raw: str) -> dict[str, Any] | None:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(event, dict):
        return None
    payload = event.get("payload")
    if isinstance(payload, dict):
        item = dict(payload)
        item.setdefault("kind", str(event.get("kind") or ""))
        item.setdefault("ts_ms", int(event.get("ts_ms") or 0))
        return item
    return event


def _scan_position_events(runtime_dir: Path, position_id: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in _iter_event_files(runtime_dir):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    if position_id not in raw:
                        continue
                    payload = _event_payload(raw)
                    if not isinstance(payload, dict):
                        continue
                    if str(payload.get("position_id") or "") == position_id:
                        events.append(payload)
        except OSError:
            continue
    return events


def _position_snapshot_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    for payload in events:
        if str(payload.get("kind") or "") not in {"entry.opened", "runtime.position_opened"}:
            continue
        return dict(payload)
    for payload in events:
        snapshot = payload.get("position_snapshot")
        if isinstance(snapshot, dict):
            return dict(snapshot)
    return {}


def _already_backfilled(events: list[dict[str, Any]]) -> bool:
    for payload in events:
        if str(payload.get("source") or "") == "reconcile_archived_close_backfills":
            return True
        if str(payload.get("backfill_source") or "") == "reconcile_archived_close_backfills":
            return True
    return False


def _merge_event_candidates(plan: dict[str, Any], events: list[dict[str, Any]]) -> None:
    by_leg = {"long": plan["long_candidates"], "short": plan["short_candidates"]}
    seen = {
        (leg, str(item.get("order_id") or ""), str(item.get("client_order_id") or ""))
        for leg, items in by_leg.items()
        for item in items
    }
    for payload in events:
        for leg, key in (("long", "long_legs"), ("short", "short_legs")):
            legs = payload.get(key)
            if not isinstance(legs, list):
                continue
            for record in legs:
                if not isinstance(record, dict):
                    continue
                order_id = str(record.get("order_id") or "")
                client_order_id = str(record.get("client_order_id") or "")
                if not order_id and not client_order_id:
                    continue
                identity = (leg, order_id, client_order_id)
                if identity in seen:
                    continue
                by_leg[leg].append(
                    {
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "source": str(record.get("source") or "runtime_event"),
                    }
                )
                seen.add(identity)


async def _fetch_leg_fills(
    *,
    venue: str,
    symbol: str,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    credential = _load_venue_credential(venue)
    if credential is None:
        return [
            {
                **candidate,
                "status": "credential_missing",
            }
            for candidate in candidates
        ]
    adapter = _create_readonly_adapter(
        venue,
        credential,
        rate_limiter=_create_readonly_rate_limiter(),
    )
    if adapter is None:
        return [{**candidate, "status": "adapter_unavailable"} for candidate in candidates]
    fetch = getattr(adapter, "fetch_order_fill_reconciliation", None)
    if not callable(fetch):
        return [{**candidate, "status": "reconciliation_unavailable"} for candidate in candidates]

    results: list[dict[str, Any]] = []
    for candidate in candidates:
        order_id = str(candidate.get("order_id") or "")
        client_order_id = str(candidate.get("client_order_id") or "")
        try:
            fill = await fetch(symbol, order_id, client_order_id)
        except Exception as exc:
            results.append({**candidate, "status": "error", "error": str(exc)})
            continue
        if fill is None:
            results.append({**candidate, "status": "not_found"})
            continue
        results.append(
            {
                **candidate,
                "status": "filled",
                "venue": getattr(getattr(fill, "venue", None), "value", venue),
                "symbol": getattr(fill, "symbol", symbol),
                "side": getattr(getattr(fill, "side", None), "value", ""),
                "quantity": float(getattr(fill, "quantity", 0.0) or 0.0),
                "average_price": float(getattr(fill, "average_price", 0.0) or 0.0),
                "fee_quote": float(getattr(fill, "fee_quote", 0.0) or 0.0),
                "filled_at_ms": int(getattr(fill, "filled_at_ms", 0) or 0),
            }
        )
    return results


def _filled_quantity(rows: list[dict[str, Any]]) -> float:
    return sum(
        float(row.get("quantity") or 0.0)
        for row in rows
        if row.get("status") == "filled"
    )


def _aggregate_fills(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fills = [row for row in rows if row.get("status") == "filled"]
    quantity = sum(float(row.get("quantity") or 0.0) for row in fills)
    notional = sum(
        float(row.get("quantity") or 0.0) * float(row.get("average_price") or 0.0)
        for row in fills
    )
    return {
        "quantity": quantity,
        "average_price": notional / quantity if quantity > 0.0 else 0.0,
        "fee_quote": sum(float(row.get("fee_quote") or 0.0) for row in fills),
        "order_id": ",".join(str(row.get("order_id") or "") for row in fills if row.get("order_id")),
        "client_order_id": ",".join(
            str(row.get("client_order_id") or "")
            for row in fills
            if row.get("client_order_id")
        ),
        "legs": fills,
    }


def _corrected_exit_payload(
    position_id: str,
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    long = _aggregate_fills(result.get("long_results", []))
    short = _aggregate_fills(result.get("short_results", []))
    long_entry = float(snapshot.get("long_entry_price") or 0.0)
    short_entry = float(snapshot.get("short_entry_price") or 0.0)
    funding_quote = float(
        snapshot.get("captured_funding_quote")
        or snapshot.get("funding_pnl_quote")
        or 0.0
    )
    entry_fee = float(
        snapshot.get("total_entry_fee_quote")
        or snapshot.get("entry_fee_quote")
        or 0.0
    )
    price_pnl = (
        (float(long["average_price"]) - long_entry) * float(long["quantity"])
    ) + (
        (short_entry - float(short["average_price"])) * float(short["quantity"])
    )
    exit_fee = float(long["fee_quote"]) + float(short["fee_quote"])
    now_ms = int(time.time() * 1000)
    return {
        "position_id": position_id,
        "symbol": result.get("symbol", ""),
        "kind": "final",
        "reason": "historical_close_statement_backfill",
        "closed_at_ms": now_ms,
        "reconciled_at_ms": now_ms,
        "long_venue": result.get("long_venue", ""),
        "short_venue": result.get("short_venue", ""),
        "long_closed_qty": float(long["quantity"]),
        "short_closed_qty": float(short["quantity"]),
        "long_average_price": float(long["average_price"]),
        "short_average_price": float(short["average_price"]),
        "long_order_id": long["order_id"],
        "short_order_id": short["order_id"],
        "long_client_order_id": long["client_order_id"],
        "short_client_order_id": short["client_order_id"],
        "long_legs": long["legs"],
        "short_legs": short["legs"],
        "price_pnl": price_pnl,
        "funding_pnl_quote": funding_quote,
        "entry_fee_quote": entry_fee,
        "exit_fee_quote": exit_fee,
        "net_quote": price_pnl + funding_quote - entry_fee - exit_fee,
        "venue_statement_reconciled": True,
        "evidence_gap": False,
        "candidate_owner_id": position_id,
        "missing_leg": "none",
        "pending_backfill": False,
        "accounting_status": "complete",
        "clean_accounting_ready": True,
        "source": "reconcile_archived_close_backfills",
        "backfill_source": "reconcile_archived_close_backfills",
        "position_snapshot": snapshot,
    }


def _order_filled_events_from_exit_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for leg_name, key in (("long", "long_legs"), ("short", "short_legs")):
        for row in payload.get(key, []):
            if not isinstance(row, dict) or row.get("status") != "filled":
                continue
            events.append(
                {
                    "position_id": payload.get("position_id", ""),
                    "symbol": payload.get("symbol", ""),
                    "venue": row.get("venue", ""),
                    "leg": leg_name,
                    "order_id": row.get("order_id", ""),
                    "client_order_id": row.get("client_order_id", ""),
                    "side": row.get("side", ""),
                    "quantity": row.get("quantity", 0.0),
                    "average_price": row.get("average_price", 0.0),
                    "fee_quote": row.get("fee_quote", 0.0),
                    "filled_at_ms": row.get("filled_at_ms", 0),
                    "source": "reconcile_archived_close_backfills",
                    "backfill_source": "reconcile_archived_close_backfills",
                }
            )
    return events


def _correction_event(position_id: str, result: dict[str, Any]) -> dict[str, Any]:
    return {
        "ts_ms": int(time.time() * 1000),
        "kind": "accounting.close_statement_backfill_corrected",
        "payload": {
            "position_id": position_id,
            "symbol": result.get("symbol", ""),
            "source": "reconcile_archived_close_backfills",
            "accounting_status": "complete" if result.get("complete") else "pending_backfill",
            "long_fills": [
                row for row in result.get("long_results", []) if row.get("status") == "filled"
            ],
            "short_fills": [
                row for row in result.get("short_results", []) if row.get("status") == "filled"
            ],
            "evidence": {
                "long_filled_quantity": result.get("long_filled_quantity", 0.0),
                "short_filled_quantity": result.get("short_filled_quantity", 0.0),
                "events_scanned": result.get("events_scanned", 0),
            },
        },
    }


def _append_runtime_events(runtime_dir: Path, events: list[dict[str, Any]]) -> Path:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    path = runtime_dir / "live-events.jsonl"
    run_id = "manual-close-backfill-{}".format(int(time.time() * 1000))
    with path.open("a", encoding="utf-8") as handle:
        for index, event in enumerate(events, start=1):
            record = {
                "seq": 0,
                "run_id": run_id,
                "ts_ms": int(event.get("ts_ms") or time.time() * 1000),
                "kind": event["kind"],
                "payload": event.get("payload", {}),
                "manual_backfill": True,
                "manual_backfill_seq": index,
            }
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
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


def _assert_apply_gates() -> None:
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


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.position_id not in KNOWN_BACKFILLS:
        raise SystemExit("unknown position_id; pass one of: {}".format(", ".join(KNOWN_BACKFILLS)))
    plan = json.loads(json.dumps(KNOWN_BACKFILLS[args.position_id]))
    runtime_dir = Path(args.runtime_dir)
    events = _scan_position_events(runtime_dir, args.position_id)
    _merge_event_candidates(plan, events)
    snapshot = _position_snapshot_from_events(events)

    previous_runtime = _install_readonly_exchange_truth_rate_limit_runtime()
    try:
        long_results, short_results = await asyncio.gather(
            _fetch_leg_fills(
                venue=plan["long_venue"],
                symbol=plan["symbol"],
                candidates=plan["long_candidates"],
            ),
            _fetch_leg_fills(
                venue=plan["short_venue"],
                symbol=plan["symbol"],
                candidates=plan["short_candidates"],
            ),
        )
    finally:
        _restore_readonly_exchange_truth_rate_limit_runtime(previous_runtime)

    result = {
        "position_id": args.position_id,
        "symbol": plan["symbol"],
        "dry_run": not args.apply,
        "runtime_dir": str(runtime_dir),
        "events_scanned": len(events),
        "long_venue": plan["long_venue"],
        "short_venue": plan["short_venue"],
        "long_results": long_results,
        "short_results": short_results,
        "long_filled_quantity": _filled_quantity(long_results),
        "short_filled_quantity": _filled_quantity(short_results),
        "already_backfilled": _already_backfilled(events),
    }
    result["complete"] = (
        result["long_filled_quantity"] > 0.0
        and result["short_filled_quantity"] > 0.0
    )

    if args.apply:
        if result["already_backfilled"]:
            raise SystemExit(
                json.dumps(
                    {"apply_allowed": False, "reason": "position_already_backfilled", "result": result},
                    indent=2,
                )
            )
        if not result["complete"]:
            raise SystemExit(json.dumps({"apply_allowed": False, "result": result}, indent=2))
        _assert_apply_gates()
        correction_dir = ROOT / CORRECTION_DIR
        correction_dir.mkdir(parents=True, exist_ok=True)
        correction_event = _correction_event(args.position_id, result)
        exit_reconciled_payload = _corrected_exit_payload(
            args.position_id,
            result,
            snapshot,
        )
        append_events = [
            correction_event,
            {
                "ts_ms": int(exit_reconciled_payload["reconciled_at_ms"]),
                "kind": "exit.reconciled",
                "payload": exit_reconciled_payload,
            },
            *[
                {
                    "ts_ms": int(row.get("filled_at_ms") or time.time() * 1000),
                    "kind": "order.filled",
                    "payload": row,
                }
                for row in _order_filled_events_from_exit_payload(
                    exit_reconciled_payload
                )
            ],
        ]
        out_path = correction_dir / "{}.json".format(args.position_id)
        out_path.write_text(
            json.dumps(append_events, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n",
            encoding="utf-8",
        )
        journal_path = _append_runtime_events(runtime_dir, append_events)
        result["correction_event_path"] = str(out_path)
        result["correction_event_kind"] = correction_event["kind"]
        result["runtime_journal_path"] = str(journal_path)
        result["exit_reconciled"] = exit_reconciled_payload
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--position-id", required=True)
    parser.add_argument("--runtime-dir", default=str(DEFAULT_RUNTIME_DIR))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True)
    mode.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = asyncio.run(run(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
