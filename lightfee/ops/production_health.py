from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


EXPECTED_VENUES = {"aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"}
KNOWN_GOOD_RESOLVERS = {"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4", "9.9.9.9"}
FIXTURE_MARKET_OBSERVED_AT_MS = 1710000075000


@dataclass(frozen=True)
class HealthReport:
    name: str
    ok: bool
    severity: str = "info"
    fingerprints: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthSummary:
    ok: bool
    critical_count: int
    warning_count: int
    reports: list[HealthReport]


def _execstart_lines(unit_text: str) -> list[str]:
    return [
        line.split("=", 1)[1].strip()
        for line in unit_text.splitlines()
        if line.strip().startswith("ExecStart=")
    ]


def _has_environment_file(unit_text: str) -> bool:
    return any(
        line.strip().startswith("EnvironmentFile=")
        for line in unit_text.splitlines()
    )


def analyze_systemd_unit(name: str, text: str) -> HealthReport:
    fingerprints: list[str] = []
    details: dict[str, Any] = {"service": name}
    execs = _execstart_lines(text)
    details["execstart"] = execs
    is_sidecar = "sidecar" in name
    is_live = name.endswith("lightfee-live.service") or "live" in name

    if not execs:
        fingerprints.append("missing_execstart")
    command = " ".join(execs)
    if (is_sidecar or is_live) and "--config" not in command:
        fingerprints.append("missing_explicit_config")
    if "config/example.toml" in command or command.endswith("example.toml"):
        fingerprints.append("example_config_in_production")
    if is_sidecar and not _has_environment_file(text):
        fingerprints.append("missing_environment_file")
    if is_sidecar and "opportunity_input_sidecar" in command and "live.auto.toml" not in command:
        fingerprints.append("rust_sidecar_without_live_auto_config")

    return HealthReport(
        name=f"systemd:{name}",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details=details,
    )


def _quote_venues(snapshot: dict[str, Any]) -> set[str]:
    venues: set[str] = set()
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for key, raw in quotes.items():
            if isinstance(raw, dict) and raw.get("venue"):
                venues.add(str(raw["venue"]).lower())
            elif isinstance(key, str) and ":" in key:
                venues.add(key.split(":", 1)[0].lower())
    return venues


def _fixture_quote_count(snapshot: dict[str, Any]) -> int:
    count = 0
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for raw in quotes.values():
            if not isinstance(raw, dict):
                continue
            try:
                if float(raw.get("bid", 0)) == 100.0 and float(raw.get("ask", 0)) == 100.0:
                    count += 1
            except (TypeError, ValueError):
                continue
    return count


def analyze_sidecar_snapshot(
    snapshot: dict[str, Any],
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    fingerprints: list[str] = []
    observed = int(snapshot.get("market_observed_at_ms") or snapshot.get("published_at_ms") or 0)
    age_ms = now_ms - observed if observed else None
    venues = _quote_venues(snapshot)
    missing = sorted(EXPECTED_VENUES - venues)
    fixture_quotes = _fixture_quote_count(snapshot)

    if observed == FIXTURE_MARKET_OBSERVED_AT_MS:
        fingerprints.append("fixture_timestamp")
    if age_ms is None or age_ms < 0 or age_ms > max_age_ms:
        fingerprints.append("snapshot_stale_or_missing_timestamp")
    if len(venues) < len(EXPECTED_VENUES):
        fingerprints.append("quote_venue_count_lt_7")
    if fixture_quotes >= 2:
        fingerprints.append("fixture_100_quotes")

    return HealthReport(
        name="sidecar_snapshot",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "observed_at_ms": observed,
            "age_ms": age_ms,
            "quote_venues": sorted(venues),
            "missing_venues": missing,
            "fixture_quote_count": fixture_quotes,
            "degraded_venues": list(snapshot.get("degraded_venues", [])),
        },
    )


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


def _local_expected_legs(state: dict[str, Any]) -> list[dict[str, Any]]:
    legs: list[dict[str, Any]] = []
    for pos in state.get("open_positions", []) or state.get("positions", []) or []:
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
    state: dict[str, Any], exchange_truth: dict[str, Any]
) -> list[dict[str, Any]]:
    live_positions = _live_position_details(exchange_truth)
    live_by_key = {
        (p["venue"], p["symbol"]): p
        for p in live_positions
    }
    expected_legs = _local_expected_legs(state)
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
                    **live,
                })

    return mismatches


def analyze_current_state(
    state: dict[str, Any],
    *,
    now_ms: int,
    max_tick_age_ms: int,
    require_exchange_truth: bool = False,
) -> HealthReport:
    fingerprints: list[str] = []
    last_tick_ms = int(state.get("last_tick_ms") or 0)
    tick_age_ms = now_ms - last_tick_ms if last_tick_ms else None
    open_count = int(state.get("open_position_count") or 0)
    pending_entries = int(state.get("pending_entry_count") or 0)
    pending_closes = int(state.get("pending_close_count") or 0)
    clean = open_count == 0 and pending_entries == 0 and pending_closes == 0

    if state.get("lifecycle") != "running":
        fingerprints.append("live_lifecycle_not_running")
    if tick_age_ms is None or tick_age_ms < 0 or tick_age_ms > max_tick_age_ms:
        fingerprints.append("live_tick_stale")
    if state.get("risk_mode") == "fail_closed" and clean and not state.get("recovery_blocked_reason"):
        fingerprints.append("stale_fail_closed_clean_state")
    if state.get("last_scan") is None:
        fingerprints.append("last_scan_missing")
    exchange_truth = state.get("exchange_truth")
    exchange_truth_mismatches: list[dict[str, Any]] = []
    exchange_truth_available = (
        isinstance(exchange_truth, dict)
        and bool(exchange_truth.get("available"))
    )
    exchange_truth_confidence = (
        str(exchange_truth.get("confidence", ""))
        if isinstance(exchange_truth, dict)
        else ""
    )
    if require_exchange_truth:
        if not isinstance(exchange_truth, dict):
            fingerprints.append("exchange_truth_missing")
        elif not exchange_truth_available:
            fingerprints.append("exchange_truth_unavailable")
        elif exchange_truth_confidence != "high":
            fingerprints.append("exchange_truth_confidence_not_high")
    if isinstance(exchange_truth, dict) and exchange_truth.get("available") and exchange_truth.get("confidence") == "high":
        leg_mismatches = _exchange_truth_position_mismatches(state, exchange_truth)
        if leg_mismatches:
            fingerprints.append("exchange_truth_mismatch")
            fingerprints.append("local_exchange_position_mismatch")
            if any(m.get("check") == "unexpected_live_position" for m in leg_mismatches):
                fingerprints.append("nonzero_live_position")
            exchange_truth_mismatches.extend(leg_mismatches)
        if clean and exchange_truth.get("has_nonzero_position"):
            if "exchange_truth_mismatch" not in fingerprints:
                fingerprints.append("exchange_truth_mismatch")
            if "nonzero_live_position" not in fingerprints:
                fingerprints.append("nonzero_live_position")
            for venue, positions in (exchange_truth.get("positions") or {}).items():
                if not isinstance(positions, dict):
                    continue
                for symbol, pos in positions.items():
                    if not isinstance(pos, dict):
                        continue
                    try:
                        qty = abs(float(pos.get("quantity") or 0.0))
                    except (TypeError, ValueError):
                        qty = 0.0
                    if qty > 1e-9:
                        exchange_truth_mismatches.append({
                            "check": "unexpected_live_position",
                            "venue": str(pos.get("venue") or venue),
                            "symbol": str(pos.get("symbol") or symbol),
                            "side": pos.get("side"),
                            "quantity": qty,
                            "entry_price": pos.get("entry_price"),
                        })

    severity = "critical" if any(fp != "last_scan_missing" for fp in fingerprints) else "warning"
    return HealthReport(
        name="current_state",
        ok=not fingerprints,
        severity=severity if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "tick_age_ms": tick_age_ms,
            "risk_mode": state.get("risk_mode"),
            "lifecycle": state.get("lifecycle"),
            "open_position_count": open_count,
            "pending_entry_count": pending_entries,
            "pending_close_count": pending_closes,
            "pending_residual_repair_count": int(state.get("pending_residual_repair_count") or 0),
            "exchange_truth_required": require_exchange_truth,
            "exchange_truth_available": exchange_truth_available,
            "exchange_truth_confidence": exchange_truth_confidence,
            "exchange_truth_mismatches": exchange_truth_mismatches,
        },
    )


def analyze_resolver_config(text: str) -> HealthReport:
    nameservers: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("nameserver ") and len(line.split()) >= 2:
            nameservers.append(line.split()[1])
        elif line.startswith("servers="):
            nameservers.extend(
                item.strip()
                for item in line.split("=", 1)[1].split(",")
                if item.strip()
            )
    fingerprints: list[str] = []
    if not nameservers:
        fingerprints.append("resolver_missing_nameserver")
    elif nameservers[0] not in KNOWN_GOOD_RESOLVERS:
        fingerprints.append("unverified_resolver_first")
    return HealthReport(
        name="resolver_config",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={"nameservers": nameservers},
    )


def summarize_reports(reports: Iterable[HealthReport]) -> HealthSummary:
    items = list(reports)
    critical = sum(1 for r in items if not r.ok and r.severity == "critical")
    warning = sum(1 for r in items if not r.ok and r.severity == "warning")
    return HealthSummary(
        ok=critical == 0 and warning == 0,
        critical_count=critical,
        warning_count=warning,
        reports=items,
    )
