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


def analyze_current_state(
    state: dict[str, Any],
    *,
    now_ms: int,
    max_tick_age_ms: int,
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
