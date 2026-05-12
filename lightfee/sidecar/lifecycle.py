"""Sidecar domain lifecycle tracking: funding, market, transfer, hint, perp liquidity."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class DomainStatus(Enum):
    FRESH = "fresh"
    STALE = "stale"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class DomainLifecycle:
    """Track one data domain's freshness across venues."""

    domain: str  # "funding", "market", "transfer", "hint", "perp_liquidity"
    observed_at_ms: int = 0
    venue_count: int = 0
    status: DomainStatus = DomainStatus.UNKNOWN

    def evaluate(self, now_ms: int, max_age_ms: int) -> DomainStatus:
        if self.observed_at_ms <= 0 or self.venue_count == 0:
            self.status = DomainStatus.UNKNOWN
        elif (now_ms - self.observed_at_ms) <= max_age_ms:
            self.status = DomainStatus.FRESH
        else:
            self.status = DomainStatus.STALE
        return self.status


@dataclass
class SidecarLifecycleState:
    """Aggregated lifecycle across all domains."""

    domains: dict[str, DomainLifecycle] = field(default_factory=dict)
    degraded_venues: list[str] = field(default_factory=list)

    def all_fresh(self, now_ms: int, max_age_ms: int) -> bool:
        for d in self.domains.values():
            d.evaluate(now_ms, max_age_ms)
            if d.status != DomainStatus.FRESH:
                return False
        return True

    def any_degraded(self) -> bool:
        return len(self.degraded_venues) > 0

    def fresh_domains(self, now_ms: int, max_age_ms: int) -> list[str]:
        return [
            name
            for name, d in self.domains.items()
            if d.evaluate(now_ms, max_age_ms) == DomainStatus.FRESH
        ]

    def stale_domains(self, now_ms: int, max_age_ms: int) -> list[str]:
        return [
            name
            for name, d in self.domains.items()
            if d.evaluate(now_ms, max_age_ms) == DomainStatus.STALE
        ]


    def to_dict(self) -> dict:
        """Serialize lifecycle state for current-state export and diagnostics (OPP-002)."""
        return {
            "domains": {
                name: {
                    "domain": d.domain,
                    "observed_at_ms": d.observed_at_ms,
                    "venue_count": d.venue_count,
                    "status": d.status.value if isinstance(d.status, DomainStatus) else str(d.status),
                }
                for name, d in self.domains.items()
            },
            "degraded_venues": list(self.degraded_venues),
            "all_fresh": self.all_fresh(0, 0),  # caller should re-evaluate with proper timestamps
        }


def create_domain_lifecycle(domain: str) -> DomainLifecycle:
    return DomainLifecycle(domain=domain)
