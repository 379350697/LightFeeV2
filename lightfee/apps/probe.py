"""lightfee-probe: live smoke, L2 probe, venue capabilities CLI."""

from __future__ import annotations

import argparse

from lightfee.config.loader import load_config
from lightfee.venues.base import VenueCapabilities
from lightfee.venues.registry import all_live_perp_venues


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-probe: Venue and runtime probes")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--list-capabilities", action="store_true", help="List venue capabilities")
    parser.add_argument("--execute", action="store_true", help="Allow order placement (dry-run by default)")
    args = parser.parse_args()

    if args.list_capabilities:
        for venue in all_live_perp_venues():
            caps = VenueCapabilities.for_venue(venue)
            print(f"{venue.value}: l2={caps.execution_liquidity.value}, "
                  f"risk={caps.risk_health.value}, reconcile={caps.reconcile_quality.value}")
        return

    config = load_config(args.config)
    execute = args.execute
    print(f"Probe {'exec' if execute else 'dry-run'} mode with {len(config.venues)} venues")
