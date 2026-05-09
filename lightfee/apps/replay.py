"""lightfee-replay: offline replay, backfill, walk-forward CLI."""

from __future__ import annotations

import argparse

from lightfee.config.loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-replay: Offline replay engine")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--from", dest="from_date", help="Start date")
    parser.add_argument("--to", dest="to_date", help="End date")
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"Replay loaded config with {len(config.symbols)} symbols")
