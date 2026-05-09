"""lightfee-scheduler: offline job runner entrypoint."""

from __future__ import annotations

import argparse

from lightfee.config.loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-scheduler: Offline job runner")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--job", choices=["daily", "analysis", "replay", "evolution"], help="Run a specific job")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"Scheduler loaded config with {len(config.symbols)} symbols, {len(config.venues)} venues")
    # Offline jobs are production stubs for the scheduler boundary
