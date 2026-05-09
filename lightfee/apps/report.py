"""lightfee-report: journal/incident/runtime posture report CLI."""

from __future__ import annotations

import argparse

from lightfee.config.loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-report: Analysis and reporting")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--format", choices=["json", "text"], default="text", help="Output format")
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"Report loaded config: {config.runtime.mode} mode, {len(config.symbols)} symbols")
