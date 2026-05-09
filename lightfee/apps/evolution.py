"""lightfee-evolution: proposal/review/outcome CLI."""

from __future__ import annotations

import argparse

from lightfee.config.loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-evolution: Parameter evolution")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--stage", choices=["propose", "review", "apply"], default="propose")
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"Evolution stage '{args.stage}' loaded config")
