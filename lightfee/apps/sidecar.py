"""lightfee-sidecar: opportunity input data plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import signal

from lightfee.config.loader import load_config
from lightfee.sidecar.service import SidecarService


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-sidecar: Opportunity input data plane")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    service = SidecarService(config)

    async def _run() -> None:
        if args.once:
            await service.refresh_once()
            return
        while True:
            await service.refresh_once()
            await asyncio.sleep(config.runtime.sidecar_refresh_ms / 1000.0)

    asyncio.run(_run())
