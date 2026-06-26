"""lightfee-spread-sidecar: spread-reversion signal process."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from lightfee.config.loader import load_config
from lightfee.spread.service import SpreadSidecarService

logger = logging.getLogger("lightfee.spread_sidecar")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(
        description="lightfee-spread-sidecar: spread-reversion signal process"
    )
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    service = SpreadSidecarService(config)

    async def _run() -> None:
        try:
            if args.once:
                await service.refresh_once()
                return
            while True:
                await service.refresh_once()
                await asyncio.sleep(config.runtime.spread_sidecar_refresh_ms / 1000.0)
        finally:
            await service.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
