"""Dedicated spread BBO process entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from lightfee.apps.sidecar import _install_shutdown_handlers
from lightfee.config.loader import load_config
from lightfee.sidecar.spread_bbo_service import SpreadBboProcessService


async def _run(service: SpreadBboProcessService) -> None:
    stop_event = asyncio.Event()
    cleanup = _install_shutdown_handlers(stop_event)
    try:
        await service.run(stop_event)
    finally:
        cleanup()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(description="LightFee V2 spread BBO data plane")
    parser.add_argument("--config", "-c", default="config/example.toml")
    args = parser.parse_args()
    asyncio.run(_run(SpreadBboProcessService(load_config(args.config))))


if __name__ == "__main__":
    main()
