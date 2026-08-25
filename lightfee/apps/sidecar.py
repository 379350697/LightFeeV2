"""lightfee-sidecar: opportunity input data plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from lightfee.config.loader import load_config
from lightfee.config.schema import AppConfig
from lightfee.engine.bootstrap import prepare_runtime_symbols
from lightfee.sidecar.service import SidecarService

logger = logging.getLogger("lightfee.sidecar")


async def _run(config: AppConfig, *, once: bool) -> None:
    resolution = await prepare_runtime_symbols(config)
    if resolution is not None:
        logger.info(
            "sidecar symbol universe resolved enabled=%s global=%s resolved=%s fallback=%s",
            resolution["daily_universe_enabled"],
            resolution["global_symbol_count"],
            resolution["resolved_symbol_count"],
            resolution["used_fallback"],
        )

    service = SidecarService(config)
    try:
        if once:
            await service.refresh_once()
            return
        refresh_interval_s = config.runtime.sidecar_refresh_ms / 1000.0
        loop = asyncio.get_running_loop()
        next_refresh_at = loop.time()
        while True:
            delay_s = next_refresh_at - loop.time()
            if delay_s > 0:
                await asyncio.sleep(delay_s)
            next_refresh_at += refresh_interval_s
            await service.refresh_once()
    finally:
        await service.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    # Never let httpx/httpcore write full private request URLs (signed query
    # params) into journald at INFO.  See configure_http_client_logging.
    from lightfee.venues.transport import configure_http_client_logging
    configure_http_client_logging()

    parser = argparse.ArgumentParser(description="lightfee-sidecar: Opportunity input data plane")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    logger.info("loading config from %s", args.config)
    config = load_config(args.config)
    asyncio.run(_run(config, once=args.once))


if __name__ == "__main__":
    main()
