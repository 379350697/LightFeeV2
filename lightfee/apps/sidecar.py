"""lightfee-sidecar: opportunity input data plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from lightfee.config.loader import load_config
from lightfee.sidecar.service import SidecarService

logger = logging.getLogger("lightfee.sidecar")


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
    service = SidecarService(config)

    async def _run() -> None:
        if args.once:
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

    asyncio.run(_run())


if __name__ == "__main__":
    main()
