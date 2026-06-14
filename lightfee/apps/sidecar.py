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


async def _install_sidecar_rate_limit_runtime(config_path: str) -> object:
    from lightfee.engine.bootstrap import rate_limit_config_path
    from lightfee.rate_limit.config import RateLimitConfigManager
    from lightfee.rate_limit.engine import (
        RateLimitRuntime,
        install_global_rate_limit_runtime,
    )

    rate_limit_config_mgr = RateLimitConfigManager(
        config_path=rate_limit_config_path(config_path)
    )
    rate_limit_rt = RateLimitRuntime(config_manager=rate_limit_config_mgr)
    await rate_limit_rt.refresh()
    install_global_rate_limit_runtime(rate_limit_rt)
    return rate_limit_rt


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="lightfee-sidecar: Opportunity input data plane")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    logger.info("loading config from %s", args.config)
    config = load_config(args.config)
    service = SidecarService(config)

    async def _run() -> None:
        await _install_sidecar_rate_limit_runtime(args.config)
        if args.once:
            await service.refresh_once()
            return
        while True:
            await service.refresh_once()
            await asyncio.sleep(config.runtime.sidecar_refresh_ms / 1000.0)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
