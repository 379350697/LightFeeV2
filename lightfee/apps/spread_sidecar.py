"""lightfee-spread-sidecar: spread-reversion signal process."""

from __future__ import annotations

import argparse
import asyncio
import logging

from lightfee.apps.logging_config import configure_app_logging
from lightfee.apps.sidecar import _install_shutdown_handlers
from lightfee.apps.sidecar import _run as _run_refresh_loop
from lightfee.config.loader import load_config
from lightfee.spread.service import SpreadSidecarService

logger = logging.getLogger("lightfee.spread_sidecar")


async def _run(
    service: SpreadSidecarService,
    *,
    once: bool,
    refresh_interval_s: float,
    install_shutdown_handlers=_install_shutdown_handlers,
) -> None:
    await _run_refresh_loop(
        service,
        once=once,
        refresh_interval_s=refresh_interval_s,
        install_shutdown_handlers=install_shutdown_handlers,
    )


def main() -> None:
    configure_app_logging(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="lightfee-spread-sidecar: spread-reversion signal process"
    )
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    service = SpreadSidecarService(config)

    asyncio.run(
        _run(
            service,
            once=bool(args.once),
            refresh_interval_s=config.runtime.spread_sidecar_refresh_ms / 1000.0,
        )
    )


if __name__ == "__main__":
    main()
