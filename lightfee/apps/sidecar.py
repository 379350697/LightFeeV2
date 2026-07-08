"""lightfee-sidecar: opportunity input data plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Callable

from lightfee.config.loader import load_config
from lightfee.sidecar.service import SidecarService

logger = logging.getLogger("lightfee.sidecar")


def _install_shutdown_handlers(
    stop_event: asyncio.Event,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> Callable[[], None]:
    loop = loop or asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request_shutdown() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        installed.append(sig)

    def cleanup() -> None:
        for sig in installed:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                continue

    return cleanup


async def _run(
    service: SidecarService,
    *,
    once: bool,
    refresh_interval_s: float,
    install_shutdown_handlers: Callable[
        [asyncio.Event], Callable[[], None]
    ] = _install_shutdown_handlers,
) -> None:
    stop_event = asyncio.Event()
    cleanup_shutdown_handlers = install_shutdown_handlers(stop_event)

    async def refresh_once_until_stop() -> bool:
        refresh_task = asyncio.create_task(service.refresh_once())
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            await asyncio.wait(
                {refresh_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task.done() and not refresh_task.done():
                refresh_task.cancel()
                try:
                    await refresh_task
                except asyncio.CancelledError:
                    pass
                return False
            await refresh_task
            return True
        except asyncio.CancelledError:
            if not refresh_task.done():
                refresh_task.cancel()
                try:
                    await refresh_task
                except asyncio.CancelledError:
                    pass
            raise
        finally:
            if not stop_task.done():
                stop_task.cancel()
                try:
                    await stop_task
                except asyncio.CancelledError:
                    pass

    try:
        if once:
            await refresh_once_until_stop()
            return
        while not stop_event.is_set():
            completed_refresh = await refresh_once_until_stop()
            if stop_event.is_set():
                break
            if not completed_refresh:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=refresh_interval_s)
            except asyncio.TimeoutError:
                continue
    finally:
        cleanup_shutdown_handlers()
        await service.close()


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

    asyncio.run(_run(
        service,
        once=args.once,
        refresh_interval_s=config.runtime.sidecar_refresh_ms / 1000.0,
    ))


if __name__ == "__main__":
    main()
