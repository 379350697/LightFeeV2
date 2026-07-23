"""lightfee-sidecar: opportunity input data plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Callable

from lightfee.config.loader import load_config
from lightfee.sidecar.service import SidecarService

logger = logging.getLogger("lightfee.sidecar")


def _next_full_refresh_deadline_s(
    *,
    refresh_started_at_s: float,
    refresh_interval_s: float,
) -> float:
    """Schedule from refresh start, so work time does not consume freshness."""
    return refresh_started_at_s + max(float(refresh_interval_s), 0.0)


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
    bbo_task: asyncio.Task[None] | None = None
    loop = asyncio.get_running_loop()
    refresh_interval_s = max(float(refresh_interval_s), 0.0)

    async def refresh_once_until_stop(*, cache_only: bool = False) -> bool:
        refresh_call = (
            getattr(service, "refresh_entry_from_latest_cache")
            if cache_only
            else service.refresh_once
        )
        refresh_task = asyncio.create_task(refresh_call())
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            waiters: set[asyncio.Task] = {refresh_task, stop_task}
            if bbo_task is not None:
                waiters.add(bbo_task)
            done, _pending = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_event.is_set():
                if not refresh_task.done():
                    refresh_task.cancel()
                    try:
                        await refresh_task
                    except asyncio.CancelledError:
                        pass
                return False
            if bbo_task is not None and bbo_task in done:
                if not refresh_task.done():
                    refresh_task.cancel()
                    try:
                        await refresh_task
                    except asyncio.CancelledError:
                        pass
                await bbo_task
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
        bbo_runner = getattr(service, "run_spread_bbo_data_plane", None)
        embedded_bbo_enabled = bool(getattr(service, "embedded_spread_bbo_enabled", True))
        if embedded_bbo_enabled and callable(bbo_runner):
            bbo_task = asyncio.create_task(bbo_runner(stop_event))
            # Establish compact-snapshot ownership before the slower metadata
            # loop can publish its one-shot compatibility view.
            await asyncio.sleep(0)
        # A full refresh's deadline is measured from its start.  The old loop
        # slept for a full interval after refresh work completed, so a 2s
        # refresh plus a 3s interval published at ~5s and regularly exceeded
        # the 4s live-market freshness budget. Cache-only republish work must
        # never postpone this periodic full-refresh deadline.
        next_full_refresh_deadline_s = loop.time()
        cache_only_next = False
        while not stop_event.is_set():
            cache_only_refresh = cache_only_next
            refresh_started_at_s = loop.time()
            completed_refresh = await refresh_once_until_stop(
                cache_only=cache_only_refresh
            )
            cache_only_next = False
            if stop_event.is_set():
                break
            if not completed_refresh:
                break
            if not cache_only_refresh:
                next_full_refresh_deadline_s = _next_full_refresh_deadline_s(
                    refresh_started_at_s=refresh_started_at_s,
                    refresh_interval_s=refresh_interval_s,
                )
            wait_timeout_s = max(
                next_full_refresh_deadline_s - loop.time(),
                0.0,
            )
            republish_event = getattr(
                service,
                "entry_venue_republish_event",
                None,
            )
            republish_task = (
                asyncio.create_task(republish_event.wait())
                if isinstance(republish_event, asyncio.Event)
                else None
            )
            if bbo_task is None:
                try:
                    stop_task = asyncio.create_task(stop_event.wait())
                    waiters = {stop_task}
                    if republish_task is not None:
                        waiters.add(republish_task)
                    done, _pending = await asyncio.wait(
                        waiters,
                        timeout=wait_timeout_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if republish_task is not None and republish_task in done:
                        republish_event.clear()
                        cache_only_next = True
                        continue
                    if stop_task not in done:
                        continue
                    break
                finally:
                    for task in (stop_task, republish_task):
                        if task is not None and not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
            try:
                stop_task = asyncio.create_task(stop_event.wait())
                waiters = {stop_task, bbo_task}
                if republish_task is not None:
                    waiters.add(republish_task)
                done, _pending = await asyncio.wait(
                    waiters,
                    timeout=wait_timeout_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if republish_task is not None and republish_task in done:
                    republish_event.clear()
                    cache_only_next = True
                    continue
                if bbo_task in done:
                    await bbo_task
                if stop_task in done:
                    break
            finally:
                for task in (stop_task, republish_task):
                    if task is not None and not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
    finally:
        stop_event.set()
        if bbo_task is not None:
            if not bbo_task.done():
                await bbo_task
            else:
                bbo_task.exception()
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
    external_spread_bbo = os.environ.get("LIGHTFEE_EXTERNAL_SPREAD_BBO", "") == "1"
    service = (
        SidecarService(config, enable_spread_bbo=False)
        if external_spread_bbo
        else SidecarService(config)
    )

    asyncio.run(
        _run(
            service,
            once=args.once,
            refresh_interval_s=config.runtime.sidecar_refresh_ms / 1000.0,
        )
    )


if __name__ == "__main__":
    main()
