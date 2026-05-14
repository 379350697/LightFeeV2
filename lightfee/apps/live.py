"""lightfee-live: live trading process entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from lightfee.config.loader import load_config
from lightfee.engine.runtime import LiveRuntime
from lightfee.venues.registry import build_adapter_map

logger = logging.getLogger("lightfee.live")


async def async_main(config_path: str = "config/example.toml") -> None:
    """Async entry point for live trading (testable without event-loop side effects).

    V1 parity: always calls runtime.start() before the loop, and runtime.stop()
    on every exit path (normal return, KeyboardInterrupt, or unexpected error).
    """
    config = load_config(config_path)
    venue_adapters = build_adapter_map(config)
    logger.info(
        "built %d venue adapters: %s",
        len(venue_adapters),
        ",".join(v.value for v in venue_adapters),
    )
    runtime = LiveRuntime(config, venue_adapters=venue_adapters)

    # Wire production executors (V1 live closure — Fix 2)
    from lightfee.engine.close_executor import CloseExecutor
    from lightfee.engine.entry_sync import EntrySyncExecutor
    from lightfee.engine.reconciliation import OrderReconciler

    runtime.entry_executor = EntrySyncExecutor(
        adapters=venue_adapters, journal=runtime.journal,
    )
    runtime.close_executor = CloseExecutor(
        adapters=venue_adapters, journal=runtime.journal,
    )
    runtime.supervisor.close_executor = runtime.close_executor
    runtime.reconciler = OrderReconciler(adapters=venue_adapters)

    # Initialize global rate-limit runtime for SIGHUP and periodic reload
    from lightfee.rate_limit.engine import (
        RateLimitRuntime,
        install_global_rate_limit_runtime,
    )
    from lightfee.rate_limit.config import RateLimitConfigManager
    from lightfee.engine.bootstrap import rate_limit_config_path

    rate_limit_config_mgr = RateLimitConfigManager(
        config_path=rate_limit_config_path(config_path)
    )
    # V1: RateLimitRuntime::new() immediately calls build_engine_from_config
    # with the current (built-in) config. Then refresh() tries disk config.
    rate_limit_rt = RateLimitRuntime(config_manager=rate_limit_config_mgr)
    # Force initial refresh from disk (V1: first refresh after construction)
    await rate_limit_rt.refresh()
    install_global_rate_limit_runtime(rate_limit_rt)
    # Wire rate-limit runtime to LiveRuntime for periodic reload
    runtime._rate_limit_runtime = rate_limit_rt

    _stopped = False

    async def _graceful_shutdown() -> None:
        nonlocal _stopped
        if _stopped:
            return
        _stopped = True
        logger.info("graceful shutdown initiated")
        await runtime.stop()

    try:
        await runtime.start()
        await runtime.run_loop()
    except KeyboardInterrupt:
        pass
    finally:
        await _graceful_shutdown()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="lightfee-live: Live trading process")
    parser.add_argument(
        "--config", "-c", default="config/example.toml", help="Path to config TOML"
    )
    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    main_task: asyncio.Task | None = None

    def _request_shutdown() -> None:
        if main_task is not None and not main_task.done():
            main_task.cancel()

    def _on_sighup() -> None:
        """V1: reload rate-limit config on SIGHUP."""
        from lightfee.rate_limit.engine import global_rate_limit_runtime

        logger.info("SIGHUP received — reloading rate-limit config")
        try:
            rt = global_rate_limit_runtime()
            asyncio.ensure_future(rt.refresh(), loop=loop)
        except Exception as e:
            logger.error("SIGHUP rate-limit reload failed: %s", e)

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:
            pass

    # SIGHUP — Unix only
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, _on_sighup)
        except NotImplementedError:
            pass

    try:
        main_task = loop.create_task(async_main(args.config))
        loop.run_until_complete(main_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("shutdown signal received — exiting")
    finally:
        # Flush any remaining pending callbacks
        pending = asyncio.all_tasks(loop)
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


if __name__ == "__main__":
    main()
