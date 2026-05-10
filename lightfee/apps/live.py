"""lightfee-live: live trading process entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from lightfee.config.loader import load_config
from lightfee.engine.runtime import LiveRuntime
from lightfee.venues.registry import build_adapter_map

logger = logging.getLogger("lightfee.live")


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-live: Live trading process")
    parser.add_argument(
        "--config", "-c", default="config/example.toml", help="Path to config TOML"
    )
    args = parser.parse_args()

    config = load_config(args.config)
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

    loop = asyncio.new_event_loop()

    shutdown_requested = False

    async def _run() -> None:
        nonlocal shutdown_requested
        await runtime.start()
        await runtime.run_loop()
        if not shutdown_requested:
            await _graceful_shutdown()

    async def _graceful_shutdown() -> None:
        nonlocal shutdown_requested
        if shutdown_requested:
            return
        shutdown_requested = True
        logger.info("graceful shutdown initiated")
        await runtime.stop()

    def _on_sigint() -> None:
        asyncio.ensure_future(_graceful_shutdown(), loop=loop)

    def _on_sighup() -> None:
        """Reload rate-limit config (placeholder for Task 22)."""
        logger.info("SIGHUP received — rate-limit reload placeholder")

    # Register signal handlers
    for sig, handler in (
        (signal.SIGINT, _on_sigint),
        (signal.SIGTERM, _on_sigint),
    ):
        try:
            loop.add_signal_handler(sig, handler)
        except NotImplementedError:
            pass

    # SIGHUP — Unix only
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, _on_sighup)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        loop.run_until_complete(_graceful_shutdown())
    finally:
        loop.run_until_complete(runtime.stop())
        loop.close()
