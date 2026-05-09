"""lightfee-live: live trading process entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import signal

from lightfee.config.loader import load_config
from lightfee.engine.runtime import LiveRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-live: Live trading process")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    args = parser.parse_args()

    config = load_config(args.config)
    runtime = LiveRuntime(config)

    loop = asyncio.new_event_loop()

    async def _run() -> None:
        await runtime.start()
        await runtime.run_loop()

    def _shutdown() -> None:
        loop.create_task(runtime.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(runtime.stop())
        loop.close()
