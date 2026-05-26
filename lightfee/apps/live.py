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


def _shutdown_timeout_s(config) -> float:
    timeout_ms = int(getattr(config.runtime, "shutdown_grace_period_ms", 3000) or 3000)
    return max(timeout_ms, 1) / 1000.0


def _task_label(task: asyncio.Task) -> str:
    try:
        name = task.get_name()
    except AttributeError:
        name = ""
    if name and not name.startswith("Task-"):
        return name
    coro = task.get_coro()
    qualname = getattr(coro, "__qualname__", None)
    if qualname:
        return qualname
    return repr(task)


async def _cancel_tasks_with_timeout(
    tasks: list[asyncio.Task],
    *,
    timeout_s: float,
    stage: str,
) -> list[asyncio.Task]:
    pending = [task for task in tasks if not task.done()]
    logger.info("shutdown stage=%s task_count=%d", stage, len(pending))
    if not pending:
        return []

    for task in pending:
        logger.info("shutdown stage=%s task=%s action=cancel", stage, _task_label(task))
        task.cancel()

    done, still_pending = await asyncio.wait(pending, timeout=timeout_s)
    for task in done:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(
                "shutdown stage=%s task=%s status=error error=%s",
                stage,
                _task_label(task),
                exc,
            )

    for task in still_pending:
        logger.error(
            "shutdown stage=%s task=%s status=timeout timeout_s=%.3f",
            stage,
            _task_label(task),
            timeout_s,
        )
    return list(still_pending)


def _cancel_tasks_without_wait(tasks: list[asyncio.Task], *, stage: str) -> None:
    pending = [task for task in tasks if not task.done()]
    if not pending:
        return
    logger.warning(
        "shutdown stage=%s task_count=%d action=cancel_without_wait",
        stage,
        len(pending),
    )
    for task in pending:
        logger.warning(
            "shutdown stage=%s task=%s action=cancel_without_wait",
            stage,
            _task_label(task),
        )
        task.cancel()

        def _consume_result(done_task: asyncio.Task, *, label: str = _task_label(task)) -> None:
            try:
                done_task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning(
                    "shutdown stage=%s task=%s status=late_error error=%s",
                    stage,
                    label,
                    exc,
                )

        task.add_done_callback(_consume_result)


def _wire_production_executors(runtime, venue_adapters) -> bool:
    """Wire live-only executor dependencies when runtime has the production contract."""

    journal = getattr(runtime, "journal", None)
    supervisor = getattr(runtime, "supervisor", None)
    missing = [
        name
        for name, value in (
            ("journal", journal),
            ("supervisor", supervisor),
        )
        if value is None
    ]
    if missing:
        logger.debug(
            "production executor wiring skipped runtime_type=%s missing=%s",
            type(runtime).__name__,
            ",".join(missing),
        )
        return False

    from lightfee.engine.close_executor import CloseExecutor
    from lightfee.engine.entry_sync import EntrySyncExecutor
    from lightfee.engine.reconciliation import OrderReconciler

    runtime.entry_executor = EntrySyncExecutor(
        adapters=venue_adapters, journal=journal,
    )
    runtime.close_executor = CloseExecutor(
        adapters=venue_adapters, journal=journal,
    )
    supervisor.close_executor = runtime.close_executor
    runtime.reconciler = OrderReconciler(adapters=venue_adapters)
    return True


async def async_main(
    config_path: str = "config/example.toml",
    *,
    shutdown_event: asyncio.Event | None = None,
    shutdown_signal_name=None,
) -> None:
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

    # Wire production executors (V1 live closure - Fix 2)
    _wire_production_executors(runtime, venue_adapters)

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
    shutdown_timeout_s = _shutdown_timeout_s(config)
    baseline_tasks = set(asyncio.all_tasks())
    run_loop_task: asyncio.Task | None = None
    shutdown_wait_task: asyncio.Task | None = None
    signal_logged = False

    def _signal_name() -> str:
        if callable(shutdown_signal_name):
            return str(shutdown_signal_name() or "unknown")
        if shutdown_signal_name:
            return str(shutdown_signal_name)
        return "unknown"

    def _log_signal_received(signal_name: str | None = None) -> None:
        nonlocal signal_logged
        if signal_logged:
            return
        signal_logged = True
        logger.info(
            "shutdown stage=signal_received signal=%s",
            signal_name or _signal_name(),
        )

    async def _graceful_shutdown() -> None:
        nonlocal _stopped
        if _stopped:
            return
        _stopped = True
        await runtime.stop()

    async def _cancel_runtime_tasks() -> None:
        current = asyncio.current_task()
        candidates = [
            task
            for task in asyncio.all_tasks()
            if task is not current
            and task not in baseline_tasks
            and task is not shutdown_wait_task
        ]
        await _cancel_tasks_with_timeout(
            candidates,
            timeout_s=shutdown_timeout_s,
            stage="cancel_tasks",
        )

    try:
        if shutdown_event is None:
            await runtime.start()
            await runtime.run_loop()
        else:
            shutdown_wait_task = asyncio.create_task(
                shutdown_event.wait(),
                name="lightfee-live:shutdown_signal_wait",
            )
            start_task = asyncio.create_task(
                runtime.start(),
                name="lightfee-live:start",
            )
            done, _pending = await asyncio.wait(
                {start_task, shutdown_wait_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            start_completed = start_task in done
            if shutdown_wait_task in done and shutdown_event.is_set():
                _log_signal_received()
                if not start_completed:
                    await _cancel_tasks_with_timeout(
                        [start_task],
                        timeout_s=shutdown_timeout_s,
                        stage="cancel_tasks",
                    )
            if start_completed:
                await start_task
            elif not shutdown_event.is_set():
                await start_task

            if shutdown_event.is_set() and not start_completed:
                return

            run_loop_task = asyncio.create_task(
                runtime.run_loop(),
                name="lightfee-live:run_loop",
            )
            done, _pending = await asyncio.wait(
                {run_loop_task, shutdown_wait_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_wait_task in done and shutdown_event.is_set():
                _log_signal_received()
            if run_loop_task in done:
                await run_loop_task
    except KeyboardInterrupt:
        _log_signal_received("KeyboardInterrupt")
        pass
    except asyncio.CancelledError:
        _log_signal_received("task_cancelled")
        raise
    finally:
        if shutdown_wait_task is not None and not shutdown_wait_task.done():
            shutdown_wait_task.cancel()
            try:
                await shutdown_wait_task
            except asyncio.CancelledError:
                pass
        if run_loop_task is not None and not run_loop_task.done():
            runtime._running = False
        await _cancel_runtime_tasks()
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
    shutdown_event: asyncio.Event | None = None
    shutdown_signal_name = "unknown"

    def _request_shutdown(sig: signal.Signals) -> None:
        nonlocal shutdown_signal_name
        shutdown_signal_name = sig.name
        if shutdown_event is not None:
            shutdown_event.set()

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
            loop.add_signal_handler(sig, _request_shutdown, sig)
        except NotImplementedError:
            pass

    # SIGHUP — Unix only
    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, _on_sighup)
        except NotImplementedError:
            pass

    try:
        shutdown_event = asyncio.Event()
        main_task = loop.create_task(
            async_main(
                args.config,
                shutdown_event=shutdown_event,
                shutdown_signal_name=lambda: shutdown_signal_name,
            )
        )
        loop.run_until_complete(main_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("shutdown signal received — exiting")
    finally:
        pending = [
            task for task in asyncio.all_tasks(loop)
            if task is not main_task and not task.done()
        ]
        if pending:
            _cancel_tasks_without_wait(list(pending), stage="cancel_tasks")
            loop.run_until_complete(asyncio.sleep(0))
            abandoned_pending = {id(task) for task in pending}
            previous_exception_handler = loop.get_exception_handler()

            def _shutdown_exception_handler(
                event_loop: asyncio.AbstractEventLoop,
                context: dict,
            ) -> None:
                task = context.get("task") or context.get("future")
                if (
                    context.get("message") == "Task was destroyed but it is pending!"
                    and task is not None
                    and id(task) in abandoned_pending
                ):
                    logger.debug(
                        "shutdown stage=cancel_tasks task=%s status=abandoned_after_timeout",
                        _task_label(task),
                    )
                    return
                if previous_exception_handler is not None:
                    previous_exception_handler(event_loop, context)
                    return
                event_loop.default_exception_handler(context)

            loop.set_exception_handler(_shutdown_exception_handler)
        loop.close()


if __name__ == "__main__":
    main()
