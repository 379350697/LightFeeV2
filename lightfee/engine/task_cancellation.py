"""Shared bounded cancellation for latency-sensitive runtime tasks."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any


DEFAULT_TASK_CANCEL_DRAIN_S = 0.005


def consume_detached_task_result(
    task: asyncio.Task[Any],
    *,
    cleanup_tasks: set[asyncio.Task[Any]],
) -> None:
    """Release a detached task without mutating the request that spawned it."""

    cleanup_tasks.discard(task)
    with suppress(BaseException):
        task.exception()


async def cancel_task_with_bounded_drain(
    task: asyncio.Task[Any],
    *,
    cleanup_tasks: set[asyncio.Task[Any]],
    drain_s: float = DEFAULT_TASK_CANCEL_DRAIN_S,
) -> None:
    """Cancel ``task`` while bounding foreground latency.

    Slow cancellation cleanup remains strongly referenced until completion.
    Its callback only consumes the terminal result; it must not write state for
    the request generation that has already returned.
    """

    if not task.done():
        task.cancel()
    done, _pending = await asyncio.wait({task}, timeout=max(0.0, drain_s))
    if task in done:
        consume_detached_task_result(task, cleanup_tasks=cleanup_tasks)
        return

    cleanup_tasks.add(task)

    def _consume_result(completed: asyncio.Task[Any]) -> None:
        consume_detached_task_result(completed, cleanup_tasks=cleanup_tasks)

    task.add_done_callback(_consume_result)
