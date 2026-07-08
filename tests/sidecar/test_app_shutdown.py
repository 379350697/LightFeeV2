import asyncio
import signal

import pytest

from lightfee.apps import sidecar as sidecar_app


@pytest.mark.asyncio
async def test_install_shutdown_handlers_registers_sigterm_and_cleans_up():
    class FakeLoop:
        def __init__(self):
            self.handlers = {}
            self.removed = []

        def add_signal_handler(self, sig, callback):
            self.handlers[sig] = callback

        def remove_signal_handler(self, sig):
            self.removed.append(sig)
            return True

    stop_event = asyncio.Event()
    loop = FakeLoop()

    cleanup = sidecar_app._install_shutdown_handlers(stop_event, loop=loop)
    loop.handlers[signal.SIGTERM]()
    cleanup()

    assert signal.SIGTERM in loop.handlers
    assert signal.SIGINT in loop.handlers
    assert stop_event.is_set()
    assert signal.SIGTERM in loop.removed
    assert signal.SIGINT in loop.removed


@pytest.mark.asyncio
async def test_run_closes_service_after_shutdown_request():
    captured = {}

    class FakeService:
        def __init__(self):
            self.refresh_count = 0
            self.closed = False

        async def refresh_once(self):
            self.refresh_count += 1
            captured["stop_event"].set()

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        captured["stop_event"] = stop_event
        captured["cleaned"] = False

        def cleanup():
            captured["cleaned"] = True

        return cleanup

    service = FakeService()

    await sidecar_app._run(
        service,
        once=False,
        refresh_interval_s=60.0,
        install_shutdown_handlers=install_handlers,
    )

    assert service.refresh_count == 1
    assert service.closed is True
    assert captured["cleaned"] is True


@pytest.mark.asyncio
async def test_run_once_cancels_inflight_refresh_after_shutdown_request():
    captured = {}
    refresh_started = asyncio.Event()
    refresh_cancelled = asyncio.Event()

    class FakeService:
        def __init__(self):
            self.closed = False

        async def refresh_once(self):
            refresh_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                refresh_cancelled.set()
                raise

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        captured["stop_event"] = stop_event

        async def request_shutdown_after_refresh_starts():
            await refresh_started.wait()
            stop_event.set()

        captured["request_shutdown_task"] = asyncio.create_task(
            request_shutdown_after_refresh_starts()
        )

        def cleanup():
            captured["cleaned"] = True

        return cleanup

    service = FakeService()

    await asyncio.wait_for(
        sidecar_app._run(
            service,
            once=True,
            refresh_interval_s=60.0,
            install_shutdown_handlers=install_handlers,
        ),
        timeout=1.0,
    )

    assert refresh_cancelled.is_set()
    assert service.closed is True
    assert captured["cleaned"] is True


@pytest.mark.asyncio
async def test_run_cancels_inflight_refresh_after_shutdown_request():
    captured = {}
    refresh_started = asyncio.Event()
    refresh_cancelled = asyncio.Event()

    class FakeService:
        def __init__(self):
            self.closed = False

        async def refresh_once(self):
            refresh_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                refresh_cancelled.set()
                raise

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        captured["stop_event"] = stop_event

        async def request_shutdown_after_refresh_starts():
            await refresh_started.wait()
            stop_event.set()

        captured["request_shutdown_task"] = asyncio.create_task(
            request_shutdown_after_refresh_starts()
        )

        def cleanup():
            captured["cleaned"] = True

        return cleanup

    service = FakeService()

    await asyncio.wait_for(
        sidecar_app._run(
            service,
            once=False,
            refresh_interval_s=60.0,
            install_shutdown_handlers=install_handlers,
        ),
        timeout=1.0,
    )

    assert refresh_cancelled.is_set()
    assert service.closed is True
    assert captured["cleaned"] is True


@pytest.mark.asyncio
async def test_run_cancels_inflight_refresh_when_runner_is_cancelled():
    refresh_started = asyncio.Event()
    refresh_cancelled = asyncio.Event()

    class FakeService:
        def __init__(self):
            self.closed = False

        async def refresh_once(self):
            refresh_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                refresh_cancelled.set()
                raise

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        def cleanup():
            return None

        return cleanup

    service = FakeService()
    run_task = asyncio.create_task(
        sidecar_app._run(
            service,
            once=False,
            refresh_interval_s=60.0,
            install_shutdown_handlers=install_handlers,
        )
    )
    await refresh_started.wait()

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert refresh_cancelled.is_set()
    assert service.closed is True
