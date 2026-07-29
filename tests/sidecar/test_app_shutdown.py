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
async def test_run_schedules_full_refresh_from_its_start_time():
    captured = {}

    class FakeService:
        def __init__(self):
            self.full_refresh_started_at_s = []
            self.closed = False

        async def refresh_once(self):
            self.full_refresh_started_at_s.append(asyncio.get_running_loop().time())
            await asyncio.sleep(0.08)
            if len(self.full_refresh_started_at_s) == 2:
                captured["stop_event"].set()

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        captured["stop_event"] = stop_event
        return lambda: None

    service = FakeService()
    await asyncio.wait_for(
        sidecar_app._run(
            service,
            once=False,
            refresh_interval_s=0.20,
            install_shutdown_handlers=install_handlers,
        ),
        timeout=1.0,
    )

    cadence_s = (
        service.full_refresh_started_at_s[1]
        - service.full_refresh_started_at_s[0]
    )
    # The former completion-plus-interval loop took about 0.28s here. The
    # periodic full refresh now starts on its 0.20s deadline.
    assert cadence_s < 0.25
    assert service.closed is True


@pytest.mark.asyncio
async def test_cache_only_republish_does_not_postpone_full_refresh_deadline():
    captured = {}

    class FakeService:
        def __init__(self):
            self.entry_venue_republish_event = asyncio.Event()
            self.full_refresh_started_at_s = []
            self.cache_refresh_started_at_s = []
            self.closed = False

        async def refresh_once(self):
            self.full_refresh_started_at_s.append(asyncio.get_running_loop().time())
            await asyncio.sleep(0.04)
            if len(self.full_refresh_started_at_s) == 2:
                captured["stop_event"].set()

        async def refresh_entry_from_latest_cache(self):
            self.cache_refresh_started_at_s.append(asyncio.get_running_loop().time())
            await asyncio.sleep(0.04)

        async def close(self):
            self.closed = True

    def install_handlers(stop_event):
        captured["stop_event"] = stop_event

        async def request_republish_after_first_refresh():
            await asyncio.sleep(0.07)
            service.entry_venue_republish_event.set()

        captured["republish_task"] = asyncio.create_task(
            request_republish_after_first_refresh()
        )
        return lambda: None

    service = FakeService()
    await asyncio.wait_for(
        sidecar_app._run(
            service,
            once=False,
            refresh_interval_s=0.20,
            install_shutdown_handlers=install_handlers,
        ),
        timeout=1.0,
    )

    assert len(service.cache_refresh_started_at_s) == 1
    cadence_s = (
        service.full_refresh_started_at_s[1]
        - service.full_refresh_started_at_s[0]
    )
    # A cache-only publish completed before the full deadline. It must not
    # reset that deadline and extend the market-data cadence to ~0.31s.
    assert cadence_s < 0.25
    assert service.closed is True


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
