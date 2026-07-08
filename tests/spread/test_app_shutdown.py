import asyncio

import pytest

from lightfee.apps import spread_sidecar as spread_sidecar_app


@pytest.mark.asyncio
async def test_spread_run_closes_service_after_shutdown_request():
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

    await spread_sidecar_app._run(
        service,
        once=False,
        refresh_interval_s=60.0,
        install_shutdown_handlers=install_handlers,
    )

    assert service.refresh_count == 1
    assert service.closed is True
    assert captured["cleaned"] is True


@pytest.mark.asyncio
async def test_spread_run_once_cancels_inflight_refresh_after_shutdown_request():
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
        spread_sidecar_app._run(
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
async def test_spread_run_cancels_inflight_refresh_after_shutdown_request():
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
        spread_sidecar_app._run(
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
