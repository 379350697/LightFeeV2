from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from lightfee.core.domain import PositionSnapshot, Venue
from lightfee.venues.specs import okx_spec
from lightfee.venues.transport import (
    LiveCredential,
    OKX_ACCOUNT_INSTRUMENTS_PATH,
    OKX_PUBLIC_INSTRUMENTS_PATH,
    VenueTransport,
)


pytestmark = pytest.mark.live_probe


def _okx_credential_or_skip() -> LiveCredential:
    api_key = os.environ.get("LIGHTFEE_OKX_API_KEY", "")
    api_secret = os.environ.get("LIGHTFEE_OKX_API_SECRET", "")
    api_passphrase = os.environ.get("LIGHTFEE_OKX_API_PASSPHRASE", "")
    if not (api_key and api_secret and api_passphrase):
        pytest.skip("LIGHTFEE_OKX_API_KEY/SECRET/PASSPHRASE required")
    return LiveCredential(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )


def _install_readonly_transport_guard(
    transport: VenueTransport,
) -> list[tuple[str, str, bool]]:
    calls: list[tuple[str, str, bool]] = []
    original_request = transport._request
    original_public_get = transport._public_get
    allowed_private_get_paths = {
        OKX_ACCOUNT_INSTRUMENTS_PATH,
        okx_spec().position_path,
    }

    async def guarded_request(
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        private: bool = False,
        **kwargs: Any,
    ):
        calls.append((method.upper(), path, bool(private)))
        assert method.upper() == "GET"
        assert path in allowed_private_get_paths
        assert body is None
        return await original_request(
            method,
            path,
            body=body,
            params=params,
            private=private,
            **kwargs,
        )

    async def guarded_public_get(
        path: str,
        params: dict[str, Any] | None = None,
    ):
        calls.append(("GET", path, False))
        assert path == OKX_PUBLIC_INSTRUMENTS_PATH
        return await original_public_get(path, params)

    transport._request = guarded_request
    transport._public_get = guarded_public_get
    return calls


def test_position_probe_file_stays_read_only_by_construction():
    source = Path(__file__).read_text()
    mutating_methods = (
        "place_order",
        "submit_passive_order",
        "cancel_order",
        "amend_order",
    )
    forbidden = tuple(f".{name}(" for name in mutating_methods)
    forbidden += ("change_" + "leverage", "set_" + "margin")

    assert not any(token in source for token in forbidden)


@pytest.mark.asyncio
async def test_readonly_okx_position_probe_payload_shape():
    credential = _okx_credential_or_skip()
    transport = VenueTransport(
        spec=okx_spec(),
        mode="live",
        credential=credential,
    )
    calls = _install_readonly_transport_guard(transport)

    try:
        await transport._ensure_okx_swap_instrument_metadata_loaded()
        positions = await transport.fetch_all_positions()
    finally:
        await transport.close()

    assert calls
    assert all(method == "GET" for method, _path, _private in calls)
    assert any(path == OKX_PUBLIC_INSTRUMENTS_PATH for _method, path, _private in calls)
    assert any(path == okx_spec().position_path for _method, path, private in calls if private)
    assert any(
        isinstance(metadata, dict)
        and metadata.get("ctVal")
        and metadata.get("ctType")
        for metadata in transport._symbol_metadata.values()
    )
    assert isinstance(positions, list)
    for position in positions:
        assert isinstance(position, PositionSnapshot)
        assert position.venue == Venue.OKX
        assert position.symbol
        assert position.observed_at_ms > 0
        assert isinstance(position.quantity, float)
