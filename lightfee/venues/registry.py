"""Venue registry: maps venue name to adapter and capabilities."""

from __future__ import annotations

import os

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.venues.base import VenueCapabilities
from lightfee.venues.binance import BinanceAdapter
from lightfee.venues.okx import OkxAdapter
from lightfee.venues.bybit import BybitAdapter
from lightfee.venues.bitget import BitgetAdapter
from lightfee.venues.gate import GateAdapter
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.hyperliquid import HyperliquidAdapter
from lightfee.venues.transport import LiveCredential

_ADAPTER_CLASSES = {
    Venue.BINANCE: BinanceAdapter,
    Venue.OKX: OkxAdapter,
    Venue.BYBIT: BybitAdapter,
    Venue.BITGET: BitgetAdapter,
    Venue.GATE: GateAdapter,
    Venue.ASTER: AsterAdapter,
    Venue.HYPERLIQUID: HyperliquidAdapter,
}


def get_capabilities(venue: Venue) -> VenueCapabilities:
    return VenueCapabilities.for_venue(venue)


def all_venues() -> list[Venue]:
    return list(Venue)


def all_live_perp_venues() -> list[Venue]:
    return [
        Venue.BINANCE,
        Venue.OKX,
        Venue.BYBIT,
        Venue.BITGET,
        Venue.GATE,
        Venue.ASTER,
        Venue.HYPERLIQUID,
    ]


def _resolve_env(env_var: str) -> str:
    if not env_var:
        return ""
    return os.environ.get(env_var, "")


def build_adapter(venue: Venue, vc: VenueConfig, mode: str,
                  exchange_http_timeout_ms: int = 10000,
                  rate_limiter = None) -> VenueAdapter:
    cls = _ADAPTER_CLASSES[venue]
    if mode == "paper":
        return cls(mode="paper", exchange_http_timeout_ms=exchange_http_timeout_ms,
                   rate_limiter=rate_limiter)

    creds = vc.live.trade_credentials
    credential = LiveCredential(
        api_key=_resolve_env(creds.api_key_env or ""),
        api_secret=_resolve_env(creds.api_secret_env or ""),
        api_passphrase=_resolve_env(creds.api_passphrase_env or ""),
        wallet_private_key=_resolve_env(creds.wallet_private_key_env or ""),
        account_address=_resolve_env(creds.account_address_env or ""),
    )
    return cls(mode="live", credential=credential,
               exchange_http_timeout_ms=exchange_http_timeout_ms,
               rate_limiter=rate_limiter)


def build_adapter_map(config: AppConfig) -> dict[Venue, VenueAdapter]:
    mode = config.runtime.mode
    exchange_http_timeout_ms = config.runtime.exchange_http_timeout_ms
    # Create a shared rate limiter (V1: Arc<EndpointRateLimiter>)
    from lightfee.venues.transport import EndpointRateLimiter
    rate_limiter = EndpointRateLimiter(1000, 8000, 25)
    adapters: dict[Venue, VenueAdapter] = {}
    for vc in config.venues:
        try:
            venue = Venue.from_str(vc.venue)
        except ValueError:
            continue
        adapters[venue] = build_adapter(venue, vc, mode,
                                        exchange_http_timeout_ms=exchange_http_timeout_ms,
                                        rate_limiter=rate_limiter)
    return adapters
