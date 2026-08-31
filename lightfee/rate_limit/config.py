"""Rate-limit config: V1-parity dataclasses, built-in defaults, and TOML hot-reload."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum


class RefreshOutcome(Enum):
    RELOADED = "reloaded"
    UNCHANGED = "unchanged"
    FAILED_BUT_KEPT_OLD = "failed_but_kept_old"


@dataclass
class RateLimitGlobalConfig:
    default_margin: float = 0.95
    refresh_interval_secs: int = 30


@dataclass
class RateLimitHostConfig:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None


@dataclass
class VenueDocsFallback:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None


@dataclass
class RateLimitVenueConfig:
    budget_per_minute: int | None = None
    min_interval_ms: int | None = None
    ws_budget_per_minute: int | None = None
    endpoint_weights: dict[str, int] = field(default_factory=dict)
    group_weights: dict[str, int] = field(default_factory=dict)
    endpoint_min_interval_ms: dict[str, int] = field(default_factory=dict)
    group_min_interval_ms: dict[str, int] = field(default_factory=dict)
    scopes: dict[str, str] = field(default_factory=dict)
    docs_fallback: VenueDocsFallback = field(default_factory=VenueDocsFallback)


@dataclass
class RateLimitConfig:
    global_config: RateLimitGlobalConfig = field(default_factory=RateLimitGlobalConfig)
    hosts: dict[str, RateLimitHostConfig] = field(default_factory=dict)
    venues: dict[str, RateLimitVenueConfig] = field(default_factory=dict)

    @property
    def default_margin(self) -> float:
        return self.global_config.default_margin

    @default_margin.setter
    def default_margin(self, value: float) -> None:
        self.global_config.default_margin = value

    @property
    def refresh_interval_secs(self) -> int:
        return self.global_config.refresh_interval_secs

    @refresh_interval_secs.setter
    def refresh_interval_secs(self, value: int) -> None:
        self.global_config.refresh_interval_secs = value


# ---------------------------------------------------------------------------
# V1 built-in defaults (exact copy from Rust rate_limit/config.rs)
# ---------------------------------------------------------------------------

_V1_COMMON_GROUP_WEIGHTS: dict[str, int] = {
    "depth": 5,
    "market": 1,
    "order": 1,
    "account": 1,
    "ws_public": 1,
    "ws_private": 1,
}


def _common_group_min_intervals(default_ms: int) -> dict[str, int]:
    return {k: default_ms for k in _V1_COMMON_GROUP_WEIGHTS}


def built_in_defaults() -> RateLimitConfig:
    hosts = {
        "fapi.binance.com": RateLimitHostConfig(budget_per_minute=2400, min_interval_ms=25),
        "fapi.asterdex.com": RateLimitHostConfig(budget_per_minute=1200, min_interval_ms=50),
        "api.bybit.com": RateLimitHostConfig(budget_per_minute=600, min_interval_ms=75),
        "api.bitget.com": RateLimitHostConfig(budget_per_minute=600, min_interval_ms=100),
        "www.okx.com": RateLimitHostConfig(budget_per_minute=600, min_interval_ms=100),
        "api.gateio.ws": RateLimitHostConfig(budget_per_minute=900, min_interval_ms=75),
        "api.hyperliquid.xyz": RateLimitHostConfig(budget_per_minute=1200, min_interval_ms=50),
    }

    venues = {
        "binance": _venue_binance_defaults(),
        "aster": _venue_aster_defaults(),
        "bybit": _venue_bybit_defaults(),
        "bitget": _venue_bitget_defaults(),
        "okx": _venue_okx_defaults(),
        "gate": _venue_gate_defaults(),
        "hyperliquid": _venue_hyperliquid_defaults(),
    }

    return RateLimitConfig(
        global_config=RateLimitGlobalConfig(default_margin=0.95, refresh_interval_secs=30),
        hosts=hosts,
        venues=venues,
    )


# ---------------------------------------------------------------------------
# Per-venue builders (exact V1 values)
# ---------------------------------------------------------------------------


def _venue_binance_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=2400,
        min_interval_ms=25,
        ws_budget_per_minute=600,
        endpoint_weights={
            "GET /fapi/v1/depth": 5,
            "GET /fapi/v1/exchangeInfo": 10,
            "GET /fapi/v1/ticker/bookTicker": 2,
            "GET /fapi/v1/premiumIndex": 1,
            "GET /fapi/v1/ticker/24hr": 2,
            "GET /fapi/v1/openInterest": 1,
            "POST /fapi/v1/order": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={
            "GET /fapi/v1/depth": 25,
            "GET /fapi/v1/ticker/24hr": 25,
            "GET /fapi/v1/openInterest": 25,
        },
        group_min_interval_ms=_common_group_min_intervals(25),
        scopes={
            "GET /fapi/v1/depth": "depth",
            "GET /fapi/v1/exchangeInfo": "market",
            "GET /fapi/v1/ticker/bookTicker": "market",
            "GET /fapi/v1/premiumIndex": "market",
            "GET /fapi/v1/ticker/24hr": "market",
            "GET /fapi/v1/openInterest": "market",
            "POST /fapi/v1/order": "order",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=1200, min_interval_ms=50),
    )


def _venue_aster_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=1200,
        min_interval_ms=50,
        ws_budget_per_minute=600,
        endpoint_weights={
            "GET /fapi/v1/depth": 5,
            "GET /fapi/v1/exchangeInfo": 10,
            "GET /fapi/v1/ticker/bookTicker": 2,
            "GET /fapi/v1/premiumIndex": 1,
            "GET /fapi/v1/ticker/24hr": 2,
            "GET /fapi/v1/openInterest": 1,
            "POST /fapi/v1/order": 1,
            "GET /fapi/v3/order": 1,
            "POST /fapi/v3/order": 1,
            "DELETE /fapi/v3/order": 1,
            "GET /fapi/v3/openOrders": 1,
            "GET /fapi/v3/positionRisk": 5,
            "GET /fapi/v3/positionSide/dual": 30,
            "GET /fapi/v3/accountWithJoinMargin": 5,
            "POST /fapi/v3/leverage": 1,
            "GET /fapi/v3/leverageBracket": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={
            "GET /fapi/v1/depth": 50,
            "GET /fapi/v1/ticker/24hr": 50,
            "GET /fapi/v1/openInterest": 50,
            "GET /fapi/v3/order": 50,
            "POST /fapi/v3/order": 50,
            "DELETE /fapi/v3/order": 50,
            "GET /fapi/v3/openOrders": 50,
            "GET /fapi/v3/positionRisk": 50,
            "GET /fapi/v3/positionSide/dual": 50,
            "GET /fapi/v3/accountWithJoinMargin": 50,
            "POST /fapi/v3/leverage": 50,
            "GET /fapi/v3/leverageBracket": 50,
        },
        group_min_interval_ms=_common_group_min_intervals(50),
        scopes={
            "GET /fapi/v1/depth": "depth",
            "GET /fapi/v1/exchangeInfo": "market",
            "GET /fapi/v1/ticker/bookTicker": "market",
            "GET /fapi/v1/premiumIndex": "market",
            "GET /fapi/v1/ticker/24hr": "market",
            "GET /fapi/v1/openInterest": "market",
            "POST /fapi/v1/order": "order",
            "GET /fapi/v3/order": "account",
            "POST /fapi/v3/order": "order",
            "DELETE /fapi/v3/order": "order",
            "GET /fapi/v3/openOrders": "account",
            "GET /fapi/v3/positionRisk": "account",
            "GET /fapi/v3/positionSide/dual": "account",
            "GET /fapi/v3/accountWithJoinMargin": "account",
            "POST /fapi/v3/leverage": "order",
            "GET /fapi/v3/leverageBracket": "account",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=1200, min_interval_ms=50),
    )


def _venue_bybit_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=600,
        min_interval_ms=75,
        ws_budget_per_minute=300,
        endpoint_weights={
            "GET /v5/market/orderbook": 5,
            "GET /v5/market/tickers": 1,
            "GET /v5/market/instruments-info": 2,
            "POST /v5/order/create": 1,
            "GET /v5/account/fee-rate": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={
            "GET /v5/market/orderbook": 75,
            "GET /v5/market/tickers": 75,
        },
        group_min_interval_ms=_common_group_min_intervals(75),
        scopes={
            "GET /v5/market/orderbook": "depth",
            "GET /v5/market/tickers": "market",
            "GET /v5/market/instruments-info": "market",
            "POST /v5/order/create": "order",
            "GET /v5/account/fee-rate": "account",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=600, min_interval_ms=100),
    )


def _venue_bitget_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=600,
        min_interval_ms=100,
        ws_budget_per_minute=300,
        endpoint_weights={
            "GET /api/v3/market/orderbook": 1,
            "GET /api/v2/mix/market/merge-depth": 5,
            "GET /api/v2/mix/market/ticker": 1,
            "GET /api/v2/mix/market/tickers": 1,
            "GET /api/v2/mix/market/current-fund-rate": 1,
            "GET /api/v2/mix/market/contracts": 2,
            "POST /api/v2/mix/order/place-order": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={
            "GET /api/v3/market/orderbook": 100,
            "GET /api/v2/mix/market/merge-depth": 100,
            "GET /api/v2/mix/market/tickers": 100,
        },
        group_min_interval_ms=_common_group_min_intervals(100),
        scopes={
            "GET /api/v3/market/orderbook": "depth",
            "GET /api/v2/mix/market/merge-depth": "depth",
            "GET /api/v2/mix/market/ticker": "market",
            "GET /api/v2/mix/market/tickers": "market",
            "GET /api/v2/mix/market/current-fund-rate": "market",
            "GET /api/v2/mix/market/contracts": "market",
            "POST /api/v2/mix/order/place-order": "order",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=600, min_interval_ms=100),
    )


def _venue_okx_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=600,
        min_interval_ms=100,
        ws_budget_per_minute=300,
        endpoint_weights={
            "GET /api/v5/market/books": 5,
            "GET /api/v5/public/funding-rate": 1,
            "GET /api/v5/market/tickers": 1,
            "GET /api/v5/public/open-interest": 1,
            "POST /api/v5/trade/order": 1,
            "GET /api/v5/account/config": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={
            "GET /api/v5/market/books": 100,
            "GET /api/v5/market/tickers": 100,
            "GET /api/v5/public/open-interest": 100,
        },
        group_min_interval_ms=_common_group_min_intervals(100),
        scopes={
            "GET /api/v5/market/books": "depth",
            "GET /api/v5/public/funding-rate": "market",
            "GET /api/v5/market/tickers": "market",
            "GET /api/v5/public/open-interest": "market",
            "POST /api/v5/trade/order": "order",
            "GET /api/v5/account/config": "account",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=600, min_interval_ms=100),
    )


def _venue_gate_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=900,
        min_interval_ms=75,
        ws_budget_per_minute=300,
        endpoint_weights={
            "GET /api/v4/futures/usdt/order_book": 5,
            "GET /api/v4/futures/usdt/tickers": 1,
            "GET /api/v4/futures/usdt/contracts": 2,
            "POST /api/v4/futures/usdt/orders": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={"GET /api/v4/futures/usdt/order_book": 75},
        group_min_interval_ms=_common_group_min_intervals(75),
        scopes={
            "GET /api/v4/futures/usdt/order_book": "depth",
            "GET /api/v4/futures/usdt/tickers": "market",
            "GET /api/v4/futures/usdt/contracts": "market",
            "POST /api/v4/futures/usdt/orders": "order",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=900, min_interval_ms=75),
    )


def _venue_hyperliquid_defaults() -> RateLimitVenueConfig:
    return RateLimitVenueConfig(
        budget_per_minute=1200,
        min_interval_ms=50,
        ws_budget_per_minute=300,
        endpoint_weights={
            "POST /info": 2,
            "POST /exchange": 1,
        },
        group_weights=dict(_V1_COMMON_GROUP_WEIGHTS),
        endpoint_min_interval_ms={"POST /info": 50},
        group_min_interval_ms=_common_group_min_intervals(50),
        scopes={
            "POST /info": "market",
            "POST /exchange": "order",
        },
        docs_fallback=VenueDocsFallback(budget_per_minute=1200, min_interval_ms=50),
    )


# ---------------------------------------------------------------------------
# Config manager with disk-backed hot-reload
# ---------------------------------------------------------------------------


class RateLimitConfigManager:
    """Manages rate-limit config with disk-backed hot-reload.

    On refresh, reads rate_limits.toml, diffs against the last raw content,
    and rebuilds the config only when changed.
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._path = config_path
        self._last_raw: str = ""
        self._last_hash: str = ""
        self.config: RateLimitConfig = built_in_defaults()

    def refresh(self, now_ms: int | None = None) -> str:
        """Read the file; return RELOADED, UNCHANGED, or FAILED_BUT_KEPT_OLD."""
        if self._path is None:
            return RefreshOutcome.UNCHANGED.value

        try:
            raw = _read_file(self._path)
        except OSError:
            return RefreshOutcome.FAILED_BUT_KEPT_OLD.value

        h = hashlib.sha256(raw.encode()).hexdigest()
        if h == self._last_hash:
            return RefreshOutcome.UNCHANGED.value

        try:
            self.config = _parse_toml_config(raw)
        except Exception:
            return RefreshOutcome.FAILED_BUT_KEPT_OLD.value

        self._last_raw = raw
        self._last_hash = h
        return RefreshOutcome.RELOADED.value

    @property
    def path(self) -> str | None:
        return self._path


def _read_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def _parse_toml_config(raw: str) -> RateLimitConfig:
    """Parse a rate_limits.toml string into RateLimitConfig.

    Supports V1 table format:

    [global]
    default_margin = 0.95
    refresh_interval_secs = 30

    [host."fapi.binance.com"]
    budget_per_minute = 2400
    min_interval_ms = 25

    [venue.binance]
    budget_per_minute = 2400
    min_interval_ms = 25
    ws_budget_per_minute = 600
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    data = tomllib.loads(raw)
    baseline = built_in_defaults()

    # [global]
    global_raw = data.get("global", {})
    if isinstance(global_raw, dict):
        margin = global_raw.get("default_margin")
        if isinstance(margin, (int, float)):
            baseline.global_config.default_margin = float(margin)
        refresh = global_raw.get("refresh_interval_secs")
        if isinstance(refresh, int):
            baseline.global_config.refresh_interval_secs = refresh

    # [host."hostname"]
    host_raw = data.get("host", {})
    if isinstance(host_raw, dict):
        for host_id, hdata in host_raw.items():
            if isinstance(hdata, dict):
                cfg = RateLimitHostConfig()
                if "budget_per_minute" in hdata:
                    cfg.budget_per_minute = int(hdata["budget_per_minute"])
                if "min_interval_ms" in hdata:
                    cfg.min_interval_ms = int(hdata["min_interval_ms"])
                baseline.hosts[host_id] = cfg

    # [venue.name]
    venue_raw = data.get("venue", {})
    if isinstance(venue_raw, dict):
        for venue_id, vdata in venue_raw.items():
            if not isinstance(vdata, dict):
                continue
            entry = baseline.venues.setdefault(venue_id, RateLimitVenueConfig())
            if "budget_per_minute" in vdata:
                entry.budget_per_minute = int(vdata["budget_per_minute"])
            if "min_interval_ms" in vdata:
                entry.min_interval_ms = int(vdata["min_interval_ms"])
            if "ws_budget_per_minute" in vdata:
                entry.ws_budget_per_minute = int(vdata["ws_budget_per_minute"])

            def _update_dict(target: dict, source: dict) -> None:
                for k, v in source.items():
                    target[k] = int(v)

            endpoints_raw = vdata.get("endpoint_weights", {})
            if isinstance(endpoints_raw, dict):
                _update_dict(entry.endpoint_weights, endpoints_raw)
            groups_raw = vdata.get("group_weights", {})
            if isinstance(groups_raw, dict):
                _update_dict(entry.group_weights, groups_raw)
            endpoint_min_raw = vdata.get("endpoint_min_interval_ms", {})
            if isinstance(endpoint_min_raw, dict):
                _update_dict(entry.endpoint_min_interval_ms, endpoint_min_raw)
            group_min_raw = vdata.get("group_min_interval_ms", {})
            if isinstance(group_min_raw, dict):
                _update_dict(entry.group_min_interval_ms, group_min_raw)
            scopes_raw = vdata.get("scopes", {})
            if isinstance(scopes_raw, dict):
                for k, v in scopes_raw.items():
                    entry.scopes[k] = str(v)
            docs_raw = vdata.get("docs_fallback", {})
            if isinstance(docs_raw, dict):
                if "budget_per_minute" in docs_raw:
                    entry.docs_fallback.budget_per_minute = int(docs_raw["budget_per_minute"])
                if "min_interval_ms" in docs_raw:
                    entry.docs_fallback.min_interval_ms = int(docs_raw["min_interval_ms"])

    return baseline
