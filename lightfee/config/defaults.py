"""Default values for configuration sections."""

from __future__ import annotations

from lightfee.config.schema import (
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    TradeCredentials,
    VenueConfig,
    VenueLiveConfig,
    VenuePassiveMakerConfig,
)


def default_runtime() -> RuntimeConfig:
    return RuntimeConfig()


def default_strategy() -> StrategyConfig:
    return StrategyConfig()


def default_persistence() -> PersistenceConfig:
    return PersistenceConfig()


def default_venue(venue_name: str) -> VenueConfig:
    return VenueConfig(venue=venue_name)


def default_trade_credentials() -> TradeCredentials:
    return TradeCredentials()


def default_venue_live() -> VenueLiveConfig:
    return VenueLiveConfig()


def default_passive_maker() -> VenuePassiveMakerConfig:
    return VenuePassiveMakerConfig()
