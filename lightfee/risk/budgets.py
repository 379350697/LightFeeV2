"""Risk budget gates for entry and exposure control."""

from __future__ import annotations

from dataclasses import dataclass, field

from lightfee.config.schema import StrategyConfig


@dataclass
class RiskBudgets:
    """Tracked risk budget state."""

    max_single_venue_exposure_quote: float = 200.0
    max_symbol_exposure_quote: float = 100.0
    max_global_net_exposure_quote: float = 0.0
    max_concurrent_positions: int = 2
    current_single_venue_exposures: dict[str, float] = field(default_factory=dict)
    current_symbol_exposures: dict[str, float] = field(default_factory=dict)
    current_position_count: int = 0

    @classmethod
    def from_config(cls, config: StrategyConfig) -> RiskBudgets:
        return cls(
            max_single_venue_exposure_quote=config.max_single_venue_exposure_quote,
            max_symbol_exposure_quote=config.max_symbol_exposure_quote,
            max_global_net_exposure_quote=config.max_global_net_exposure_quote,
            max_concurrent_positions=config.max_concurrent_positions,
        )

    def check_entry(
        self, venue: str, symbol: str, notional: float
    ) -> tuple[bool, str]:
        """Check if a new entry fits within risk budgets. Returns (allowed, reason)."""
        if self.max_concurrent_positions > 0 and self.current_position_count >= self.max_concurrent_positions:
            return False, f"max_concurrent_positions ({self.max_concurrent_positions}) reached"

        venue_exp = self.current_single_venue_exposures.get(venue, 0.0)
        if self.max_single_venue_exposure_quote > 0 and venue_exp + notional > self.max_single_venue_exposure_quote:
            return False, f"venue exposure limit for {venue}"

        symbol_exp = self.current_symbol_exposures.get(symbol, 0.0)
        if self.max_symbol_exposure_quote > 0 and symbol_exp + notional > self.max_symbol_exposure_quote:
            return False, f"symbol exposure limit for {symbol}"

        return True, ""

    def record_entry(self, venue: str, symbol: str, notional: float) -> None:
        self.current_position_count += 1
        self.current_single_venue_exposures[venue] = (
            self.current_single_venue_exposures.get(venue, 0.0) + notional
        )
        self.current_symbol_exposures[symbol] = (
            self.current_symbol_exposures.get(symbol, 0.0) + notional
        )

    def record_exit(self, venue: str, symbol: str, notional: float) -> None:
        self.current_position_count = max(0, self.current_position_count - 1)
        self.current_single_venue_exposures[venue] = max(
            0, self.current_single_venue_exposures.get(venue, 0.0) - notional
        )
        self.current_symbol_exposures[symbol] = max(
            0, self.current_symbol_exposures.get(symbol, 0.0) - notional
        )
