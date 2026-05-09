"""Market view: computes cross-bps and reference prices from snapshot data."""

from __future__ import annotations

from lightfee.sidecar.snapshot import QuoteSnapshot


def compute_reference_mid(long_quote: QuoteSnapshot, short_quote: QuoteSnapshot) -> float:
    """reference_mid = (long_ask + short_bid) / 2"""
    return (long_quote.ask + short_quote.bid) / 2.0


def compute_raw_cross_bps(long_quote: QuoteSnapshot, short_quote: QuoteSnapshot) -> float:
    """raw_cross_bps = (short_bid - long_ask) / reference_mid * 10000"""
    mid = compute_reference_mid(long_quote, short_quote)
    if mid <= 0:
        return 0.0
    return ((short_quote.bid - long_quote.ask) / mid) * 10000.0


def select_maker_leg(long_quote: QuoteSnapshot, short_quote: QuoteSnapshot) -> str:
    """Select which leg should be the maker (higher estimated slippage → maker)."""
    long_spread = long_quote.ask - long_quote.bid
    short_spread = short_quote.ask - short_quote.bid
    return "long" if long_spread >= short_spread else "short"
