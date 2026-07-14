"""Execution liquidity: multi-level VWAP, slippage, capacity estimation.

Rust V1 parity: converts LocalL2Book → ExecutionLiquiditySnapshot for entry/delever/close.

V1 enforces that in parity mode, execution liquidity MUST come from local-L2
when local-L2 is enabled and the book is ready.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite

from lightfee.marketdata.l2 import (
    ExecutionLiquiditySource,
    L2BookStatus,
    LocalL2Book,
    _book_structure_fault,
)


@dataclass
class LiquidityLevel:
    """One price level in an order book."""

    price: float
    size: float  # base asset quantity


@dataclass
class ExecutionLiquiditySnapshot:
    """Multi-level order book with VWAP and slippage estimates."""

    symbol: str
    venue: str
    bids: list[LiquidityLevel] = field(default_factory=list)
    asks: list[LiquidityLevel] = field(default_factory=list)
    observed_at_ms: int = 0
    source: str = "top_book"  # "true_l2", "top_book", "cached", "none"
    fallback_reason: str = ""
    book_ready: bool = True

    def estimate_vwap_buy(self, target_quote: float) -> tuple[float, float]:
        """Estimate VWAP and avg price for buying target_quote notional."""
        return self._walk_levels(self.asks, target_quote)

    def estimate_vwap_sell(self, target_quote: float) -> tuple[float, float]:
        """Estimate VWAP and avg price for selling target_quote notional."""
        return self._walk_levels(self.bids, target_quote)

    def _walk_levels(
        self, levels: list[LiquidityLevel], target_quote: float
    ) -> tuple[float, float]:
        if not levels or not isfinite(target_quote) or target_quote <= 0:
            return (0.0, 0.0)
        filled_quote = 0.0
        filled_base = 0.0
        for lvl in levels:
            if (
                not isfinite(lvl.price)
                or not isfinite(lvl.size)
                or lvl.price <= 0.0
                or lvl.size <= 0.0
            ):
                return (0.0, 0.0)
            level_quote = lvl.price * lvl.size
            if not isfinite(level_quote) or level_quote <= 0.0:
                return (0.0, 0.0)
            take = min(level_quote, target_quote - filled_quote)
            filled_quote += take
            # ``take`` is quote notional.  Reconstruct VWAP as quote/base;
            # quote-weighting the price again would scale it by price and
            # grossly overstate slippage on multi-level fills.
            filled_base += take / lvl.price
            if filled_quote >= target_quote:
                break
        if (
            not isfinite(filled_quote)
            or not isfinite(filled_base)
            or filled_quote <= 0
            or filled_base <= 0
        ):
            return (0.0, 0.0)
        vwap = filled_quote / filled_base
        return (filled_quote, vwap) if isfinite(vwap) and vwap > 0.0 else (0.0, 0.0)

    def buy_slippage_bps(self, target_quote: float, reference_price: float) -> float:
        if not isfinite(reference_price) or reference_price <= 0:
            return 0.0
        filled, avg_price = self.estimate_vwap_buy(target_quote)
        if filled <= 0 or avg_price <= 0:
            return 0.0
        return (avg_price / reference_price - 1.0) * 10000.0

    def sell_slippage_bps(self, target_quote: float, reference_price: float) -> float:
        if not isfinite(reference_price) or reference_price <= 0:
            return 0.0
        filled, avg_price = self.estimate_vwap_sell(target_quote)
        if filled <= 0 or avg_price <= 0:
            return 0.0
        return (1.0 - avg_price / reference_price) * 10000.0

    def max_fillable_buy(self, slippage_limit_bps: float) -> float:
        return self._max_fillable(self.asks, slippage_limit_bps)

    def max_fillable_sell(self, slippage_limit_bps: float) -> float:
        return self._max_fillable(self.bids, slippage_limit_bps)

    def _max_fillable(
        self, levels: list[LiquidityLevel], slippage_limit_bps: float
    ) -> float:
        if not levels or not isfinite(slippage_limit_bps) or slippage_limit_bps < 0.0:
            return 0.0
        ref = levels[0].price
        if not isfinite(ref) or ref <= 0:
            return 0.0
        filled = 0.0
        for lvl in levels:
            if (
                not isfinite(lvl.price)
                or not isfinite(lvl.size)
                or lvl.price <= 0.0
                or lvl.size <= 0.0
            ):
                return 0.0
            slippage = abs(lvl.price / ref - 1.0) * 10000.0
            if not isfinite(slippage) or slippage > slippage_limit_bps:
                break
            filled += lvl.price * lvl.size
        return filled if isfinite(filled) and filled > 0.0 else 0.0


def chunked_l2_close_capacity(
    snapshot: ExecutionLiquiditySnapshot,
    target_quantity: float,
    max_slippage_bps: float,
    side: str,
) -> float:
    """How much of target_quantity can be filled within slippage budget."""
    if (
        not isfinite(target_quantity)
        or not isfinite(max_slippage_bps)
        or target_quantity <= 0.0
        or max_slippage_bps < 0.0
    ):
        return 0.0
    if side == "buy":
        return min(target_quantity, snapshot.max_fillable_buy(max_slippage_bps))
    else:
        return min(target_quantity, snapshot.max_fillable_sell(max_slippage_bps))


# ---------------------------------------------------------------------------
# LocalL2Book → ExecutionLiquiditySnapshot conversion (Rust V1 parity)
# ---------------------------------------------------------------------------


def execution_liquidity_from_local_l2(
    book: LocalL2Book,
    max_depth: int = 0,
    max_age_ms: int = 5000,
    now_ms: int = 0,
    require_ready: bool = True,
) -> ExecutionLiquiditySnapshot:
    """Convert a LocalL2Book to an ExecutionLiquiditySnapshot.

    In parity mode, requires the book to be HOT and fresh.
    Returns NONE source if the book is not ready.
    """
    depth = max_depth or book.max_depth or 50

    # Check lifecycle/freshness separately from book structure.  ``is_ready``
    # also validates the stored levels, but preserving the invalid-book reason
    # below keeps a corrupt HOT book distinguishable from an ordinary stale or
    # not-yet-HOT one in entry gates and incident diagnostics.
    if require_ready:
        if (
            book.status != L2BookStatus.HOT
            or book.observed_at_ms > now_ms
            or book.is_stale(max_age_ms, now_ms)
        ):
            return ExecutionLiquiditySnapshot(
                symbol=book.symbol,
                venue=book.venue,
                observed_at_ms=book.observed_at_ms,
                source=ExecutionLiquiditySource.NONE.value,
                fallback_reason=f"book_not_ready status={book.status.value} age={book.age_ms(now_ms)}ms",
                book_ready=False,
            )

    # Always use the canonical whole-book structure gate before slicing to the
    # requested depth.  ``require_ready=False`` relaxes only lifecycle and
    # freshness for diagnostic callers; it must never relax execution-data
    # integrity.  Validating the whole stored book also prevents duplicate or
    # non-monotonic levels beyond ``max_depth`` from being hidden by slicing.
    if _book_structure_fault(book.bids, book.asks):
        return ExecutionLiquiditySnapshot(
            symbol=book.symbol,
            venue=book.venue,
            observed_at_ms=book.observed_at_ms,
            source=ExecutionLiquiditySource.NONE.value,
            fallback_reason="book_invalid_for_execution_liquidity",
            book_ready=False,
        )

    selected_bids = book.bids[:depth]
    selected_asks = book.asks[:depth]
    if (
        not selected_bids
        or not selected_asks
        or any(
            not isfinite(level.price)
            or not isfinite(level.quantity)
            or level.price <= 0.0
            or level.quantity <= 0.0
            for level in [*selected_bids, *selected_asks]
        )
        or selected_bids[0].price >= selected_asks[0].price
    ):
        return ExecutionLiquiditySnapshot(
            symbol=book.symbol,
            venue=book.venue,
            observed_at_ms=book.observed_at_ms,
            source=ExecutionLiquiditySource.NONE.value,
            fallback_reason="book_invalid_for_execution_liquidity",
            book_ready=False,
        )

    bids = [LiquidityLevel(price=lvl.price, size=lvl.quantity) for lvl in selected_bids]
    asks = [LiquidityLevel(price=lvl.price, size=lvl.quantity) for lvl in selected_asks]

    return ExecutionLiquiditySnapshot(
        symbol=book.symbol,
        venue=book.venue,
        bids=bids,
        asks=asks,
        observed_at_ms=book.observed_at_ms,
        source=ExecutionLiquiditySource.TRUE_L2.value,
        book_ready=True,
    )


def execution_liquidity_fallback(
    symbol: str,
    venue: str,
    reason: str,
    source: ExecutionLiquiditySource = ExecutionLiquiditySource.TOP_BOOK,
) -> ExecutionLiquiditySnapshot:
    """Create a fallback liquidity snapshot with explicit reason."""
    return ExecutionLiquiditySnapshot(
        symbol=symbol,
        venue=venue,
        source=source.value,
        fallback_reason=reason,
        book_ready=False,
    )
