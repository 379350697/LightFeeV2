"""Execution liquidity helpers."""

from __future__ import annotations

from lightfee.core.domain import ExecutionLiquiditySnapshot


def chunked_l2_close_capacity(
    snapshot: ExecutionLiquiditySnapshot,
    target_quantity: float,
    max_slippage_bps: float,
    side: str,
) -> float:
    """Compute how much quantity can be closed within slippage budget."""
    if target_quantity <= 0 or snapshot.best_bid <= 0 or snapshot.best_ask <= 0:
        return 0.0
    ref_price = snapshot.best_bid if side == "sell" else snapshot.best_ask
    if ref_price <= 0:
        return 0.0
    max_slippage_price = ref_price * (1.0 - max_slippage_bps / 10000.0) if side == "sell" else ref_price * (1.0 + max_slippage_bps / 10000.0)
    book_qty = snapshot.bid_size if side == "sell" else snapshot.ask_size
    return min(target_quantity, book_qty)
