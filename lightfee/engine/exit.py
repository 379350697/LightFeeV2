"""Exit execution state machine matching Rust exit flow."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
import math
import os
from enum import Enum
from typing import Mapping
from lightfee.core.domain import OrderFill, OrderRequest, Side
from lightfee.engine.state import OpenPosition


EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION = 3
# A pre-trade L2 observation is evidence only while it remains contemporaneous
# with the order it benchmarks.  This is deliberately an acceptance boundary:
# a delayed receipt never prevents V1 close/recovery, but cannot be promoted as
# an execution-quality observation.
EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS = 1_000
TRUSTED_EXECUTION_BENCHMARK_HMAC_ENV = "LIGHTFEE_EXECUTION_BENCHMARK_HMAC_KEY"
TRUSTED_EXECUTION_BENCHMARK_KEY_ID = "lightfee-execution-benchmark-v1"


def execution_benchmark_receipt_digest(receipt: dict[str, object]) -> str:
    """Checksum the unsigned receipt for accidental-corruption diagnostics.

    The digest is intentionally not the authenticity control.  Promotion
    requires the HMAC generated and checked below.
    """
    canonical = {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_digest", "integrity"}
    }
    try:
        encoded = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return ""
    return hashlib.sha256(encoded).hexdigest()


def seal_execution_benchmark_receipt(
    receipt: dict[str, object],
    *,
    secret: str | None = None,
) -> dict[str, object] | None:
    """Attach the trusted HMAC required for execution-quality promotion.

    A missing trusted environment key leaves the receipt unsealed and the
    close continues normally, but it cannot become acceptance evidence.
    """
    signing_secret = secret or os.environ.get(TRUSTED_EXECUTION_BENCHMARK_HMAC_ENV)
    if not signing_secret:
        return None
    sealed = {
        key: value
        for key, value in receipt.items()
        if key not in {"receipt_digest", "integrity"}
    }
    sealed["schema_version"] = EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION
    digest = execution_benchmark_receipt_digest(sealed)
    if not digest:
        return None
    sealed["receipt_digest"] = digest
    integrity = {
        "algorithm": "hmac-sha256",
        "key_id": TRUSTED_EXECUTION_BENCHMARK_KEY_ID,
    }
    unsigned = {**sealed, "integrity": integrity}
    try:
        encoded = json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    sealed["integrity"] = {
        **integrity,
        "signature": hmac.new(
            signing_secret.encode("utf-8"), encoded, hashlib.sha256
        ).hexdigest(),
    }
    return sealed


def execution_benchmark_receipt_integrity_verified(receipt: object) -> bool:
    """Verify the fixed-key HMAC at the persistence/recovery boundary."""
    if not isinstance(receipt, dict):
        return False
    secret = os.environ.get(TRUSTED_EXECUTION_BENCHMARK_HMAC_ENV)
    integrity = receipt.get("integrity")
    if (
        not secret
        or receipt.get("schema_version") != EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION
        or not isinstance(integrity, dict)
        or integrity.get("algorithm") != "hmac-sha256"
        or integrity.get("key_id") != TRUSTED_EXECUTION_BENCHMARK_KEY_ID
    ):
        return False
    signature = str(integrity.get("signature") or "").lower()
    if len(signature) != 64:
        return False
    try:
        int(signature, 16)
    except ValueError:
        return False
    unsigned = {
        key: value for key, value in receipt.items() if key != "integrity"
    }
    if receipt.get("receipt_digest") != execution_benchmark_receipt_digest(unsigned):
        return False
    unsigned["integrity"] = {
        "algorithm": integrity["algorithm"],
        "key_id": integrity["key_id"],
    }
    try:
        encoded = json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return False
    expected = hmac.new(
        secret.encode("utf-8"), encoded, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def execution_benchmark_receipt_semantically_verified(
    receipt: object,
    *,
    position_id: str,
    symbol: str,
    expected_legs: Mapping[str, tuple[str, str]],
    require_fee_observations: bool = False,
) -> bool:
    """Verify a sealed L2 receipt against its fills and lifecycle identity.

    HMAC proves that the writer sealed *some* JSON.  Promotion also needs the
    semantic relation between immutable L2 depth, requested quantity, venue /
    side, each accepted fill, and the stored adverse-selection aggregate.
    Promotion paths additionally require the literal fill-fee facts to be
    present in the same sealed receipt.
    Keeping this in the execution module gives close reconciliation and
    offline canary analysis one exact verification contract.
    """
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema_version") != EXECUTION_BENCHMARK_RECEIPT_SCHEMA_VERSION
        or receipt.get("source") != "local_l2_vwap"
        or receipt.get("position_id") != position_id
        or receipt.get("symbol") != symbol
        or not execution_benchmark_receipt_integrity_verified(receipt)
    ):
        return False
    captured_at_ms = _receipt_positive_int(receipt.get("captured_at_ms"))
    max_delay_ms = _receipt_positive_int(
        receipt.get("max_observation_to_submit_ms")
    )
    requested_quantity = _receipt_positive_float(receipt.get("requested_base_quantity"))
    reported_shortfall = _receipt_nonnegative_float(
        receipt.get("implementation_shortfall_quote")
    )
    if (
        captured_at_ms is None
        or max_delay_ms != EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS
        or requested_quantity is None
        or reported_shortfall is None
        or set(expected_legs) != {"long", "short"}
    ):
        return False
    quantity_tolerance = max(1e-10, requested_quantity * 1e-8)
    recomputed_total = 0.0
    for name, (expected_venue, expected_side) in expected_legs.items():
        leg = receipt.get(name)
        if not isinstance(leg, dict):
            return False
        vwap_price = _receipt_positive_float(leg.get("vwap_price"))
        available_quantity = _receipt_positive_float(
            leg.get("available_base_quantity")
        )
        observed_at_ms = _receipt_positive_int(leg.get("observed_at_ms"))
        age_ms = _receipt_nonnegative_int(leg.get("age_ms"))
        filled_quantity = _receipt_positive_float(leg.get("filled_base_quantity"))
        reported_leg_shortfall = _receipt_nonnegative_float(
            leg.get("implementation_shortfall_quote")
        )
        fills = leg.get("fills")
        if (
            leg.get("venue") != expected_venue
            or leg.get("side") != expected_side
            or vwap_price is None
            or available_quantity is None
            or available_quantity + quantity_tolerance < requested_quantity
            or observed_at_ms is None
            or age_ms is None
            or observed_at_ms > captured_at_ms
            or captured_at_ms - observed_at_ms != age_ms
            or filled_quantity is None
            or abs(filled_quantity - requested_quantity) > quantity_tolerance
            or reported_leg_shortfall is None
            or not isinstance(fills, list)
            or not fills
        ):
            return False
        observed_fill_quantity = 0.0
        recomputed_leg_shortfall = 0.0
        for fill in fills:
            if not isinstance(fill, dict):
                return False
            fill_quantity = _receipt_positive_float(fill.get("quantity"))
            fill_price = _receipt_positive_float(fill.get("price"))
            submitted_at_ms = _receipt_positive_int(fill.get("submitted_at_ms"))
            filled_at_ms = _receipt_positive_int(fill.get("filled_at_ms"))
            fee_quote = _receipt_finite_float(fill.get("fee_quote"))
            if (
                fill_quantity is None
                or fill_price is None
                or submitted_at_ms is None
                or filled_at_ms is None
                or captured_at_ms > submitted_at_ms
                or submitted_at_ms > filled_at_ms
                or submitted_at_ms - observed_at_ms > max_delay_ms
                or (
                    not str(fill.get("order_id", "") or "")
                    and not str(fill.get("client_order_id", "") or "")
                )
                or (require_fee_observations and fee_quote is None)
            ):
                return False
            observed_fill_quantity += fill_quantity
            adverse_move = (
                fill_price - vwap_price
                if expected_side == "buy"
                else vwap_price - fill_price
            )
            recomputed_leg_shortfall += max(adverse_move, 0.0) * fill_quantity
        leg_tolerance = max(
            1e-8, max(recomputed_leg_shortfall, reported_leg_shortfall) * 1e-8
        )
        if (
            abs(observed_fill_quantity - filled_quantity) > quantity_tolerance
            or abs(recomputed_leg_shortfall - reported_leg_shortfall) > leg_tolerance
        ):
            return False
        recomputed_total += recomputed_leg_shortfall
    total_tolerance = max(1e-8, max(recomputed_total, reported_shortfall) * 1e-8)
    return abs(recomputed_total - reported_shortfall) <= total_tolerance


def position_execution_benchmark_evidence_complete(
    position: OpenPosition,
    *,
    exit_receipts: object | None = None,
    exit_shortfall_quote: object | None = None,
) -> bool:
    """Verify all persisted execution-quality evidence for one position.

    This is the single promotion contract used by terminal PnL reporting and
    post-close funding reconciliation.  It deliberately has no impact on V1
    order routing, recovery, realised PnL, or risk handling: a missing or
    malformed receipt merely makes execution-cost attribution unavailable.
    ``exit_receipts`` and ``exit_shortfall_quote`` let a stateless terminal
    close validate the current close before its facts have been written back
    to ``position``.
    """
    if (
        position.execution_benchmark_complete is not True
        or position.execution_fee_complete is not True
    ):
        return False

    entry_receipt = position.entry_execution_benchmark_receipt
    if not execution_benchmark_receipt_semantically_verified(
        entry_receipt,
        position_id=position.position_id,
        symbol=position.symbol,
        expected_legs={
            "long": (position.long_venue.value, "buy"),
            "short": (position.short_venue.value, "sell"),
        },
    ):
        return False
    assert isinstance(entry_receipt, dict)
    try:
        entry_long = float(entry_receipt["long"]["vwap_price"])
        entry_short = float(entry_receipt["short"]["vwap_price"])
        entry_shortfall = float(entry_receipt["implementation_shortfall_quote"])
        stored_entry_long = float(position.entry_benchmark_long_price)
        stored_entry_short = float(position.entry_benchmark_short_price)
        stored_entry_shortfall = float(position.entry_implementation_shortfall_quote)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    if not all(
        math.isfinite(value)
        for value in (
            entry_long,
            entry_short,
            entry_shortfall,
            stored_entry_long,
            stored_entry_short,
            stored_entry_shortfall,
        )
    ):
        return False
    if (
        entry_long <= 0.0
        or entry_short <= 0.0
        or entry_shortfall < 0.0
        or stored_entry_long <= 0.0
        or stored_entry_short <= 0.0
        or stored_entry_shortfall < 0.0
        or not _receipt_amount_matches(entry_long, stored_entry_long)
        or not _receipt_amount_matches(entry_short, stored_entry_short)
        or not _receipt_amount_matches(entry_shortfall, stored_entry_shortfall)
    ):
        return False

    receipts = (
        position.exit_execution_benchmark_receipts
        if exit_receipts is None
        else exit_receipts
    )
    aggregate_shortfall = (
        position.exit_implementation_shortfall_quote
        if exit_shortfall_quote is None
        else exit_shortfall_quote
    )
    if not isinstance(receipts, list) or not receipts:
        return False
    try:
        stored_exit_shortfall = float(aggregate_shortfall)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(stored_exit_shortfall) or stored_exit_shortfall < 0.0:
        return False

    total_exit_shortfall = 0.0
    for receipt in receipts:
        if not execution_benchmark_receipt_semantically_verified(
            receipt,
            position_id=position.position_id,
            symbol=position.symbol,
            expected_legs={
                "long": (position.long_venue.value, "sell"),
                "short": (position.short_venue.value, "buy"),
            },
        ):
            return False
        assert isinstance(receipt, dict)
        try:
            receipt_shortfall = float(receipt["implementation_shortfall_quote"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(receipt_shortfall) or receipt_shortfall < 0.0:
            return False
        total_exit_shortfall += receipt_shortfall
    return _receipt_amount_matches(total_exit_shortfall, stored_exit_shortfall)


def _receipt_amount_matches(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-8, max(abs(left), abs(right)) * 1e-8)


def execution_benchmark_receipt_max_fill_at_ms(receipt: object) -> int | None:
    """Return the final receipt fill time only for a structurally valid shape."""
    if not isinstance(receipt, dict):
        return None
    latest = 0
    for name in ("long", "short"):
        leg = receipt.get(name)
        fills = leg.get("fills") if isinstance(leg, dict) else None
        if not isinstance(fills, list) or not fills:
            return None
        for fill in fills:
            filled_at_ms = _receipt_positive_int(
                fill.get("filled_at_ms") if isinstance(fill, dict) else None
            )
            if filled_at_ms is None:
                return None
            latest = max(latest, filled_at_ms)
    return latest or None


def _receipt_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _receipt_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _receipt_positive_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _receipt_nonnegative_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _receipt_finite_float(value: object) -> float | None:
    """Accept a literal finite fill fee, including a legitimate rebate."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


class ExitReason(Enum):
    PROFIT_TAKE = "profit_take"
    NET_STOP_LOSS = "net_stop_loss"
    TRAILING_EXIT = "trailing_exit"
    FIRST_STAGE_CAPTURE = "first_stage_capture"
    SECOND_STAGE_CAPTURE = "second_stage_capture"
    FUNDING_CAPTURE = "funding_capture"
    SETTLEMENT_FORCE_CLOSE = "settlement_force_close"
    MARK_PRICE_HARD_STOP = "mark_price_hard_stop"
    RISK_DEATH = "risk_death"
    RISK_DELEVER = "risk_delever"


class CloseState(Enum):
    IDLE = "idle"
    CLOSING_LONG = "closing_long"
    CLOSING_SHORT = "closing_short"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CloseExecution:
    position_id: str
    reason: str
    long_close_price: float
    short_close_price: float
    long_close_qty: float
    short_close_qty: float
    long_fee_quote: float = 0.0
    short_fee_quote: float = 0.0
    realized_price_pnl_quote: float = 0.0
    funding_pnl_quote: float = 0.0
    net_quote: float = 0.0
    # None means the benchmark is unavailable; zero is a verified
    # no-adverse-fill result.  Keep this separate from price PnL.
    implementation_shortfall_quote: float | None = None
    # Backwards-compatible single-receipt view.  Multi-chunk closes use the
    # complete ``execution_benchmark_receipts`` collection below.
    execution_benchmark_receipt: dict[str, object] | None = None
    # One receipt per submitted close chunk.  It is never an input to V1
    # routing, risk, or realised-PnL calculations.
    execution_benchmark_receipts: list[dict[str, object]] = field(
        default_factory=list
    )
    # Unknown venue fees retain V1 accounting behaviour but must not enter a
    # zero-cost canary claim.
    execution_fee_complete: bool = False


def build_reduce_only_close_orders(
    position: OpenPosition,
    reason: ExitReason,
) -> tuple[OrderRequest, OrderRequest]:
    """Build reduce-only close orders for both legs."""
    long_close = OrderRequest(
        venue=position.long_venue,
        symbol=position.symbol,
        side=Side.SELL,
        quantity=abs(position.long_quantity),
        reduce_only=True,
    )
    short_close = OrderRequest(
        venue=position.short_venue,
        symbol=position.symbol,
        side=Side.BUY,
        quantity=abs(position.short_quantity),
        reduce_only=True,
    )
    return long_close, short_close


def compute_close_pnl(
    position: OpenPosition,
    long_fill: OrderFill,
    short_fill: OrderFill,
) -> CloseExecution:
    """Compute PnL attribution for a close execution."""
    matched_qty = min(long_fill.quantity, short_fill.quantity)
    realized_pnl = (
        (long_fill.price - position.long_entry_price) * matched_qty
        + (position.short_entry_price - short_fill.price) * matched_qty
    )
    long_fee = long_fill.fee_quote or 0.0
    short_fee = short_fill.fee_quote or 0.0
    return CloseExecution(
        position_id=position.position_id,
        reason="manual",
        long_close_price=long_fill.price,
        short_close_price=short_fill.price,
        long_close_qty=long_fill.quantity,
        short_close_qty=short_fill.quantity,
        long_fee_quote=long_fee,
        short_fee_quote=short_fee,
        realized_price_pnl_quote=realized_pnl,
        net_quote=realized_pnl - long_fee - short_fee,
    )
