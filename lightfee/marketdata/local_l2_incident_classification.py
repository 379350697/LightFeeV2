from __future__ import annotations

from typing import Any

BINANCE_LOCAL_BOOK_DOC = (
    "https://developers.binance.com/docs/derivatives/usds-margined-futures/"
    "websocket-market-streams/How-to-manage-a-local-order-book-correctly"
)
ASTER_LOCAL_BOOK_DOC = "https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/"

LOCAL_BOOK_DOC_BY_VENUE = {
    "aster": ASTER_LOCAL_BOOK_DOC,
    "binance": BINANCE_LOCAL_BOOK_DOC,
}


def _int_payload(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def official_local_book_doc_url(venue: Any) -> str:
    return LOCAL_BOOK_DOC_BY_VENUE.get(str(venue or "").lower(), "")


def official_sequence_rebuild_reason(payload: dict[str, Any]) -> str:
    """Classify Binance-compatible local-book continuity evidence.

    Binance and Aster diff-depth semantics require `pu` to match the previous
    update's `u`; a mismatch or an unbridged snapshot boundary requires a
    local-book rebuild. A real sequence skip after a valid previous link is
    also an exchange-continuity rebuild, not a parser relaxation point.
    """

    venue = str(payload.get("venue", "") or "").lower()
    if venue not in LOCAL_BOOK_DOC_BY_VENUE:
        return ""
    if payload.get("previous_sequence_present") is not True:
        return ""

    raw_pu = _int_payload(payload, "raw_pu")
    raw_u = _int_payload(payload, "raw_u")
    raw_U = _int_payload(payload, "raw_U")
    expected_previous = _int_payload(payload, "expected_previous_sequence")
    if None in (raw_pu, raw_u, raw_U, expected_previous):
        return ""
    if raw_U > raw_u:
        return ""

    snapshot_last_update_id = _int_payload(payload, "snapshot_last_update_id")
    if snapshot_last_update_id is None:
        snapshot_last_update_id = _int_payload(payload, "snapshot_lastUpdateId")

    if raw_pu != expected_previous:
        if snapshot_last_update_id is not None and expected_previous < snapshot_last_update_id:
            return ""
        return "previous_link_mismatch"

    if raw_U > expected_previous + 1:
        return "expected_real_gap"

    if snapshot_last_update_id is not None and raw_U > snapshot_last_update_id:
        return "snapshot_boundary"

    return ""


def has_official_sequence_rebuild_evidence(payload: dict[str, Any]) -> bool:
    return bool(official_sequence_rebuild_reason(payload))
