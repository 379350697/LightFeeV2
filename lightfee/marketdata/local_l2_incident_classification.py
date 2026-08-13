from __future__ import annotations

from typing import Any

BINANCE_LOCAL_BOOK_DOC = (
    "https://developers.binance.com/docs/derivatives/usds-margined-futures/"
    "websocket-market-streams/How-to-manage-a-local-order-book-correctly"
)
ASTER_LOCAL_BOOK_DOC = "https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/"
OKX_LOCAL_BOOK_DOC = "https://my.okx.com/docs-v5/en/?language=python"

LOCAL_BOOK_DOC_BY_VENUE = {
    "aster": ASTER_LOCAL_BOOK_DOC,
    "binance": BINANCE_LOCAL_BOOK_DOC,
    "okx": OKX_LOCAL_BOOK_DOC,
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

    Binance requires `pu` to match the previous update's `u`.  Aster's V1
    live path has one narrower rule: a stale `pu` is not a rebuild when its
    `U..u` range already covers the next expected local sequence.  A real
    sequence skip after a valid previous link remains a rebuild.
    """

    venue = str(payload.get("venue", "") or "").lower()
    if venue == "okx":
        return _official_okx_sequence_rebuild_reason(payload)
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
        if venue == "aster" and raw_U <= expected_previous + 1 <= raw_u:
            return ""
        if snapshot_last_update_id is not None and expected_previous < snapshot_last_update_id:
            return ""
        return "previous_link_mismatch"

    if raw_U > expected_previous + 1:
        return "expected_real_gap"

    if snapshot_last_update_id is not None and raw_U > snapshot_last_update_id:
        return "snapshot_boundary"

    return ""


def _official_okx_sequence_rebuild_reason(payload: dict[str, Any]) -> str:
    """Classify OKX local-book continuity evidence.

    OKX documents `prevSeqId`/`seqId` as the continuity source for incremental
    order-book messages. A previous-link mismatch is therefore exchange
    sequence evidence, while a checksum mismatch is official data-integrity
    evidence until OKX's JSON checksum deprecation date.
    """

    text = " ".join(
        str(payload.get(key, "") or "")
        for key in ("error", "reason", "rebuild_trigger", "fault_reason")
    ).lower()
    if "checksum_mismatch" in text or "checksum mismatch" in text:
        return "checksum_mismatch"

    if payload.get("previous_sequence_present") is not True:
        return ""

    raw_prev = _int_payload(payload, "raw_pu")
    raw_seq = _int_payload(payload, "raw_u")
    expected_previous = _int_payload(payload, "expected_previous_sequence")
    if None in (raw_prev, raw_seq, expected_previous):
        return ""

    if raw_prev != expected_previous and raw_seq > expected_previous:
        return "previous_link_mismatch"

    if raw_prev == expected_previous and raw_seq < expected_previous:
        return "sequence_reset"

    return ""


def has_official_sequence_rebuild_evidence(payload: dict[str, Any]) -> bool:
    return bool(official_sequence_rebuild_reason(payload))
