from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import pytest

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime

Classification = Literal[
    "V1 parity drift",
    "official-doc exchange reset/sequence behavior",
    "expected real gap",
    "insufficient evidence",
]


ALLOWED_CLASSIFICATIONS: set[str] = {
    "V1 parity drift",
    "official-doc exchange reset/sequence behavior",
    "expected real gap",
    "insufficient evidence",
}

BINANCE_LOCAL_BOOK_DOC = (
    "https://developers.binance.com/docs/derivatives/usds-margined-futures/"
    "websocket-market-streams/How-to-manage-a-local-order-book-correctly"
)

FIXTURE_PATH = Path("tests/fixtures/live_incidents/2026-05-26/local_l2_after_1310.jsonl")
BINANCE_REPLAY_FIXTURE_PATH = Path(
    "tests/fixtures/live_incidents/2026-05-27/binance_local_l2_replay_mismatch.jsonl"
)
POST_1310_CUTOFF = datetime.fromisoformat("2026-05-26T13:10:00+08:00")
BINANCE_REPLAY_REQUIRED_FIELDS = {
    "raw_U",
    "raw_u",
    "raw_pu",
    "expected_previous_sequence",
    "snapshot_last_update_id",
    "status_before",
    "status_after",
}


@dataclass(frozen=True)
class IncidentClassification:
    event_kind: str
    classification: Classification
    evidence: str
    official_doc_url: str = ""
    evidence_gap: bool = False
    v1_behavior: str = ""
    v2_behavior: str = ""
    proven_local_l2_drift: bool = False
    data_plane_change_allowed: bool = False
    stale_threshold_change_allowed: bool = False


def load_local_l2_incident_samples() -> list[dict]:
    return [
        json.loads(line)
        for line in FIXTURE_PATH.read_text().splitlines()
        if line.strip()
    ]


def load_binance_replay_samples() -> list[dict]:
    return [
        json.loads(line)
        for line in BINANCE_REPLAY_FIXTURE_PATH.read_text().splitlines()
        if line.strip()
    ]


def _int_payload(payload: dict, key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    return int(value)


def _has_required_binance_replay_evidence(payload: dict) -> bool:
    return all(payload.get(field) is not None for field in BINANCE_REPLAY_REQUIRED_FIELDS)


def classify_local_l2_incident(sample: dict) -> IncidentClassification:
    kind = str(sample["kind"])
    payload = sample.get("payload", {})
    venue = str(payload.get("venue", "")).lower()

    if kind == "runtime.local_l2_sequence_gap_rebuild":
        raw_pu = _int_payload(payload, "raw_pu")
        expected_previous = _int_payload(payload, "expected_previous_sequence")
        raw_u = _int_payload(payload, "raw_u")
        raw_U = _int_payload(payload, "raw_U")
        has_strict_gap_evidence = (
            raw_pu is not None
            and expected_previous is not None
            and raw_u is not None
            and raw_U is not None
            and raw_pu != expected_previous
            and raw_U <= raw_u
        )
        has_real_gap_evidence = (
            raw_pu is not None
            and expected_previous is not None
            and raw_u is not None
            and raw_U is not None
            and raw_pu == expected_previous
            and raw_U > expected_previous + 1
            and raw_U <= raw_u
        )
        if venue == "binance" and payload.get("previous_sequence_present") and has_real_gap_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="expected real gap",
                evidence="Binance diff-depth stream skipped at least one update after the known previous sequence.",
                official_doc_url=BINANCE_LOCAL_BOOK_DOC,
            )
        if venue == "binance" and payload.get("previous_sequence_present") and has_strict_gap_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="Binance diff-depth updates require strict pu-to-previous-u continuity.",
                official_doc_url=BINANCE_LOCAL_BOOK_DOC,
            )
        return IncidentClassification(
            event_kind=kind,
            classification="insufficient evidence",
            evidence="Missing official sequence semantics for this venue/sample.",
            evidence_gap=True,
        )

    if kind == "runtime.local_l2_snapshot_error":
        reason = str(payload.get("reason", ""))
        if venue == "binance" and payload.get("category") == "buffered_replay_failed":
            if not _has_required_binance_replay_evidence(payload):
                return IncidentClassification(
                    event_kind=kind,
                    classification="insufficient evidence",
                    evidence="Buffered replay sample lacks required raw sequence/status evidence.",
                    evidence_gap=True,
                )
        raw_pu = _int_payload(payload, "raw_pu")
        raw_u = _int_payload(payload, "raw_u")
        raw_U = _int_payload(payload, "raw_U")
        expected_previous = _int_payload(payload, "expected_previous_sequence")
        snapshot_last_update_id = _int_payload(payload, "snapshot_last_update_id")
        has_previous_link_mismatch_evidence = (
            raw_pu is not None
            and expected_previous is not None
            and snapshot_last_update_id is not None
            and raw_pu != expected_previous
            and expected_previous >= snapshot_last_update_id
        )
        has_snapshot_boundary_evidence = (
            raw_U is not None
            and raw_u is not None
            and snapshot_last_update_id is not None
            and raw_U <= raw_u
            and raw_U > snapshot_last_update_id
        )
        if (
            venue == "binance"
            and "previous_link_mismatch" in reason
            and has_previous_link_mismatch_evidence
        ):
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="A buffered replay pu mismatch requires local book reinitialization.",
                official_doc_url=BINANCE_LOCAL_BOOK_DOC,
            )
        if (
            venue == "binance"
            and "snapshot_boundary" in reason
            and has_snapshot_boundary_evidence
        ):
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="The first buffered update does not bridge the REST snapshot lastUpdateId.",
                official_doc_url=BINANCE_LOCAL_BOOK_DOC,
            )
        return IncidentClassification(
            event_kind=kind,
            classification="insufficient evidence",
            evidence="Snapshot error lacks venue-specific replay proof.",
            evidence_gap=True,
        )

    if kind == "runtime.snapshot_fallback_last_good":
        v1_behavior = str(payload.get("v1_expected_behavior", ""))
        v2_behavior = str(payload.get("v2_observed_behavior", ""))
        if (
            payload.get("v1_parity_evidence")
            and _int_payload(payload, "fallback_duration_ms") is not None
            and _int_payload(payload, "last_good_age_ms") is not None
            and payload.get("domain")
            and v1_behavior
            and v2_behavior
            and v1_behavior != v2_behavior
        ):
            return IncidentClassification(
                event_kind=kind,
                classification="V1 parity drift",
                evidence=str(payload["v1_parity_evidence"]),
                v1_behavior=v1_behavior,
                v2_behavior=v2_behavior,
            )
        return IncidentClassification(
            event_kind=kind,
            classification="insufficient evidence",
            evidence="Last-good fallback sample lacks V1 parity or official-doc evidence.",
            evidence_gap=True,
        )

    return IncidentClassification(
        event_kind=kind,
        classification="insufficient evidence",
        evidence="Unsupported Local-L2 incident kind for this replay gate.",
        evidence_gap=True,
    )


def test_post_1310_local_l2_samples_are_classified_into_closed_evidence_set():
    samples = load_local_l2_incident_samples()
    results = [classify_local_l2_incident(sample) for sample in samples]

    assert {sample["kind"] for sample in samples} == {
        "runtime.local_l2_sequence_gap_rebuild",
        "runtime.local_l2_snapshot_error",
        "runtime.snapshot_fallback_last_good",
    }
    assert all(datetime.fromisoformat(sample["ts"]) >= POST_1310_CUTOFF for sample in samples)
    assert {result.classification for result in results} <= ALLOWED_CLASSIFICATIONS


def test_official_doc_classifications_include_doc_and_v1_drift_includes_parity_key():
    results = [classify_local_l2_incident(sample) for sample in load_local_l2_incident_samples()]

    for result in results:
        if result.classification == "official-doc exchange reset/sequence behavior":
            assert result.official_doc_url.startswith("https://")
            assert result.evidence_gap is False
        if result.classification == "V1 parity drift":
            assert result.evidence.startswith("CL-")
            assert result.official_doc_url == ""
            assert result.v1_behavior
            assert result.v2_behavior
            assert result.v1_behavior != result.v2_behavior


def test_replay_gate_does_not_authorize_local_l2_data_plane_logic_changes():
    results = [classify_local_l2_incident(sample) for sample in load_local_l2_incident_samples()]

    assert all(not result.proven_local_l2_drift for result in results)
    assert all(not result.data_plane_change_allowed for result in results)
    assert all(not result.stale_threshold_change_allowed for result in results)


def test_insufficient_evidence_is_a_terminal_non_authorizing_classification():
    samples = load_local_l2_incident_samples()
    insufficient = [
        classify_local_l2_incident(sample)
        for sample in samples
        if sample["payload"].get("reason") == "transport_timeout"
    ]

    assert len(insufficient) == 1
    result = insufficient[0]
    assert result.classification == "insufficient evidence"
    assert result.evidence_gap is True
    assert result.official_doc_url == ""
    assert result.data_plane_change_allowed is False
    assert result.stale_threshold_change_allowed is False


def test_expected_real_gap_is_classified_without_authorizing_threshold_relaxation():
    samples = load_local_l2_incident_samples()
    real_gaps = [
        classify_local_l2_incident(sample)
        for sample in samples
        if sample["payload"].get("symbol") == "ACEUSDT"
    ]

    assert len(real_gaps) == 1
    result = real_gaps[0]
    assert result.classification == "expected real gap"
    assert result.official_doc_url == BINANCE_LOCAL_BOOK_DOC
    assert result.data_plane_change_allowed is False
    assert result.stale_threshold_change_allowed is False


def test_binance_buffered_replay_samples_require_raw_sequence_and_status_evidence():
    samples = load_binance_replay_samples()
    results = [classify_local_l2_incident(sample) for sample in samples]

    assert len(samples) == 3
    for sample, result in zip(samples, results):
        payload = sample["payload"]
        if payload.get("fixture_case") == "missing_required_fields":
            assert result.classification == "insufficient evidence"
            assert result.evidence_gap is True
            continue

        assert _has_required_binance_replay_evidence(payload)
        assert result.classification == "official-doc exchange reset/sequence behavior"
        assert result.official_doc_url == BINANCE_LOCAL_BOOK_DOC
        assert result.evidence_gap is False
        assert result.data_plane_change_allowed is False
        assert result.stale_threshold_change_allowed is False


class _RecordingJournal:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict]] = []

    def append(self, kind: str, payload: dict, **kwargs):
        self.records.append((kind, payload))
        return len(self.records)


class _MockL2Adapter:
    def __init__(self, sequence: int):
        self.sequence = sequence

    async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
        return LocalL2Update(
            venue="binance",
            symbol=symbol,
            bids=[PriceLevel(100.0, 1.0)],
            asks=[PriceLevel(101.0, 1.0)],
            sequence=self.sequence,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )


@pytest.mark.asyncio
async def test_binance_buffered_replay_mismatch_rebuilds_without_entry_readiness_and_recovers_on_later_snapshot_ok():
    rt = LocalL2Runtime()
    journal = _RecordingJournal()
    dp = LocalL2DataPlane(rt, journal)
    book = rt.ensure_book("binance", "EDENUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    rt.assign("binance", "EDENUSDT", L2PoolAssignment.HOT_EXEC, now_ms=1000)

    for now_ms, raw_U, raw_u, raw_pu in (
        (1100, 101, 102, 100),
        (1200, 103, 104, 101),
    ):
        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="EDENUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[PriceLevel(101.0, 1.0)],
                first_sequence=raw_U,
                sequence=raw_u,
                previous_sequence=raw_pu,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=now_ms,
        )

    ok = await dp.bootstrap_book(
        "binance",
        "EDENUSDT",
        _MockL2Adapter(sequence=100),
        now_ms=2000,
    )

    assert ok is False
    assert book.status == L2BookStatus.REBUILDING
    assert book.is_ready(max_age_ms=5000, now_ms=2000) is False
    rt.sync(now_ms=2000)
    assert rt.metrics.active_books == 0
    assert rt.metrics.hot_exec_not_ready_books == 1

    payload = [
        payload for kind, payload in journal.records
        if kind == "runtime.local_l2_snapshot_error"
        and payload.get("category") == "buffered_replay_failed"
    ][0]
    classification = classify_local_l2_incident(
        {"kind": "runtime.local_l2_snapshot_error", "payload": payload}
    )
    assert classification.classification == "official-doc exchange reset/sequence behavior"
    assert payload["status_after"] == "rebuilding"

    recovered = await dp.bootstrap_book(
        "binance",
        "EDENUSDT",
        _MockL2Adapter(sequence=200),
        now_ms=3000,
    )

    assert recovered is True
    assert book.status == L2BookStatus.HOT
    assert book.is_ready(max_age_ms=5000, now_ms=3000) is True
    assert [
        payload for kind, payload in journal.records
        if kind == "runtime.local_l2_snapshot_ok"
        and payload.get("venue") == "binance"
        and payload.get("symbol") == "EDENUSDT"
    ]
