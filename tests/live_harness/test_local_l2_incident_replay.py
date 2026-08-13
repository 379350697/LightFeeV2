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
from lightfee.marketdata.local_l2_incident_classification import (
    BINANCE_LOCAL_BOOK_DOC,
    official_local_book_doc_url,
    official_sequence_rebuild_reason,
)
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


def _aster_overlap_v1_parity_drift(
    *,
    event_kind: str,
    venue: str,
    raw_U: int | None,
    raw_u: int | None,
    expected_previous: int | None,
) -> IncidentClassification | None:
    if not (
        venue == "aster"
        and raw_U is not None
        and raw_u is not None
        and expected_previous is not None
        and raw_U <= expected_previous + 1 <= raw_u
    ):
        return None
    return IncidentClassification(
        event_kind=event_kind,
        classification="V1 parity drift",
        evidence=(
            "CL-L2-ASTER-OVERLAP: V1 accepts an Aster U..u range "
            "that covers the next expected sequence despite a stale pu."
        ),
        v1_behavior="accept overlapping Aster ranged delta",
        v2_behavior="rebuilt on stale previous-link before applying range",
        proven_local_l2_drift=True,
        data_plane_change_allowed=True,
    )


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
        doc_url = official_local_book_doc_url(venue)
        aster_v1_drift = _aster_overlap_v1_parity_drift(
            event_kind=kind,
            venue=venue,
            raw_U=raw_U,
            raw_u=raw_u,
            expected_previous=expected_previous,
        )
        if aster_v1_drift is not None:
            return aster_v1_drift
        official_reason = official_sequence_rebuild_reason(payload)
        if doc_url and venue == "okx" and official_reason in {
            "previous_link_mismatch",
            "sequence_reset",
            "checksum_mismatch",
        }:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="OKX documents seqId/prevSeqId continuity and checksum data-integrity semantics.",
                official_doc_url=doc_url,
            )
        if doc_url and official_reason == "expected_real_gap" and has_real_gap_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="expected real gap",
                evidence="Diff-depth stream skipped at least one update after the known previous sequence.",
                official_doc_url=doc_url,
            )
        if doc_url and official_reason == "previous_link_mismatch" and has_strict_gap_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="Diff-depth updates require strict pu-to-previous-u continuity.",
                official_doc_url=doc_url,
            )
        return IncidentClassification(
            event_kind=kind,
            classification="insufficient evidence",
            evidence="Missing official sequence semantics for this venue/sample.",
            evidence_gap=True,
        )

    if kind == "runtime.local_l2_snapshot_error":
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
        doc_url = official_local_book_doc_url(venue)
        aster_v1_drift = _aster_overlap_v1_parity_drift(
            event_kind=kind,
            venue=venue,
            raw_U=raw_U,
            raw_u=raw_u,
            expected_previous=expected_previous,
        )
        if aster_v1_drift is not None:
            return aster_v1_drift
        official_reason = official_sequence_rebuild_reason(payload)
        if doc_url and venue == "okx" and official_reason in {
            "previous_link_mismatch",
            "sequence_reset",
            "checksum_mismatch",
        }:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="OKX documents seqId/prevSeqId continuity and checksum data-integrity semantics.",
                official_doc_url=doc_url,
            )
        if doc_url and official_reason == "previous_link_mismatch" and has_previous_link_mismatch_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="A buffered replay pu mismatch requires local book reinitialization.",
                official_doc_url=doc_url,
            )
        if doc_url and official_reason == "snapshot_boundary" and has_snapshot_boundary_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="The first buffered update does not bridge the REST snapshot lastUpdateId.",
                official_doc_url=doc_url,
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


def test_aster_overlap_previous_link_is_v1_parity_drift_not_an_exchange_reset():
    sample = {
        "kind": "runtime.local_l2_snapshot_error",
        "payload": {
            "venue": "aster",
            "symbol": "LABUSDT",
            "category": "buffered_replay_failed",
            "reason": "buffered_replay_previous_link_mismatch: expected 468889077688 got 468889077062",
            "previous_sequence_present": True,
            "snapshot_last_update_id": 468889077688,
            "expected_previous_sequence": 468889077688,
            "raw_U": 468889077553,
            "raw_u": 468889077847,
            "raw_pu": 468889077062,
            "status_before": "bootstrapping",
            "status_after": "rebuilding",
        },
    }

    result = classify_local_l2_incident(sample)

    assert result.classification == "V1 parity drift"
    assert result.official_doc_url == ""
    assert result.evidence_gap is False
    assert result.data_plane_change_allowed is True
    assert result.stale_threshold_change_allowed is False


class _RecordingJournal:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict]] = []

    def append(self, kind: str, payload: dict, **kwargs):
        self.records.append((kind, payload))
        return len(self.records)


class _MockL2Adapter:
    def __init__(self, venue: str, sequence: int):
        self.venue = venue
        self.sequence = sequence

    async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
        return LocalL2Update(
            venue=self.venue,
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
        _MockL2Adapter("binance", sequence=100),
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
        _MockL2Adapter("binance", sequence=200),
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


@pytest.mark.asyncio
async def test_aster_overlapping_previous_link_survives_ws_and_buffered_replay():
    """Production Local-L2 harness for the Aster U..u overlap V1 accepts.

    Aster's `pu` can lag the local sequence while its `U..u` range already
    contains the next required sequence.  V1 accepts that update both while
    hot and during REST-snapshot buffered replay; V2 used to rebuild in both
    paths and permanently block the owned entry session.
    """
    update = LocalL2Update(
        venue="aster",
        symbol="ASTERUSDT",
        bids=[PriceLevel(100.0, 1.0)],
        asks=[PriceLevel(101.0, 1.0)],
        first_sequence=101,
        sequence=103,
        previous_sequence=99,
        previous_sequence_present=True,
        update_kind=LocalL2UpdateKind.DELTA,
    )

    hot_runtime = LocalL2Runtime()
    hot_plane = LocalL2DataPlane(hot_runtime, _RecordingJournal())
    hot_book = hot_runtime.ensure_book("aster", "ASTERUSDT")
    hot_book.apply_snapshot(
        [PriceLevel(100.0, 1.0)], [PriceLevel(101.0, 1.0)],
        sequence=100, now_ms=1_000,
    )
    hot_book.status = L2BookStatus.BOOTSTRAPPING
    hot_book.transition_to_hot()
    hot_plane.ingest_external_update(update, now_ms=1_100)

    assert hot_book.status == L2BookStatus.HOT
    assert hot_book.sequence == 103

    replay_runtime = LocalL2Runtime()
    replay_plane = LocalL2DataPlane(replay_runtime, _RecordingJournal())
    replay_book = replay_runtime.ensure_book("aster", "ASTERUSDT")
    replay_book.status = L2BookStatus.BOOTSTRAPPING
    replay_plane.ingest_external_update(update, now_ms=1_100)

    recovered = await replay_plane.bootstrap_book(
        "aster", "ASTERUSDT", _MockL2Adapter("aster", sequence=100), now_ms=1_200,
    )

    assert recovered is True
    assert replay_book.status == L2BookStatus.HOT
    assert replay_book.sequence == 103


def test_aster_uncovered_range_still_rebuilds_fail_closed():
    runtime = LocalL2Runtime()
    data_plane = LocalL2DataPlane(runtime, _RecordingJournal())
    book = runtime.ensure_book("aster", "ASTERUSDT")
    book.apply_snapshot(
        [PriceLevel(100.0, 1.0)], [PriceLevel(101.0, 1.0)],
        sequence=100, now_ms=1_000,
    )
    book.status = L2BookStatus.BOOTSTRAPPING
    book.transition_to_hot()

    data_plane.ingest_external_update(
        LocalL2Update(
            venue="aster",
            symbol="ASTERUSDT",
            bids=[PriceLevel(100.0, 1.0)],
            asks=[PriceLevel(101.0, 1.0)],
            first_sequence=102,
            sequence=103,
            previous_sequence=99,
            previous_sequence_present=True,
            update_kind=LocalL2UpdateKind.DELTA,
        ),
        now_ms=1_100,
    )

    assert book.status == L2BookStatus.REBUILDING


@pytest.mark.asyncio
async def test_runtime_l2_sync_uses_decision_time_not_tick_start_time(tmp_path, monkeypatch):
    """Production runtime harness for an async tick crossing the clock domain.

    The old tick timestamp is intentionally 12 seconds behind a fresh WS book;
    only using the decision-time clock must keep the book HOT.  A real future
    observed timestamp remains covered by the data-plane stale-clock test.
    """
    from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
    from lightfee.engine.runtime import LiveRuntime

    runtime = LiveRuntime(
        AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            ),
            strategy=StrategyConfig(local_l2_enabled=True, local_l2_ws_enabled=True),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "state.json"),
            ),
        )
    )
    runtime.l2_data_plane.hot_stale_after_ms = 60_000
    book = runtime.local_l2_runtime.ensure_book("binance", "BTCUSDT")
    book.apply_snapshot(
        [PriceLevel(100.0, 1.0)], [PriceLevel(101.0, 1.0)],
        sequence=10, now_ms=12_000,
    )
    book.status = L2BookStatus.BOOTSTRAPPING
    book.transition_to_hot()
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 13_000)

    await runtime._sync_local_l2_data(1_000)

    assert book.status == L2BookStatus.HOT
