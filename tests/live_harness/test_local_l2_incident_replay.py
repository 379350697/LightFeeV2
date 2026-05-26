from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal


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
POST_1310_CUTOFF = datetime.fromisoformat("2026-05-26T13:10:00+08:00")


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


def _int_payload(payload: dict, key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    return int(value)


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
        raw_pu = _int_payload(payload, "raw_pu")
        expected_previous = _int_payload(payload, "expected_previous_sequence")
        snapshot_last_update_id = _int_payload(payload, "snapshot_last_update_id")
        has_replay_gap_evidence = (
            raw_pu is not None
            and expected_previous is not None
            and snapshot_last_update_id is not None
            and raw_pu != expected_previous
            and expected_previous >= snapshot_last_update_id
        )
        if venue == "binance" and "previous_link_mismatch" in reason and has_replay_gap_evidence:
            return IncidentClassification(
                event_kind=kind,
                classification="official-doc exchange reset/sequence behavior",
                evidence="A buffered replay pu mismatch requires local book reinitialization.",
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
