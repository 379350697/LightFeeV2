from __future__ import annotations

import json

import pytest

from lightfee.offline.acceptance_evidence import (
    AcceptanceEvidenceError,
    build_signed_manifest,
    read_verified_jsonl,
)


def _manifest(path, *, secret: str = "secret") -> dict[str, object]:
    return build_signed_manifest(
        [path],
        report_kind="funding_canary",
        secret=secret,
    )


def test_verified_jsonl_binds_exact_source_bytes_and_metadata(tmp_path, monkeypatch) -> None:
    events = tmp_path / "events.jsonl"
    manifest = tmp_path / "events.manifest.json"
    events.write_text(json.dumps({"kind": "x", "payload": {}}) + "\n", encoding="utf-8")
    manifest.write_text(json.dumps(_manifest(events)), encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY", "secret")

    records, evidence = read_verified_jsonl(
        [events],
        report_kind="funding_canary",
        manifest_path=manifest,
    )

    assert records == [{"kind": "x", "payload": {}}]
    assert evidence.as_dict()["verified"] is True
    assert evidence.sources[0].record_count == 1


def test_verified_jsonl_rejects_tampered_or_malformed_input(tmp_path, monkeypatch) -> None:
    events = tmp_path / "events.jsonl"
    manifest = tmp_path / "events.manifest.json"
    events.write_text(json.dumps({"kind": "x", "payload": {}}) + "\n", encoding="utf-8")
    manifest.write_text(json.dumps(_manifest(events)), encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY", "secret")
    events.write_text("{not-json}\n", encoding="utf-8")

    with pytest.raises(AcceptanceEvidenceError, match="malformed JSONL"):
        read_verified_jsonl(
            [events],
            report_kind="funding_canary",
            manifest_path=manifest,
        )


def test_verified_jsonl_rejects_duplicate_source_content(tmp_path, monkeypatch) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    manifest = tmp_path / "events.manifest.json"
    content = json.dumps({"kind": "x", "payload": {}}) + "\n"
    first.write_text(content, encoding="utf-8")
    second.write_text(content, encoding="utf-8")
    manifest.write_text(json.dumps(_manifest(first)), encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY", "secret")

    with pytest.raises(AcceptanceEvidenceError, match="duplicate source content"):
        read_verified_jsonl(
            [first, second],
            report_kind="funding_canary",
            manifest_path=manifest,
        )


def test_verified_jsonl_rejects_manifest_with_nonfinite_integrity_payload(
    tmp_path, monkeypatch
) -> None:
    events = tmp_path / "events.jsonl"
    manifest = tmp_path / "events.manifest.json"
    events.write_text(json.dumps({"kind": "x", "payload": {}}) + "\n", encoding="utf-8")
    forged = _manifest(events)
    forged["extra"] = float("nan")
    manifest.write_text(json.dumps(forged), encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY", "secret")

    with pytest.raises(AcceptanceEvidenceError, match="integrity invalid"):
        read_verified_jsonl(
            [events],
            report_kind="funding_canary",
            manifest_path=manifest,
        )


def test_acceptance_key_identity_is_code_owned_not_caller_controlled(
    tmp_path, monkeypatch
) -> None:
    events = tmp_path / "events.jsonl"
    manifest = tmp_path / "events.manifest.json"
    events.write_text(json.dumps({"kind": "x", "payload": {}}) + "\n", encoding="utf-8")
    monkeypatch.setenv("LIGHTFEE_ACCEPTANCE_EVIDENCE_HMAC_KEY", "secret")

    with pytest.raises(AcceptanceEvidenceError, match="key policy mismatch"):
        build_signed_manifest(
            [events],
            report_kind="funding_canary",
            integrity_key_id="attacker-controlled-key",
            secret="secret",
        )
    manifest.write_text(json.dumps(_manifest(events)), encoding="utf-8")
    with pytest.raises(AcceptanceEvidenceError, match="key policy mismatch"):
        read_verified_jsonl(
            [events],
            report_kind="funding_canary",
            manifest_path=manifest,
            integrity_key_env="ATTACKER_CONTROLLED_ENV",
        )
