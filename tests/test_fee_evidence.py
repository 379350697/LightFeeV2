from __future__ import annotations

import json

from lightfee.strategy.fee_evidence import (
    FEE_EVIDENCE_SCHEMA_VERSION,
    TRUSTED_FEE_EVIDENCE_HMAC_ENV,
    TRUSTED_FEE_EVIDENCE_KEY_ID,
    effective_fee_maps,
    load_fee_evidence,
    sign_fee_evidence_payload,
)


def _payload(*, observed_at_ms: int, source: str = "account_fee_api") -> dict:
    return {
        "schema_version": 1,
        "venues": {
            "cheap": {
                "taker_fee_bps": 0.7,
                "maker_fee_bps": 0.2,
                "observed_at_ms": observed_at_ms,
                "source": source,
                "evidence_ref": "account-fee-snapshot-20260714",
                "account_identity_hash": "a" * 64,
            },
            "rich": {
                "taker_fee_bps": 0.6,
                "maker_fee_bps": 0.1,
                "observed_at_ms": observed_at_ms,
                "source": source,
                "evidence_ref": "account-fee-snapshot-20260714",
                "account_identity_hash": "b" * 64,
            },
        },
    }


def test_fee_evidence_requires_fresh_private_schedule_and_preserves_static_floor(
    tmp_path,
) -> None:
    path = tmp_path / "account-fees.json"
    path.write_text(json.dumps(_payload(observed_at_ms=1_000)), encoding="utf-8")

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)
    taker, maker = effective_fee_maps(
        {"cheap": 0.8, "rich": 0.5},
        {"cheap": 0.3, "rich": 0.5},
        evidence,
    )

    assert evidence.complete_for("cheap", "rich")
    assert evidence.observed_at_ms_for("cheap", "rich") == 1_000
    assert evidence.source_for("cheap", "rich") == "account_fee_api"
    # Verified evidence can make a model more conservative, never less.
    assert taker == {"cheap": 0.8, "rich": 0.6}
    assert maker == {"cheap": 0.3, "rich": 0.5}


def test_fee_evidence_fails_closed_when_stale_or_untrusted(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    path.write_text(json.dumps(_payload(observed_at_ms=1, source="web_page")), encoding="utf-8")

    untrusted = load_fee_evidence(path, now_ms=100, max_age_ms=1_000)
    assert untrusted.loaded is False
    assert untrusted.reason == "fee_evidence_untrusted_source:cheap"

    path.write_text(json.dumps(_payload(observed_at_ms=1)), encoding="utf-8")
    stale = load_fee_evidence(path, now_ms=1_000, max_age_ms=100)
    assert stale.loaded is False
    assert stale.reason == "fee_evidence_stale:cheap"


def test_fee_evidence_rejects_case_colliding_venue_records(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    payload = _payload(observed_at_ms=1_000)
    payload["venues"]["CHEAP"] = dict(payload["venues"]["cheap"])
    path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is False
    assert evidence.reason == "fee_evidence_duplicate_venue:cheap"


def test_fee_evidence_hmac_binds_immutable_pair_provenance(tmp_path, monkeypatch) -> None:
    path = tmp_path / "account-fees.json"
    payload = _payload(observed_at_ms=1_000)
    payload["schema_version"] = FEE_EVIDENCE_SCHEMA_VERSION
    payload["integrity"] = {
        "algorithm": "hmac-sha256",
        "key_id": TRUSTED_FEE_EVIDENCE_KEY_ID,
        "signature": "",
    }
    monkeypatch.setenv(TRUSTED_FEE_EVIDENCE_HMAC_ENV, "secret")
    payload = sign_fee_evidence_payload(payload, "secret")
    path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = load_fee_evidence(
        path,
        now_ms=1_100,
        max_age_ms=500,
    )

    assert evidence.integrity_verified is True
    assert evidence.fingerprint_for("cheap", "rich")
    assert all(
        row["document_sha256"] == evidence.document_sha256
        for row in evidence.provenance_for("cheap", "rich")
    )

    payload["venues"]["cheap"]["taker_fee_bps"] = 0.1
    path.write_text(json.dumps(payload), encoding="utf-8")
    tampered = load_fee_evidence(
        path,
        now_ms=1_100,
        max_age_ms=500,
    )
    assert tampered.loaded is False
    assert tampered.reason == "fee_evidence_integrity_mismatch"

    payload = sign_fee_evidence_payload(_payload(observed_at_ms=1_000) | {
        "schema_version": FEE_EVIDENCE_SCHEMA_VERSION,
        "integrity": {
            "algorithm": "hmac-sha256",
            "key_id": TRUSTED_FEE_EVIDENCE_KEY_ID,
            "signature": "",
        },
    }, "secret")
    payload["integrity"]["key_id"] = "forged-key-id"
    path.write_text(json.dumps(payload), encoding="utf-8")
    metadata_tampered = load_fee_evidence(
        path,
        now_ms=1_100,
        max_age_ms=500,
    )
    assert metadata_tampered.loaded is False
    assert metadata_tampered.reason == "fee_evidence_integrity_key_policy_mismatch"


def test_v3_fee_evidence_binds_the_configured_account_identity(tmp_path, monkeypatch) -> None:
    path = tmp_path / "account-fees.json"
    payload = _payload(observed_at_ms=1_000)
    payload["schema_version"] = FEE_EVIDENCE_SCHEMA_VERSION
    payload["integrity"] = {
        "algorithm": "hmac-sha256",
        "key_id": TRUSTED_FEE_EVIDENCE_KEY_ID,
        "signature": "",
    }
    monkeypatch.setenv(TRUSTED_FEE_EVIDENCE_HMAC_ENV, "secret")
    path.write_text(
        json.dumps(sign_fee_evidence_payload(payload, "secret")), encoding="utf-8"
    )

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.identity_matches(
        {"cheap": "a" * 64, "rich": "b" * 64}, "cheap", "rich"
    )
    assert not evidence.identity_matches(
        {"cheap": "a" * 64, "rich": "c" * 64}, "cheap", "rich"
    )


def test_v3_fee_evidence_rejects_caller_selected_integrity_secret(tmp_path, monkeypatch) -> None:
    path = tmp_path / "account-fees.json"
    payload = _payload(observed_at_ms=1_000)
    payload["schema_version"] = FEE_EVIDENCE_SCHEMA_VERSION
    payload["integrity"] = {
        "algorithm": "hmac-sha256",
        "key_id": TRUSTED_FEE_EVIDENCE_KEY_ID,
        "signature": "",
    }
    monkeypatch.setenv(TRUSTED_FEE_EVIDENCE_HMAC_ENV, "secret")
    path.write_text(
        json.dumps(sign_fee_evidence_payload(payload, "secret")), encoding="utf-8"
    )

    evidence = load_fee_evidence(
        path,
        now_ms=1_100,
        max_age_ms=500,
        integrity_key_env="ATTACKER_CONTROLLED_SECRET",
    )

    assert evidence.loaded is False
    assert evidence.reason == "fee_evidence_integrity_key_policy_mismatch"


def test_verified_maker_rebate_requires_explicit_opt_in() -> None:
    from lightfee.strategy.fee_evidence import FeeEvidenceBook, FeeScheduleEvidence

    evidence = FeeEvidenceBook(
        schedules={
            "cheap": FeeScheduleEvidence(
                venue="cheap",
                taker_fee_bps=1.0,
                maker_fee_bps=-0.2,
                observed_at_ms=1_000,
                source="account_fee_api",
                evidence_ref="rebate-proof",
            )
        },
        reason="",
        document_sha256="test",
        integrity_verified=True,
        integrity_key_id=TRUSTED_FEE_EVIDENCE_KEY_ID,
    )

    _, conservative = effective_fee_maps({"cheap": 1.0}, {"cheap": 0.1}, evidence)
    _, rebate = effective_fee_maps(
        {"cheap": 1.0},
        {"cheap": 0.1},
        evidence,
        allow_verified_maker_rebates=True,
    )

    assert conservative["cheap"] == 0.1
    assert rebate["cheap"] == -0.2


def test_negative_static_maker_fee_never_becomes_a_rebate() -> None:
    _, maker = effective_fee_maps({"cheap": 1.0}, {"cheap": -0.2}, None)

    assert maker["cheap"] == 1.0
