from __future__ import annotations

import json
import os

from lightfee.strategy.fee_evidence import (
    FEE_EVIDENCE_SCHEMA_VERSION,
    LOCAL_FEE_EVIDENCE_SCHEMA_VERSION,
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


def _local_payload(*, observed_at_ms: int) -> dict:
    payload = _payload(observed_at_ms=observed_at_ms)
    payload["schema_version"] = LOCAL_FEE_EVIDENCE_SCHEMA_VERSION
    for row in payload["venues"].values():
        row.pop("account_identity_hash", None)
        row["covered_symbols"] = ["BTCUSDT", "ETHUSDT"]
        row["symbol_schedules"] = {
            symbol: {
                "taker_fee_bps": row["taker_fee_bps"],
                "maker_fee_bps": row["maker_fee_bps"],
                "observed_at_ms": row["observed_at_ms"],
                "evidence_ref": f"{row['evidence_ref']}:{symbol}",
            }
            for symbol in row["covered_symbols"]
        }
    return payload


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


def test_local_v4_fee_evidence_is_authoritative_without_hmac_or_identity(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "account-fees.json"
    payload = _local_payload(observed_at_ms=1_000)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.delenv(TRUSTED_FEE_EVIDENCE_HMAC_ENV, raising=False)

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is True
    assert evidence.integrity_verified is False
    assert evidence.local_file_verified is True
    assert evidence.account_authoritative_for({}, "cheap", "rich") is False
    assert evidence.account_authoritative_for(
        {}, "cheap", "rich", symbol="BTCUSDT"
    ) is True
    assert evidence.account_authoritative_for(
        {}, "cheap", "rich", symbol="SOLUSDT"
    ) is False
    assert all(
        row["account_fee_evidence_authoritative"] is True
        for row in evidence.provenance_for("cheap", "rich", symbol="BTCUSDT")
    )


def test_local_v4_fee_evidence_rejects_group_or_world_readable_file(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    payload = _local_payload(observed_at_ms=1_000)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o640)

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is False
    assert evidence.reason == "fee_evidence_local_file_permissions_unsafe"


def test_local_v4_fee_evidence_rejects_wrong_owner(tmp_path, monkeypatch) -> None:
    path = tmp_path / "account-fees.json"
    path.write_text(json.dumps(_local_payload(observed_at_ms=1_000)), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(os, "geteuid", lambda: path.stat().st_uid + 1)

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is False
    assert evidence.reason == "fee_evidence_local_file_owner_mismatch"


def test_fee_schedule_digest_is_stable_across_refresh_epochs(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    first = _local_payload(observed_at_ms=1_000)
    path.write_text(json.dumps(first), encoding="utf-8")
    path.chmod(0o600)
    first_book = load_fee_evidence(path, now_ms=1_100, max_age_ms=10_000)

    second = _local_payload(observed_at_ms=2_000)
    for row in second["venues"].values():
        row["evidence_ref"] = "new-private-api-observation"
    path.write_text(json.dumps(second), encoding="utf-8")
    path.chmod(0o600)
    second_book = load_fee_evidence(path, now_ms=2_100, max_age_ms=10_000)

    assert first_book.document_sha256 != second_book.document_sha256
    assert first_book.schedule_sha256 == second_book.schedule_sha256


def test_local_v4_rejects_stale_venue_without_poisoning_fresh_venues(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    payload = _local_payload(observed_at_ms=1_000)
    payload["venues"]["rich"]["observed_at_ms"] = 1
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is True
    assert evidence.complete_for("cheap") is True
    assert evidence.complete_for("rich") is False
    assert evidence.account_authoritative_for({}, "cheap", symbol="BTCUSDT") is True


def test_local_v4_rejects_missing_or_ambiguous_symbol_coverage(tmp_path) -> None:
    path = tmp_path / "account-fees.json"
    payload = _payload(observed_at_ms=1_000)
    payload["schema_version"] = LOCAL_FEE_EVIDENCE_SCHEMA_VERSION
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)

    missing = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)
    assert missing.loaded is False
    assert missing.reason == "fee_evidence_symbol_coverage_invalid:cheap"

    for row in payload["venues"].values():
        row["covered_symbols"] = ["ETHUSDT", "BTCUSDT"]
        row["symbol_schedules"] = {
            symbol: {
                "taker_fee_bps": row["taker_fee_bps"],
                "maker_fee_bps": row["maker_fee_bps"],
                "observed_at_ms": row["observed_at_ms"],
                "evidence_ref": f"coverage:{symbol}",
            }
            for symbol in row["covered_symbols"]
        }
    path.write_text(json.dumps(payload), encoding="utf-8")
    unsorted = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)
    assert unsorted.loaded is False
    assert unsorted.reason == "fee_evidence_symbol_coverage_invalid:cheap"


def test_local_v4_rejects_future_child_even_when_aggregate_epoch_is_current(
    tmp_path,
) -> None:
    path = tmp_path / "account-fees.json"
    payload = _local_payload(observed_at_ms=1_000)
    payload["venues"]["cheap"]["symbol_schedules"]["ETHUSDT"][
        "observed_at_ms"
    ] = 2_000
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)

    evidence = load_fee_evidence(path, now_ms=1_100, max_age_ms=500)

    assert evidence.loaded is True
    assert evidence.complete_for("cheap") is True
    assert evidence.complete_for("rich") is True
    assert evidence.account_authoritative_for(
        {}, "cheap", symbol="BTCUSDT"
    ) is True
    assert evidence.account_authoritative_for(
        {}, "cheap", symbol="ETHUSDT"
    ) is False


def test_fee_evidence_rejects_non_regular_symlink_and_oversized_inputs(tmp_path) -> None:
    fifo = tmp_path / "account-fees.fifo"
    os.mkfifo(fifo, 0o600)
    fifo_result = load_fee_evidence(fifo, now_ms=1_100, max_age_ms=500)
    assert fifo_result.reason == "fee_evidence_local_file_not_regular"

    target = tmp_path / "target.json"
    target.write_text(json.dumps(_local_payload(observed_at_ms=1_000)), encoding="utf-8")
    target.chmod(0o600)
    symlink = tmp_path / "account-fees-link.json"
    symlink.symlink_to(target)
    symlink_result = load_fee_evidence(symlink, now_ms=1_100, max_age_ms=500)
    assert symlink_result.reason == "fee_evidence_unreadable"

    oversized = tmp_path / "oversized.json"
    with oversized.open("wb") as handle:
        handle.truncate(1024 * 1024 + 1)
    oversized.chmod(0o600)
    oversized_result = load_fee_evidence(oversized, now_ms=1_100, max_age_ms=500)
    assert oversized_result.reason == "fee_evidence_file_too_large"
