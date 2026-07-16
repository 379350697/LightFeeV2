from __future__ import annotations

import json
import sys

from scripts.validate_fee_evidence import main


def _local_payload() -> dict[str, object]:
    row = {
        "taker_fee_bps": 5.0,
        "maker_fee_bps": 2.0,
        "observed_at_ms": 1_000,
        "source": "account_fee_api",
        "evidence_ref": "binance:worst-case",
        "covered_symbols": ["BTCUSDT"],
        "symbol_schedules": {
            "BTCUSDT": {
                "taker_fee_bps": 5.0,
                "maker_fee_bps": 2.0,
                "observed_at_ms": 1_000,
                "evidence_ref": "binance:BTCUSDT",
            }
        },
    }
    return {"schema_version": 4, "venues": {"binance": row}}


def test_required_symbol_controls_validator_exit_code(tmp_path, monkeypatch) -> None:
    path = tmp_path / "funding-fees.json"
    path.write_text(json.dumps(_local_payload()), encoding="utf-8")
    path.chmod(0o600)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_fee_evidence.py",
            str(path),
            "--now-ms",
            "1100",
            "--max-age-ms",
            "500",
            "--require-venue",
            "binance",
            "--require-symbol",
            "SOLUSDT",
        ],
    )
    assert main() == 1

    sys.argv[-1] = "BTCUSDT"
    assert main() == 0


def test_funding_validator_defaults_to_five_day_expiry(tmp_path, monkeypatch) -> None:
    path = tmp_path / "funding-fees.json"
    path.write_text(json.dumps(_local_payload()), encoding="utf-8")
    path.chmod(0o600)
    two_days_later_ms = 1_000 + 2 * 24 * 60 * 60 * 1000
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_fee_evidence.py",
            str(path),
            "--now-ms",
            str(two_days_later_ms),
            "--require-venue",
            "binance",
            "--require-symbol",
            "BTCUSDT",
        ],
    )

    assert main() == 0
