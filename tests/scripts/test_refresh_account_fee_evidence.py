from __future__ import annotations

import pytest

from scripts.refresh_account_fee_evidence import (
    parse_bybit_evidence,
    parse_okx_evidence,
)


NOW_MS = 1_800_000_000_000


def test_bybit_fee_evidence_preserves_api_cost_semantics() -> None:
    result = parse_bybit_evidence(
        {
            "retCode": 0,
            "time": NOW_MS - 100,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "takerFeeRate": "0.00055",
                        "makerFeeRate": "0.0002",
                    }
                ]
            },
        },
        {"retCode": 0, "result": {"userID": "12345", "secret": "ignored"}},
        now_ms=NOW_MS,
    )

    assert result["taker_fee_bps"] == pytest.approx(5.5)
    assert result["maker_fee_bps"] == pytest.approx(2.0)
    assert len(str(result["account_identity_hash"])) == 64
    assert "12345" not in str(result)


def test_okx_fee_evidence_inverts_exchange_charge_sign() -> None:
    result = parse_okx_evidence(
        {
            "code": "0",
            "data": [
                {
                    "instType": "SWAP",
                    "taker": "-0.0005",
                    "maker": "-0.0002",
                    "ts": str(NOW_MS - 100),
                }
            ],
        },
        {"code": "0", "data": [{"uid": "67890"}]},
        now_ms=NOW_MS,
    )

    assert result["taker_fee_bps"] == pytest.approx(5.0)
    assert result["maker_fee_bps"] == pytest.approx(2.0)
    assert len(str(result["account_identity_hash"])) == 64
    assert "67890" not in str(result)


def test_fee_evidence_rejects_future_or_ambiguous_rows() -> None:
    with pytest.raises(ValueError, match="observation timestamp"):
        parse_okx_evidence(
            {
                "code": "0",
                "data": [
                    {
                        "taker": "-0.0005",
                        "maker": "-0.0002",
                        "ts": str(NOW_MS + 10_000),
                    }
                ],
            },
            {"code": "0", "data": [{"uid": "67890"}]},
            now_ms=NOW_MS,
        )

    with pytest.raises(ValueError, match="shape"):
        parse_bybit_evidence(
            {"retCode": 0, "time": NOW_MS, "result": {"list": []}},
            {"retCode": 0, "result": {"userID": "12345"}},
            now_ms=NOW_MS,
        )
