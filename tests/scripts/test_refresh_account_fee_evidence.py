from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lightfee.core.domain import Venue
import scripts.refresh_account_fee_evidence as refresh_module
from scripts.refresh_account_fee_evidence import (
    _aggregate_symbol_rows,
    collect_evidence,
    merge_evidence_rows,
    parse_aster_evidence,
    parse_binance_evidence,
    parse_bitget_evidence,
    parse_bybit_evidence,
    parse_gate_evidence,
    parse_hyperliquid_evidence,
    parse_okx_evidence,
)


NOW_MS = 1_800_000_000_000


class _FakeTransport:
    def __init__(self, venue: Venue, calls: list[dict[str, object]]) -> None:
        self.venue = venue
        self.calls = calls

    def _venue_symbol(self, symbol: str) -> str:
        if self.venue is Venue.GATE:
            return symbol.replace("USDT", "_USDT")
        if self.venue is Venue.OKX:
            return symbol.replace("USDT", "-USDT-SWAP")
        return symbol

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        body: dict[str, object] | None = None,
        private: bool | None = None,
    ) -> dict[str, object]:
        params = dict(params or {})
        self.calls.append(
            {
                "venue": self.venue.value,
                "method": method,
                "path": path,
                "params": params,
                "body": body,
                "private": private,
            }
        )
        symbol = str(params.get("symbol") or "")
        if self.venue is Venue.BINANCE:
            # Simulate an ignored ETH request returning BTC.  The collector
            # must keep BTC but never claim ETH coverage from the wrong row.
            return {
                "symbol": "BTCUSDT",
                "takerCommissionRate": "0.0005",
                "makerCommissionRate": "0.0002",
            }
        if self.venue is Venue.BITGET:
            return {
                "code": "00000",
                "data": {
                    "symbol": symbol,
                    "takerFeeRate": "0.0006",
                    "makerFeeRate": "0.0002",
                },
            }
        if self.venue is Venue.BYBIT:
            return {
                "retCode": 0,
                # The response is received six seconds after the refresh job
                # started; collectors must validate this against receipt time.
                "time": NOW_MS + 6_000,
                "result": {
                    "list": [
                        {
                            "symbol": symbol,
                            "takerFeeRate": "0.00055",
                            "makerFeeRate": "0.0002",
                        }
                    ]
                },
            }
        if self.venue is Venue.GATE:
            return {"taker_fee": "0.0005", "maker_fee": "0.0001"}
        if self.venue is Venue.HYPERLIQUID:
            return {"userCrossRate": "0.00045", "userAddRate": "0.00015"}
        if self.venue is Venue.OKX:
            if path == "/api/v5/account/instruments":
                return {
                    "code": "0",
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "groupId": "2"},
                        {"instId": "ETH-USDT-SWAP", "groupId": "4"},
                    ],
                }
            group_id = str(params["groupId"])
            return {
                "code": "0",
                "data": [
                    {
                        "feeGroup": [
                            {
                                "groupId": group_id,
                                "taker": "-0.0005",
                                "maker": "-0.0002",
                            }
                        ],
                        "ts": str(NOW_MS + 6_000),
                    }
                ],
            }
        raise AssertionError(self.venue)


class _FakeAsterPrivate:
    def __init__(self, calls: list[dict[str, object]]) -> None:
        self.calls = calls

    async def _request(
        self, method: str, path: str, *, params: dict[str, object]
    ) -> dict[str, object]:
        self.calls.append(
            {
                "venue": "aster",
                "method": method,
                "path": path,
                "params": dict(params),
                "private": True,
            }
        )
        return {
            "code": 0,
            "data": {
                "symbol": params["symbol"],
                "takerCommissionRate": "0.0004",
                "makerCommissionRate": "0.0001",
            },
        }


class _FakeAdapter:
    def __init__(
        self,
        venue: Venue,
        calls: list[dict[str, object]],
        shutdowns: list[str],
    ) -> None:
        self._transport = _FakeTransport(venue, calls)
        self._private = _FakeAsterPrivate(calls) if venue is Venue.ASTER else None
        self._credential = SimpleNamespace(account_address="0xabc")
        self._venue = venue
        self._shutdowns = shutdowns

    async def shutdown(self) -> None:
        self._shutdowns.append(self._venue.value)


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
                    "feeGroup": [
                        {
                            "groupId": "2",
                            "taker": "-0.0005",
                            "maker": "-0.0002",
                        }
                    ],
                    "ts": str(NOW_MS - 100),
                }
            ],
        },
        {"code": "0", "data": [{"uid": "67890"}]},
        now_ms=NOW_MS,
        symbol="BTC-USDT-SWAP",
        expected_group_id="2",
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
                        "feeGroup": [
                            {
                                "groupId": "2",
                                "taker": "-0.0005",
                                "maker": "-0.0002",
                            }
                        ],
                        "ts": str(NOW_MS + 10_000),
                    }
                ],
            },
            {"code": "0", "data": [{"uid": "67890"}]},
            now_ms=NOW_MS,
            symbol="BTC-USDT-SWAP",
            expected_group_id="2",
        )


@pytest.mark.parametrize(
    ("parser", "payload", "taker_bps", "maker_bps"),
    [
        (
            parse_binance_evidence,
            {"symbol": "BTCUSDT", "takerCommissionRate": "0.0005", "makerCommissionRate": "0.0002"},
            5.0,
            2.0,
        ),
        (
            parse_aster_evidence,
            {"code": 0, "data": {"symbol": "BTCUSDT", "takerCommissionRate": "0.0004", "makerCommissionRate": "0.0001"}},
            4.0,
            1.0,
        ),
        (
            parse_bitget_evidence,
            {"code": "00000", "data": {"makerFeeRate": "0.0002", "takerFeeRate": "0.0006"}},
            6.0,
            2.0,
        ),
        (
            parse_gate_evidence,
            {"BTC_USDT": {"taker_fee": "0.0005", "maker_fee": "-0.0001"}},
            5.0,
            -1.0,
        ),
        (
            parse_hyperliquid_evidence,
            {"userCrossRate": "0.00045", "userAddRate": "0.00015"},
            4.5,
            1.5,
        ),
    ],
)
def test_all_additional_venue_fee_parsers_preserve_cost_semantics(
    parser, payload, taker_bps, maker_bps
) -> None:
    result = parser(payload, now_ms=NOW_MS)

    assert result["taker_fee_bps"] == pytest.approx(taker_bps)
    assert result["maker_fee_bps"] == pytest.approx(maker_bps)
    assert result["observed_at_ms"] == NOW_MS
    assert result["source"] == "account_fee_api"


def test_partial_refresh_reuses_fresh_last_good_rows_but_never_overwrites_fresh() -> None:
    fresh = {
        "binance": {
            "taker_fee_bps": 5.0,
            "maker_fee_bps": 2.0,
            "observed_at_ms": NOW_MS,
            "source": "account_fee_api",
            "evidence_ref": "fresh",
        }
    }
    previous = {
        "binance": {
            "taker_fee_bps": 7.0,
            "maker_fee_bps": 3.0,
            "observed_at_ms": NOW_MS - 10,
            "source": "account_fee_api",
            "evidence_ref": "old-binance",
        },
        "gate": {
            "taker_fee_bps": 6.0,
            "maker_fee_bps": 2.0,
            "observed_at_ms": NOW_MS - 10,
            "source": "account_fee_api",
            "evidence_ref": "old-gate",
        },
        "okx": {
            "taker_fee_bps": 5.0,
            "maker_fee_bps": 2.0,
            "observed_at_ms": NOW_MS - 10_001,
            "source": "account_fee_api",
            "evidence_ref": "stale-okx",
        },
    }

    merged, reused = merge_evidence_rows(
        fresh,
        previous,
        requested={"binance", "gate", "okx"},
        now_ms=NOW_MS,
        max_age_ms=10_000,
    )

    assert merged["binance"]["evidence_ref"] == "fresh"
    assert merged["gate"]["evidence_ref"] == "old-gate"
    assert "okx" not in merged
    assert reused == ["gate"]

    with pytest.raises(ValueError, match="shape"):
        parse_bybit_evidence(
            {"retCode": 0, "time": NOW_MS, "result": {"list": []}},
            {"retCode": 0, "result": {"userID": "12345"}},
            now_ms=NOW_MS,
        )


def test_partial_refresh_reuses_last_good_per_symbol_before_reaggregating() -> None:
    fresh = {
        "binance": _aggregate_symbol_rows(
            "binance",
            {
                "BTCUSDT": {
                    "taker_fee_bps": 5.0,
                    "maker_fee_bps": 2.0,
                    "observed_at_ms": NOW_MS,
                    "source": "account_fee_api",
                    "evidence_ref": "fresh-btc",
                }
            },
        )
    }
    previous = {
        "binance": _aggregate_symbol_rows(
            "binance",
            {
                "ETHUSDT": {
                    "taker_fee_bps": 6.0,
                    "maker_fee_bps": 3.0,
                    "observed_at_ms": NOW_MS - 10,
                    "source": "account_fee_api",
                    "evidence_ref": "last-good-eth",
                }
            },
        )
    }

    merged, reused = merge_evidence_rows(
        fresh,
        previous,
        requested={"binance"},
        now_ms=NOW_MS,
        max_age_ms=10_000,
    )

    assert merged["binance"]["covered_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert merged["binance"]["taker_fee_bps"] == pytest.approx(6.0)
    assert merged["binance"]["observed_at_ms"] == NOW_MS - 10
    assert reused == ["binance"]


def test_collect_evidence_uses_seven_private_read_paths_and_binds_symbol_coverage(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []
    shutdowns: list[str] = []
    adapters = {
        venue: _FakeAdapter(venue, calls, shutdowns)
        for venue in (
            Venue.ASTER,
            Venue.BINANCE,
            Venue.BITGET,
            Venue.BYBIT,
            Venue.GATE,
            Venue.HYPERLIQUID,
            Venue.OKX,
        )
    }
    config = SimpleNamespace(
        symbols=["BTCUSDT", "ETHUSDT"],
        strategy=SimpleNamespace(
            funding_canary_allowed_venues=[venue.value for venue in adapters]
        ),
    )
    monkeypatch.setattr(refresh_module, "load_config", lambda _: config)
    monkeypatch.setattr(refresh_module, "build_adapter_map", lambda _: adapters)

    rows, failures, requested = asyncio.run(
        collect_evidence(
            "ignored.toml",
            now_ms=NOW_MS,
            clock_ms=lambda: NOW_MS + 6_000,
        )
    )

    assert failures == {}
    assert requested == {venue.value for venue in adapters}
    assert set(rows) == requested
    # Binance deliberately returned BTC for the ETH request; no false ETH
    # coverage may survive response/request symbol validation.
    assert rows["binance"]["covered_symbols"] == ["BTCUSDT"]
    assert rows["okx"]["covered_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert rows["hyperliquid"]["covered_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert all("symbol_schedules" in row for row in rows.values())
    assert set(shutdowns) == requested

    paths = {(call["venue"], call["path"]) for call in calls}
    assert ("aster", "/fapi/v3/commissionRate") in paths
    assert ("binance", "/fapi/v1/commissionRate") in paths
    assert ("bitget", "/api/v2/common/trade-rate") in paths
    assert ("bybit", "/v5/account/fee-rate") in paths
    assert ("gate", "/api/v4/futures/usdt/fee") in paths
    assert ("hyperliquid", "/info") in paths
    assert ("okx", "/api/v5/account/instruments") in paths
    assert ("okx", "/api/v5/account/trade-fee") in paths
    assert all(
        call["private"] is True
        for call in calls
        if call["venue"] != "hyperliquid"
    )
    hyperliquid_call = next(call for call in calls if call["venue"] == "hyperliquid")
    assert hyperliquid_call["body"] == {"type": "userFees", "user": "0xabc"}
    assert hyperliquid_call["private"] is False
    okx_fee_calls = [
        call
        for call in calls
        if call["venue"] == "okx"
        and call["path"] == "/api/v5/account/trade-fee"
    ]
    assert {call["params"]["groupId"] for call in okx_fee_calls} == {"2", "4"}
    assert all("instId" not in call["params"] for call in okx_fee_calls)


def test_refresh_main_resolves_default_output_from_loaded_config_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    config_dir = project / "config"
    runtime_dir = project / "runtime"
    config_dir.mkdir(parents=True)
    runtime_dir.mkdir()
    path = config_dir / "live.toml"
    path.write_text(
        """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"
funding_fee_evidence_path = "runtime/funding-account-fee-evidence.json"
""",
        encoding="utf-8",
    )
    output = runtime_dir / "funding-account-fee-evidence.json"
    output.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "generated_at_ms": NOW_MS - 10,
                "venues": {
                    "binance": {
                        "taker_fee_bps": 5.0,
                        "maker_fee_bps": 2.0,
                        "observed_at_ms": NOW_MS - 10,
                        "source": "account_fee_api",
                        "evidence_ref": "last-good",
                        "covered_symbols": ["BTCUSDT"],
                        "symbol_schedules": {
                            "BTCUSDT": {
                                "taker_fee_bps": 5.0,
                                "maker_fee_bps": 2.0,
                                "observed_at_ms": NOW_MS - 10,
                                "evidence_ref": "last-good",
                            }
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    output.chmod(0o600)
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()

    async def no_fresh_collect(_config_path: str, *, now_ms: int, **_kwargs):
        assert now_ms == NOW_MS
        return {}, {}, {"binance"}

    monkeypatch.chdir(other_cwd)
    monkeypatch.setattr(sys, "argv", ["refresh", "--config", str(path)])
    monkeypatch.setattr(refresh_module.time, "time", lambda: NOW_MS / 1000)
    monkeypatch.setattr(refresh_module, "collect_evidence", no_fresh_collect)

    assert refresh_module.main() == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["venues"]["binance"]["observed_at_ms"] == NOW_MS - 10
    assert payload["venues"]["binance"]["covered_symbols"] == ["BTCUSDT"]
    assert not (other_cwd / "runtime" / "funding-account-fee-evidence.json").exists()


def test_okx_fee_parser_requires_the_instrument_mapped_fee_group() -> None:
    response = {
        "code": "0",
        "data": [
            {
                "feeGroup": [
                    {"groupId": "1", "taker": "-0.0009", "maker": "-0.0004"},
                    {"groupId": "2", "taker": "-0.0005", "maker": "-0.0002"},
                ],
                "taker": "-0.0099",
                "maker": "-0.0099",
                "ts": str(NOW_MS),
            }
        ],
    }

    selected = parse_okx_evidence(
        response,
        now_ms=NOW_MS,
        symbol="BTC-USDT-SWAP",
        expected_group_id="2",
    )
    assert selected["taker_fee_bps"] == pytest.approx(5.0)
    assert selected["maker_fee_bps"] == pytest.approx(2.0)

    with pytest.raises(ValueError, match="ambiguous okx feeGroup"):
        parse_okx_evidence(
            response,
            now_ms=NOW_MS,
            symbol="BTC-USDT-SWAP",
            expected_group_id="7",
        )
