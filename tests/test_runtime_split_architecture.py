"""Runtime split architecture guardrails."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.passive_close import PassiveCloseExecutor
from lightfee.engine.recovery_startup_runtime import RecoveryStartupRuntime
from lightfee.engine.residual_repair_runtime import ResidualRepairRuntime
from scripts.diagnose_live import _fetch_venue_open_orders


def _function_source(source: str, signature: str, next_signature: str) -> str:
    start = source.index(signature)
    end = source.find(next_signature, start + len(signature))
    if end < 0:
        end = len(source)
    return source[start:end]


def test_runtime_split_checker_fails_on_delegate_runtime_module_dependency():
    result = subprocess.run(
        [sys.executable, "scripts/check_runtime_split_architecture.py"],
        check=False,
        text=True,
        capture_output=True,
    )

    assert "delegate_runtime_module_dependency" in result.stdout
    assert "PASS delegate_runtime_module_dependency: none" in result.stdout
    assert "contract_truth_endpoint_registry_bypass" in result.stdout
    assert "PASS contract_truth_endpoint_registry_bypass: none" in result.stdout
    assert result.returncode == 0


def test_runtime_split_checker_catches_bitget_adapter_direct_truth_literals(monkeypatch):
    import scripts.check_runtime_split_architecture as guard

    bitget_path = guard.ROOT / "lightfee" / "venues" / "bitget.py"
    source = """
class BitgetAdapter:
    async def illegal_direct_truth(self):
        return "/api/v3/position/current-position"

    async def detect_position_hedge_mode(self):
        return "/api/v2/mix/account/account"
"""

    monkeypatch.setattr(guard, "tracked_python_files", lambda: [bitget_path])
    monkeypatch.setattr(guard, "TRUTH_ENDPOINT_GUARD_FILES", [])
    monkeypatch.setattr(guard, "read", lambda path: source)

    hits = guard.contract_truth_endpoint_bypass_hits()

    assert hits == ["lightfee/venues/bitget.py:4:/api/v3/position/current-position"]


def test_runtime_split_checker_catches_hyperliquid_info_body_variants(monkeypatch):
    import scripts.check_runtime_split_architecture as guard

    diagnose_path = guard.ROOT / "scripts" / "diagnose_live.py"
    source = """
async def illegal_direct_info_body(account):
    await client.post('/info', json={'type': 'clearinghouseState', 'user': account})
    return dict(type='userAbstraction', user=account)
"""

    monkeypatch.setattr(guard, "tracked_python_files", lambda: [])
    monkeypatch.setattr(guard, "TRUTH_ENDPOINT_GUARD_FILES", [diagnose_path])
    monkeypatch.setattr(guard, "read", lambda path: source)

    hits = guard.contract_truth_endpoint_bypass_hits()

    assert hits == [
        "scripts/diagnose_live.py:3:type=clearinghouseState",
        "scripts/diagnose_live.py:4:type=userAbstraction",
    ]


def test_passive_amend_cannot_reuse_generic_order_path():
    source = Path("lightfee/venues/transport.py").read_text(encoding="utf-8")
    amend_source = _function_source(
        source,
        "    async def amend_passive_order",
        "    async def _cancel_okx_passive_order_once",
    )

    assert "spec.order_path" not in amend_source
    assert '"PUT", spec.order_path' not in amend_source
    assert "VenueOperation.AMEND_ORDER" in amend_source


def test_critical_private_order_operations_use_operation_contract_registry():
    source = Path("lightfee/venues/transport.py").read_text(encoding="utf-8")
    operations = {
        "place_order": (
            "    async def place_order",
            "    async def fetch_order_fill_reconciliation",
            "VenueOperation.CREATE_ORDER",
        ),
        "submit_passive_order": (
            "    async def submit_passive_order",
            "    def _parse_passive_order_ack",
            "VenueOperation.CREATE_ORDER",
        ),
        "query_passive_order_progress": (
            "    async def query_passive_order_progress",
            "    async def amend_passive_order",
            "VenueOperation.ORDER_STATUS",
        ),
        "cancel_passive_order": (
            "    async def cancel_passive_order",
            "    def _parse_cancel_ack",
            "VenueOperation.CANCEL_ORDER",
        ),
    }

    for name, (signature, next_signature, operation) in operations.items():
        body = _function_source(source, signature, next_signature)
        assert "spec.order_path" not in body, name
        assert operation in body, name


def test_operation_contract_registry_does_not_fallback_to_generic_order_path():
    source = Path("lightfee/venues/specs.py").read_text(encoding="utf-8")
    body = _function_source(
        source,
        "def get_operation_contract(",
        "def ",
    )

    assert "spec.order_path" not in body


def test_order_truth_paths_are_derived_from_venue_operation_contracts():
    wrapper_source = Path("lightfee/engine/order_submit_uncertainty.py").read_text(
        encoding="utf-8"
    )
    ledger_source = Path("lightfee/engine/order_truth_ledger.py").read_text(
        encoding="utf-8"
    )
    wrapper_body = _function_source(
        wrapper_source,
        "def order_truth_probe_paths",
        "def is_order_truth_gap",
    )
    body = _function_source(ledger_source, "    def probe_paths", "    def is_order_truth_gap")

    assert "ORDER_TRUTH_LEDGER.probe_paths" in wrapper_body
    assert "get_operation_contract" in body
    assert "VenueOperation.ORDER_STATUS" in body
    assert "VenueOperation.OPEN_ORDERS" in body
    assert "VenueOperation.POSITION" in body
    for forbidden in (
        "/v5/order/realtime",
        "/api/v5/trade/order",
        "/fapi/v3/order",
        "/api/v2/mix/order/detail",
        "/info orderStatus",
    ):
        assert forbidden not in body


def test_diagnose_live_does_not_hardcode_hyperliquid_info_type_bodies():
    source = Path("scripts/diagnose_live.py").read_text(encoding="utf-8")
    body = _function_source(
        source,
        "async def _fetch_hyperliquid_balance_view",
        "def _probe_venue_symbol",
    )

    for forbidden in (
        '"type": "clearinghouseState"',
        '"type": "userAbstraction"',
        '"type": "spotClearinghouseState"',
    ):
        assert forbidden not in body
    assert "request_venue_operation" in body
    assert "VenueOperation.POSITION" in body


def test_runtime_context_exposes_runtime_split_semantic_services():
    context_source = Path("lightfee/engine/runtime_context.py").read_text(encoding="utf-8")
    runtime_source = Path("lightfee/engine/runtime.py").read_text(encoding="utf-8")

    for service in (
        "venue_contracts",
        "order_truth",
        "lifecycle_closure",
        "quote_truth",
        "catalog_support",
        "exchange_truth",
    ):
        assert f"def {service}(self)" in context_source
        assert f"def {service}(self)" in runtime_source


def test_recovery_delegate_consumes_runtime_context_exchange_truth_service():
    source = Path("lightfee/engine/recovery_startup_runtime.py").read_text(encoding="utf-8")
    body = _function_source(
        source,
        "    async def _fetch_recovery_ledger_account_open_orders",
        "    async def _collect_recovery_ledger_exchange_truth",
    )

    assert 'getattr(self.ctx, "exchange_truth", None)' in body
    assert "request_venue_operation" in body


def test_recovery_startup_account_open_orders_use_venue_operation_contract():
    from lightfee.engine.exchange_truth import request_venue_operation

    calls: list[tuple[str, str, dict]] = []

    class Transport:
        async def _request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            return []

    ctx = SimpleNamespace(
        _recovery_ledger_order_rows=lambda rows: rows,
        venue_contracts=SimpleNamespace(request_venue_operation=request_venue_operation),
    )
    runtime = RecoveryStartupRuntime(ctx)
    adapter = SimpleNamespace(_transport=Transport())

    rows, endpoint = asyncio.run(
        runtime._fetch_recovery_ledger_account_open_orders(Venue.ASTER, adapter)
    )

    assert rows == []
    assert endpoint == "GET /fapi/v3/openOrders"
    assert calls == [("GET", "/fapi/v3/openOrders", {"params": {}, "private": True})]


def test_diagnose_symbol_open_orders_use_venue_operation_contract():
    calls: list[tuple[str, str, dict]] = []

    class Transport:
        def _venue_symbol(self, symbol: str) -> str:
            return symbol

        async def _request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            return []

    adapter = SimpleNamespace(venue="aster", _transport=Transport())

    orders, succeeded, failed, evidence = asyncio.run(
        _fetch_venue_open_orders(adapter, ["HOMEUSDT"])
    )

    assert orders == {"HOMEUSDT": []}
    assert succeeded == {"HOMEUSDT"}
    assert failed == set()
    assert evidence["HOMEUSDT"]["venue_symbol"] == "HOMEUSDT"
    assert calls == [
        (
            "GET",
            "/fapi/v3/openOrders",
            {"params": {"symbol": "HOMEUSDT"}, "private": True},
        )
    ]


def test_passive_and_residual_open_order_truth_share_contract_helper():
    calls: list[tuple[str, str, dict]] = []

    class Transport:
        def _venue_symbol(self, symbol: str) -> str:
            return symbol

        async def _request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            return []

    adapter = SimpleNamespace(_transport=Transport())

    passive = PassiveCloseExecutor.__new__(PassiveCloseExecutor)
    residual = ResidualRepairRuntime(SimpleNamespace())

    passive_result = asyncio.run(
        passive._probe_venue_open_orders_flat(Venue.ASTER, "HOMEUSDT", {Venue.ASTER: adapter})
    )
    residual_result = asyncio.run(
        residual._fetch_residual_repair_open_orders(adapter, Venue.ASTER, "HOMEUSDT")
    )

    assert passive_result == (True, None)
    assert residual_result == []
    assert calls == [
        (
            "GET",
            "/fapi/v3/openOrders",
            {"params": {"symbol": "HOMEUSDT"}, "private": True},
        ),
        (
            "GET",
            "/fapi/v3/openOrders",
            {"params": {"symbol": "HOMEUSDT"}, "private": True},
        ),
    ]


def test_hyperliquid_contract_request_uses_account_address_not_agent_wallet():
    from lightfee.engine.exchange_truth import build_venue_operation_request
    from lightfee.venues.specs import VenueOperation

    request = build_venue_operation_request(
        Venue.HYPERLIQUID,
        VenueOperation.OPEN_ORDERS,
        account_address="0xACCOUNT",
        agent_wallet_address="0xAGENT",
    )

    assert request.method == "POST"
    assert request.path == "/info"
    assert request.body == {"type": "openOrders", "user": "0xACCOUNT"}
    assert "0xAGENT" not in repr(request)


def test_bitget_runtime_contract_request_is_family_locked_for_classic_and_uta():
    from lightfee.engine.exchange_truth import build_venue_operation_request
    from lightfee.venues.specs import BitgetContractFamily, VenueOperation

    classic_position = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.POSITION,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
    )
    uta_position = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.POSITION,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.UTA_V3,
    )
    classic_open = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.OPEN_ORDERS,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
    )
    uta_open = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.OPEN_ORDERS,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.UTA_V3,
    )
    classic_all_positions = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.ALL_POSITIONS,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
    )
    uta_all_positions = build_venue_operation_request(
        Venue.BITGET,
        VenueOperation.ALL_POSITIONS,
        symbol="HOMEUSDT",
        resolved_account_family=BitgetContractFamily.UTA_V3,
    )

    assert classic_position.path == "/api/v2/mix/position/single-position"
    assert classic_position.params == {
        "productType": "USDT-FUTURES",
        "marginCoin": "USDT",
        "symbol": "HOMEUSDT",
    }
    assert classic_open.path == "/api/v2/mix/order/orders-pending"
    assert classic_open.params == {
        "productType": "USDT-FUTURES",
        "marginCoin": "USDT",
        "symbol": "HOMEUSDT",
    }
    assert classic_all_positions.path == "/api/v2/mix/position/all-position"
    assert classic_all_positions.params == {
        "productType": "USDT-FUTURES",
        "marginCoin": "USDT",
        "symbol": "HOMEUSDT",
    }

    assert uta_position.path == "/api/v3/position/current-position"
    assert uta_position.params == {
        "category": "USDT-FUTURES",
        "symbol": "HOMEUSDT",
    }
    assert uta_open.path == "/api/v3/trade/unfilled-orders"
    assert uta_open.params == {
        "category": "USDT-FUTURES",
        "symbol": "HOMEUSDT",
    }
    assert uta_all_positions.path == "/api/v3/position/current-position"
    assert uta_all_positions.params == {
        "category": "USDT-FUTURES",
        "symbol": "HOMEUSDT",
    }


def test_bitget_runtime_operation_requests_use_same_transport_family_lock():
    from lightfee.engine.exchange_truth import request_venue_operation
    from lightfee.venues.specs import BitgetContractFamily, VenueOperation

    calls: list[tuple[str, str, dict]] = []
    resolve_calls = 0

    class Transport:
        async def _bitget_resolve_contract_family(self):
            nonlocal resolve_calls
            resolve_calls += 1
            return BitgetContractFamily.UTA_V3

        async def _request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {"code": "00000", "data": []}

    transport = Transport()

    asyncio.run(
        request_venue_operation(
            transport,
            Venue.BITGET,
            VenueOperation.OPEN_ORDERS,
            symbol="HOMEUSDT",
        )
    )
    asyncio.run(
        request_venue_operation(
            transport,
            Venue.BITGET,
            VenueOperation.POSITION,
            symbol="HOMEUSDT",
        )
    )

    assert resolve_calls == 2
    assert calls == [
        (
            "GET",
            "/api/v3/trade/unfilled-orders",
            {"params": {"category": "USDT-FUTURES", "symbol": "HOMEUSDT"}, "private": True},
        ),
        (
            "GET",
            "/api/v3/position/current-position",
            {"params": {"category": "USDT-FUTURES", "symbol": "HOMEUSDT"}, "private": True},
        ),
    ]
    assert all("/api/v2/mix/" not in path for _method, path, _kwargs in calls)


def test_bitget_runtime_operation_without_family_resolver_fails_closed():
    from lightfee.engine.exchange_truth import request_venue_operation
    from lightfee.venues.specs import VenueOperation
    from lightfee.venues.transport import TransportError, TransportErrorCategory

    class Transport:
        async def _request(self, method, path, **kwargs):
            raise AssertionError(f"must not request Bitget truth without family resolver: {path}")

    with pytest.raises(TransportError) as exc:
        asyncio.run(
            request_venue_operation(
                Transport(),
                Venue.BITGET,
                VenueOperation.OPEN_ORDERS,
                symbol="HOMEUSDT",
            )
        )

    assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
    assert "family resolver" in str(exc.value)


def test_duplicate_reconcile_reason_normalization_is_stable():
    from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER

    payload = ORDER_TRUTH_LEDGER.build_duplicate_reconcile_result_payload(
        result=SimpleNamespace(
            classification="none",
            decision="backoff_recheck",
            target_qty=1.0,
            reconciled_qty=0.0,
            live_qty=0.0,
            remaining_qty=1.0,
            retry_qty=1.0,
        ),
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        client_order_id="cid-1",
        reason="bybit retCode=110072 OrderLinkedID is duplicate",
    )

    assert payload["reason"] == "bybit retCode=110072 OrderLinkedID is duplicate"
    assert payload["uncertain_subtype"] == "duplicate_client_id"
    assert payload["truth_required_by"] == "duplicate_client_id"


def test_entry_gate_uses_shared_bootstrap_wall_clock():
    from lightfee.engine import bootstrap
    from lightfee.engine import entry_gate_runtime

    assert entry_gate_runtime.wall_clock_now_ms is bootstrap.wall_clock_now_ms
