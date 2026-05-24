# V2 Exchange and Local L2 Root Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Root-fix the May 13 live issues by bringing V2 exchange execution, local L2 readiness, and recovery parsing to V1-equivalent or better behavior without weakening safety gates.

**Architecture:** Keep exchange-specific behavior behind `lightfee/venues/*` and keep engine orchestration in `lightfee/engine/*`. Copy V1 semantics where they are production-proven, but improve V2 by making ACK-vs-fill explicit, using fixture-driven parser contracts, and separating local L2 candidate readiness from final execution readiness. Do not bypass local L2 safety; make the system select fewer, ready candidates earlier.

**Tech Stack:** Python 3.12, asyncio, `httpx`, `pytest`, `pytest-asyncio`, LightFee V2 dataclasses, V1 Rust references from `/media/wl/新加卷/codex/LightFee`.

---

## Scope and Non-Goals

This plan covers only the V2 implementation gaps and the V1/V2 shared safety semantics implicated by the latest cloud logs.

Do not make live-network calls in unit tests. Use fixtures, `httpx.MockTransport`, fake adapters, and replayed journal snippets.

Do not relax these safeguards:

- Do not open if required local L2 books are not ready.
- Do not rotate client order IDs after an uncertain submit.
- Do not treat an order ACK as a fill.
- Do not create a hedge leg until the maker leg has an observed fill or a reconciled fill.
- Do not hide exchange rejects as generic uncertain errors when the exchange returned a definitive error code.

## Official Exchange Sources Checked

- Bybit V5 `POST /v5/order/create`: requires `category`, `symbol`, `side=Buy/Sell`, `orderType`, `qty`; `positionIdx` is required in hedge mode; `orderLinkId` is unique and <=36 chars; order placement response is an ACK and must be confirmed through WebSocket/order status. Source: https://bybit-exchange.github.io/docs/v5/order/create-order
- Bybit V5 `GET /v5/order/realtime`: supports querying active/recent orders and returns `orderId`, `orderLinkId`, `orderStatus`, `cumExecQty`, `avgPrice`. Source: https://bybit-exchange.github.io/docs/v5/order/open-order
- Bitget Classic futures `POST /api/v2/mix/order/place-order`: requires `productType`, `marginMode`, `marginCoin`, `size`, `side`, `orderType`, `force` for limit orders, and `clientOid`; `post_only` is the post-only force. Source: https://www.bitget.com/api-doc/contract/trade/Place-Order
- Bitget UTA `POST /api/v3/trade/place-order`: requires `category`, `symbol`, `qty`, `side`, `orderType`; futures hedge mode uses `posSide`; limit orders use `timeInForce`, including `post_only`; `clientOid` is recommended, especially for ambiguous outcomes. Source: https://www.bitget.com/api-doc/uta/trade/Place-Order
- Bitget UTA orderbook `GET /api/v3/market/orderbook`: requires `category=USDT-FUTURES`, `symbol`, and optional `limit`, maximum `200`. Source: https://www.bitget.com/api-doc/uta/public/OrderBook
- Bitget best-practices guide: a successful order response means the exchange received the request and assigned an order ID; users still need order status or WS updates to confirm fill/cancel states. Source: https://www.bitget.com/api-doc/uta/best-practices
- Aster perpetuals API: base REST URL is `https://fapi.asterdex.com`; WS base is `wss://fstream.asterdex.com`; local order book bootstraps from `https://fapi.asterdex.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`. Source: https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation

## Root-Cause Classification

| Issue | Classification | Root-Fix Direction |
| --- | --- | --- |
| V2 maker entry calls `place_order()` and expects immediate fill | V2-only implementation defect | Copy V1 passive maker ACK/progress lifecycle; make ACK and fill different result types. |
| Bybit request body/response handling | V2-only implementation defect | Copy V1 Bybit payload fields, position mode handling, orderLinkId validation, and retCode checks. |
| Bitget order 404/malformed submit | V2-only implementation defect | Copy V1 Classic/UTA profile-aware builder and endpoint selection; improve with fixtures for both account profiles. |
| Aster DNS errors | V2-only endpoint drift | Change V2 spec to official/V1 host `fapi.asterdex.com`; keep configurable override. |
| Bitget L2 400 `400172` | V2 guard/parsing gap, not endpoint mismatch | Keep official `/api/v3/market/orderbook`; add V1 metadata/symbol support guard before snapshot fetch. |
| Position fetch `float("")` / shape errors | V2-only parser robustness gap | Replace generic parser with venue-specific parsers and safe numeric helpers. |
| Many `entry_blocked_local_l2_not_ready` events | Shared safety gate, V2 missing upstream readiness selection | Preserve final gate; add V1 prewarm/primary/dual-ready selection before dispatch. |
| Duplicate client order ID after uncertain submit | Shared intentional safety behavior | Keep dedup; reduce root uncertain submits and add clearer observability. |

## V1 References to Copy or Improve

- Local L2 selection blocker: `/media/wl/新加卷/codex/LightFee/src/execution_core/market_data.rs:1681`
- Local L2 ranked candidate filter and dual-ready check: `/media/wl/新加卷/codex/LightFee/src/execution_core/market_data.rs:3188`
- Final local L2 gate: `/media/wl/新加卷/codex/LightFee/src/execution_core/final_gate.rs:4`
- Venue-specific local L2 grace: `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs:3960`
- V1 default local L2 quiet grace: `/media/wl/新加卷/codex/LightFee/src/runtime_state/config.rs:2839`
- V1 venue L2 readiness rules: `/media/wl/新加卷/codex/LightFee/src/market_gateway/venue_rules.rs:90`
- Passive incremental entry submit: `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs:3173`
- Bybit order builders and retCode handling: `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs:1298`
- Bitget profile-aware order builder: `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs:4605`
- Bitget execution liquidity metadata guard: `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs:5464`
- Bitget position parsing: `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs:2777`
- Bybit position parsing: `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs:2566`
- OKX position parsing: `/media/wl/新加卷/codex/LightFee/src/live/okx.rs:3529`
- Aster default endpoint: `/media/wl/新加卷/codex/LightFee/src/live/aster.rs:584`

## V2 Files Expected to Change During Implementation

- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/marketdata/local_l2_venues.py`
- Modify: `tests/test_venues_contract.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_entry_sync.py`
- Modify: `tests/test_runtime_entry_flow.py`
- Modify: `tests/test_entry_local_l2.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`
- Create: `tests/fixtures/venues/bybit/place_order_ack_only.json`
- Create: `tests/fixtures/venues/bybit/place_order_reject_retcode.json`
- Create: `tests/fixtures/venues/bitget/classic_place_order_ack_only.json`
- Create: `tests/fixtures/venues/bitget/uta_place_order_ack_only.json`
- Create: `tests/fixtures/venues/bitget/orderbook_uta_success.json`
- Create: `tests/fixtures/venues/bitget/orderbook_unsupported_symbol.json`
- Create: `tests/fixtures/venues/aster/depth_success.json`

Before editing any function, class, or method, run GitNexus impact analysis for the target symbol, per project rules. Example: `gitnexus_impact({target: "VenueTransport.place_order", direction: "upstream", repo: "LightFeeV2"})`.

---

### Task 1: Lock Exchange ACK Semantics and Error Envelopes

**Files:**
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_venues_contract.py`
- Create: `tests/fixtures/venues/bybit/place_order_ack_only.json`
- Create: `tests/fixtures/venues/bybit/place_order_reject_retcode.json`
- Create: `tests/fixtures/venues/bitget/classic_place_order_ack_only.json`
- Create: `tests/fixtures/venues/bitget/uta_place_order_ack_only.json`

**Goal of this task:** make tests state the exchange contract before implementation: order ACK is accepted/resting/uncertain, not an immediate fill.

- [ ] **Step 1: Write fixtures**

Create `tests/fixtures/venues/bybit/place_order_ack_only.json`:

```json
{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "orderId": "1321003749386327552",
    "orderLinkId": "lfv2-entry-maker-001"
  },
  "retExtInfo": {},
  "time": 1672211918471
}
```

Create `tests/fixtures/venues/bybit/place_order_reject_retcode.json`:

```json
{
  "retCode": 110003,
  "retMsg": "Order price exceeds allowable range",
  "result": {},
  "retExtInfo": {},
  "time": 1672211918471
}
```

Create `tests/fixtures/venues/bitget/classic_place_order_ack_only.json`:

```json
{
  "code": "00000",
  "msg": "success",
  "requestTime": 1695806875837,
  "data": {
    "clientOid": "lfv2-entry-maker-001",
    "orderId": "121211212122"
  }
}
```

Create `tests/fixtures/venues/bitget/uta_place_order_ack_only.json`:

```json
{
  "code": "00000",
  "msg": "success",
  "requestTime": 1695806875837,
  "data": {
    "clientOid": "lfv2-entry-maker-001",
    "orderId": "121211212122"
  }
}
```

- [ ] **Step 2: Add tests for ACK-only responses**

Add tests that prove `place_order()` does not turn ACK-only responses into `OrderFill`, and `submit_passive_order()` does return `PassiveOrderAck`.

```python
@pytest.mark.asyncio
async def test_bybit_ack_only_place_order_is_uncertain_but_passive_submit_is_ack():
    transport = _make_live_transport_with_mock_response(Venue.BYBIT, "bybit/place_order_ack_only.json")
    req = OrderRequest(
        venue=Venue.BYBIT,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=0.001,
        price=50000.0,
        post_only=True,
        client_order_id="lfv2-entry-maker-001",
    )
    with pytest.raises(OrderSubmitError) as exc:
        await transport.place_order(req)
    assert exc.value.failure_class == SubmitFailureClass.UNCERTAIN

    ack = await transport.submit_passive_order(req)
    assert ack.order_id == "1321003749386327552"
    assert ack.client_order_id == "lfv2-entry-maker-001"
```

```python
@pytest.mark.asyncio
async def test_bitget_ack_only_place_order_is_uncertain_but_passive_submit_is_ack():
    transport = _make_live_transport_with_mock_response(Venue.BITGET, "bitget/classic_place_order_ack_only.json")
    req = OrderRequest(
        venue=Venue.BITGET,
        symbol="BTCUSDT",
        side=Side.SELL,
        quantity=0.001,
        price=50000.0,
        post_only=True,
        client_order_id="lfv2-entry-maker-001",
    )
    with pytest.raises(OrderSubmitError) as exc:
        await transport.place_order(req)
    assert exc.value.failure_class == SubmitFailureClass.UNCERTAIN

    ack = await transport.submit_passive_order(req)
    assert ack.order_id == "121211212122"
    assert ack.client_order_id == "lfv2-entry-maker-001"
```

- [ ] **Step 3: Add tests for exchange error envelopes**

```python
@pytest.mark.asyncio
async def test_bybit_retcode_reject_maps_to_rejected():
    transport = _make_live_transport_with_mock_response(Venue.BYBIT, "bybit/place_order_reject_retcode.json")
    req = OrderRequest(venue=Venue.BYBIT, symbol="BTCUSDT", side=Side.BUY, quantity=0.001)

    with pytest.raises(OrderSubmitError) as exc:
        await transport.place_order(req)

    assert exc.value.failure_class == SubmitFailureClass.REJECTED
    assert "110003" in str(exc.value)
```

- [ ] **Step 4: Run failing tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py -k "ack_only or retcode_reject" -q
```

Expected before implementation: tests fail because current V2 still treats ACK parsing and exchange envelope validation inconsistently.

---

### Task 2: Introduce Venue-Specific Response Guards and Safe Numeric Parsing

**Files:**
- Modify: `lightfee/venues/transport.py`
- Test: `tests/test_venues_transport.py`

**Goal of this task:** replace generic shape assumptions with explicit venue success checks and safe numeric conversion.

- [ ] **Step 1: Add failing tests for empty strings and list-shaped data**

```python
def test_safe_float_empty_string_returns_default():
    assert _safe_float("", default=0.0) == 0.0
    assert _safe_float(None, default=0.0) == 0.0
    assert _safe_float("1.25", default=0.0) == 1.25
```

```python
def test_bybit_position_result_list_shape_is_supported():
    raw = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "side": "Buy", "size": "", "avgPrice": ""}
            ]
        }
    }
    pos = _parse_bybit_position(raw, "BTCUSDT", now_ms=1000)
    assert pos.quantity == 0.0
    assert pos.entry_price == 0.0
```

- [ ] **Step 2: Implement safe helpers**

Add helpers near transport parsing utilities:

```python
def _safe_float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, str) and value.strip() == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
```

```python
def _require_bybit_success(raw: dict[str, Any], context: str) -> None:
    if int(raw.get("retCode", 0) or 0) != 0:
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bybit retCode={raw.get('retCode')} retMsg={raw.get('retMsg', '')}",
        )
```

```python
def _require_bitget_success(raw: dict[str, Any], context: str) -> None:
    code = str(raw.get("code", "00000"))
    if code not in ("00000", "0"):
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bitget code={code} msg={raw.get('msg', '')}",
        )
```

- [ ] **Step 3: Wire guards into order, passive-order, position, and L2 parsing**

Apply the guards before parsing venue payloads:

```python
if spec.venue_id == Venue.BYBIT:
    _require_bybit_success(raw, "bybit order failed")
elif spec.venue_id == Venue.BITGET:
    _require_bitget_success(raw, "bitget order failed")
```

Use `_safe_float` instead of direct `float(...)` for optional or exchange-returned fields in `_parse_position`, `_parse_order_fill`, `_parse_passive_order_ack`, and account risk parsing.

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py -k "safe_float or bybit_position or retcode or bitget" -q
```

Expected after implementation: tests pass without changing engine behavior.

---

### Task 3: Fix Bybit Order Builders to Match Official Docs and V1

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_venues_contract.py`

**Goal of this task:** make V2 Bybit submit the same fields V1 submits, with official field names.

- [ ] **Step 1: Write request-body tests**

```python
@pytest.mark.asyncio
async def test_bybit_passive_order_body_uses_qty_order_link_id_and_position_idx():
    seen_body = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_body.update(json.loads(request.content.decode()))
        return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

    transport = _make_live_transport(Venue.BYBIT, handler)
    req = OrderRequest(
        venue=Venue.BYBIT,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=0.001,
        price=50000.0,
        post_only=True,
        client_order_id="lfv2-entry-maker-001",
    )

    await transport.submit_passive_order(req)

    assert seen_body["category"] == "linear"
    assert seen_body["symbol"] == "BTCUSDT"
    assert seen_body["side"] == "Buy"
    assert seen_body["orderType"] == "Limit"
    assert seen_body["timeInForce"] == "PostOnly"
    assert seen_body["qty"] == "0.001"
    assert seen_body["orderLinkId"] == "lfv2-entry-maker-001"
    assert seen_body["positionIdx"] in (0, 1, 2)
    assert "quantity" not in seen_body
    assert "newClientOrderId" not in seen_body
```

- [ ] **Step 2: Implement Bybit body builders**

Create private builder functions:

```python
def _bybit_side(side: Side) -> str:
    return "Buy" if side == Side.BUY else "Sell"
```

```python
def _bybit_position_idx(request: OrderRequest, *, hedge_mode: bool) -> int:
    if not hedge_mode:
        return 0
    return 1 if request.side == Side.BUY else 2
```

```python
def _build_bybit_order_body(
    request: OrderRequest,
    venue_sym: str,
    *,
    passive: bool,
    hedge_mode: bool,
) -> dict[str, Any]:
    body = {
        "category": "linear",
        "symbol": venue_sym,
        "side": _bybit_side(request.side),
        "orderType": "Limit" if passive or request.price is not None else "Market",
        "qty": _format_quantity(request.quantity),
        "reduceOnly": bool(request.reduce_only),
        "positionIdx": _bybit_position_idx(request, hedge_mode=hedge_mode),
    }
    if request.client_order_id:
        body["orderLinkId"] = request.client_order_id
    if request.price is not None and request.price > 0:
        body["price"] = _format_price(request.price)
    if passive:
        body["timeInForce"] = "PostOnly"
    return body
```

- [ ] **Step 3: Replace generic Bybit branches**

Use `_build_bybit_order_body(... passive=False)` in `place_order()` and `_build_bybit_order_body(... passive=True)` in `submit_passive_order()`.

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py -k "bybit" -q
```

Expected: Bybit request body tests pass, ACK-only `place_order()` remains uncertain, passive submit returns ACK.

---

### Task 4: Fix Bitget Classic/UTA Order Builders and Profile Selection

**Files:**
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_venues_contract.py`

**Goal of this task:** eliminate Bitget 404/malformed order submissions by copying V1's account-profile-aware path/body selection.

- [ ] **Step 1: Write Classic request-body test**

```python
@pytest.mark.asyncio
async def test_bitget_classic_passive_order_body_uses_v2_mix_contract_fields():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json=_fixture("bitget/classic_place_order_ack_only.json"))

    transport = _make_live_transport(Venue.BITGET, handler, bitget_profile="classic")
    req = OrderRequest(
        venue=Venue.BITGET,
        symbol="BTCUSDT",
        side=Side.SELL,
        quantity=0.001,
        price=50000.0,
        post_only=True,
        client_order_id="lfv2-entry-maker-001",
    )

    await transport.submit_passive_order(req)

    assert seen["path"] == "/api/v2/mix/order/place-order"
    assert seen["body"]["productType"] == "USDT-FUTURES"
    assert seen["body"]["marginMode"] == "crossed"
    assert seen["body"]["marginCoin"] == "USDT"
    assert seen["body"]["size"] == "0.001"
    assert seen["body"]["side"] == "sell"
    assert seen["body"]["orderType"] == "limit"
    assert seen["body"]["force"] == "post_only"
    assert seen["body"]["clientOid"] == "lfv2-entry-maker-001"
    assert "quantity" not in seen["body"]
```

- [ ] **Step 2: Write UTA request-body test**

```python
@pytest.mark.asyncio
async def test_bitget_uta_passive_order_body_uses_v3_trade_fields():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json=_fixture("bitget/uta_place_order_ack_only.json"))

    transport = _make_live_transport(Venue.BITGET, handler, bitget_profile="uta")
    req = OrderRequest(
        venue=Venue.BITGET,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=0.001,
        price=50000.0,
        post_only=True,
        client_order_id="lfv2-entry-maker-001",
    )

    await transport.submit_passive_order(req)

    assert seen["path"] == "/api/v3/trade/place-order"
    assert seen["body"]["category"] == "USDT-FUTURES"
    assert seen["body"]["qty"] == "0.001"
    assert seen["body"]["side"] == "buy"
    assert seen["body"]["orderType"] == "limit"
    assert seen["body"]["timeInForce"] == "post_only"
    assert seen["body"]["clientOid"] == "lfv2-entry-maker-001"
    assert "quantity" not in seen["body"]
```

- [ ] **Step 3: Implement profile-aware builder**

Mirror V1 `build_bitget_place_order_request` with Python equivalents:

```python
def _build_bitget_order_request(
    request: OrderRequest,
    venue_sym: str,
    *,
    passive: bool,
    profile: str,
    hedge_mode: bool,
) -> tuple[str, dict[str, Any]]:
    side = "buy" if request.side == Side.BUY else "sell"
    if profile == "uta":
        body = {
            "category": "USDT-FUTURES",
            "symbol": venue_sym,
            "qty": _format_quantity(request.quantity),
            "side": side,
            "orderType": "limit" if passive or request.price is not None else "market",
            "clientOid": request.client_order_id or "",
        }
        if passive or request.price is not None:
            body["timeInForce"] = "post_only" if passive else "ioc"
            body["price"] = _format_price(request.price or 0.0)
        if hedge_mode:
            body["posSide"] = "long" if request.side == Side.BUY else "short"
        else:
            body["reduceOnly"] = "yes" if request.reduce_only else "no"
        return "/api/v3/trade/place-order", body

    body = {
        "symbol": venue_sym,
        "productType": "USDT-FUTURES",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "size": _format_quantity(request.quantity),
        "side": side,
        "orderType": "limit" if passive or request.price is not None else "market",
        "force": "post_only" if passive else "ioc",
        "clientOid": request.client_order_id or "",
    }
    if passive or request.price is not None:
        body["price"] = _format_price(request.price or 0.0)
    if hedge_mode:
        body["tradeSide"] = "open" if not request.reduce_only else "close"
    else:
        body["reduceOnly"] = "YES" if request.reduce_only else "NO"
    return "/api/v2/mix/order/place-order", body
```

- [ ] **Step 4: Use returned path instead of static `spec.order_path` for Bitget**

For Bitget only, `place_order()` and `submit_passive_order()` must call `_request("POST", request_path, body=body, private=True)`.

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py -k "bitget and order" -q
```

Expected: Classic and UTA path/body tests pass, ACK-only remains ACK/uncertain according to API method.

---

### Task 5: Restore Official Aster Endpoints and Local Book Bootstrap Semantics

**Files:**
- Modify: `lightfee/venues/specs.py`
- Modify: `tests/test_venues_transport.py`
- Create: `tests/fixtures/venues/aster/depth_success.json`

**Goal of this task:** make V2 use the official Aster/V1 hosts and prevent DNS failures from `fapi.aster.exchange`.

- [ ] **Step 1: Add Aster spec test**

```python
def test_aster_spec_uses_official_asterdex_hosts():
    spec = aster_spec()
    assert spec.public_base_url == "https://fapi.asterdex.com"
    assert spec.private_base_url == "https://fapi.asterdex.com"
    assert spec.l2_snapshot_path == "/fapi/v1/depth"
```

- [ ] **Step 2: Change Aster spec defaults**

Update `aster_spec()`:

```python
public_base_url="https://fapi.asterdex.com",
private_base_url="https://fapi.asterdex.com",
```

Keep environment/config override support if it already exists; only change defaults.

- [ ] **Step 3: Add depth fixture**

Create `tests/fixtures/venues/aster/depth_success.json`:

```json
{
  "lastUpdateId": 123456,
  "E": 1710000000000,
  "T": 1710000000000,
  "bids": [["50000.0", "1.25"], ["49999.5", "0.5"]],
  "asks": [["50000.5", "1.0"], ["50001.0", "0.75"]]
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py -k "aster" -q
```

Expected: Aster spec and depth parser tests pass.

---

### Task 6: Add Bitget L2 Metadata Guard and Official Orderbook Parser Contract

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/marketdata/local_l2_venues.py`
- Modify: `tests/test_venues_transport.py`
- Create: `tests/fixtures/venues/bitget/orderbook_uta_success.json`
- Create: `tests/fixtures/venues/bitget/orderbook_unsupported_symbol.json`

**Goal of this task:** keep the official Bitget V3 orderbook endpoint but prevent unsupported symbols from turning into runtime snapshot errors.

- [ ] **Step 1: Add fixtures**

Create `tests/fixtures/venues/bitget/orderbook_uta_success.json`:

```json
{
  "code": "00000",
  "msg": "success",
  "requestTime": 1730969017897,
  "data": {
    "a": [[73000.0, 0.007], [74000.0, 0.007]],
    "b": [[71213.8, 1.836], [71213.3, 10.0]],
    "ts": "1730969017964"
  }
}
```

Create `tests/fixtures/venues/bitget/orderbook_unsupported_symbol.json`:

```json
{
  "code": "400172",
  "msg": "Parameter verification failed",
  "requestTime": 1730969017897,
  "data": null
}
```

- [ ] **Step 2: Add metadata guard test**

```python
@pytest.mark.asyncio
async def test_bitget_l2_unsupported_symbol_is_blocked_before_http_call():
    transport = _make_live_transport(Venue.BITGET, handler=_handler_that_fails_if_called)
    transport.set_symbol_metadata({"BTCUSDT": {"sizeMultiplier": "0.001"}})

    with pytest.raises(TransportError) as exc:
        await transport.fetch_l2_snapshot("INJUSDT", depth=50)

    assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
    assert "metadata missing" in str(exc.value)
```

- [ ] **Step 3: Add official parser test**

```python
@pytest.mark.asyncio
async def test_bitget_l2_v3_orderbook_parses_a_b_arrays():
    transport = _make_live_transport_with_mock_response(Venue.BITGET, "bitget/orderbook_uta_success.json")
    update = await transport.fetch_l2_snapshot("BTCUSDT", depth=50)

    assert update.venue == Venue.BITGET
    assert update.symbol == "BTCUSDT"
    assert update.bids[0].price == 71213.8
    assert update.asks[0].price == 73000.0
```

- [ ] **Step 4: Implement guard and parser normalization**

Before issuing Bitget L2 request, check symbol metadata if transport has a metadata cache. If metadata is unavailable during startup, fetch/refresh metadata first or classify as warmup-not-ready rather than snapshot error.

Use official Bitget response keys:

```python
data = raw.get("data") or {}
asks = data.get("a", data.get("asks", []))
bids = data.get("b", data.get("bids", []))
observed_at_ms = int(data.get("ts") or raw.get("requestTime") or now_ms)
```

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/marketdata/test_local_l2_runtime_targets.py -k "bitget or local_l2" -q
```

Expected: Bitget L2 parser succeeds on official payload and unsupported symbols do not become repeated snapshot errors.

---

### Task 7: Replace Generic Position Parser with Venue-Specific Parsers

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_venues_contract.py`

**Goal of this task:** eliminate `float("")` and response-shape crashes by copying V1 venue-specific parsing.

- [ ] **Step 1: Add failing parser tests**

```python
def test_bitget_position_parser_accepts_data_list_and_empty_numbers():
    raw = {
        "code": "00000",
        "msg": "success",
        "data": [
            {
                "symbol": "BTCUSDT",
                "total": "",
                "holdSide": "long",
                "openPriceAvg": ""
            }
        ]
    }
    pos = _parse_bitget_position(raw, "BTCUSDT", now_ms=1000)
    assert pos.quantity == 0.0
    assert pos.entry_price == 0.0
```

```python
def test_okx_position_parser_scales_contracts_and_handles_empty_pos():
    raw = {
        "code": "0",
        "data": [
            {"instId": "BTC-USDT-SWAP", "pos": "", "posSide": "long", "avgPx": ""}
        ]
    }
    pos = _parse_okx_position(raw, "BTCUSDT", now_ms=1000, contract_size=0.01)
    assert pos.quantity == 0.0
    assert pos.entry_price == 0.0
```

- [ ] **Step 2: Implement parser dispatch**

Replace `_parse_position()` internals with explicit dispatch:

```python
if spec.venue_id == Venue.BYBIT:
    return _parse_bybit_position(raw, venue_sym, now_ms)
if spec.venue_id == Venue.BITGET:
    return _parse_bitget_position(raw, venue_sym, now_ms)
if spec.venue_id == Venue.OKX:
    return _parse_okx_position(raw, venue_sym, now_ms, contract_size=spec.contract_size)
if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
    return _parse_binance_like_position(raw, venue_sym, now_ms)
if spec.venue_id == Venue.GATE:
    return _parse_gate_position(raw, venue_sym, now_ms)
if spec.venue_id == Venue.HYPERLIQUID:
    return _parse_hyperliquid_position(raw, venue_sym, now_ms)
```

- [ ] **Step 3: Implement Bybit parser**

```python
def _parse_bybit_position(raw: dict[str, Any], symbol: str, now_ms: int) -> PositionSnapshot:
    _require_bybit_success(raw, "bybit position failed")
    rows = ((raw.get("result") or {}).get("list") or [])
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if row.get("symbol") and row.get("symbol") != symbol:
            continue
        qty = abs(_safe_float(row.get("size"), default=0.0))
        side = str(row.get("side", ""))
        net += qty if side == "Buy" else -qty if side == "Sell" else 0.0
        entry_price = _safe_float(row.get("avgPrice") or row.get("entryPrice"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.BYBIT,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )
```

- [ ] **Step 4: Implement Bitget parser**

```python
def _parse_bitget_position(raw: dict[str, Any], symbol: str, now_ms: int) -> PositionSnapshot:
    _require_bitget_success(raw, "bitget position failed")
    data = raw.get("data", [])
    rows = data if isinstance(data, list) else data.get("list", []) if isinstance(data, dict) else []
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if normalize_symbol(row.get("symbol", "")) != normalize_symbol(symbol):
            continue
        qty = abs(_safe_float(row.get("total") or row.get("available") or row.get("holdVolume") or row.get("size")))
        hold_side = str(row.get("holdSide") or row.get("posSide") or "").lower()
        net += qty if hold_side in ("long", "buy") else -qty if hold_side in ("short", "sell") else qty
        entry_price = _safe_float(row.get("openPriceAvg") or row.get("avgPrice"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.BITGET,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )
```

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py -k "position" -q
```

Expected: all position parser tests pass and no parser uses direct `float(exchange_value)` on optional fields.

---

### Task 8: Make Entry Maker Lifecycle Passive-ACK First

**Files:**
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/core/contracts.py`
- Modify: `tests/test_entry_sync.py`
- Modify: `tests/test_runtime_entry_flow.py`
- Modify: `tests/test_v1_record_layer_parity.py`

**Goal of this task:** stop initial maker entry from using `place_order()` as if maker orders fill synchronously.

- [ ] **Step 1: Extend fake adapters with passive order support**

In tests, add `submit_passive_order_outcomes` and call counters:

```python
submit_passive_order_outcomes: list = field(default_factory=list)
submit_passive_order_call_count: int = 0

async def submit_passive_order(self, request):
    self.submit_passive_order_call_count += 1
    if self.submit_passive_order_outcomes:
        outcome = self.submit_passive_order_outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome
    return PassiveOrderAck(
        venue=request.venue,
        symbol=request.symbol,
        side=request.side,
        order_id=f"passive-{self._venue.value}-{self.submit_passive_order_call_count}",
        client_order_id=request.client_order_id or "",
        price=request.price or 0.0,
        quantity=request.quantity,
        accepted_at_ms=1234,
    )
```

- [ ] **Step 2: Add entry executor test**

```python
@pytest.mark.asyncio
async def test_entry_maker_uses_submit_passive_order_not_place_order(btc_context):
    maker = FakeVenueAdapter(Venue.BINANCE)
    hedge = FakeVenueAdapter(Venue.OKX)
    maker.submit_passive_order_outcomes = [
        PassiveOrderAck(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            order_id="maker-order-1",
            client_order_id=f"{btc_context.entry_id}-maker",
            price=50000.0,
            quantity=0.001,
            accepted_at_ms=1000,
        )
    ]
    executor = EntryExecutor({Venue.BINANCE: maker, Venue.OKX: hedge})

    result = await executor.execute(btc_context)

    assert maker.submit_passive_order_call_count == 1
    assert maker.place_order_call_count == 0
    assert hedge.place_order_call_count == 0
    assert result.pending_entry is not None
    assert result.pending_entry.maker_order_id == "maker-order-1"
    assert result.pending_entry.entry_type == "passive_maker"
```

- [ ] **Step 3: Refactor `_submit_maker()`**

When maker request is `post_only=True`, call `adapter.submit_passive_order(request)` and return an ACK outcome:

```python
if is_maker and request.post_only:
    ack = await adapter.submit_passive_order(request)
    self.journal.append(
        "order.passive_submitted",
        {
            "position_id": position_id,
            "leg": leg,
            "venue": request.venue.value,
            "symbol": request.symbol,
            "order_id": ack.order_id,
            "client_order_id": ack.client_order_id,
            "price": ack.price,
            "quantity": ack.quantity,
        },
    )
    return {"outcome": "resting", "ack": ack, "order_id": ack.order_id}
```

- [ ] **Step 4: Refactor `execute()` result handling**

If maker outcome is `resting`, create `PendingEntry` and return without submitting hedge:

```python
if maker_result["outcome"] == "resting":
    ack = maker_result["ack"]
    result.state = EntryState.MAKER_RESTING
    result.pending_entry = self._make_pending_entry(
        ctx,
        maker_req,
        hedge_req,
        now_ms,
        outcome="maker_resting",
        maker_order_id=ack.order_id,
        hedge_order_id="",
    )
    return result
```

- [ ] **Step 5: Keep taker/hedge path unchanged**

`place_order()` remains valid for hedge/taker IOC paths and paper-mode immediate fills.

- [ ] **Step 6: Run tests**

Run:

```bash
pytest tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/test_v1_record_layer_parity.py -k "maker or pending_entry or passive" -q
```

Expected: maker initial entry no longer calls `place_order()`; pending passive maker state is created and hedge is not submitted until maker fill/progress is observed.

---

### Task 9: Wire V1 Entry Local L2 Primary/Shadow/Prewarm Selection into Runtime

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/entry_local_l2.py`
- Modify: `lightfee/config/schema.py`
- Modify: `tests/test_entry_local_l2.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`

**Goal of this task:** stop V2 from selecting candidates that V1 would have held back for local L2 prewarm or dual-ready.

- [ ] **Step 1: Add runtime selection test**

```python
@pytest.mark.asyncio
async def test_runtime_does_not_dispatch_tradeable_candidate_before_entry_l2_dual_ready(runtime_with_candidates):
    runtime = runtime_with_candidates
    runtime.config.strategy.local_l2_enabled = True
    runtime.config.strategy.entry_local_l2_primary_count = 1
    runtime.config.strategy.entry_local_l2_prewarm_window_secs = 480
    runtime.entry_l2_sessions.sessions.clear()

    await runtime.tick()

    assert not runtime.journal.contains("runtime.entry_dispatched")
    assert runtime.journal.contains("runtime.entry_blocked_local_l2_selection")
```

- [ ] **Step 2: Add ready candidate dispatch test**

```python
@pytest.mark.asyncio
async def test_runtime_dispatches_candidate_after_entry_l2_dual_ready(runtime_with_ready_l2_candidate):
    runtime = runtime_with_ready_l2_candidate
    runtime.config.strategy.local_l2_enabled = True

    await runtime.tick()

    assert runtime.journal.contains("runtime.entry_dispatched")
```

- [ ] **Step 3: Implement selection gating function**

Add a runtime helper:

```python
def _entry_local_l2_selection_blocker(self, candidate, now_ms: int) -> str | None:
    if not self.config.strategy.local_l2_enabled:
        return None
    if not _candidate_in_entry_l2_prewarm_window(candidate, now_ms, self.config.strategy.entry_local_l2_prewarm_window_secs):
        return "entry_local_l2_waiting_for_prewarm_window"
    session = self.entry_l2_sessions.sessions.get(candidate.pair_id)
    if session is None or not session.both_legs_ready(now_ms, self._entry_l2_readiness_max_age_ms(candidate)):
        return "entry_local_l2_waiting_for_dual_ready"
    return None
```

- [ ] **Step 4: Wire `select_tracked_opportunities()`**

After `discover_tradeable_candidates(...)`, select primary/shadow tracked opportunities and update sessions before dispatch:

```python
tracked = select_tracked_opportunities(
    tradeable,
    primary_count=self.config.strategy.entry_local_l2_primary_count,
    shadow_count=getattr(self.config.strategy, "entry_local_l2_shadow_count", self.config.strategy.entry_local_l2_primary_count),
)
self.entry_l2_sessions.update_tracked(tracked, now_ms)
```

Filter dispatch list:

```python
dispatchable = []
for candidate in tradeable:
    blocker = self._entry_local_l2_selection_blocker(candidate, now_ms)
    if blocker:
        self.journal.append(
            "runtime.entry_blocked_local_l2_selection",
            {"symbol": candidate.symbol, "pair_id": candidate.pair_id, "reason": blocker, "ts_ms": now_ms},
        )
        continue
    dispatchable.append(candidate)
```

- [ ] **Step 5: Add venue-specific readiness age**

Add V1-style config fields:

```python
local_l2_quiet_book_grace_ms: int = 10000
local_l2_okx_min_readiness_age_ms: int = 65000
```

Use these for selection readiness. Keep final execution export gate strict unless V1 proves both gates use the same readiness window for the venue.

- [ ] **Step 6: Run tests**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py -k "entry_l2 or local_l2" -q
```

Expected: candidates blocked at selection do not produce `runtime.entry_blocked_local_l2_not_ready` spam at dispatch; ready candidates still dispatch.

---

### Task 10: Preserve Final Local L2 Safety Gate and Improve Observability

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`

**Goal of this task:** keep V1/V2 shared no-open-without-ready-L2 behavior while making reasons actionable.

- [ ] **Step 1: Add final gate reason test**

```python
@pytest.mark.asyncio
async def test_final_l2_gate_reports_age_threshold_and_pool(runtime_with_stale_l2_book):
    runtime = runtime_with_stale_l2_book

    await runtime._dispatch_entry(runtime.candidate, now_ms=100000, price_hint=50000.0)

    event = runtime.journal.last("runtime.entry_blocked_local_l2_not_ready")
    assert "max_age_ms" in event["reasons"][0]
    assert "pool=" in event["reasons"][0]
```

- [ ] **Step 2: Enhance reason strings**

Change final L2 not-ready detail to include venue, symbol, status, age, threshold, pool, and last update source:

```python
f"long leg not ready: {long_venue.value}:{candidate.symbol} "
f"status={long_book.status.value} pool={long_book.pool.value} "
f"age={long_book.age_ms(now_ms)}ms max_age_ms={max_age_ms}"
```

- [ ] **Step 3: Keep final return behavior unchanged**

If either leg is missing or not ready, return without creating a pending entry or submitting any order.

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_runtime_maker_event_local_l2.py -k "final_l2_gate or local_l2_not_ready" -q
```

Expected: final safety gate still blocks, but logs show the threshold that caused the block.

---

### Task 11: Add Order Status Reconciliation for ACK-Only and Uncertain Submits

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `tests/test_recovery_reconciliation.py`
- Modify: `tests/test_venues_transport.py`

**Goal of this task:** turn shared ACK/uncertain exchange behavior into deterministic recovery using client IDs.

- [ ] **Step 1: Extend adapter contract**

Add an optional method:

```python
async def fetch_order_status(
    self,
    symbol: str,
    *,
    order_id: str = "",
    client_order_id: str = "",
) -> PassiveOrderProgress | OrderFillReconciliation | None:
    transport = getattr(self, "_transport", None)
    if transport is not None and hasattr(transport, "fetch_order_status"):
        return await transport.fetch_order_status(symbol, order_id=order_id, client_order_id=client_order_id)
    return None
```

- [ ] **Step 2: Add Bybit orderLinkId status test**

```python
@pytest.mark.asyncio
async def test_bybit_fetch_order_status_by_order_link_id_parses_cum_exec_qty():
    transport = _make_live_transport_with_mock_response(Venue.BYBIT, "bybit/order_realtime_filled.json")
    status = await transport.fetch_order_status("BTCUSDT", client_order_id="lfv2-entry-maker-001")

    assert status.client_order_id == "lfv2-entry-maker-001"
    assert status.filled_quantity == 0.001
```

- [ ] **Step 3: Add Bitget clientOid status test**

```python
@pytest.mark.asyncio
async def test_bitget_fetch_order_status_by_client_oid_parses_order_info():
    transport = _make_live_transport_with_mock_response(Venue.BITGET, "bitget/order_info_filled.json")
    status = await transport.fetch_order_status("BTCUSDT", client_order_id="lfv2-entry-maker-001")

    assert status.client_order_id == "lfv2-entry-maker-001"
    assert status.filled_quantity == 0.001
```

- [ ] **Step 4: Implement status endpoints**

Use official endpoints:

```python
if spec.venue_id == Venue.BYBIT:
    params = {"category": "linear", "symbol": venue_sym, "openOnly": 0}
    if order_id:
        params["orderId"] = order_id
    if client_order_id:
        params["orderLinkId"] = client_order_id
    raw = await self._request("GET", "/v5/order/realtime", params=params, private=True)
```

```python
if spec.venue_id == Venue.BITGET:
    params = {}
    if order_id:
        params["orderId"] = order_id
    if client_order_id:
        params["clientOid"] = client_order_id
    raw = await self._request("GET", "/api/v3/trade/order-info", params=params, private=True)
```

- [ ] **Step 5: Wire reconciliation**

When pending entry has maker order ID or maker client order ID but no fill, try `fetch_order_status()` before falling back to position-only reconciliation.

- [ ] **Step 6: Run tests**

Run:

```bash
pytest tests/test_recovery_reconciliation.py tests/test_venues_transport.py -k "order_status or client_oid or order_link" -q
```

Expected: ACK-only and uncertain pending entries can be reconciled by order/client ID.

---

### Task 12: Runtime and Cloud Verification Harness

**Files:**
- Modify: `tests/test_live_full_closure.py`
- Modify: `tests/test_runtime_smoke.py`
- Create: `scripts/lightfee_v2_live_dryrun_audit.py`

**Goal of this task:** verify the root fixes without sending live orders in CI and with an explicit dry-run path on the cloud box.

- [ ] **Step 1: Add smoke test for no order when L2 missing**

```python
@pytest.mark.asyncio
async def test_live_runtime_with_missing_l2_never_submits_order(runtime_with_missing_l2):
    await runtime_with_missing_l2.tick()
    assert not runtime_with_missing_l2.journal.contains("order.submitted")
    assert runtime_with_missing_l2.journal.contains("runtime.entry_blocked_local_l2_selection")
```

- [ ] **Step 2: Add smoke test for passive maker ACK**

```python
@pytest.mark.asyncio
async def test_live_runtime_ready_l2_submits_passive_maker_only(runtime_with_ready_l2_and_ack):
    await runtime_with_ready_l2_and_ack.tick()
    assert runtime_with_ready_l2_and_ack.journal.contains("order.passive_submitted")
    assert not runtime_with_ready_l2_and_ack.journal.contains("order.filled")
    assert runtime_with_ready_l2_and_ack.state.pending_entries
```

- [ ] **Step 3: Create dry-run audit script**

Create a script that reads the last N minutes of journal/log files and reports:

```text
open_position_count
pending_entry_count
entry_blocked_local_l2_selection count and top reasons
entry_blocked_local_l2_not_ready count and top reasons
order.passive_submitted count
order.uncertain count
runtime.local_l2_snapshot_error count by venue/reason
position fetch failed count by venue/reason
```

The script must not import live credentials or submit orders.

- [ ] **Step 4: Run full targeted suite**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py tests/test_recovery_reconciliation.py -q
```

Expected: all targeted tests pass.

- [ ] **Step 5: Cloud dry-run acceptance**

After deployment, run audit only:

```bash
python3 scripts/lightfee_v2_live_dryrun_audit.py --minutes 120 --log /var/log/lightfee-v2/live.log
```

Acceptance:

- `order.uncertain` caused by ACK-only maker responses is zero.
- Bybit order bodies include `qty` and `orderLinkId`, not `quantity`.
- Bitget order bodies use either `/api/v2/mix/order/place-order` with classic fields or `/api/v3/trade/place-order` with UTA fields.
- Aster DNS errors for `fapi.aster.exchange` are zero.
- Bitget L2 `400172` is not repeated for unsupported symbols; such symbols are classified as metadata/support gaps before HTTP snapshot.
- `entry_blocked_local_l2_not_ready` drops sharply because selection blocks unready candidates earlier.
- No live order is submitted unless both local L2 legs are ready.

---

## Implementation Order

1. Task 1 and Task 2 first, because they define exchange contracts and safe parsing used by every later task.
2. Task 3 and Task 4 next, because Bybit/Bitget caused actual live order failures.
3. Task 5 and Task 6 next, because Aster/Bitget L2 snapshot errors are preventing readiness.
4. Task 7 next, because reconciliation/position health must be robust before live testing.
5. Task 8 next, because passive maker lifecycle is the main open-risk fix.
6. Task 9 and Task 10 next, because local L2 selection should prevent dispatch spam while preserving the final safety gate.
7. Task 11 next, because ACK-only and uncertain states need deterministic recovery.
8. Task 12 last, because it verifies the whole root-fix chain end-to-end.

## Acceptance Criteria

- V2 no longer submits initial maker entry through `place_order()` when `post_only=True`.
- Bybit order payloads match official field names and V1 semantics: `qty`, `orderLinkId`, `positionIdx`, `Buy/Sell`, `PostOnly`.
- Bitget Classic and UTA order payloads use the correct endpoint/body shape for the detected account profile.
- Aster default REST/WS hosts match official docs and V1 defaults.
- Bitget L2 uses official V3 orderbook and blocks unsupported symbols before repeated HTTP failures.
- Position parsers never call `float("")` and never assume `result`/`data` shape without type checks.
- Local L2 final gate remains strict, but candidate selection now filters prewarm/primary/dual-ready candidates before dispatch.
- Duplicate client order ID protection remains enabled and covered by tests.
- Reconciliation can query Bybit by `orderLinkId` and Bitget by `clientOid`.
- Targeted test suite passes.

## Self-Review

Spec coverage:

- Bybit order failure is covered by Tasks 1, 2, 3, and 11.
- Bitget order 404/malformed submit is covered by Tasks 1, 2, 4, and 11.
- Aster DNS is covered by Task 5.
- Bitget L2 400 is covered by Task 6.
- Position parser failures are covered by Task 7.
- Local L2 no-open safety and dispatch spam are covered by Tasks 9 and 10.
- Duplicate client order ID behavior is preserved by non-goals and Task 11.
- Cloud validation is covered by Task 12.

Placeholder scan:

- No placeholder markers or vague “handle edge cases” steps remain.
- Every implementation task names files, tests, commands, and expected behavior.

Type consistency:

- ACK paths consistently use `PassiveOrderAck`.
- Immediate fill paths consistently use `OrderFill`.
- Reconciliation status paths use `PassiveOrderProgress` or `OrderFillReconciliation`.
- Bybit client ID uses `orderLinkId`; Bitget client ID uses `clientOid`.
