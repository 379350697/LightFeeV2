# V1 Semantic 100% Parity Marketdata and Venues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns venue contracts, symbol authority, and local-L2 data-plane behavior.

**Goal:** Make LightFeeV2's venue adapters, symbol conversion, local-L2 data plane, and worker lifecycle match V1 semantics for all supported venues.

**Architecture:** Venue modules own exchange-specific truth. The local-L2 data plane owns ingestion, worker lifecycle, and book bootstrap, but it should not invent venue rules on its own. Canonical symbols must be resolved at the boundaries and then preserved internally.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, websockets, httpx, existing LightFeeV2 marketdata and venue modules, GitNexus MCP, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
- Current gap baseline: `docs/superpowers/parity/2026-05-11-v1-full-parity-gap-closure-matrix.md`
- Rust V1 anchors:
  - `/media/wl/新加卷/codex/LightFee/src/live/*.rs`
  - `/media/wl/新加卷/codex/LightFee/src/market_gateway/*`
  - `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_*.rs`

## File Ownership

- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/*.py`
- Modify: `lightfee/marketdata/l2.py`
- Modify: `lightfee/marketdata/liquidity.py`
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `lightfee/marketdata/local_l2_runtime.py`
- Modify: `lightfee/marketdata/local_l2_venues.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Modify: `tests/test_local_l2_runtime.py`
- Modify: `tests/test_local_l2_venue_rules.py`
- Modify: `tests/test_local_l2_ws.py`
- Modify: `tests/test_venues_contract.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_marketdata_l2.py`

Do not edit runtime orchestration or execution routing in this plan.

## Task 1: Lock Canonical Symbol And Venue Truth

**Files:**
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `tests/test_venues_contract.py`

- [ ] **Step 1: Write the failing test**

Add a test proving the wire symbol is translated to a canonical internal symbol and that unsupported capabilities are not silently swallowed.

```python
def test_canonical_symbol_round_trip_and_unsupported_logging():
    spec = get_venue_spec("okx")
    assert spec.symbol_to_venue("BTC-USDT-SWAP") == "BTCUSDT"
```

- [ ] **Step 2: Run the test**

Run:

```bash
rtk pytest tests/test_venues_contract.py -q -W error
```

- [ ] **Step 3: Implement the symbol authority**

Keep canonical symbols internal. Wire symbols stay at request/subscribe/parse boundaries only.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_venues_contract.py -q -W error
```

## Task 2: Match Local-L2 Worker Lifecycle And Bootstrap Semantics

**Files:**
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Modify: `tests/test_local_l2_ws.py`

- [ ] **Step 1: Write the failing test**

Add tests proving worker registration, start, stop, and abort are explicit and idempotent, and that websocket connection setup is bounded.

```python
async def test_ws_connect_has_open_timeout(monkeypatch):
    assert LocalL2WsClient.OPEN_TIMEOUT_SECONDS == 10
```

- [ ] **Step 2: Run the test**

Run:

```bash
rtk pytest tests/test_local_l2_ws.py -q -W error
```

- [ ] **Step 3: Implement the worker boundary**

The data plane should own worker bookkeeping and start/stop/abort semantics. The websocket client should never rely on unbounded connects or silent failures.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_local_l2_ws.py -q -W error
```

## Task 3: Preserve Local-L2 Book Semantics

**Files:**
- Modify: `lightfee/marketdata/l2.py`
- Modify: `lightfee/marketdata/local_l2_runtime.py`
- Modify: `lightfee/marketdata/local_l2_venues.py`
- Modify: `lightfee/marketdata/liquidity.py`
- Modify: `tests/test_marketdata_l2.py`
- Modify: `tests/test_local_l2_runtime.py`
- Modify: `tests/test_local_l2_venue_rules.py`

- [ ] **Step 1: Write the failing test**

Add tests that prove snapshot/delta, checksum, sequence gap, readiness, and venue normalization all line up with V1 behavior.

```python
def test_checksum_is_deterministic_for_same_book_state():
    book_a = make_book(...)
    book_b = make_book(...)
    assert book_a.compute_checksum() == book_b.compute_checksum()
```

- [ ] **Step 2: Run the focused suite**

Run:

```bash
rtk pytest tests/test_marketdata_l2.py tests/test_local_l2_runtime.py tests/test_local_l2_venue_rules.py -q -W error
```

- [ ] **Step 3: Implement the book-state semantics**

Preserve V1 meaning for:

- book readiness
- bootstrapping versus hot versus resume-waiting
- checksum and sequence-gap rebuild behavior
- per-venue rule tables and depth defaults
- execution-liquidity readiness checks

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_marketdata_l2.py tests/test_local_l2_runtime.py tests/test_local_l2_venue_rules.py -q -W error
```

## Task 4: Run Blast Radius Check

**Files:**
- No code edits

- [ ] **Step 1: Inspect changed symbols**

Run:

```bash
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

- [ ] **Step 2: Confirm scope**

Expected: changes should stay inside the venue and marketdata surfaces.

