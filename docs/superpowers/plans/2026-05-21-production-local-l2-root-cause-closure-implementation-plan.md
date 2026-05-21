# Production Local-L2 Root-Cause Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fully root-fix the production Local-L2 / Entry-L2 bootstrap, rebuild, readiness, and entry-blocker issues found from the multi-day 2026-05-12 to 2026-05-21 cloud sample.

**Architecture:** Split the work into evidence freeze, deterministic harness, V1-parity venue policies, exchange-doc-driven venue policies, and cloud acceptance. V1/V2 drifts are fixed by exact V1 replication; non-V1 issues are fixed only after probe/log evidence and official exchange docs prove the root cause.

**Tech Stack:** Python 3.11/3.12, pytest, pytest-asyncio, LightFeeV2 journal/current-state, GitNexus, public exchange REST/WS probes, V1 Rust references in `/media/wl/新加卷/codex/LightFee`.

---

## Required Context

- Spec: `docs/superpowers/specs/2026-05-21-production-local-l2-root-cause-closure-design.md`
- Bug ledger index: `docs/bugs/BUG_INDEX.md`
- Current daily bug ledger: `docs/bugs/daily/2026-05-21.md`
- Prior exchange semantics fix: `docs/bugs/daily/2026-05-20.md#cluster-cl-003-local-l2-exchange-semantics-root-fix`
- V2 data-plane: `lightfee/marketdata/local_l2_data_plane.py`
- V2 book/runtime: `lightfee/marketdata/l2.py`, `lightfee/marketdata/local_l2_runtime.py`
- V2 parsers/WS: `lightfee/marketdata/local_l2_venues.py`, `lightfee/marketdata/local_l2_ws.py`
- V2 entry readiness: `lightfee/engine/entry_local_l2.py`, `lightfee/engine/runtime.py`
- V1 Binance bootstrap/replay: `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- V1 Bybit WS local-L2: `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- V1 OKX replay/checksum: `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- V1 entry local-L2 gate: `/media/wl/新加卷/codex/LightFee/src/execution_core/market_data.rs`

Before editing any function, class, or method, run GitNexus impact analysis per `AGENTS.md`. If risk is HIGH or CRITICAL, stop and report before editing.

## Execution Note - 2026-05-21 Review Correction

- Review found the first execution over-broadened Bybit's `WS_SNAPSHOT_AUTHORITATIVE` policy to OKX/Bitget/Gate and left OKX replay classification insufficiently exercised.
- Corrected working tree scope:
  - Bybit remains `WS_SNAPSHOT_AUTHORITATIVE`, but REST bootstrap defers only while the WS client is connected.
  - OKX remains V1 `REST_SNAPSHOT_BUFFERED_REPLAY`, and Keepalive/Reset now pass through the replay path before generic boundary checks.
  - Bitget/Gate are restored to legacy REST bridge behavior until live probe/log evidence proves a behavior change is required.
  - Public probes now use `VenueSpec.symbol_to_venue` wire symbols and parse Hyperliquid two-sided `levels`.
- A later cloud probe proved Gate legacy `futures.order_book` was rejected with `invalid accuracy 100ms`; official Gate futures WS docs place `100ms` on `futures.order_book_update`, so the root fix keeps legacy `futures.order_book` and changes only its interval to `"0"`. The probe now fails closed on subscribe failures.
- Hyperliquid probe diagnostics no longer reference an out-of-scope `levels` variable and now report total bid+ask levels from the parsed two-sided response.
- Verified locally after correction: `pytest -q` = 2852 passed, 2 skipped, 1 warning.
- Cloud acceptance after deploy:
  - `/opt/lightfee-v2` current run `lightfee-1779330433630-1279973` is `running/running`, open/pending counts zero, and `degraded_venues=[]`.
  - Post-deploy Local-L2 event window contained 121 `runtime.local_l2_snapshot_ok`, 26 `runtime.local_l2_snapshots_synced`, and 4 policy-expected Bybit `runtime.local_l2_rest_bootstrap_deferred_for_ws_snapshot` events; no `snapshot_error`, `snapshot_stale`, `buffer_overflow_rebuild`, or `hot_stale_rebuild` appeared in that window.
  - Public probes passed for Bybit IRYSUSDT, Binance JTOUSDT, OKX INJUSDT, Bitget BTCUSDT, Gate BTCUSDT, and Hyperliquid BTCUSDT.
  - No entry order was selected/dispatched in the short window, so order-path acceptance remains watch-based.

## File Structure

| Path | Responsibility |
|---|---|
| `scripts/probe_local_l2_rebuilds.py` | New public-network probe for real REST/WS payloads and old-vs-new bridge decisions |
| `tests/fixtures/local_l2/` | Sanitized fixture payloads captured from failed symbols and generated probe samples |
| `tests/test_local_l2_replay_harness.py` | Deterministic replay harness tests for venue bootstrap/replay policies |
| `lightfee/marketdata/local_l2_policy.py` | New small venue policy module: bridge mode, buffer cap, sequence-domain comparability, replay classifier |
| `lightfee/marketdata/local_l2_data_plane.py` | Apply venue policy during bootstrap, buffer, replay, snapshot stale checks, and evidence logging |
| `lightfee/marketdata/local_l2_ws.py` | Venue-specific WS parser/subscription fixes only where docs/probes prove drift |
| `lightfee/marketdata/local_l2_venues.py` | Parser metadata/source/depth fields only if needed by policy and tests |
| `lightfee/marketdata/l2.py` | Minimal update metadata fields if policy cannot be expressed with existing update fields |
| `tests/test_local_l2_runtime.py` | Runtime/data-plane tests for V1 parity and exchange-doc policies |
| `tests/test_local_l2_ws.py` | WS parser/subscription tests for Bybit/OKX/Bitget/Gate semantics |
| `tests/test_local_l2_venue_rules.py` | Parser and venue rule tests |
| `docs/bugs/BUG_INDEX.md` | Status update after implementation evidence |
| `docs/bugs/daily/2026-05-21.md` | Append root-fix evidence and residual classification |

## Task 1: Freeze Multi-Day Evidence and Bug Ledger Scope

**Files:**
- Modify: `docs/bugs/BUG_INDEX.md`
- Modify: `docs/bugs/daily/2026-05-21.md`

- [ ] **Step 1: Re-run multi-day event classification**

Run on cloud with absolute paths:

```bash
python3 /tmp/lightfee_remote_cmd.py "python3 - <<'PY'
import json, collections, datetime
path='/opt/lightfee-v2/runtime/live-events.jsonl'
keys={
 'runtime.entry_blocked_local_l2_selection',
 'runtime.local_l2_buffer_overflow_rebuild',
 'runtime.local_l2_hot_stale_rebuild',
 'runtime.local_l2_snapshot_error',
 'runtime.local_l2_snapshot_stale',
 'runtime.entry_local_l2_readiness_diagnostics',
 'scan.no_entry_diagnostics',
 'runtime.local_l2_symbol_skipped',
}
counts=collections.Counter()
by_day=collections.defaultdict(collections.Counter)
by_vs=collections.defaultdict(collections.Counter)
first=last=None
for line in open(path):
    e=json.loads(line)
    k=e.get('kind') or e.get('event') or e.get('type')
    ts=e.get('ts') or e.get('timestamp') or e.get('ts_ms')
    dt=None
    if isinstance(ts,(int,float)):
        v=float(ts)
        dt=datetime.datetime.fromtimestamp(v/1000 if v>10**12 else v, datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=8)))
    elif isinstance(ts,str):
        dt=datetime.datetime.fromisoformat(ts.replace('Z','+00:00')).astimezone(datetime.timezone(datetime.timedelta(hours=8)))
    if dt:
        first=dt if first is None or dt<first else first
        last=dt if last is None or dt>last else last
    if k in keys:
        p=e.get('payload') or e
        counts[k]+=1
        by_day[dt.date().isoformat() if dt else 'unknown'][k]+=1
        by_vs[k][(str(p.get('venue') or ''), str(p.get('symbol') or ''))]+=1
print('timespan', first.isoformat(), last.isoformat())
print('counts', counts)
for k in keys:
    print('top', k, by_vs[k].most_common(20))
PY"
```

Expected: counts remain materially consistent with the spec. If the current log rotated, collect rotated absolute paths before proceeding.

- [ ] **Step 2: Append evidence summary to daily ledger**

Update `docs/bugs/daily/2026-05-21.md` with a new subsection:

```markdown
### Multi-Day Local-L2 Closure Scope - 2026-05-21

Evidence window: `/opt/lightfee-v2/runtime/live-events.jsonl`, 2026-05-12 22:55:03 +08 through 2026-05-21 02:41:45 +08, 2,544,221 JSONL rows.

Closed/currently effective:
- Unsupported catalog symbols are filtered before active local-L2 book creation/restore: Binance `SYSUSDT`, Aster `RLSUSDT`, Hyperliquid `MAV`.
- REST snapshot parser/schema drift from CL-003 is no longer the dominant current-run snapshot-error source.

Open/root-fix required:
- Bybit REST/WS depth sequence-domain drift, currently visible as `IRYSUSDT` stale snapshot loop and entry-critical `book_bootstrapping`.
- V2 pre-snapshot buffer cap/replay policy drift versus V1, visible as 129,616 `runtime.local_l2_buffer_overflow_rebuild` events.
- OKX keepalive/reset/checksum replay parity gap versus V1.
- Hot-stale rebuild classification requires venue-specific worker/timestamp/subscription evidence.
```

- [ ] **Step 3: Update bug index row**

Update `CL-002-live-entry-l2-and-exchange-residual-watch` in `docs/bugs/BUG_INDEX.md` to mention:

```text
Multi-day evidence confirms this remains open: Bybit IRYSUSDT REST/WS depth sequence-domain stale loop, buffer_overflow_rebuild V1 cap/replay drift, OKX replay-classification parity gap, and hot_stale_rebuild evidence classification. Unsupported catalog symbols are no longer the active root cause.
```

- [ ] **Step 4: Verify docs only**

Run:

```bash
git diff --check
```

Expected: no whitespace errors.

## Task 2: Add Venue Policy Skeleton with Tests First

**Files:**
- Create: `lightfee/marketdata/local_l2_policy.py`
- Create: `tests/test_local_l2_policy.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LocalL2DataPlane", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LocalL2Update", direction: "upstream", repo: "LightFeeV2"})
```

Report risk and direct callers before editing.

- [ ] **Step 2: Write policy tests**

Create `tests/test_local_l2_policy.py`:

```python
from lightfee.marketdata.local_l2_policy import (
    BridgeMode,
    ReplayLinkKind,
    policy_for_venue,
)


def test_binance_uses_v1_rest_snapshot_buffered_replay_policy():
    policy = policy_for_venue("binance")

    assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
    assert policy.pre_snapshot_buffer_cap == 4096
    assert policy.rest_snapshot_sequence_comparable is True


def test_aster_uses_binance_style_rest_snapshot_buffered_replay_policy():
    policy = policy_for_venue("aster")

    assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
    assert policy.pre_snapshot_buffer_cap == 4096
    assert policy.rest_snapshot_sequence_comparable is True


def test_bybit_ws_snapshot_is_authoritative_and_rest_sequence_not_comparable():
    policy = policy_for_venue("bybit")

    assert policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE
    assert policy.rest_snapshot_sequence_comparable is False
    assert policy.replay_rest_snapshot_with_ws_deltas is False


def test_okx_replay_classifier_accepts_keepalive_and_reset():
    policy = policy_for_venue("okx")

    keepalive = policy.classify_replay_link(
        previous_sequence=15,
        sequence=15,
        previous_sequence_from_update=15,
        bid_count=0,
        ask_count=0,
    )
    reset = policy.classify_replay_link(
        previous_sequence=15,
        sequence=3,
        previous_sequence_from_update=15,
        bid_count=1,
        ask_count=1,
    )

    assert keepalive is ReplayLinkKind.KEEPALIVE
    assert reset is ReplayLinkKind.RESET
```

- [ ] **Step 3: Run tests and confirm red**

Run:

```bash
pytest tests/test_local_l2_policy.py -q
```

Expected before implementation: import failure for `lightfee.marketdata.local_l2_policy`.

- [ ] **Step 4: Implement the policy module**

Create `lightfee/marketdata/local_l2_policy.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BridgeMode(Enum):
    REST_SNAPSHOT_BUFFERED_REPLAY = "rest_snapshot_buffered_replay"
    WS_SNAPSHOT_AUTHORITATIVE = "ws_snapshot_authoritative"
    REST_POLLING_SNAPSHOT_ONLY = "rest_polling_snapshot_only"


class ReplayLinkKind(Enum):
    NORMAL = "normal"
    KEEPALIVE = "keepalive"
    RESET = "reset"
    OBSOLETE = "obsolete"
    INVALID = "invalid"


@dataclass(frozen=True)
class LocalL2VenuePolicy:
    venue: str
    bridge_mode: BridgeMode
    pre_snapshot_buffer_cap: int
    rest_snapshot_sequence_comparable: bool = True
    replay_rest_snapshot_with_ws_deltas: bool = True

    def classify_replay_link(
        self,
        *,
        previous_sequence: int,
        sequence: int,
        previous_sequence_from_update: int,
        bid_count: int,
        ask_count: int,
    ) -> ReplayLinkKind:
        if self.venue == "okx":
            if previous_sequence_from_update == previous_sequence and sequence == previous_sequence and bid_count == 0 and ask_count == 0:
                return ReplayLinkKind.KEEPALIVE
            if previous_sequence_from_update == previous_sequence and sequence < previous_sequence:
                return ReplayLinkKind.RESET
            if previous_sequence_from_update == previous_sequence and sequence > previous_sequence:
                return ReplayLinkKind.NORMAL
            if sequence <= previous_sequence:
                return ReplayLinkKind.OBSOLETE
            return ReplayLinkKind.INVALID

        if previous_sequence_from_update > 0:
            if previous_sequence_from_update == previous_sequence:
                return ReplayLinkKind.NORMAL
            if sequence <= previous_sequence:
                return ReplayLinkKind.OBSOLETE
            return ReplayLinkKind.INVALID

        if sequence <= previous_sequence:
            return ReplayLinkKind.OBSOLETE
        return ReplayLinkKind.NORMAL


def policy_for_venue(venue: str) -> LocalL2VenuePolicy:
    normalized = str(venue).lower()
    if normalized in {"binance", "aster"}:
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            pre_snapshot_buffer_cap=4096,
        )
    if normalized == "hyperliquid":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_POLLING_SNAPSHOT_ONLY,
            pre_snapshot_buffer_cap=0,
            rest_snapshot_sequence_comparable=False,
            replay_rest_snapshot_with_ws_deltas=False,
        )
    if normalized == "bybit":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.WS_SNAPSHOT_AUTHORITATIVE,
            pre_snapshot_buffer_cap=4096,
            rest_snapshot_sequence_comparable=False,
            replay_rest_snapshot_with_ws_deltas=False,
        )
    if normalized in {"okx", "bitget", "gate"}:
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.WS_SNAPSHOT_AUTHORITATIVE,
            pre_snapshot_buffer_cap=4096,
            rest_snapshot_sequence_comparable=False,
            replay_rest_snapshot_with_ws_deltas=False,
        )
    return LocalL2VenuePolicy(
        venue=normalized,
        bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
        pre_snapshot_buffer_cap=4096,
    )
```

- [ ] **Step 5: Verify policy tests**

Run:

```bash
pytest tests/test_local_l2_policy.py -q
```

Expected: `4 passed`.

## Task 3: Build Deterministic Replay Harness

**Files:**
- Create: `tests/test_local_l2_replay_harness.py`
- Create: `tests/fixtures/local_l2/bybit_irysusdt_rest_ws_sequence_domain.json`
- Create: `tests/fixtures/local_l2/binance_jtousdt_buffered_replay_previous_link_mismatch.json`

- [ ] **Step 1: Add Bybit fixture**

Create `tests/fixtures/local_l2/bybit_irysusdt_rest_ws_sequence_domain.json`:

```json
{
  "venue": "bybit",
  "symbol": "IRYSUSDT",
  "ws_depth": 50,
  "rest_depth": 50,
  "current_book": {
    "status": "bootstrapping",
    "sequence": 13700598,
    "last_update_id": 13700598,
    "observed_at_ms": 0
  },
  "rest_snapshot": {
    "retCode": 0,
    "retMsg": "OK",
    "result": {
      "s": "IRYSUSDT",
      "b": [["0.02000", "1000"]],
      "a": [["0.02010", "1000"]],
      "ts": 1779302500000,
      "u": 7103120,
      "seq": 13700610,
      "cts": 1779302499999
    },
    "time": 1779302500001
  },
  "expected": {
    "old_stale_decision": true,
    "new_rest_sequence_comparable": false,
    "must_not_replay_rest_snapshot_with_ws_deltas": true
  }
}
```

- [ ] **Step 2: Add Binance replay fixture**

Create `tests/fixtures/local_l2/binance_jtousdt_buffered_replay_previous_link_mismatch.json`:

```json
{
  "venue": "binance",
  "symbol": "JTOUSDT",
  "snapshot_last_update_id": 10591999713003,
  "buffered_updates": [
    {"U": 10591999713004, "u": 10591999713004, "pu": 10591999713003, "b": [["1.0000", "10"]], "a": [["1.0010", "10"]]},
    {"U": 10591999715265, "u": 10591999715270, "pu": 10591999715264, "b": [["1.0001", "10"]], "a": [["1.0011", "10"]]}
  ],
  "expected": {
    "reason": "previous_link_mismatch",
    "expected_previous": 10591999713004,
    "observed_previous": 10591999715264
  }
}
```

- [ ] **Step 3: Write replay harness tests**

Create `tests/test_local_l2_replay_harness.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

from lightfee.marketdata.l2 import L2BookStatus
from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
from lightfee.marketdata.local_l2_venues import parse_l2_update


FIXTURES = Path(__file__).parent / "fixtures" / "local_l2"


def test_bybit_rest_snapshot_sequence_is_not_comparable_to_ws_depth50_book():
    fixture = json.loads((FIXTURES / "bybit_irysusdt_rest_ws_sequence_domain.json").read_text())
    policy = policy_for_venue("bybit")
    rt = LocalL2Runtime()
    book = rt.ensure_book("bybit", "IRYSUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    book.sequence = fixture["current_book"]["sequence"]
    book.last_update_id = fixture["current_book"]["last_update_id"]
    book.observed_at_ms = 0

    update = parse_l2_update(
        "bybit",
        fixture["rest_snapshot"],
        symbol="IRYSUSDT",
        now_ms=1779302500002,
    )

    old_decision = update.sequence < book.last_update_id

    assert old_decision is True
    assert policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE
    assert policy.rest_snapshot_sequence_comparable is False
    assert policy.replay_rest_snapshot_with_ws_deltas is False


def test_binance_previous_link_mismatch_fixture_matches_production_error():
    fixture = json.loads((FIXTURES / "binance_jtousdt_buffered_replay_previous_link_mismatch.json").read_text())
    previous = fixture["snapshot_last_update_id"]
    first = fixture["buffered_updates"][0]
    second = fixture["buffered_updates"][1]

    assert first["pu"] == previous
    previous = first["u"]
    assert second["pu"] != previous
    assert fixture["expected"]["expected_previous"] == previous
    assert fixture["expected"]["observed_previous"] == second["pu"]
```

- [ ] **Step 4: Run harness tests**

Run:

```bash
pytest tests/test_local_l2_replay_harness.py -q
```

Expected: tests pass once Task 2 exists. They freeze the old failure evidence before data-plane behavior changes.

## Task 4: Fix Bybit as WS-Snapshot-Authoritative

**Files:**
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Modify: `tests/test_local_l2_runtime.py`
- Modify: `tests/test_local_l2_ws.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LocalL2DataPlane.bootstrap_book", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LocalL2DataPlane._replay_buffered_updates", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "BybitL2WsClient.parse_depth_message", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add red data-plane tests**

Add to `tests/test_local_l2_runtime.py`:

```python
@pytest.mark.asyncio
async def test_bybit_rest_snapshot_does_not_use_cross_depth_stale_compare(make_journal):
    from lightfee.marketdata.l2 import L2BookStatus, LocalL2UpdateKind, PriceLevel, LocalL2Update
    from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
    from lightfee.marketdata.local_l2_runtime import LocalL2Runtime

    class Adapter:
        async def fetch_l2_snapshot(self, symbol, depth=50):
            return LocalL2Update(
                venue="bybit",
                symbol=symbol,
                bids=[PriceLevel(0.0200, 1000.0)],
                asks=[PriceLevel(0.0201, 1000.0)],
                sequence=7103120,
                event_time_ms=1779302500000,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            )

    rt = LocalL2Runtime()
    book = rt.ensure_book("bybit", "IRYSUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    book.sequence = 13700598
    book.last_update_id = 13700598
    book.observed_at_ms = 0
    dp = LocalL2DataPlane(rt, make_journal())

    ok = await dp.bootstrap_book("bybit", "IRYSUSDT", Adapter(), now_ms=1779302500001)

    assert ok is False
    assert rt.get_book("bybit", "IRYSUSDT").status == L2BookStatus.BOOTSTRAPPING
```

This test locks the policy: with active Bybit WS-snapshot-authoritative semantics, REST bootstrap must not falsely clear or falsely stale-loop a WS-depth book. The exact implementation may return `False` while waiting for WS snapshot; it must not append repeated stale events for cross-depth sequence comparison.

- [ ] **Step 3: Add Bybit WS snapshot reset test**

Add to `tests/test_local_l2_ws.py`:

```python
def test_bybit_ws_snapshot_is_authoritative_reset():
    dp, rt, journal = _make_data_plane()
    try:
        client = BybitL2WsClient(venue="bybit", symbol="IRYSUSDT", data_plane=dp)
        first = client.parse_depth_message({
            "topic": "orderbook.50.IRYSUSDT",
            "type": "snapshot",
            "ts": 1779302500000,
            "data": {"s": "IRYSUSDT", "b": [["0.0200", "1000"]], "a": [["0.0201", "1000"]], "u": 13700598, "seq": 7103120},
        })
        second = client.parse_depth_message({
            "topic": "orderbook.50.IRYSUSDT",
            "type": "snapshot",
            "ts": 1779302501000,
            "data": {"s": "IRYSUSDT", "b": [["0.0199", "900"]], "a": [["0.0202", "1100"]], "u": 1, "seq": 7103200},
        })

        assert first.update_kind.value == "snapshot"
        assert second.update_kind.value == "snapshot"
        assert second.sequence == 1
    finally:
        _close_data_plane(dp, journal)
```

- [ ] **Step 4: Implement Bybit policy in data-plane**

In `LocalL2DataPlane.bootstrap_book()`:

- Load `policy = policy_for_venue(venue)`.
- Before stale comparison, if `policy.bridge_mode is WS_SNAPSHOT_AUTHORITATIVE` and `policy.rest_snapshot_sequence_comparable is False`, do not compare REST `update.sequence` with `book.last_update_id`.
- If a WS client is registered for `LocalL2BookKey(venue, symbol)` and the venue policy is WS-snapshot-authoritative, return `False` from REST bootstrap while leaving the book in `BOOTSTRAPPING`/`REBUILDING` for the next WS snapshot.
- Append a bounded diagnostic event `runtime.local_l2_rest_bootstrap_deferred_for_ws_snapshot` with `venue`, `symbol`, `book_seq`, `snapshot_seq`, `policy`.
- Do not replay REST snapshot against active WS delta buffers for Bybit.

- [ ] **Step 5: Verify Bybit tests**

Run:

```bash
pytest tests/test_local_l2_runtime.py tests/test_local_l2_ws.py -k "bybit" -q
```

Expected: all Bybit local-L2 tests pass; new tests pass.

## Task 5: Fix Binance/Aster V1 Buffered Replay Parity

**Files:**
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `tests/test_local_l2_runtime.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LocalL2DataPlane.ingest_external_update", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LocalL2DataPlane._replay_buffered_updates", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add buffer cap regression**

Add to `tests/test_local_l2_runtime.py`:

```python
def test_binance_pre_snapshot_buffer_uses_v1_capacity(make_journal):
    from lightfee.marketdata.l2 import L2BookStatus, LocalL2Update, LocalL2UpdateKind, PriceLevel
    from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
    from lightfee.marketdata.local_l2_runtime import LocalL2Runtime

    rt = LocalL2Runtime()
    book = rt.ensure_book("binance", "CHIPUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    dp = LocalL2DataPlane(rt, make_journal())

    for seq in range(1, 513):
        events = dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="CHIPUSDT",
                bids=[PriceLevel(1.0, 1.0)],
                asks=[],
                sequence=seq,
                previous_sequence=seq - 1,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=seq,
        )
        assert events == []

    assert rt.get_book("binance", "CHIPUSDT").status == L2BookStatus.BOOTSTRAPPING
```

- [ ] **Step 3: Add replay previous-link mismatch test**

Add:

```python
def test_binance_buffered_replay_previous_link_mismatch_keeps_book_rebuilding(make_journal):
    from lightfee.marketdata.l2 import L2BookStatus, LocalL2Update, LocalL2UpdateKind, PriceLevel
    from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
    from lightfee.marketdata.local_l2_runtime import LocalL2Runtime

    rt = LocalL2Runtime()
    book = rt.ensure_book("binance", "JTOUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    dp = LocalL2DataPlane(rt, make_journal())

    for seq, prev in [(10591999713004, 10591999713003), (10591999715270, 10591999715264)]:
        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="JTOUSDT",
                bids=[PriceLevel(1.0, 10.0)],
                asks=[PriceLevel(1.1, 10.0)],
                sequence=seq,
                previous_sequence=prev,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=seq,
        )

    book.apply_snapshot(
        [PriceLevel(1.0, 10.0)],
        [PriceLevel(1.1, 10.0)],
        sequence=10591999713003,
        now_ms=1,
    )

    replay = dp._replay_buffered_updates("binance", "JTOUSDT")

    assert replay.ok is False
    assert "previous_link_mismatch" in rt.get_book("binance", "JTOUSDT").fault_reason
    assert rt.get_book("binance", "JTOUSDT").status == L2BookStatus.REBUILDING
```

- [ ] **Step 4: Implement V1 cap and evidence**

In `LocalL2DataPlane.ingest_external_update()`:

- Replace `_PRE_SNAPSHOT_BUFFER_CAP` usage with `policy_for_venue(update.venue).pre_snapshot_buffer_cap`.
- Binance/Aster must use `4096`.
- Keep fail-closed behavior on real overflow: clear buffer, reset sequence, mark rebuilding.
- Add evidence fields: `first_buffered_sequence`, `last_buffered_sequence`, `incoming_previous_sequence`, `incoming_sequence`, `policy_buffer_cap`.

- [ ] **Step 5: Verify replay tests**

Run:

```bash
pytest tests/test_local_l2_runtime.py -k "pre_snapshot_buffer_uses_v1_capacity or previous_link_mismatch" -q
```

Expected: tests pass.

## Task 6: Port OKX V1 Replay Classification

**Files:**
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `tests/test_local_l2_runtime.py`
- Modify: `tests/test_local_l2_ws.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LocalL2DataPlane._replay_buffered_updates", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "OkxL2WsClient.parse_depth_message", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add OKX keepalive/reset tests**

Add to `tests/test_local_l2_runtime.py` tests that:

- buffer an OKX keepalive update with `prevSeqId == seqId == previous_sequence` and empty bids/asks;
- replay does not mark sequence gap;
- buffer an OKX reset update with `prevSeqId == previous_sequence` and `seqId < prevSeqId`;
- replay accepts reset according to V1 classifier instead of previous-link mismatch.

- [ ] **Step 3: Implement classifier use**

In `_replay_buffered_updates()`:

- For OKX, use `policy.classify_replay_link(...)`.
- `OBSOLETE`: skip.
- `KEEPALIVE`: apply as freshness refresh if local book supports empty update; otherwise update observed time without changing levels.
- `RESET`: apply and set previous sequence to reset sequence.
- `NORMAL`: apply.
- `INVALID`: mark sequence gap and rebuild.

- [ ] **Step 4: Verify OKX tests**

Run:

```bash
pytest tests/test_local_l2_runtime.py tests/test_local_l2_ws.py -k "okx" -q
```

Expected: existing and new OKX tests pass.

## Task 7: Verify Bitget UTA Contract Before Editing Behavior

**Files:**
- Modify: `scripts/probe_local_l2_rebuilds.py`
- Modify: `tests/test_local_l2_ws.py` only if probe proves drift
- Modify: `lightfee/marketdata/local_l2_ws.py` only if probe proves drift

- [ ] **Step 1: Add probe coverage for Bitget subscribe schema**

In `scripts/probe_local_l2_rebuilds.py`, include a Bitget WS probe that subscribes with current V2 schema and records:

- subscribe request payload
- subscribe response
- first `action`
- `arg` fields
- `seq`
- `pseq`
- `ts`

- [ ] **Step 2: Run cloud probe**

Run from cloud or approved network environment:

```bash
python3 scripts/probe_local_l2_rebuilds.py --venue bitget --symbol BTCUSDT --duration-s 15 --json
```

Expected: probe returns either confirmed current schema or documented mismatch.

- [ ] **Step 3: If mismatch is confirmed, add red tests**

Add tests for `topic` and `channel` compatibility:

```python
def test_bitget_parser_accepts_uta_topic_field():
    dp, rt, journal = _make_data_plane()
    try:
        client = BitgetL2WsClient("bitget", "BTCUSDT", dp)
        raw = {
            "action": "snapshot",
            "arg": {"instType": "usdt-futures", "topic": "books", "symbol": "BTCUSDT"},
            "data": [{"a": [["100.1", "1"]], "b": [["100.0", "1"]], "seq": 10, "pseq": 0, "ts": "1779302500000"}],
        }
        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.sequence == 10
        assert update.previous_sequence == 0
    finally:
        _close_data_plane(dp, journal)
```

- [ ] **Step 4: Implement only if red test proves drift**

Update `BitgetL2WsClient` to accept both docs UTA keys and any still-working legacy keys:

- subscribe arg should match endpoint family selected by transport/spec;
- parser should accept `arg.topic == "books"` and `arg.channel == "books"`;
- symbol should accept `arg.symbol` and `arg.instId`.

- [ ] **Step 5: Verify**

Run:

```bash
pytest tests/test_local_l2_ws.py -k "bitget" -q
```

Expected: all Bitget WS tests pass.

## Task 8: Gate Channel Decision Requires Probe Evidence

**Files:**
- Modify: `scripts/probe_local_l2_rebuilds.py`
- Modify: `tests/test_local_l2_ws.py` only if Gate becomes target
- Modify: `lightfee/marketdata/local_l2_ws.py` only if Gate becomes target

- [ ] **Step 1: Add Gate schema probe**

The probe must capture both:

- legacy `futures.order_book`
- recommended `futures.order_book_update`

For `order_book_update`, record `full`, `U`, `u`, `t`, `b`, `a`.

- [ ] **Step 2: Run only if Gate appears in entry-critical blockers**

Run:

```bash
python3 scripts/probe_local_l2_rebuilds.py --venue gate --symbol BTCUSDT --duration-s 20 --json
```

Expected: schema evidence is captured. Do not change Gate code unless logs show Gate is an active blocker or probe proves current channel cannot maintain readiness.

- [ ] **Step 3: If needed, implement official `order_book_update` policy**

Only after Step 2 proves need:

- `full=true` replaces local book and sets depth id to `u`.
- Incremental update requires `U == local_depth_id + 1`.
- `u` becomes new local depth id.
- `size == 0` deletes level.
- Any gap triggers rebuild/resubscribe.

## Task 9: Add Live Public Probe Script

**Files:**
- Create: `scripts/probe_local_l2_rebuilds.py`

- [ ] **Step 1: Create CLI entrypoint and venue dispatch**

Create a script with:

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Awaitable, Callable


JsonObject = dict[str, object]
ProbeHandler = Callable[[argparse.Namespace], Awaitable[JsonObject]]
HANDLERS: dict[str, ProbeHandler] = {}


async def probe(args) -> JsonObject:
    venue = args.venue.lower()
    handler = HANDLERS.get(venue)
    if handler is None:
        return {
            "ok": False,
            "venue": venue,
            "symbol": args.symbol.upper(),
            "duration_s": args.duration_s,
            "error": f"unsupported venue: {venue}",
            "supported_venues": sorted(HANDLERS),
        }
    return await handler(args)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--venue", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--duration-s", type=float, default=20.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(probe(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Implement Bybit handler first**

The Bybit handler must:

- subscribe to `wss://stream.bybit.com/v5/public/linear` `orderbook.50.<symbol>`;
- capture first snapshot and first 20 deltas or timeout;
- fetch REST `/v5/market/orderbook?category=linear&symbol=<symbol>&limit=50`;
- output `ws_depth`, `ws_u`, `ws_seq`, `rest_u`, `rest_seq`, `old_stale_decision`, `sequence_comparable=false`.

- [ ] **Step 3: Implement Binance handler**

The Binance handler must:

- connect to `wss://fstream.binance.com/ws/<lower>@depth`;
- buffer events for the requested duration;
- fetch REST `/fapi/v1/depth`;
- compute bridge start by official `U <= lastUpdateId <= u` rule;
- output whether 512 cap would overflow and whether 4096 cap would preserve the bridge.

- [ ] **Step 4: Implement OKX handler**

The OKX handler must:

- subscribe to `books` for `<symbol>` wire id;
- capture snapshot and updates;
- classify each update as normal/keepalive/reset/obsolete/invalid using `policy_for_venue("okx")`;
- output checksum fields without logging account data.

- [ ] **Step 5: Smoke-test CLI without network in unit test**

Add a test that imports `scripts/probe_local_l2_rebuilds.py` and calls parser/format helpers without opening network sockets.

## Task 10: Update Runtime/Data-Plane Diagnostics for True Closure

**Files:**
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `tests/test_local_l2_runtime.py`

- [ ] **Step 1: Add structured evidence tests**

Test that overflow/replay/stale events include:

- `venue`
- `symbol`
- `policy`
- `bridge_mode`
- `buffered_count`
- `snapshot_seq`
- `book_seq`
- `first_buffered_sequence`
- `last_buffered_sequence`
- `reason_class`

- [ ] **Step 2: Implement evidence payloads**

Extend `_rebuild_evidence()` and snapshot stale logging without changing unrelated journal schemas.

- [ ] **Step 3: Verify diagnostics tests**

Run:

```bash
pytest tests/test_local_l2_runtime.py -k "evidence or stale or overflow or replay" -q
```

Expected: evidence fields are present and bounded.

## Task 11: Local Verification Gate

**Files:**
- No source files unless failures expose direct defects.

- [ ] **Step 1: Run targeted suite**

Run:

```bash
pytest tests/test_local_l2_policy.py tests/test_local_l2_replay_harness.py tests/test_local_l2_runtime.py tests/test_local_l2_ws.py tests/test_local_l2_venue_rules.py tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py -q
```

Expected: all pass.

- [ ] **Step 2: Run compile and whitespace checks**

Run:

```bash
python3 -m compileall -q lightfee scripts
git diff --check
```

Expected: both pass.

- [ ] **Step 3: Run full suite**

Run:

```bash
pytest -q
```

Expected: full suite passes. Do not mark fixed if only targeted tests pass.

- [ ] **Step 4: Run GitNexus change detection**

Run:

```text
gitnexus_detect_changes({repo: "LightFeeV2", scope: "all"})
```

Expected: changed symbols and affected processes match this plan. Investigate unexpected flows.

## Task 12: Cloud Probe and Current-Run Acceptance

**Files:**
- Modify: `docs/bugs/BUG_INDEX.md`
- Modify: `docs/bugs/daily/2026-05-21.md`

- [ ] **Step 1: Deploy only after local gate passes**

Use the existing deploy workflow for `/opt/lightfee-v2`. Record deployed commit.

- [ ] **Step 2: Run cloud probes for failed symbols**

Run:

```bash
python3 scripts/probe_local_l2_rebuilds.py --venue bybit --symbol IRYSUSDT --duration-s 30 --json
python3 scripts/probe_local_l2_rebuilds.py --venue bybit --symbol CHIPUSDT --duration-s 30 --json
python3 scripts/probe_local_l2_rebuilds.py --venue binance --symbol JTOUSDT --duration-s 30 --json
python3 scripts/probe_local_l2_rebuilds.py --venue binance --symbol CHIPUSDT --duration-s 30 --json
python3 scripts/probe_local_l2_rebuilds.py --venue okx --symbol INJUSDT --duration-s 30 --json
python3 scripts/probe_local_l2_rebuilds.py --venue okx --symbol CHIPUSDT --duration-s 30 --json
```

Expected:

- Bybit probe reports REST/WS sequence domains not comparable and no false stale decision.
- Binance probe reports bridge-cap behavior and no cap-induced 512 overflow under the new policy.
- OKX probe classifies keepalive/reset correctly if observed.

- [ ] **Step 3: Verify current run only**

Run:

```bash
python3 /tmp/lightfee_remote_cmd.py "python3 - <<'PY'
import json, collections
state=json.load(open('/opt/lightfee-v2/runtime/live-state.json'))
run_id=state.get('run_id')
counts=collections.Counter()
details=[]
for line in open('/opt/lightfee-v2/runtime/live-events.jsonl'):
    e=json.loads(line)
    if e.get('run_id') != run_id:
        continue
    k=e.get('kind') or e.get('event') or e.get('type')
    p=e.get('payload') or e
    if k in {
        'runtime.local_l2_snapshot_stale',
        'runtime.local_l2_snapshot_error',
        'runtime.local_l2_buffer_overflow_rebuild',
        'runtime.local_l2_hot_stale_rebuild',
        'runtime.entry_local_l2_readiness_diagnostics',
        'runtime.local_l2_symbol_skipped',
    }:
        counts[k]+=1
        if len(details)<200:
            details.append((k,p.get('venue'),p.get('symbol'),p.get('reason'),p.get('error'),p.get('book_seq'),p.get('snapshot_seq')))
print('run_id', run_id)
print('counts', dict(counts))
for row in details:
    print(row)
PY"
```

Expected:

- No Binance `SYSUSDT`, Aster `RLSUSDT`, Hyperliquid `MAV` snapshot errors.
- No repeated Bybit `IRYSUSDT` false stale loop.
- Any remaining buffer overflow has structured evidence and is not caused by the old 512 cap.
- Entry-critical primary pairs are either dual-ready or have per-leg evidence.

- [ ] **Step 4: Observe at least one real entry window or equivalent harness window**

Do not claim opening stability from idle time. Acceptance requires either:

- a real production entry-local-L2 prewarm/finalization window where primary pairs become dual-ready or produce specific per-leg root reasons; or
- a harness run with previously failing symbols that exercises bootstrap -> ready/rebuild -> readiness diagnostics end-to-end.

- [ ] **Step 5: Update bug ledger status**

Only after Step 4:

- mark Bybit sequence-domain drift fixed if cloud/harness proves no recurrence;
- mark buffer overflow cap/replay drift fixed only for Binance/Aster class if V1 parity and harness prove it;
- leave OKX/Bitget/Gate/Hot-stale residuals open if no entry-critical evidence exercised them;
- record exact commit, current run_id, probe command outputs, and remaining watch terms.
