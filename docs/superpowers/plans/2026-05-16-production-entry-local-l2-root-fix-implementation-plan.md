# Production Entry Local-L2 Root Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Root-fix the 2026-05-15+ production no-entry condition by driving entry local-L2 session readiness from real local-L2 books, restoring V1-grade no-entry diagnostics, and preparing the exchange order path for ACK/fill/reconcile hardening.

**Architecture:** Keep local-L2 book ownership in `lightfee/marketdata/*`, entry session readiness in `lightfee/engine/entry_local_l2.py`, and orchestration in `lightfee/engine/runtime.py`. The first phase is behavior-preserving except that healthy HOT books can now satisfy the existing safety gate; diagnostics are structured, sampled, and V1-compatible.

**Tech Stack:** Python 3.11/3.12, pytest, pytest-asyncio, LightFeeV2 journal/current-state, V1 Rust references from `/media/wl/新加卷/codex/LightFee`, official exchange REST/WS docs.

---

## Required Context

- Spec: `docs/superpowers/specs/2026-05-16-production-entry-local-l2-root-fix-design.md`
- Bug ledger: `docs/bugs/daily/2026-05-16.md`
- Prior related plan: `docs/superpowers/plans/2026-05-13-v2-exchange-local-l2-root-fix-implementation-plan.md`
- V1 no-entry diagnostics: `/media/wl/新加卷/codex/LightFee/src/execution_core/market_data.rs:4840`
- V1 local-L2 tracking: `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_tracking.rs`
- V1 entry local-L2 sessions: `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2_sessions.rs`
- V2 gate: `lightfee/engine/runtime.py:2606`
- V2 sessions: `lightfee/engine/entry_local_l2.py`
- V2 local-L2 runtime/book: `lightfee/marketdata/local_l2_runtime.py`, `lightfee/marketdata/l2.py`

Before editing any function, class, or method, follow `AGENTS.md`: run GitNexus impact analysis for the target symbol, report risk, then write tests first.

## File Structure

| Path | Responsibility |
|---|---|
| `lightfee/engine/entry_local_l2.py` | Add a pure readiness mapping from `LocalL2Book` to entry leg/session state; expose diagnostics snapshots with stable reason/detail fields |
| `lightfee/engine/runtime.py` | Call the readiness sync after local-L2 data sync and before selection blocker checks; emit sampled session state diagnostics |
| `lightfee/marketdata/l2.py` | No broad rewrite; only add tiny read-only helpers if tests prove existing fields are insufficient |
| `lightfee/offline/analysis/journal.py` | Add summarization support for new diagnostics if existing offline analysis misses them |
| `scripts/analyze_production_blockers.py` | Add a local/remote-log analyzer for post-5.15 blocker frequency and local-L2 readiness reasons |
| `tests/test_entry_local_l2.py` | Unit tests for pure readiness mapping, stale/missing/degraded/crossed book reasons, and diagnostics snapshots |
| `tests/test_runtime_maker_event_local_l2.py` | Runtime tests proving fresh HOT books unblock existing selection gate and missing/stale books remain blocked |
| `tests/test_offline_analysis.py` | Regression tests for blocker summaries and no-entry diagnostics parsing |
| `tests/fixtures/journals/production_entry_l2_blockers_20260515.jsonl` | Small synthetic fixture, not raw cloud logs, representing the 5.15+ blocker mix |

## Task 1: Lock the Production Failure as Tests

**Files:**
- Modify: `tests/test_entry_local_l2.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "EntryLocalL2Session", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime._entry_local_l2_selection_blocker", direction: "upstream", repo: "LightFeeV2"})
```

Expected: direct test/runtime callers are identified. If risk is HIGH/CRITICAL, stop and report before editing.

- [ ] **Step 2: Add a red test proving tracking alone is not readiness**

Add or keep this behavior in `tests/test_entry_local_l2.py`:

```python
def test_track_opportunity_does_not_mark_legs_ready():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2SessionRuntime,
        TrackedOpportunity,
        TrackedOpportunityClass,
    )

    runtime = EntryLocalL2SessionRuntime()
    opp = TrackedOpportunity(
        pair_id="btcusdt:binance->bybit",
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="bybit",
        ranking_edge_bps=12.5,
        class_=TrackedOpportunityClass.PRIMARY,
    )

    session = runtime.track_opportunity(opp, now_ms=10_000)

    assert not session.both_legs_ready(now_ms=10_000, stale_after_ms=300_000)
    assert session.ready_leg_count(now_ms=10_000, stale_after_ms=300_000) == 0
```

- [ ] **Step 3: Add a red runtime test for HOT fresh books unblocking selection**

Add to `tests/test_runtime_maker_event_local_l2.py` using the existing runtime fixture style in that file:

```python
@pytest.mark.asyncio
async def test_runtime_selection_unblocks_after_entry_l2_books_are_hot(runtime_with_candidates):
    from lightfee.marketdata.l2 import L2BookStatus, PriceLevel

    runtime = runtime_with_candidates
    runtime.config.strategy.local_l2_enabled = True
    runtime.config.strategy.entry_local_l2_prewarm_window_secs = 480
    runtime.config.strategy.entry_local_l2_primary_count = 1

    candidate = runtime._test_candidate(
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="bybit",
        first_funding_timestamp_ms=10_000 + 60_000,
    )
    runtime.entry_l2_sessions.track_opportunity(
        runtime._test_tracked_opportunity(candidate, class_="primary_tracked"),
        now_ms=10_000,
    )
    runtime._tracked_primary_pair_ids = {"btcusdt:binance->bybit"}

    for venue in ("binance", "bybit"):
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.bids = [PriceLevel(price=50_000.0, quantity=1.0)]
        book.asks = [PriceLevel(price=50_001.0, quantity=1.0)]
        book.observed_at_ms = 10_000
        book.status = L2BookStatus.HOT

    runtime._refresh_entry_l2_session_readiness(now_ms=10_000)

    assert runtime._entry_local_l2_selection_blocker(candidate, now_ms=10_000) is None
```

If fixture helpers differ, create local helper dataclasses in the test file rather than loosening the assertion.

- [ ] **Step 4: Run the new tests and confirm failure**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py -k "track_opportunity_does_not_mark_legs_ready or selection_unblocks_after_entry_l2_books_are_hot" -q
```

Expected before implementation: tracking test passes; runtime HOT-book test fails because `_refresh_entry_l2_session_readiness` does not exist or does not mark legs ready.

## Task 2: Add Pure Book-to-Leg Readiness Mapping

**Files:**
- Modify: `lightfee/engine/entry_local_l2.py`
- Modify: `tests/test_entry_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "EntryLocalL2LegSession.mark_ready", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "EntryLocalL2LegSession.mark_faulted", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add unit tests for readiness reasons**

Add these tests to `tests/test_entry_local_l2.py`; adjust import grouping only if the file already has a stricter local style.

```python
def _entry_l2_test_book(status, observed_at_ms=10_000, bid=50_000.0, ask=50_001.0):
    from lightfee.marketdata.l2 import LocalL2Book, PriceLevel

    book = LocalL2Book(venue="binance", symbol="BTCUSDT")
    book.status = status
    book.observed_at_ms = observed_at_ms
    book.bids = [PriceLevel(price=bid, quantity=1.0)]
    book.asks = [PriceLevel(price=ask, quantity=1.0)]
    book.sequence = 123
    return book


def test_apply_book_readiness_marks_ready_for_hot_fresh_book():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2LegSession,
        EntryLocalL2LegState,
        apply_book_readiness_to_leg,
    )
    from lightfee.marketdata.l2 import L2BookStatus

    leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
    result = apply_book_readiness_to_leg(
        leg,
        _entry_l2_test_book(L2BookStatus.HOT),
        now_ms=10_100,
        stale_after_ms=300_000,
    )

    assert result.ready is True
    assert result.reason == "ready"
    assert leg.state == EntryLocalL2LegState.READY


def test_apply_book_readiness_reports_book_missing():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2LegSession,
        EntryLocalL2LegState,
        SessionArmingReason,
        apply_book_readiness_to_leg,
    )

    leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
    result = apply_book_readiness_to_leg(
        leg,
        None,
        now_ms=10_100,
        stale_after_ms=300_000,
    )

    assert result.ready is False
    assert result.reason == "book_missing"
    assert result.detail == "local_l2_book_missing"
    assert leg.state == EntryLocalL2LegState.ARMING
    assert leg.arming_reason == SessionArmingReason.FIRST_SESSION


def test_apply_book_readiness_reports_stale_book():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2LegFault,
        EntryLocalL2LegSession,
        EntryLocalL2LegState,
        apply_book_readiness_to_leg,
    )
    from lightfee.marketdata.l2 import L2BookStatus

    leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
    result = apply_book_readiness_to_leg(
        leg,
        _entry_l2_test_book(L2BookStatus.HOT, observed_at_ms=1_000),
        now_ms=10_100,
        stale_after_ms=5_000,
    )

    assert result.ready is False
    assert result.reason == "stale_book"
    assert "age_ms=9100" in result.detail
    assert leg.state == EntryLocalL2LegState.FAULTED
    assert leg.fault == EntryLocalL2LegFault.STALE_BOOK


def test_apply_book_readiness_reports_crossed_book():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2LegFault,
        EntryLocalL2LegSession,
        EntryLocalL2LegState,
        apply_book_readiness_to_leg,
    )
    from lightfee.marketdata.l2 import L2BookStatus

    leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
    result = apply_book_readiness_to_leg(
        leg,
        _entry_l2_test_book(L2BookStatus.HOT, bid=50_002.0, ask=50_001.0),
        now_ms=10_100,
        stale_after_ms=300_000,
    )

    assert result.ready is False
    assert result.reason == "crossed_or_locked_book"
    assert leg.state == EntryLocalL2LegState.FAULTED
    assert leg.fault == EntryLocalL2LegFault.CROSSED_OR_LOCKED_BOOK


def test_apply_book_readiness_reports_degraded_book():
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2LegFault,
        EntryLocalL2LegSession,
        EntryLocalL2LegState,
        apply_book_readiness_to_leg,
    )
    from lightfee.marketdata.l2 import L2BookStatus

    leg = EntryLocalL2LegSession(venue="binance", symbol="BTCUSDT")
    book = _entry_l2_test_book(L2BookStatus.DEGRADED)
    book.fault_reason = "snapshot_bootstrap: timeout"
    result = apply_book_readiness_to_leg(
        leg,
        book,
        now_ms=10_100,
        stale_after_ms=300_000,
    )

    assert result.ready is False
    assert result.reason == "book_degraded"
    assert "snapshot_bootstrap: timeout" in result.detail
    assert leg.state == EntryLocalL2LegState.FAULTED
    assert leg.fault == EntryLocalL2LegFault.RUNTIME_SUSPENDED
```

Each test asserts state plus stable reason/detail; do not replace these with broad truthy checks.

- [ ] **Step 3: Implement a pure helper**

Add to `lightfee/engine/entry_local_l2.py`:

```python
@dataclass(frozen=True)
class EntryLocalL2LegReadiness:
    venue: str
    symbol: str
    ready: bool
    reason: str
    detail: str
    book_status: str
    age_ms: int
    observed_at_ms: int
    sequence: int


def apply_book_readiness_to_leg(
    leg: EntryLocalL2LegSession,
    book,
    now_ms: int,
    stale_after_ms: int,
) -> EntryLocalL2LegReadiness:
    if book is None:
        leg.mark_arming(SessionArmingReason.FIRST_SESSION)
        return EntryLocalL2LegReadiness(
            venue=leg.venue,
            symbol=leg.symbol,
            ready=False,
            reason="book_missing",
            detail="local_l2_book_missing",
            book_status="missing",
            age_ms=0,
            observed_at_ms=0,
            sequence=0,
        )

    age_ms = book.age_ms(now_ms)
    status = getattr(book.status, "value", str(book.status))
    sequence = int(getattr(book, "sequence", 0) or getattr(book, "last_update_id", 0) or 0)

    if book.has_crossed_book():
        leg.mark_faulted(
            EntryLocalL2LegFault.CROSSED_OR_LOCKED_BOOK,
            f"status={status} bid={book.best_bid()} ask={book.best_ask()}",
            seen_at_ms=getattr(book, "observed_at_ms", 0),
        )
        return EntryLocalL2LegReadiness(leg.venue, leg.symbol, False, "crossed_or_locked_book", leg.fault_detail, status, age_ms, book.observed_at_ms, sequence)

    if book.is_ready(stale_after_ms, now_ms):
        leg.mark_ready(seen_at_ms=book.observed_at_ms or now_ms)
        return EntryLocalL2LegReadiness(leg.venue, leg.symbol, True, "ready", f"status={status} age_ms={age_ms}", status, age_ms, book.observed_at_ms, sequence)

    if book.is_stale(stale_after_ms, now_ms):
        leg.mark_faulted(
            EntryLocalL2LegFault.STALE_BOOK,
            f"status={status} age_ms={age_ms} stale_after_ms={stale_after_ms}",
            seen_at_ms=getattr(book, "observed_at_ms", 0),
        )
        return EntryLocalL2LegReadiness(leg.venue, leg.symbol, False, "stale_book", leg.fault_detail, status, age_ms, book.observed_at_ms, sequence)

    if status in {"rebuilding", "resume_waiting", "bootstrapping", "cold"}:
        leg.mark_arming(SessionArmingReason.BOOK_STATUS_TRANSITION)
        return EntryLocalL2LegReadiness(leg.venue, leg.symbol, False, f"book_{status}", f"status={status} age_ms={age_ms}", status, age_ms, book.observed_at_ms, sequence)

    leg.mark_faulted(
        EntryLocalL2LegFault.RUNTIME_SUSPENDED,
        f"status={status} fault={getattr(book, 'fault_reason', '') or getattr(book, 'last_error', '')}",
        seen_at_ms=getattr(book, "observed_at_ms", 0),
    )
    return EntryLocalL2LegReadiness(leg.venue, leg.symbol, False, f"book_{status}", leg.fault_detail, status, age_ms, book.observed_at_ms, sequence)
```

If line length or constructor style needs adjustment, keep the fields and reason strings exactly stable.

- [ ] **Step 4: Run focused tests**

Run:

```bash
pytest tests/test_entry_local_l2.py -k "apply_book_readiness or track_opportunity_does_not_mark_legs_ready" -q
```

Expected: all new readiness mapping tests pass.

## Task 3: Wire Readiness Sync into Live Runtime

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LiveRuntime.tick", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime._entry_local_l2_selection_blocker", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add `_refresh_entry_l2_session_readiness`**

Add a `LiveRuntime` method:

```python
def _refresh_entry_l2_session_readiness(self, now_ms: int) -> list[dict]:
    from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

    stale_after_ms = int(getattr(self.config.strategy, "entry_local_l2_book_stale_after_ms", 300_000) or 300_000)
    diagnostics: list[dict] = []
    for session in self.entry_l2_sessions.sessions.values():
        for leg in session.legs.values():
            book = self.local_l2_runtime.get_book(leg.venue, leg.symbol)
            readiness = apply_book_readiness_to_leg(leg, book, now_ms, stale_after_ms)
            diagnostics.append({
                "pair_id": session.pair_id,
                "venue": readiness.venue,
                "symbol": readiness.symbol,
                "ready": readiness.ready,
                "reason": readiness.reason,
                "detail": readiness.detail,
                "book_status": readiness.book_status,
                "age_ms": readiness.age_ms,
                "observed_at_ms": readiness.observed_at_ms,
                "sequence": readiness.sequence,
            })
        session.refresh_state(now_ms, stale_after_ms)
    return diagnostics
```

- [ ] **Step 3: Call the method before selection blocker checks**

In `LiveRuntime.tick`, after tracked opportunities are refreshed and after `_sync_local_l2_data()`, call:

```python
self._refresh_entry_l2_session_readiness(now_ms)
```

The call must happen before iterating `tradeable` candidates and before `_entry_local_l2_selection_blocker()`.

- [ ] **Step 4: Emit sampled diagnostics for not-ready primary pairs**

When no entries are dispatched and local-L2 is enabled, append a compact event:

```python
self.journal.append(
    "runtime.entry_local_l2_readiness_diagnostics",
    {
        "primary_pair_ids": sorted(self._tracked_primary_pair_ids),
        "not_ready": [item for item in diagnostics if not item["ready"]][:24],
        "ts_ms": now_ms,
    },
)
```

Use fingerprint/sampling if this event becomes too noisy in existing tests.

- [ ] **Step 5: Run focused runtime tests**

Run:

```bash
pytest tests/test_runtime_maker_event_local_l2.py tests/test_entry_local_l2.py -k "selection_unblocks_after_entry_l2_books_are_hot or local_l2 or readiness" -q
```

Expected: HOT fresh books unblock selection; missing/stale/degraded books remain blocked with stable diagnostics.

## Task 4: Port V1 No-Entry Diagnostics Shape

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_entry_local_l2.py`
- Modify: `tests/test_offline_analysis.py`

- [ ] **Step 1: Add tests for `scan.no_entry_diagnostics`**

Create tests asserting payload contains:

```python
required_keys = {
    "reason",
    "candidate_count",
    "tradeable_count",
    "selected_candidate_count",
    "remaining_slots",
    "blocked_reason_counts",
    "tradeable_selection_blocker_counts",
    "entry_local_l2_primary_ready_filter_active",
    "entry_local_l2_primary_not_ready_reason_counts",
    "entry_local_l2_primary_not_ready_reason_totals",
    "entry_local_l2_primary_not_ready_detail_samples",
    "candidates",
}
```

Use a bounded synthetic candidate set with one `book_missing`, one `stale_book`, and one outside prewarm window.

- [ ] **Step 2: Implement a small diagnostic builder**

Add a runtime helper such as:

```python
def _build_no_entry_diagnostics(self, candidates, selected, now_ms: int) -> dict:
    def count_values(values):
        counts = {}
        for value in values:
            counts[value] = counts.get(value, 0) + 1
        return counts

    selected_ids = {
        getattr(candidate, "pair_id", "")
        for candidate in selected
        if getattr(candidate, "pair_id", "")
    }
    readiness_items = getattr(self, "_last_entry_l2_readiness_diagnostics", [])
    not_ready_items = [item for item in readiness_items if not item.get("ready")]
    not_ready_reasons = [item.get("reason", "unknown") for item in not_ready_items]
    not_ready_venue_reasons = [
        f"{item.get('venue', 'unknown')}:{item.get('reason', 'unknown')}"
        for item in not_ready_items
    ]

    detail_samples = {}
    for item in not_ready_items:
        key = f"{item.get('venue', 'unknown')}:{item.get('reason', 'unknown')}"
        sample = str(item.get("detail", ""))
        bucket = detail_samples.setdefault(key, [])
        if sample and sample not in bucket and len(bucket) < 3:
            bucket.append(sample)

    candidate_rows = []
    for rank, candidate in enumerate(candidates[:24], start=1):
        pair_id = getattr(candidate, "pair_id", "")
        if not pair_id:
            from lightfee.engine.entry_local_l2 import make_candidate_pair_id
            pair_id = make_candidate_pair_id(
                str(getattr(candidate, "symbol", "")),
                str(getattr(candidate, "long_venue", "")),
                str(getattr(candidate, "short_venue", "")),
            )
        first_funding_ms = int(getattr(candidate, "first_funding_timestamp_ms", 0) or 0)
        candidate_rows.append({
            "rank": rank,
            "pair_id": pair_id,
            "symbol": getattr(candidate, "symbol", ""),
            "long_venue": str(getattr(candidate, "long_venue", "")),
            "short_venue": str(getattr(candidate, "short_venue", "")),
            "selected": pair_id in selected_ids,
            "primary_tracked": pair_id in self._tracked_primary_pair_ids,
            "remaining_ms": first_funding_ms - now_ms if first_funding_ms > 0 else 0,
            "blocked_reasons": list(getattr(candidate, "blocked_reasons", [])),
        })

    blocked_reasons = []
    for candidate in candidates:
        blocked_reasons.extend(getattr(candidate, "blocked_reasons", []))

    return {
        "reason": "no_tradeable_candidates" if not selected else "tradeable_candidates_not_selected",
        "candidate_count": len(candidates),
        "tradeable_count": sum(
            1 for candidate in candidates if not getattr(candidate, "blocked", False)
        ),
        "selected_candidate_count": len(selected),
        "remaining_slots": max(
            0,
            self.config.strategy.max_concurrent_positions - len(self.state.open_positions),
        ),
        "blocked_reason_counts": count_values(blocked_reasons),
        "tradeable_selection_blocker_counts": count_values(
            row["blocked_reasons"][0] if row["blocked_reasons"] else "none"
            for row in candidate_rows
        ),
        "entry_local_l2_primary_ready_filter_active": bool(self._tracked_primary_pair_ids),
        "entry_local_l2_primary_not_ready_reason_counts": count_values(not_ready_venue_reasons),
        "entry_local_l2_primary_not_ready_reason_totals": count_values(not_ready_reasons),
        "entry_local_l2_primary_not_ready_detail_samples": detail_samples,
        "candidates": candidate_rows,
        "ts_ms": now_ms,
    }
```

It should produce V1-compatible key names and bounded candidate details. Do not serialize entire sidecar snapshots.

- [ ] **Step 3: Add fingerprint-based sampling**

Keep a small in-memory fingerprint on `LiveRuntime`:

```python
self._last_no_entry_diagnostics_fingerprint = ""
self._last_no_entry_diagnostics_cycle = 0
self._suppressed_no_entry_diagnostics_count = 0
```

Emit immediately on fingerprint change and at a fixed interval for repeats.

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_offline_analysis.py -k "no_entry_diagnostics or local_l2" -q
```

Expected: diagnostics tests pass and existing offline analysis remains green.

## Task 5: Add Production Blocker Analyzer

**Files:**
- Create: `scripts/analyze_production_blockers.py`
- Create: `tests/fixtures/journals/production_entry_l2_blockers_20260515.jsonl`
- Modify: `tests/test_offline_analysis.py`

- [ ] **Step 1: Create a tiny fixture**

The fixture must contain representative synthetic JSONL events only:

```jsonl
{"ts_ms":1778784001000,"kind":"runtime.entry_blocked_local_l2_selection","payload":{"symbol":"POLYXUSDT","pair_id":"polyxusdt:binance->hyperliquid","reason":"entry_local_l2_waiting_for_prewarm_window"}}
{"ts_ms":1778784002000,"kind":"runtime.entry_blocked_local_l2_selection","payload":{"symbol":"POLYXUSDT","pair_id":"polyxusdt:binance->hyperliquid","reason":"entry_local_l2_waiting_for_dual_ready"}}
{"ts_ms":1778784003000,"kind":"runtime.entry_local_l2_readiness_diagnostics","payload":{"not_ready":[{"pair_id":"polyxusdt:binance->hyperliquid","venue":"hyperliquid","symbol":"POLYXUSDT","reason":"book_missing","detail":"local_l2_book_missing"}]}}
{"ts_ms":1778784004000,"kind":"runtime.snapshot_degraded","payload":{"venue":"hyperliquid","symbol":"POLYXUSDT","reason":"snapshot_stale"}}
```

- [ ] **Step 2: Implement analyzer output**

`scripts/analyze_production_blockers.py` should read JSONL from a path and accept:

```bash
python3 scripts/analyze_production_blockers.py --since-ms 1778784000000 --json tests/fixtures/journals/production_entry_l2_blockers_20260515.jsonl
```

The JSON output must include:

- `event_counts`
- `entry_l2_blocker_counts`
- `top_pairs`
- `top_symbols`
- `entry_l2_not_ready_reason_counts`
- `snapshot_degraded_counts`
- `order_event_counts`

- [ ] **Step 3: Add tests**

Assert the fixture yields:

```python
assert report["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_prewarm_window"] == 1
assert report["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_dual_ready"] == 1
assert report["entry_l2_not_ready_reason_counts"]["book_missing"] == 1
assert report["order_event_counts"] == {}
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_offline_analysis.py -k "production_blocker" -q
```

Expected: analyzer fixture test passes.

## Task 6: Exchange Order Path Preflight and Follow-Up Guardrails

**Files:**
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_recovery_reconciliation.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "VenueTransport.place_order", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "OrderReconciler.reconcile_position", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add startup preflight tests**

Tests must verify:

- Hyperliquid/Aster signing dependencies are checked at startup.
- missing dependency disables only the affected venue or fails closed with a clear `startup.preflight_failed` event, according to runtime mode.
- no secret or signature appears in the event payload.

- [ ] **Step 3: Add ACK classification regression tests**

Use existing venue fixtures or add small fixtures to assert:

- ACK-only response is not `order.filled`.
- order id + client id are persisted for reconciliation.
- fill can be recognized only from filled response/private WS/query/reconcile.

- [ ] **Step 4: Add precision/sizing guard tests**

For Gate/Bybit/OKX/Bitget/Binance/Aster, assert request building logs sanitized raw and quantized price/qty fields and rejects impossible tick/step input before sending.

- [ ] **Step 5: Run venue tests**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_recovery_reconciliation.py tests/test_venues_contract.py -q
```

Expected: ACK/fill/reconcile semantics remain explicit; no live network calls.

## Task 7: End-to-End Verification

**Files:**
- No new files unless tests reveal a missing fixture.

- [ ] **Step 1: Run focused suite**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py tests/test_offline_analysis.py tests/test_venues_transport.py tests/test_recovery_reconciliation.py -q
```

Expected: all focused tests pass.

- [ ] **Step 2: Run broader regression**

Run:

```bash
pytest tests/test_venues_contract.py tests/test_runtime_entry_flow.py tests/test_engine_entry_exit.py tests/test_local_l2_runtime.py tests/test_marketdata_l2.py -q
```

Expected: no regressions in venue contracts, entry flow, local-L2 runtime, or marketdata calculations.

- [ ] **Step 3: Run GitNexus change detection**

Run:

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

Expected: affected flows are limited to entry local-L2 readiness/diagnostics and exchange order preflight/reconciliation guardrails.

- [ ] **Step 4: Production dry-run acceptance**

On the cloud host, without sending orders:

```bash
python3 scripts/analyze_production_blockers.py --since "2026-05-15T00:00:00+08:00" /opt/lightfee-v2/runtime/journal.jsonl
python3 -m lightfee.apps.probe --config /opt/lightfee-v2/config/live.toml --no-orders
```

Expected:

- sidecar snapshot fresh with 7 venues
- live `risk_mode=running`
- `entry_local_l2_waiting_for_dual_ready` either drops or has per-leg root reasons
- `order.submitted` remains 0 during dry-run probe
- startup preflight reports dependency/signing readiness without leaking secrets

## Execution Order

1. Task 1 first, to preserve the 5.15+ production failure as a regression.
2. Task 2 next, because the book-to-leg mapping is pure and testable.
3. Task 3 wires the pure mapping into runtime and should be the first behavior-changing task.
4. Task 4 adds V1-grade no-entry diagnostics so future production runs are self-explaining.
5. Task 5 makes the blocker frequency analysis repeatable.
6. Task 6 handles the next risk layer after local-L2 unblocks: exchange ACK/fill/reconcile/preflight.
7. Task 7 verifies scope and production dry-run acceptance.

## Self-Review Checklist

- The plan does not bypass local-L2 safety gates.
- Every behavior-changing task has a red test first.
- The first production fix is the readiness bridge, matching the 5.15+ evidence.
- V1-copy tasks are separated from V1/V2 shared exchange hardening.
- Official exchange docs are referenced in the spec, not re-quoted in code.
- No secret, API key, or raw cloud log is stored.
