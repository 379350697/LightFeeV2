# V1 Trading Lifecycle Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a shared V1 trading lifecycle semantic core so entry, pending-entry, close, residual, recovery, and exchange-truth gates use one funding/ownership/terminality contract and reduce avoidable quick open-then-close behavior.

**Architecture:** Add pure `FundingLifecycle` and `V1TradingLifecycle` decision layers that reuse the existing `RecoveryLedger`, `RecoveryOwnerIndex`, `PendingEntryTerminalizer`, and V1 close helpers. Runtime remains the orchestrator: it asks for decisions, writes journal evidence, and executes existing entry/close/recovery intents without owning duplicate V1 semantics.

**Tech Stack:** Python 3.12, pytest, dataclasses, existing LightFeeV2 runtime modules, existing V1 parity contracts, GitNexus impact/detect-changes, existing production diagnose and verification scripts.

---

## Global Rules

- Start production work read-only. Do not submit orders, cancel orders, edit runtime state, or deploy while collecting evidence.
- Before modifying any function, class, or method, run GitNexus impact analysis for the target symbol. Private-symbol failures must be recorded as manual runtime hot-path risk.
- Docs-only edits do not require symbol impact analysis.
- Every behavior change starts with a failing test that demonstrates the current scattered V2 lifecycle gap.
- Do not add symbol-specific branches for `MAGMAUSDT`, `MEUUSDT`, `SEIUSDT`, `TRXUSDT`, or any future recurrence.
- Do not make close slower or less V1-compatible to reduce quick-flat counts.
- Entry must not call `standard_close_reason()` or import close executors.

## Target Files

Create:

- `lightfee/engine/funding_lifecycle.py`
- `lightfee/engine/v1_lifecycle.py`
- `tests/engine/test_funding_lifecycle.py`
- `tests/engine/test_v1_trading_lifecycle.py`
- `tests/live_harness/test_quick_flat_lifecycle_incidents.py`

Modify:

- `lightfee/config/schema.py`
- `lightfee/config/validation.py`
- `config/example.toml`
- `config/live.example.toml`
- `lightfee/strategy/discovery.py`
- `lightfee/engine/exit_decision.py`
- `lightfee/engine/runtime.py`
- `lightfee/engine/pending_entry_terminalizer.py`
- `scripts/diagnose_live.py`
- `lightfee/offline/analysis/journal.py`
- `tests/test_config.py`
- `tests/test_exit_decisions.py`
- `tests/test_runtime_entry_flow.py`
- `tests/test_entry_local_l2.py`
- `tests/test_pending_entry_v1_semantic_drift.py`
- `tests/test_live_entry_hedge_root_fix.py`
- `tests/test_diagnose_live.py`
- `tests/offline/test_journal_analysis_semantics.py`
- `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- `docs/bugs/cards/pending-entry-terminality-live-truth.md`
- `docs/bugs/BUG_INDEX.md`
- `docs/bugs/daily/2026-06-05.md`

## Task 1: Add Funding Horizon Config And RED Tests

**Files:**

- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/config/validation.py`
- Modify: `config/example.toml`
- Modify: `config/live.example.toml`
- Test: `tests/test_config.py`
- Test: `tests/engine/test_funding_lifecycle.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol StrategyConfig
```

Expected: high or broad config impact. Treat config as shared runtime surface.

- [ ] **Step 2: Write RED config tests**

Add tests:

```python
def test_strategy_config_defaults_first_funding_horizon_floor_to_60s():
    from lightfee.config.schema import StrategyConfig

    cfg = StrategyConfig()

    assert cfg.entry_min_first_funding_remaining_secs == 60


def test_strategy_config_rejects_negative_first_funding_horizon():
    from lightfee.config.schema import AppConfig
    from lightfee.config.validation import validate_config

    cfg = AppConfig()
    cfg.strategy.entry_min_first_funding_remaining_secs = -1

    issues = validate_config(cfg)

    assert any("entry_min_first_funding_remaining_secs" in issue for issue in issues)
```

- [ ] **Step 3: Run RED config tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_config.py::test_strategy_config_defaults_first_funding_horizon_floor_to_60s tests/test_config.py::test_strategy_config_rejects_negative_first_funding_horizon
```

Expected before implementation: first test fails because the config field does
not exist.

- [ ] **Step 4: Add config field and validation**

Add to `StrategyConfig`:

```python
entry_min_first_funding_remaining_secs: int = 60
```

Add validation:

```python
if config.strategy.entry_min_first_funding_remaining_secs < 0:
    issues.append(
        "strategy.entry_min_first_funding_remaining_secs must be >= 0"
    )
```

Add to both TOML examples:

```toml
entry_min_first_funding_remaining_secs = 60
```

- [ ] **Step 5: Run config tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_config.py
```

Expected: pass.

## Task 2: Create Pure FundingLifecycle

**Files:**

- Create: `lightfee/engine/funding_lifecycle.py`
- Test: `tests/engine/test_funding_lifecycle.py`
- Test: `tests/test_exit_decisions.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol update_position_funding_capture_state
npx gitnexus impact --name LightFeeV2 --symbol standard_close_reason
```

Expected: close/normal-exit impact. Do not change behavior in this task beyond
introducing shared pure helpers and parity tests.

- [ ] **Step 2: Write RED FundingLifecycle tests**

Create `tests/engine/test_funding_lifecycle.py` with:

```python
from types import SimpleNamespace

from lightfee.config.schema import StrategyConfig
from lightfee.engine.funding_lifecycle import FundingLifecycle


def _candidate(first_ms: int):
    return SimpleNamespace(
        symbol="BTCUSDT",
        first_funding_timestamp_ms=first_ms,
        funding_timestamp_ms=first_ms,
        long_funding_timestamp_ms=first_ms,
        short_funding_timestamp_ms=first_ms,
        second_funding_timestamp_ms=0,
        opportunity_type="aligned",
    )


def test_entry_horizon_blocks_under_60_seconds_by_default():
    cfg = StrategyConfig()
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 59_000)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"
    assert decision.remaining_to_first_funding_ms == 59_000
    assert decision.effective_min_before_ms == 300_000


def test_entry_horizon_allows_when_remaining_meets_existing_min_scan():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 3
    cfg.entry_min_first_funding_remaining_secs = 60
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 180_000)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is True
    assert decision.reason == ""
    assert decision.effective_min_before_ms == 180_000


def test_entry_horizon_uses_60s_when_min_scan_is_zero():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 59_999)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is False
    assert decision.effective_min_before_ms == 60_000


def test_entry_horizon_blocks_missing_first_funding():
    cfg = StrategyConfig()

    decision = FundingLifecycle.entry_horizon(_candidate(0), 1_000_000, cfg)

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_missing"
```

The first test documents that default `min_scan_minutes_before_funding=5`
dominates the new 60-second floor.

- [ ] **Step 3: Run RED FundingLifecycle tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_funding_lifecycle.py
```

Expected before implementation: import failure for
`lightfee.engine.funding_lifecycle`.

- [ ] **Step 4: Implement FundingLifecycle**

Create `lightfee/engine/funding_lifecycle.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntryHorizonDecision:
    allowed: bool
    reason: str = ""
    first_funding_timestamp_ms: int = 0
    remaining_to_first_funding_ms: int = 0
    effective_min_before_ms: int = 0
    source: str = ""


class FundingLifecycle:
    @staticmethod
    def positive_ms(value: Any) -> int:
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return parsed if parsed > 0 else 0

    @classmethod
    def first_funding_ms(cls, obj: Any) -> int:
        direct = cls.positive_ms(getattr(obj, "first_funding_timestamp_ms", 0))
        if direct > 0:
            return direct
        fallback = cls.positive_ms(getattr(obj, "funding_timestamp_ms", 0))
        if fallback > 0:
            return fallback
        leg_times = [
            cls.positive_ms(getattr(obj, "long_funding_timestamp_ms", 0)),
            cls.positive_ms(getattr(obj, "short_funding_timestamp_ms", 0)),
        ]
        positives = [ts for ts in leg_times if ts > 0]
        return min(positives) if positives else 0

    @staticmethod
    def effective_entry_min_before_ms(strategy: Any) -> int:
        min_scan_ms = int(
            getattr(strategy, "min_scan_minutes_before_funding", 0) or 0
        ) * 60_000
        floor_ms = int(
            getattr(strategy, "entry_min_first_funding_remaining_secs", 60) or 0
        ) * 1_000
        return max(min_scan_ms, floor_ms, 0)

    @classmethod
    def entry_horizon(
        cls,
        obj: Any,
        now_ms: int,
        strategy: Any,
        *,
        source: str = "candidate",
    ) -> EntryHorizonDecision:
        first_ms = cls.first_funding_ms(obj)
        effective_min = cls.effective_entry_min_before_ms(strategy)
        if first_ms <= 0:
            return EntryHorizonDecision(
                allowed=False,
                reason="entry_blocked_first_funding_missing",
                first_funding_timestamp_ms=0,
                remaining_to_first_funding_ms=0,
                effective_min_before_ms=effective_min,
                source=source,
            )
        remaining = first_ms - max(int(now_ms or 0), 0)
        if remaining < effective_min:
            return EntryHorizonDecision(
                allowed=False,
                reason="entry_blocked_first_funding_too_close",
                first_funding_timestamp_ms=first_ms,
                remaining_to_first_funding_ms=remaining,
                effective_min_before_ms=effective_min,
                source=source,
            )
        return EntryHorizonDecision(
            allowed=True,
            first_funding_timestamp_ms=first_ms,
            remaining_to_first_funding_ms=remaining,
            effective_min_before_ms=effective_min,
            source=source,
        )
```

- [ ] **Step 5: Run FundingLifecycle tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_funding_lifecycle.py
```

Expected: pass.

## Task 3: Refactor Close Funding Stage To Share FundingLifecycle Facts

**Files:**

- Modify: `lightfee/engine/exit_decision.py`
- Test: `tests/test_exit_decisions.py`
- Test: `tests/engine/test_funding_lifecycle.py`

- [ ] **Step 1: Write parity tests before refactor**

Add tests that confirm close behavior does not change:

```python
def test_close_funding_capture_still_waits_until_funding_timestamp_plus_hold():
    cfg = _config(post_funding_hold_secs=30)
    pos = _position(
        funding_timestamp_ms=1_000_000,
        funding_captured=False,
        current_net_quote=0.0,
    )

    update_position_funding_capture_state(pos, 1_029_999, post_funding_hold_ms=30_000)
    assert pos.funding_captured is False

    update_position_funding_capture_state(pos, 1_030_000, post_funding_hold_ms=30_000)
    assert pos.funding_captured is True
```

Use the existing helpers in `tests/test_exit_decisions.py`.

- [ ] **Step 2: Run parity tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_exit_decisions.py
```

Expected before refactor: pass.

- [ ] **Step 3: Import FundingLifecycle without changing close reason ordering**

In `exit_decision.py`, replace local positive timestamp parsing only where it
does not change behavior:

```python
from lightfee.engine.funding_lifecycle import FundingLifecycle
```

Keep `standard_close_reason()` reason priority unchanged:

```text
hard_stop -> delay active -> aligned funding_capture -> trailing_exit ->
first_stage_capture -> second_stage_capture -> general funding_capture
```

- [ ] **Step 4: Run close and lifecycle tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_exit_decisions.py tests/engine/test_funding_lifecycle.py
```

Expected: pass. If any close reason changes, revert the refactor and keep
FundingLifecycle entry-only until a separate RED proves the close change.

## Task 4: Add V1TradingLifecycle Entry Facade

**Files:**

- Create: `lightfee/engine/v1_lifecycle.py`
- Test: `tests/engine/test_v1_trading_lifecycle.py`
- Test: `tests/engine/test_recovery_ledger.py`

- [ ] **Step 1: Run impact analysis for reused symbols**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol RecoveryLedger
npx gitnexus impact --name LightFeeV2 --symbol PendingEntryTerminalizer
npx gitnexus impact --name LightFeeV2 --symbol RecoveryOwnerIndex
```

Expected: existing recovery and runtime gate impact. Record risk before edits.

- [ ] **Step 2: Write RED facade tests**

Create tests:

```python
from types import SimpleNamespace

from lightfee.config.schema import StrategyConfig
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.v1_lifecycle import V1TradingLifecycle


def _candidate(symbol="BTCUSDT", first_ms=1_300_000):
    return SimpleNamespace(
        symbol=symbol,
        long_venue="binance",
        short_venue="bybit",
        first_funding_timestamp_ms=first_ms,
        funding_timestamp_ms=first_ms,
        long_funding_timestamp_ms=first_ms,
        short_funding_timestamp_ms=first_ms,
        blocked=False,
        blocked_reasons=[],
        entry_notional_quote=30.0,
    )


def test_entry_admissibility_blocks_recovery_ledger_before_funding_horizon():
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                }
            ],
        },
    )

    decision = V1TradingLifecycle.entry_admissibility(
        _candidate("BTCUSDT"),
        now_ms=1_000_000,
        strategy=StrategyConfig(),
        recovery_ledger=ledger,
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_recovery_ledger"


def test_entry_admissibility_blocks_first_funding_too_close():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    candidate = _candidate(first_ms=1_059_000)

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"


def test_entry_admissibility_allows_clean_candidate():
    cfg = StrategyConfig()
    candidate = _candidate(first_ms=1_300_000)

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is True
    assert decision.reason == ""
```

- [ ] **Step 3: Run RED facade tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_trading_lifecycle.py
```

Expected before implementation: import failure for `V1TradingLifecycle`.

- [ ] **Step 4: Implement facade**

Create `lightfee/engine/v1_lifecycle.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lightfee.engine.funding_lifecycle import EntryHorizonDecision, FundingLifecycle


@dataclass(frozen=True)
class LifecycleDecision:
    allowed: bool
    reason: str = ""
    evidence: dict = field(default_factory=dict)


class V1TradingLifecycle:
    @staticmethod
    def _ledger_blocks(candidate: Any, recovery_ledger: Any) -> bool:
        if recovery_ledger is None:
            return False
        if hasattr(recovery_ledger, "allows_new_entry"):
            try:
                return not bool(recovery_ledger.allows_new_entry(candidate))
            except TypeError:
                return not bool(recovery_ledger.allows_new_entries)
        return bool(getattr(recovery_ledger, "has_blocking_work", lambda: False)())

    @classmethod
    def entry_admissibility(
        cls,
        candidate: Any,
        *,
        now_ms: int,
        strategy: Any,
        recovery_ledger: Any = None,
        source: str = "candidate",
    ) -> LifecycleDecision:
        if cls._ledger_blocks(candidate, recovery_ledger):
            return LifecycleDecision(
                allowed=False,
                reason="entry_blocked_recovery_ledger",
                evidence={"source": source},
            )
        horizon = FundingLifecycle.entry_horizon(
            candidate,
            now_ms,
            strategy,
            source=source,
        )
        if not horizon.allowed:
            return LifecycleDecision(
                allowed=False,
                reason=horizon.reason,
                evidence={
                    "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
                    "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
                    "effective_min_before_ms": horizon.effective_min_before_ms,
                    "source": source,
                },
            )
        return LifecycleDecision(
            allowed=True,
            evidence={
                "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
                "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
                "effective_min_before_ms": horizon.effective_min_before_ms,
                "source": source,
            },
        )
```

- [ ] **Step 5: Run facade tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_trading_lifecycle.py tests/engine/test_recovery_ledger.py
```

Expected: pass.

## Task 5: Wire Entry Selection Through V1TradingLifecycle

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Test: `tests/test_runtime_entry_flow.py`
- Test: `tests/test_entry_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol LiveRuntime._select_entry_candidates
```

If private symbol lookup fails, manually treat `LiveRuntime._select_entry_candidates`
as entry hot-path MEDIUM/HIGH risk.

- [ ] **Step 2: Write RED selection tests**

Add to `tests/test_runtime_entry_flow.py`:

```python
def test_select_entry_candidates_blocks_first_funding_too_close(config, tmp_journal):
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    config.strategy.min_scan_minutes_before_funding = 0
    config.strategy.entry_min_first_funding_remaining_secs = 60
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 59_000,
        funding_timestamp_ms=now_ms + 59_000,
    )
    blockers = {}
    counts = Counter()

    selected = runtime._select_entry_candidates(
        [candidate],
        now_ms=now_ms,
        remaining_slots=1,
        selection_blocker_counts=counts,
        candidate_blockers=blockers,
    )

    assert selected == []
    assert counts["entry_blocked_first_funding_too_close"] == 1
```

- [ ] **Step 3: Run RED selection test**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py::test_select_entry_candidates_blocks_first_funding_too_close
```

Expected before wiring: selected contains the candidate or blocker reason is not
the lifecycle reason.

- [ ] **Step 4: Replace local funding-window decision with lifecycle call**

In `_select_entry_candidates()`, after existing tradeable checks and before
entry-readiness provider checks, call:

```python
from lightfee.engine.v1_lifecycle import V1TradingLifecycle

decision = V1TradingLifecycle.entry_admissibility(
    candidate,
    now_ms=now_ms,
    strategy=self.config.strategy,
    recovery_ledger=getattr(self, "recovery_ledger", None),
    source="selection",
)
if not decision.allowed:
    blocker = decision.reason
```

When journaling, include `decision.evidence` under `readiness_evidence` or a
new `lifecycle_evidence` key. Do not call close helpers here.

- [ ] **Step 5: Run selection/local-L2 tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py
```

Expected: pass.

## Task 6: Recheck Lifecycle At Dispatch Pre-Submit

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Test: `tests/test_runtime_entry_flow.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol LiveRuntime._dispatch_entry
```

Treat as HIGH if unresolved: this method submits entry orders.

- [ ] **Step 2: Write RED dispatch-delay test**

Add:

```python
@pytest.mark.asyncio
async def test_dispatch_entry_rechecks_first_funding_horizon_after_selection_delay(config, tmp_journal):
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    config.strategy.min_scan_minutes_before_funding = 0
    config.strategy.entry_min_first_funding_remaining_secs = 60
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.entry_executor = object()
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 59_000,
        funding_timestamp_ms=now_ms + 59_000,
    )

    dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=1.0)

    assert dispatched is False
    assert any(
        event["kind"] == "runtime.entry_blocked_lifecycle"
        and event["payload"]["reason"] == "entry_blocked_first_funding_too_close"
        for event in tmp_journal.read_all()
    )
```

- [ ] **Step 3: Run RED dispatch test**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py::test_dispatch_entry_rechecks_first_funding_horizon_after_selection_delay
```

Expected before wiring: dispatch continues past the lifecycle gate or fails for
an unrelated executor shape.

- [ ] **Step 4: Add dispatch lifecycle gate before order planning**

In `_dispatch_entry()`, after deterministic admission block and tradeable check,
before runtime entry guards and before quantity/price planning:

```python
decision = V1TradingLifecycle.entry_admissibility(
    candidate,
    now_ms=now_ms,
    strategy=self.config.strategy,
    recovery_ledger=getattr(self, "recovery_ledger", None),
    source="dispatch",
)
if not decision.allowed:
    self.journal.append(
        "runtime.entry_blocked_lifecycle",
        {
            "symbol": getattr(candidate, "symbol", ""),
            "long_venue": getattr(candidate, "long_venue", ""),
            "short_venue": getattr(candidate, "short_venue", ""),
            "reason": decision.reason,
            **decision.evidence,
            "ts_ms": now_ms,
        },
    )
    return False
```

- [ ] **Step 5: Run dispatch suite**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py::TestPlannerDispatchIntegration tests/test_runtime_entry_flow.py::test_dispatch_entry_rechecks_first_funding_horizon_after_selection_delay
```

Expected: pass.

## Task 7: Extend Pending-Entry Viability Without Dropping Exposure

**Files:**

- Modify: `lightfee/engine/v1_lifecycle.py`
- Modify: `lightfee/engine/pending_entry_terminalizer.py`
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/test_pending_entry_v1_semantic_drift.py`
- Test: `tests/test_live_entry_hedge_root_fix.py`
- Test: `tests/engine/test_v1_trading_lifecycle.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol PendingEntryTerminalizer
npx gitnexus impact --name LightFeeV2 --symbol LiveRuntime._finalize_pending_entry
```

Treat unresolved private runtime symbols as HIGH risk because this path can
convert exchange exposure into managed state.

- [ ] **Step 2: Write RED pending viability tests**

Add pure test:

```python
def test_pending_without_positive_exposure_becomes_nonviable_when_first_funding_too_close():
    pending = SimpleNamespace(
        symbol="BTCUSDT",
        first_funding_timestamp_ms=1_059_000,
        funding_timestamp_ms=1_059_000,
        long_funding_timestamp_ms=1_059_000,
        short_funding_timestamp_ms=1_059_000,
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
    )
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60

    decision = V1TradingLifecycle.pending_entry_viability(
        pending,
        now_ms=1_000_000,
        strategy=cfg,
    )

    assert decision.allowed is False
    assert decision.reason == "pending_entry_viability_first_funding_too_close"


def test_pending_positive_exposure_is_not_discarded_when_first_funding_too_close():
    pending = SimpleNamespace(
        symbol="BTCUSDT",
        first_funding_timestamp_ms=1_059_000,
        funding_timestamp_ms=1_059_000,
        long_funding_timestamp_ms=1_059_000,
        short_funding_timestamp_ms=1_059_000,
        maker_leg_filled=10.0,
        hedge_leg_filled=0.0,
    )
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60

    decision = V1TradingLifecycle.pending_entry_viability(
        pending,
        now_ms=1_000_000,
        strategy=cfg,
    )

    assert decision.allowed is True
    assert decision.reason == "pending_entry_terminality_positive_fill_recovery"
```

- [ ] **Step 3: Run RED pending tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_trading_lifecycle.py -k pending
```

Expected before implementation: missing `pending_entry_viability`.

- [ ] **Step 4: Implement pending viability**

Add to `V1TradingLifecycle`:

```python
@classmethod
def pending_entry_viability(cls, pending, *, now_ms: int, strategy) -> LifecycleDecision:
    maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
    hedge_filled = float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0)
    has_positive = maker_filled > 0 or hedge_filled > 0
    horizon = FundingLifecycle.entry_horizon(
        pending,
        now_ms,
        strategy,
        source="pending_entry",
    )
    evidence = {
        "first_funding_timestamp_ms": horizon.first_funding_timestamp_ms,
        "remaining_to_first_funding_ms": horizon.remaining_to_first_funding_ms,
        "effective_min_before_ms": horizon.effective_min_before_ms,
        "source": "pending_entry",
    }
    if not horizon.allowed and has_positive:
        return LifecycleDecision(
            allowed=True,
            reason="pending_entry_terminality_positive_fill_recovery",
            evidence=evidence,
        )
    if not horizon.allowed:
        return LifecycleDecision(
            allowed=False,
            reason="pending_entry_viability_first_funding_too_close",
            evidence=evidence,
        )
    return LifecycleDecision(allowed=True, evidence=evidence)
```

- [ ] **Step 5: Wire non-positive pending continuation paths**

In runtime branches that continue/repost/passively wait on a pending entry with
zero positive fill, ask `pending_entry_viability()` before normal continuation.
If blocked, retain pending work with backoff or route through existing terminal
no-fill/cancel/cleanup authority. Do not remove pending directly.

Journal:

```python
self.journal.append(
    "pending_entry.viability_blocked",
    {
        "entry_id": entry_id,
        "symbol": pending.symbol,
        "reason": decision.reason,
        **decision.evidence,
        "ts_ms": now_ms,
    },
)
```

- [ ] **Step 6: Run pending suites**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_trading_lifecycle.py tests/test_pending_entry_v1_semantic_drift.py tests/test_live_entry_hedge_root_fix.py
```

Expected: pass.

## Task 8: Add Production Quick-Flat Incident Replay

**Files:**

- Create: `tests/fixtures/live_incidents/2026-06-05/quick_flat_entry_close_chain.jsonl`
- Create: `tests/live_harness/test_quick_flat_lifecycle_incidents.py`
- Modify: `docs/bugs/contracts/pending-entry-live-truth-contract.md`

- [ ] **Step 1: Create sanitized quick-flat event fixture**

Use this shape:

```json
{"ts_ms":1780163908000,"kind":"execution.entry_selected","payload":{"entry_id":"entry-quick-flat","symbol":"MAGMAUSDT","first_funding_timestamp_ms":1780163940000,"funding_timestamp_ms":1780163940000}}
{"ts_ms":1780163910000,"kind":"entry.opened","payload":{"position_id":"entry-quick-flat","symbol":"MAGMAUSDT","funding_timestamp_ms":1780163940000,"opportunity_type":"aligned","exit_after_first_stage":false}}
{"ts_ms":1780163940000,"kind":"runtime.funding_capture_state_updated","payload":{"position_id":"entry-quick-flat","symbol":"MAGMAUSDT","funding_captured_before":false,"funding_captured_after":true}}
{"ts_ms":1780163940001,"kind":"runtime.normal_close_routing_passive","payload":{"position_id":"entry-quick-flat","reason":"funding_capture"}}
{"ts_ms":1780163940100,"kind":"exit.closed","payload":{"position_id":"entry-quick-flat","reason":"funding_capture","close_id":"close-quick-flat"}}
```

- [ ] **Step 2: Write RED replay test**

Add:

```python
def test_quick_flat_entry_chain_would_have_been_blocked_by_lifecycle_horizon():
    events = load_jsonl_fixture("quick_flat_entry_close_chain.jsonl")
    selected = next(event for event in events if event["kind"] == "execution.entry_selected")
    payload = selected["payload"]
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60
    candidate = SimpleNamespace(
        symbol=payload["symbol"],
        first_funding_timestamp_ms=payload["first_funding_timestamp_ms"],
        funding_timestamp_ms=payload["funding_timestamp_ms"],
        long_venue="binance",
        short_venue="bybit",
    )

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=selected["ts_ms"],
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"
```

- [ ] **Step 3: Run replay test**

Run:

```bash
.venv/bin/python -m pytest -q tests/live_harness/test_quick_flat_lifecycle_incidents.py
```

Expected after lifecycle facade: pass.

- [ ] **Step 4: Update contract docs**

Add a lifecycle row to `docs/bugs/contracts/pending-entry-live-truth-contract.md`:

```markdown
| LC-01 | candidate or pending normal-entry work is inside the effective first-funding minimum horizon with no positive exposure | block normal entry risk with `entry_blocked_first_funding_too_close` or `pending_entry_viability_first_funding_too_close` | submit new maker/hedge risk that close lane will immediately capture |
```

## Task 9: Deduplicate Quick-Flat Observability

**Files:**

- Modify: `scripts/diagnose_live.py`
- Modify: `lightfee/offline/analysis/journal.py`
- Test: `tests/test_diagnose_live.py`
- Test: `tests/offline/test_journal_analysis_semantics.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol analyze_journal_records
```

Also identify the diagnose helper that counts entry/close events and record
manual impact if private-symbol lookup fails.

- [ ] **Step 2: Write RED duplicate close count tests**

Add a diagnose/offline test with two `exit.closed` records for the same
`position_id`, `reason`, `close_id`, and `ts_ms`:

```python
def test_quick_flat_close_count_deduplicates_double_exit_closed_projection():
    events = [
        {"ts_ms": 1000, "kind": "entry.opened", "payload": {"position_id": "p1", "symbol": "BTCUSDT"}},
        {"ts_ms": 1500, "kind": "exit.closed", "payload": {"position_id": "p1", "reason": "funding_capture", "close_id": "c1"}},
        {"ts_ms": 1500, "kind": "exit.closed", "payload": {"position_id": "p1", "reason": "funding_capture", "close_id": "c1"}},
    ]

    summary = summarize_quick_flat_events(events, quick_flat_window_ms=60_000)

    assert summary["quick_flat_count"] == 1
    assert summary["duplicate_event_count"] == 1
```

- [ ] **Step 3: Implement dedup helper**

Add a pure helper in the module that owns quick-flat diagnostics:

```python
def quick_flat_event_key(event: dict) -> tuple:
    payload = event.get("payload", {}) or {}
    return (
        payload.get("position_id", ""),
        payload.get("reason", ""),
        payload.get("close_id", ""),
        int(event.get("ts_ms", 0) or 0),
    )
```

If `close_id` is missing, use `(position_id, reason, ts_ms)` and mark evidence
quality as lower confidence.

- [ ] **Step 4: Run diagnose/offline tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_diagnose_live.py tests/offline/test_journal_analysis_semantics.py
```

Expected: pass.

## Task 10: Remove Runtime Duplicate Lifecycle Branches

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Test: `tests/test_runtime_entry_flow.py`
- Test: `tests/test_entry_local_l2.py`
- Test: `tests/test_pending_entry_v1_semantic_drift.py`

- [ ] **Step 1: Inventory duplicate lifecycle checks**

Run:

```bash
rg -n "first_funding_timestamp_ms|entry_finalization_window|funding_timestamp_ms <= 0|pending_entries\\.pop\\(|standard_close_reason|force_close_due" lightfee/engine/runtime.py
```

Expected: identify local branches that should now call `FundingLifecycle`,
`V1TradingLifecycle`, `PendingEntryTerminalizer`, or `RecoveryLedger`.

- [ ] **Step 2: Convert only duplicate entry-horizon branches**

Keep V1 finalization-window labels, but source the timestamp facts from
`FundingLifecycle`. Runtime should only adapt evidence to journal payloads.

Allowed pattern:

```python
decision = V1TradingLifecycle.entry_admissibility(...)
if not decision.allowed:
    blocker = decision.reason
```

Disallowed pattern:

```python
if candidate.first_funding_timestamp_ms - now_ms < 60_000:
    ...
```

- [ ] **Step 3: Add static bypass test**

Add a test that reads `lightfee/engine/runtime.py` and rejects direct 60-second
funding horizon literals outside `FundingLifecycle`:

```python
def test_runtime_does_not_embed_first_funding_60s_horizon_literal():
    source = Path("lightfee/engine/runtime.py").read_text()

    assert "60_000" not in source
    assert "entry_min_first_funding_remaining_secs" not in source
```

Runtime may pass strategy config to the lifecycle facade, but must not compute
the rule itself.

- [ ] **Step 4: Run runtime suites**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py tests/test_pending_entry_v1_semantic_drift.py
```

Expected: pass.

## Task 11: Update Bug Docs And Daily Ledger

**Files:**

- Modify: `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- Modify: `docs/bugs/cards/pending-entry-terminality-live-truth.md`
- Modify: `docs/bugs/BUG_INDEX.md`
- Modify: `docs/bugs/daily/2026-06-05.md`

- [ ] **Step 1: Document lifecycle contract**

Add a new section to the contract:

```markdown
## Lifecycle Core Rows

| ID | Evidence shape | Required decision | Must not happen |
|---|---|---|---|
| LC-01 | candidate or pending normal-entry work is inside the effective first-funding minimum horizon with no positive exposure | block normal entry risk with stable lifecycle evidence | submit new maker/hedge risk that close lane will immediately capture |
| LC-02 | positive fill or live exposure already exists inside the funding horizon | own, recover, residualize, close, or fail-closed with evidence | discard exposure or call local flat |
| LC-03 | quick-flat report sees duplicate `exit.closed` projections for one close identity | count one real quick flat and one duplicate observation | inflate quick-flat frequency |
```

- [ ] **Step 2: Update recurrence checklist**

Add checklist items:

```markdown
17. For quick-flat reports, join `execution.entry_selected`, `entry.opened`,
    `runtime.funding_capture_state_updated`, `runtime.normal_close_routing_*`,
    and `exit.closed` by `position_id`.
18. Deduplicate duplicate `exit.closed` projections before judging frequency.
19. Classify each quick flat as bug, avoidable timing, unavoidable recovery, or
    duplicate observation.
```

- [ ] **Step 3: Run docs grep sanity**

Run:

```bash
rg -n "LC-01|entry_blocked_first_funding_too_close|quick-flat|quick flat" docs/bugs docs/superpowers
```

Expected: new docs reference the lifecycle rows and stable reasons.

## Task 12: Verification And Detect Changes

**Files:**

- No new source changes beyond previous tasks.

- [ ] **Step 1: Run focused lifecycle suites**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_funding_lifecycle.py tests/engine/test_v1_trading_lifecycle.py tests/live_harness/test_quick_flat_lifecycle_incidents.py
```

Expected: pass.

- [ ] **Step 2: Run adjacent entry/exit/recovery suites**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_exit_decisions.py tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py tests/test_pending_entry_v1_semantic_drift.py tests/test_live_entry_hedge_root_fix.py tests/engine/test_recovery_ledger.py tests/engine/test_pending_entry_terminalizer.py
```

Expected: pass.

- [ ] **Step 3: Run diagnose/offline suites**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_diagnose_live.py tests/offline/test_journal_analysis_semantics.py tests/ops/test_production_health.py
```

Expected: pass.

- [ ] **Step 4: Run hygiene**

Run:

```bash
.venv/bin/python -m compileall -q lightfee tests scripts
git diff --check
```

Expected: both pass.

- [ ] **Step 5: Run full suite**

Run:

```bash
.venv/bin/python -m pytest -q
```

Expected: pass or only pre-existing unrelated skips/warnings documented with
evidence. Do not claim completion on a failing lifecycle-related test.

- [ ] **Step 6: Refresh GitNexus and detect changes**

Run:

```bash
npx gitnexus status
npx gitnexus analyze --index-only --name LightFeeV2 --drop-embeddings .
npx gitnexus status
npx gitnexus detect-changes --scope all --repo LightFeeV2
```

Expected: index current; detect-changes scope matches lifecycle, entry,
funding, runtime, diagnose/offline, config, and docs. Any HIGH/CRITICAL flow
must be summarized before commit/deploy.

## Task 13: Production Read-Only Acceptance Before Any Deploy

**Files:**

- No code changes.

- [ ] **Step 1: Verify production state read-only**

On the cloud host, source service env and run:

```bash
python3 scripts/verify_production_services.py --json
python3 scripts/diagnose_live.py --json --since-deploy
```

Expected: no local-flat/live-open-order false green. If exchange truth finds
live open orders or live positions, do not deploy as acceptance; classify as
recovery work first.

- [ ] **Step 2: Scan quick-flat event chains**

Run a read-only event scan over `runtime/live-events.jsonl`:

```text
join execution.entry_selected -> entry.opened -> runtime.funding_capture_state_updated
-> runtime.normal_close_routing_* -> exit.closed
```

Expected output categories:

- bug quick flat;
- avoidable timing quick flat;
- unavoidable recovery quick flat;
- duplicate observation.

- [ ] **Step 3: Acceptance threshold**

Proceed to deploy only if:

- bug quick flats are reproduced by RED tests or absent;
- avoidable timing quick flats map to `LC-01`;
- unavoidable recovery quick flats map to recovery-ledger rows;
- duplicate observations are excluded from real frequency.

## Task 14: Commit And Handoff

**Files:**

- All modified files.

- [ ] **Step 1: Review diff**

Run:

```bash
git status --short
git diff --stat
git diff -- docs/superpowers docs/bugs
```

Expected: changed files match this plan. No unrelated metadata or generated
artifacts.

- [ ] **Step 2: Commit**

Run:

```bash
git add lightfee tests scripts config docs
git commit -m "feat: centralize v1 trading lifecycle semantics"
```

Expected: commit succeeds.

- [ ] **Step 3: Handoff summary**

Summarize:

- lifecycle decisions added;
- quick-flat categories and counts;
- tests run and exact results;
- GitNexus risk;
- production read-only evidence;
- whether deployment is safe or blocked.

## Self-Review

- Spec coverage: tasks cover funding lifecycle, V1 lifecycle facade, entry
  selection, dispatch, pending viability, close parity, quick-flat replay,
  deduped observability, docs, verification, and production read-only
  acceptance.
- Open-item scan: no open-ended task stubs remain.
- Type consistency: `FundingLifecycle.entry_horizon()` returns
  `EntryHorizonDecision`; `V1TradingLifecycle.entry_admissibility()` and
  `pending_entry_viability()` return `LifecycleDecision`; stable reasons match
  the design spec.
