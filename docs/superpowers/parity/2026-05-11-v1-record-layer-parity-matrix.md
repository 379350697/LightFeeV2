# V1 Record Layer Parity Matrix

**Rust source:** `/media/wl/新加卷/codex/LightFee`

**Python target:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-11

**Status values:** `open`, `in_progress`, `fixed`, `partial` (presentation-only surfaces, evidence preserved)

---

## 1. Journal Envelope Shape

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| JE-001 | `run_id` format: Rust uses `lightfee-{ts}-{pid}`, Python uses bare ms timestamp | `journal_bridge.rs:293-297` | `JsonlJournal::new` | `lightfee/persistence/journal.py:23` | `Journal.__init__` | `LiveRuntime` startup | Python run_id missing pid suffix, different prefix | Align run_id format to include process id or maintain parity semantics | `tests/test_persistence.py` | P1 | open |
| JE-002 | Envelope fields `seq,run_id,ts_ms,kind,payload` match but type divergence: Rust `seq:u64,ts_ms:i64` vs Python `seq:int,ts_ms:int` | `journal_bridge.rs:32-41` | `JournalRecord` | `lightfee/persistence/journal.py:52-58` | `Journal.append` | All journal callers | Type semantic drift (unsigned vs signed for seq) | Document that Python int covers both; behavior is equivalent | `tests/test_persistence.py` | P2 | open |
| JE-003 | Rust JSONL line is `\n` terminated and validated; Python `json.dumps` with `ensure_ascii=False` | `journal_bridge.rs:689-705` | `serialize_record_line` | `lightfee/persistence/journal.py:59` | `Journal.append` | All journal callers | Python uses ensure_ascii=False (UTF-8); Rust uses serde_json default (ASCII-safe) | Ensure round-trip preserves non-ASCII payload field values | `tests/test_persistence.py` | P2 | fixed |
| JE-004 | Critical append: Rust uses dedicated worker thread + sync fallback; Python uses inline fsync | `journal_bridge.rs:585-644` | `JsonlJournal::write_record` | `lightfee/persistence/journal.py:65-76` | `Journal.append_critical` | Recovery state changes, operator commands | Python lacks async queue drop counter, sync fallback tracking | Keep critical append semantics (fsync before return); document async queue is simplified | `tests/test_persistence.py` | P1 | fixed |

## 2. Order Lifecycle Records

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| OL-001 | Rust emits `entry.opened` with full OpenPosition payload (17+ fields); Python emits `entry.completed` with subset (6 fields) | `entry_sync.rs` | `finalize_opened_position` | `lightfee/engine/entry_sync.py:218-228` | `EntrySyncExecutor.execute` | `LiveRuntime._maybe_tick_entry` | Missing: `long_entry_price`, `short_entry_price`, `opened_at_ms`, `current_net_quote`, `peak_net_quote`, `captured_funding_quote`, `long_entry_fee_quote`, `short_entry_fee_quote`, `second_stage_funding_quote`, `funding_captured` | Emit full OpenPosition payload matching Rust `entry.opened` shape | `tests/test_engine_entry_exit.py` | P0 | fixed |
| OL-002 | Rust emits `entry.aborted`, `entry.aborted_failed_pending_retained`; Python has no abort lifecycle records | `entry_sync.rs` | abort paths | N/A | N/A | `LiveRuntime` abort handling | Python drops abort evidence entirely | Add `entry.aborted`, `entry.aborted_failed_pending_retained` journal events | `tests/test_engine_entry_exit.py` | P1 | open |
| OL-003 | Rust emits `order.submitted`, `order.filled`, `order.failed` per-order; Python uses f-string dynamic kinds (`entry.{leg}_submitted`, `entry.{leg}_filled`, etc.) | `entry_sync.rs`, `entry.rs` | order submission paths | `lightfee/engine/entry_sync.py:238-322` | `_submit_maker`, `_submit_hedge`, `_submit_order` | `EntrySyncExecutor.execute` | Python dynamically generates kind strings, making consumption harder; Rust uses fixed canonical kinds | Normalize to canonical kinds with leg/venue in payload; preserve all order fields | `tests/test_engine_entry_exit.py` | P1 | open |
| OL-004 | Rust order.filled payload includes `order_id`, `client_order_id`, `symbol`, `side`, `venue`, `quantity`, `price`, `fee_quote`, `latency_ms`, `is_maker`; Python missing `client_order_id`, `latency_ms`, `is_maker` | `entry_sync.rs` | order fill record emission | `lightfee/engine/entry_sync.py:284-293` | `_submit_order` | `EntrySyncExecutor` | Missing order fill payload fields | Add `client_order_id`, `latency_ms`, `is_maker` to order fill journal payloads | `tests/test_engine_entry_exit.py` | P1 | open |
| OL-005 | Rust emits `exit.closed` with full PnL attribution; Python matches core shape but missing `funding_pnl_quote` breakdown | `exit.rs:4896` | `finalize_close_position_execution` | `lightfee/engine/close_executor.py:389-401` | `execute_close` | `CloseExecutor` / `Supervisor` | Missing `funding_pnl_quote`, `entry_fee_quote` in exit.closed payload | Add `funding_pnl_quote`, `entry_fee_quote` to exit.closed journal payload | `tests/test_close_execution.py` | P0 | fixed |
| OL-006 | Rust `exit.partial_closed` payload includes `position_id`, `quantity`, `current_net_quote`, `peak_net_quote`, `funding_captured`, `second_stage_funding_captured`; Python matches core fields | `replay_bridge.rs:373-397` | `update_position_state` | `lightfee/persistence/journal.py:156-162` | `replay_journal_records` (consumer) | Replay/Persistence | Partial close payload shape matches; verify `peak_net_quote`, `funding_captured`, `second_stage_funding_captured` are emitted at producer | `tests/test_persistence_replay.py` | P1 | open |

## 3. Candidate/Filter List Records

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CF-001 | Rust emits `scan.completed` with `candidate_count`, `blocked_count`, `accepted_count`, `pair_id` list, `blocked_reasons` dict, `no_entry_reason`; Python sidecar snapshot has `CandidateInput` but journal emission is limited | `engine.rs` scan completion paths | scan completion record emission | `lightfee/sidecar/snapshot.py:65-82` | `CandidateInput` (data shape only) | `LiveRuntime._maybe_tick_scan` | Python CandidateInput has `blocked` bool, `blocked_reason` str — compressed vs Rust list; no scan.completed journal emission found in live runtime | Emit `scan.completed` with full candidate list, blocked reasons per candidate; never compress to boolean | `tests/test_engine_entry_exit.py` | P0 | fixed |
| CF-002 | Rust emits `scan.no_entry_diagnostics`, `scan.runtime_gate_blocked` with detailed reason payloads; Python has no equivalent | `engine.rs` | scan gate/diagnostic record emission | N/A | N/A | `LiveRuntime._maybe_tick_scan` | Python drops scan diagnostic evidence | Add `scan.no_entry_diagnostics`, `scan.runtime_gate_blocked` journal events | `tests/test_engine_entry_exit.py` | P1 | fixed |
| CF-003 | CandidateInput.blocked_reason is a single string; Rust maintains a list of blocked_reasons per candidate | `engine.rs` | candidate scoring paths | `lightfee/sidecar/snapshot.py:79` | `CandidateInput` dataclass | `LiveRuntime` candidate filtering | Python collapses multiple blocked reasons into one | Change `blocked_reason` to `blocked_reasons: list[str]` to preserve all rejection reasons | `tests/test_offline_analysis.py` | P0 | fixed |

## 4. Replay Input/Output Reconstruction

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RI-001 | Rust replay tracks positions as full `ReplayPositionSnapshot` with 9 fields; Python positions dict preserves incoming payload keys but doesn't normalize shape | `replay_bridge.rs:42-52` | `ReplayPositionSnapshot` | `lightfee/persistence/journal.py:125-182` | `replay_journal_records` | `Recovery`, `Offline analysis` | Python positions dict shape is ad-hoc (depends on what was in journal payload), not a fixed schema | Normalize replay position output to fixed `ReplayPositionSnapshot` shape | `tests/test_persistence_replay.py` | P0 | fixed |
| RI-002 | Rust replay distinguishes `pending_entry_count` and `pending_close_count`; Python hardcodes both to 0 with comment "journal alone can't distinguish" | `replay_bridge.rs:128-233` | `ReplayAccumulator::ingest` | `lightfee/persistence/journal.py:177-178` | `replay_journal_records` | `Recovery` | Python admits inability to track pending counts from journal | Track pending entries/closes from journal events (`entry.pending_registered`, `exit.pending_close_registered`) | `tests/test_persistence_replay.py` | P0 | fixed |
| RI-003 | Rust `ReplayEventStep` captures per-step `lifecycle`, `global_risk_mode`, `active_position_count`, `position_snapshot`; Python returns flat dict with final state only | `replay_bridge.rs:15-24` | `ReplayEventStep` | `lightfee/persistence/journal.py:174-182` | `replay_journal_records` | `Recovery` | Python replay output is a summary, not a step-by-step reconstruction | Return per-step or per-event reconstruction data; at minimum return event-level timeline | `tests/test_persistence_replay.py` | P1 | fixed |
| RI-004 | Rust `replay_dataset` consumes journal records and produces `ReplayResult` with full candidate statistics; Python `replay_dataset` only counts blocked vs accepted from `sidecar.candidate_published` | `replay_bridge.rs:90-97` | `replay_journal_file` | `lightfee/offline/replay/engine.py:21-34` | `replay_dataset` | Offline analysis | Python replay is summary-only: counts accepted/rejected from `blocked` boolean | Replay must read raw evidence (candidate list, filter list, reasons) not just boolean `blocked` | `tests/test_persistence_replay.py` | P0 | fixed |
| RI-005 | Rust `CounterfactualSpec` applies config_overrides and replays; Python `run_counterfactual` ignores config_overrides and calls through to summary-only `replay_dataset` | `replay_bridge.rs`, counterfactual paths | N/A | `lightfee/offline/replay/counterfactual.py:22-27` | `run_counterfactual` | Offline counterfactual analysis | Python counterfactual is a stub | Apply config_overrides and use full recorded evidence for replay | `tests/test_persistence_replay.py` | P2 | fixed |
| RI-006 | Rust `WalkForwardWindow` generates real date windows; Python uses placeholder date logic | `walk_forward.rs` | `generate_walk_forward_windows` | `lightfee/offline/replay/walk_forward.py:17-40` | `generate_walk_forward_windows` | Offline walk-forward analysis | Python date arithmetic is stubbed; window generation returns same date for train/test | Implement proper date arithmetic for walk-forward windows | `tests/test_persistence_replay.py` | P2 | fixed |

## 5. Persistence Metrics Contract

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PM-001 | Rust `JournalRuntimeMetrics` has 19 counters; Python `PersistenceMetrics` has 7 fields | `journal_bridge.rs:90-114` | `JournalRuntimeMetrics` | `lightfee/persistence/metrics.py:8-35` | `PersistenceMetrics` | `Journal`, diagnostics | Missing: `async_appends`, `critical_appends`, `sync_fallback_appends`, `dropped_async_appends`, `flush_requests`, `writer_flushes`, `writer_failures`, `queue_disconnects`, and all health/risk counters | Add journal quality metrics (async/critical/sync_fallback/dropped/flush/writer counters) to PersistenceMetrics | `tests/test_persistence.py` | P1 | fixed |
| PM-002 | Rust metrics include runtime health counters (`open_position_count`, `global_risk_mode`, `net_exposure_milli_quote`, venue health counts, risk trigger counts); Python persists counts separately through journal only | `journal_bridge.rs:116-140` | `JournalRuntimeMetricsSnapshot` | `lightfee/persistence/metrics.py` | `PersistenceMetrics` | `LiveRuntime`, Prometheus export | Python has no runtime health metrics in persistence layer | Add runtime health metrics snapshot to persistence layer for Prometheus export parity | `tests/test_persistence.py` | P1 | fixed |
| PM-003 | Rust records per-event-type counters (`order_timeout_count`, `ws_disconnect_count`, `rest_failure_count`, `reconcile_drift_count`); Python tracks errors as generic string list | `journal_bridge.rs:543-583` | `record_*` methods | `lightfee/persistence/metrics.py:33-34` | `record_error` | `LiveRuntime` | Python error tracking is unstructured string list vs typed counters | Add typed event counters matching Rust per-event metrics | `tests/test_persistence.py` | P2 | fixed |

## 6. Offline Analysis Consumers

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| OA-001 | Python `analyze_journal_records` consumes `entry.opened`, `exit.closed`, `order.submitted`, `order.filled`, `order.failed`; missing many V1 kinds (recovery, risk, runtime diagnostics) | `analysis/journal.rs` (Rust analog) | analysis functions | `lightfee/offline/analysis/journal.py:30-69` | `analyze_journal_records` | Offline reporting | Python analysis only covers 5 event kinds; Rust analysis covers 40+ | Extend analysis to consume all record-layer event kinds that Rust V1 processes | `tests/test_offline_analysis.py` | P2 | partial |
| OA-002 | Python `DailyPnLSummary` has 7 fields; Rust PnL attribution has separate price_pnl, entry_fee, exit_fee, funding per position | `exit.rs:5960` | `build_exit_pnl_attribution` | `lightfee/engine/close_executor.py:534-553` | `build_exit_pnl_attribution` | Offline PnL analysis | Python has `build_exit_pnl_attribution` but `DailyPnLSummary` doesn't use it; analysis reads from journal raw payloads | Wire `analyze_journal_records` to use full PnL attribution from journal records, not just `net_quote` | `tests/test_offline_analysis.py` | P1 | fixed |

## 7. Recovery Evidence Preservation

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RE-001 | Rust emits `recovery.live_detected` with full OpenPosition; Python already supports this but only on replay consumer side | `recovery.rs` | `finalize_startup_position_recovery` | `lightfee/engine/recovery.py:333-386` | `_apply_journal_replay_to_state` | `LiveRuntime.start` | Python consumes `recovery.live_detected` on replay but doesn't emit it at producer | Ensure `recovery.live_detected` is emitted when live positions are detected at startup | `tests/test_engine_recovery.py` | P1 | open |
| RE-002 | Rust emits `recovery.flat` for positions that are flat (closed on venue but in snapshot); Python consumes it on replay but producer emission is missing | `recovery.rs` | flat position handling | `lightfee/engine/recovery.py:354` | `_apply_journal_replay_to_state` | `LiveRuntime.start` | Python consumes but doesn't emit | Ensure `recovery.flat` is emitted when position is detected as flat during reconciliation | `tests/test_engine_recovery.py` | P1 | open |
| RE-003 | Rust emits `recovery.blocked`, `recovery.mismatch_detected`, `recovery.mismatch_flattened`, `recovery.resumed`; Python has no recovery diagnostic records | `recovery.rs` | recovery diagnostic record emission | N/A | N/A | `LiveRuntime.reconcile` | Python drops recovery diagnostic evidence | Add recovery diagnostic journal events | `tests/test_engine_recovery.py` | P1 | open |
| RE-004 | Python `build_persistent_state_view` serializes pending entries/closes but doesn't include `retained_local_l2_books`, `local_l2_books_snapshot`, `local_l2_session_snapshot` in producer path | `recovery.rs` | `persistent_state_view` | `lightfee/engine/recovery.py:440-493` | `build_persistent_state_view` | `LiveRuntime._write_snapshot` | Local-L2 state fields are consumed on restore but may not be emitted on snapshot write | Verify local-L2 state fields are included in snapshot producer | `tests/test_engine_recovery.py` | P1 | open |

## 8. Risk Mode and Lifecycle Transition Records

| ID | Gap | Rust Source | Rust Function | Python Source | Python Function | Live-Path Caller | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RL-001 | Rust emits `risk.warning_triggered`, `risk.warning_cleared`, `risk.death_triggered`, `risk.single_side_protection_triggered/failed/unavailable` with structured payloads; Python risk journal events are minimal | `risk.rs`, `supervision.rs` | risk record emission | `lightfee/engine/supervisor.py` | supervisor journal emit | `LiveRuntime._maybe_tick_supervisor` | Python risk events lack full trigger payload (health ratios, adjusted quantities, blocked reasons) | Ensure risk trigger journal payloads match Rust V1 shape | `tests/test_engine_recovery.py` | P0 | open |
| RL-002 | Rust `runtime.lifecycle_changed` and `runtime.risk_mode_changed` payloads include `from`, `to`, `reason`; Python matches this | `replay_bridge.rs:332-361` | `update_runtime_state` | `lightfee/persistence/journal.py:164-172` | `replay_journal_records` | `LiveRuntime` lifecycle transitions | Shape matches; verify `reason` is always populated at producer | `tests/test_persistence_replay.py` | P2 | fixed |

---

## Verification Column

| ID | Focused Test | Production Caller |
| --- | --- | --- |
| JE-001 | `tests/test_persistence.py::TestJournal` | `LiveRuntime.start` |
| JE-002 | `tests/test_persistence.py` (add type checks) | All journal callers |
| JE-003 | `tests/test_persistence.py` (add Unicode round-trip test) | All journal callers |
| JE-004 | `tests/test_persistence.py` (add critical append durability test) | `LiveRuntime` recovery/operator paths |
| OL-001 | `tests/test_engine_entry_exit.py` | `EntrySyncExecutor.execute` |
| OL-002 | `tests/test_engine_entry_exit.py` (add abort lifecycle test) | `LiveRuntime._maybe_tick_entry` |
| OL-003 | `tests/test_engine_entry_exit.py` (add order lifecycle test) | `EntrySyncExecutor._submit_order` |
| OL-004 | `tests/test_engine_entry_exit.py` (add fill payload test) | `EntrySyncExecutor._submit_order` |
| OL-005 | `tests/test_close_execution.py` | `CloseExecutor.execute_close` |
| OL-006 | `tests/test_persistence_replay.py::TestJournalReplayEngine` | Replay/Recovery consumers |
| CF-001 | `tests/test_engine_entry_exit.py` (add scan test) | `LiveRuntime._maybe_tick_scan` |
| CF-002 | `tests/test_engine_entry_exit.py` (add scan diagnostic test) | `LiveRuntime._maybe_tick_scan` |
| CF-003 | `tests/test_offline_analysis.py` | `LiveRuntime` candidate filtering |
| RI-001 | `tests/test_persistence_replay.py` | `Recovery`, offline analysis |
| RI-002 | `tests/test_persistence_replay.py` (add pending tracking test) | `Recovery` |
| RI-003 | `tests/test_persistence_replay.py` (add step-by-step replay test) | `Recovery` |
| RI-004 | `tests/test_persistence_replay.py` (add full evidence replay test) | Offline analysis |
| RI-005 | `tests/test_persistence_replay.py` (add counterfactual test) | Offline counterfactual |
| RI-006 | `tests/test_persistence_replay.py` (add walk-forward test) | Offline walk-forward |
| PM-001 | `tests/test_persistence.py::TestMetrics` | `Journal`, diagnostics |
| PM-002 | `tests/test_persistence.py::TestMetrics` (add health metric test) | `LiveRuntime`, Prometheus export |
| PM-003 | `tests/test_persistence.py::TestMetrics` (add event counter test) | `LiveRuntime` |
| OA-001 | `tests/test_offline_analysis.py` | Offline reporting |
| OA-002 | `tests/test_offline_analysis.py` (add PnL attribution test) | Offline PnL analysis |
| RE-001 | `tests/test_engine_recovery.py` | `LiveRuntime.start` |
| RE-002 | `tests/test_engine_recovery.py` (add flat detection test) | `LiveRuntime.reconcile` |
| RE-003 | `tests/test_engine_recovery.py` (add recovery diagnostic test) | `LiveRuntime.reconcile` |
| RE-004 | `tests/test_engine_recovery.py` (add snapshot producer test) | `LiveRuntime._write_snapshot` |
| RL-001 | `tests/test_engine_recovery.py` (add risk trigger test) | `LiveRuntime._maybe_tick_supervisor` |
| RL-002 | `tests/test_persistence_replay.py::TestJournalReplayEngine` | `LiveRuntime` lifecycle transitions |
