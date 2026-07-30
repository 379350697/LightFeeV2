# 2026-07-28 V1 Recovery Evidence and Traceability

This file is the phase-1 recovery manifest for rebuilding LightFeeV2 from the
last known V1-compatible baseline. It intentionally records evidence without
embedding credentials, raw production logs, signed URLs, or exchange secrets.

## Branch and commit anchors

- Recovery branch: `codex/recovery-v1-baseline`
- Recovery baseline: `4bfd856607b452b6690bd45439515227d7b50146`
- Baseline timestamp: `2026-07-14 21:19:23 +0800`
- Baseline subject: `feat: harden funding and spread acceptance evidence`
- Audited post-baseline head: `7164ec3821453a83d3d1b717974f3555e421679b`
- Branch creation worktree status: only pre-existing untracked `.dev-flow/`
  content was present before recovery files were added.

## Evidence boundaries

- The recovery line starts from the 2026-07-14 baseline instead of cherry-picking
  the post-2026-07-15 refactor stack wholesale.
- V1 remains authoritative for compatibility-sensitive semantics: recovery,
  passive close, residual repair, private-stream lifecycle, and live-position
  truth.
- Production exchange state outranks recovered local state.
- Phase 6 is not authorized until phase-5 gates prove that code, state, tests,
  and live-safety controls are ready for directly supervised small-notional
  verification.
- No raw cloud logs were copied into this repository by this phase-1 manifest.

## Retained-vs-excluded traceability matrix

| Candidate | Source evidence | Recovery decision | Required proof |
| --- | --- | --- | --- |
| OKX exact instrument identity / listing absence | Post-baseline fix `e8258f24`; OKX live diagnostics showed prefixed aliases such as `1000BONKUSDT` could be incorrectly mapped to a different one-unit `instId`; OKX public docs identify `instId` as the instrument ID used by the API (`https://www.okx.com/docs-v5/en/`) | Retain narrowly in OKX public market-data mapping; do not import broad BBO/spread modules from the later stack | Contract test: requesting only a prefixed alias must not reuse an unprefixed OKX ticker/funding/OI row |
| Bybit account truth pagination/netting | Post-baseline fix `b5a3d120`; Bybit official V5 position-list docs show default `limit=20`, allowed `1-200`, and `cursor` pagination (`https://bybit-exchange.github.io/docs/api-explorer/v5/position/position-info`) | Retain narrowly in Bybit private position truth read path | Contract tests: `fetch_all_positions` uses `limit=200`, follows `nextPageCursor`, fails closed on cursor loops or later-page failures, and nets hedge-mode long/short rows per symbol |
| Private WS persistence | Post-baseline fix in `dc38c1b8`; V1 `live_startup_activate_private_streams` starts persistent private workers and does not stop them just because a candidate tick has no tracked symbols | Retain only lifecycle persistence/update behavior, not the broad commit | Contract tests: empty tracked-symbol refresh does not stop an already-started worker; symbol additions update in place instead of restart-looping |
| Pair-scoped terminal close archive | V1 `try_abandon_stale_pending_close_reconciliation` checks only the two relevant legs before abandoning terminal reconciliation | Keep/rewrite only V1 pair scope; do not retain the later global all-venue archive barrier | Contract test: unrelated venue errors do not block pair-terminal cleanup, while relevant exchange truth still blocks if stale, unavailable, or non-flat |
| Log safety / signed URL leakage | Bug card CL-162 identified signed URL leakage risk in INFO logs; post-baseline app entrypoints still used raw INFO logging | Retain as cross-cutting hardening only | Contract test: app logging configuration suppresses noisy transport libraries at WARNING or stricter without changing trading behavior |
| Exchange truth over recovered local state | V1 compatibility docs and bug cards require live exchange truth to dominate recovered records | Retain as state/migration guard in later phase 4; phase 2 only pins the close/reconciliation truth boundary | Contract test: pair-scoped current exchange truth is required for close archive decisions; stale/unscoped recovered payloads cannot be treated as global account truth |
| Open interest durable store | Post-baseline `a2c93aa4`; local verification only and later removed by broader refactor | Exclude from recovery critical path | Not ported before phase 6 |
| Broad spread/BBO/research refactors | Post-baseline commits `c6b29973`, `9364b8ce`, `9f10b244` and related spread modules | Exclude from recovery critical path | Diff audit shows no broad spread/BBO/OI runtime refactor imported |
| Broad economics/accounting/runtime refactor | Post-baseline commits including `c6cecc34` and wholesale `dc38c1b8` | Exclude except explicitly traced narrow fixes above | Diff audit shows no wholesale commit import |

## Phase gates

1. Contract tests must be written before function-level implementation edits.
2. GitNexus freshness and impact analysis must be available before editing any
   function, class, or method.
3. Runtime-state cleanup must use timestamped explicit-path backups and
   append-only decisions.
4. Live phase must have an explicit allowlist, notional cap, safety switch,
   private-WS health checks, close/reconciliation criteria, rollback plan, and
   stop conditions.

## Phase-2 contract anchor

- Focused recovery contracts are collected in
  `tests/test_v1_recovery_phase23_contracts.py`.
- These tests are expected to fail against the raw recovery baseline for the
  deliberately unported fixes; the phase-3 implementation pass must make them
  pass without importing broad post-baseline refactors.

## Phase-4 runtime-state migration anchor

- Runtime-state migration and deployment safeguards are recorded in
  `docs/recovery/2026-07-28-runtime-state-migration-procedure.md`.
- The procedure maps configured event/snapshot/sidecar/checkpoint paths,
  pending-close/passive-close/residual-repair records, exchange-truth authority,
  append-only migration decisions, backup/restore rules, and hard stop gates.
- Phase 6 remains blocked unless phase 5 proves the procedure with exact runtime
  paths, file hashes, pair-scoped exchange truth, and focused recovery tests.
