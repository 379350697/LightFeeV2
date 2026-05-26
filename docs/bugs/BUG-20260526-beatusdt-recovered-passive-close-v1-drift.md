# BUG-20260526 BEATUSDT Recovered Passive Close V1 Drift

## GitNexus Keys

- Fingerprint: `v2.beatusdt.recovered-passive-close.live-flat-probe-after-hedge-dust + price-hint-zero-min-notional-misclassification`
- Status: fixed locally; main push in this workstream; production verification pending.
- Severity: critical.
- Components: `passive-close`, `recovery`, `min-notional`, `venue-bybit`, `venue-okx`, `v1-parity`, `exchange-docs`.
- Symbols: `PassiveCloseExecutor.start_pending_passive_close`, `PassiveCloseExecutor.recover_passive_close`, `PassiveCloseExecutor._submit_hedge_for_delta`, `PassiveCloseExecutor._clear_if_live_flat`, `PassiveCloseExecutor._resolve_hedge_reference_price`, `PassiveCloseExecutor._resolve_hedge_min_notional_quote`, `PassiveCloseExecutor._check_hedge_min_notional`.
- Files: `lightfee/engine/passive_close.py`, `tests/test_passive_close.py`.
- Watch: `live-recovered:BEATUSDT:okx->bybit`, `exit.passive_close_hedge_dust_aborted`, `price_hint=0.0`, `leg_notional_quote=0.0`, `reason=min_notional_rejected`, `price_unavailable_for_min_notional`, `recovery.flat`, `runtime.position_drift_corrected`, `runtime.stale_recovery_block_cleared`.

## Summary

After deploy, recovered state showed `live-recovered:BEATUSDT:okx->bybit`. The recovered passive close path continued into hedge dust handling and emitted `exit.passive_close_hedge_dust_aborted` with `price_hint=0.0`, `leg_notional_quote=0.0`, and `reason=min_notional_rejected`.

Later live probes proved both legs were flat and produced `recovery.flat`, `runtime.position_drift_corrected`, and `runtime.stale_recovery_block_cleared`. That order of events was a V2 semantic drift: V1 recovery semantics require live exchange truth to dominate recovered local state before driving close logic.

## V1 Comparison

V1 recovery/passive-close semantics prioritize live positions as the source of truth in recovered state. If both live legs are approximately flat, V1 clears local `open` / `pending` / recovery-block state immediately. It does not submit taker orders, drive dual-taker, or emit hedge dust aborts from stale recovered local quantities.

V2 was able to drive hedge dust first and only later prove flat. That inverted the V1 ordering.

## Root Cause

Recovered passive close startup did not run a mandatory live-flat probe before building chunks and driving close/hedge state. Separately, min-notional classification accepted `price_hint=0` as an input to notional math, which produced fake `leg_notional_quote=0.0` and misclassified missing price evidence as `min_notional_rejected`.

For OKX/Bybit, local small-fill buffer policy also must not be used as the exchange's hard min-notional rule. Exchange constraints need to come from instrument metadata when available.

## Exchange Docs

- Bybit official `GET /v5/market/instruments-info` documents `lotSizeFilter.minNotionalValue`, `minOrderQty`, and `qtyStep` for linear instruments: https://bybit-exchange.github.io/docs/v5/market/instrument
- OKX official instruments data documents `lotSz`, `minSz`, and `tickSz` for order sizing and price increments: https://tr.okx.com/docs-v5/en/#rest-api-public-data-get-instruments

## Fix Status

Fixed in this workstream.

Implemented behavior:

- `live-recovered:*` passive-close startup performs a live-flat probe before chunking or order submission.
- If both recovered legs are live-flat, V2 clears pending/open state and stale recovery block immediately, emitting `recovery.flat`, `runtime.position_drift_corrected`, and `runtime.stale_recovery_block_cleared`.
- Recovered `recover_passive_close()` also probes live-flat before returning resumed pending work.
- Hedge min-notional checks resolve a reference price before notional math from `price_hint`, local best bid/ask, local mid, or venue market snapshot bid/ask/mark/index.
- If no acceptable price source exists, the reason is `price_unavailable_for_min_notional`, not `min_notional_rejected`.
- OKX/Bybit hard min-notional uses dynamic symbol rules from venue instrument metadata when available. Local small-fill buffer is not used as an OKX/Bybit exchange min-notional substitute.

## Verification

| Date | Environment | Command / Evidence | Result |
|---|---|---|---|
| 2026-05-26 | local | `pytest tests/test_passive_close.py -q` | `103 passed` |
| 2026-05-26 | local | `pytest tests/test_engine_recovery.py tests/test_live_full_closure.py::TestLiveFullClosure::test_housekeeping_clears_clean_recovery_block_after_passive_orphan_cleanup -q` | `17 passed` |
| 2026-05-26 | local | `python3 -m py_compile lightfee/engine/passive_close.py tests/test_passive_close.py` | passed |
| 2026-05-26 | local | `git diff --check -- lightfee/engine/passive_close.py tests/test_passive_close.py` | passed |
| 2026-05-26 | local live harness | `python3 -m pytest -q tests/live_harness/test_recovered_close_and_duplicate_incidents.py` | `2 passed`; BEATUSDT recovered local open/pending with both venues live-flat clears before order submission, and BIOUSDT Bybit duplicate CID with old fill plus live nonzero retries a fresh reduce-only CID instead of clearing from historical fill evidence |

Regression coverage added:

- BEATUSDT-like recovered local state with both live legs flat clears state before any order submission.
- `price_hint=0` resolves market snapshot price before min-notional math.
- Missing price evidence is classified as `price_unavailable_for_min_notional`.
- Bybit instrument `minNotionalValue` is used instead of local small-fill buffer.
- OKX market snapshot matching accepts `BEAT-USDT-SWAP` for canonical `BEATUSDT`.
- Independent live-harness coverage for the production incident pair: BEATUSDT recovered passive close emits `recovery.flat` / `runtime.position_drift_corrected` and clears stale recovery block without order submission; BIOUSDT duplicate `110072` classifies old full-fill evidence as `stale_full_live_nonzero` when live quantity remains nonzero and retries with a fresh CID.

## Regression Watch

- Any recovered `live-recovered:BEATUSDT:*` flow where `order.submit_attempt`, taker submit, or `exit.passive_close_hedge_dust_aborted` occurs before a live-flat proof.
- `exit.passive_close_hedge_dust_aborted` with `price_hint=0.0`, `leg_notional_quote=0.0`, and `reason=min_notional_rejected`.
- Missing `runtime.stale_recovery_block_cleared` after `recovery.flat` for a clean recovered flat state.
- OKX/Bybit min-notional decisions that cite local buffer policy instead of instrument metadata or explicit venue spec fallback.
