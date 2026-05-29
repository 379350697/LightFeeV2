# Bug Card: Passive Close Terminal Flatness

Purpose: keep the reusable memory for passive close terminal states, under-min
residuals, and live-flat cleanup.

## Stable Fingerprints

- `exit.passive_close_fallback_terminal_flat`
- `pending_passive_close_flat_probe`
- `price_unavailable_for_min_notional`
- `passive_close_maker_filled_under_chunk`
- `entry.cleanup_leg_exposure`
- Recurrence shape: local pending passive close/open state keeps retrying while exchange truth is already flat or while terminal maker/under-min branches need V1 compensation.

## Current Effective Rule

Terminal reduce-only, already-flat, under-min, or price-unavailable close branches can clear only after live exchange truth proves both legs flat. If live truth shows residual exposure and the residual is tradeable, route through V1-style compensation/flattening. If truth is incomplete or cleanup cannot prove flat, retain/fail-closed with structured evidence.

## V1 / Exchange Semantics

- V1 lets live exchange truth dominate stale recovered local close state.
- V1 terminal-maker and under-min branches do not spin forever; they either prove flat, compensate, or fail-closed.
- Unsupported or failed open-order truth is not flat evidence.
- Exchange min-notional / reduce-only terminal rejects must be interpreted with live position truth, not used blindly as success.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-25 | Live-flat sweep for recovered passive closes | effective | UBUSDT/XCNUSDT style stale recovered closes clear only after live flat proof. |
| 2026-05-28 | Conservative open-order truth + DUAL_TAKER fallback | effective | OPGUSDT maker-under-chunk path closes through real flat clearing harness. |
| 2026-05-29 | Terminal-maker price-unavailable compensation | effective | JCTUSDT terminal maker branch no longer loops; cloud probe flat/no-open-orders. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-25 | `UBUSDT`, `XCNUSDT` | `6845d9b` family | closed | [daily/2026-05-25.md](../daily/2026-05-25.md) |
| 2026-05-28 | `OPGUSDT` | local CL-014 | closed by harness | [daily/2026-05-28.md#cluster-cl-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision](../daily/2026-05-28.md#cluster-cl-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision) |
| 2026-05-29 | `JCTUSDT` | `9cdb9df` | closed by cloud probe | [daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression](../daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression) |

## Regression Harness

- `tests/test_passive_close.py`
- `tests/live_harness/test_opgusdt_passive_close_stuck_incident.py`
- `tests/live_harness/test_20260529_jct_parti_regressions.py`
- `tests/live_harness/test_historical_passive_close_incidents.py`

## Next Recurrence Checklist

1. Inspect `pending_passive_closes`, `open_positions`, and `last_error` in current state.
2. Run `diagnose_live.py --symbol <symbol> --venues <long,short>` for high-confidence live truth.
3. Check whether open-order truth is supported and successful on both venues.
4. Search for terminal flat, under-min, price-unavailable, and fallback events.
5. If live truth is flat, bug is stale local terminality. If live truth is nonzero, bug is compensation/repair path.
6. Closure requires harness replay plus credentialed flat/no-open-orders probe.

