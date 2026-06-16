# Bug Card: Market Data Snapshot Freshness

Reusable root-cause memory for sidecar snapshot freshness, quote fast paths,
and slow enrichment that must not block publication.

## Stable Fingerprints

- `snapshot_stale_or_missing_timestamp`
- `quote_venue_count_lt_7`
- `sidecar_snapshot` degraded or stale while `current_state` remains healthy
- Long sidecar log sequences of per-symbol enrichment calls before snapshot
  publication

## Current Effective Rule

Quote publication is the fast path. Venue bulk ticker/book data that contains
bid/ask must be allowed to publish even if slower enrichment domains such as
funding-rate details, open interest, liquidity, or transfer evidence are late
or unavailable.

Exchange implementations can still use their native capabilities. The business
contract is unified at the sidecar snapshot level: slow enrichment may degrade
metadata, but it must not remove an otherwise usable venue quote or make the
whole snapshot stale.

Entry readiness is stricter than sidecar publication. Sidecar/last-good data may
seed shortlist and warm tracking, but execution still needs a fresh validated
top-book quote lease. Slow OI/liquidity evidence can be cached, capped, deferred,
or marked unavailable for sidecar publication, but entry gates remain fail-closed
when required evidence is not available.

## Attempts Ledger

| Date | Shape | Status | Notes |
|---|---|---|---|
| 2026-06-14 | Binance/Aster quote blocked by slow OI enrichment | fixed, deployed/cloud verified | Kept premium/ticker quote publication on the fast path and marked OI evidence unavailable when enrichment timed out. |
| 2026-06-15 | OKX bulk ticker quote blocked by cold-cache per-symbol funding enrichment | fixed, deployed/cloud verified | CL-084 bounds OKX funding-rate enrichment after bulk ticker fetch. Slow funding fills as unavailable/zero for that refresh, while bid/ask quote rows still publish. Cloud `fd1579d` verification passed with a fresh snapshot and all quote venues present. |
| 2026-06-15 | Aster/Binance WS-BBO quote lease and OI evidence remained diagnostically ambiguous | fixed, deployed/cloud verified | CL-085 adds WS-BBO lease readiness buckets and Binance/Aster-style OI cache/cap evidence statuses. This keeps V2 WS-BBO instead of restoring V1 full local L2, but restores V1 business semantics: fresh finalist-scope top-book truth before entry and fail-closed liquidity/OI evidence without blocking quote/funding publication. Cloud `2306922` verified flat/no-open-orders with no unmapped lifecycle drift. |
| 2026-06-16 | REST fallback `rest_invalid_quote` and OI cap/timeout evidence remained too coarse | local fixed; local verified | CL-086 splits REST quote fallback failures into stale/missing-timestamp/invalid-bid-ask/unsupported/http/parse/timeout buckets while keeping `reason_family=rest_invalid_quote`, and carries OI cache hit/miss, refresh cap, attempts, deferred, timeout, and elapsed evidence through sidecar quotes, entry liquidity blockers, diagnose, and offline analysis. No stale-quote or liquidity/OI gate is loosened. Local focused verification passed 409 tests, compileall, diff-check, and GitNexus detect_changes with expected high-risk market-data/diagnose scope. |

## Regression Harness

- `tests/venues/test_market_data_client.py`
- `tests/sidecar/test_sources.py`
- `tests/sidecar/test_v1_parity_lifecycle.py`
- `tests/ops/test_production_health.py`
