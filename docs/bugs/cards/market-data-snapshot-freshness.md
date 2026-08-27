# Bug Card: Market Data Snapshot Freshness

Reusable root-cause memory for sidecar snapshot freshness, quote fast paths,
and slow enrichment that must not block publication.

## Stable Fingerprints

- `snapshot_stale_or_missing_timestamp`
- `quote_venue_count_lt_7`
- `sidecar_snapshot` degraded or stale while `current_state` remains healthy
- Long sidecar log sequences of per-symbol enrichment calls before snapshot
  publication
- `okx_funding_fanout_cancelled` / increasing `CLOSE_WAIT` to `www.okx.com:443`

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

An enrichment fast path must not create one HTTP request per requested symbol
and then cancel an already-open batch merely to meet a snapshot budget. For a
multi-symbol OKX cold cache, V1's authoritative invariant is at most four REST
funding fallbacks per refresh. Each started request must reach a normal response,
its own transport timeout, or service shutdown cancellation followed by an
awaited cleanup. V2 rotates the bounded fallback over cache misses because it
does not have V1's persistent public funding stream; it must not widen the
four-request ceiling to compensate.

## Attempts Ledger

| Date | Shape | Status | Notes |
|---|---|---|---|
| 2026-06-14 | Binance/Aster quote blocked by slow OI enrichment | fixed, deployed/cloud verified | Kept premium/ticker quote publication on the fast path and marked OI evidence unavailable when enrichment timed out. |
| 2026-06-15 | OKX bulk ticker quote blocked by cold-cache per-symbol funding enrichment | fixed, deployed/cloud verified | CL-084 bounds OKX funding-rate enrichment after bulk ticker fetch. Slow funding fills as unavailable/zero for that refresh, while bid/ask quote rows still publish. Cloud `fd1579d` verification passed with a fresh snapshot and all quote venues present. |
| 2026-06-15 | Aster/Binance WS-BBO quote lease and OI evidence remained diagnostically ambiguous | fixed, deployed/cloud verified | CL-085 adds WS-BBO lease readiness buckets and Binance/Aster-style OI cache/cap evidence statuses. This keeps V2 WS-BBO instead of restoring V1 full local L2, but restores V1 business semantics: fresh finalist-scope top-book truth before entry and fail-closed liquidity/OI evidence without blocking quote/funding publication. Cloud `2306922` verified flat/no-open-orders with no unmapped lifecycle drift. |
| 2026-06-16 | REST fallback `rest_invalid_quote` and OI cap/timeout evidence remained too coarse | fixed, deployed/cloud verified | CL-086 splits REST quote fallback failures into stale/missing-timestamp/invalid-bid-ask/unsupported/http/parse/timeout buckets while keeping `reason_family=rest_invalid_quote`, and carries OI cache hit/miss, refresh cap, attempts, deferred, timeout, and elapsed evidence through sidecar quotes, entry liquidity blockers, diagnose, and offline analysis. No stale-quote or liquidity/OI gate is loosened. Cloud `5886b93` verified flat/no-open-orders, no order-error evidence, and no lifecycle unmapped drift. |
| 2026-06-16 | WS-BBO cold-start and full-universe OI cap/timeout could still block finalist evidence | local fixed; real public smoke verified; deployment pending | CL-087 adds a sticky WS-BBO warm set for recent V1 primary/shadow/current finalist targets and candidate-scoped Binance/Aster public OI refresh before the entry liquidity gate. It does not relax quote TTL, OI floor, liquidity/admission/sizing/order guards, or raise the global OI cap. Targeted OI success must still pass the original OI floor; timeout/unsupported remains fail-closed with explicit diagnostics. A real public smoke showed Binance/Aster BTC/ETH candidate-scoped OI resolves in about 354-368ms under the separate bounded entry budget, while the sidecar 100ms fast-path budget remains unchanged. Targeted OI runtime events are mapped as diagnostic-only lifecycle evidence to avoid unmapped drift. |
| 2026-08-26 | `336ad5f8` widened OKX cold-cache fanout to 40; `fd1579d6` added a 0.2s aggregate cancellation to keep snapshots fast | ineffective / regressed production | The two changes were called V1 parity/fast path, but V1 caps multi-symbol REST fallback at four and completes it. The aggregate cancellation left 36 OKX `CLOSE_WAIT` sockets, made OKX data stale, and fail-closed OKX candidates. Do not reuse this pattern. |
| 2026-08-26 | Restore bounded four-request fallback with miss rotation and no ordinary batch cancellation | local green; deployment pending | Real `httpx` production-path regressions prove exactly four slow requests start and complete with zero ordinary cancellations, and outer cancellation awaits exactly those four children. Quote rows remain available and incomplete funding stays fail-closed for entry. |

## Regression Harness

- `tests/venues/test_market_data_client.py`
- `tests/sidecar/test_sources.py`
- `tests/sidecar/test_v1_parity_lifecycle.py`
- `tests/ops/test_production_health.py`

## Next Recurrence Checklist

1. Compare every claimed V1 parity limit with the actual V1 owner and value;
   matching only cache fields is not parity if the request lifecycle changed.
2. Never use an aggregate `wait_for` budget shorter than in-flight request
   deadlines to cancel a bulk HTTP enrichment batch for publication latency.
3. A fast-path test must prove bounded real transport starts, normal completion
   or per-request timeout, and no cancellation/FD accumulation; a quote-only
   assertion is insufficient.
4. Keep incomplete/stale funding fail-closed at entry. Do not trade to hide a
   delayed enrichment result.
