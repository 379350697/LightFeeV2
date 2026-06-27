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

The main sidecar is the default public quote/OI truth publisher. Spread-sidecar
must consume `SidecarSnapshot.quotes` from `runtime.sidecar_snapshot_path` by
default instead of independently pulling Binance/Aster-compatible quote or OI
HTTP. If the main snapshot is missing, malformed, or stale, spread-sidecar
publishes a degraded empty spread snapshot. Direct public fetch is an explicit
emergency fallback only, and must record `source_mode=direct_market_fallback`.

Binance/Aster-compatible symbol-level OI is gated before OI HTTP. Full sidecar
OI enrichment may call `/fapi/v1/openInterest` only when bulk `premiumIndex`
and bulk `bookTicker` both confirm the venue symbol. Missing bulk
`premiumIndex`/`bookTicker` evidence is diagnostic filtered evidence, while
bookTicker-only non-perpetual symbols remain skipped. Candidate-scoped OI may
probe symbol mark truth, but if mark/symbol truth is missing or the mark probe
rejects the symbol it must return `symbol_not_listed_before_http` rather than
calling OI and treating the exchange 400 as normal market-data pressure.

The same pre-HTTP eligibility principle also applies to symbol-scoped private
truth probes, but it is a separate business domain from public OI. Aster
`fetch_position(symbol)` and `fetch_open_orders(symbol)` may skip V3 private
HTTP only when the adapter catalog proves the venue symbol is unsupported, and
must classify the evidence as
`symbol_not_listed_before_private_truth_http`. Missing catalog truth is not
permission to invent flat/no-open-order truth; it remains `truth_unavailable`
and preserves the existing live probe behavior. Account-level unfiltered
private truth must remain available and must not be blocked by one unsupported
candidate symbol.

Diagnose must count public OI and private truth filters separately:
`public_oi_pre_http_filtered_count` is market-data/OI evidence, while
`private_truth_pre_http_filtered_count` is position/open-order truth evidence.
Neither is an order failure or abnormal position by itself.

Entry readiness is stricter than sidecar publication. Sidecar/last-good data may
seed shortlist and warm tracking, but execution still needs a fresh validated
top-book quote lease. Slow OI/liquidity evidence can be cached, capped, deferred,
or marked unavailable for sidecar publication, but entry gates remain fail-closed
when required evidence is not available.

Quote/OI admission evidence is now classified by
`entry_market_evidence_contract(...)` before runtime diagnostics interpret it.
The contract distinguishes `allow_entry_evidence`, `block_stale_quote`,
`block_oi_unavailable`, `terminal_candidate_rewarm`, `refresh_evidence`, and
`diagnostic_recovered_overbudget`. Advisory or recovered evidence can explain a
past no-entry window, but only blocking contract actions are current entry
blockers. This keeps the business rule unified: quote/OI evidence decides
whether a candidate may open; it never changes close terminal truth.

Diagnose summaries must preserve that scope explicitly. Quote stale, quote
rewarm, and OI unavailable samples that are still unresolved use
`scope=entry_candidate_admission` and
`unresolved_blocker_scope=entry_candidate_admission`. They can block new entry
for that candidate, but they are not current exchange exposure, not passive-close
terminal truth, and not permission to loosen quote TTL, OI floors, liquidity
floors, or entry admission.

This scope must remain true even when another current blocker exists in the
same diagnose window. Candidate quote/OI blockers are admission evidence; they
do not make terminal close accounting gaps current close blockers and they do
not create live exposure without exchange position/open-order truth.

Quote rewarm must also have terminal evidence. A venue-symbol that stays stale
past the hard `quote_rewarm` budget must emit
`runtime.entry_quote_rewarm_terminal_stale`, enter a cooldown/stale terminal
state, and stop being reselected until fresh quote truth arrives or the cooldown
expires. Runtime and diagnose must get `action_taken` and
`action_evidence_kind` from `quote_rewarm_handoff_contract` so a hard timeout
cannot appear as an over-budget phase with a blank takeover action.

Candidate lease has the same active-handoff contract. A tradeable candidate
that exceeds the hard `candidate_lease` budget without being selected, rejected,
or admission-blocked must emit `runtime.candidate_lease_expired`, record
`review.candidate_rejected rejected_stage=candidate_lease`, leave the current
tick, and force rescan. Diagnose must expose `phase_handoff_quality` so
operators can distinguish active takeover from passive over-budget drift.
Catalog-level skips such as `runtime.candidate_symbol_skipped
reason=unsupported_symbol` are pre-candidate rejections. They must not create a
candidate lease by themselves; if they follow an existing shortlist artifact,
they count as terminal/takeover evidence rather than a missing-handoff issue.

Entry prewarm has a wider but non-executing scope. After V1 primary+shadow
selection, runtime may warm a bounded set of near-promotion candidates so a
larger Gate/Bitget-inclusive universe does not cold-start quote/OI evidence at
promotion time. Those events must carry `evidence_role=prewarm_only` and
`candidate_scope=prewarm_extra`. Prewarm failures are diagnostic refresh
evidence, not current entry blockers; only `entry_execution` evidence from the
V1 primary+shadow scope can increment current unresolved entry blockers.

## Attempts Ledger

| Date | Shape | Status | Notes |
|---|---|---|---|
| 2026-06-14 | Binance/Aster quote blocked by slow OI enrichment | fixed, deployed/cloud verified | Kept premium/ticker quote publication on the fast path and marked OI evidence unavailable when enrichment timed out. |
| 2026-06-15 | OKX bulk ticker quote blocked by cold-cache per-symbol funding enrichment | fixed, deployed/cloud verified | CL-084 bounds OKX funding-rate enrichment after bulk ticker fetch. Slow funding fills as unavailable/zero for that refresh, while bid/ask quote rows still publish. Cloud `fd1579d` verification passed with a fresh snapshot and all quote venues present. |
| 2026-06-15 | Aster/Binance WS-BBO quote lease and OI evidence remained diagnostically ambiguous | fixed, deployed/cloud verified | CL-085 adds WS-BBO lease readiness buckets and Binance/Aster-style OI cache/cap evidence statuses. This keeps V2 WS-BBO instead of restoring V1 full local L2, but restores V1 business semantics: fresh finalist-scope top-book truth before entry and fail-closed liquidity/OI evidence without blocking quote/funding publication. Cloud `2306922` verified flat/no-open-orders with no unmapped lifecycle drift. |
| 2026-06-16 | REST fallback `rest_invalid_quote` and OI cap/timeout evidence remained too coarse | fixed, deployed/cloud verified | CL-086 splits REST quote fallback failures into stale/missing-timestamp/invalid-bid-ask/unsupported/http/parse/timeout buckets while keeping `reason_family=rest_invalid_quote`, and carries OI cache hit/miss, refresh cap, attempts, deferred, timeout, and elapsed evidence through sidecar quotes, entry liquidity blockers, diagnose, and offline analysis. No stale-quote or liquidity/OI gate is loosened. Cloud `5886b93` verified flat/no-open-orders, no order-error evidence, and no lifecycle unmapped drift. |
| 2026-06-16 | WS-BBO cold-start and full-universe OI cap/timeout could still block finalist evidence | local fixed; real public smoke verified; deployment pending | CL-087 adds a sticky WS-BBO warm set for recent V1 primary/shadow/current finalist targets and candidate-scoped Binance/Aster public OI refresh before the entry liquidity gate. It does not relax quote TTL, OI floor, liquidity/admission/sizing/order guards, or raise the global OI cap. Targeted OI success must still pass the original OI floor; timeout/unsupported remains fail-closed with explicit diagnostics. A real public smoke showed Binance/Aster BTC/ETH candidate-scoped OI resolves in about 354-368ms under the separate bounded entry budget, while the sidecar 100ms fast-path budget remains unchanged. Targeted OI runtime events are mapped as diagnostic-only lifecycle evidence to avoid unmapped drift. |
| 2026-06-18 | Quote rewarm hard-budget terminal evidence | local green, deploy pending | CL-097 adds `runtime.entry_quote_rewarm_terminal_stale` and cooldown suppression when the same venue-symbol remains stale past the hard quote-rewarm budget. The event maps to V1 `ENTRY_QUOTE_LEASE` and keeps `phase_duration_summary` evidence-driven. |
| 2026-06-19 | Candidate lease and quote rewarm active handoff quality | local green, deploy pending | CL-099 adds candidate-lease expiration/takeover evidence and `phase_handoff_quality` diagnostics for candidate and quote-rewarm stages. A stage that exceeds budget without terminal/takeover evidence is now a production issue, while explicit stale/expired evidence is counted as active business progression. Post-deploy diagnostic drift also closed the `runtime.candidate_symbol_skipped reason=unsupported_symbol` false positive so catalog skips do not manufacture missing lease takeover. |
| 2026-06-19 | Unified quote-rewarm handoff contract | local green, deploy pending | CL-102 follow-up moves quote-rewarm hard timeout/takeover action evidence into `lightfee/engine/business_contract.py`. ALICEUSDT-style hard rewarm samples must now report `action_taken=skip_candidate_after_hard_rewarm` with contract evidence, not a blank action. This does not relax quote TTL, OI, liquidity, entry admission, or stale-cooldown rules. |
| 2026-06-20 | Unified quote/OI entry admission evidence contract | fixed in `447218a`, deployed/cloud verified | CL-104 adds `entry_market_evidence_contract(...)` and root `entry_market_evidence_summary`. Quote stale and OI unavailable remain fail-closed candidate blockers; resolved OI/quote evidence, advisory evidence, and over-budget recovery are diagnostic/accounting facts rather than threshold changes. Binance `bookTicker` / `openInterest` stay evidence sources; no quote TTL, OI floor, liquidity floor, or entry admission guard is loosened. Cloud `447218a` showed `entry_market_evidence_summary.unresolved_blocker_count=0`. |
| 2026-06-20 | Quote/OI diagnostic noise visibility | fixed in `091ce2c`, deployed/cloud verified | `classify_noise_visibility(...)` keeps quote stale and OI unavailable as `current_admission_blocker` evidence instead of health-critical current exposure. `diagnostic_noise_summary` aggregates candidate blockers by visibility while preserving the raw journal evidence, and terminal-flat historical unpaired cleanup is no longer counted as current quote/OI or risk exposure noise. No quote TTL, OI floor, liquidity floor, or entry admission guard is loosened. Cloud `091ce2c` showed `entry_market_evidence_summary.unresolved_blocker_count=0` and `diagnostic_noise_summary.current_blocker_count=0`; quote/OI samples remained admission evidence only. |
| 2026-06-20 | Admission blocker scope and non-handoff phase action evidence | local green, deploy pending | Post-deploy strict review found unresolved quote/OI blocker samples could be correct but under-scoped in diagnostics, and over-budget non-handoff phases could still be counted as blank-action noise. The follow-up keeps raw evidence visible, labels unresolved quote/OI blockers as `entry_candidate_admission`, and fills action/evidence for action-required phases without terminalizing `candidate_lease` handoff samples. |
| 2026-06-21 | Quote/OI admission scope preserved during 2-7 close/accounting diagnostics | `9dee9f3` deployed/cloud verified; terminal quantity-warning residual fixed locally, deploy pending | CL-105 keeps quote/OI and quote-rewarm evidence as candidate admission blockers only. Close accounting gaps now use current exchange flat/no-open-orders truth for terminal downgrade rather than the whole production gate, so an unrelated admission blocker cannot make a terminal-flat accounting gap look like current close risk. The local residual follow-up applies terminal/unopened/repair/clean-truth evidence to entry-plan quantity warnings while preserving unproven active quantity mismatches as current warnings. |
| 2026-06-21 | Quote/OI top-offender admission diagnostics | local green, deploy pending | Latest cloud truth stayed flat/no-open-orders and gate green, but quote/OI candidate blockers still required raw counter inspection. `entry_market_evidence_summary` now reports `candidate_admission_noise_summary.top_blocked_owner_ids` with action/class/reason counts and `next_action=targeted_refresh_or_data_source_backfill`. Candidate admission remains fail-closed for new entries; no quote TTL, OI floor, liquidity floor, or entry admission guard is loosened. |
| 2026-06-22 | Non-Binance public OI targeted refresh and evidence semantics | local green, deploy pending | OKX/Bybit/Bitget/Gate/Hyperliquid public OI is treated as market-data evidence, not acceptable structural noise. Candidate-scoped OI refresh now covers every venue with a public market-data path; venue parsers only mark OI `available` when a real OI field is present and quote-normalized. Missing fields, HTTP errors, and parse gaps stay fail-closed as entry admission blockers, while confirmed below-floor OI remains the only path to low/structural OI classification. No OI floor, quote TTL, liquidity floor, entry ranking, sizing, order, close, or recovery guard is loosened. |
| 2026-06-23 | Binance/Aster entry OI over-wide fetch and sidecar OI cancellation | local green, deploy pending | CL-107 moves Binance/Aster entry OI to single-symbol `premiumIndex?symbol=` + `openInterest?symbol=` and keeps the 0.1s sidecar quote path non-blocking by reporting `refresh_inflight` instead of cancelling slow OI tasks. Successful background OI writes the existing 10 minute cache; refresh cap rotates across cycles; runtime targeted refresh batches same-venue symbols; Hyperliquid `openInterest` is multiplied by `markPx`. OI unavailable remains a fail-closed entry admission blocker, not current exposure or structural low OI unless confirmed below floor. |
| 2026-06-23 | OI admission action taxonomy | local green, deploy pending | CL-108 splits `entry_market_evidence_summary.action_counts`: `block_oi_unavailable` is reserved for missing/timeout/rate-limit/unsupported/parse OI evidence, while confirmed available low OI is reported as `block_oi_below_floor` or `block_oi_structural`. Diagnose also reports OI sub-counts and uses `confirmed_oi_below_floor_no_data_backfill` when no data-source action is needed. This is diagnostic taxonomy only; admission remains fail-closed and no OI floor or trading guard is changed. |
| 2026-06-23 | Stale quote WS/REST revalidate diagnostics | local green, deploy pending | CL-110 carries `sidecar_reason`, `ws_bbo_lease_hit`, `rest_revalidate_attempted`, `rest_revalidate_hit`, and `rest_revalidate_terminal_stale` through quote revalidate events. Fresh WS-BBO or REST evidence clears the stale blocker; REST quotes that remain stale still fail closed with precise buckets such as `rest_resolved_but_stale`. No quote TTL or entry admission rule is loosened. |
| 2026-06-24 | Gate/Bitget true funding timestamp sources and Gate contracts cache | `5d9c49e` code deployed/cloud verified; docs closure synced | CL-113 replaces synthetic Gate/Bitget funding observation timestamps with official future funding metadata: Bitget `current-fund-rate.nextUpdate`, Gate `contracts.funding_next_apply`, and fail-closed `0` when true future evidence is missing. Cloud smoke confirmed Gate/Bitget BTCUSDT future `funding_timestamp_ms=1782316800000`; Gate contracts metadata reuses the existing 10 minute funding cache so full `/contracts` is not fetched every sidecar cycle. No strategy window, sidecar pairing, quote TTL, OI floor, or trading guard is loosened. |
| 2026-06-24 | Entry quote/OI prewarm horizon after Gate/Bitget candidate recovery | `653b21a` deployed/cloud verified | CL-115 adds a bounded `entry_quote_prewarm_extra_candidate_count=24` near-promotion horizon. Extra candidates only warm quote/OI caches and emit `prewarm_only` evidence; final entry filtering and dispatch remain V1 primary+shadow. Diagnose now separates `prewarm_extra` counters, avoids double-counting paired quote-revalidate/rewarm events, and does not count prewarm-only failures as unresolved blockers. Active venue admission cooldowns, including Aster `max_notional_admission_blocked`, prune before prewarm. Cloud since-deploy diagnose passed with no entry evidence blockers, no quote rewarm timeout, and exchange truth flat/no-open-orders. |
| 2026-06-27 | Public OI pre-HTTP symbol filter and spread snapshot sharing | code fix `d2d89af` deployed/cloud verified | CL-123 filters Binance/Aster-compatible OI symbols before OI HTTP when bulk premiumIndex/bookTicker does not confirm tradeability or candidate mark truth is missing/rejected. Spread-sidecar defaults to consuming the main sidecar snapshot and only direct-fetches public data through an explicit fallback config that marks `source_mode=direct_market_fallback`. |
| 2026-06-27 | Private truth pre-HTTP symbol filter and spread source-state classification | local green, deploy pending | CL-124 adds shared `venue_symbol_eligibility(...)` for Aster private position/open-order probes, reports `symbol_not_listed_before_private_truth_http` before V3 private HTTP, keeps account-level unfiltered truth untouched, and splits spread stale source evidence into current degraded vs `transient_stale_recovered`. No quote/OI threshold, entry sizing, order, close, or recovery behavior is changed. |

## Regression Harness

- `tests/venues/test_market_data_client.py`
- `tests/sidecar/test_sources.py`
- `tests/spread/test_snapshot_and_service.py`
- `tests/sidecar/test_v1_parity_lifecycle.py`
- `tests/ops/test_production_health.py`
