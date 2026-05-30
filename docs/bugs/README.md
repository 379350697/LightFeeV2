# Bug Ledger README

This directory is the lightweight, long-term bug ledger for LightFeeV2.

The goal is to preserve enough context for future debugging, GitNexus search, and regression review without storing large logs or raw command output.

## Quick Workflow

When debugging or documenting a bug, follow this order:

1. Search existing cards first: `rg "<event|error|symbol|fingerprint>" docs/bugs/cards docs/bugs/BUG_INDEX.md`.
2. If the failure matches an existing card, update that card's `Recurrences` and, if needed, `Attempts Ledger`.
3. Record the full incident in `docs/bugs/daily/YYYY-MM-DD.md`.
4. Add or update only an index-level row in `docs/bugs/BUG_INDEX.md`; do not duplicate the full investigation there.
5. Mark a bug `closed` only after the card/daily entry records the required harness/probe evidence. Local tests alone are `local green`, not closure, unless the bug is explicitly non-production.

If no existing card matches, write the daily entry first. Add a new card only
when the failure family has recurred, is likely to recur, or contains a partial
or ineffective fix that future debugging must not repeat.

## Minimal Templates

Use these short templates when speed matters. Keep details compact and link to
the daily entry for full evidence.

### Daily Cluster

```md
## Cluster CL-XXX-short-topic

### GitNexus Keys
- Fingerprint: `stable.failure.fingerprint`
- Components: `component-a`, `component-b`
- Symbols: `SymbolName`, `Class.method_name`
- Files: `path/to/file.py`
- Commits: `abc1234`
- Watch: `event.kind`, `error_code`, `log_fingerprint`
- Card: [card-name](../cards/card-name.md) or `none yet`

### Summary
Observed behavior, affected symbols/venues, and whether local/exchange state had open positions or orders.

### Root Cause
Concrete cause. State whether it is V1 drift, exchange semantics, missing evidence, or operational/deploy issue.

### V1 / Exchange Decision
- V1 copy: what was copied, with source if known.
- Exchange docs: what official rule/error code controls the fix.
- Not allowed: any rejected direction that would cause semantic drift.

### Attempts
| Attempt | Status | Why |
|---|---|---|
| short attempt name | effective / partial / ineffective | one-line reason |

### Fix
Behavioral change, not just file names.

### Verification
| Environment | Evidence | Result |
|---|---|---|
| local | harness/test/probe | short result |
| cloud | harness/probe/state truth | short result |

### Acceptance
Closed / local green / deployed pending probe / blocked, with the exact remaining evidence needed.
```

### Card Row Updates

```md
## Attempts Ledger
| Date | Attempt | Status | Why |
|---|---|---|---|
| YYYY-MM-DD | attempted fix or diagnostic | effective / partial / ineffective | why it did or did not close the family |

## Recurrences
| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| YYYY-MM-DD | `SYMBOL` venue pair | `commit` or `working tree` | closed / open / partial | [daily link](../daily/YYYY-MM-DD.md#cluster-anchor) |
```

### BUG_INDEX Row

```md
| [CL-XXX-topic](daily/YYYY-MM-DD.md#cluster-cl-xxx-topic) | status | severity | `component-a`, `component-b` | `stable.failure.fingerprint` | YYYY-MM-DD | first evidence commit/run | fixed commit or working tree | latest short verification | related rule/card | one-sentence current outcome |
```

## File Layout

Use short bug cards for recurring bug families. A card is not an incident log;
it is the compact reusable memory for future root-cause work:

```text
docs/bugs/cards/<family-fingerprint>.md
```

Use one daily ledger file for normal work:

```text
docs/bugs/daily/YYYY-MM-DD.md
```

Use a standalone bug file only when a cluster becomes too large for the daily file:

```text
docs/bugs/BUG-YYYYMMDD-topic-fingerprint.md
```

Keep `docs/bugs/BUG_INDEX.md` as the cross-day index. The index should list daily clusters or standalone bug files, not duplicate full investigations.

## Bug Cards

Bug cards are for recurring families such as pending-entry admission, Local-L2
continuity, passive-close terminality, or live-truth false flat. Keep cards
short enough to read during an incident, roughly 100-180 lines. Do not paste
full daily evidence into a card.

Required card sections:

```md
# Bug Card: Family Name

## Stable Fingerprints
## Current Effective Rule
## V1 / Exchange Semantics
## Attempts Ledger
## Recurrences
## Regression Harness
## Next Recurrence Checklist
```

Card rules:

- Add a card when the same failure family has recurred or is likely to recur.
- A daily entry owns the full incident evidence; the card owns reusable
  conclusions.
- Every same-family recurrence gets one row in `Recurrences`.
- Every partial or ineffective fix gets one row in `Attempts Ledger`.
- Update `Current Effective Rule` only when the actual decision rule changes.
- Keep commands as compact harness/probe names, not raw output.
- If a card grows beyond quick-read size, split by root decision boundary, not
  by date.

## Daily Ledger Structure

A daily file may contain multiple independent session/incident clusters. Do not separate clusters by time alone; every cluster must have a stable topic fingerprint.

Recommended structure:

```md
# Bug Ledger Daily - YYYY-MM-DD

## GitNexus Index

| Cluster ID | Status | Severity | Fingerprint | Components | Symbols | Files | Commits | Outcome |
|---|---|---:|---|---|---|---|---|---|

## Cluster CL-001-short-topic

### GitNexus Keys
- Fingerprint: `stable.machine.searchable.fingerprint`
- Components: `component-a`, `component-b`
- Symbols: `SymbolName`, `Class.method_name`
- Files: `path/to/file.py`, `path/to/other.py`
- Commits: `abc1234`, `def5678`
- Watch: `event.kind`, `error_code`, `log_fingerprint`

### Summary
### Shared Root Cause
### Timeline

### Issue CL-001-A: Specific Subproblem
#### Fingerprint
#### Root Cause
#### Failed / Ineffective Attempts
#### Fix Status
#### Verification
#### Regression Watch
```

## Cluster Rules

- One conversation/debugging thread can produce one cluster.
- A cluster can contain multiple sub-issues, but each sub-issue must have its own root cause, fix status, verification, and watch indicators.
- Do not let sub-issues share vague conclusions. If the root cause differs, give it a separate issue section.
- If a cluster grows too large, split it into a standalone `BUG-YYYYMMDD-topic-fingerprint.md` file and link it from the daily ledger.

## GitNexus-Friendly Fields

GitNexus indexes documentation as files and is strongest when the same stable words appear across docs and code. Every cluster should include:

- `Fingerprint`: short, stable, grep-able string that captures the failure mode.
- `Components`: domain labels such as `entry-local-l2`, `order-reconciliation`, `venue-bitget`.
- `Symbols`: exact code symbols such as `CandidateInput`, `LiveRuntime._entry_local_l2_selection_blocker`, `VenueTransport._parse_bybit_execution_list`.
- `Files`: exact repo paths such as `lightfee/engine/runtime.py`.
- `Commits`: short SHAs related to failed attempts and fixes.
- `Watch`: event kinds, error codes, or compact log fingerprints that indicate recurrence.

Prefer exact symbol and file names over prose-only descriptions. This makes `rg` and GitNexus queries useful after re-indexing.

## Storage Policy

Do store:

- Fingerprints.
- Root causes.
- Failed or ineffective attempts.
- Fix summaries.
- Verification commands and short result summaries.
- Regression watch indicators.
- Links or paths to source files and commits.
- Bug-card recurrence rows for same-family incidents.

Do not store:

- Full cloud logs.
- Raw pytest output beyond a short result line.
- Large JSON payloads.
- Secrets, API keys, account identifiers, or credentials.
- Repeated command transcripts.

If raw evidence is necessary, store only a small excerpt or a stable pointer to where it can be reproduced.

## Verification Style

Use concise evidence:

```md
| Date | Environment | Command / Evidence | Result |
|---|---|---|---|
| YYYY-MM-DD | local | `pytest tests/test_x.py -q` | `42 passed` |
| YYYY-MM-DD | local | probe: Bybit `side=Sell` execution | returned `Side.SELL` |
```

When a fix is incomplete, say so explicitly in `Fix Status` and keep the item in `Regression Watch`.

## Maintenance

- Update the ledger in the same branch as the code fix whenever possible.
- For same-family incidents, update both the daily entry and the matching card.
- After adding or changing bug docs, run `npx gitnexus analyze` when a fresh GitNexus index is needed.
- Keep `BUG_INDEX.md` small and index-like. The daily or standalone ledger owns the details.

### Handoff Checklist

Before handing off a bug-ledger-only update:

- Confirm any same-family issue has an updated card in `docs/bugs/cards/`.
- Confirm the daily or standalone bug entry has a stable `Fingerprint`, exact
  `Components`, exact `Symbols`, exact `Files`, and concrete `Watch` indicators.
- Confirm `docs/bugs/BUG_INDEX.md` links to the entry and keeps only index-level
  summary text.
- Record verification as short evidence rows or compact result lines, not raw
  command transcripts.
- Run the documentation-only validation gate from
  `docs/testing-validation-strategy.md`:

```bash
python3 scripts/validate_change.py --profile smoke
```

- Use `git status --short` to confirm the handoff diff is documentation-only, or
  explicitly call out any pre-existing non-documentation changes that are outside
  the handoff scope.
