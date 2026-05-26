# Bug Ledger README

This directory is the lightweight, long-term bug ledger for LightFeeV2.

The goal is to preserve enough context for future debugging, GitNexus search, and regression review without storing large logs or raw command output.

## File Layout

Use one daily ledger file for normal work:

```text
docs/bugs/daily/YYYY-MM-DD.md
```

Use a standalone bug file only when a cluster becomes too large for the daily file:

```text
docs/bugs/BUG-YYYYMMDD-topic-fingerprint.md
```

Keep `docs/bugs/BUG_INDEX.md` as the cross-day index. The index should list daily clusters or standalone bug files, not duplicate full investigations.

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
- After adding or changing bug docs, run `npx gitnexus analyze` when a fresh GitNexus index is needed.
- Keep `BUG_INDEX.md` small and index-like. The daily or standalone ledger owns the details.

### Handoff Checklist

Before handing off a bug-ledger-only update:

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
