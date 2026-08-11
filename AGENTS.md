# Agent Instructions - LightFeeV2

These instructions apply to `/Users/wl/projects/LightFeeV2`.

This project is used by both Codex and Claude Code. Keep `AGENTS.md` and
`CLAUDE.md` synchronized; `AGENTS.md` is the source of truth when the two files
disagree.

## V1 Reference

`LightFee V1` means the original legacy LightFee production behavior, especially
the Rust V1 live-trading semantics. When the sibling V1 repo is available, it is
expected at `/Users/wl/projects/LightFee`.

For LightFeeV2, V1 is authoritative only for compatibility-sensitive trading
semantics such as recovery, passive close, residual repair, and live-position
truth. If V2 behavior disagrees with V1 in those areas, treat V1 as the expected
semantic contract before changing the V2 implementation.

## Production Safety

- Treat production diagnostics and trading-state changes as high risk.
- Start read-only unless the user explicitly asks for a fix, deploy, or mutation.
- Do not submit orders, cancel orders, edit runtime state, or deploy while
  gathering evidence.
- For bugs involving live exchange state, exchange truth outranks local recovered
  state.

## GitNexus

The actual GitNexus repo name for this project is `LightFeeV2`.

Prefer GitNexus MCP tools when available. If MCP tools are unavailable, use the
GitNexus CLI equivalents. Prefer the `rtk` prefix for compact output; if `rtk`
is not installed in the current shell, run the same command without `rtk`.

Before relying on GitNexus for the current code:

1. Get the current commit:

   ```bash
   rtk git rev-parse HEAD
   ```

2. Call GitNexus `list_repos` and check repo `LightFeeV2`:
   - `lastCommit` must match the current `HEAD`.
   - `staleness.commitsBehind` must be `0`.

   CLI fallback:

   ```bash
   rtk npx gitnexus list
   rtk npx gitnexus status
   ```

3. If `staleness.commitsBehind > 0`, `lastCommit` differs from `HEAD`, symbols
   are missing, call relationships look wrong, or node versions do not match the
   current code, rebuild the index from the repo root:

   ```bash
   rtk npx gitnexus analyze --index-only --name LightFeeV2 --drop-embeddings .
   ```

4. Re-run GitNexus `list_repos` or the CLI fallback and confirm the index is
   fresh before running `impact`, `query`, `context`, or `detect_changes`.

Embedding rules:

- Default rebuilds must use `--index-only --drop-embeddings`.
- Do not use `--embeddings` by default.
- Do not use `--embeddings=0` as a way to disable embeddings. It enables
  embeddings and removes the generation cap.
- Use `--embeddings` only when the user explicitly asks for semantic search or
  better natural-language query accuracy.

Code-change rules:

- Before modifying any function, class, or method, run upstream impact analysis
  for the target symbol.
- Report direct callers, affected processes, and risk level to the user.
- If impact returns `HIGH` or `CRITICAL`, warn the user before editing.
- Before committing, confirm the GitNexus index is fresh and run
  `detect_changes`; verify the affected scope matches the intended change.

Docs-only edits that do not modify functions, classes, or methods do not require
symbol impact analysis.

## Root-Cause Repair Protocol

All bug fixes, regressions, compatibility repairs, production remediations, and
review follow-ups must follow
[`docs/root-cause-repair-protocol.md`](docs/root-cause-repair-protocol.md).

Mandatory rules:

- Define the authoritative contract, root-cause mechanism, full production path,
  and affected bug family before editing.
- Fix the earliest shared broken boundary; do not patch only the reported
  example or create a parallel implementation.
- Keep implementation minimal: reuse existing owners and abstractions, remove
  superseded duplicate logic when safe, and do not add speculative frameworks,
  configurability, compatibility layers, helpers, or files without a current
  caller or contract need.
- Build a counterexample matrix before implementation and cover complementary
  outcomes such as block/release, retry/terminal, and complete/partial evidence.
  Parameterize equivalent cases instead of adding repetitive tests.
- Add a real production-path RED/GREEN regression. Helper-only tests or tests
  that monkeypatch the method under repair are insufficient closure evidence.
- Search for duplicate predicates, copied reason lists, alternate APIs, legacy
  bypasses, and every state-transition/clear path in the same bug family.
- Run layered verification and an independent read-only closure review. Report
  incomplete, interrupted, or scoped test runs accurately.
- If a P0/P1 appears after a fix was declared complete, or the same bug family
  fails two fix-review cycles, trigger the protocol's repair-loop circuit
  breaker: stop point-fixing, mark the change not closed, and redo the contract,
  path, and counterexample analysis before further implementation.
- A GitNexus index matching `HEAD` does not include unstaged changes. Combine
  graph results with local diff/search analysis for the current working tree.
- Use the protocol's completion checklist and handoff format. If any mandatory
  gate is missing, say `implemented but not closed`, not `fixed`.
- During review, reject unnecessary code and over-design even when behavior is
  correct; require the smallest coherent root fix.
