# Agent Instructions - LightFeeV2

These instructions apply to `/Users/wl/projects/LightFeeV2`.

This project is used by both Codex and Claude Code. Keep `AGENTS.md` and
`CLAUDE.md` synchronized; `AGENTS.md` is the source of truth when the two files
disagree.

DO NOT send optional commentary

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

## Context Hygiene

- Default searches must exclude `.gitnexus/**` and large audit artifacts such as
  `runtime/audits/**` unless the user explicitly asks to inspect them.
- Diagnostic commands must emit field-focused output instead of full dumps. Use
  filters such as `jq`, targeted SQL columns, bounded `rg`, and explicit
  `max_output_tokens` limits for large JSON, JSONL, logs, or session files.

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
