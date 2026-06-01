<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **LightFeeV2** (20166 symbols, 42709 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/LightFeeV2/context` | Codebase overview, check index freshness |
| `gitnexus://repo/LightFeeV2/clusters` | All functional areas |
| `gitnexus://repo/LightFeeV2/processes` | All execution flows |
| `gitnexus://repo/LightFeeV2/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

<!-- lightfee-v1-contract:start -->
# LightFee V1 Recovery / Close Semantic Contract

When a production bug is a V2/V1 semantic drift, V1 is authoritative. Do not
replace these rules with wider thresholds, manual state edits, or approximate
V2-only shortcuts.

## Passive Close / Recovery

- Exchange truth outranks local recovered state. Local matched quantities are
  evidence, not authority, once live position probes disagree.
- Before retrying a pending passive close from recovered or fallback state,
  probe both live legs and trusted open-order truth when available.
- If both live legs are flat and there are no trusted open orders, clear the
  pending passive close, local open position, and related last-error/recovery
  latch. Persist only after both pending and open state are removed.
- If live truth is one-sided, close the actual live nonzero exposure with
  reduce-only taker semantics. Do not derive the close target from stale local
  matched deltas.
- Terminal under-minimum or normalized-zero hedge branches must not schedule an
  identical retry loop. V1 buffers small fills while the maker is still live;
  once the maker is terminal or attempts are exhausted, it emits the terminal
  min-notional path and compensates/flattens from live exchange truth.
- Missing price evidence is not allowed to become an infinite retry loop. Use a
  V1-compatible live-truth compensation path when there is confirmed live
  one-sided exposure; if compensation cannot prove flat, remain fail-closed with
  structured evidence.

## Residual Repair

- Residual repair is normal runtime housekeeping, not a startup-only cleanup.
- Fetch live position for the repair venue. V2 may also fetch counter venue and
  open-order truth, but that safety check must not block a tradeable repair on
  the repair venue.
- Compute live excess relative to the V1 baseline. If live excess is zero and
  trusted open-order truth is clear, complete the repair and release the pair
  gate.
- If live excess is tradeable, submit one reduce-only IOC repair for the live
  excess even when the task was previously paused or attempts were exhausted.
- A paused or attempt-exhausted repair may be resumed only when live position
  truth and open-order truth are trusted, and repeated submit failures must still
  respect `next_attempt_ms` backoff.
- When no local open position remains but the repair venue has trusted nonzero
  live exposure, rebuild the close side from the signed live position before
  submitting reduce-only IOC. This is a V2 root-fix extension over V1's
  conservative paused-task behavior, not a reason to keep stale local repair
  side authoritative.
- If live excess is below official venue quantity/notional rules, terminalize
  the residual as dust and release the pair gate with the official metadata
  source in the evidence.
- If live truth or open-order truth is unavailable, keep fail-closed and emit a
  non-empty structured reason. Do not clear or mark green from missing truth.

## Required Closure Evidence

- Every production P0 must have a sanitized production fixture or fake-adapter
  harness that fails before the fix and passes after it.
- Branches that depend on real exchange state must have a read-only probe path.
  Probes may fetch positions, open orders, instrument metadata, account/risk
  metadata, and public market data; they must not submit, cancel, or mutate.
- A fix is not complete until the focused harness and the relevant probe/default
  probe guard both pass, and production current state no longer shows the stale
  open/pending/residual condition or has an explicitly documented dust terminal.

<!-- lightfee-v1-contract:end -->

<!-- rtk-skills:start -->
# RTK Skills — Auto-Invoke Rules

All RTK skills are installed at `~/.config/opencode/skills/`. When any scenario below matches, you **MUST** invoke the corresponding skill via the `skill` tool **before** taking any action. Do not wait to be asked.

## Must-Invoke Mapping

| Scenario | Skill | Trigger Keywords |
|----------|-------|-----------------|
| Writing or modifying Rust code (new filter, feature, bugfix) | `tdd-rust` | implement, add feature, new filter, fix bug, write code |
| Writing or modifying any code (any language) — TDD cycle | `rtk-tdd` | test, refactor, implement, bug fix |
| Reviewing or simplifying existing Rust code | `code-simplifier` | simplify, review code, clean up, refactor rust |
| Designing a new module, struct, or refactoring architecture | `design-patterns` | design, architecture, new module, refactor, pattern |
| Triage open issues (categorize, detect duplicates, risk) | `issue-triage` | triage issues, audit issues, issue list, bug triage |
| Triage open PRs (review, assess, comment) | `pr-triage` | triage PR, review PR, PR list, audit PR |
| Full triage — both issues and PRs | `rtk-triage` | full triage, triage all, project health |
| Deep review of a specific PR | `pr-review` | review PR, code review, PR # |
| Security audit or vulnerability check | `security-guardian` | security, vulnerability, injection, CVE, exploit |
| Performance optimization (startup, memory, token savings) | `performance` | slow, performance, optimize, benchmark, memory |
| Build, commit, push, version bump, release | `ship` | release, ship, deploy, bump version, publish |
| Repo status summary (PRs, issues, releases) | `repo-recap` | recap, summary, status, what happened |

## Rules

1. **Invoke BEFORE action.** When a user request matches a scenario, call `skill({name: "skill-name"})` first, then follow the skill's instructions.
2. **Multiple matches.** If more than one skill applies (e.g., writing Rust code matches both `tdd-rust` and `design-patterns`), invoke the most specific one first, then the others.
3. **Never skip.** Even if you think you know how to proceed, invoke the skill. The skill may contain project-specific rules you would otherwise miss.
4. **RTK project context.** These skills are written for the RTK Rust CLI project. When working on RTK code, always respect their Rust-specific conventions, testing patterns, and filter module structure.

<!-- rtk-skills:end -->
