# Bug Ledger Handoff Checklist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a small bug-ledger handoff checklist so documentation-only incident updates close with consistent index, evidence, and validation notes.

**Architecture:** This is a documentation-only change. It extends the existing bug ledger README instead of creating new process tooling.

**Tech Stack:** Markdown, existing bug ledger docs, existing `scripts/validate_change.py` smoke profile.

---

### Task 1: Add Bug Ledger Handoff Checklist

**Files:**
- Modify: `docs/bugs/README.md`

- [ ] **Step 1: Add the checklist**

Append this subsection under `## Maintenance` in `docs/bugs/README.md`:

````md
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
````

- [ ] **Step 2: Validate the docs-only change**

Run:

```bash
python3 scripts/validate_change.py --profile smoke
```

Expected: compileall and `git diff --check` pass. If unrelated existing changes make the gate fail, report the exact failure and keep the doc-only change unchanged.

- [ ] **Step 3: Confirm scope**

Run:

```bash
git status --short
```

Expected: this task only adds or modifies Markdown files. Existing unrelated code changes may remain in the worktree and must be reported rather than reverted.

