# Documentation-Only Validation Guidance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make documentation-only changes easier to close by documenting the exact low-risk validation and handoff checklist.

**Architecture:** This is a documentation-only maintenance change. It updates the existing validation strategy document rather than adding new tooling or changing runtime behavior.

**Tech Stack:** Markdown, existing `scripts/validate_change.py` smoke profile, Git diff checks.

---

### Task 1: Document Doc-Only Validation Closure

**Files:**
- Modify: `docs/testing-validation-strategy.md`

- [ ] **Step 1: Add a documentation-only section**

Insert this section after the default flow command block in `docs/testing-validation-strategy.md`:

````md
## Documentation-Only Changes

For documentation-only changes, use the `smoke` profile as the default gate:

```bash
python3 scripts/validate_change.py --profile smoke
```

This keeps closure cheap while still checking Python import/compile health and
diff whitespace. Do not run focused pytest profiles for a docs-only patch unless
the documentation change claims a specific runtime behavior, test result, or
bug-fix status that needs fresh proof.

Before handoff, confirm tracked and untracked paths are documentation-only:

```bash
git status --short
```

If bug-ledger docs were added or materially changed, run `npx gitnexus analyze`
only when the next workflow needs a fresh GitNexus index.
````

- [ ] **Step 2: Validate the docs-only change**

Run:

```bash
python3 scripts/validate_change.py --profile smoke
```

Expected: compileall passes and `git diff --check` passes.

- [ ] **Step 3: Confirm scope**

Run:

```bash
git status --short
```

Expected: only Markdown files under `docs/` are listed.
