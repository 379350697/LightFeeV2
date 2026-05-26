# Production Health Read-Only Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a small read-only evidence section to the production health runbook so operators know what to capture before remediating or redeploying.

**Architecture:** This is a documentation-only change. It extends the existing production health runbook with a short evidence checklist and keeps all commands read-only.

**Tech Stack:** Markdown, existing production health scripts, existing `scripts/validate_change.py` smoke profile.

---

### Task 1: Add Read-Only Evidence Guidance

**Files:**
- Modify: `docs/ops/production-health-runbook.md`

- [ ] **Step 1: Add the runbook section**

Insert this section after the `## Verify` expected-results list in `docs/ops/production-health-runbook.md`:

````md
## Capture Read-Only Evidence

Before remediation, deploy, restart, or manual state repair, capture read-only
evidence first. Keep the evidence compact enough to paste into a bug ledger entry:

```bash
cd /opt/lightfee-v2
python3 scripts/verify_production_services.py --json
python3 scripts/check_process_singleton.py --strict
systemctl show lightfee-live.service lightfee-sidecar.service \
  --property=Id,ActiveState,SubState,ExecMainStartTimestamp,ExecMainPID
```

Record the current deploy identity (`git rev-parse --short HEAD` and
`.deploy_version`), live/sidecar process state, open/pending/recovery counts, and
the first failing health field. Do not include secrets, raw account identifiers,
or long journal excerpts in the bug ledger.
````

- [ ] **Step 2: Validate the docs-only change**

Run:

```bash
python3 scripts/validate_change.py --profile smoke
```

Expected: compileall and `git diff --check` pass.

- [ ] **Step 3: Confirm scope**

Run:

```bash
git status --short
```

Expected: this task only adds or modifies Markdown files. Existing unrelated code changes may remain in the worktree and must be reported rather than reverted.

