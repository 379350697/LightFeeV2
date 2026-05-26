# Testing Validation Strategy

Use focused validation for bug fixes. Full pytest is not the default gate for every
local repair because broad suites can be slow or silent when a fixture path stalls.

## Default Flow

For normal bug-fix closure, run the smallest profile that matches the touched
surface:

```bash
python3 scripts/validate_change.py --profile close
```

Every profile includes:

- `python3 -m compileall -q lightfee tests scripts`
- `git diff --check`
- focused pytest commands for that profile
- per-step timeout
- heartbeat output when a step is silent
- per-step logs under `/tmp/lightfee-validate-change-*`

## Profiles

| Profile | Use when |
|---|---|
| `smoke` | Documentation-only changes or a quick sanity check |
| `close` | Close/passive-close/recovery bug fixes |
| `venue-bybit` | Bybit tick, quantity, reduce-only, or duplicate-id fixes |
| `venue-okx` | OKX contract units, residual repair, position sign, or body schema fixes |
| `venue-hyperliquid` | Hyperliquid IOC, L2 fallback, cloid, signing, or rounding fixes |
| `venue-aster` | Aster reduce-only, open-order, recovered close, or diagnose routing fixes |
| `local-l2` | Local-L2 bootstrap/rebuild/readiness fixes |
| `full` | Pre-merge, release, or nightly verification only |

List commands without running them:

```bash
python3 scripts/validate_change.py --profile close --dry-run
```

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

Run a broad suite with timeout and keep going after failures:

```bash
python3 scripts/validate_change.py --profile full --keep-going
```

## Closure Rule

For a root-fix bug ledger entry, record:

- the RED/GREEN test that proves the bug branch
- the focused validation profile and result
- `compileall` and `git diff --check` status
- GitNexus `detect_changes` risk
- production read-only evidence when a live symptom was involved

Do not claim full-suite success unless `full` actually completed.
