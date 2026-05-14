# Bug Ledger Index

This index tracks LightFeeV2 production/parity bugs with stable fingerprints. It follows the V1 bug-ledger style so regressions can be tied back to prior fixes, failed attempts, and verification evidence.

| Bug ID | Status | Severity | Component | Fingerprint | First Seen | First Seen Commit | Fixed In | Last Verified | Related Refactor | Latest Outcome |
|---|---|---:|---|---|---|---|---|---|---|---|
| [BUG-20260514-v2-v1-parity-root-fix-loop](BUG-20260514-v2-v1-parity-root-fix-loop.md) | partially fixed; residual P1/P2 open | high | `entry-local-l2`, `sidecar-candidate-contract`, `order-reconciliation`, `bitget-l2-metadata`, `dryrun-audit`, `test-coverage` | `v2.v1-parity.surface-copy.not-data-contract + fake-tests.green.real-path-red` | 2026-05-14 | `72ae905` review target, with prior issues visible in cloud/runtime audit loop | `eb9f793` partially; residual follow-up required | `V1-to-V2 execution and venue parity replication` | Bitget L2 guard, audit script, V1 compat preservation, pairing-generated CandidateInput, and basic HTTP reconciliation are improved. Remaining open: Bybit execution side hardcoded to BUY, Bybit retCode business errors return None, and legacy/external V2 snapshots with missing candidate fields still prewarm-block. |

## Query Hints

- Find local-L2 parity bugs: `rg "entry-local-l2|local_l2|prewarm" docs/bugs`
- Find ineffective attempts: `rg "Ineffective|Why Ineffective|No Effect|half-effective" docs/bugs`
- Find unresolved items: `rg "Residual|Open|not closed|follow-up" docs/bugs`
