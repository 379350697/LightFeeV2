# Bug Ledger Index

This index tracks LightFeeV2 production/parity bugs with stable fingerprints. It follows the V1 bug-ledger style so regressions can be tied back to prior fixes, failed attempts, and verification evidence.

| Bug ID | Status | Severity | Component | Fingerprint | First Seen | First Seen Commit | Fixed In | Last Verified | Related Refactor | Latest Outcome |
|---|---|---:|---|---|---|---|---|---|---|---|
| [BUG-20260514-v2-v1-parity-root-fix-loop](BUG-20260514-v2-v1-parity-root-fix-loop.md) | fixed | high | `entry-local-l2`, `sidecar-candidate-contract`, `order-reconciliation`, `bitget-l2-metadata`, `dryrun-audit`, `test-coverage`, `bybit-execution-side`, `bitget-quantity-fallback` | `v2.v1-parity.surface-copy.not-data-contract + fake-tests.green.real-path-red` | 2026-05-14 | `72ae905` review target, with prior issues visible in cloud/runtime audit loop | working tree (post-`c378352`) | 2026-05-14 local: `pytest -q` 2080 passed; targeted probes passed | `V1-to-V2 execution and venue parity replication` | All known P1/P2 residuals closed: schema-v2 candidate enrichment derives-or-fails-closed, Bitget quantity fallback covers V1 fields (`fillQty`, `filled_amount`, `size`) plus V2 extras, Bybit execution/order-status side is fail-closed, and ledger index/detail status is consistent. |

## Query Hints

- Find local-L2 parity bugs: `rg "entry-local-l2|local_l2|prewarm" docs/bugs`
- Find ineffective attempts: `rg "Ineffective|Why Ineffective|No Effect|half-effective" docs/bugs`
- Find unresolved items: `rg "Residual|Open|not closed|follow-up" docs/bugs`
