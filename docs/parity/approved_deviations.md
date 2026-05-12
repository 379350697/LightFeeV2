# Approved Deviations Ledger

> Each deviation must be intentional, documented, tested, and operator-visible.
> Deviations referenced in `v1_semantic_contract_catalog.md` that are missing from
> this ledger will cause `tests/parity/test_contract_catalog_coverage.py` to fail.

## Schema

| Field | Required | Description |
|-------|----------|-------------|
| id | yes | Unique deviation ID (DEV-NNN) |
| area | yes | Category from the contract catalog |
| v1_behavior | yes | What V1 did |
| v2_behavior | yes | What V2 does instead |
| reason | yes | Why this deviation is intentional |
| risk | yes | Risk level: LOW / MEDIUM / HIGH |
| operator_impact | yes | How this affects operators |
| test_coverage | yes | Test that validates this deviation is intentional |

---

## DEV-001: Chillybot Inputs Removed

| Field | Value |
|-------|-------|
| **id** | DEV-001 |
| **area** | opportunity-input |
| **v1_behavior** | V1 consumed Chillybot feed data as an additional opportunity signal source, with its own pairing, freshness tracking, and health domain. |
| **v2_behavior** | V2 does not implement Chillybot feed integration. The Chillybot-related config keys, health domain, and opportunity provider branch are absent. |
| **reason** | Chillybot was a proprietary internal feed retired before V2 development began. Its signals have been subsumed by the sidecar snapshot's coarse-scan domain. Implementing it in V2 would add maintenance burden with no production value. |
| **risk** | LOW. No production deployment of V2 runs Chillybot. The sidecar scan mode provides equivalent coverage. |
| **operator_impact** | Operators cannot configure `chillybot.*` config keys. Any V1 config referencing Chillybot will produce a clear "unknown key" error in V2 rather than silent ignore. Config migration notes document the removal. |
| **test_coverage** | `tests/sidecar/test_opportunity_input_semantics.py::test_chillybot_removal_is_explicit` |

---

## DEV-002: Bitget/Gate risk_health Capability Drift (RESOLVED — Fixed)

| Field | Value |
|-------|-------|
| **id** | DEV-002 |
| **area** | venue-capabilities |
| **v1_behavior** | V1 marked Bitget and Gate `risk_health` as **unsupported** — these venues did not expose a private REST endpoint that the engine could use for risk-oriented health checks (position, balance, margin). |
| **v2_behavior** | V2 initially marked Bitget and Gate `risk_health` as supported. As of the V1 parity fix, they are now marked **UNSUPPORTED** in both `VenueCapabilities.for_venue()`, `VenueCapabilityFlags`, and the adapter-level `supports_risk_health` property. |
| **reason** | This was a known implementation drift from early V2 venue adapter scaffolding. Fixed to match V1 semantics. The capability declarations are now honest. |
| **risk** | ~~MEDIUM~~ → LOW (fix applied). Engine code will not attempt risk_health evaluation on Bitget/Gate. |
| **operator_impact** | Bitget and Gate venues no longer participate in risk-health lanes. This matches V1 production behavior. |
| **test_coverage** | `tests/venues/test_v1_capability_matrix.py::test_bitget_gate_risk_health_deviation` |

---

## Summary

| ID | Area | Risk | Status |
|----|------|------|--------|
| DEV-001 | opportunity-input | LOW | Approved |
| DEV-002 | venue-capabilities | LOW (fixed) | Resolved |
