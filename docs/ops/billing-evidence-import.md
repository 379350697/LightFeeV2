# Historical Billing Evidence Import

Use this procedure only for a persisted `evidence_debt` close-reconciliation
owner whose original exchange records are available. It is a repair of
historical accounting evidence, not an order-management command: it does not
submit, cancel, or close an order.

The import has one business boundary:

1. an operator supplies one auditable evidence pack for one exact owner;
2. the state gate validates the runtime-required accounting snapshot and both
   close-leg identities, then replaces only that existing `evidence_debt` owner;
3. the live runtime still fetches exchange execution history using the supplied
   `order_id`/`client_order_id`; only that result can emit `exit.reconciled`.

Never infer a venue, order ID, fill, fee, or owner from symbol, timestamp, or
quantity. One evidence pack cannot target multiple owners.

## Evidence Pack

The file is one UTF-8 JSON object. `evidence_reference` must identify the
retained exchange export or case record; do not put credentials or raw account
identifiers in it.

```json
{
  "schema_version": 1,
  "evidence_reference": "exchange-export:case-YYYYMMDD-01",
  "reconciliation": {
    "position_id": "persisted-owner-id",
    "kind": "final",
    "closed_at_ms": 1700000000000,
    "position_snapshot": {
      "position_id": "persisted-owner-id",
      "symbol": "EXAMPLEUSDT",
      "long_venue": "bybit",
      "short_venue": "okx",
      "long_quantity": 2.0,
      "short_quantity": 2.0,
      "matched_quantity": 2.0,
      "long_entry_price": 10.0,
      "short_entry_price": 11.0,
      "long_entry_fee_quote": 0.01,
      "short_entry_fee_quote": 0.02,
      "total_entry_fee_quote": 0.03,
      "entry_fee_evidence_complete": true,
      "captured_funding_quote": 0.0,
      "opened_at_ms": 1699999990000
    },
    "long_legs": [
      {
        "venue": "bybit",
        "order_id": "long-close-order-id",
        "client_order_id": "long-close-client-id",
        "quantity": 2.0,
        "average_price": 12.0,
        "fee_quote": 0.02
      }
    ],
    "short_legs": [
      {
        "venue": "okx",
        "order_id": "short-close-order-id",
        "client_order_id": "short-close-client-id",
        "quantity": 2.0,
        "average_price": 9.0,
        "fee_quote": 0.02
      }
    ]
  }
}
```

The owner tuple (`position_id`, `kind`, `closed_at_ms`) must match exactly one
existing queued debt. Snapshot symbol and venues must agree with that owner,
every leg venue must match its snapshot route, and entry-fee evidence must be
explicitly complete. The CLI computes and journals a SHA-256 hash of the
canonical evidence pack.

### Legacy Orphan Partial Terminalization

Ordinary `partial` owners are non-terminal. The sole exception is a historical
orphan whose two exchange close executions have independently been proven.
Set `"kind": "partial"` and add `"terminalize_orphan_partial": true` in its
`reconciliation`. The state gate then permits the transition only when all of
the following are true:

- the tuple identifies an existing `evidence_debt` partial, with no local open
  position and no later final owner;
- the evidence contains non-empty exact-identity legs for both venues;
- each leg's aggregate quantity exactly equals its original paired snapshot
  quantity.

This flag does not make the supplied quantities or prices authoritative. The
import remains a pending final reconciliation, and the live runtime must
re-fetch both order identities from the venues before it writes
`exit.reconciled`. A missing, partial, or mismatched re-fetch retains the debt.
Do not use this flag for a time/side/quantity candidate; candidate discovery is
not exact execution evidence.

## Read-Only Binance Candidate Discovery

For a Binance `missing_close_order_identity` debt with no retained exact order
key, `lightfee-ops discover-binance-close-evidence` can narrow an offline
investigation using a separately captured `allOrders` JSON export. It compares
only the persisted owner, symbol, close side, `reduceOnly=TRUE`, full executed
quantity, and a bounded close-time window. It is deliberately **not** an
evidence import: it does not call an exchange, take the writer lease, open a
journal, write a snapshot, or change accounting state.

For a `partial` owner it never treats the original position quantity as the
close quantity. The candidate output records whether its quantity came from an
unidentified close leg, recorded close total, or the exact opposite close leg;
if none exists, it returns `missing_expected_quantity` instead of guessing.

```bash
cd /opt/lightfee-v2
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/lightfee-ops \
  discover-binance-close-evidence \
  --snapshot-path runtime/live-state.json \
  --orders-file /secure/binance-all-orders.json \
  --position-id persisted-owner-id \
  --kind partial \
  --closed-at-ms 1700000000000
```

The output is candidate-discovery data only. `unique_candidate_requires_operator_evidence`
means an operator must retrieve and retain the exact execution history for that
returned order ID/client ID, verify fill price, quantity, and fees, and then
create one ordinary evidence pack. `ambiguous_candidates` and `no_candidate`
must remain unresolved; never copy a candidate into an evidence pack without
that exact exchange verification.

## Production Procedure

Use the `event_log_path` and `snapshot_path` from the deployed live config. Do
not use the CLI defaults unless that config actually uses `data/journal.jsonl`
and `data/snapshot.json`.

1. Capture the read-only health/diagnose output and retain the original
   exchange exports. Confirm the target is still `evidence_debt` and only one
   owner matches the tuple.
2. Stop the live service and keep a timestamped copy of both persistence files.
   Every state-mutating `lightfee-ops` command and `lightfee-live` take the
   same single-host writer lease. If the service still owns the persistence
   pair, the import fails before reading or changing either file. Treat
   `lightfee-ops` as an offline maintenance CLI; never remove a `.writer.lock`
   file to bypass this check: a stale file is harmless because the kernel lock
   is released when its owning process exits.
3. Apply one file in the maintenance window:

   ```bash
   cd /opt/lightfee-v2
   PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/lightfee-ops \
     import-billing-evidence --file /secure/evidence-owner-01.json --apply \
     --event-log-path runtime/live-events.jsonl --snapshot-path runtime/live-state.json
   ```

4. Restart the live service. Its normal close runtime performs the exact
   exchange order-history lookup; importing the file is never the terminal
   accounting decision.
5. Verify that the journal records `exit.billing_evidence_imported` followed by
   either `exit.reconciled` with complete venue evidence, or a visible
   fail-closed reconciliation outcome. Do not delete a record merely because
   it remains unresolved.

Repeat independently for every historical owner. The current four debt owners
remain non-green until four separately verifiable evidence packs have passed
the same workflow.
