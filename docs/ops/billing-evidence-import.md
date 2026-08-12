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
