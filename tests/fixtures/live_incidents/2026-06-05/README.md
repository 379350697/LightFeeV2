# 2026-06-05 Exchange Truth Recovery Ledger Fixtures

These sanitized fixtures capture the current V1-parity recovery ledger incident
family. They intentionally omit account, credential, and runtime file details.

- `trxusdt_open_order_local_flat.json`: local runtime is flat while Bybit still
  has a live non-reduce maker order.
- `seiusdt_positive_fill_local_false_flat.json`: local runtime is flat while
  positive maker fill evidence proves the entry is not ledger-proven flat.

Expected recovery semantics:

- local flat is not accepted without exchange-truth proof;
- non-reduce live maker orders require a runtime owner or become blocking
  `orphan_maker_order` work;
- positive fill evidence remains blocking until terminalized by the V1-parity
  recovery path.
