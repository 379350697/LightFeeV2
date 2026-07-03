#!/usr/bin/env bash
set -euo pipefail

ROOT="${LIGHTFEE_ROOT:-/opt/lightfee-v2}"
PYTHON="${LIGHTFEE_PYTHON:-$ROOT/.venv/bin/python3}"
AUDIT_DIR="$ROOT/runtime/audits/trade_optimization"
LOG_DIR="$ROOT/logs"

mkdir -p "$AUDIT_DIR" "$LOG_DIR"
exec >>"$LOG_DIR/trade-optimization-report.log" 2>&1

LOCK_FILE="$AUDIT_DIR/.refresh.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf '%s trade_optimization_report skipped reason=already_running\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  exit 0
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
TMP_JSON="$AUDIT_DIR/latest.$RUN_ID.tmp.json"
TMP_CSV="$AUDIT_DIR/samples.$RUN_ID.tmp.csv"
TMP_REPORT="$AUDIT_DIR/report.$RUN_ID.tmp.md"

cleanup() {
  rm -f "$TMP_JSON" "$TMP_CSV" "$TMP_REPORT"
}
trap cleanup EXIT

printf '%s trade_optimization_report start run_id=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID"

cd "$ROOT"
env PYTHONPATH="$ROOT" "$PYTHON" scripts/analyze_trade_optimization_samples.py \
  --history all \
  --normal-only \
  --include-counterfactual \
  --json "$TMP_JSON" \
  --csv "$TMP_CSV" \
  --report-md "$TMP_REPORT"

"$PYTHON" - "$TMP_JSON" <<'PY'
import json
import sys

required_pnl_fields = (
    "price_pnl_quote",
    "funding_pnl_quote",
    "entry_fee_quote",
    "exit_fee_quote",
    "rebate_adjustment_quote",
    "net_pnl_quote",
    "notional_quote",
)


def decimal_positive(value):
    try:
        return float(value) > 0
    except Exception:
        return False


def snapshot_has_quote(snapshot):
    if not isinstance(snapshot, dict):
        return False
    return decimal_positive(snapshot.get("bid_price")) and decimal_positive(
        snapshot.get("ask_price")
    )


with open(sys.argv[1], "r", encoding="utf-8") as handle:
    report = json.load(handle)

bad_samples = []
for sample in report.get("samples") or []:
    if not isinstance(sample, dict):
        continue
    gaps = []
    pnl = sample.get("pnl") if isinstance(sample.get("pnl"), dict) else {}
    for field in required_pnl_fields:
        value = pnl.get(field)
        if value is None or value == "":
            gaps.append(f"missing_required_pnl_field:{field}")
        elif field == "notional_quote" and not decimal_positive(value):
            gaps.append(f"missing_required_pnl_field:{field}")
    refs = pnl.get("evidence_refs")
    if not isinstance(refs, list) or not refs:
        gaps.append("missing_required_pnl_field:evidence_refs")
    market = sample.get("market") if isinstance(sample.get("market"), dict) else {}
    if not snapshot_has_quote(market.get("entry_snapshot")):
        gaps.append("missing_entry_market_snapshot")
    if not snapshot_has_quote(market.get("exit_snapshot")):
        gaps.append("missing_exit_market_snapshot")
    features = sample.get("features") if isinstance(sample.get("features"), dict) else {}
    if features.get("time_to_funding_ms") is None:
        gaps.append("missing_time_to_funding")
    if gaps:
        bad_samples.append(
            {
                "position_id": sample.get("position_id"),
                "symbol": sample.get("symbol"),
                "gaps": sorted(set(gaps)),
            }
        )

summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
if bad_samples:
    print(
        "trade_optimization_report validation_failed invalid_sample_count=%s"
        % len(bad_samples)
    )
    for row in bad_samples:
        print(
            "trade_optimization_report invalid_sample position_id=%s symbol=%s gaps=%s"
            % (
                row.get("position_id"),
                row.get("symbol"),
                ",".join(row.get("gaps") or []),
            )
        )
    sys.exit(1)

print(
    "trade_optimization_report validation_pass field_gap_excluded=%s required_field_gaps=%s"
    % (
        summary.get("field_gap_excluded_count", 0),
        summary.get("required_field_gap_count", 0),
    )
)
PY

mv -f "$TMP_JSON" "$AUDIT_DIR/latest.json"
mv -f "$TMP_CSV" "$AUDIT_DIR/samples.csv"
mv -f "$TMP_REPORT" "$AUDIT_DIR/report.md"

"$PYTHON" -c 'import json, sys; p=json.load(open(sys.argv[1])); s=p.get("summary", {}); print("trade_optimization_report summary normal=%s excluded=%s field_gap_excluded=%s required_field_gaps=%s event_count=%s recommendations=%s" % (s.get("normal_sample_count"), s.get("excluded_position_count"), s.get("field_gap_excluded_count"), s.get("required_field_gap_count"), s.get("event_count"), len(p.get("recommendations", []))))' "$AUDIT_DIR/latest.json"

printf '%s trade_optimization_report complete run_id=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID"
trap - EXIT
