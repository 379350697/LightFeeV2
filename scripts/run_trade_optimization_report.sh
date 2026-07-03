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

mv -f "$TMP_JSON" "$AUDIT_DIR/latest.json"
mv -f "$TMP_CSV" "$AUDIT_DIR/samples.csv"
mv -f "$TMP_REPORT" "$AUDIT_DIR/report.md"

"$PYTHON" -c 'import json, sys; p=json.load(open(sys.argv[1])); s=p.get("summary", {}); print("trade_optimization_report summary normal=%s excluded=%s event_count=%s recommendations=%s" % (s.get("normal_sample_count"), s.get("excluded_position_count"), s.get("event_count"), len(p.get("recommendations", []))))' "$AUDIT_DIR/latest.json"

printf '%s trade_optimization_report complete run_id=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID"
trap - EXIT
