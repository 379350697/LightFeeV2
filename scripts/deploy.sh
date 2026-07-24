#!/usr/bin/env bash
# Version-controlled deploy entrypoint. Generates a fresh script per execution.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REMOTE_HOST="${LIGHTFEE_DEPLOY_REMOTE_HOST:-root@38.60.253.248}"
REMOTE_PATH="${LIGHTFEE_DEPLOY_REMOTE_PATH:-/opt/lightfee-v2}"
SSH_PORT="${LIGHTFEE_DEPLOY_SSH_PORT:-2222}"
GENERATED_SCRIPT="$(mktemp "${TMPDIR:-/tmp}/lightfee-deploy.XXXXXX")"

cleanup() {
  rm -f "$GENERATED_SCRIPT"
}
trap cleanup EXIT

python3 "$SCRIPT_DIR/verify_deploy_manifest.py" --generate-deploy \
  --output "$GENERATED_SCRIPT" \
  --remote "$REMOTE_HOST" \
  --path "$REMOTE_PATH" \
  --ssh-port "$SSH_PORT"

LIGHTFEE_DEPLOY_LOCAL="${LIGHTFEE_DEPLOY_LOCAL:-$PROJECT_ROOT}" bash "$GENERATED_SCRIPT"
