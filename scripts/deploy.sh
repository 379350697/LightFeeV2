#!/bin/bash
# Auto-generated deploy script — syncs all git-tracked files to remote
# Generated: 1690ee7
set -euo pipefail

REMOTE="root@38.60.253.248:/opt/lightfee-v2"
REMOTE_HOST="root@38.60.253.248"
REMOTE_PATH="/opt/lightfee-v2"
LOCAL="/Users/wl/projects/LightFeeV2"
SSH_OPTS="-p 2222 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/private/tmp/lightfee_known_hosts -o ConnectTimeout=10"
RSYNC_RSH="ssh $SSH_OPTS"

echo "=== Generating deploy manifest ==="
python3 "$LOCAL/scripts/verify_deploy_manifest.py" --local

echo "=== Syncing files to $REMOTE ==="
rsync -avz -e "$RSYNC_RSH" --delete --exclude .venv --exclude __pycache__ --exclude *.pyc --exclude .git --exclude config/live.toml --exclude config/*.local.toml --exclude runtime/ --exclude logs/ --exclude .env --exclude *.log --exclude .DS_Store --exclude .claude/ --exclude docs/ "$LOCAL/" "$REMOTE/"

echo "=== Uploading deploy manifest ==="
scp $SSH_OPTS "$LOCAL/.deploy_manifest.json" "$REMOTE/.deploy_manifest.json"

echo "=== Writing .deploy_version ==="
echo "1690ee7" | ssh $SSH_OPTS root@38.60.253.248 "cat > /opt/lightfee-v2/.deploy_version"

echo "=== Verifying deployment integrity on remote ==="
ssh $SSH_OPTS root@38.60.253.248 "cd /opt/lightfee-v2 && python3 scripts/verify_deploy_manifest.py --check /opt/lightfee-v2"

echo "=== Deploy complete: 1690ee7 ==="
