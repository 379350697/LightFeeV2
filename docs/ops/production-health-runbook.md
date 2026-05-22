# Production Health Runbook

## Verify

Run on the production host:

```bash
cd /opt/lightfee-v2
python3 scripts/verify_production_services.py --json
python3 scripts/check_process_singleton.py --strict
```

Expected:

- `ok: true`
- one live process
- one sidecar process
- sidecar snapshot with 7 venues
- current state `lifecycle=running`, `risk_mode=running`

## Remediate Sidecar Config Drift

The sidecar runs as a Python V2 service (not Rust V1 bridge). Install the versioned template:

```bash
sudo cp deploy/systemd/lightfee-sidecar.service /etc/systemd/system/lightfee-sidecar.service
sudo systemctl daemon-reload
sudo systemctl restart lightfee-sidecar.service
```

## Pre-Deploy Safety Gate

Before deploying, verify no open work on remote:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && python3 scripts/verify_production_services.py --json"
```

Must show `ok: true`, no open orders, no pending entries, no recovery work.

## Deploy

Run the auto-generated deploy script or manual equivalent:

```bash
python3 scripts/verify_deploy_manifest.py --generate-deploy --remote root@38.60.253.248 --path /opt/lightfee-v2
bash scripts/deploy.sh
```

The script syncs code, uploads `.deploy_manifest.json`, writes `.deploy_version`, and runs remote verification. Any step failure exits non-zero.

## Post-Deploy Verification

After every deploy, verify integrity on the remote host:

```bash
# 1. Git HEAD and .deploy_version must match the expected commit
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && echo \"HEAD: \$(git rev-parse --short HEAD)\" && echo \"VERSION: \$(cat .deploy_version)\""

# 2. Manifest integrity (single SSH call, checks all critical files)
python3 scripts/verify_deploy_manifest.py --remote root@38.60.253.248 --path /opt/lightfee-v2

# 3. Remote --check (manifest count and critical file hashes)
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && python3 scripts/verify_deploy_manifest.py --check /opt/lightfee-v2"

# 4. Systemd fragments must match repo templates
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "systemctl show lightfee-live.service lightfee-sidecar.service | grep -E '^(Id|ExecStart|ExecMainStartTimestamp)='"

# 5. Process start time must be later than code sync time
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "systemctl show lightfee-live.service | grep ExecMainStartTimestamp"

# 6. No-orders health probe (must not submit orders)
ssh -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && python3 scripts/verify_production_services.py --json"
```

All checks must pass before marking a deploy complete. If any check fails, do NOT resume trading — diagnose and re-deploy.

## Remediate DNS Drift

Install the NetworkManager DNS template or apply equivalent persistent resolver configuration:

```bash
sudo cp deploy/network/NetworkManager-lightfee-dns.conf /etc/NetworkManager/conf.d/99-lightfee-dns.conf
sudo systemctl reload NetworkManager || sudo systemctl restart NetworkManager
getent hosts www.okx.com
```

## Resume Live Only If Safe

Only resume from stale fail-closed when open/pending/recovery work is zero. Prefer the code-level clean-start recovery. Manual state edits require a backup and must be followed by `verify_production_services.py`.
