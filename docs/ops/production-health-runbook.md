# Production Health Runbook

## Verify

Run on the production host:

```bash
cd /opt/lightfee-v2
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --json
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/check_process_singleton.py --strict
```

Expected:

- `ok: true`
- one live process
- one sidecar process
- sidecar snapshot with 7 venues
- current state `lifecycle=running`, `risk_mode=running`

## Capture Read-Only Evidence

Before remediation, deploy, restart, or manual state repair, capture read-only
evidence first. Keep the evidence compact enough to paste into a bug ledger entry:

```bash
cd /opt/lightfee-v2
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --json
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/check_process_singleton.py --strict
systemctl show lightfee-live.service lightfee-sidecar.service \
  --property=Id,ActiveState,SubState,ExecMainStartTimestamp,ExecMainPID
```

Record the current deploy identity (`git rev-parse --short HEAD` and
`.deploy_version`), live/sidecar process state, open/pending/recovery counts, and
the first failing health field. Do not include secrets, raw account identifiers,
or long journal excerpts in the bug ledger.

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
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --json"
```

Must show `ok: true`, no open orders, no pending entries, and no execution
recovery work. A durable close-accounting reconciliation is allowed only when
the report retains `background_close_reconciliation_pending: true`; it is not a
settled bill and must remain in the incident record until exact exchange
evidence reconciles it.

## Deploy

Run the auto-generated deploy script or manual equivalent:

```bash
python3 scripts/verify_deploy_manifest.py --generate-deploy --remote root@38.60.253.248 --path /opt/lightfee-v2 --ssh-port 2222
bash scripts/deploy.sh
```

The script syncs code, uploads `.deploy_manifest.json`, writes `.deploy_version`,
runs remote verification with `/opt/lightfee-v2/.venv/bin/python3`, restarts the
live services, and collects `diagnose_live.py --since-deploy` evidence. Any step
failure exits non-zero.

## Post-Deploy Verification

After every deploy, verify integrity on the remote host:

```bash
# 1. Git HEAD and .deploy_version must match the expected commit
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && echo \"HEAD: \$(git rev-parse --short HEAD)\" && echo \"VERSION: \$(cat .deploy_version)\""

# 2. Manifest integrity (single SSH call, checks all critical files)
python3 scripts/verify_deploy_manifest.py --remote root@38.60.253.248 --path /opt/lightfee-v2 --ssh-port 2222

# 3. Remote --check (manifest count and critical file hashes)
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_deploy_manifest.py --check /opt/lightfee-v2"

# 4. Systemd fragments must match repo templates
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "systemctl show lightfee-live.service lightfee-sidecar.service | grep -E '^(Id|ExecStart|ExecMainStartTimestamp)='"

# 5. Process start time must be later than code sync time
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "systemctl show lightfee-live.service | grep ExecMainStartTimestamp"

# 6. No-orders health probe (must not submit orders)
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --json"

# 7. Since-deploy diagnostic evidence (same interpreter as production)
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/diagnose_live.py --json --since-deploy"
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

Only resume from stale fail-closed when open positions, entry/pending execution
work, and residual repairs are zero. A background close-accounting
reconciliation may remain only when `verify_production_services.py` passes;
never remove it manually to make a health check green. Prefer the code-level
clean-start recovery. Manual state edits require a backup and must be followed
by `verify_production_services.py`.
