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
- process-owned FD and `CLOSE_WAIT` counts below the health limits
- no venue with more than three private-WS worker starts in the last hour
- a Binance listenKey success no older than 35 minutes and not expired

Exception: a known, physically flat close-accounting evidence debt intentionally
keeps ordinary health at `ok: false`.  In that case the only acceptable shape is
one `pending_close_owner_present` warning,
`background_close_reconciliation_pending: true`, and
`deployment_acceptable: true`.  Every close-reconciliation owner must be
explicitly classified `evidence_debt`; active, mixed, compact-unknown, or
truth-incomplete owner sets are not eligible.  This is still an unsettled
accounting warning, not normal-green health.

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

## Continuous Resource Gate

The deployment installs `lightfee-production-health.timer`. It runs the same
read-only deployment-acceptance verifier every five minutes and writes a failed
oneshot result to the system journal when resource evidence is missing or bad.
It never submits, cancels, or alters orders.

```bash
systemctl is-active lightfee-production-health.timer
systemctl list-timers lightfee-production-health.timer
systemctl status lightfee-production-health.service --no-pager
journalctl -u lightfee-production-health.service -n 100 --no-pager
```

The collector attributes `CLOSE_WAIT` sockets by each service PID's socket
inode. Do not diagnose from a global host `CLOSE_WAIT` count: unrelated
processes would be a false positive. A failure is fail-closed for deployment
acceptance; inspect its JSON report before restarting either trading service.

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
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --deployment-acceptance --json"
```

Must show `deployment_acceptable: true`, no open orders, no pending entries, and
no execution recovery work.  A fully clean state also shows `ok: true`.  A
durable close-accounting reconciliation is allowed only when `ok: false` is
caused by the sole `pending_close_owner_present` warning and the report retains
`background_close_reconciliation_pending: true`; every reconciliation owner
must be `evidence_debt`, with high-confidence flat/no-open-order truth and no
execution/residual owner.  Active or mixed queues are rejected even when the
V1 runtime lifecycle may remain `running` to manage background close work.  It
is not a settled bill and must remain in the incident record until exact
exchange evidence reconciles it. Never use `--deployment-acceptance` as a
general health-suppression flag.

## Deploy

Run the auto-generated deploy script or manual equivalent:

```bash
python3 scripts/verify_deploy_manifest.py --generate-deploy --remote root@38.60.253.248 --path /opt/lightfee-v2 --ssh-port 2222
bash scripts/deploy.sh
```

The script syncs code, uploads `.deploy_manifest.json`, writes `.deploy_version`,
runs remote verification with `/opt/lightfee-v2/.venv/bin/python3`, installs the
health timer, restarts the live services, runs the health gate once immediately,
and collects `diagnose_live.py --since-deploy` evidence. Any step failure exits
non-zero.

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

# 6. No-orders deployment-acceptance probe (must not submit orders)
ssh -p 2222 -o BatchMode=yes -o ConnectTimeout=10 root@38.60.253.248 \
  "cd /opt/lightfee-v2 && PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --deployment-acceptance --json"

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
for that exact case, “passes” means the explicit `--deployment-acceptance`
result is true while ordinary `ok` remains false.  Never remove it manually to
make a health check green. Prefer the code-level clean-start recovery. Manual
state edits require a backup and must be followed by the ordinary verifier and,
when deployment is intended, the explicit deployment-acceptance verifier.

For an explicitly authorized, audited offline recovery, every state-mutating
`lightfee-ops` command requires the exact deployed persistence pair. It never
guesses `data/journal.jsonl` or `data/snapshot.json` from a working directory
or environment variable. On this deployment that pair is
`runtime/live-events.jsonl` and `runtime/live-state.json`:

```bash
cd /opt/lightfee-v2
systemctl stop lightfee-live
backup_dir=/root/lightfee-recovery-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup_dir"
cp -a runtime/live-events.jsonl runtime/live-state.json "$backup_dir"
find runtime -maxdepth 1 -type f -name 'live-events.jsonl.*' -exec cp -a {} "$backup_dir"/ \;
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/lightfee-ops \
  resume-if-safe \
  --event-log-path runtime/live-events.jsonl \
  --snapshot-path runtime/live-state.json
systemctl start lightfee-live
```

Record the command output, the appended `ops.command_applied` journal event,
and post-start verifier output. `discover-binance-close-evidence` is the only
read-only `lightfee-ops` command and needs only its snapshot input.
