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

Install the versioned sidecar service template, then reload systemd:

```bash
sudo cp deploy/systemd/lightfee-sidecar-rust-v1.service /etc/systemd/system/lightfee-sidecar.service
sudo systemctl daemon-reload
sudo systemctl restart lightfee-sidecar.service
```

## Remediate DNS Drift

Install the NetworkManager DNS template or apply equivalent persistent resolver configuration:

```bash
sudo cp deploy/network/NetworkManager-lightfee-dns.conf /etc/NetworkManager/conf.d/99-lightfee-dns.conf
sudo systemctl reload NetworkManager || sudo systemctl restart NetworkManager
getent hosts www.okx.com
```

## Resume Live Only If Safe

Only resume from stale fail-closed when open/pending/recovery work is zero. Prefer the code-level clean-start recovery. Manual state edits require a backup and must be followed by `verify_production_services.py`.
