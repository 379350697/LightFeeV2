#!/usr/bin/env python3
"""Deploy manifest integrity verification.

Ensures that .deploy_version hash matches the actual deployed files,
preventing the "deploy says X but code is Y" bug seen on cloud.

Usage:
  python scripts/verify_deploy_manifest.py [--local] [--remote root@host]
  python scripts/verify_deploy_manifest.py --check /opt/lightfee-v2

V1 parity: deployment MUST sync all git-tracked files and verify key
runtime files match. Not doing so caused the cloud incident where
.deploy_version=4974d9b but runtime.py/snapshot.py hashes didn't match.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

# --- Files that MUST match between local and remote ---
# These are the files that were mismatched on the cloud deployment.
CRITICAL_FILES = [
    "lightfee/engine/runtime.py",
    "lightfee/engine/recovery.py",
    "lightfee/engine/entry_sync.py",
    "lightfee/engine/state.py",
    "lightfee/sidecar/snapshot.py",
    "lightfee/sidecar/publisher.py",
    "lightfee/sidecar/spread_bbo.py",
    "lightfee/sidecar/spread_bbo_service.py",
    "lightfee/sidecar/v1_compat.py",
    "lightfee/spread/quote_snapshot.py",
    "lightfee/apps/spread_bbo.py",
    "lightfee/config/schema.py",
    "lightfee/engine/lifecycle.py",
    "lightfee/ops/production_health.py",
    "scripts/verify_production_services.py",
    "scripts/refresh_account_fee_evidence.py",
    "scripts/run_trade_optimization_report.sh",
    "deploy/systemd/lightfee-live.service",
    "deploy/systemd/lightfee-sidecar.service",
    "deploy/systemd/lightfee-spread-bbo.service",
    "deploy/systemd/lightfee-spread-sidecar.service",
    "deploy/systemd/lightfee-trade-optimization-report.service",
    "deploy/systemd/lightfee-trade-optimization-report.timer",
    "deploy/systemd/lightfee-fee-evidence-refresh.service",
    "deploy/systemd/lightfee-fee-evidence-refresh.timer",
    "deploy/network/NetworkManager-lightfee-dns.conf",
]

# --- Files/dirs to exclude from sync ---
EXCLUDE_PATTERNS = [
    ".deploy_manifest.json",  # generated artifact; self-hash cannot be stable
    ".deploy_version",        # remote runtime metadata; rewritten after sync
    ".venv",
    "__pycache__",
    "*.pyc",
    ".git",
    ".gitnexus/",             # local code-index/cache data
    ".pytest_cache/",
    ".ruff_cache/",
    "config/live.toml",       # local secrets
    "config/*.local.toml",    # local secrets
    "runtime/",               # runtime output
    "logs/",
    ".env",
    "*.log",
    ".DS_Store",
    ".claude/",
    "docs/",                  # docs not needed on server
]


def repo_root() -> Path:
    """Find the git repo root."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, cwd=Path(__file__).resolve().parent.parent,
    )
    if result.returncode != 0:
        print("ERROR: not in a git repository", file=sys.stderr)
        sys.exit(1)
    return Path(result.stdout.strip())


def git_tracked_files(root: Path) -> list[str]:
    """List all git-tracked files (relative paths)."""
    result = subprocess.run(
        ["git", "ls-files"],
        capture_output=True, text=True, cwd=root,
    )
    if result.returncode != 0:
        print("ERROR: git ls-files failed", file=sys.stderr)
        sys.exit(1)
    return [f for f in result.stdout.strip().split("\n") if f]


def should_exclude(path: str) -> bool:
    """Check if a path matches any exclude pattern."""
    import fnmatch
    for pattern in EXCLUDE_PATTERNS:
        if fnmatch.fnmatch(path, pattern) or path.startswith(pattern.rstrip("/").rstrip("*")):
            return True
        # Also match any path component
        parts = path.split("/")
        for part in parts:
            if fnmatch.fnmatch(part, pattern):
                return True
    return False


def sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_manifest(root: Path) -> dict[str, str]:
    """Build {relpath: sha256} manifest for all deployable files."""
    all_files = git_tracked_files(root)
    manifest = {}
    for f in all_files:
        if should_exclude(f):
            continue
        fpath = root / f
        if fpath.is_file():
            manifest[f] = sha256_file(fpath)
    return manifest


def check_critical_files(manifest: dict[str, str]) -> list[str]:
    """Verify all CRITICAL_FILES are in the manifest."""
    missing = []
    for f in CRITICAL_FILES:
        if f not in manifest:
            missing.append(f)
    return missing


def _ssh_command(remote_host: str, command: str, ssh_port: int) -> list[str]:
    return [
        "ssh",
        "-p",
        str(ssh_port),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        remote_host,
        command,
    ]


def verify_remote_manifest(
    remote_host: str,
    remote_path: str,
    local_manifest: dict[str, str],
    ssh_port: int = 2222,
) -> bool:
    """Single-SSH-call sha256 verification of all critical files on remote."""
    cmds = []
    for f in CRITICAL_FILES:
        cmds.append(
            f"sha256sum {remote_path}/{f} 2>/dev/null || echo 'MISSING {f}'"
        )
    batch_cmd = "; ".join(cmds)

    result = subprocess.run(
        _ssh_command(remote_host, batch_cmd, ssh_port),
        capture_output=True,
        text=True,
    )

    all_ok = True
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        if line.startswith("MISSING "):
            f = line[len("MISSING "):]
            print(f"  MISSING {f}: file not found on remote")
            all_ok = False
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        remote_hash = parts[0]
        f = parts[1].replace(f"{remote_path}/", "", 1) if parts[1].startswith(remote_path) else parts[1]

        local_hash = local_manifest.get(f)
        if local_hash is None:
            print(f"  SKIP {f}: not in local manifest")
            continue

        if remote_hash != local_hash:
            print(f"  MISMATCH {f}:")
            print(f"    local:  {local_hash}")
            print(f"    remote: {remote_hash}")
            all_ok = False
        else:
            print(f"  OK {f}")

    return all_ok


def read_deploy_version(remote_host: str, remote_path: str, ssh_port: int = 2222) -> str | None:
    """Read .deploy_version from remote."""
    cmd = f"cat {remote_path}/.deploy_version 2>/dev/null"
    result = subprocess.run(
        _ssh_command(remote_host, cmd, ssh_port),
        capture_output=True, text=True,
    )
    return result.stdout.strip() or None


def check_deploy_version_matches(remote_host: str, remote_path: str, ssh_port: int = 2222) -> bool:
    """Verify .deploy_version on remote matches local HEAD."""
    root = repo_root()
    local_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=root,
    ).stdout.strip()

    remote_ver = read_deploy_version(remote_host, remote_path, ssh_port)
    if remote_ver is None:
        print("  WARN: .deploy_version not found on remote")
        return False

    if remote_ver != local_head:
        print("  MISMATCH .deploy_version:")
        print(f"    local:  {local_head}")
        print(f"    remote: {remote_ver}")
        return False

    print(f"  OK .deploy_version: {remote_ver}")
    return True


def generate_deploy_script(
    root: Path,
    remote_host: str,
    remote_path: str,
    ssh_port: int = 2222,
) -> str:
    """Generate a safe rsync deploy script that syncs all tracked files."""
    manifest = build_manifest(root)
    # Write manifest
    manifest_path = root / ".deploy_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)

    # Build rsync exclude args
    exclude_args = []
    for pat in EXCLUDE_PATTERNS:
        exclude_args.extend(["--exclude", pat])
    script = f"""#!/bin/bash
# Auto-generated deploy script — syncs all git-tracked files to remote
# Generated by scripts/verify_deploy_manifest.py
set -euo pipefail

REMOTE="{remote_host}:{remote_path}"
REMOTE_HOST="{remote_host}"
REMOTE_PATH="{remote_path}"
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
DEFAULT_LOCAL="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL="${{LIGHTFEE_DEPLOY_LOCAL:-$DEFAULT_LOCAL}}"
DEPLOY_VERSION="$(git -C "$LOCAL" rev-parse HEAD)"
REMOTE_PYTHON="$REMOTE_PATH/.venv/bin/python3"
SSH_OPTS="-p {ssh_port} -o BatchMode=yes -o ConnectTimeout=10"
SCP_OPTS="-P {ssh_port} -o BatchMode=yes -o ConnectTimeout=10"
HEALTH_ATTEMPTS=37
HEALTH_RETRY_SECONDS=5

verify_local_production_health() {{
  local attempt output
  for ((attempt = 1; attempt <= HEALTH_ATTEMPTS; attempt++)); do
    if output="$(env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_production_services.py --json)"; then
      printf '%s\\n' "$output"
      return 0
    fi
    if ((attempt < HEALTH_ATTEMPTS)); then
      printf 'production health warming: attempt=%s/%s retry_in_seconds=%s\\n' \
        "$attempt" "$HEALTH_ATTEMPTS" "$HEALTH_RETRY_SECONDS"
      sleep "$HEALTH_RETRY_SECONDS"
    fi
  done
  printf '%s\\n' "$output"
  return 1
}}

verify_remote_production_health() {{
  local attempt output
  for ((attempt = 1; attempt <= HEALTH_ATTEMPTS; attempt++)); do
    if output="$(ssh $SSH_OPTS {remote_host} "cd {remote_path} && env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/verify_production_services.py --json")"; then
      printf '%s\\n' "$output"
      return 0
    fi
    if ((attempt < HEALTH_ATTEMPTS)); then
      printf 'production health warming: attempt=%s/%s retry_in_seconds=%s\\n' \
        "$attempt" "$HEALTH_ATTEMPTS" "$HEALTH_RETRY_SECONDS"
      sleep "$HEALTH_RETRY_SECONDS"
    fi
  done
  printf '%s\\n' "$output"
  return 1
}}

install_systemd_units() {{
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-sidecar.service" /etc/systemd/system/lightfee-sidecar.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-spread-bbo.service" /etc/systemd/system/lightfee-spread-bbo.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-spread-sidecar.service" /etc/systemd/system/lightfee-spread-sidecar.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-live.service" /etc/systemd/system/lightfee-live.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-trade-optimization-report.service" /etc/systemd/system/lightfee-trade-optimization-report.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-trade-optimization-report.timer" /etc/systemd/system/lightfee-trade-optimization-report.timer
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-fee-evidence-refresh.service" /etc/systemd/system/lightfee-fee-evidence-refresh.service
  install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-fee-evidence-refresh.timer" /etc/systemd/system/lightfee-fee-evidence-refresh.timer
  chmod 0755 "$REMOTE_PATH/scripts/run_trade_optimization_report.sh"
}}

echo "=== Generating deploy manifest ==="
python3 "$LOCAL/scripts/verify_deploy_manifest.py" --local

if [[ "$LOCAL" == "$REMOTE_PATH" ]]; then
  echo "=== Remote-local deploy mode: skipping rsync/scp ==="
  echo "=== Writing .deploy_version ==="
  echo "$DEPLOY_VERSION" > "$REMOTE_PATH/.deploy_version"

  echo "=== Verifying deploy metadata ==="
  REMOTE_GIT_HEAD="$(git -C "$REMOTE_PATH" rev-parse HEAD)"
  REMOTE_DEPLOY_VERSION="$(cat "$REMOTE_PATH/.deploy_version")"
  printf 'deploy_metadata git_head=%s deploy_version=%s expected=%s\n' "$REMOTE_GIT_HEAD" "$REMOTE_DEPLOY_VERSION" "$DEPLOY_VERSION"
  if [[ "$REMOTE_GIT_HEAD" != "$DEPLOY_VERSION" || "$REMOTE_DEPLOY_VERSION" != "$DEPLOY_VERSION" ]]; then
    echo "deploy metadata mismatch" >&2
    exit 1
  fi

  echo "=== Verifying deployment integrity on remote ==="
  cd "$REMOTE_PATH"
  env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_deploy_manifest.py --check "$REMOTE_PATH"

  echo "=== Installing systemd units ==="
  install_systemd_units

  echo "=== Restarting production services ==="
  systemctl daemon-reload && systemctl enable --now lightfee-trade-optimization-report.timer && systemctl enable --now lightfee-fee-evidence-refresh.timer && systemctl restart lightfee-sidecar.service && systemctl enable lightfee-spread-bbo.service && systemctl restart lightfee-spread-bbo.service && systemctl restart lightfee-spread-sidecar.service && systemctl restart lightfee-live.service
  sleep 12

  echo "=== Verifying production health ==="
  env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/check_process_singleton.py --strict
  if ! verify_local_production_health; then
    echo "=== Production health failed; collecting diagnose evidence ==="
    env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/diagnose_live.py --json --since-deploy
    exit 1
  fi
  env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/diagnose_live.py --json --since-deploy

  echo "=== Deploy complete: $DEPLOY_VERSION ==="
  exit 0
fi

echo "=== Syncing files to $REMOTE ==="
rsync -avz --delete {' '.join(exclude_args)} -e "ssh $SSH_OPTS" "$LOCAL/" "$REMOTE/"

echo "=== Uploading deploy manifest ==="
scp $SCP_OPTS "$LOCAL/.deploy_manifest.json" "$REMOTE/.deploy_manifest.json"

echo "=== Writing .deploy_version ==="
echo "$DEPLOY_VERSION" | ssh $SSH_OPTS {remote_host} "cat > {remote_path}/.deploy_version"

echo "=== Verifying deploy metadata ==="
ssh $SSH_OPTS {remote_host} "cd {remote_path} && git_head=\\$(git -C {remote_path} rev-parse HEAD) deploy_version=\\$(cat {remote_path}/.deploy_version) expected=$DEPLOY_VERSION; printf 'deploy_metadata git_head=%s deploy_version=%s expected=%s\\n' \"\\$git_head\" \"\\$deploy_version\" \"\\$expected\"; if [ \"\\$git_head\" != \"\\$expected\" ] || [ \"\\$deploy_version\" != \"\\$expected\" ]; then echo 'deploy metadata mismatch' >&2; exit 1; fi"

echo "=== Verifying deployment integrity on remote ==="
ssh $SSH_OPTS {remote_host} "cd {remote_path} && env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/verify_deploy_manifest.py --check {remote_path}"

echo "=== Installing systemd units ==="
ssh $SSH_OPTS {remote_host} "install -m 0644 {remote_path}/deploy/systemd/lightfee-sidecar.service /etc/systemd/system/lightfee-sidecar.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-spread-bbo.service /etc/systemd/system/lightfee-spread-bbo.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-spread-sidecar.service /etc/systemd/system/lightfee-spread-sidecar.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-live.service /etc/systemd/system/lightfee-live.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-trade-optimization-report.service /etc/systemd/system/lightfee-trade-optimization-report.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-trade-optimization-report.timer /etc/systemd/system/lightfee-trade-optimization-report.timer && install -m 0644 {remote_path}/deploy/systemd/lightfee-fee-evidence-refresh.service /etc/systemd/system/lightfee-fee-evidence-refresh.service && install -m 0644 {remote_path}/deploy/systemd/lightfee-fee-evidence-refresh.timer /etc/systemd/system/lightfee-fee-evidence-refresh.timer && chmod 0755 {remote_path}/scripts/run_trade_optimization_report.sh"

echo "=== Restarting production services ==="
ssh $SSH_OPTS {remote_host} "systemctl daemon-reload && systemctl enable --now lightfee-trade-optimization-report.timer && systemctl enable --now lightfee-fee-evidence-refresh.timer && systemctl restart lightfee-sidecar.service && systemctl enable lightfee-spread-bbo.service && systemctl restart lightfee-spread-bbo.service && systemctl restart lightfee-spread-sidecar.service && systemctl restart lightfee-live.service"
sleep 12

echo "=== Verifying production health ==="
ssh $SSH_OPTS {remote_host} "cd {remote_path} && env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/check_process_singleton.py --strict"
if ! verify_remote_production_health; then
  echo "=== Production health failed; collecting diagnose evidence ==="
  ssh $SSH_OPTS {remote_host} "cd {remote_path} && env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/diagnose_live.py --json --since-deploy"
  exit 1
fi
ssh $SSH_OPTS {remote_host} "cd {remote_path} && env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/diagnose_live.py --json --since-deploy"

echo "=== Deploy complete: $DEPLOY_VERSION ==="
"""
    return script


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy manifest integrity verification")
    parser.add_argument("--local", action="store_true", help="Build and save local manifest")
    parser.add_argument("--remote", type=str, help="SSH host (e.g., root@38.60.253.248)")
    parser.add_argument("--path", type=str, default="/opt/lightfee-v2", help="Remote path")
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=int(os.environ.get("LIGHTFEE_DEPLOY_SSH_PORT", "2222")),
        help="SSH port for remote deploy/check commands (default: 2222)",
    )
    parser.add_argument("--check", type=str, help="Verify manifest at given path (local)")
    parser.add_argument("--generate-deploy", action="store_true", help="Generate deploy script")
    args = parser.parse_args()

    root = repo_root()

    if args.generate_deploy:
        script = generate_deploy_script(
            root,
            args.remote or "root@YOUR_HOST",
            args.path,
            ssh_port=args.ssh_port,
        )
        script_path = root / "scripts" / "deploy.sh"
        with open(script_path, "w") as f:
            f.write(script)
        os.chmod(script_path, 0o755)
        print(f"Deploy script written: {script_path}")
        print("Review before running!")

    elif args.local:
        manifest = build_manifest(root)
        manifest_path = root / ".deploy_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        print(f"Manifest written: {manifest_path} ({len(manifest)} files)")
        missing = check_critical_files(manifest)
        if missing:
            print(f"WARNING: critical files missing from manifest: {missing}")
        else:
            print("All critical files present in manifest")

    elif args.check:
        check_path = Path(args.check)
        if not check_path.exists():
            print(f"ERROR: path does not exist: {args.check}")
            sys.exit(1)

        # Load manifest from the deployed path (no git required for --check)
        manifest_path = check_path / ".deploy_manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                expected = json.load(f)
            print(f"Loaded manifest: {len(expected)} files")
        else:
            print("WARNING: no .deploy_manifest.json at check path — "
                  "verifying file existence only (no hash comparison)")
            expected = {}

        print(f"Checking {len(CRITICAL_FILES)} critical files...")
        all_ok = True
        for f in CRITICAL_FILES:
            fpath = check_path / f
            if not fpath.exists():
                print(f"  MISSING {f}")
                all_ok = False
                continue
            actual = sha256_file(fpath)
            expected_hash = expected.get(f)
            if expected_hash is None:
                print(f"  WARN {f}: not in manifest, actual_hash={actual[:16]}...")
            elif actual != expected_hash:
                print(f"  MISMATCH {f}:")
                print(f"    expected: {expected_hash}")
                print(f"    actual:   {actual}")
                all_ok = False
            else:
                print(f"  OK {f}")

        if not all_ok:
            print("\nFAIL: deployment manifest integrity check failed")
            sys.exit(1)
        print("\nPASS: all critical files verified")

    elif args.remote:
        print("Building local manifest...")
        manifest = build_manifest(root)
        print(f"Manifest: {len(manifest)} files")

        print(f"\nChecking .deploy_version on {args.remote}...")
        ver_ok = check_deploy_version_matches(args.remote, args.path, args.ssh_port)

        print(f"\nVerifying critical files on {args.remote}:{args.path}...")
        files_ok = verify_remote_manifest(args.remote, args.path, manifest, args.ssh_port)

        if ver_ok and files_ok:
            print("\nPASS: deployment integrity verified")
        else:
            print("\nFAIL: deployment integrity mismatch detected")
            sys.exit(1)

    else:
        # Default: build manifest and check local
        manifest = build_manifest(root)
        print(f"Manifest: {len(manifest)} files")
        missing = check_critical_files(manifest)
        if missing:
            print(f"WARNING: critical files missing from manifest: {missing}")
            sys.exit(1)
        print("All critical files present in manifest")
        print("\nCritical file hashes:")
        for f in CRITICAL_FILES:
            print(f"  {manifest[f][:16]}  {f}")


if __name__ == "__main__":
    main()
