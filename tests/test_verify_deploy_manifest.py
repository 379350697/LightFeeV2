import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import verify_deploy_manifest as manifest

_REAL_SUBPROCESS_RUN = subprocess.run


def test_build_manifest_excludes_deploy_manifest_self_hash(tmp_path, monkeypatch):
    (tmp_path / ".deploy_manifest.json").write_text('{"old": "hash"}', encoding="utf-8")
    deploy_file = tmp_path / "lightfee" / "engine" / "runtime.py"
    deploy_file.parent.mkdir(parents=True)
    deploy_file.write_text("print('runtime')\n", encoding="utf-8")

    monkeypatch.setattr(
        manifest,
        "git_tracked_files",
        lambda root: [".deploy_manifest.json", "lightfee/engine/runtime.py"],
    )

    generated = manifest.build_manifest(tmp_path)

    assert ".deploy_manifest.json" not in generated
    assert "lightfee/engine/runtime.py" in generated


def test_spread_sidecar_template_is_deploy_critical():
    assert "lightfee/venues/market_data.py" in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-spread-sidecar.service" in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-sidecar.service" in manifest.CRITICAL_FILES
    assert "lightfee-spread-bbo.service" in manifest.RETIRED_SYSTEMD_UNITS


def test_local_manifest_generation_fails_when_a_critical_file_is_untracked(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(manifest, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        manifest,
        "build_manifest",
        lambda root: {"lightfee/engine/runtime.py": "abc"},
    )
    monkeypatch.setattr(
        manifest.sys,
        "argv",
        ["verify_deploy_manifest.py", "--local"],
    )

    with pytest.raises(SystemExit, match="1"):
        manifest.main()


def test_trade_optimization_timer_assets_are_deploy_critical():
    assert "scripts/run_trade_optimization_report.sh" in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-trade-optimization-report.service" in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-trade-optimization-report.timer" in manifest.CRITICAL_FILES


def test_fee_evidence_refresh_assets_are_offline_only():
    assert "scripts/refresh_account_fee_evidence.py" not in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-fee-evidence-refresh.service" not in manifest.CRITICAL_FILES
    assert "deploy/systemd/lightfee-fee-evidence-refresh.timer" not in manifest.CRITICAL_FILES


def _stub_manifest_generation(monkeypatch):
    monkeypatch.setattr(
        manifest, "build_manifest", lambda root: {"lightfee/engine/runtime.py": "abc"}
    )

    def fake_run(*args, **kwargs):
        return SimpleNamespace(stdout="abc123\n", returncode=0)

    monkeypatch.setattr(manifest.subprocess, "run", fake_run)


def test_generate_deploy_script_uses_ssh_port_2222(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert 'SSH_OPTS="-p 2222 -o BatchMode=yes -o ConnectTimeout=10"' in script
    assert 'SCP_OPTS="-P 2222 -o BatchMode=yes -o ConnectTimeout=10"' in script
    assert "rsync -avz --delete" in script
    assert '-e "ssh $SSH_OPTS"' in script
    assert "scp $SCP_OPTS" in script


def test_deploy_sync_preserves_operator_backups_and_local_orchestration(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert manifest.should_exclude("config/live.toml.pre-before-release")
    assert manifest.should_exclude("config/live.toml.removed-fields-20260730T120000Z.bak")
    assert manifest.should_exclude(".dev-flow/runs/release/flow.md")
    assert "--exclude 'config/live.toml.pre-*'" in script
    assert "--exclude 'config/live.toml.removed-fields-*'" in script
    assert "--exclude .dev-flow/" in script


def test_generate_deploy_main_preserves_versioned_entrypoint(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)
    entrypoint = tmp_path / "scripts" / "deploy.sh"
    generated_path = tmp_path / "temporary" / "deploy.sh"
    entrypoint.parent.mkdir()
    entrypoint.write_text("versioned entrypoint\n", encoding="utf-8")
    monkeypatch.setattr(manifest, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        manifest.sys,
        "argv",
        [
            "verify_deploy_manifest.py",
            "--generate-deploy",
            "--remote",
            "root@38.60.253.248",
            "--output",
            str(generated_path),
        ],
    )

    manifest.main()

    assert entrypoint.read_text(encoding="utf-8") == "versioned entrypoint\n"
    assert generated_path.is_file()
    assert not (tmp_path / "scripts" / ".deploy.generated.sh").exists()


def test_versioned_deploy_entrypoint_generates_ephemeral_script():
    entrypoint = Path(__file__).resolve().parents[1] / "scripts" / "deploy.sh"
    script = entrypoint.read_text(encoding="utf-8")

    assert 'GENERATED_SCRIPT="$(mktemp "${TMPDIR:-/tmp}/lightfee-deploy.XXXXXX")"' in script
    assert 'trap cleanup EXIT' in script
    assert '"$SCRIPT_DIR/verify_deploy_manifest.py" --generate-deploy' in script
    assert '--output "$GENERATED_SCRIPT"' in script
    assert 'PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"' in script
    assert 'LIGHTFEE_DEPLOY_LOCAL="${LIGHTFEE_DEPLOY_LOCAL:-$PROJECT_ROOT}" bash "$GENERATED_SCRIPT"' in script


def test_generate_deploy_script_uses_remote_venv_for_production_checks(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert 'REMOTE_PYTHON="$REMOTE_PATH/.venv/bin/python3"' in script
    assert (
        'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_deploy_manifest.py --check "$REMOTE_PATH"'
        in script
    )
    assert (
        'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/check_process_singleton.py --strict'
        in script
    )
    assert (
        'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_production_services.py --json'
        in script
    )
    assert (
        'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/diagnose_live.py --json --since-deploy'
        in script
    )
    assert script.count(
        'scripts/migrate_removed_config_fields.py --config'
    ) == 2
    assert 'scripts/migrate_removed_config_fields.py --config "$REMOTE_PATH/config/live.toml" --apply' in script
    assert script.count(
        "scripts/diagnose_live.py --json --since-deploy --require-gate-pass"
    ) == 2
    assert (
        "env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/verify_production_services.py --json"
        in script
    )
    assert (
        "cd /opt/lightfee-v2 && python3 scripts/verify_production_services.py --json" not in script
    )
    assert "HEALTH_ATTEMPTS=37" in script
    assert 'if output="$(env PYTHONPATH=' in script
    assert 'if output="$(ssh $SSH_OPTS' in script
    assert "printf '%s\\n' \"$output\"" in script


def test_generate_deploy_script_resolves_local_from_script_dir_for_remote_execution(
    tmp_path, monkeypatch
):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert f'LOCAL="{tmp_path}"' not in script
    assert 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' in script
    assert 'LOCAL="${LIGHTFEE_DEPLOY_LOCAL:-$DEFAULT_LOCAL}"' in script
    assert 'if [[ "$LOCAL" == "$REMOTE_PATH" ]]; then' in script
    assert 'echo "=== Remote-local deploy mode: skipping rsync/scp ==="' in script
    assert (
        "systemctl daemon-reload && systemctl enable --now lightfee-trade-optimization-report.timer "
        "&& systemctl enable lightfee-sidecar.service lightfee-spread-sidecar.service lightfee-live.service "
        "&& systemctl restart lightfee-sidecar.service "
        "&& systemctl restart lightfee-spread-sidecar.service "
        "&& systemctl restart lightfee-live.service"
    ) in script
    assert 'install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-spread-bbo.service"' not in script
    assert "systemctl restart lightfee-spread-bbo.service" not in script
    assert "lightfee-spread-bbo.service" in manifest.RETIRED_SYSTEMD_UNITS
    assert "systemctl enable --now lightfee-fee-evidence-refresh.timer" not in script
    assert "sleep 12" in script


def test_generate_deploy_script_computes_deploy_version_at_runtime(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert 'DEPLOY_VERSION="$(git -C "$LOCAL" rev-parse HEAD)"' in script
    assert 'echo "$DEPLOY_VERSION" > "$REMOTE_PATH/.deploy_version"' in script
    assert (
        'echo "$DEPLOY_VERSION" | ssh $SSH_OPTS root@38.60.253.248 "cat > /opt/lightfee-v2/.deploy_version"'
        in script
    )
    assert 'echo "abc123" > "$REMOTE_PATH/.deploy_version"' not in script


def test_generate_deploy_script_reports_post_deploy_metadata(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert 'echo "=== Verifying deploy metadata ==="' in script
    assert 'REMOTE_GIT_HEAD="$(git -C "$REMOTE_PATH" rev-parse HEAD)"' in script
    assert 'REMOTE_DEPLOY_VERSION="$(cat "$REMOTE_PATH/.deploy_version")"' in script
    assert "deploy_metadata git_head=%s deploy_version=%s expected=%s\\n" in script
    assert "deploy metadata mismatch" in script
    assert (
        "git_head=\\$(git -C /opt/lightfee-v2 rev-parse HEAD) "
        "deploy_version=\\$(cat /opt/lightfee-v2/.deploy_version)"
    ) in script


def test_generate_deploy_script_installs_trade_optimization_timer(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert (
        'install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-trade-optimization-report.service" /etc/systemd/system/lightfee-trade-optimization-report.service'
        in script
    )
    assert (
        'install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-trade-optimization-report.timer" /etc/systemd/system/lightfee-trade-optimization-report.timer'
        in script
    )
    assert "systemctl enable --now lightfee-trade-optimization-report.timer" in script
    assert "systemctl restart lightfee-trade-optimization-report.service" not in script


def test_generate_deploy_script_retires_removed_systemd_units_before_restart(
    tmp_path, monkeypatch
):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    units = " ".join(manifest.RETIRED_SYSTEMD_UNITS)
    first_retire = script.index('echo "=== Retiring removed systemd units ==="')
    first_restart = script.index('echo "=== Restarting production services ==="')
    last_retire = script.rindex('echo "=== Retiring removed systemd units ==="')
    last_restart = script.rindex('echo "=== Restarting production services ==="')
    assert first_retire < first_restart
    assert last_retire < last_restart
    local_helper = script[
        script.index("verify_retired_systemd_unit() {"):
        script.index("retire_remote_systemd_units() {")
    ]
    remote_helper = script[
        script.index("retire_remote_systemd_units() {"):
        script.index('echo "=== Generating deploy manifest ==="')
    ]
    remote_retire_call = script.rindex("retire_remote_systemd_units")
    assert remote_retire_call < last_restart
    assert f"for unit in {units}; do" in local_helper
    assert f'units="{units}"' in remote_helper
    for helper in (local_helper, remote_helper):
        assert 'systemctl stop "$unit" >/dev/null 2>&1 || true' in helper
        assert 'systemctl disable "$unit" >/dev/null 2>&1 || true' in helper
        assert 'rm -f "$unit_file"' in helper
        assert 'systemctl daemon-reload' in helper
        assert 'systemctl is-active "$unit"' not in helper
        assert (
            'active_state="$(systemctl show "$unit" --property=ActiveState --value 2>/dev/null)"'
            in helper
        )
        assert (
            'load_state="$(systemctl show "$unit" --property=LoadState --value 2>/dev/null)"'
            in helper
        )
        assert 'enabled_state="$(systemctl is-enabled "$unit" 2>/dev/null)"' in helper
        assert 'case "$load_state:$active_state" in' in helper
        assert "not-found:inactive|loaded:inactive" in helper
        assert "disabled|not-found" in helper
        assert "retired systemd unit active-state query failed" in helper
        assert "retired systemd unit active-state query returned empty" in helper
        assert "retired systemd unit load-state query failed" in helper
        assert "retired systemd unit load-state query returned empty" in helper
        assert "retired systemd unit not inactive or absent" in helper
        assert "retired systemd unit enabled-state query returned empty" in helper
        assert "retired systemd unit not disabled or absent" in helper
        assert "retired systemd unit file still exists" in helper
    assert 'verify_retired_systemd_unit "$unit"' in local_helper
    assert 'verify_retired_remote_systemd_unit "$unit"' in remote_helper
    assert "return 1" in local_helper
    assert "exit 1" in remote_helper
    assert f"systemctl reset-failed {units} >/dev/null 2>&1 || true" in local_helper
    assert "systemctl reset-failed $units >/dev/null 2>&1 || true" in remote_helper
    assert "disable --now" not in script
    assert 'install -m 0644 "$REMOTE_PATH/deploy/systemd/lightfee-fee-evidence-refresh.service"' not in script
    assert "systemctl enable --now lightfee-fee-evidence-refresh.timer" not in script


def _local_retired_unit_verifier(script: str) -> str:
    return script[
        script.index("verify_retired_systemd_unit() {"):
        script.index("retire_systemd_units() {")
    ]


def _remote_retired_unit_verifier(script: str) -> str:
    start = script.index("verify_retired_remote_systemd_unit() {")
    return script[start:script.index("for unit in $units; do", start)]


def _write_fake_systemctl(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        """#!/bin/sh
case "$1" in
  show)
    case "$3" in
      --property=ActiveState)
        if [ "${LIGHTFEE_FAKE_ACTIVE_RC:-0}" != "0" ]; then
          printf '%s\\n' "${LIGHTFEE_FAKE_ACTIVE_STATE-}"
          exit "$LIGHTFEE_FAKE_ACTIVE_RC"
        fi
        printf '%s\\n' "${LIGHTFEE_FAKE_ACTIVE_STATE-}"
        ;;
      --property=LoadState)
        if [ "${LIGHTFEE_FAKE_LOAD_RC:-0}" != "0" ]; then
          printf '%s\\n' "${LIGHTFEE_FAKE_LOAD_STATE-}"
          exit "$LIGHTFEE_FAKE_LOAD_RC"
        fi
        printf '%s\\n' "${LIGHTFEE_FAKE_LOAD_STATE-}"
        ;;
      *)
        exit 64
        ;;
    esac
    ;;
  is-enabled)
    printf '%s\\n' "${LIGHTFEE_FAKE_ENABLED_STATE-}"
    exit "${LIGHTFEE_FAKE_ENABLED_RC:-0}"
    ;;
  *)
    exit 64
    ;;
esac
""",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    return fake_bin


def _run_retired_unit_verifier(
    *,
    verifier: str,
    verifier_function: str,
    tmp_path: Path,
    env_overrides: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    fake_bin = _write_fake_systemctl(tmp_path)
    env = os.environ.copy()
    env.update(env_overrides)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    command = (
        f"{verifier}\n"
        f"{verifier_function} lightfee-generated-contract-test.service\n"
    )
    return _REAL_SUBPROCESS_RUN(
        ["bash", "-c", command],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_local_retired_unit_verifier(
    *,
    script: str,
    tmp_path: Path,
    env_overrides: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return _run_retired_unit_verifier(
        verifier=_local_retired_unit_verifier(script),
        verifier_function="verify_retired_systemd_unit",
        tmp_path=tmp_path,
        env_overrides=env_overrides,
    )


def _run_remote_retired_unit_verifier(
    *,
    script: str,
    tmp_path: Path,
    env_overrides: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return _run_retired_unit_verifier(
        verifier=_remote_retired_unit_verifier(script),
        verifier_function="verify_retired_remote_systemd_unit",
        tmp_path=tmp_path,
        env_overrides=env_overrides,
    )


def test_generated_retired_unit_verifier_accepts_only_verified_safe_states(
    tmp_path, monkeypatch
):
    _stub_manifest_generation(monkeypatch)
    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )
    active_safe = {
        "LIGHTFEE_FAKE_ACTIVE_STATE": "inactive",
        "LIGHTFEE_FAKE_LOAD_STATE": "loaded",
        "LIGHTFEE_FAKE_ENABLED_STATE": "disabled",
    }
    absent_safe = {
        "LIGHTFEE_FAKE_ACTIVE_STATE": "inactive",
        "LIGHTFEE_FAKE_LOAD_STATE": "not-found",
        "LIGHTFEE_FAKE_ENABLED_STATE": "not-found",
        "LIGHTFEE_FAKE_ENABLED_RC": "1",
    }
    runners = (
        ("local", _run_local_retired_unit_verifier),
        ("ssh", _run_remote_retired_unit_verifier),
    )

    for runner_name, run_verifier in runners:
        assert run_verifier(
            script=script,
            tmp_path=tmp_path,
            env_overrides=active_safe,
        ).returncode == 0, runner_name
        assert run_verifier(
            script=script,
            tmp_path=tmp_path,
            env_overrides=absent_safe,
        ).returncode == 0, runner_name

    failing_cases = [
        (
            "active query error",
            {**active_safe, "LIGHTFEE_FAKE_ACTIVE_RC": "7"},
            "active-state query failed",
        ),
        (
            "active empty",
            {**active_safe, "LIGHTFEE_FAKE_ACTIVE_STATE": ""},
            "active-state query returned empty",
        ),
        (
            "active",
            {**active_safe, "LIGHTFEE_FAKE_ACTIVE_STATE": "active"},
            "not inactive or absent",
        ),
        (
            "activating",
            {**active_safe, "LIGHTFEE_FAKE_ACTIVE_STATE": "activating"},
            "not inactive or absent",
        ),
        (
            "deactivating",
            {**active_safe, "LIGHTFEE_FAKE_ACTIVE_STATE": "deactivating"},
            "not inactive or absent",
        ),
        (
            "load query error",
            {**active_safe, "LIGHTFEE_FAKE_LOAD_RC": "8"},
            "load-state query failed",
        ),
        (
            "load empty",
            {**active_safe, "LIGHTFEE_FAKE_LOAD_STATE": ""},
            "load-state query returned empty",
        ),
        (
            "load unknown",
            {**active_safe, "LIGHTFEE_FAKE_LOAD_STATE": "masked"},
            "not inactive or absent",
        ),
        (
            "enabled empty",
            {**active_safe, "LIGHTFEE_FAKE_ENABLED_STATE": ""},
            "enabled-state query returned empty",
        ),
        (
            "enabled",
            {**active_safe, "LIGHTFEE_FAKE_ENABLED_STATE": "enabled"},
            "not disabled or absent",
        ),
        (
            "static",
            {**active_safe, "LIGHTFEE_FAKE_ENABLED_STATE": "static"},
            "not disabled or absent",
        ),
    ]
    for runner_name, run_verifier in runners:
        for label, env_overrides, expected_stderr in failing_cases:
            result = run_verifier(
                script=script,
                tmp_path=tmp_path,
                env_overrides=env_overrides,
            )
            case_label = f"{runner_name} {label}"
            assert result.returncode != 0, case_label
            assert expected_stderr in result.stderr, case_label


def test_generate_deploy_script_preserves_remote_version_and_skips_local_caches(
    tmp_path, monkeypatch
):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    rsync_line = next(line for line in script.splitlines() if line.startswith("rsync "))
    for pattern in (
        ".deploy_version",
        ".gitnexus/",
        ".pytest_cache/",
        ".ruff_cache/",
    ):
        assert f"--exclude {pattern}" in rsync_line


def test_verify_remote_manifest_uses_configured_ssh_port(monkeypatch):
    local_manifest = {path: "abc" for path in manifest.CRITICAL_FILES}
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        stdout = "\n".join(f"{path} abc abc OK" for path in manifest.CRITICAL_FILES)
        return SimpleNamespace(stdout=stdout, stderr="", returncode=0)

    monkeypatch.setattr(manifest.subprocess, "run", fake_run)

    assert manifest.verify_remote_manifest(
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        local_manifest,
        ssh_port=2222,
    )

    assert calls
    assert calls[0][:3] == ["ssh", "-p", "2222"]
