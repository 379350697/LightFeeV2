from types import SimpleNamespace

from scripts import verify_deploy_manifest as manifest


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
    assert "deploy/systemd/lightfee-spread-sidecar.service" in manifest.CRITICAL_FILES


def _stub_manifest_generation(monkeypatch):
    monkeypatch.setattr(manifest, "build_manifest", lambda root: {"lightfee/engine/runtime.py": "abc"})

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
    assert 'rsync -avz --delete' in script
    assert '-e "ssh $SSH_OPTS"' in script
    assert "scp $SCP_OPTS" in script


def test_generate_deploy_script_uses_remote_venv_for_production_checks(tmp_path, monkeypatch):
    _stub_manifest_generation(monkeypatch)

    script = manifest.generate_deploy_script(
        tmp_path,
        "root@38.60.253.248",
        "/opt/lightfee-v2",
        ssh_port=2222,
    )

    assert 'REMOTE_PYTHON="$REMOTE_PATH/.venv/bin/python3"' in script
    assert 'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_deploy_manifest.py --check "$REMOTE_PATH"' in script
    assert 'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/check_process_singleton.py --strict' in script
    assert 'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/verify_production_services.py --json' in script
    assert 'env PYTHONPATH="$REMOTE_PATH" "$REMOTE_PYTHON" scripts/diagnose_live.py --json --since-deploy' in script
    assert "env PYTHONPATH=$REMOTE_PATH $REMOTE_PYTHON scripts/verify_production_services.py --json" in script
    assert "cd /opt/lightfee-v2 && python3 scripts/verify_production_services.py --json" not in script


def test_generate_deploy_script_resolves_local_from_script_dir_for_remote_execution(tmp_path, monkeypatch):
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
        "systemctl daemon-reload && systemctl restart lightfee-sidecar.service "
        "&& systemctl restart lightfee-spread-sidecar.service "
        "&& systemctl restart lightfee-live.service"
    ) in script


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
