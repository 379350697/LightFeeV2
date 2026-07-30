from __future__ import annotations

from scripts import migrate_removed_config_fields as migration


def _config_text() -> str:
    return """[runtime]\nsidecar_perp_liquidity_budget_ms = 1000\nquote_fetch_timeout_s = 10\n\n[strategy]\npending_entry_max_lifetime_ms = 30000\nmin_profit_rate = 0.1\n"""


def test_check_reports_only_retired_fields_without_writing(tmp_path, capsys) -> None:
    path = tmp_path / "live.toml"
    path.write_text(_config_text(), encoding="utf-8")

    assert migration.main(["--config", str(path)]) == 1
    assert path.read_text(encoding="utf-8") == _config_text()
    assert "runtime.sidecar_perp_liquidity_budget_ms" in capsys.readouterr().err


def test_apply_backs_up_and_removes_only_retired_assignments(tmp_path, monkeypatch) -> None:
    path = tmp_path / "live.toml"
    path.write_text(_config_text(), encoding="utf-8")
    monkeypatch.setattr(migration, "load_config", lambda _: object())

    assert migration.main(["--config", str(path), "--apply"]) == 0
    migrated = path.read_text(encoding="utf-8")
    assert "sidecar_perp_liquidity_budget_ms" not in migrated
    assert "pending_entry_max_lifetime_ms" not in migrated
    assert "quote_fetch_timeout_s = 10" in migrated
    assert "min_profit_rate = 0.1" in migrated
    backups = list(tmp_path.glob("live.toml.removed-fields-*.bak"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == _config_text()


def test_apply_fails_closed_when_validation_still_fails(tmp_path, monkeypatch, capsys) -> None:
    path = tmp_path / "live.toml"
    path.write_text(_config_text(), encoding="utf-8")

    def fail_validation(_):
        raise migration.ConfigError("unrelated invalid setting")

    monkeypatch.setattr(migration, "load_config", fail_validation)
    assert migration.main(["--config", str(path), "--apply"]) == 1
    assert path.read_text(encoding="utf-8") == _config_text()
    assert "unrelated invalid setting" in capsys.readouterr().err
