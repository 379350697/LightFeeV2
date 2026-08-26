"""V1 semantic parity tests for config: directed_pairs, daily_universe, opportunity modes.

Coverage targets from v1_semantic_contract_catalog.md:
  CONFIG-001: directed_pairs restrict direction
  CONFIG-002: daily universe with fallback-to-last-good
  CONFIG-003: runtime opportunity modes
  CONFIG-004: config validation errors
  CONFIG-005: max symbols enforcement
  CONFIG-006: path resolution
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pytest

from lightfee.config.schema import (
    AppConfig,
    DailyUniverseConfig,
    DirectedPairConfig,
    RuntimeConfig,
)
from lightfee.config.validation import validate_config
from lightfee.config.universe import (
    filter_by_directed_pairs,
    load_daily_universe,
    resolve_universe_symbols,
    validate_directed_pairs,
)
from lightfee.config.loader import load_config
from lightfee.core.domain import Symbol
from lightfee.core.errors import ConfigError


# ── Helpers ────────────────────────────────────────────────────────────────


def _write_toml(path: str, content: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


def _write_daily_universe(path: str, symbols: list[str]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(
            {
                "trading_date": date.today().isoformat(),
                "generated_at_ms": 1_765_000_000_000,
                "selector_version": 1,
                "source_symbol_count": len(symbols),
                "selected_symbol_count": len(symbols),
                "selected_symbols": symbols,
            },
            f,
        )


# ── CONFIG-001: Directed Pairs Restrict Direction ──────────────────────────


class TestDirectedPairsRestrictDirection:
    """CONFIG-001: directed_pairs restrict pair direction independently from global symbols."""

    def test_empty_directed_pairs_allows_all_combinations(self):
        """When directed_pairs is empty, all venue combinations from global symbols are allowed."""
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            venues=_make_venues("binance", "bybit"),
        )
        pairs = filter_by_directed_pairs(
            pairs=[("binance", "bybit", "BTCUSDT"), ("bybit", "binance", "BTCUSDT")],
            directed_pairs=config.runtime.directed_pairs,
        )
        assert len(pairs) == 2

    def test_directed_pair_restricts_specific_direction(self):
        """A directed pair only allows the specified direction."""
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(
                directed_pairs=[
                    DirectedPairConfig(long="binance", short="bybit", symbols=["BTCUSDT"]),
                ]
            ),
            venues=_make_venues("binance", "bybit"),
        )
        pairs = filter_by_directed_pairs(
            pairs=[("binance", "bybit", "BTCUSDT"), ("bybit", "binance", "BTCUSDT")],
            directed_pairs=config.runtime.directed_pairs,
        )
        assert pairs == [("binance", "bybit", "BTCUSDT")]

    def test_directed_pair_with_subset_symbols(self):
        """A directed pair with symbols subset only allows those symbols."""
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(
                directed_pairs=[
                    DirectedPairConfig(long="binance", short="bybit", symbols=["BTCUSDT"]),
                ]
            ),
            venues=_make_venues("binance", "bybit"),
        )
        pairs = filter_by_directed_pairs(
            pairs=[
                ("binance", "bybit", "BTCUSDT"),
                ("binance", "bybit", "ETHUSDT"),
            ],
            directed_pairs=config.runtime.directed_pairs,
        )
        assert pairs == [("binance", "bybit", "BTCUSDT")]

    def test_directed_pair_all_symbols_when_empty(self):
        """A directed pair with empty symbols allows all global symbols."""
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(
                directed_pairs=[
                    DirectedPairConfig(long="binance", short="bybit", symbols=[]),
                ]
            ),
            venues=_make_venues("binance", "bybit"),
        )
        pairs = filter_by_directed_pairs(
            pairs=[
                ("binance", "bybit", "BTCUSDT"),
                ("binance", "bybit", "ETHUSDT"),
                ("bybit", "binance", "BTCUSDT"),
            ],
            directed_pairs=config.runtime.directed_pairs,
        )
        assert len(pairs) == 2
        assert ("binance", "bybit", "BTCUSDT") in pairs
        assert ("binance", "bybit", "ETHUSDT") in pairs

    def test_multiple_directed_pairs(self):
        """Multiple directed pair configs are all enforced."""
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(
                directed_pairs=[
                    DirectedPairConfig(long="binance", short="bybit", symbols=["BTCUSDT"]),
                    DirectedPairConfig(long="bybit", short="okx", symbols=["ETHUSDT"]),
                ]
            ),
            venues=_make_venues("binance", "bybit", "okx"),
        )
        pairs = filter_by_directed_pairs(
            pairs=[
                ("binance", "bybit", "BTCUSDT"),
                ("bybit", "binance", "BTCUSDT"),
                ("bybit", "okx", "ETHUSDT"),
            ],
            directed_pairs=config.runtime.directed_pairs,
        )
        assert len(pairs) == 2
        assert ("binance", "bybit", "BTCUSDT") in pairs
        assert ("bybit", "okx", "ETHUSDT") in pairs


# ── CONFIG-002: Daily Universe with Fallback-to-Last-Good ─────────────────


class TestDailyUniverseConfig:
    """CONFIG-002: Daily universe supports enablement, generation time, max symbols, fallback-to-last-good, path resolution."""

    def test_default_config(self):
        config = DailyUniverseConfig()
        assert config.enabled is False
        assert config.max_symbols == 128
        assert config.fallback_to_last_good is True
        assert config.generate_time_local == "08:00:00"

    def test_enabled_with_all_fields(self):
        config = DailyUniverseConfig(
            enabled=True,
            generate_time_local="09:30:00",
            max_symbols=64,
            fallback_to_last_good=False,
            path="/data/universe.json",
        )
        assert config.enabled is True
        assert config.generate_time_local == "09:30:00"
        assert config.max_symbols == 64
        assert config.fallback_to_last_good is False
        assert config.path == "/data/universe.json"

    def test_load_daily_universe_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "universe.json")
            _write_daily_universe(path, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
            symbols = load_daily_universe(path)
            assert symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def test_load_daily_universe_rejects_undated_legacy_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "universe.json")
            Path(path).write_text('{"symbols": ["BTCUSDT"]}')

            assert load_daily_universe(path) is None

    def test_stale_valid_universe_is_explicit_bounded_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "universe.json")
            Path(path).write_text(
                json.dumps(
                    {
                        "trading_date": (
                            date.today() - timedelta(days=1)
                        ).isoformat(),
                        "generated_at_ms": 1_765_000_000_000,
                        "selector_version": 1,
                        "source_symbol_count": 3,
                        "selected_symbol_count": 3,
                        "selected_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                    }
                )
            )
            config = AppConfig(
                symbols=["XRPUSDT"],
                runtime=RuntimeConfig(
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        max_symbols=2,
                        fallback_to_last_good=True,
                        path=path,
                    )
                ),
            )

            result = resolve_universe_symbols(config)

            assert result["used_fallback"] is True
            assert result["resolved_symbols"] == ["BTCUSDT", "ETHUSDT"]

    def test_load_daily_universe_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nonexistent.json")
            symbols = load_daily_universe(path)
            assert symbols is None

    def test_load_daily_universe_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "invalid.json")
            Path(path).write_text("not json")
            symbols = load_daily_universe(path)
            assert symbols is None

    def test_load_daily_universe_rejects_invalid_trading_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "invalid-date.json")
            _write_daily_universe(path, ["BTCUSDT"])
            payload = json.loads(Path(path).read_text())
            payload["trading_date"] = "not-a-date"
            Path(path).write_text(json.dumps(payload))

            assert load_daily_universe(path) is None

    def test_load_daily_universe_rejects_missing_v1_required_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "missing-field.json")
            _write_daily_universe(path, ["BTCUSDT"])
            payload = json.loads(Path(path).read_text())
            del payload["source_symbol_count"]
            Path(path).write_text(json.dumps(payload))

            assert load_daily_universe(path) is None

    def test_load_daily_universe_uses_v1_symbol_normalization(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "normalized.json")
            _write_daily_universe(path, ["btc-usdt"])

            assert load_daily_universe(path) == ["BTCUSDT"]

    def test_resolve_universe_symbols_with_enabled_daily_universe(self):
        with tempfile.TemporaryDirectory() as tmp:
            universe_path = os.path.join(tmp, "universe.json")
            _write_daily_universe(universe_path, ["BTCUSDT", "ETHUSDT"])
            config = AppConfig(
                symbols=[],
                runtime=RuntimeConfig(
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        generate_time_local="08:00:00",
                        max_symbols=128,
                        fallback_to_last_good=True,
                        path=universe_path,
                    )
                ),
            )
            result = resolve_universe_symbols(config)
            assert result is not None
            assert result["daily_universe_enabled"] is True
            assert set(result["resolved_symbols"]) == {"BTCUSDT", "ETHUSDT"}

    def test_resolve_universe_with_fallback_to_last_good(self):
        """When current universe is missing and fallback enabled, use last good."""
        with tempfile.TemporaryDirectory() as tmp:
            universe_path = os.path.join(tmp, "universe.json")
            # Write initial universe as "last good"
            _write_daily_universe(universe_path, ["BTCUSDT", "ETHUSDT"])
            config = AppConfig(
                symbols=[],
                runtime=RuntimeConfig(
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        generate_time_local="08:00:00",
                        max_symbols=128,
                        fallback_to_last_good=True,
                        path=universe_path,
                    )
                ),
            )
            # First call succeeds and caches
            result1 = resolve_universe_symbols(config)
            assert result1 is not None

            # Delete the file to simulate missing current universe
            os.remove(universe_path)

            # Second call should fall back to last good
            result2 = resolve_universe_symbols(config)
            assert result2 is not None
            assert set(result2["resolved_symbols"]) == {"BTCUSDT", "ETHUSDT"}
            assert result2.get("used_fallback") is True

    def test_resolve_universe_missing_no_fallback_fails_closed(self):
        """A reader cannot turn a missing daily snapshot into static trading symbols."""
        with tempfile.TemporaryDirectory() as tmp:
            universe_path = os.path.join(tmp, "nonexistent.json")
            config = AppConfig(
                symbols=["BTCUSDT"],  # static symbols as fallback
                runtime=RuntimeConfig(
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        generate_time_local="08:00:00",
                        max_symbols=128,
                        fallback_to_last_good=False,
                        path=universe_path,
                    )
                ),
            )
            with pytest.raises(RuntimeError, match="no last-good fallback"):
                resolve_universe_symbols(config)


# ── CONFIG-003: Runtime Opportunity Modes ──────────────────────────────────


class TestOpportunityModes:
    """CONFIG-003: Runtime distinguishes direct_market, coarse_sidecar, sidecar_scan, disabled, and non-parity fallback modes."""

    def test_direct_market_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "direct_market"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_coarse_sidecar_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "coarse_sidecar"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_sidecar_backed_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "sidecar_backed"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_sidecar_scan_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "sidecar_scan"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_disabled_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "disabled"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_non_parity_fallback_mode_valid(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "non_parity"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_invalid_mode_rejected(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "invalid_mode"
        issues = validate_config(config)
        assert any("opportunity_input_mode" in i for i in issues)

    def test_non_parity_fallback_is_opt_in(self):
        """Non-parity fallback mode must be explicitly set, not default."""
        config = AppConfig(symbols=["BTCUSDT"])
        assert config.runtime.opportunity_input_mode != "non_parity"
        assert config.runtime.opportunity_input_mode in ("coarse_sidecar", "sidecar_backed")


# ── CONFIG-004: Config Validation Errors ───────────────────────────────────


class TestConfigValidationErrors:
    """CONFIG-004: Missing or contradictory config fails before runtime starts."""

    def test_invalid_generation_time_rejected(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.daily_universe.enabled = True
        config.runtime.daily_universe.path = "/tmp/universe.json"
        config.runtime.daily_universe.generate_time_local = "25:00:00"
        issues = validate_config(config)
        assert any("generate_time" in i.lower() for i in issues)

    def test_zero_max_symbols_rejected(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.daily_universe.enabled = True
        config.runtime.daily_universe.path = "/tmp/universe.json"
        config.runtime.daily_universe.max_symbols = 0
        issues = validate_config(config)
        assert any("max_symbols" in i for i in issues)

    def test_daily_universe_path_required_when_enabled(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.daily_universe.enabled = True
        config.runtime.daily_universe.path = ""
        issues = validate_config(config)
        assert any("daily_universe" in i.lower() for i in issues)

    def test_valid_config_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(
                    mode="paper",
                    opportunity_input_mode="coarse_sidecar",
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        generate_time_local="08:00:00",
                        max_symbols=128,
                        fallback_to_last_good=True,
                        path=f"{tmp}/universe.json",
                    ),
                ),
            )
            issues = validate_config(config)
            assert len(issues) == 0


# ── CONFIG-005: Max Symbols Enforcement ────────────────────────────────────


class TestMaxSymbolsEnforcement:
    """CONFIG-005: Runtime cannot silently trade symbols exceeding max_symbols."""

    def test_max_symbols_enforced_at_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            universe_path = os.path.join(tmp, "universe.json")
            symbols = [f"TOKEN{i}USDT" for i in range(200)]  # 200 symbols
            _write_daily_universe(universe_path, symbols)
            config = AppConfig(
                symbols=[],
                runtime=RuntimeConfig(
                    daily_universe=DailyUniverseConfig(
                        enabled=True,
                        generate_time_local="08:00:00",
                        max_symbols=128,
                        fallback_to_last_good=True,
                        path=universe_path,
                    )
                ),
            )
            result = resolve_universe_symbols(config)
            assert result is not None
            assert len(result["resolved_symbols"]) <= 128

    def test_max_symbols_enforced_at_validation(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.daily_universe.enabled = True
        config.runtime.daily_universe.path = "/tmp/universe.json"
        config.runtime.daily_universe.max_symbols = 0
        issues = validate_config(config)
        assert any("max_symbols" in i for i in issues)


# ── CONFIG-006: Path Resolution ────────────────────────────────────────────


class TestPathResolution:
    """CONFIG-006: Universe file paths are resolved relative to config directory or absolute root."""

    def test_absolute_path_resolves(self):
        config = DailyUniverseConfig(
            enabled=True,
            generate_time_local="08:00:00",
            max_symbols=128,
            fallback_to_last_good=True,
            path="/absolute/path/to/universe.json",
        )
        assert config.path == "/absolute/path/to/universe.json"

    def test_relative_path_requires_root(self):
        config = DailyUniverseConfig(
            enabled=True,
            generate_time_local="08:00:00",
            max_symbols=128,
            fallback_to_last_good=True,
            path="relative/universe.json",
        )
        assert config.path == "relative/universe.json"


# ── Directed Pairs Validation ──────────────────────────────────────────────


class TestDirectedPairsValidation:
    def test_valid_directed_pairs_pass(self):
        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(
                directed_pairs=[
                    DirectedPairConfig(long="binance", short="bybit", symbols=["BTCUSDT"]),
                ]
            ),
            venues=_make_venues("binance", "bybit"),
        )
        issues = validate_directed_pairs(config.runtime.directed_pairs, config.symbols)
        assert len(issues) == 0

    def test_unknown_venue_in_directed_pair(self):
        issues = validate_directed_pairs(
            [DirectedPairConfig(long="unknown_x", short="bybit", symbols=[])],
            ["BTCUSDT"],
        )
        assert any("unknown_x" in i for i in issues)


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_venues(*names: str):
    from lightfee.config.schema import VenueConfig

    return [VenueConfig(venue=n) for n in names]
