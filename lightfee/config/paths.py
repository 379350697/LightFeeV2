"""Config-relative artifact path helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_CONFIG_ARTIFACT_ROOT_ATTR = "_config_artifact_root"
DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR = "runtime/hyperliquid-info-coordinator"
_CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR: Path | None = None


def remember_config_artifact_root(
    config_or_runtime: Any,
    config_path: str | Path,
) -> None:
    """Attach loader provenance without changing literal TOML values."""
    runtime = getattr(config_or_runtime, "runtime", config_or_runtime)
    setattr(
        runtime,
        _CONFIG_ARTIFACT_ROOT_ATTR,
        str(infer_config_artifact_root(config_path)),
    )


def infer_config_artifact_root(config_path: str | Path) -> Path:
    path = _resolve_safely(Path(config_path).expanduser())
    config_dir = path.parent
    if config_dir.name == "config":
        return config_dir.parent
    return config_dir


def config_artifact_root(config_or_runtime: Any) -> Path | None:
    runtime = getattr(config_or_runtime, "runtime", config_or_runtime)
    root = str(getattr(runtime, _CONFIG_ARTIFACT_ROOT_ATTR, "") or "").strip()
    if not root:
        return None
    return _resolve_safely(Path(root).expanduser())


def resolve_config_artifact_path(config_or_runtime: Any, value: object) -> Path:
    text = str(value or "").strip()
    path = Path(text).expanduser()
    root = config_artifact_root(config_or_runtime)
    if text and root is not None and not path.is_absolute():
        path = root / path
    return _resolve_safely(path)


def remember_hyperliquid_info_coordinator_dir(config_or_runtime: Any) -> Path:
    global _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR
    runtime = getattr(config_or_runtime, "runtime", config_or_runtime)
    configured = getattr(
        runtime,
        "hyperliquid_info_coordinator_dir",
        DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR,
    )
    directory = resolve_config_artifact_path(runtime, configured)
    _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR = directory
    return directory


def remember_hyperliquid_info_coordinator_directory(
    directory: str | Path,
) -> Path:
    """Set an explicit process-wide coordinator directory for read-only tools.

    Standalone diagnostics do not instantiate ``AppConfig`` but still need to
    join the same local IPC namespace as the service.  Accepting an already
    resolved directory avoids falling back to the caller's working directory.
    """
    global _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR
    _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR = _resolve_safely(
        Path(directory).expanduser()
    )
    return _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR


def configured_hyperliquid_info_coordinator_dir() -> Path | None:
    return _CONFIGURED_HYPERLIQUID_INFO_COORDINATOR_DIR


def _resolve_safely(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except (OSError, RuntimeError):
        return path.absolute()
