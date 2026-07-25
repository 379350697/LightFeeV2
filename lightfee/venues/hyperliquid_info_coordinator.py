"""Cross-process coordination for Hyperliquid ``POST /info`` calls."""

from __future__ import annotations

import asyncio
import copy
import email.utils
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from lightfee.config.paths import (
    DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR,
    configured_hyperliquid_info_coordinator_dir,
)
from lightfee.core.domain import Venue


DEFAULT_HYPERLIQUID_INFO_MIN_INTERVAL_MS = 250
DEFAULT_HYPERLIQUID_INFO_COOLDOWN_MS = 5_000
DEFAULT_HYPERLIQUID_INFO_MAX_COOLDOWN_MS = 60_000
DEFAULT_HYPERLIQUID_INFO_CACHE_TTL_MS = 10_000
DEFAULT_HYPERLIQUID_INFO_LOCK_STALE_MS = 10_000

HYPERLIQUID_INFO_COORDINATOR_DIR_ENV = "LIGHTFEE_HYPERLIQUID_INFO_COORDINATOR_DIR"
HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV = "LIGHTFEE_HYPERLIQUID_INFO_MIN_INTERVAL_MS"
HYPERLIQUID_INFO_COOLDOWN_MS_ENV = "LIGHTFEE_HYPERLIQUID_INFO_COOLDOWN_MS"
HYPERLIQUID_INFO_CACHE_TTL_MS_ENV = "LIGHTFEE_HYPERLIQUID_INFO_CACHE_TTL_MS"
HYPERLIQUID_INFO_DISABLED_ENV = "LIGHTFEE_HYPERLIQUID_INFO_COORDINATOR_DISABLED"

READONLY_HYPERLIQUID_INFO_TYPES = frozenset(
    {
        "meta",
        "metaAndAssetCtxs",
        "l2Book",
        "clearinghouseState",
        "openOrders",
        "orderStatus",
        "historicalOrders",
        "userFees",
        "userAbstraction",
        "spotClearinghouseState",
    }
)

METADATA_CACHEABLE_HYPERLIQUID_INFO_TYPES = frozenset({"meta", "metaAndAssetCtxs"})


@dataclass(frozen=True)
class HyperliquidInfoCacheHit:
    payload: Any
    received_at_ms: int
    age_ms: int
    request_type: str
    cache_path: str


@dataclass(frozen=True)
class HyperliquidInfoReservation:
    request_type: str
    waited_ms: int
    reserved_at_monotonic_ns: int


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return max(int(raw), 0)
    except (TypeError, ValueError):
        return default


def _env_disabled() -> bool:
    raw = str(os.environ.get(HYPERLIQUID_INFO_DISABLED_ENV, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resolve_path_safely(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except (OSError, RuntimeError):
        return path.absolute()


def _default_coordinator_directory() -> Path:
    env_dir = str(os.environ.get(HYPERLIQUID_INFO_COORDINATOR_DIR_ENV) or "").strip()
    if env_dir:
        return _resolve_path_safely(Path(env_dir).expanduser())
    configured = configured_hyperliquid_info_coordinator_dir()
    if configured is not None:
        return configured
    return _resolve_path_safely(Path(DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR))


def _body_type(body: Mapping[str, Any] | None) -> str:
    if not isinstance(body, Mapping):
        return ""
    return str(body.get("type") or "")


def is_metadata_cacheable_hyperliquid_info_body(
    body: Mapping[str, Any] | None,
) -> bool:
    request_type = _body_type(body)
    if request_type not in METADATA_CACHEABLE_HYPERLIQUID_INFO_TYPES:
        return False
    return "user" not in (body or {})


def should_coordinate_hyperliquid_info_request(
    venue: Venue | str,
    method: str,
    path: str,
    body: Mapping[str, Any] | None,
) -> bool:
    venue_value = venue.value if isinstance(venue, Venue) else str(venue or "")
    if venue_value.lower() != Venue.HYPERLIQUID.value:
        return False
    if str(method or "").upper() != "POST":
        return False
    if str(path or "").split("?", 1)[0] != "/info":
        return False
    return _body_type(body) in READONLY_HYPERLIQUID_INFO_TYPES


def should_coordinate_hyperliquid_info_url(
    method: str,
    url: str,
    body: Mapping[str, Any] | None,
) -> bool:
    if str(method or "").upper() != "POST":
        return False
    parsed = urlparse(str(url or ""))
    if parsed.netloc != "api.hyperliquid.xyz" or parsed.path != "/info":
        return False
    return _body_type(body) in READONLY_HYPERLIQUID_INFO_TYPES


class HyperliquidInfoCoordinator:
    """File-backed pacing/cooldown coordinator for local Hyperliquid info calls."""

    def __init__(
        self,
        *,
        directory: str | os.PathLike[str] | None = None,
        min_interval_ms: int | None = None,
        cooldown_ms: int | None = None,
        max_cooldown_ms: int = DEFAULT_HYPERLIQUID_INFO_MAX_COOLDOWN_MS,
        cache_ttl_ms: int | None = None,
        lock_stale_ms: int = DEFAULT_HYPERLIQUID_INFO_LOCK_STALE_MS,
        enabled: bool = True,
    ) -> None:
        base_dir = (
            _resolve_path_safely(Path(directory).expanduser())
            if directory is not None
            else _default_coordinator_directory()
        )
        self.directory = base_dir
        self.min_interval_ms = (
            DEFAULT_HYPERLIQUID_INFO_MIN_INTERVAL_MS
            if min_interval_ms is None
            else max(int(min_interval_ms), 0)
        )
        self.cooldown_ms = (
            DEFAULT_HYPERLIQUID_INFO_COOLDOWN_MS
            if cooldown_ms is None
            else max(int(cooldown_ms), 0)
        )
        self.max_cooldown_ms = max(
            int(max_cooldown_ms),
            self.cooldown_ms,
        )
        self.cache_ttl_ms = (
            DEFAULT_HYPERLIQUID_INFO_CACHE_TTL_MS
            if cache_ttl_ms is None
            else max(int(cache_ttl_ms), 0)
        )
        self.lock_stale_ms = max(int(lock_stale_ms), 1)
        self.enabled = bool(enabled)
        self._state_path = self.directory / "state.json"
        self._lock_path = self.directory / "state.lock"

    @classmethod
    def from_environment(cls) -> "HyperliquidInfoCoordinator":
        return cls(
            min_interval_ms=_env_int(
                HYPERLIQUID_INFO_MIN_INTERVAL_MS_ENV,
                DEFAULT_HYPERLIQUID_INFO_MIN_INTERVAL_MS,
            ),
            cooldown_ms=_env_int(
                HYPERLIQUID_INFO_COOLDOWN_MS_ENV,
                DEFAULT_HYPERLIQUID_INFO_COOLDOWN_MS,
            ),
            cache_ttl_ms=_env_int(
                HYPERLIQUID_INFO_CACHE_TTL_MS_ENV,
                DEFAULT_HYPERLIQUID_INFO_CACHE_TTL_MS,
            ),
            enabled=not _env_disabled(),
        )

    def lookup_metadata_response(
        self,
        body: Mapping[str, Any] | None,
    ) -> HyperliquidInfoCacheHit | None:
        if not self.enabled or not is_metadata_cacheable_hyperliquid_info_body(body):
            return None
        if self.cache_ttl_ms <= 0:
            return None
        path = self._cache_path(body)
        try:
            with path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(raw, dict):
            return None
        received_at_ms = int(raw.get("received_at_ms") or 0)
        if received_at_ms <= 0:
            return None
        age_ms = max(_now_ms() - received_at_ms, 0)
        if age_ms > self.cache_ttl_ms:
            return None
        return HyperliquidInfoCacheHit(
            payload=copy.deepcopy(raw.get("payload")),
            received_at_ms=received_at_ms,
            age_ms=age_ms,
            request_type=_body_type(body),
            cache_path=str(path),
        )

    def store_metadata_response(
        self,
        body: Mapping[str, Any] | None,
        payload: Any,
        *,
        received_at_ms: int | None = None,
    ) -> bool:
        if not self.enabled or not is_metadata_cacheable_hyperliquid_info_body(body):
            return False
        if self.cache_ttl_ms <= 0:
            return False
        try:
            json.dumps(payload)
        except (TypeError, ValueError):
            return False
        received_ms = int(received_at_ms if received_at_ms is not None else _now_ms())
        record = {
            "request_type": _body_type(body),
            "request_body": dict(body or {}),
            "received_at_ms": received_ms,
            "payload": copy.deepcopy(payload),
        }
        self._ensure_directory()
        path = self._cache_path(body)
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(record, fh, sort_keys=True, separators=(",", ":"))
            os.replace(tmp_path, path)
            return True
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

    async def async_wait_until_ready(
        self,
        body: Mapping[str, Any] | None = None,
    ) -> HyperliquidInfoReservation:
        return await asyncio.to_thread(self.wait_until_ready, body)

    def wait_until_ready(
        self,
        body: Mapping[str, Any] | None = None,
    ) -> HyperliquidInfoReservation:
        request_type = _body_type(body)
        if not self.enabled:
            return HyperliquidInfoReservation(
                request_type=request_type,
                waited_ms=0,
                reserved_at_monotonic_ns=time.monotonic_ns(),
            )
        self._ensure_directory()
        waited_ns = 0
        interval_ns = self.min_interval_ms * 1_000_000
        while True:
            wait_ns = 0
            with self._lock():
                state = self._read_state()
                now_ns = time.monotonic_ns()
                last_ns = self._state_monotonic_ns(state, "last_request_monotonic_ns", now_ns)
                cooldown_until_ns = self._state_monotonic_ns(
                    state,
                    "cooldown_until_monotonic_ns",
                    now_ns,
                )
                ready_at_ns = max(last_ns + interval_ns, cooldown_until_ns)
                if ready_at_ns > now_ns:
                    wait_ns = ready_at_ns - now_ns
                else:
                    state["last_request_monotonic_ns"] = now_ns
                    self._write_state(state)
                    return HyperliquidInfoReservation(
                        request_type=request_type,
                        waited_ms=int(waited_ns / 1_000_000),
                        reserved_at_monotonic_ns=now_ns,
                    )
            if wait_ns > 0:
                time.sleep(wait_ns / 1_000_000_000)
                waited_ns += wait_ns

    def record_http_response(
        self,
        status_code: int,
        headers: Mapping[str, Any] | None = None,
        *,
        body: Mapping[str, Any] | None = None,
    ) -> int:
        if not self.enabled:
            return 0
        status = int(status_code)
        if status not in (418, 429):
            # A confirmed successful /info response is the only signal that
            # clears accumulated throttling pressure.  Other failures must
            # not make a subsequent 429 start again from the minimum delay.
            if 200 <= status < 300:
                self._clear_rate_limit_backoff()
            return 0
        retry_after_ms = self._retry_after_ms(headers)
        if self.cooldown_ms <= 0 and retry_after_ms <= 0:
            return 0
        self._ensure_directory()
        with self._lock():
            state = self._read_state()
            now_ns = time.monotonic_ns()
            prior_failures = self._state_positive_int(
                state,
                "consecutive_rate_limit_count",
            )
            failures = prior_failures + 1
            exponential_ms = min(
                self.cooldown_ms * (2 ** min(failures - 1, 30)),
                self.max_cooldown_ms,
            )
            cooldown_ms = max(retry_after_ms, exponential_ms)
            cooldown_ns = cooldown_ms * 1_000_000
            existing = self._state_monotonic_ns(
                state,
                "cooldown_until_monotonic_ns",
                now_ns,
            )
            state["cooldown_until_monotonic_ns"] = max(existing, now_ns + cooldown_ns)
            state["last_rate_limit_type"] = _body_type(body)
            state["last_rate_limit_status_code"] = status
            state["last_rate_limit_wall_ms"] = _now_ms()
            state["last_rate_limit_scope"] = "POST /info"
            state["consecutive_rate_limit_count"] = failures
            self._write_state(state)
        return cooldown_ms

    def _clear_rate_limit_backoff(self) -> None:
        self._ensure_directory()
        with self._lock():
            state = self._read_state()
            if self._state_positive_int(state, "consecutive_rate_limit_count") <= 0:
                return
            state["consecutive_rate_limit_count"] = 0
            state["last_rate_limit_recovered_wall_ms"] = _now_ms()
            self._write_state(state)

    def _ensure_directory(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, body: Mapping[str, Any] | None) -> Path:
        canonical = json.dumps(
            dict(body or {}),
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return self.directory / f"metadata-{digest}.json"

    def _read_state(self) -> dict[str, Any]:
        try:
            with self._state_path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError):
            return {}
        return raw if isinstance(raw, dict) else {}

    def _write_state(self, state: Mapping[str, Any]) -> None:
        tmp_path = self._state_path.with_name(f"{self._state_path.name}.{os.getpid()}.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(dict(state), fh, sort_keys=True, separators=(",", ":"))
            os.replace(tmp_path, self._state_path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

    def _lock(self) -> "_AtomicLock":
        return _AtomicLock(self._lock_path, stale_ms=self.lock_stale_ms)

    @staticmethod
    def _state_monotonic_ns(
        state: Mapping[str, Any],
        key: str,
        now_ns: int,
    ) -> int:
        try:
            value = int(state.get(key) or 0)
        except (TypeError, ValueError):
            return 0
        if value < 0:
            return 0
        max_reasonable_future_ns = now_ns + 3_600_000_000_000
        if value > max_reasonable_future_ns:
            return 0
        return value

    @staticmethod
    def _state_positive_int(state: Mapping[str, Any], key: str) -> int:
        try:
            return max(int(state.get(key) or 0), 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _retry_after_ms(headers: Mapping[str, Any] | None) -> int:
        if not headers:
            return 0
        retry_after = None
        for key, value in headers.items():
            if str(key).lower() == "retry-after":
                retry_after = str(value).strip()
                break
        if not retry_after:
            return 0
        try:
            return max(int(float(retry_after) * 1000), 0)
        except (TypeError, ValueError):
            try:
                parsed = email.utils.parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return 0
            if parsed is None:
                return 0
            return max(int(parsed.timestamp() * 1000) - _now_ms(), 0)


class _AtomicLock:
    def __init__(self, path: Path, *, stale_ms: int) -> None:
        self.path = path
        self.stale_ms = stale_ms
        self._fd: int | None = None

    def __enter__(self) -> "_AtomicLock":
        while True:
            try:
                self._fd = os.open(
                    str(self.path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                os.write(self._fd, str(os.getpid()).encode("ascii"))
                return self
            except FileExistsError:
                self._remove_if_stale()
                time.sleep(0.005)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        fd = self._fd
        self._fd = None
        if fd is not None:
            os.close(fd)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def _remove_if_stale(self) -> None:
        try:
            age_ms = max((time.time_ns() - self.path.stat().st_mtime_ns) // 1_000_000, 0)
        except FileNotFoundError:
            return
        if age_ms < self.stale_ms:
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def hyperliquid_info_coordinator() -> HyperliquidInfoCoordinator:
    return HyperliquidInfoCoordinator.from_environment()
