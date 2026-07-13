"""Bounded, restart-safe calibration of funding forecast revision errors."""

from __future__ import annotations

import json
import math
import os
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from lightfee.sidecar.snapshot import QuoteSnapshot


_CALIBRATION_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
_MAX_ERRORS_PER_KEY = 512


@dataclass(frozen=True)
class _PendingForecast:
    settlement_timestamp_ms: int
    predicted_rate_bps: float
    observed_at_ms: int
    funding_interval_ms: int


class FundingForecastCalibrator:
    """Tracks forecast-to-settlement errors without an extra REST request.

    A rate becomes a settlement observation only when an exchange advances its
    next-settlement timestamp and explicitly exposes the prior settled rate.
    This avoids guessing that a current rate was settled or inventing a
    universal funding interval.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        min_samples: int = 0,
        max_quantile_drift_bps: float = 0.0,
    ) -> None:
        self._path = Path(path)
        self._min_samples = max(int(min_samples or 0), 0)
        try:
            drift_limit = float(max_quantile_drift_bps)
        except (TypeError, ValueError, OverflowError):
            drift_limit = -1.0
        self._max_quantile_drift_bps = (
            drift_limit if math.isfinite(drift_limit) and drift_limit >= 0.0 else -1.0
        )
        self._pending: dict[str, _PendingForecast] = {}
        self._errors: dict[str, list[tuple[int, float]]] = defaultdict(list)
        self._started_at_ms: dict[str, int] = {}
        self._load()

    @staticmethod
    def _key(quote: QuoteSnapshot) -> str:
        return f"{str(quote.venue).lower()}:{str(quote.symbol).upper()}"

    def prime(self, quotes: dict[str, QuoteSnapshot]) -> None:
        for quote in quotes.values():
            key = self._key(quote)
            started_at_ms = int(
                quote.funding_forecast_started_at_ms or quote.observed_at_ms or 0
            )
            if started_at_ms > 0:
                self._started_at_ms.setdefault(key, started_at_ms)
            timestamp = int(quote.funding_timestamp_ms or 0)
            if timestamp <= 0:
                continue
            rate = quote.predicted_funding_rate_bps
            predicted = float(rate if rate is not None else quote.funding_rate_bps)
            if math.isfinite(predicted):
                self._pending.setdefault(
                    key,
                    _PendingForecast(
                        timestamp,
                        predicted,
                        int(quote.observed_at_ms or 0),
                        max(int(quote.funding_interval_ms or 0), 0),
                    ),
                )

    def apply(self, quotes: dict[str, QuoteSnapshot], *, now_ms: int) -> None:
        changed = False
        current_now_ms = max(int(now_ms or 0), 0)
        for quote in quotes.values():
            key = self._key(quote)
            observed_at_ms = int(quote.observed_at_ms or current_now_ms or 0)
            # A source observation after the refresh clock is not evidence of
            # a settlement.  Recording it would let a bad venue/local clock
            # advance the persisted error distribution and later make an
            # enhanced-live forecast appear calibrated.  Preserve historical
            # calibration state for a later valid observation, but expose no
            # usable confidence on this untrusted quote.
            if observed_at_ms > current_now_ms:
                quote.funding_forecast_sample_count = 0
                quote.funding_forecast_uncertainty_bps = 0.0
                quote.funding_forecast_started_at_ms = 0
                quote.funding_forecast_distribution_stable = False
                quote.funding_forecast_stability_reason = "future_quote_timestamp"
                quote.funding_forecast_median_drift_bps = 0.0
                quote.funding_forecast_p90_drift_bps = 0.0
                continue
            effective_now_ms = current_now_ms
            if observed_at_ms > 0 and key not in self._started_at_ms:
                self._started_at_ms[key] = observed_at_ms
                changed = True
            timestamp = int(quote.funding_timestamp_ms or 0)
            pending = self._pending.get(key)
            settled = quote.settled_funding_rate_bps
            if (
                timestamp > 0
                and pending is not None
                and timestamp
                == pending.settlement_timestamp_ms + pending.funding_interval_ms
                and pending.funding_interval_ms > 0
                # An advanced ``nextFundingTime`` alone is not proof that the
                # preceding cash event has happened.  A venue/local clock can
                # be ahead, or a payload can be internally inconsistent.  Do
                # not let a claimed settled rate calibrate an enhanced-live
                # model before its own settlement timestamp.
                and observed_at_ms >= pending.settlement_timestamp_ms
                and settled is not None
                and math.isfinite(float(settled))
            ):
                error = abs(float(settled) - pending.predicted_rate_bps)
                if math.isfinite(error):
                    self._errors[key].append((effective_now_ms, error))
                    changed = True
            rate = quote.predicted_funding_rate_bps
            predicted = float(rate if rate is not None else quote.funding_rate_bps)
            if timestamp > 0 and math.isfinite(predicted) and (
                pending is None or timestamp != pending.settlement_timestamp_ms
            ):
                self._pending[key] = _PendingForecast(
                    timestamp,
                    predicted,
                    int(quote.observed_at_ms or now_ms or 0),
                    max(int(quote.funding_interval_ms or 0), 0),
                )
                changed = True
            prior_errors = tuple(self._errors.get(key, []))
            self._prune(key, now_ms)
            changed = changed or tuple(self._errors.get(key, [])) != prior_errors
            errors = self._errors.get(key, [])
            quote.funding_forecast_sample_count = len(errors)
            quote.funding_forecast_uncertainty_bps = self._p90(error for _, error in errors)
            quote.funding_forecast_started_at_ms = int(
                self._started_at_ms.get(key, 0)
            )
            (
                quote.funding_forecast_distribution_stable,
                quote.funding_forecast_stability_reason,
                quote.funding_forecast_median_drift_bps,
                quote.funding_forecast_p90_drift_bps,
            ) = self._distribution_stability(
                errors,
                now_ms=effective_now_ms,
                funding_interval_ms=int(quote.funding_interval_ms or 0),
            )
        if changed:
            self._save()

    def _prune(self, key: str, now_ms: int) -> None:
        cutoff = max(int(now_ms or 0) - _CALIBRATION_WINDOW_MS, 0)
        kept = [item for item in self._errors.get(key, []) if item[0] >= cutoff]
        self._errors[key] = sorted(kept, key=lambda item: item[0])[-_MAX_ERRORS_PER_KEY:]

    def _distribution_stability(
        self,
        errors: list[tuple[int, float]],
        *,
        now_ms: int,
        funding_interval_ms: int,
    ) -> tuple[bool, str, float, float]:
        """Validate a bounded forecast-error distribution for enhanced live.

        The test deliberately uses the newest configured sample window and
        compares its older/newer halves.  A sample count alone cannot detect a
        forecast that has recently begun revising more often.  We check both a
        robust central statistic (median) and tail statistic (p90), and also
        require that the latest settled observation has not gone stale by more
        than two advertised funding intervals.
        """
        if self._min_samples <= 0:
            return False, "invalid_min_samples", 0.0, 0.0
        if self._max_quantile_drift_bps < 0.0:
            return False, "invalid_quantile_drift_limit", 0.0, 0.0
        ordered = sorted(
            (
                (int(observed_at_ms), float(error))
                for observed_at_ms, error in errors
                if int(observed_at_ms) > 0 and math.isfinite(float(error)) and float(error) >= 0.0
            ),
            key=lambda item: item[0],
        )
        if len(ordered) < self._min_samples:
            return False, "insufficient_settlement_samples", 0.0, 0.0
        if funding_interval_ms <= 0:
            return False, "missing_funding_interval", 0.0, 0.0
        newest_observed_at_ms = ordered[-1][0]
        if now_ms > newest_observed_at_ms and (
            now_ms - newest_observed_at_ms > 2 * funding_interval_ms
        ):
            return False, "stale_settlement_evidence", 0.0, 0.0

        window = ordered[-self._min_samples:]
        split = len(window) // 2
        older = [error for _, error in window[:split]]
        newer = [error for _, error in window[split:]]
        if not older or not newer:
            return False, "insufficient_distribution_halves", 0.0, 0.0
        median_drift_bps = abs(
            self._quantile(older, 0.50) - self._quantile(newer, 0.50)
        )
        p90_drift_bps = abs(self._quantile(older, 0.90) - self._quantile(newer, 0.90))
        if median_drift_bps > self._max_quantile_drift_bps:
            return False, "median_error_distribution_drift", median_drift_bps, p90_drift_bps
        if p90_drift_bps > self._max_quantile_drift_bps:
            return False, "p90_error_distribution_drift", median_drift_bps, p90_drift_bps
        return True, "stable", median_drift_bps, p90_drift_bps

    @staticmethod
    def _p90(values: object) -> float:
        return FundingForecastCalibrator._quantile(values, 0.90)

    @staticmethod
    def _quantile(values: object, quantile: float) -> float:
        ordered = sorted(float(value) for value in values)
        if not ordered:
            return 0.0
        index = min(math.ceil(len(ordered) * quantile) - 1, len(ordered) - 1)
        return max(ordered[index], 0.0)

    def _load(self) -> None:
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(raw, dict):
            return
        pending = raw.get("pending", {})
        if isinstance(pending, dict):
            for key, item in pending.items():
                if not isinstance(item, dict):
                    continue
                try:
                    candidate = _PendingForecast(
                        settlement_timestamp_ms=int(item.get("settlement_timestamp_ms", 0)),
                        predicted_rate_bps=float(item.get("predicted_rate_bps", 0.0)),
                        observed_at_ms=int(item.get("observed_at_ms", 0)),
                        funding_interval_ms=max(
                            int(item.get("funding_interval_ms", 0) or 0), 0
                        ),
                    )
                except (TypeError, ValueError):
                    continue
                if candidate.settlement_timestamp_ms > 0 and math.isfinite(candidate.predicted_rate_bps):
                    self._pending[str(key)] = candidate
        errors = raw.get("errors", {})
        if isinstance(errors, dict):
            for key, items in errors.items():
                if not isinstance(items, list):
                    continue
                valid: list[tuple[int, float]] = []
                for item in items[-_MAX_ERRORS_PER_KEY:]:
                    if not isinstance(item, list) or len(item) != 2:
                        continue
                    try:
                        observed, error = int(item[0]), float(item[1])
                    except (TypeError, ValueError):
                        continue
                    if observed > 0 and math.isfinite(error) and error >= 0.0:
                        valid.append((observed, error))
                self._errors[str(key)] = valid
        started_at_ms = raw.get("started_at_ms", {})
        if isinstance(started_at_ms, dict):
            for key, value in started_at_ms.items():
                try:
                    started = int(value or 0)
                except (TypeError, ValueError):
                    continue
                if started > 0:
                    self._started_at_ms[str(key)] = started

    def _save(self) -> None:
        payload = {
            "schema_version": 1,
            "pending": {
                key: {
                    "settlement_timestamp_ms": value.settlement_timestamp_ms,
                    "predicted_rate_bps": value.predicted_rate_bps,
                    "observed_at_ms": value.observed_at_ms,
                    "funding_interval_ms": value.funding_interval_ms,
                }
                for key, value in self._pending.items()
            },
            "errors": {
                key: [[observed, error] for observed, error in values]
                for key, values in self._errors.items()
            },
            "started_at_ms": dict(self._started_at_ms),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            dir=str(self._path.parent), prefix=".funding-forecast-calibration-"
        )
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self._path)
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
