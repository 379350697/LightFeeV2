"""Bounded, no-look-ahead Expected Shortfall for paired funding basis risk.

Funding carry is delta-neutral only at entry.  The economically relevant risk
budget is therefore the adverse movement of the *cross-venue basis*, not the
underlying asset's outright return.  This module owns that observation and ES
contract so candidate generation, dispatch and portfolio admission cannot each
invent a different volatility proxy.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Deque


FUNDING_BASIS_RISK_CHECKPOINT_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class BasisObservation:
    observed_at_ms: int
    signed_basis_bps: float
    batch_id: int


@dataclass(frozen=True, slots=True)
class FundingBasisExpectedShortfallEstimate:
    expected_shortfall_bps: float
    sample_count: int
    return_count: int
    history_ms: int
    confidence: float
    evidence_complete: bool
    reason: str
    model_version: str = "funding_basis_es_v2"


class FundingBasisExpectedShortfallModel:
    """Estimate one-sided historical ES from canonical signed-basis returns.

    A caller groups observations from one snapshot into a ``batch_id``.  The
    current batch is deliberately excluded from estimates.  This gives the
    same no-current-observation rule as the spread signal: an entry cannot use
    its own snapshot to make its risk estimate look safer.
    """

    def __init__(
        self,
        *,
        window_ms: int,
        max_samples: int,
        max_pairs: int,
        horizon_ms: int,
        min_samples: int,
        min_history_ms: int,
        confidence: float,
        quote_skew_ms: int,
    ) -> None:
        self.window_ms = _positive_int(window_ms)
        self.max_samples = _positive_int(max_samples)
        self.max_pairs = _positive_int(max_pairs)
        self.horizon_ms = _positive_int(horizon_ms)
        self.min_samples = _positive_int(min_samples)
        self.min_history_ms = _positive_int(min_history_ms)
        self.confidence = _finite_float(confidence)
        self.quote_skew_ms = _positive_int(quote_skew_ms)
        self._states: OrderedDict[tuple[str, str, str], Deque[BasisObservation]] = (
            OrderedDict()
        )
        self._next_batch_id = 1
        self._current_batch_id = 0

    @property
    def state_count(self) -> int:
        return len(self._states)

    def begin_observation_batch(self) -> int:
        """Begin a fresh public-snapshot batch and return its opaque id."""

        batch_id = self._next_batch_id
        self._next_batch_id += 1
        self._current_batch_id = batch_id
        return batch_id

    def observe_pair(
        self,
        *,
        symbol: str,
        venue_a: str,
        venue_b: str,
        bid_a: float,
        ask_a: float,
        observed_a_ms: int,
        bid_b: float,
        ask_b: float,
        observed_b_ms: int,
        now_ms: int,
        batch_id: int,
    ) -> bool:
        """Record one fresh canonical basis observation if it is evidenced.

        Both timestamps must be explicit and contemporaneous; absent timestamp
        data is never replaced by ``now_ms``.  Only the public sidecar calls
        this method, so it adds no REST traffic.
        """

        key = _canonical_pair_key(symbol, venue_a, venue_b)
        if key is None or int(batch_id or 0) <= 0:
            return False
        values = (bid_a, ask_a, bid_b, ask_b)
        if not all(value is not None and value > 0.0 for value in map(_finite_float, values)):
            return False
        observed_a = _positive_int(observed_a_ms)
        observed_b = _positive_int(observed_b_ms)
        current = _positive_int(now_ms)
        if (
            observed_a <= 0
            or observed_b <= 0
            or current <= 0
            or observed_a > current
            or observed_b > current
            or abs(observed_a - observed_b) > self.quote_skew_ms
        ):
            return False
        mid_a = (float(bid_a) + float(ask_a)) / 2.0
        mid_b = (float(bid_b) + float(ask_b)) / 2.0
        reference_mid = (mid_a + mid_b) / 2.0
        if not math.isfinite(reference_mid) or reference_mid <= 0.0:
            return False

        # ``key`` venue order is canonical; flip the raw basis when the
        # caller supplied the reverse directed candidate.
        canonical_a = key[1]
        raw_basis_bps = (mid_a - mid_b) / reference_mid * 10_000.0
        signed_basis_bps = raw_basis_bps if _venue_key(venue_a) == canonical_a else -raw_basis_bps
        observed_at_ms = min(observed_a, observed_b)
        if not math.isfinite(signed_basis_bps):
            return False

        state = self._states.get(key)
        if state is None:
            if len(self._states) >= self.max_pairs:
                # Eviction produces a cold-start estimate for an inactive pair;
                # it can never make an existing pair look lower risk.
                self._states.popitem(last=False)
            state = deque()
            self._states[key] = state
        else:
            self._states.move_to_end(key)
        if state and observed_at_ms < state[-1].observed_at_ms:
            return False
        observation = BasisObservation(
            observed_at_ms=observed_at_ms,
            signed_basis_bps=signed_basis_bps,
            batch_id=int(batch_id),
        )
        if state and observed_at_ms == state[-1].observed_at_ms:
            # A sidecar retry must not turn one market instant into a second
            # independent return.  Preserve the original price rather than
            # letting retry ordering modify historical risk.
            return True
        state.append(observation)
        self._prune_state(state, now_ms=current)
        return True

    def estimate(
        self,
        *,
        symbol: str,
        long_venue: str,
        short_venue: str,
        now_ms: int,
    ) -> FundingBasisExpectedShortfallEstimate:
        key = _canonical_pair_key(symbol, long_venue, short_venue)
        if key is None:
            return _incomplete("missing_candidate_basis_identity", self.confidence)
        if not 0.0 < self.confidence < 1.0:
            return _incomplete("invalid_model_configuration", self.confidence)
        state = self._states.get(key)
        if not state:
            return _incomplete("basis_risk_cold_start", self.confidence)
        self._states.move_to_end(key)
        current = _positive_int(now_ms)
        if current <= 0:
            return _incomplete("invalid_estimate_time", self.confidence)
        self._prune_state(state, now_ms=current)
        # Current-batch observations are excluded by design.  The snapshot
        # that made an entry attractive cannot influence its risk estimate.
        observations = [
            item
            for item in state
            if item.batch_id != self._current_batch_id and item.observed_at_ms < current
        ]
        history_ms = _history_ms(observations)
        if history_ms < self.min_history_ms:
            return FundingBasisExpectedShortfallEstimate(
                0.0,
                len(observations),
                0,
                history_ms,
                self.confidence,
                False,
                "insufficient_basis_history",
            )

        # Long canonical venue A / short B loses when signed basis falls;
        # the opposite orientation loses when it rises.  Build disjoint
        # horizon pairs rather than a rolling t-horizon return.  Reusing a
        # dense stream's prior observations makes thousands of highly
        # correlated ticks look like independent tail samples and can
        # materially understate expected shortfall.
        long_is_canonical_a = _venue_key(long_venue) == key[1]
        losses: list[float] = []
        max_gap_ms = self.horizon_ms * 2
        prior_index = 0
        while prior_index + 1 < len(observations):
            prior = observations[prior_index]
            target_ms = prior.observed_at_ms + self.horizon_ms
            later_index = prior_index + 1
            while (
                later_index < len(observations)
                and observations[later_index].observed_at_ms < target_ms
            ):
                later_index += 1
            if later_index >= len(observations):
                break
            later = observations[later_index]
            elapsed = later.observed_at_ms - prior.observed_at_ms
            if elapsed < self.horizon_ms or elapsed > max_gap_ms:
                # A sparse outage cannot manufacture a regular return.  Skip
                # this anchor and seek a later independently observable pair.
                prior_index = later_index + 1
                continue
            basis_change_bps = later.signed_basis_bps - prior.signed_basis_bps
            adverse_loss_bps = -basis_change_bps if long_is_canonical_a else basis_change_bps
            losses.append(max(adverse_loss_bps, 0.0))
            # Do not reuse either endpoint in the next return sample.
            prior_index = later_index + 1
        if len(losses) < self.min_samples:
            return FundingBasisExpectedShortfallEstimate(
                0.0,
                len(observations),
                len(losses),
                history_ms,
                self.confidence,
                False,
                "insufficient_basis_return_samples",
            )
        sorted_losses = sorted(losses)
        tail_size = max(1, math.ceil((1.0 - self.confidence) * len(sorted_losses)))
        expected_shortfall_bps = sum(sorted_losses[-tail_size:]) / tail_size
        if not math.isfinite(expected_shortfall_bps) or expected_shortfall_bps <= 0.0:
            # A zero empirical tail is not a license for free leverage.  The
            # legacy static floor may raise a calibrated estimate, but it may
            # not replace evidence that no adverse return has been observed.
            return FundingBasisExpectedShortfallEstimate(
                0.0,
                len(observations),
                len(losses),
                history_ms,
                self.confidence,
                False,
                "nonpositive_basis_expected_shortfall",
            )
        return FundingBasisExpectedShortfallEstimate(
            expected_shortfall_bps,
            len(observations),
            len(losses),
            history_ms,
            self.confidence,
            True,
            "",
        )

    def checkpoint(self, *, now_ms: int) -> dict:
        current = _positive_int(now_ms)
        states: dict[str, list[dict]] = {}
        for key, state in self._states.items():
            self._prune_state(state, now_ms=current)
            if not state:
                continue
            states["|".join(key)] = [asdict(item) for item in state]
        return {
            "schema_version": FUNDING_BASIS_RISK_CHECKPOINT_SCHEMA_VERSION,
            "saved_at_ms": current,
            "next_batch_id": self._next_batch_id,
            "states": states,
        }

    def restore(self, payload: object, *, now_ms: int, max_age_ms: int) -> bool:
        """Restore only a current, complete bounded checkpoint."""

        if not isinstance(payload, dict):
            return False
        if int(payload.get("schema_version", 0) or 0) != FUNDING_BASIS_RISK_CHECKPOINT_SCHEMA_VERSION:
            return False
        current = _positive_int(now_ms)
        saved = _positive_int(payload.get("saved_at_ms", 0))
        if current <= 0 or saved <= 0 or saved > current or current - saved > _positive_int(max_age_ms):
            return False
        raw_states = payload.get("states")
        if not isinstance(raw_states, dict) or len(raw_states) > self.max_pairs:
            return False
        restored: OrderedDict[tuple[str, str, str], Deque[BasisObservation]] = OrderedDict()
        try:
            for raw_key, raw_observations in raw_states.items():
                parts = str(raw_key).split("|")
                key = _canonical_pair_key(*parts) if len(parts) == 3 else None
                if key is None or not isinstance(raw_observations, list):
                    return False
                state: Deque[BasisObservation] = deque()
                previous_ms = 0
                for raw in raw_observations:
                    if not isinstance(raw, dict):
                        return False
                    observed = _positive_int(raw.get("observed_at_ms", 0))
                    basis = _finite_float(raw.get("signed_basis_bps"))
                    batch = _positive_int(raw.get("batch_id", 0))
                    if (
                        observed <= previous_ms
                        or observed > current
                        or basis is None
                        or batch <= 0
                    ):
                        return False
                    previous_ms = observed
                    if current - observed <= self.window_ms:
                        state.append(BasisObservation(observed, basis, batch))
                if len(state) > self.max_samples:
                    return False
                if state:
                    restored[key] = state
            next_batch = _positive_int(payload.get("next_batch_id", 0))
            if next_batch <= 0:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        self._states = restored
        self._next_batch_id = next_batch
        self._current_batch_id = 0
        return True

    def _prune_state(self, state: Deque[BasisObservation], *, now_ms: int) -> None:
        cutoff = max(_positive_int(now_ms) - self.window_ms, 0)
        while state and state[0].observed_at_ms < cutoff:
            state.popleft()
        while len(state) > self.max_samples:
            state.popleft()


def restore_funding_basis_risk_checkpoint(
    model: FundingBasisExpectedShortfallModel,
    path: str | Path,
    *,
    now_ms: int,
    max_age_ms: int,
) -> bool:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return model.restore(payload, now_ms=now_ms, max_age_ms=max_age_ms)


def publish_funding_basis_risk_checkpoint(
    model: FundingBasisExpectedShortfallModel,
    path: str | Path,
    *,
    now_ms: int,
) -> None:
    """Atomically publish only the model's bounded state."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = model.checkpoint(now_ms=now_ms)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".funding-basis-risk-")
    os.close(fd)
    temporary = Path(tmp_name)
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _canonical_pair_key(symbol: object, venue_a: object, venue_b: object) -> tuple[str, str, str] | None:
    symbol_key = str(symbol or "").upper().strip()
    first = _venue_key(venue_a)
    second = _venue_key(venue_b)
    if not symbol_key or not first or not second or first == second:
        return None
    low, high = sorted((first, second))
    return symbol_key, low, high


def _venue_key(value: object) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _positive_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return 0
    return int(numeric)


def _finite_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _history_ms(observations: list[BasisObservation]) -> int:
    if len(observations) < 2:
        return 0
    return max(observations[-1].observed_at_ms - observations[0].observed_at_ms, 0)


def _incomplete(reason: str, confidence: float) -> FundingBasisExpectedShortfallEstimate:
    return FundingBasisExpectedShortfallEstimate(
        0.0, 0, 0, 0, confidence, False, reason
    )
