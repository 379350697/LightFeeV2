"""Versioned research-manifest contract for paper-spread cohorts.

The manifest is deliberately data, not a collection of symbol or bot
conditionals in the paper simulator.  Changing a hypothesis, control group,
or acceptance eligibility therefore creates a traceable new research epoch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SpreadResearchCohort:
    bot_id: str
    cohort: str
    hypothesis: str
    enabled: bool
    control_group: bool
    acceptance_eligible: bool
    entry_long_role: str = "taker"
    entry_short_role: str = "taker"
    exit_long_role: str = "taker"
    exit_short_role: str = "taker"
    maker_leg: str = ""
    hedge_delay_ms: int = 0


@dataclass(frozen=True, slots=True)
class SpreadResearchManifest:
    version: str
    model_epoch: str
    hypothesis: str
    cohorts: tuple[SpreadResearchCohort, ...]

    def cohort_for(self, bot_id: str) -> SpreadResearchCohort | None:
        target = str(bot_id or "").strip()
        return next(
            (cohort for cohort in self.cohorts if cohort.bot_id == target), None
        )

    @property
    def enabled_bot_ids(self) -> tuple[str, ...]:
        return tuple(cohort.bot_id for cohort in self.cohorts if cohort.enabled)


DEFAULT_SPREAD_RESEARCH_MANIFEST = SpreadResearchManifest(
    version="spread_research_manifest_v2",
    model_epoch="v2_signed_reversion",
    hypothesis="signed-basis mean reversion after executable costs",
    cohorts=(
        SpreadResearchCohort(
            bot_id="tt_conservative",
            cohort="taker_taker_baseline",
            hypothesis="executable taker/taker baseline",
            enabled=True,
            control_group=False,
            acceptance_eligible=True,
        ),
        SpreadResearchCohort(
            bot_id="mt_selected_maker_delay_1000ms",
            cohort="maker_taker_delay_control",
            hypothesis="maker fill and delayed hedge control",
            enabled=False,
            control_group=True,
            acceptance_eligible=False,
            entry_long_role="maker",
            maker_leg="long",
            hedge_delay_ms=1_000,
        ),
    ),
)


def load_spread_research_manifest(path: str | Path) -> SpreadResearchManifest:
    """Load a strict JSON manifest; malformed research configuration fails closed."""
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid spread research manifest: {manifest_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("spread research manifest must be an object")

    version = _required_string(payload, "version")
    model_epoch = _required_string(payload, "model_epoch")
    hypothesis = _required_string(payload, "hypothesis")
    raw_cohorts = payload.get("cohorts")
    if not isinstance(raw_cohorts, list) or not raw_cohorts:
        raise ValueError("spread research manifest requires non-empty cohorts")

    cohorts = tuple(_cohort(item) for item in raw_cohorts)
    bot_ids = [cohort.bot_id for cohort in cohorts]
    if len(bot_ids) != len(set(bot_ids)):
        raise ValueError("spread research manifest has duplicate bot_id")
    if not any(cohort.enabled and cohort.acceptance_eligible for cohort in cohorts):
        raise ValueError("spread research manifest has no enabled acceptance cohort")
    return SpreadResearchManifest(version, model_epoch, hypothesis, cohorts)


def _cohort(value: object) -> SpreadResearchCohort:
    if not isinstance(value, dict):
        raise ValueError("spread research cohort must be an object")
    role_fields = ("entry_long_role", "entry_short_role", "exit_long_role", "exit_short_role")
    roles = {field: str(value.get(field, "taker") or "taker").lower() for field in role_fields}
    if any(role not in {"maker", "taker"} for role in roles.values()):
        raise ValueError("spread research cohort liquidity role must be maker or taker")
    maker_leg = str(value.get("maker_leg", "") or "").lower()
    if maker_leg not in {"", "long", "short"}:
        raise ValueError("spread research cohort maker_leg must be long, short, or empty")
    cohort = SpreadResearchCohort(
        bot_id=_required_string(value, "bot_id"),
        cohort=_required_string(value, "cohort"),
        hypothesis=_required_string(value, "hypothesis"),
        enabled=_optional_bool(value, "enabled"),
        control_group=_optional_bool(value, "control_group"),
        acceptance_eligible=_optional_bool(value, "acceptance_eligible"),
        maker_leg=maker_leg,
        hedge_delay_ms=max(int(value.get("hedge_delay_ms", 0) or 0), 0),
        **roles,
    )
    _validate_cohort_execution_contract(cohort)
    return cohort


def _optional_bool(value: dict, field: str) -> bool:
    """Accept only JSON booleans for cohort admission controls.

    A string such as ``"false"`` is truthy in Python; accepting it would
    silently turn an experimental or disabled cohort into an enabled
    acceptance cohort and contaminate the paper baseline.
    """
    raw = value.get(field, False)
    if raw is True or raw is False:
        return raw
    raise ValueError(f"spread research cohort {field} must be a boolean")


def _validate_cohort_execution_contract(cohort: SpreadResearchCohort) -> None:
    """Reject cohorts the paper state machine cannot model truthfully.

    The baseline has a fully specified taker/taker fill contract.  The current
    control simulator can model exactly one *entry* maker leg followed by a
    delayed taker hedge, but cannot prove an exit-maker queue position.  A
    loose ``maker_leg`` field previously let a malformed maker cohort take an
    immediate fill at maker fees and, in one shape, be counted as official.
    """
    entry_maker_legs = tuple(
        leg
        for leg, role in (
            ("long", cohort.entry_long_role),
            ("short", cohort.entry_short_role),
        )
        if role == "maker"
    )
    exit_has_maker = (
        cohort.exit_long_role == "maker" or cohort.exit_short_role == "maker"
    )
    if exit_has_maker:
        raise ValueError("spread research cohort exit maker is not supported")
    if len(entry_maker_legs) > 1:
        raise ValueError("spread research cohort may have only one entry maker leg")
    if entry_maker_legs:
        maker_leg = entry_maker_legs[0]
        if cohort.maker_leg != maker_leg:
            raise ValueError("spread research cohort maker_leg must match entry maker role")
        if not cohort.control_group or cohort.acceptance_eligible:
            raise ValueError("spread maker cohort must be a non-acceptance control")
        return
    if cohort.maker_leg:
        raise ValueError("spread research cohort maker_leg requires an entry maker role")
    if cohort.acceptance_eligible and cohort.control_group:
        raise ValueError("spread acceptance cohort cannot be a control group")


def _required_string(payload: dict, key: str) -> str:
    value = str(payload.get(key, "") or "").strip()
    if not value:
        raise ValueError(f"spread research manifest requires {key}")
    return value
