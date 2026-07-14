from __future__ import annotations

import json

import pytest

from lightfee.spread.research_manifest import load_spread_research_manifest


def _manifest() -> dict:
    return {
        "version": "test_manifest_v2",
        "model_epoch": "v2_signed_reversion",
        "hypothesis": "test signed basis",
        "cohorts": [
            {
                "bot_id": "tt_baseline",
                "cohort": "baseline",
                "hypothesis": "taker/taker",
                "enabled": True,
                "control_group": False,
                "acceptance_eligible": True,
            },
            {
                "bot_id": "maker_control",
                "cohort": "control",
                "hypothesis": "maker delay",
                "enabled": False,
                "control_group": True,
                "acceptance_eligible": False,
                "entry_long_role": "maker",
                "maker_leg": "long",
                "hedge_delay_ms": 1_000,
            },
        ],
    }


def test_research_manifest_records_cohort_controls_and_acceptance(tmp_path) -> None:
    path = tmp_path / "spread.json"
    path.write_text(json.dumps(_manifest()), encoding="utf-8")

    manifest = load_spread_research_manifest(path)

    assert manifest.version == "test_manifest_v2"
    assert manifest.enabled_bot_ids == ("tt_baseline",)
    assert manifest.cohort_for("maker_control").control_group is True
    assert manifest.cohort_for("maker_control").hedge_delay_ms == 1_000
    assert len(manifest.digest) == 64


def test_research_manifest_digest_changes_when_execution_contract_changes(tmp_path) -> None:
    first_path = tmp_path / "first.json"
    first_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    second = _manifest()
    second["cohorts"][0]["hypothesis"] = "changed execution contract"
    second_path = tmp_path / "second.json"
    second_path.write_text(json.dumps(second), encoding="utf-8")

    assert load_spread_research_manifest(first_path).digest != load_spread_research_manifest(
        second_path
    ).digest


def test_research_manifest_fails_closed_for_duplicate_or_unacceptable_cohorts(tmp_path) -> None:
    payload = _manifest()
    payload["cohorts"].append(dict(payload["cohorts"][0]))
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate bot_id"):
        load_spread_research_manifest(path)


@pytest.mark.parametrize("field", ["enabled", "control_group", "acceptance_eligible"])
def test_research_manifest_rejects_truthy_non_boolean_admission_controls(
    tmp_path, field: str
) -> None:
    payload = _manifest()
    payload["cohorts"][0][field] = "false"
    path = tmp_path / "invalid-bool.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=f"{field} must be a boolean"):
        load_spread_research_manifest(path)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda cohort: cohort.update(
                entry_long_role="maker", maker_leg="", control_group=True
            ),
            "maker_leg must match",
        ),
        (
            lambda cohort: cohort.update(
                entry_long_role="maker",
                maker_leg="long",
                control_group=False,
                acceptance_eligible=True,
            ),
            "non-acceptance control",
        ),
        (
            lambda cohort: cohort.update(exit_short_role="maker"),
            "exit maker is not supported",
        ),
    ],
)
def test_research_manifest_rejects_cohorts_the_paper_state_machine_cannot_model(
    tmp_path, mutate, message: str
) -> None:
    payload = _manifest()
    maker = payload["cohorts"][1]
    mutate(maker)
    path = tmp_path / "invalid-maker.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_spread_research_manifest(path)
