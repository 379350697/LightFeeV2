import sys

import pytest

from scripts import check_bug_ledger as ledger


def test_read_tracker_parses_current_batch(tmp_path):
    tracker = tmp_path / "ACTIVE.md"
    tracker.write_text(
        "\n".join(
            [
                "- Last production SHA checked: `1234567`",
                "",
                "| ID | Status | Fix commit | Regression evidence | Production evidence / next condition | History |",
                "|---|---|---|---|---|---|",
                "| CL-001 | deployed-awaiting-verification | `7654321` | regression | needs a probe | [daily](daily.md) |",
            ]
        ),
        encoding="utf-8",
    )

    parsed = ledger.read_tracker(tracker)

    assert parsed.recorded_deploy_sha == "1234567"
    assert parsed.rows == [
        ledger.LedgerRow(
            bug_id="CL-001",
            status="deployed-awaiting-verification",
            fix_commit="7654321",
            regression_evidence="regression",
            production_evidence="needs a probe",
            history="[daily](daily.md)",
        )
    ]


def test_validate_tracker_rejects_deployed_fix_absent_from_recorded_deploy():
    tracker = ledger.Tracker(
        recorded_deploy_sha="recorded",
        rows=[
            ledger.LedgerRow(
                bug_id="CL-001",
                status="deployed-awaiting-verification",
                fix_commit="fix",
                regression_evidence="regression",
                production_evidence="needs a probe",
                history="[daily](daily.md)",
            )
        ],
    )

    errors = ledger.validate_tracker(
        tracker,
        resolve_commit=lambda sha: sha,
        is_ancestor=lambda _older, _newer: False,
    )

    assert errors == [
        "CL-001: status 'deployed-awaiting-verification' claims deployment, but "
        "fix is not in recorded deploy recorded"
    ]


def test_validate_tracker_rejects_unknown_status_and_unnamed_supersession():
    tracker = ledger.Tracker(
        recorded_deploy_sha="recorded",
        rows=[
            ledger.LedgerRow(
                bug_id="CL-001",
                status="done-ish",
                fix_commit="fix",
                regression_evidence="some regression",
                production_evidence="some evidence",
                history="[daily](daily.md)",
            ),
            ledger.LedgerRow(
                bug_id="CL-002",
                status="superseded",
                fix_commit="fix",
                regression_evidence="",
                production_evidence="replaced by a better design",
                history="[daily](daily.md)",
            ),
        ],
    )

    errors = ledger.validate_tracker(
        tracker,
        resolve_commit=lambda sha: sha,
        is_ancestor=lambda _older, _newer: True,
    )

    assert errors == [
        "CL-001: unknown status 'done-ish'",
        "CL-002: superseded rows must name their replacement CL",
    ]


def test_validate_tracker_requires_two_independent_closed_evidence_cells():
    tracker = ledger.Tracker(
        recorded_deploy_sha="recorded",
        rows=[
            ledger.LedgerRow(
                bug_id="CL-001",
                status="closed",
                fix_commit="fix",
                regression_evidence="",
                production_evidence="",
                history="[daily](daily.md)",
            )
        ],
    )

    errors = ledger.validate_tracker(
        tracker,
        resolve_commit=lambda sha: sha,
        is_ancestor=lambda _older, _newer: True,
    )

    assert errors == [
        "CL-001: closed requires regression evidence",
        "CL-001: closed requires production evidence",
    ]


def test_parse_args_requires_a_production_sha(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["check_bug_ledger.py"])

    with pytest.raises(SystemExit) as error:
        ledger.parse_args()

    assert error.value.code == 2
