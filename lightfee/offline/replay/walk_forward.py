"""Walk-forward replay: deterministically generated time windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class WalkForwardWindow:
    index: int
    train_from: str
    train_to: str
    test_from: str
    test_to: str


def generate_walk_forward_windows(
    start_date: str,
    end_date: str,
    train_days: int = 7,
    test_days: int = 1,
) -> list[WalkForwardWindow]:
    """Generate deterministic walk-forward time windows.

    V1: each window has train period (config fitting) and test period
    (out-of-sample replay). Windows are non-overlapping in test periods
    and roll forward by test_days each step.
    """
    windows: list[WalkForwardWindow] = []
    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
    except (ValueError, TypeError):
        return windows

    idx = 0
    cursor = start
    while cursor + timedelta(days=train_days + test_days) <= end:
        train_end = cursor + timedelta(days=train_days)
        test_end = train_end + timedelta(days=test_days)
        windows.append(
            WalkForwardWindow(
                index=idx,
                train_from=cursor.strftime("%Y%m%d"),
                train_to=train_end.strftime("%Y%m%d"),
                test_from=train_end.strftime("%Y%m%d"),
                test_to=test_end.strftime("%Y%m%d"),
            )
        )
        cursor += timedelta(days=test_days)
        idx += 1

    return windows
