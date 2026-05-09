"""Walk-forward replay: deterministically generated time windows."""

from __future__ import annotations

from dataclasses import dataclass


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
    """Generate deterministic walk-forward time windows."""
    windows: list[WalkForwardWindow] = []
    # Simple deterministic generation
    idx = 0
    d = start_date
    while d < end_date:
        windows.append(
            WalkForwardWindow(
                index=idx,
                train_from=d,
                train_to=d,
                test_from=d,
                test_to=d,
            )
        )
        idx += 1
        d = end_date  # placeholder - would implement date arithmetic
    return windows
