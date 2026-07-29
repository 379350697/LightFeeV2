"""Shared app logging configuration."""

from __future__ import annotations

import logging
import sys
from typing import TextIO


_NOISY_TRANSPORT_LOGGERS = (
    "httpx",
    "httpcore",
    "websockets",
    "websockets.client",
)


def configure_app_logging(
    *,
    level: int = logging.INFO,
    stream: TextIO | None = None,
) -> None:
    """Configure process logging while suppressing noisy transport internals."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=stream if stream is not None else sys.stderr,
    )
    for logger_name in _NOISY_TRANSPORT_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
