"""Pytest configuration.

Puts ``src/`` on ``sys.path`` so the suite runs straight from a fresh clone
without requiring ``pip install -e .`` first.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def _reset_metrics():
    """Keep metric counters from leaking between tests."""
    from cadb.core.telemetry import METRICS

    yield
    METRICS.reset()


@pytest.fixture(autouse=True)
def _reset_sim_price_process():
    """The simulated venues share a class-level price process; isolate tests."""
    from cadb.modules.exchange.feeds import _GlobalPriceProcess

    _GlobalPriceProcess.reset()
    yield
    _GlobalPriceProcess.reset()
