"""Structured logging + in-process metrics registry.

Latency is tracked as a streaming histogram so p50/p95/p99 are available without
retaining every sample — the 200 ms budget from the spec is asserted against p95.
"""

from __future__ import annotations

import json
import logging
import math
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

__all__ = ["setup_logging", "Metrics", "METRICS", "LatencyTracker"]


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, val in getattr(record, "extra_fields", {}).items():
            payload[key] = val
        return json.dumps(payload, default=str)


class _ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[1;41m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        name = record.name.replace("cadb.", "")
        return (
            f"\033[90m{ts}\033[0m {color}{record.levelname:<8}{self.RESET} "
            f"\033[90m{name:<26}\033[0m {record.getMessage()}"
            + (f"\n{self.formatException(record.exc_info)}" if record.exc_info else "")
        )


def setup_logging(level: str = "INFO", json_logs: bool = False) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter() if json_logs else _ColorFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for noisy in ("websockets", "urllib3", "asyncio", "ccxt", "web3", "aiohttp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@dataclass
class LatencyTracker:
    """Fixed-window latency samples with percentile queries."""

    name: str
    maxlen: int = 2048
    _samples: deque[float] = field(default_factory=lambda: deque(maxlen=2048), repr=False)
    breaches: int = 0
    budget_ms: float = 200.0

    def observe(self, ms: float) -> None:
        self._samples.append(ms)
        if ms > self.budget_ms:
            self.breaches += 1

    def percentile(self, q: float) -> float:
        if not self._samples:
            return 0.0
        ordered = sorted(self._samples)
        idx = min(len(ordered) - 1, max(0, int(math.ceil(q * len(ordered)) - 1)))
        return ordered[idx]

    def summary(self) -> dict[str, float]:
        if not self._samples:
            return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "breaches": 0}
        return {
            "count": len(self._samples),
            "p50": round(self.percentile(0.50), 2),
            "p95": round(self.percentile(0.95), 2),
            "p99": round(self.percentile(0.99), 2),
            "max": round(max(self._samples), 2),
            "breaches": self.breaches,
        }


class Metrics:
    """Minimal counter/gauge/histogram registry (Prometheus-exposable)."""

    def __init__(self) -> None:
        self.counters: dict[str, float] = defaultdict(float)
        self.gauges: dict[str, float] = {}
        self.latencies: dict[str, LatencyTracker] = {}
        self.started = time.monotonic()

    def incr(self, name: str, value: float = 1.0) -> None:
        self.counters[name] += value

    def gauge(self, name: str, value: float) -> None:
        self.gauges[name] = value

    def latency(self, name: str, budget_ms: float = 200.0) -> LatencyTracker:
        tracker = self.latencies.get(name)
        if tracker is None:
            tracker = LatencyTracker(name=name, budget_ms=budget_ms)
            self.latencies[name] = tracker
        return tracker

    def observe(self, name: str, ms: float, budget_ms: float = 200.0) -> None:
        self.latency(name, budget_ms).observe(ms)

    @property
    def uptime_s(self) -> float:
        return time.monotonic() - self.started

    def snapshot(self) -> dict[str, Any]:
        return {
            "uptime_s": round(self.uptime_s, 1),
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "latency": {k: v.summary() for k, v in self.latencies.items()},
        }

    def prometheus(self) -> str:
        lines: list[str] = []
        for k, v in self.counters.items():
            metric = k.replace(".", "_").replace("-", "_")
            lines.append(f"# TYPE cadb_{metric} counter")
            lines.append(f"cadb_{metric} {v}")
        for k, v in self.gauges.items():
            metric = k.replace(".", "_").replace("-", "_")
            lines.append(f"# TYPE cadb_{metric} gauge")
            lines.append(f"cadb_{metric} {v}")
        for k, tr in self.latencies.items():
            metric = k.replace(".", "_").replace("-", "_")
            s = tr.summary()
            lines.append(f"# TYPE cadb_{metric}_ms summary")
            for q in ("p50", "p95", "p99"):
                lines.append(f'cadb_{metric}_ms{{quantile="{q}"}} {s[q]}')
            lines.append(f"cadb_{metric}_ms_count {s['count']}")
        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        self.counters.clear()
        self.gauges.clear()
        self.latencies.clear()


METRICS = Metrics()
