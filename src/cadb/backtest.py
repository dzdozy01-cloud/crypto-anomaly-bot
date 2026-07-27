"""Replay recorded telemetry through the scoring pipeline.

Feed it a JSONL file of :class:`MarketEvent` payloads (the format written by
``EventRecorder``) and it reconstructs feature vectors in timestamp order,
scores them, and reports what would have alerted. Useful for threshold tuning
and post-incident analysis without touching live feeds.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .core.schema import MarketEvent
from .core.telemetry import setup_logging
from .modules.ml.classifier import ManipulationClassifier
from .modules.ml.features import FeatureStore
from .modules.ml.training import generate_training_data

__all__ = ["run_backtest", "EventRecorder"]


class EventRecorder:
    """Append-only JSONL recorder — attach to the bus to capture a session."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")
        self.count = 0

    async def __call__(self, event: MarketEvent) -> None:
        self._fh.write(json.dumps(event.to_wire(), default=str) + "\n")
        self.count += 1
        if self.count % 500 == 0:
            self._fh.flush()

    def close(self) -> None:
        self._fh.flush()
        self._fh.close()


async def run_backtest(
    events_path: str, threshold: float = 80.0, speed: float = 0.0, model_path: str = ""
) -> int:
    """Replay ``events_path`` and report the alerts it would have produced."""
    setup_logging("WARNING")
    path = Path(events_path)
    if not path.exists():
        print(f"error: {path} not found")
        return 1

    events: list[MarketEvent] = []
    bad = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            events.append(MarketEvent.from_wire(json.loads(line)))
        except Exception:
            bad += 1
    if not events:
        print("error: no valid events found")
        return 1
    events.sort(key=lambda e: e.timestamp)

    store = FeatureStore()
    clf = ManipulationClassifier(alert_threshold=threshold)
    if model_path and Path(model_path).exists():
        clf.load(model_path)
    else:
        clf.fit(generate_training_data(5000, 0.02, 42))

    span_s = (events[-1].timestamp - events[0].timestamp) / 1000.0
    print(f"\nReplaying {len(events):,} events over {span_s / 60:.1f} minutes"
          + (f" ({bad} malformed lines skipped)" if bad else ""))

    alerts: list[Any] = []
    peak: dict[str, float] = defaultdict(float)
    score_every_ms = 1000
    next_score = events[0].timestamp

    for event in events:
        store.ingest(event)
        if event.timestamp >= next_score:
            next_score = event.timestamp + score_every_ms
            for fv in store.build_all(event.timestamp):
                if not fv.is_informative:
                    continue
                clf.observe(fv)
                signal = clf.classify(fv)
                peak[fv.asset] = max(peak[fv.asset], signal.score)
                if signal.score >= threshold:
                    alerts.append(signal)
            store.decay(0.9)

    print(f"\n{'=' * 64}\n  BACKTEST RESULTS (threshold {threshold:.0f})\n{'=' * 64}")
    print(f"\nAlerts: {len(alerts)}")
    if peak:
        print("\nPeak score by asset:")
        for asset, score in sorted(peak.items(), key=lambda kv: -kv[1])[:15]:
            bar = "█" * int(score / 5) + "░" * (20 - int(score / 5))
            flag = " 🚨" if score >= threshold else ""
            print(f"  {asset:<10} {score:>5.1f} {bar}{flag}")

    if alerts:
        print(f"\nFirst {min(5, len(alerts))} alerts:")
        for sig in alerts[:5]:
            print(f"\n  {sig.asset_pair} @ {sig.score:.1f} ({sig.severity.value})")
            for reason in sig.reasons[:3]:
                print(f"    • {reason}")
    print()
    return 0
