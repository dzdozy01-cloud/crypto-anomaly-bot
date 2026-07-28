"""Module 4 — ML Anomaly Scoring service.

Subscribes to every telemetry channel, maintains per-asset feature vectors, and
scores them on a fixed cadence. Emits :class:`AnomalySignal` objects to the
alert router and republishes them on the bus as ``manipulation_score`` events.

Scoring is cadence-driven rather than event-driven on purpose: at 1000+ ticks/s
per-event scoring would burn CPU re-evaluating near-identical vectors. A 250 ms
cadence keeps end-to-end latency well inside the 200 ms *processing* budget
while bounding CPU. Events that are individually extreme trigger an immediate
out-of-band score so nothing urgent waits for the next cycle.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ...core.bus import EventBus
from ...core.config import MLConfig
from ...core.schema import AnomalySignal, MarketEvent, MetricType, monotonic_ns, now_ms
from ...core.telemetry import METRICS
from ..base import Module
from .classifier import ManipulationClassifier
from .features import FeatureStore
from .training import generate_training_data

__all__ = ["MLScorer"]

SignalHandler = Callable[[AnomalySignal], Awaitable[None]]


class MLScorer(Module):
    """Feature aggregation + Isolation Forest scoring + signal emission."""

    name = "ml"

    def __init__(self, bus: EventBus, config: MLConfig) -> None:
        super().__init__(bus)
        self.config = config
        self.store = FeatureStore(ttl_s=config.feature_ttl_s)
        self.classifier = ManipulationClassifier(
            contamination=config.contamination,
            n_estimators=config.n_estimators,
            max_samples=config.max_samples,
            random_state=config.random_state,
            min_training_samples=config.min_training_samples,
            ml_blend=config.ml_blend,
            weights=config.weights,
            alert_threshold=config.alert_threshold,
        )
        self.handlers: list[SignalHandler] = []
        self.signals_emitted = 0
        self.alerts_fired = 0
        self.last_scores: dict[str, float] = {}
        self._urgent: set[str] = set()
        self._lock = asyncio.Lock()
        self._yield_every = 8  # cooperative yield cadence inside a scoring cycle
        self._last_alert_log: dict[str, float] = {}
        self._alert_log_interval_s = 60.0

    def add_handler(self, handler: SignalHandler) -> None:
        """Register a downstream consumer of high-scoring signals."""
        self.handlers.append(handler)

    # ---- lifecycle -------------------------------------------------------
    async def run(self) -> None:
        loaded = self.classifier.load(self.config.model_path)
        if not loaded:
            self.log.info("no saved model; bootstrapping on synthetic corpus")
            data = await asyncio.get_running_loop().run_in_executor(
                None, generate_training_data, 5000, 0.02, self.config.random_state
            )
            await asyncio.get_running_loop().run_in_executor(None, self.classifier.fit, data)
            with contextlib.suppress(Exception):
                self.classifier.save(self.config.model_path)

        self.bus.add_handler(
            self._on_event,
            "cadb.exchange.*",
            "cadb.onchain.*",
            "cadb.social.*",
            name="ml-ingest",
        )
        self.spawn("scoring-loop", self._score_loop())
        if self.config.online_training:
            self.spawn("retrain-loop", self._retrain_loop())
        self.log.info(
            "ML scorer online (%s, threshold=%.0f)",
            "trained" if self.classifier.is_trained else "rules-only",
            self.config.alert_threshold,
        )

    # ---- ingestion -------------------------------------------------------
    async def _on_event(self, event: MarketEvent) -> None:
        if event.metric_type is MetricType.MANIPULATION_SCORE:
            return  # never feed our own output back in
        asset = self.store.ingest(event)
        if self._is_urgent(event):
            self._urgent.add(asset)

    @staticmethod
    def _is_urgent(event: MarketEvent) -> bool:
        """Events extreme enough to bypass the scoring cadence."""
        z = event.normalized_z_score
        if z is not None and abs(z) >= 4.0:
            return True
        if event.metric_type is MetricType.LIQUIDITY and abs(event.raw_value) >= 30:
            return True
        if event.metric_type is MetricType.BOT_FARM and event.raw_value >= 0.6:
            return True
        return bool(
            event.metric_type is MetricType.WALLET_TRANSFER
            and (event.usd_value or 0) >= 5_000_000
        )

    # ---- scoring ---------------------------------------------------------
    async def _score_loop(self) -> None:
        interval = max(self.config.score_interval_ms / 1000.0, 0.05)
        decay_every = max(1, int(5.0 / interval))
        cycles = 0
        while True:
            await asyncio.sleep(interval)
            cycles += 1
            try:
                await self._score_cycle()
            except Exception:
                self.log.exception("scoring cycle failed")
            if cycles % decay_every == 0:
                self.store.decay(0.85)
            if cycles % (decay_every * 60) == 0:
                pruned = self.store.prune()
                if pruned:
                    self.log.debug("pruned %d idle assets", pruned)

    async def _score_cycle(self) -> None:
        async with self._lock:
            urgent, self._urgent = self._urgent, set()
        t0 = monotonic_ns()
        now = now_ms()
        vectors = self.store.build_all(now)
        if not vectors:
            return

        # Yield to the event loop periodically. Discovery can push the tracked
        # universe into the hundreds, and scoring every asset synchronously —
        # each with an IsolationForest call — starved the WebSocket readers and
        # pushed cycle p95 past the 200ms budget. Scoring is cooperative now, so
        # cycle duration grows with the universe but tick handling does not stall.
        # One forest call for the whole sweep instead of one per asset.
        live = [fv for fv in vectors if fv.is_informative]
        ml_scores = (
            self.classifier.ml_scores_batch([fv.values for fv in live])
            if self.classifier.is_trained else [0.0] * len(live)
        )

        scored = 0
        for i, (fv, ml) in enumerate(zip(live, ml_scores)):
            self.classifier.observe(fv)
            signal = self.classifier.classify(fv, ml_score=ml)
            self.last_scores[fv.asset] = signal.score
            METRICS.gauge(f"ml.score.{fv.asset}", round(signal.score, 2))
            scored += 1

            if signal.score >= 40 or fv.asset in urgent:
                await self._dispatch(signal, t0)

            if (i + 1) % self._yield_every == 0:
                await asyncio.sleep(0)

        METRICS.observe("ml.cycle_ms", (monotonic_ns() - t0) / 1e6)
        METRICS.gauge("ml.assets_tracked", len(vectors))
        METRICS.gauge("ml.assets_scored", scored)

    async def _dispatch(self, signal: AnomalySignal, t0: int) -> None:
        latency = (monotonic_ns() - t0) / 1e6
        signal = signal.model_copy(update={"latency_ms": round(latency, 3)})
        self.signals_emitted += 1
        await self.emit(signal.to_event())

        if signal.score >= self.config.alert_threshold:
            self.alerts_fired += 1
            METRICS.incr("ml.alerts")
            # Log at WARNING only when this is a *new* episode for the asset.
            # Scoring runs every 250ms, so a single sustained event logged ~60
            # near-identical WARNING lines in 30s, which reads as an alert storm
            # even though the router correctly dispatched once. Subsequent
            # scores during the same episode go to DEBUG.
            last = self._last_alert_log.get(signal.asset_pair, 0.0)
            now = time.monotonic()
            if now - last >= self._alert_log_interval_s:
                self._last_alert_log[signal.asset_pair] = now
                self.log.warning(
                    "🚨 MANIPULATION %s score=%.1f severity=%s | %s",
                    signal.asset_pair, signal.score, signal.severity.value,
                    "; ".join(signal.reasons[:2]),
                )
            else:
                self.log.debug(
                    "manipulation %s score=%.1f (ongoing)",
                    signal.asset_pair, signal.score,
                )
            for handler in self.handlers:
                try:
                    await handler(signal)
                except Exception:
                    self.log.exception("signal handler failed")

    # ---- online training --------------------------------------------------
    async def _retrain_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.retrain_interval_s)
            if not self.classifier.can_train:
                self.log.debug(
                    "retrain skipped: %d/%d samples",
                    len(self.classifier.buffer), self.config.min_training_samples,
                )
                continue
            self.log.info("retraining on %d live samples", len(self.classifier.buffer))
            ok = await asyncio.get_running_loop().run_in_executor(None, self.classifier.fit, None)
            if ok:
                with contextlib.suppress(Exception):
                    self.classifier.save(self.config.model_path)
                METRICS.incr("ml.retrains")

    # ---- manual API -------------------------------------------------------
    def score_asset(self, asset: str) -> AnomalySignal | None:
        """Synchronously score one asset (used by the CLI / API)."""
        fv = self.store.build(asset.upper())
        return self.classifier.classify(fv) if fv else None

    def top_scores(self, n: int = 10) -> list[tuple[str, float]]:
        return sorted(self.last_scores.items(), key=lambda kv: -kv[1])[:n]

    def health(self) -> dict[str, Any]:
        base = super().health()
        base["classifier"] = self.classifier.info()
        base["assets"] = len(self.store.assets)
        base["signals"] = self.signals_emitted
        base["alerts"] = self.alerts_fired
        base["top_scores"] = [(a, round(s, 1)) for a, s in self.top_scores(5)]
        return base
