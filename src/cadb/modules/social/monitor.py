"""Module 3 — NLP & Social Sentiment Monitor.

Consumes X + Telegram streams, batches posts through FinBERT (or the lexicon
fallback), and publishes three metric families per tracked ticker:

* ``social_mentions``   mention rate, z-score and volume *acceleration*
* ``social_sentiment``  aggregate sentiment (-1..+1) and its momentum
* ``bot_farm``          coordinated-inauthentic-behaviour verdict

Batching is what makes FinBERT viable: posts are accumulated for up to
``flush_interval`` seconds or ``batch_size`` items, whichever comes first, then
scored in a single forward pass on a worker thread.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from ...core.bus import EventBus
from ...core.config import SocialConfig
from ...core.resilience import BackoffPolicy
from ...core.schema import MarketEvent, MetricType, SourceType, now_ms
from ...core.stats import DynamicZScore, RollingWindow, clamp
from ...core.telemetry import METRICS
from ..base import Module
from .botfarm import BotFarmDetector, SocialPost
from .sentiment import SentimentScorer, build_scorer
from .sources import SimulatedSocialSource, SocialSource, TelegramSource, XSource

__all__ = ["SocialMonitor", "TickerState"]


@dataclass
class TickerState:
    """Per-ticker rolling social state."""

    ticker: str
    window_s: int = 300

    mentions: RollingWindow = field(init=False, repr=False)
    mention_z: DynamicZScore = field(init=False, repr=False)
    sentiment_window: RollingWindow = field(init=False, repr=False)
    sentiment_z: DynamicZScore = field(init=False, repr=False)
    botfarm: BotFarmDetector = field(init=False, repr=False)

    _rate_history: deque[tuple[int, float]] = field(
        default_factory=lambda: deque(maxlen=120), repr=False
    )
    total_posts: int = 0
    last_emit_ms: int = 0

    def __post_init__(self) -> None:
        self.mentions = RollingWindow(window_ms=self.window_s * 1000)
        self.mention_z = DynamicZScore(
            half_life_s=self.window_s / 2, window=240, warmup=15, base_threshold=3.0
        )
        self.sentiment_window = RollingWindow(window_ms=self.window_s * 1000)
        self.sentiment_z = DynamicZScore(
            half_life_s=self.window_s, window=240, warmup=15, base_threshold=2.5
        )

    def bind_detector(self, detector: BotFarmDetector) -> None:
        self.botfarm = detector

    def add_post(self, post: SocialPost) -> None:
        self.total_posts += 1
        self.mentions.add(post.timestamp, 1.0)
        self.sentiment_window.add(post.timestamp, post.sentiment)
        self.botfarm.add(post)

    @property
    def mention_rate(self) -> float:
        """Mentions per minute over the window."""
        return self.mentions.rate_per_minute()

    @property
    def avg_sentiment(self) -> float:
        return self.sentiment_window.mean

    def acceleration(self) -> float:
        """d(rate)/dt in mentions per minute squared — the 'volume acceleration'.

        A pump's tell is not a high mention rate but a rapidly *increasing* one;
        organic interest ramps smoothly, coordinated pushes step-change.
        """
        if len(self._rate_history) < 3:
            return 0.0
        recent = list(self._rate_history)[-6:]
        (t0, r0), (t1, r1) = recent[0], recent[-1]
        dt_min = (t1 - t0) / 60_000.0
        if dt_min <= 1e-6:
            return 0.0
        return (r1 - r0) / dt_min

    def tick(self, ts: int) -> tuple[float, float | None]:
        """Sample the rate series; returns (rate, z-score of the rate)."""
        self.mentions.expire(ts)
        self.sentiment_window.expire(ts)
        rate = self.mention_rate
        self._rate_history.append((ts, rate))
        z = self.mention_z.update(rate, ts)
        return rate, z

    def snapshot(self) -> dict[str, float]:
        return {
            "mention_rate": self.mention_rate,
            "mention_z": self.mention_z.last_z or 0.0,
            "acceleration": self.acceleration(),
            "sentiment": self.avg_sentiment,
            "sentiment_z": self.sentiment_z.last_z or 0.0,
            "posts": float(self.total_posts),
        }


class SocialMonitor(Module):
    """X + Telegram ingestion, FinBERT scoring, bot-farm detection."""

    name = "social"

    def __init__(self, bus: EventBus, config: SocialConfig) -> None:
        super().__init__(bus)
        self.config = config
        self.tickers = [t.upper() for t in config.tracked_tickers]
        self.scorer: SentimentScorer | None = None
        self.sources: list[SocialSource] = []
        self.states: dict[str, TickerState] = {}
        self._batch: list[SocialPost] = []
        self._batch_lock = asyncio.Lock()
        self.enabled_sources = True
        self.flush_interval_s = 2.0
        self.emit_interval_s = 5.0

    def state_for(self, ticker: str) -> TickerState:
        st = self.states.get(ticker)
        if st is None:
            st = TickerState(ticker=ticker, window_s=self.config.mention_window_s)
            st.bind_detector(
                BotFarmDetector(
                    window_s=max(self.config.mention_window_s, 600),
                    min_posts=self.config.bot_farm_min_posts,
                    max_age_days=self.config.bot_account_age_days,
                    age_cv_threshold=self.config.bot_age_variance_threshold,
                )
            )
            st.mention_z.base_threshold = self.config.mention_z_threshold
            self.states[ticker] = st
        return st

    # ---- lifecycle -------------------------------------------------------
    async def run(self) -> None:
        # Model loading is blocking; keep it off the event loop.
        self.scorer = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: build_scorer(
                use_finbert=self.config.use_finbert and not self.config.simulate,
                model_name=self.config.finbert_model,
                batch_size=self.config.sentiment_batch_size,
            ),
        )
        self.log.info("sentiment backend: %s", self.scorer.backend)

        if self.config.simulate:
            self.sources.append(SimulatedSocialSource(self.tickers))
        else:
            if self.config.x_bearer_token:
                self.sources.append(
                    XSource(
                        self.tickers,
                        self.config.x_bearer_token,
                        poll_interval_s=self.config.poll_interval_s,
                    )
                )
            if self.config.telegram_api_id and self.config.telegram_channels:
                self.sources.append(
                    TelegramSource(
                        self.tickers,
                        self.config.telegram_api_id,
                        self.config.telegram_api_hash,
                        self.config.telegram_channels,
                    )
                )
            if not self.sources:
                # NEVER silently substitute synthetic data in a live run. The
                # simulator injects fake shill campaigns, which flow into the
                # feature vector and produce real alerts about manipulation
                # that never happened. Fabricated intelligence is strictly
                # worse than an absent module: it is indistinguishable from a
                # true positive and destroys trust in every other alert.
                self.log.error(
                    "SOCIAL MODULE DISABLED — no credentials configured. "
                    "Set X_BEARER_TOKEN (and/or TELEGRAM_API_ID + telegram_channels) "
                    "to enable it. Refusing to run a simulated feed in live mode; "
                    "use `simulate: true` explicitly if you want synthetic data."
                )
                self.enabled_sources = False
                return

        for src in self.sources:
            self.supervise(
                f"source:{src.platform}",
                self._make_source_loop(src),
                BackoffPolicy(initial=5.0, maximum=300.0),
            )
        self.spawn("flusher", self._flush_loop())
        self.spawn("emitter", self._emit_loop())
        self.log.info(
            "social monitor tracking %d ticker(s) across %d source(s)",
            len(self.tickers), len(self.sources),
        )

    def _make_source_loop(self, src: SocialSource) -> Any:
        async def loop() -> None:
            async for post in src.stream():
                await self._enqueue(post)
            # A finished generator means the source is disabled; idle politely.
            await asyncio.sleep(3600)
        return loop

    async def cleanup(self) -> None:
        for src in self.sources:
            with contextlib.suppress(Exception):
                await src.close()

    # ---- pipeline --------------------------------------------------------
    async def _enqueue(self, post: SocialPost) -> None:
        async with self._batch_lock:
            self._batch.append(post)
            ready = len(self._batch) >= self.config.sentiment_batch_size
        if ready:
            await self._flush()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_s)
            await self._flush()

    async def _flush(self) -> None:
        async with self._batch_lock:
            if not self._batch:
                return
            batch, self._batch = self._batch, []

        assert self.scorer is not None
        results = await self.scorer.score_batch([p.text for p in batch])
        for post, res in zip(batch, results):
            post.sentiment = res.score
            for ticker in post.tickers or set():
                if ticker in self.tickers:
                    self.state_for(ticker).add_post(post)
        METRICS.incr("social.posts_scored", len(batch))

    async def _emit_loop(self) -> None:
        """Publish aggregated social telemetry on a fixed cadence."""
        while True:
            await asyncio.sleep(self.emit_interval_s)
            ts = now_ms()
            for ticker, st in list(self.states.items()):
                rate, z = st.tick(ts)
                accel = st.acceleration()
                sentiment = st.avg_sentiment
                sent_z = st.sentiment_z.update(sentiment, ts)

                await self.emit(
                    MarketEvent(
                        timestamp=ts,
                        source_type=SourceType.SOCIAL,
                        venue="aggregate",
                        asset_pair=ticker,
                        metric_type=MetricType.SOCIAL_MENTIONS,
                        raw_value=rate,
                        normalized_z_score=z,
                        meta={
                            "acceleration": round(accel, 4),
                            "window_s": self.config.mention_window_s,
                            "posts_total": st.total_posts,
                            "posts_in_window": len(st.mentions),
                            "threshold": round(st.mention_z.threshold, 3),
                            "spike": bool(z is not None and z > st.mention_z.threshold),
                            "platforms": [s.platform for s in self.sources],
                        },
                    )
                )

                await self.emit(
                    MarketEvent(
                        timestamp=ts,
                        source_type=SourceType.SOCIAL,
                        venue="aggregate",
                        asset_pair=ticker,
                        metric_type=MetricType.SOCIAL_SENTIMENT,
                        raw_value=sentiment,
                        normalized_z_score=sent_z,
                        confidence=clamp(len(st.mentions) / 30.0, 0.1, 1.0),
                        meta={
                            "backend": self.scorer.backend if self.scorer else "none",
                            "samples": len(st.sentiment_window),
                            "label": (
                                "bullish" if sentiment > 0.15
                                else "bearish" if sentiment < -0.15
                                else "neutral"
                            ),
                        },
                    )
                )

                verdict = st.botfarm.evaluate(mention_z=z)
                if verdict.posts_considered >= self.config.bot_farm_min_posts:
                    await self.emit(
                        MarketEvent(
                            timestamp=ts,
                            source_type=SourceType.SOCIAL,
                            venue="aggregate",
                            asset_pair=ticker,
                            metric_type=MetricType.BOT_FARM,
                            raw_value=verdict.score,
                            normalized_z_score=z,
                            confidence=clamp(verdict.posts_considered / 50.0, 0.2, 1.0),
                            meta=verdict.as_dict(),
                        )
                    )
                    if verdict.is_bot_farm:
                        METRICS.incr("social.bot_farms_detected")
                        self.log.warning(
                            "bot farm on %s score=%.2f (%s)",
                            ticker, verdict.score, "; ".join(verdict.reasons[:3]),
                        )

    # ---- introspection ---------------------------------------------------
    def snapshot(self, ticker: str) -> dict[str, float] | None:
        st = self.states.get(ticker.upper())
        return st.snapshot() if st else None

    def health(self) -> dict[str, Any]:
        base = super().health()
        if not self.enabled_sources:
            base["healthy"] = False
            base["error"] = "no social credentials configured — module inert"
        base["backend"] = self.scorer.backend if self.scorer else "loading"
        base["sources"] = [
            {"platform": s.platform, "enabled": s.enabled, "posts": s.posts_seen}
            for s in self.sources
        ]
        base["tickers"] = {t: st.total_posts for t, st in self.states.items()}
        return base
