"""Module 1 — Exchange Anomaly Engine.

Subscribes to L2 order books and raw trades for every (venue, symbol) pair and
publishes normalised telemetry:

* ``volume``     rolling 5-minute bucket volume + z-score (alerts when V > mu+3sigma)
* ``order_book`` dynamic OBI, weighted OBI, depth, spread, spoof/absorption walls
* ``cvd``        cumulative volume delta, divergence and absorption score
* ``price``      last trade price (context for downstream modules)

One supervised task per stream, so a Bybit outage cannot stall Binance.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from ...core.bus import EventBus
from ...core.config import ExchangeConfig
from ...core.resilience import BackoffPolicy
from ...core.schema import MarketEvent, MetricType, SourceType, monotonic_ns, now_ms
from ...core.telemetry import METRICS
from ..base import Module
from .discovery import SymbolDiscovery
from .feeds import ExchangeFeed, build_feed
from .microstructure import MicrostructureState

__all__ = ["ExchangeEngine"]


class ExchangeEngine(Module):
    """Level-2 order book + raw trade anomaly detection across venues."""

    name = "exchange"

    def __init__(self, bus: EventBus, config: ExchangeConfig) -> None:
        super().__init__(bus)
        self.config = config
        self.feeds: dict[str, ExchangeFeed] = {}
        self.states: dict[tuple[str, str], MicrostructureState] = {}
        self._obi_emit_gate: dict[tuple[str, str], int] = {}
        self.obi_emit_interval_ms = 500  # throttle steady-state book telemetry
        self.discovery: dict[str, SymbolDiscovery] = {}
        self.watched: dict[str, set[str]] = {}   # venue -> currently streamed symbols
        self._stream_tasks: dict[tuple[str, str], list[Any]] = {}

    # ---- state ---------------------------------------------------------
    def state_for(self, venue: str, symbol: str) -> MicrostructureState:
        key = (venue, symbol)
        st = self.states.get(key)
        if st is None:
            st = MicrostructureState(
                venue=venue,
                symbol=symbol,
                depth_levels=self.config.obi_depth_levels,
                volume_window_s=self.config.volume_window_s,
                volume_bucket_s=self.config.volume_bucket_s,
                volume_threshold=self.config.volume_z_threshold,
                cvd_window_s=self.config.cvd_window_s,
            )
            self.states[key] = st
        return st

    # ---- lifecycle -----------------------------------------------------
    async def run(self) -> None:
        for venue in self.config.exchanges:
            feed = build_feed(
                venue=venue,
                symbols=self.config.symbols,
                depth=self.config.orderbook_depth,
                simulate=self.config.simulate,
                prefer_ccxt=self.config.use_ccxt_pro,
            )
            self.feeds[venue] = feed
            self.watched[venue] = set()
            for symbol in self.config.symbols:
                self._subscribe(venue, symbol)

            if self.config.discovery_enabled and not self.config.simulate:
                self.discovery[venue] = SymbolDiscovery(
                    venue=venue,
                    max_symbols=self.config.discovery_max_symbols,
                    min_volume_usd=self.config.discovery_min_volume_usd,
                    max_volume_usd=self.config.discovery_max_volume_usd,
                    min_change_pct=self.config.discovery_min_change_pct,
                    volume_surge_ratio=self.config.discovery_volume_surge,
                    always_include=tuple(self.config.symbols),
                )

        self.spawn("bucket-flusher", self._flush_loop())
        if self.discovery:
            self.spawn("discovery", self._discovery_loop())
        self.log.info(
            "exchange engine watching %d venue(s) x %d symbol(s)",
            len(self.feeds), len(self.config.symbols),
        )

    def _subscribe(self, venue: str, symbol: str) -> None:
        """Start book + trade streams for one pair (idempotent)."""
        if symbol in self.watched.setdefault(venue, set()):
            return
        feed = self.feeds[venue]
        tasks = [
            self.supervise(
                f"book:{venue}:{symbol}",
                self._make_book_loop(feed, symbol),
                BackoffPolicy(initial=1.0, maximum=60.0),
            ),
            self.supervise(
                f"trades:{venue}:{symbol}",
                self._make_trade_loop(feed, symbol),
                BackoffPolicy(initial=1.0, maximum=60.0),
            ),
        ]
        self._stream_tasks[(venue, symbol)] = tasks
        self.watched[venue].add(symbol)

    async def _unsubscribe(self, venue: str, symbol: str) -> None:
        """Stop streaming a pair that no longer qualifies."""
        tasks = self._stream_tasks.pop((venue, symbol), [])
        for t in tasks:
            with contextlib.suppress(Exception):
                await t.stop()
            if t in self._tasks:
                self._tasks.remove(t)
        self.watched.get(venue, set()).discard(symbol)
        # Drop the state so a re-listed pair starts from a clean baseline
        # rather than inheriting a stale distribution.
        self.states.pop((venue, symbol), None)

    async def _discovery_loop(self) -> None:
        """Periodically re-rank each venue and adjust subscriptions."""
        # Let the pinned symbols establish themselves before adding more.
        await asyncio.sleep(10)
        while True:
            for venue, disco in list(self.discovery.items()):
                try:
                    await self._rescan(venue, disco)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log.warning("%s discovery scan failed: %s", venue, exc)
            await asyncio.sleep(self.config.discovery_interval_s)

    async def _rescan(self, venue: str, disco: SymbolDiscovery) -> None:
        feed = self.feeds.get(venue)
        client = getattr(feed, "_client", None) or (
            feed._ensure_client() if hasattr(feed, "_ensure_client") else None
        )
        if client is None or not hasattr(client, "fetch_tickers"):
            return

        tickers = await client.fetch_tickers()
        wanted = set(disco.watchlist(tickers))
        pinned = set(self.config.symbols)
        current = set(self.watched.get(venue, set()))

        for symbol in wanted - current:
            self._subscribe(venue, symbol)
            self.log.info("📡 now watching %s %s", venue, symbol)
        # Never drop a pinned symbol, regardless of how quiet it gets.
        for symbol in (current - wanted) - pinned:
            await self._unsubscribe(venue, symbol)
            self.log.info("💤 stopped watching %s %s", venue, symbol)

        METRICS.gauge(f"exchange.{venue}.watched", len(self.watched.get(venue, set())))

    def _make_book_loop(self, feed: ExchangeFeed, symbol: str) -> Any:
        async def loop() -> None:
            async for update in feed.watch_order_book(symbol):
                await self._on_book(update)
        return loop

    def _make_trade_loop(self, feed: ExchangeFeed, symbol: str) -> Any:
        async def loop() -> None:
            async for trade in feed.watch_trades(symbol):
                await self._on_trade(trade)
        return loop

    async def cleanup(self) -> None:
        for feed in self.feeds.values():
            await feed.close()
        self.feeds.clear()

    # ---- handlers ------------------------------------------------------
    async def _on_book(self, update: Any) -> None:
        t0 = monotonic_ns()
        st = self.state_for(update.venue, update.symbol)
        st.book.update(update.bids, update.asks, update.timestamp)
        obi = st.book.imbalance()
        if obi.mid_price <= 0:
            return

        key = (update.venue, update.symbol)
        gate = self._obi_emit_gate.get(key, 0)
        anomalous = abs(obi.obi) >= self.config.obi_threshold or (
            obi.z_score is not None and abs(obi.z_score) >= st.book.obi_z.threshold
        )
        # Always publish anomalies; throttle routine updates to protect the bus.
        if not anomalous and update.timestamp - gate < self.obi_emit_interval_ms:
            return
        self._obi_emit_gate[key] = update.timestamp

        walls = st.book.detect_walls(min_notional=max(50_000.0, obi.bid_depth * 0.05))
        spoofed = [w for w in walls if w.absorbed_pct < 0]
        absorbed = [w for w in walls if w.absorbed_pct > 0]

        await self.emit(
            MarketEvent(
                timestamp=update.timestamp,
                source_type=SourceType.EXCHANGE,
                venue=update.venue,
                asset_pair=update.symbol,
                metric_type=MetricType.ORDER_BOOK,
                raw_value=obi.obi,
                normalized_z_score=obi.z_score,
                usd_value=obi.bid_depth + obi.ask_depth,
                meta={
                    "weighted_obi": round(obi.weighted_obi, 5),
                    "bid_depth": round(obi.bid_depth, 2),
                    "ask_depth": round(obi.ask_depth, 2),
                    "spread_bps": round(obi.spread_bps, 3),
                    "mid_price": obi.mid_price,
                    "direction": obi.direction,
                    "levels": obi.levels,
                    "obi_percentile": round(st.book.obi_percentile, 4),
                    "spoofed_walls": len(spoofed),
                    "absorbed_walls": len(absorbed),
                    "wall_notional": round(sum(w.notional for w in walls), 2),
                    "anomalous": anomalous,
                },
            )
        )
        METRICS.observe("exchange.book_ms", (monotonic_ns() - t0) / 1e6)

    async def _on_trade(self, trade: Any) -> None:
        t0 = monotonic_ns()
        st = self.state_for(trade.venue, trade.symbol)
        bucket_closed, bucket_z = st.on_trade(
            trade.timestamp, trade.price, trade.size, trade.side  # type: ignore[arg-type]
        )

        # A closed volume bucket is the sampling point for the 5-minute z-score.
        # Publish on *closure*, not on z availability — a None z means "no
        # defensible dispersion estimate yet", but the volume observation itself
        # is still real and downstream consumers need it.
        if bucket_closed:
            exceeded = st.volume.exceeds_threshold(bucket_z)
            await self.emit(
                MarketEvent(
                    timestamp=trade.timestamp,
                    source_type=SourceType.EXCHANGE,
                    venue=trade.venue,
                    asset_pair=trade.symbol,
                    metric_type=MetricType.VOLUME,
                    raw_value=st.volume.rolling_volume,
                    normalized_z_score=bucket_z,
                    usd_value=st.volume.rolling_volume * trade.price,
                    meta={
                        "window_s": self.config.volume_window_s,
                        "bucket_s": self.config.volume_bucket_s,
                        "mean_bucket": round(st.volume.mean_bucket_volume, 8),
                        "std_bucket": round(st.volume.std_bucket_volume, 8),
                        "threshold": round(st.volume.zscore.threshold, 3),
                        "exceeded": exceeded,
                        "spike_ratio": round(st.volume.spike_ratio(), 3),
                        "trades": st.volume.total_trades,
                        "price": trade.price,
                    },
                )
            )
            if exceeded:
                METRICS.incr("exchange.volume_spikes")
                self.log.info(
                    "volume spike %s %s z=%.2f (thr %.2f)",
                    trade.venue, trade.symbol, bucket_z, st.volume.zscore.threshold,
                )

            # CVD is published on the same cadence as the volume bucket.
            divergence = st.cvd.divergence()
            absorption = st.cvd.absorption_score()
            await self.emit(
                MarketEvent(
                    timestamp=trade.timestamp,
                    source_type=SourceType.EXCHANGE,
                    venue=trade.venue,
                    asset_pair=trade.symbol,
                    metric_type=MetricType.CVD,
                    raw_value=st.cvd.cvd,
                    normalized_z_score=st.cvd.zscore.last_z,
                    meta={
                        "window_delta": round(st.cvd.window_delta, 2),
                        "buy_ratio": round(st.cvd.buy_ratio, 4),
                        "divergence": round(divergence, 4),
                        "absorption": round(absorption, 4),
                        "price": trade.price,
                        "aggressive_side": "buy" if st.cvd.buy_ratio > 0.5 else "sell",
                    },
                )
            )
            if absorption > 0.5:
                METRICS.incr("exchange.absorption_events")

        METRICS.observe("exchange.trade_ms", (monotonic_ns() - t0) / 1e6)

    async def _flush_loop(self) -> None:
        """Close stale volume buckets so quiet symbols still produce samples."""
        interval = max(self.config.volume_bucket_s, 1)
        while True:
            await asyncio.sleep(interval)
            ts = now_ms()
            for st in list(self.states.values()):
                st.volume.force_close(ts)
                st.volume.buckets.expire(ts)

    # ---- introspection --------------------------------------------------
    def snapshot(self, venue: str, symbol: str) -> dict[str, float] | None:
        st = self.states.get((venue, symbol))
        return st.snapshot() if st else None

    def health(self) -> dict[str, Any]:
        base = super().health()
        base["venues"] = {
            v: {"connected": f.connected, "messages": f.messages} for v, f in self.feeds.items()
        }
        base["tracked_pairs"] = len(self.states)
        base["watched"] = {v: sorted(syms) for v, syms in self.watched.items()}
        base["discovery"] = {v: d.stats() for v, d in self.discovery.items()}
        return base
