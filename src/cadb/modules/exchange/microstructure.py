"""Order-book / trade-flow microstructure analytics (pure, testable core).

Deliberately free of I/O: feeds in, metrics out. The WebSocket layer only shovels
normalised snapshots into these classes, which makes the maths unit-testable and
keeps per-tick cost predictable.

Implements the three exchange metrics from the spec:

* rolling 5-minute **Volume Z-Score** (fires when ``V > mu + 3*sigma``)
* dynamic **Order Book Imbalance** ``(bid_depth - ask_depth)/(bid_depth + ask_depth)``
* **Cumulative Volume Delta** with passive-wall absorption detection
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Literal

from ...core.stats import CusumDetector, DynamicZScore, RollingWindow, clamp

Side = Literal["buy", "sell"]

__all__ = [
    "OrderBookState",
    "VolumeProfile",
    "CVDTracker",
    "MicrostructureState",
    "OBIResult",
    "WallEvent",
]


@dataclass
class OBIResult:
    """Result of an order-book imbalance computation."""

    obi: float                    # (bid - ask) / (bid + ask), in [-1, 1]
    bid_depth: float              # quote-currency notional on the bid side
    ask_depth: float
    spread_bps: float
    mid_price: float
    weighted_obi: float           # distance-weighted (near-touch liquidity matters more)
    z_score: float | None = None
    levels: int = 0

    @property
    def direction(self) -> str:
        if self.obi > 0.05:
            return "bid_heavy"
        if self.obi < -0.05:
            return "ask_heavy"
        return "balanced"


@dataclass
class WallEvent:
    """A passive limit wall being consumed by aggressive market orders."""

    price: float
    side: Side                 # side of the *wall* (bid wall eaten by sells)
    size_removed: float
    notional: float
    absorbed_pct: float


@dataclass
class OrderBookState:
    """Maintains an L2 book and derives imbalance metrics.

    Depth is measured in *quote notional* (price × size) rather than raw base
    size, because a 1 BTC wall and a 1 000 000 PEPE wall are not comparable
    otherwise. Weighted OBI additionally discounts levels by their distance from
    the mid so that spoofed far-touch liquidity cannot dominate the signal.
    """

    symbol: str
    venue: str
    depth_levels: int = 20
    decay_bps: float = 25.0  # weighting half-distance in basis points

    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    last_update_ms: int = 0
    obi_z: DynamicZScore = field(default_factory=lambda: DynamicZScore(half_life_s=120, warmup=20))
    _prev_bids: dict[float, float] = field(default_factory=dict, repr=False)
    _prev_asks: dict[float, float] = field(default_factory=dict, repr=False)
    _obi_history: deque[float] = field(default_factory=lambda: deque(maxlen=600), repr=False)

    # ---- ingestion ----------------------------------------------------
    def update(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        timestamp_ms: int,
    ) -> None:
        """Replace the book with a fresh (already sorted) snapshot."""
        self._prev_bids = dict(self.bids[: self.depth_levels])
        self._prev_asks = dict(self.asks[: self.depth_levels])
        self.bids = sorted((b for b in bids if b[1] > 0), key=lambda x: -x[0])
        self.asks = sorted((a for a in asks if a[1] > 0), key=lambda x: x[0])
        self.last_update_ms = timestamp_ms

    # ---- derived quantities -------------------------------------------
    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        if self.bids and self.asks:
            return (self.best_bid + self.best_ask) / 2.0
        return self.best_bid or self.best_ask

    @property
    def spread_bps(self) -> float:
        mid = self.mid_price
        if not (self.bids and self.asks and mid > 0):
            return 0.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    def _weight(self, price: float, mid: float) -> float:
        """Exponential distance decay: near-touch liquidity is what can actually fill."""
        if mid <= 0:
            return 1.0
        dist_bps = abs(price - mid) / mid * 10_000.0
        return math.exp(-dist_bps / max(self.decay_bps, 1e-6))

    def imbalance(self, levels: int | None = None) -> OBIResult:
        """Compute OBI = (bid_depth - ask_depth) / (bid_depth + ask_depth)."""
        n = levels or self.depth_levels
        mid = self.mid_price
        bid_slice = self.bids[:n]
        ask_slice = self.asks[:n]

        bid_depth = sum(p * s for p, s in bid_slice)
        ask_depth = sum(p * s for p, s in ask_slice)
        total = bid_depth + ask_depth
        obi = (bid_depth - ask_depth) / total if total > 0 else 0.0

        w_bid = sum(p * s * self._weight(p, mid) for p, s in bid_slice)
        w_ask = sum(p * s * self._weight(p, mid) for p, s in ask_slice)
        w_total = w_bid + w_ask
        weighted = (w_bid - w_ask) / w_total if w_total > 0 else 0.0

        self._obi_history.append(obi)
        z = self.obi_z.update(obi, self.last_update_ms)

        return OBIResult(
            obi=obi,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            spread_bps=self.spread_bps,
            mid_price=mid,
            weighted_obi=weighted,
            z_score=z,
            levels=min(n, max(len(bid_slice), len(ask_slice))),
        )

    def detect_walls(self, min_notional: float = 100_000.0) -> list[WallEvent]:
        """Detect large passive levels that were consumed since the last snapshot.

        A wall that *disappears* while the price trades through it is absorption
        (real aggression). A wall that disappears while the price moves *away* is
        a cancellation — classic spoofing — and is reported with a negative
        absorbed_pct so the classifier can tell the two apart.
        """
        events: list[WallEvent] = []
        for side, prev, current in (
            ("buy", self._prev_bids, dict(self.bids[: self.depth_levels])),
            ("sell", self._prev_asks, dict(self.asks[: self.depth_levels])),
        ):
            for price, prev_size in prev.items():
                notional = price * prev_size
                if notional < min_notional:
                    continue
                new_size = current.get(price, 0.0)
                removed = prev_size - new_size
                if removed <= 0:
                    continue
                pct = removed / prev_size if prev_size > 0 else 0.0
                if pct < 0.5:
                    continue
                traded_through = (
                    price >= self.best_bid if side == "buy" else price <= self.best_ask
                )
                events.append(
                    WallEvent(
                        price=price,
                        side=side,  # type: ignore[arg-type]
                        size_removed=removed,
                        notional=price * removed,
                        absorbed_pct=pct if traded_through else -pct,
                    )
                )
        return events

    @property
    def obi_percentile(self) -> float:
        """Where the current OBI sits within its own recent distribution."""
        if len(self._obi_history) < 30:
            return 0.5
        current = self._obi_history[-1]
        below = sum(1 for v in self._obi_history if v <= current)
        return below / len(self._obi_history)


@dataclass
class VolumeProfile:
    """Rolling 5-minute traded-volume profile with bucketed z-scoring.

    Raw per-trade volume is far too noisy for a meaningful z-score, so trades are
    accumulated into fixed buckets (default 5 s) and the *bucket totals* form the
    distribution. That is the standard way to express "V > mu + 3sigma" for a
    rolling 5-minute window.
    """

    symbol: str
    venue: str
    window_s: int = 300
    bucket_s: int = 5
    threshold: float = 3.0

    _current_bucket_start: int = 0
    _current_bucket_volume: float = 0.0
    _current_bucket_notional: float = 0.0
    buckets: RollingWindow = field(init=False, repr=False)
    zscore: DynamicZScore = field(init=False, repr=False)
    cusum: CusumDetector = field(default_factory=CusumDetector, repr=False)
    total_trades: int = 0

    def __post_init__(self) -> None:
        self.buckets = RollingWindow(window_ms=self.window_s * 1000)
        self.zscore = DynamicZScore(
            half_life_s=self.window_s / 4,
            window=max(60, self.window_s // max(self.bucket_s, 1)),
            warmup=max(10, (self.window_s // max(self.bucket_s, 1)) // 4),
            base_threshold=self.threshold,
            # Empty buckets are normal for any symbol that is not trading every
            # second — without this, the first trade after a quiet spell scored
            # 50 sigma and every calm market looked like manipulation.
            zero_is_normal=True,
        )

    def add_trade(self, timestamp_ms: int, size: float, price: float) -> float | None:
        """Accumulate a trade. Returns the bucket z-score when a bucket closes."""
        self.total_trades += 1
        bucket_ms = self.bucket_s * 1000
        bucket_start = (timestamp_ms // bucket_ms) * bucket_ms

        if self._current_bucket_start == 0:
            self._current_bucket_start = bucket_start

        if bucket_start > self._current_bucket_start:
            z = self._close_bucket(self._current_bucket_start)
            # Account for silent gaps: empty buckets are real zero-volume samples.
            gap = (bucket_start - self._current_bucket_start) // bucket_ms - 1
            for i in range(min(int(gap), self.window_s // max(self.bucket_s, 1))):
                ts = self._current_bucket_start + bucket_ms * (i + 1)
                self.buckets.add(ts, 0.0)
                self.zscore.update(0.0, ts)
            self._current_bucket_start = bucket_start
            self._current_bucket_volume = size
            self._current_bucket_notional = size * price
            return z

        self._current_bucket_volume += size
        self._current_bucket_notional += size * price
        return None

    def _close_bucket(self, ts: int) -> float | None:
        vol = self._current_bucket_volume
        self.buckets.add(ts, vol)
        z = self.zscore.update(vol, ts)
        if z is not None:
            self.cusum.update(clamp(z, -10, 10))
        return z

    def force_close(self, now_ms: int) -> float | None:
        """Close the in-flight bucket (used on shutdown / periodic flush)."""
        if self._current_bucket_start and self._current_bucket_volume > 0:
            z = self._close_bucket(self._current_bucket_start)
            self._current_bucket_start = (now_ms // (self.bucket_s * 1000)) * (self.bucket_s * 1000)
            self._current_bucket_volume = 0.0
            self._current_bucket_notional = 0.0
            return z
        return None

    @property
    def rolling_volume(self) -> float:
        return self.buckets.total

    @property
    def mean_bucket_volume(self) -> float:
        return self.buckets.mean

    @property
    def std_bucket_volume(self) -> float:
        return self.buckets.std

    def spike_ratio(self) -> float:
        """Current bucket volume divided by the rolling mean (1.0 == normal)."""
        mean = self.mean_bucket_volume
        return self._current_bucket_volume / mean if mean > 1e-12 else 1.0

    def exceeds_threshold(self, z: float | None) -> bool:
        """V > mu + k*sigma."""
        return z is not None and z > self.zscore.threshold


@dataclass
class CVDTracker:
    """Cumulative Volume Delta — aggressive buy volume minus aggressive sell volume.

    CVD rising while price stalls means aggressive buyers are being absorbed by a
    passive seller (a hidden wall); the reverse is distribution. That divergence
    is one of the cleanest tells of engineered price action, so we surface both
    the level and the divergence explicitly.
    """

    symbol: str
    venue: str
    window_s: int = 300

    cvd: float = 0.0
    session_cvd: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    delta_window: RollingWindow = field(init=False, repr=False)
    zscore: DynamicZScore = field(init=False, repr=False)
    _price_at_start: float = 0.0
    _last_price: float = 0.0
    _cvd_series: deque[tuple[int, float, float]] = field(
        default_factory=lambda: deque(maxlen=1200), repr=False
    )

    def __post_init__(self) -> None:
        self.delta_window = RollingWindow(window_ms=self.window_s * 1000)
        self.zscore = DynamicZScore(
            half_life_s=self.window_s / 3, warmup=20, base_threshold=2.5,
            zero_is_normal=True,
        )

    def add_trade(self, timestamp_ms: int, size: float, price: float, side: Side) -> float:
        """Apply a trade; ``side`` is the aggressor side. Returns the new CVD."""
        signed = size if side == "buy" else -size
        notional = signed * price
        self.cvd += notional
        self.session_cvd += notional
        if side == "buy":
            self.buy_volume += size * price
        else:
            self.sell_volume += size * price

        if self._price_at_start == 0:
            self._price_at_start = price
        self._last_price = price

        self.delta_window.add(timestamp_ms, notional)
        self._cvd_series.append((timestamp_ms, self.cvd, price))
        self.zscore.update(self.delta_window.total, timestamp_ms)
        return self.cvd

    @property
    def window_delta(self) -> float:
        return self.delta_window.total

    @property
    def buy_ratio(self) -> float:
        total = self.buy_volume + self.sell_volume
        return self.buy_volume / total if total > 0 else 0.5

    def divergence(self) -> float:
        """Signed divergence between CVD direction and price direction.

        Returns a value in [-1, 1]. Large positive = aggressive buying that is
        *not* moving price (absorption by a passive seller). Large negative =
        price rising without aggressive buying (thin-book markup, i.e. a likely
        wash/ramp).
        """
        if len(self._cvd_series) < 20:
            return 0.0
        window = list(self._cvd_series)[-min(len(self._cvd_series), 200):]
        t0_cvd, t0_px = window[0][1], window[0][2]
        t1_cvd, t1_px = window[-1][1], window[-1][2]
        if t0_px <= 0:
            return 0.0

        price_chg = (t1_px - t0_px) / t0_px
        scale = max(abs(v[1]) for v in window) or 1.0
        cvd_chg = (t1_cvd - t0_cvd) / scale

        norm_price = clamp(price_chg * 100.0, -1.0, 1.0)   # 1% move -> full scale
        norm_cvd = clamp(cvd_chg, -1.0, 1.0)
        return clamp((norm_cvd - norm_price) / 2.0, -1.0, 1.0)

    def absorption_score(self) -> float:
        """0-1 score: aggressive flow hitting an immovable passive wall."""
        div = self.divergence()
        z = self.zscore.last_z or 0.0
        if abs(div) < 0.25:
            return 0.0
        return clamp(abs(div) * clamp(abs(z) / 3.0, 0.0, 1.0), 0.0, 1.0)

    def reset_session(self) -> None:
        self.session_cvd = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self._price_at_start = self._last_price


@dataclass
class MicrostructureState:
    """Per (venue, symbol) container binding the three analytics together."""

    venue: str
    symbol: str
    book: OrderBookState = field(init=False)
    volume: VolumeProfile = field(init=False)
    cvd: CVDTracker = field(init=False)
    last_price: float = 0.0
    trades_seen: int = 0

    depth_levels: int = 20
    volume_window_s: int = 300
    volume_bucket_s: int = 5
    volume_threshold: float = 3.0
    cvd_window_s: int = 300

    def __post_init__(self) -> None:
        self.book = OrderBookState(
            symbol=self.symbol, venue=self.venue, depth_levels=self.depth_levels
        )
        self.volume = VolumeProfile(
            symbol=self.symbol,
            venue=self.venue,
            window_s=self.volume_window_s,
            bucket_s=self.volume_bucket_s,
            threshold=self.volume_threshold,
        )
        self.cvd = CVDTracker(symbol=self.symbol, venue=self.venue, window_s=self.cvd_window_s)

    def on_trade(self, timestamp_ms: int, price: float, size: float, side: Side) -> float | None:
        self.trades_seen += 1
        self.last_price = price
        self.cvd.add_trade(timestamp_ms, size, price, side)
        return self.volume.add_trade(timestamp_ms, size, price)

    def snapshot(self) -> dict[str, float]:
        obi = self.book.imbalance()
        return {
            "price": self.last_price,
            "obi": obi.obi,
            "obi_weighted": obi.weighted_obi,
            "obi_z": obi.z_score or 0.0,
            "spread_bps": obi.spread_bps,
            "bid_depth": obi.bid_depth,
            "ask_depth": obi.ask_depth,
            "volume_5m": self.volume.rolling_volume,
            "volume_z": self.volume.zscore.last_z or 0.0,
            "volume_spike_ratio": self.volume.spike_ratio(),
            "cvd": self.cvd.cvd,
            "cvd_z": self.cvd.zscore.last_z or 0.0,
            "cvd_divergence": self.cvd.divergence(),
            "absorption": self.cvd.absorption_score(),
            "buy_ratio": self.cvd.buy_ratio,
            "trades": float(self.trades_seen),
        }
