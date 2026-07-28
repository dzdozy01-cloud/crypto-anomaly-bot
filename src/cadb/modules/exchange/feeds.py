"""Exchange connectivity: CCXT Pro WebSockets, native WS fallback, simulator.

Three interchangeable feed backends behind :class:`ExchangeFeed`:

1. :class:`CCXTProFeed`   — ``watch_order_book`` / ``watch_trades`` (Binance, Bybit, MEXC).
2. :class:`NativeWSFeed`  — hand-rolled WebSocket clients used when ccxt is absent.
3. :class:`SimulatedFeed` — deterministic synthetic market with injectable
   manipulation episodes; powers the demo and the integration tests.

All three yield the same normalised dicts, so the engine never branches on venue.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from ...core.schema import now_ms

log = logging.getLogger(__name__)

__all__ = [
    "ExchangeFeed",
    "CCXTProFeed",
    "NativeWSFeed",
    "SimulatedFeed",
    "build_feed",
    "OrderBookUpdate",
    "TradeUpdate",
]

# Native WebSocket endpoints, used only when ccxt.pro is unavailable.
NATIVE_WS_ENDPOINTS: dict[str, str] = {
    "binance": "wss://stream.binance.com:9443/stream?streams=",
    "bybit": "wss://stream.bybit.com/v5/public/spot",
    "mexc": "wss://wbs.mexc.com/ws",
}


@dataclass
class OrderBookUpdate:
    venue: str
    symbol: str
    timestamp: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


@dataclass
class TradeUpdate:
    venue: str
    symbol: str
    timestamp: int
    price: float
    size: float
    side: str  # aggressor: "buy" | "sell"


class ExchangeFeed(ABC):
    """Abstract market-data feed for a single venue."""

    def __init__(self, venue: str, symbols: list[str], depth: int = 50) -> None:
        self.venue = venue.lower()
        self.symbols = symbols
        self.depth = depth
        self.connected = False
        self.messages = 0

    @abstractmethod
    async def watch_order_book(self, symbol: str) -> AsyncIterator[OrderBookUpdate]: ...

    @abstractmethod
    async def watch_trades(self, symbol: str) -> AsyncIterator[TradeUpdate]: ...

    async def close(self) -> None:
        self.connected = False


# Per-venue WebSocket quirks, all verified live against ccxt 4.5.
#
# `limit` — the depth argument watch_order_book will accept. Kraken rejects
#   anything outside {10,25,100,500,1000} with NotSupported; several venues
#   ignore the argument entirely and stream a fixed book, so None means
#   "call without a limit and take whatever the venue sends".
# `options` — constructor options needed for *public* access. OKX defaults to
#   the `books5` channel, which it treats as authenticated; `books` is the free
#   public feed and needs no credentials.
VENUE_WS_QUIRKS: dict[str, dict[str, Any]] = {
    "kraken": {"limit": 25},                       # must be 10/25/100/500/1000
    "okx": {"limit": None, "options": {"depth": "books"}},  # books5 needs auth
    "bitget": {"limit": None},
    "kucoin": {"limit": None},
    "coinbase": {"limit": None},
    "gate": {"limit": 20},
    "mexc": {"limit": 20},
    "binance": {"limit": 20},
    "bybit": {"limit": 50},
}


class CCXTProFeed(ExchangeFeed):
    """CCXT Pro WebSocket feed (the production path).

    ccxt.pro's ``watch_*`` methods already implement per-venue reconnection, but
    we still wrap each consumer in the module-level supervisor so that a hard
    failure (auth error, delisted symbol, venue-wide outage) is retried with our
    own exponential backoff rather than spinning.
    """

    def __init__(self, venue: str, symbols: list[str], depth: int = 50) -> None:
        super().__init__(venue, symbols, depth)
        self._client: Any = None
        self._ws_limit: int | None = depth

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import ccxt.pro as ccxtpro
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("ccxt>=4 required for CCXTProFeed") from exc
        if not hasattr(ccxtpro, self.venue):
            raise ValueError(f"ccxt.pro has no exchange '{self.venue}'")
        quirks = VENUE_WS_QUIRKS.get(self.venue, {})
        options: dict[str, Any] = {"defaultType": "spot"}
        options.update(quirks.get("options", {}))
        self._client = getattr(ccxtpro, self.venue)(
            {
                "enableRateLimit": True,
                "newUpdates": True,
                "options": options,
            }
        )
        # None => omit the limit argument entirely (venue streams a fixed book).
        self._ws_limit = quirks.get("limit", self.depth)
        self.connected = True
        return self._client

    async def watch_order_book(self, symbol: str) -> AsyncIterator[OrderBookUpdate]:
        client = self._ensure_client()
        limit = getattr(self, "_ws_limit", self.depth)
        while True:
            book = (
                await client.watch_order_book(symbol, limit=limit)
                if limit is not None
                else await client.watch_order_book(symbol)
            )
            self.messages += 1
            yield OrderBookUpdate(
                venue=self.venue,
                symbol=symbol,
                timestamp=int(book.get("timestamp") or now_ms()),
                bids=[(float(p), float(s)) for p, s, *_ in book.get("bids", [])[: self.depth]],
                asks=[(float(p), float(s)) for p, s, *_ in book.get("asks", [])[: self.depth]],
            )

    async def watch_trades(self, symbol: str) -> AsyncIterator[TradeUpdate]:
        client = self._ensure_client()
        while True:
            trades = await client.watch_trades(symbol)
            self.messages += len(trades)
            for t in trades:
                price = float(t.get("price") or 0)
                size = float(t.get("amount") or 0)
                if price <= 0 or size <= 0:
                    continue
                yield TradeUpdate(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=int(t.get("timestamp") or now_ms()),
                    price=price,
                    size=size,
                    side="buy" if (t.get("side") or "buy") == "buy" else "sell",
                )

    async def close(self) -> None:
        await super().close()
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.close()
            self._client = None


class NativeWSFeed(ExchangeFeed):
    """Raw WebSocket implementation for Binance / Bybit / MEXC.

    Used when ccxt is not installed. Each venue speaks a different dialect, so
    subscription framing and message parsing are dispatched per venue while the
    output stays normalised.
    """

    def __init__(self, venue: str, symbols: list[str], depth: int = 50) -> None:
        super().__init__(venue, symbols, depth)
        self._queues: dict[str, asyncio.Queue[Any]] = {}

    @staticmethod
    def _ws_symbol(venue: str, symbol: str) -> str:
        raw = symbol.replace("/", "").upper()
        return raw.lower() if venue == "binance" else raw

    def _subscribe_frames(self, symbol: str) -> tuple[str, list[dict[str, Any]]]:
        s = self._ws_symbol(self.venue, symbol)
        if self.venue == "binance":
            depth = min(self.depth, 20)
            streams = f"{s}@depth{depth}@100ms/{s}@aggTrade"
            return NATIVE_WS_ENDPOINTS["binance"] + streams, []
        if self.venue == "bybit":
            depth = 50 if self.depth > 1 else 1
            return NATIVE_WS_ENDPOINTS["bybit"], [
                {"op": "subscribe", "args": [f"orderbook.{depth}.{s}", f"publicTrade.{s}"]}
            ]
        if self.venue == "mexc":
            return NATIVE_WS_ENDPOINTS["mexc"], [
                {
                    "method": "SUBSCRIPTION",
                    "params": [
                        f"spot@public.limit.depth.v3.api@{s}@20",
                        f"spot@public.deals.v3.api@{s}",
                    ],
                }
            ]
        raise ValueError(f"unsupported venue for native feed: {self.venue}")

    async def _stream(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("`websockets` required for NativeWSFeed") from exc

        url, frames = self._subscribe_frames(symbol)
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=512) as ws:
            self.connected = True
            for frame in frames:
                await ws.send(json.dumps(frame))
            log.info("native ws connected %s %s", self.venue, symbol)
            async for raw in ws:
                self.messages += 1
                with contextlib.suppress(json.JSONDecodeError):
                    yield json.loads(raw)

    def _parse(self, symbol: str, msg: dict[str, Any]) -> list[OrderBookUpdate | TradeUpdate]:
        out: list[OrderBookUpdate | TradeUpdate] = []
        ts = now_ms()
        if self.venue == "binance":
            data = msg.get("data", msg)
            stream = msg.get("stream", "")
            if "depth" in stream or ("bids" in data and "asks" in data):
                out.append(
                    OrderBookUpdate(
                        self.venue, symbol, int(data.get("E", ts)),
                        [(float(p), float(q)) for p, q in data.get("bids", data.get("b", []))],
                        [(float(p), float(q)) for p, q in data.get("asks", data.get("a", []))],
                    )
                )
            elif data.get("e") == "aggTrade":
                # Binance 'm' = buyer is market maker => the aggressor was a seller.
                out.append(
                    TradeUpdate(
                        self.venue, symbol, int(data.get("T", ts)),
                        float(data["p"]), float(data["q"]),
                        "sell" if data.get("m") else "buy",
                    )
                )
        elif self.venue == "bybit":
            topic = msg.get("topic", "")
            data = msg.get("data")
            if not data:
                return out
            if topic.startswith("orderbook"):
                out.append(
                    OrderBookUpdate(
                        self.venue, symbol, int(msg.get("ts", ts)),
                        [(float(p), float(q)) for p, q in data.get("b", [])],
                        [(float(p), float(q)) for p, q in data.get("a", [])],
                    )
                )
            elif topic.startswith("publicTrade"):
                for t in data:
                    out.append(
                        TradeUpdate(
                            self.venue, symbol, int(t.get("T", ts)),
                            float(t["p"]), float(t["v"]),
                            "buy" if t.get("S", "Buy").lower() == "buy" else "sell",
                        )
                    )
        elif self.venue == "mexc":
            channel = msg.get("c", msg.get("channel", ""))
            data = msg.get("d", msg.get("data", {}))
            if "depth" in channel:
                out.append(
                    OrderBookUpdate(
                        self.venue, symbol, int(msg.get("t", ts)),
                        [(float(b["p"]), float(b["v"])) for b in data.get("bids", [])],
                        [(float(a["p"]), float(a["v"])) for a in data.get("asks", [])],
                    )
                )
            elif "deals" in channel:
                for t in data.get("deals", []):
                    out.append(
                        TradeUpdate(
                            self.venue, symbol, int(t.get("t", ts)),
                            float(t["p"]), float(t["v"]),
                            "buy" if str(t.get("S", 1)) == "1" else "sell",
                        )
                    )
        return out

    async def _fanout(self, symbol: str) -> None:
        """Single socket per symbol; demultiplex into book/trade queues."""
        books = self._queues.setdefault(f"{symbol}:book", asyncio.Queue(maxsize=1000))
        trades = self._queues.setdefault(f"{symbol}:trade", asyncio.Queue(maxsize=5000))
        async for msg in self._stream(symbol):
            for parsed in self._parse(symbol, msg):
                q = books if isinstance(parsed, OrderBookUpdate) else trades
                if q.full():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        q.get_nowait()
                q.put_nowait(parsed)

    async def watch_order_book(self, symbol: str) -> AsyncIterator[OrderBookUpdate]:
        q: asyncio.Queue[OrderBookUpdate] = self._queues.setdefault(
            f"{symbol}:book", asyncio.Queue(maxsize=1000)
        )
        task = asyncio.create_task(self._fanout(symbol), name=f"ws-{self.venue}-{symbol}")
        try:
            while True:
                yield await q.get()
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def watch_trades(self, symbol: str) -> AsyncIterator[TradeUpdate]:
        q: asyncio.Queue[TradeUpdate] = self._queues.setdefault(
            f"{symbol}:trade", asyncio.Queue(maxsize=5000)
        )
        while True:
            yield await q.get()


@dataclass
class _SimSymbolState:
    price: float
    drift: float = 0.0
    vol: float = 0.0006
    base_size: float = 0.4
    episode_until: float = 0.0
    episode_kind: str = ""


class _GlobalPriceProcess:
    """Shared price process so simulated venues stay arbitraged together.

    Without this, each venue random-walks independently and cross-venue metrics
    (dispersion, OBI disagreement) light up on pure noise — which is not how real
    markets behave: arbitrageurs keep spot venues within a few basis points of
    each other. Venues therefore share one reference price and differ only by a
    small persistent basis.
    """

    _prices: dict[str, float] = {}
    _last_step: dict[str, float] = {}

    @classmethod
    def reference(cls, symbol: str, seed_price: float) -> float:
        return cls._prices.setdefault(symbol, seed_price)

    @classmethod
    def step(cls, symbol: str, seed_price: float, drift: float, vol: float, rng: random.Random) -> float:
        """Advance the shared reference at most once per 20 ms."""
        now = time.monotonic()
        price = cls._prices.setdefault(symbol, seed_price)
        if now - cls._last_step.get(symbol, 0.0) < 0.02:
            return price
        cls._last_step[symbol] = now
        price *= math.exp(rng.gauss(drift, vol))
        cls._prices[symbol] = price
        return price

    @classmethod
    def reset(cls) -> None:
        cls._prices.clear()
        cls._last_step.clear()


class SimulatedFeed(ExchangeFeed):
    """Deterministic synthetic venue with injectable manipulation episodes.

    Not a toy: it reproduces the microstructure signatures we detect — spoofed
    book imbalance, volume bursts with one-sided aggression, and price ramps that
    are unsupported by CVD — so the whole pipeline can be validated end-to-end
    (and regression-tested) without touching a live exchange.
    """

    SEED_PRICES = {
        "BTC/USDT": 64_000.0, "ETH/USDT": 3_200.0, "SOL/USDT": 145.0,
        "PEPE/USDT": 0.0000112, "ARB/USDT": 0.92, "DOGE/USDT": 0.148,
    }

    def __init__(
        self,
        venue: str,
        symbols: list[str],
        depth: int = 50,
        seed: int | None = None,
        tick_interval: float = 0.05,
        episode_probability: float = 0.004,
    ) -> None:
        super().__init__(venue, symbols, depth)
        self.rng = random.Random(seed if seed is not None else hash(venue) & 0xFFFF)
        self.tick_interval = tick_interval
        self.episode_probability = episode_probability
        self.connected = True
        # Order sizes are derived from price so every symbol carries a
        # comparable ~$25k of notional per level. A fixed base size would give a
        # micro-cap pair a fraction of a cent of depth and make its OBI, CVD and
        # depth readouts meaningless.
        self.state: dict[str, _SimSymbolState] = {}
        for sym in symbols:
            seed = self.SEED_PRICES.get(sym, 100.0)
            self.state[sym] = _SimSymbolState(
                price=seed, base_size=max(25_000.0 / seed, 1e-8)
            )
        # Persistent per-venue basis of a few bps, as real venues exhibit.
        self.basis: dict[str, float] = {
            s: 1.0 + self.rng.gauss(0, 0.0004) for s in symbols
        }
        self._last_episode_check: dict[str, float] = {}

    def inject_episode(self, symbol: str, kind: str = "pump", duration_s: float = 20.0) -> None:
        """Force a manipulation episode (used by the demo and tests)."""
        st = self.state.get(symbol)
        if st:
            st.episode_until = asyncio.get_event_loop().time() + duration_s
            st.episode_kind = kind
            log.info("[sim] injected %s episode on %s (%.0fs)", kind, symbol, duration_s)

    def _maybe_episode(self, symbol: str) -> None:
        """Spontaneously start an episode at a bounded *time-based* rate.

        Must not be a per-tick probability: with ~20 ticks/s per stream across
        several venues and symbols, even p=1e-3 produces episodes continuously
        and the 'background' regime is never actually calm.
        """
        st = self.state[symbol]
        loop_t = asyncio.get_event_loop().time()
        if st.episode_until > loop_t:
            return
        if st.episode_kind and st.episode_until <= loop_t:
            st.episode_kind = ""

        last = self._last_episode_check.get(symbol, 0.0)
        if loop_t - last < 1.0:
            return
        self._last_episode_check[symbol] = loop_t
        # episode_probability is interpreted as "expected episodes per second".
        if self.rng.random() < self.episode_probability:
            self.inject_episode(
                symbol,
                self.rng.choice(["pump", "spoof", "dump", "wash"]),
                self.rng.uniform(15, 40),
            )

    def _step_price(self, symbol: str) -> _SimSymbolState:
        st = self.state[symbol]
        self._maybe_episode(symbol)
        active = st.episode_until > asyncio.get_event_loop().time()
        drift = 0.0
        vol = st.vol
        if active:
            if st.episode_kind == "pump":
                drift, vol = 0.0012, st.vol * 3
            elif st.episode_kind == "dump":
                drift, vol = -0.0014, st.vol * 3.2
            elif st.episode_kind == "wash":
                drift, vol = 0.0, st.vol * 0.4
        # Track the shared reference (arbitrage) plus this venue's small basis.
        reference = _GlobalPriceProcess.step(
            symbol, self.SEED_PRICES.get(symbol, 100.0), drift, vol, self.rng
        )
        st.price = reference * self.basis.get(symbol, 1.0)
        return st

    def _build_book(self, symbol: str) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        st = self.state[symbol]
        active = st.episode_until > asyncio.get_event_loop().time()
        mid = st.price
        tick = max(mid * 0.00005, 1e-12)
        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        # Spoof episodes stack a fake wall on one side of the book.
        spoof_side = None
        if active and st.episode_kind in ("spoof", "pump"):
            spoof_side = "bid" if st.episode_kind == "pump" else self.rng.choice(["bid", "ask"])

        for i in range(self.depth):
            decay = math.exp(-i / 12)
            bsz = st.base_size * decay * self.rng.uniform(0.6, 1.5)
            asz = st.base_size * decay * self.rng.uniform(0.6, 1.5)
            if spoof_side == "bid" and 2 <= i <= 6:
                bsz *= 14
            elif spoof_side == "ask" and 2 <= i <= 6:
                asz *= 14
            bids.append((round(mid - tick * (i + 1), 10), round(bsz, 8)))
            asks.append((round(mid + tick * (i + 1), 10), round(asz, 8)))
        return bids, asks

    async def watch_order_book(self, symbol: str) -> AsyncIterator[OrderBookUpdate]:
        while True:
            await asyncio.sleep(self.tick_interval * self.rng.uniform(0.7, 1.3))
            self._step_price(symbol)
            bids, asks = self._build_book(symbol)
            self.messages += 1
            yield OrderBookUpdate(self.venue, symbol, now_ms(), bids, asks)

    async def watch_trades(self, symbol: str) -> AsyncIterator[TradeUpdate]:
        while True:
            await asyncio.sleep(self.tick_interval * self.rng.uniform(0.4, 1.6))
            st = self._step_price(symbol)
            active = st.episode_until > asyncio.get_event_loop().time()
            n_trades, size_mult, buy_bias = 1, 1.0, 0.5
            if active:
                if st.episode_kind == "pump":
                    n_trades, size_mult, buy_bias = self.rng.randint(3, 8), 7.0, 0.85
                elif st.episode_kind == "dump":
                    n_trades, size_mult, buy_bias = self.rng.randint(3, 9), 8.0, 0.12
                elif st.episode_kind == "wash":
                    n_trades, size_mult, buy_bias = self.rng.randint(4, 10), 5.0, 0.5
            for _ in range(n_trades):
                side = "buy" if self.rng.random() < buy_bias else "sell"
                size = abs(self.rng.gauss(st.base_size, st.base_size / 2)) * size_mult + 1e-9
                self.messages += 1
                yield TradeUpdate(
                    self.venue, symbol, now_ms(),
                    st.price * (1 + self.rng.gauss(0, 0.00002)),
                    size, side,
                )


def build_feed(
    venue: str,
    symbols: list[str],
    depth: int = 50,
    simulate: bool = False,
    prefer_ccxt: bool = True,
    seed: int | None = None,
) -> ExchangeFeed:
    """Select the best available backend for a venue."""
    if simulate:
        return SimulatedFeed(venue, symbols, depth, seed=seed)
    if prefer_ccxt:
        try:
            import ccxt.pro as _ccxtpro

            if hasattr(_ccxtpro, venue):
                return CCXTProFeed(venue, symbols, depth)
            log.error(
                "venue '%s' is not supported by ccxt.pro — it has no public "
                "market-data WebSocket. Remove it from exchange.exchanges.",
                venue,
            )
        except ImportError:
            log.warning("ccxt.pro unavailable; using native websocket feed for %s", venue)
    if venue in NATIVE_WS_ENDPOINTS:
        return NativeWSFeed(venue, symbols, depth)

    # Never silently substitute synthetic data for a live venue. Doing so
    # produces alerts about markets that were never observed — indistinguishable
    # from real detections, and corrosive to trust in every other alert.
    raise ValueError(
        f"no live feed backend for '{venue}'. Supported venues are the ccxt.pro "
        f"exchange ids (binance, bybit, mexc, gate, kucoin, coinbase, okx, kraken, …). "
        f"Use simulate=true explicitly if you want synthetic data."
    )
