"""On-demand analysis for any listed symbol, via REST.

The streaming engine only subscribes to what discovery flags as anomalous — a
deliberate choice, since watching every pair on nine venues is neither possible
nor useful. But that leaves a gap: a user asking `/check BTC` gets "no data"
purely because BTC is behaving normally, which reads as the bot being broken.

This module closes that gap. It reconstructs the same feature vector the live
pipeline produces, using REST calls made at query time:

* ``fetch_ticker``      → 24h change, quote volume, last price
* ``fetch_order_book``  → order book imbalance, depth, spread
* ``fetch_ohlcv``       → volume z-score against a rolling candle baseline

The result feeds the identical :class:`~cadb.modules.ml.classifier.RuleEngine`,
so an on-demand score is directly comparable to a streamed one. Every command
therefore answers for *any* listed token, not just the ones currently streaming.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import statistics
import time
from dataclasses import dataclass, field
from typing import Any

from ...core.schema import now_ms
from ...core.stats import clamp
from ..ml.features import FEATURE_NAMES, FeatureVector

log = logging.getLogger(__name__)

__all__ = ["OnDemandScanner", "SymbolSnapshot"]


@dataclass
class SymbolSnapshot:
    """A point-in-time measurement of one symbol on one venue."""

    symbol: str
    venue: str
    price: float = 0.0
    change_pct: float = 0.0
    quote_volume: float = 0.0
    obi: float = 0.0
    bid_depth: float = 0.0
    ask_depth: float = 0.0
    spread_bps: float = 0.0
    volume_z: float = 0.0
    volume_spike_ratio: float = 1.0
    candles: int = 0
    fetched_ms: int = field(default_factory=now_ms)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.price > 0

    def to_feature_vector(self, asset: str) -> FeatureVector:
        """Map onto the canonical feature vector used by the live pipeline.

        Only the exchange block is populated — an on-demand REST snapshot has no
        on-chain or social context — so ``coverage`` reflects that honestly and
        the classifier damps accordingly.
        """
        values = dict.fromkeys(FEATURE_NAMES, 0.0)
        values["volume_spike_ratio"] = self.volume_spike_ratio
        values["volume_z"] = clamp(self.volume_z, -20, 20)
        values["obi"] = clamp(self.obi, -1, 1)
        values["obi_abs"] = abs(values["obi"])
        values["spread_bps"] = clamp(math.log1p(max(self.spread_bps, 0.0)), 0.0, 10.0)
        return FeatureVector(
            asset=asset,
            timestamp=self.fetched_ms,
            values=[values[n] for n in FEATURE_NAMES],
            sources_fresh={"exchange": True, "onchain": False, "social": False},
            coverage=1 / 3,
        )


class OnDemandScanner:
    """Fetches live metrics for arbitrary symbols across configured venues."""

    def __init__(
        self,
        venues: list[str],
        quote: str = "USDT",
        cache_ttl_s: float = 20.0,
        timeout_s: float = 12.0,
    ) -> None:
        self.venues = venues
        self.quote = quote
        self.cache_ttl_s = cache_ttl_s
        self.timeout_s = timeout_s
        self._clients: dict[str, Any] = {}
        self._markets: dict[str, set[str]] = {}
        self._cache: dict[str, tuple[float, SymbolSnapshot]] = {}
        self._lock = asyncio.Lock()

    # ---- client management ------------------------------------------
    def _client(self, venue: str) -> Any:
        client = self._clients.get(venue)
        if client is not None:
            return client
        try:
            import ccxt.async_support as accxt
        except ImportError:  # pragma: no cover
            raise RuntimeError("ccxt required for on-demand scanning") from None
        if not hasattr(accxt, venue):
            raise ValueError(f"unknown venue {venue}")
        client = getattr(accxt, venue)({"enableRateLimit": True, "timeout": int(self.timeout_s * 1000)})
        self._clients[venue] = client
        return client

    async def markets(self, venue: str) -> set[str]:
        """Cached set of spot symbols a venue lists."""
        cached = self._markets.get(venue)
        if cached is not None:
            return cached
        try:
            client = self._client(venue)
            data = await asyncio.wait_for(client.load_markets(), timeout=self.timeout_s)
            symbols = {
                s for s, m in data.items()
                if m.get("spot") and m.get("active") is not False
            }
        except Exception as exc:
            log.debug("%s: load_markets failed: %s", venue, exc)
            symbols = set()
        self._markets[venue] = symbols
        return symbols

    async def resolve(self, query: str) -> list[tuple[str, str]]:
        """Map a user query to concrete (venue, symbol) pairs.

        Accepts ``BTC``, ``btc/usdt`` or ``BTC/USDT``. Returns every venue that
        lists the pair, so a query answers across the whole configured set
        rather than an arbitrary single venue.
        """
        q = query.strip().upper()
        symbol = q if "/" in q else f"{q}/{self.quote}"
        found: list[tuple[str, str]] = []
        for venue in self.venues:
            symbols = await self.markets(venue)
            if symbol in symbols:
                found.append((venue, symbol))
        return found

    async def search(self, fragment: str, limit: int = 12) -> list[str]:
        """Find listed symbols containing ``fragment`` (for typo help)."""
        frag = fragment.strip().upper().split("/")[0]
        hits: set[str] = set()
        for venue in self.venues:
            for sym in await self.markets(venue):
                if sym.endswith(f"/{self.quote}") and frag in sym.split("/")[0]:
                    hits.add(sym)
                    if len(hits) >= limit * 3:
                        break
        return sorted(hits)[:limit]

    # ---- measurement --------------------------------------------------
    async def snapshot(self, venue: str, symbol: str) -> SymbolSnapshot:
        """Fetch ticker, order book and candles for one pair."""
        key = f"{venue}:{symbol}"
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached and now - cached[0] < self.cache_ttl_s:
            return cached[1]

        snap = SymbolSnapshot(symbol=symbol, venue=venue)
        try:
            client = self._client(venue)
            ticker, book, ohlcv = await asyncio.gather(
                asyncio.wait_for(client.fetch_ticker(symbol), timeout=self.timeout_s),
                asyncio.wait_for(client.fetch_order_book(symbol, limit=50), timeout=self.timeout_s),
                asyncio.wait_for(
                    client.fetch_ohlcv(symbol, "1m", limit=60), timeout=self.timeout_s
                ),
                return_exceptions=True,
            )

            if isinstance(ticker, dict):
                snap.price = float(ticker.get("last") or ticker.get("close") or 0.0)
                snap.change_pct = float(ticker.get("percentage") or 0.0)
                snap.quote_volume = float(ticker.get("quoteVolume") or 0.0)

            if isinstance(book, dict) and book.get("bids") and book.get("asks"):
                bids = [(float(p), float(s)) for p, s, *_ in book["bids"][:20]]
                asks = [(float(p), float(s)) for p, s, *_ in book["asks"][:20]]
                snap.bid_depth = sum(p * s for p, s in bids)
                snap.ask_depth = sum(p * s for p, s in asks)
                total = snap.bid_depth + snap.ask_depth
                snap.obi = (snap.bid_depth - snap.ask_depth) / total if total > 0 else 0.0
                if bids and asks:
                    mid = (bids[0][0] + asks[0][0]) / 2
                    if mid > 0:
                        snap.spread_bps = (asks[0][0] - bids[0][0]) / mid * 10_000

            if isinstance(ohlcv, list) and len(ohlcv) >= 10:
                vols = [float(c[5]) for c in ohlcv if c and len(c) > 5]
                snap.candles = len(vols)
                if len(vols) >= 10:
                    # Log space, matching the streaming path: raw volume is
                    # log-normal and a linear z-score is meaningless on it.
                    lv = [math.log1p(max(v, 0.0)) for v in vols[:-1]]
                    mu = statistics.fmean(lv)
                    sd = statistics.pstdev(lv)
                    latest = math.log1p(max(vols[-1], 0.0))
                    snap.volume_z = (latest - mu) / sd if sd > 1e-9 else 0.0
                    mean_raw = statistics.fmean(vols[:-1]) or 1.0
                    snap.volume_spike_ratio = clamp(vols[-1] / mean_raw, 0.0, 50.0)

            if not snap.ok and snap.error is None:
                snap.error = "no price data returned"
        except Exception as exc:
            snap.error = f"{type(exc).__name__}: {exc}"[:120]

        self._cache[key] = (now, snap)
        return snap

    async def best_snapshot(self, query: str) -> tuple[SymbolSnapshot | None, list[str]]:
        """Snapshot the venue with the deepest book for a query.

        Returns ``(snapshot, venues_listing_it)``. Picking the deepest book
        rather than the first match avoids reporting a thin secondary listing as
        though it were the primary market.
        """
        pairs = await self.resolve(query)
        if not pairs:
            return None, []
        snaps = await asyncio.gather(
            *(self.snapshot(v, s) for v, s in pairs), return_exceptions=True
        )
        valid = [s for s in snaps if isinstance(s, SymbolSnapshot) and s.ok]
        if not valid:
            return None, [v for v, _ in pairs]
        best = max(valid, key=lambda s: s.bid_depth + s.ask_depth)
        return best, [v for v, _ in pairs]

    async def close(self) -> None:
        for client in self._clients.values():
            with contextlib.suppress(Exception):
                await client.close()
        self._clients.clear()

    def stats(self) -> dict[str, Any]:
        return {
            "venues": len(self.venues),
            "markets_cached": {v: len(s) for v, s in self._markets.items()},
            "snapshots_cached": len(self._cache),
        }
