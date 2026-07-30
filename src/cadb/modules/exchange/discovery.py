"""Dynamic symbol discovery — find the pairs actually worth watching.

A static symbol list has a fundamental blind spot: **manipulation concentrates in
the assets you did not think to list**. BTC is far too deep and too heavily
arbitraged to move on a single actor's flow; a three-day-old meme coin with
$400k of liquidity is where pumps and dumps actually happen.

This module periodically ranks a venue's tradable universe and returns the
subset most likely to be manipulated, using three orthogonal criteria:

* **Movers** — large absolute 24h price change. A dump is as interesting as a
  pump, so the ranking is direction-agnostic.
* **Volume surges** — 24h volume far above the pair's typical level, which is the
  earliest public sign of coordinated activity.
* **New listings** — pairs that appeared since the last scan. Freshly listed
  low-float tokens are the single highest-risk category.

Liquidity is used as a *filter*, not a ranking: pairs below a floor are noise
(a $2k-volume pair moves 80% on one order), and pairs above a ceiling are too
deep to manipulate cheaply and would just crowd out the interesting ones.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

__all__ = ["SymbolDiscovery", "DiscoveredSymbol"]


@dataclass(frozen=True)
class DiscoveredSymbol:
    """A candidate pair with the evidence that surfaced it."""

    symbol: str
    venue: str
    quote_volume_usd: float
    change_pct: float
    reason: str
    score: float

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.symbol} ({self.reason}, {self.change_pct:+.1f}%, ${self.quote_volume_usd:,.0f})"


@dataclass
class SymbolDiscovery:
    """Ranks a venue's universe and tracks which pairs are newly listed.

    Runs off REST ``fetch_tickers`` (one request per venue per interval), so it
    adds negligible load compared with the WebSocket streams it feeds.
    """

    venue: str
    max_symbols: int = 25
    min_volume_usd: float = 100_000.0
    max_volume_usd: float = 50_000_000.0
    min_change_pct: float = 15.0
    volume_surge_ratio: float = 3.0
    always_include: tuple[str, ...] = ()
    # Quote currencies to scan, in preference order. A single hardcoded "USDT"
    # made Coinbase and Kraken almost invisible: they are USD/USDC venues, so
    # scanning only USDT covered 3% and 6% of their markets respectively —
    # 1,557 pairs unseen across the two. Fiat quotes (EUR/GBP) are excluded
    # deliberately: they duplicate the same asset at an FX offset.
    quotes: tuple[str, ...] = ("USDT", "USDC", "USD", "FDUSD", "USD1")
    quote: str = "USDT"  # retained for the on-demand resolver's default
    track_new_listings: bool = True
    new_listing_grace_h: float = 48.0
    new_listing_min_volume_usd: float = 20_000.0
    # A new listing must also show real movement or real liquidity. Without
    # this, "first seen in the ticker feed" alone was enough to win a slot.
    new_listing_min_change_pct: float = 10.0

    # symbol -> rolling median 24h volume, for surge detection
    _volume_history: dict[str, list[float]] = field(default_factory=dict, repr=False)
    _known_symbols: set[str] = field(default_factory=set, repr=False)
    # symbol -> unix ts first observed, so a new pair can be watched for a
    # grace period even while it is still quiet.
    _first_seen: dict[str, float] = field(default_factory=dict, repr=False)
    _first_scan: bool = True
    last_scan_ts: float = 0.0
    scans: int = 0

    # ---- ranking ---------------------------------------------------------
    def _volume_baseline(self, symbol: str) -> float | None:
        hist = self._volume_history.get(symbol)
        if not hist or len(hist) < 3:
            return None
        ordered = sorted(hist)
        return ordered[len(ordered) // 2]

    def _record_volume(self, symbol: str, volume: float, keep: int = 24) -> None:
        hist = self._volume_history.setdefault(symbol, [])
        hist.append(volume)
        if len(hist) > keep:
            del hist[: len(hist) - keep]

    def evaluate(self, tickers: dict[str, dict[str, Any]]) -> list[DiscoveredSymbol]:
        """Rank ``fetch_tickers`` output into a watchlist."""
        self.scans += 1
        now = time.time()
        self.last_scan_ts = now
        candidates: list[DiscoveredSymbol] = []
        seen_now: set[str] = set()

        for symbol, data in tickers.items():
            quote_ccy = symbol.split("/")[-1] if "/" in symbol else ""
            if quote_ccy not in self.quotes:
                continue
            # Record the symbol as *known* before any filtering. Previously a
            # pair with zero 24h volume was skipped before this line, so it
            # never entered `_known_symbols` — and the moment it traded once it
            # looked brand new. That is how dormant Binance altcoins (A2Z, ACA,
            # ATA, DENT, FIO, HARD) ended up occupying the watchlist for 48h on
            # a +0.4% move.
            seen_now.add(symbol)
            volume = float(data.get("quoteVolume") or 0.0)
            if volume <= 0:
                continue

            change = data.get("percentage")
            change = float(change) if change is not None else 0.0
            baseline = self._volume_baseline(symbol)
            self._record_volume(symbol, volume)

            # Register the listing *before* the liquidity filter. Otherwise a
            # brand-new pair is judged against the high floor on the very scan
            # it appears, filtered out, and never recorded — so it is treated as
            # "new" again on every subsequent scan and never actually tracked.
            is_new = not self._first_scan and symbol not in self._known_symbols
            if is_new and symbol not in self._first_seen:
                self._first_seen[symbol] = now
            age_h = (
                (now - self._first_seen[symbol]) / 3600.0
                if symbol in self._first_seen else None
            )

            # Liquidity band: too thin is noise, too deep is not cheaply moved.
            # New listings get a lower floor — they legitimately start small,
            # and excluding them would defeat the point of tracking them.
            recently_listed = (
                symbol in self._first_seen
                and (now - self._first_seen[symbol]) / 3600.0 <= self.new_listing_grace_h
            )
            floor = (
                self.new_listing_min_volume_usd
                if (recently_listed and self.track_new_listings)
                else self.min_volume_usd
            )
            if volume < floor or volume > self.max_volume_usd:
                continue

            reasons: list[str] = []
            score = 0.0

            if abs(change) >= self.min_change_pct:
                # Direction-agnostic, and scaled logarithmically. A linear ratio
                # let a +1200% outlier saturate the cap while a -55% dump — just
                # as strong a manipulation signal, and bounded at -100% by
                # definition — scored far lower purely because downside moves
                # cannot exceed 100%. Normalising the magnitude first keeps
                # pumps and dumps of comparable severity comparably ranked.
                severity = abs(change) / self.min_change_pct
                score += min(10.0 * math.log1p(severity) / math.log(2), 40.0)
                reasons.append(f"{'pump' if change > 0 else 'dump'} {change:+.0f}%")

            if baseline and baseline > 0:
                surge = volume / baseline
                if surge >= self.volume_surge_ratio:
                    score += min(surge, 20.0) * 3
                    reasons.append(f"volume {surge:.1f}x baseline")

            # A pair inside its grace window is watched unconditionally: the
            # abnormal move on a fresh listing usually arrives minutes-to-hours
            # after it starts trading, so subscribing only once it is already
            # spiking misses the entire run-up.
            if (
                self.track_new_listings
                and not is_new
                and age_h is not None
                and age_h <= self.new_listing_grace_h
                and (
                    abs(change) >= self.new_listing_min_change_pct
                    or volume >= self.min_volume_usd
                )
            ):
                score += 25
                reasons.append(f"new listing ({age_h:.1f}h old)")

            if is_new:
                # Newly listed pairs are the highest-risk category — no price
                # history, tiny float, the usual venue for a rug — but "new"
                # alone is not an anomaly. A first appearance in the ticker feed
                # can also mean a dormant pair simply traded again, so require
                # genuine activity before spending a WebSocket slot on it.
                active = (
                    abs(change) >= self.new_listing_min_change_pct
                    or volume >= self.min_volume_usd
                )
                if active:
                    score += 60
                    reasons.append("newly listed")

            if reasons:
                candidates.append(
                    DiscoveredSymbol(
                        symbol=symbol,
                        venue=self.venue,
                        quote_volume_usd=volume,
                        change_pct=change,
                        reason=", ".join(reasons),
                        score=score,
                    )
                )

        self._known_symbols |= seen_now
        self._first_scan = False

        candidates.sort(key=lambda c: -c.score)

        # One slot per base asset. BTC/USDT and BTC/USDC are the same market for
        # surveillance purposes, and letting both through would halve the
        # effective breadth of the watchlist. Keep whichever has more volume;
        # ties break on the `quotes` preference order.
        best_per_base: dict[str, DiscoveredSymbol] = {}
        for cand in candidates:
            base = cand.symbol.split("/")[0]
            incumbent = best_per_base.get(base)
            if incumbent is None or cand.quote_volume_usd > incumbent.quote_volume_usd:
                best_per_base[base] = cand
        deduped = sorted(best_per_base.values(), key=lambda c: -c.score)
        selected = deduped[: self.max_symbols]

        # Pinned symbols are always watched, even when quiet — they are the
        # reference series the operator explicitly asked for.
        pinned = [s for s in self.always_include if s in tickers]
        chosen = list(dict.fromkeys(pinned + [c.symbol for c in selected]))

        if selected:
            log.info(
                "%s discovery: %d candidate(s) from %d pairs — top: %s",
                self.venue, len(selected), len(seen_now),
                "; ".join(str(c) for c in selected[:3]),
            )
        return [c for c in selected if c.symbol in chosen]

    def watchlist(self, tickers: dict[str, dict[str, Any]]) -> list[str]:
        """Convenience: pinned symbols plus the ranked discoveries."""
        found = self.evaluate(tickers)
        pinned = [s for s in self.always_include if s in tickers]
        return list(dict.fromkeys(pinned + [c.symbol for c in found]))

    def stats(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "scans": self.scans,
            "universe": len(self._known_symbols),
            "tracked_baselines": len(self._volume_history),
            "last_scan_s_ago": round(time.time() - self.last_scan_ts, 1)
            if self.last_scan_ts
            else None,
        }
