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
    quote: str = "USDT"

    # symbol -> rolling median 24h volume, for surge detection
    _volume_history: dict[str, list[float]] = field(default_factory=dict, repr=False)
    _known_symbols: set[str] = field(default_factory=set, repr=False)
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
        self.last_scan_ts = time.time()
        candidates: list[DiscoveredSymbol] = []
        seen_now: set[str] = set()

        for symbol, data in tickers.items():
            if not symbol.endswith(f"/{self.quote}"):
                continue
            volume = float(data.get("quoteVolume") or 0.0)
            if volume <= 0:
                continue
            seen_now.add(symbol)

            change = data.get("percentage")
            change = float(change) if change is not None else 0.0
            baseline = self._volume_baseline(symbol)
            self._record_volume(symbol, volume)

            # Liquidity band: too thin is noise, too deep is not cheaply moved.
            if volume < self.min_volume_usd or volume > self.max_volume_usd:
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

            if not self._first_scan and symbol not in self._known_symbols:
                # Newly listed pairs are the highest-risk category by far: no
                # price history, tiny float, and the usual venue for a rug. This
                # weight deliberately exceeds the maximum any single price or
                # volume criterion can contribute, so a new listing always
                # surfaces even when established pairs are moving harder.
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
        selected = candidates[: self.max_symbols]

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
