"""Multi-feature state vector assembly.

Aggregates the live telemetry from Modules 1-3 into one fixed-order numeric
vector per asset. Fixed ordering matters: the Isolation Forest is trained on
positional features, so :data:`FEATURE_NAMES` is the contract between training
and inference and must never be reordered without retraining.

Staleness handling is explicit — a whale transfer from 20 minutes ago should not
keep inflating the current vector, so every feature decays toward its neutral
value based on the age of the observation that produced it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from ...core.schema import MarketEvent, MetricType, now_ms
from ...core.stats import clamp

__all__ = ["FEATURE_NAMES", "FeatureVector", "FeatureStore", "AssetFeatures"]

# ---- Feature contract (ORDER IS LOAD-BEARING) ------------------------------
FEATURE_NAMES: tuple[str, ...] = (
    # Module 1 — exchange microstructure (8)
    "volume_z",             # rolling 5m volume z-score
    "volume_spike_ratio",   # current bucket / mean
    "obi",                  # order book imbalance [-1, 1]
    "obi_abs",              # |OBI| — direction-agnostic lopsidedness
    "obi_z",                # z-score of OBI
    "spread_bps",           # log1p-scaled spread
    "cvd_z",                # z-score of windowed CVD
    "cvd_divergence",       # CVD vs price divergence [-1, 1]
    # Module 1 — cross-venue (2)
    "venue_dispersion",     # disagreement of OBI across venues
    "wall_activity",        # spoof / absorption wall events
    # Module 2 — on-chain (5)
    "whale_inflow_z",       # z-score of CEX inflow notional
    "net_flow_norm",        # tanh-scaled net exchange flow (+ = deposits)
    "liquidity_drop",       # worst recent pool drop, 0..1
    "bridge_activity",      # tanh-scaled bridge stablecoin volume
    "bridge_to_cex",        # bridge inflow followed by CEX deposit (0/1-ish)
    # Module 3 — social (5)
    "mention_z",            # mention-rate z-score
    "mention_accel",        # tanh-scaled mention acceleration
    "sentiment",            # -1..+1
    "sentiment_abs",        # |sentiment| — extremity regardless of direction
    "bot_farm_score",       # 0..1 coordinated-behaviour confidence
)

N_FEATURES = len(FEATURE_NAMES)

# Neutral (no-information) value each feature decays back to when stale.
_NEUTRAL: dict[str, float] = dict.fromkeys(FEATURE_NAMES, 0.0)
_NEUTRAL["volume_spike_ratio"] = 1.0


@dataclass
class FeatureVector:
    """A materialised state vector with provenance."""

    asset: str
    timestamp: int
    values: list[float]
    sources_fresh: dict[str, bool] = field(default_factory=dict)
    coverage: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return dict(zip(FEATURE_NAMES, self.values))

    def get(self, name: str) -> float:
        try:
            return self.values[FEATURE_NAMES.index(name)]
        except (ValueError, IndexError):
            return 0.0

    @property
    def is_informative(self) -> bool:
        """At least one module contributing and some non-neutral content."""
        return self.coverage > 0.0 and any(
            abs(v - _NEUTRAL[n]) > 1e-9 for n, v in zip(FEATURE_NAMES, self.values)
        )


@dataclass
class _Observation:
    value: float
    timestamp: int
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class AssetFeatures:
    """Mutable per-asset accumulator fed by bus events."""

    asset: str
    ttl_s: int = 600

    # metric_key -> observation (per venue where relevant)
    exchange: dict[str, dict[str, _Observation]] = field(default_factory=dict)
    onchain: dict[str, _Observation] = field(default_factory=dict)
    social: dict[str, _Observation] = field(default_factory=dict)
    last_update: int = 0
    updates: int = 0

    # rolling on-chain accumulators
    inflow_usd_recent: float = 0.0
    outflow_usd_recent: float = 0.0
    bridge_usd_recent: float = 0.0
    bridge_to_cex_hits: int = 0

    def _decay(self, obs: _Observation | None, now: int, half_life_s: float = 120.0) -> float:
        """Exponentially decay a value toward zero based on observation age."""
        if obs is None:
            return 0.0
        age_s = max(0.0, (now - obs.timestamp) / 1000.0)
        if age_s > self.ttl_s:
            return 0.0
        return obs.value * math.pow(0.5, age_s / max(half_life_s, 1e-6))

    # ---- ingestion -------------------------------------------------------
    def update(self, event: MarketEvent) -> None:
        self.updates += 1
        self.last_update = max(self.last_update, event.timestamp)
        mt = event.metric_type
        z = event.normalized_z_score

        if mt in (MetricType.VOLUME, MetricType.ORDER_BOOK, MetricType.CVD):
            per_venue = self.exchange.setdefault(mt.value, {})
            per_venue[event.venue] = _Observation(
                value=event.raw_value, timestamp=event.timestamp,
                meta={**event.meta, "z": z},
            )
        elif mt is MetricType.WALLET_TRANSFER:
            usd = event.usd_value or 0.0
            direction = event.meta.get("direction", "")
            if direction == "inflow":
                self.inflow_usd_recent += usd
            elif direction == "outflow":
                self.outflow_usd_recent += usd
            if event.meta.get("bridge_precursor"):
                self.bridge_to_cex_hits += 1
            self.onchain["wallet_transfer"] = _Observation(usd, event.timestamp,
                                                           {**event.meta, "z": z})
        elif mt is MetricType.LIQUIDITY:
            self.onchain["liquidity"] = _Observation(
                abs(event.raw_value), event.timestamp, {**event.meta, "z": z}
            )
        elif mt is MetricType.BRIDGE_FLOW:
            self.bridge_usd_recent += event.usd_value or 0.0
            self.onchain["bridge"] = _Observation(event.usd_value or 0.0, event.timestamp,
                                                  event.meta)
        elif mt is MetricType.SOCIAL_MENTIONS:
            self.social["mentions"] = _Observation(event.raw_value, event.timestamp,
                                                   {**event.meta, "z": z})
        elif mt is MetricType.SOCIAL_SENTIMENT:
            self.social["sentiment"] = _Observation(event.raw_value, event.timestamp,
                                                    {**event.meta, "z": z})
        elif mt is MetricType.BOT_FARM:
            self.social["bot_farm"] = _Observation(event.raw_value, event.timestamp, event.meta)

    def decay_accumulators(self, factor: float = 0.85) -> None:
        """Bleed off the rolling USD accumulators each scoring cycle."""
        self.inflow_usd_recent *= factor
        self.outflow_usd_recent *= factor
        self.bridge_usd_recent *= factor
        if self.bridge_to_cex_hits and factor < 1.0:
            self.bridge_to_cex_hits = max(0, self.bridge_to_cex_hits - 1)

    # ---- vector construction ---------------------------------------------
    def _agg_exchange(self, metric: str, now: int, key: str = "z") -> tuple[float, float, int]:
        """Aggregate a per-venue metric -> (max_abs_signed, dispersion, n_fresh).

        Dispersion is computed on *undecayed* values from venues observed within
        a tight recency window. Comparing decayed values would manufacture
        disagreement out of nothing more than one venue having reported less
        recently than another.
        """
        per_venue = self.exchange.get(metric, {})
        vals: list[float] = []
        concurrent: list[float] = []
        for obs in per_venue.values():
            age_s = (now - obs.timestamp) / 1000.0
            if age_s > self.ttl_s:
                continue
            raw = obs.meta.get(key) if key != "raw" else obs.value
            if raw is None:
                continue
            value = float(raw)
            decay = math.pow(0.5, age_s / 120.0)
            vals.append(value * decay)
            if age_s <= 5.0:
                concurrent.append(value)
        if not vals:
            return 0.0, 0.0, 0
        peak = max(vals, key=abs)
        dispersion = (max(concurrent) - min(concurrent)) if len(concurrent) > 1 else 0.0
        return peak, dispersion, len(vals)

    def build(self, now: int | None = None) -> FeatureVector:
        now = now or now_ms()
        f: dict[str, float] = dict(_NEUTRAL)

        # --- Module 1 ---
        vol_z, _, n_vol = self._agg_exchange("volume", now)
        f["volume_z"] = clamp(vol_z, -20, 20)
        spike = 1.0
        for obs in self.exchange.get("volume", {}).values():
            if (now - obs.timestamp) / 1000.0 <= self.ttl_s:
                spike = max(spike, float(obs.meta.get("spike_ratio", 1.0)))
        f["volume_spike_ratio"] = clamp(spike, 0.0, 50.0)

        obi_peak, obi_disp, n_obi = self._agg_exchange("order_book", now, key="raw")
        f["obi"] = clamp(obi_peak, -1, 1)
        f["obi_abs"] = abs(f["obi"])
        obi_z, _, _ = self._agg_exchange("order_book", now, key="z")
        f["obi_z"] = clamp(obi_z, -20, 20)
        f["venue_dispersion"] = clamp(obi_disp, 0.0, 2.0)

        spread = 0.0
        walls = 0.0
        for obs in self.exchange.get("order_book", {}).values():
            if (now - obs.timestamp) / 1000.0 > self.ttl_s:
                continue
            spread = max(spread, float(obs.meta.get("spread_bps", 0.0)))
            walls += float(obs.meta.get("spoofed_walls", 0)) + float(
                obs.meta.get("absorbed_walls", 0)
            )
        f["spread_bps"] = clamp(math.log1p(max(spread, 0.0)), 0.0, 10.0)
        f["wall_activity"] = clamp(math.log1p(walls), 0.0, 5.0)

        cvd_z, _, n_cvd = self._agg_exchange("cvd", now, key="z")
        f["cvd_z"] = clamp(cvd_z, -20, 20)
        div = 0.0
        for obs in self.exchange.get("cvd", {}).values():
            if (now - obs.timestamp) / 1000.0 <= self.ttl_s:
                d = float(obs.meta.get("divergence", 0.0))
                if abs(d) > abs(div):
                    div = d
        f["cvd_divergence"] = clamp(div, -1, 1)

        # --- Module 2 ---
        wt = self.onchain.get("wallet_transfer")
        if wt and (now - wt.timestamp) / 1000.0 <= self.ttl_s:
            f["whale_inflow_z"] = clamp(float(wt.meta.get("z") or 0.0), -20, 20)
        net = self.inflow_usd_recent - self.outflow_usd_recent
        f["net_flow_norm"] = math.tanh(net / 5_000_000.0)
        liq = self.onchain.get("liquidity")
        if liq and (now - liq.timestamp) / 1000.0 <= self.ttl_s:
            f["liquidity_drop"] = clamp(liq.value / 100.0, 0.0, 1.0)
        f["bridge_activity"] = math.tanh(self.bridge_usd_recent / 20_000_000.0)
        f["bridge_to_cex"] = clamp(self.bridge_to_cex_hits / 3.0, 0.0, 1.0)

        # --- Module 3 ---
        men = self.social.get("mentions")
        if men and (now - men.timestamp) / 1000.0 <= self.ttl_s:
            f["mention_z"] = clamp(float(men.meta.get("z") or 0.0), -20, 20)
            f["mention_accel"] = math.tanh(float(men.meta.get("acceleration", 0.0)) / 50.0)
        sen = self.social.get("sentiment")
        if sen and (now - sen.timestamp) / 1000.0 <= self.ttl_s:
            f["sentiment"] = clamp(sen.value, -1, 1)
            f["sentiment_abs"] = abs(f["sentiment"])
        bot = self.social.get("bot_farm")
        if bot and (now - bot.timestamp) / 1000.0 <= self.ttl_s:
            f["bot_farm_score"] = clamp(bot.value, 0.0, 1.0)

        fresh = {
            "exchange": bool(n_vol or n_obi or n_cvd),
            "onchain": bool(
                (wt and (now - wt.timestamp) / 1000 <= self.ttl_s)
                or (liq and (now - liq.timestamp) / 1000 <= self.ttl_s)
                or self.bridge_usd_recent > 0
            ),
            "social": bool(men and (now - men.timestamp) / 1000 <= self.ttl_s),
        }
        return FeatureVector(
            asset=self.asset,
            timestamp=now,
            values=[f[name] for name in FEATURE_NAMES],
            sources_fresh=fresh,
            coverage=sum(fresh.values()) / 3.0,
        )


class FeatureStore:
    """Registry of per-asset feature accumulators."""

    def __init__(self, ttl_s: int = 600) -> None:
        self.ttl_s = ttl_s
        self.assets: dict[str, AssetFeatures] = {}

    def ingest(self, event: MarketEvent) -> str:
        """Route an event to its asset bucket. Returns the asset key."""
        asset = event.base_asset
        store = self.assets.get(asset)
        if store is None:
            store = AssetFeatures(asset=asset, ttl_s=self.ttl_s)
            self.assets[asset] = store
        store.update(event)
        return asset

    def build(self, asset: str, now: int | None = None) -> FeatureVector | None:
        store = self.assets.get(asset)
        return store.build(now) if store else None

    def build_all(self, now: int | None = None) -> list[FeatureVector]:
        now = now or now_ms()
        return [s.build(now) for s in self.assets.values()]

    def decay(self, factor: float = 0.85) -> None:
        for store in self.assets.values():
            store.decay_accumulators(factor)

    def prune(self, now: int | None = None, max_idle_s: int = 3600) -> int:
        now = now or now_ms()
        stale = [
            a for a, s in self.assets.items()
            if s.last_update and (now - s.last_update) / 1000.0 > max_idle_s
        ]
        for a in stale:
            del self.assets[a]
        return len(stale)
