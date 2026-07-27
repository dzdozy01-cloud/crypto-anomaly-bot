"""Unified telemetry schema shared by every ingestion module.

Every collector in the system — exchange, on-chain, social — normalises its
observations into exactly one wire format, :class:`MarketEvent`. That contract is
what lets the ML layer treat an order-book imbalance, a whale transfer and a
Twitter mention burst as columns of the same feature vector.
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "MetricType",
    "SourceType",
    "Severity",
    "MarketEvent",
    "AnomalySignal",
    "now_ms",
    "monotonic_ns",
]


def now_ms() -> int:
    """Wall-clock epoch milliseconds (used for cross-process ordering)."""
    return time.time_ns() // 1_000_000


def monotonic_ns() -> int:
    """Monotonic nanoseconds — the only safe source for latency deltas."""
    return time.perf_counter_ns()


class SourceType(str, Enum):
    """Which of the four subsystems produced an event."""

    EXCHANGE = "exchange"
    ONCHAIN = "onchain"
    SOCIAL = "social"
    DERIVED = "derived"


class MetricType(str, Enum):
    """Normalised metric taxonomy.

    The four canonical types required by the data contract are ``volume``,
    ``order_book``, ``wallet_transfer`` and ``social_mentions``. The remaining
    members are refinements that stay inside the same taxonomy so downstream
    consumers can subscribe coarsely (``metric_type.startswith``) or precisely.
    """

    # --- Module 1: exchange microstructure ---
    VOLUME = "volume"
    ORDER_BOOK = "order_book"
    CVD = "cvd"
    TRADE = "trade"
    PRICE = "price"

    # --- Module 2: on-chain ---
    WALLET_TRANSFER = "wallet_transfer"
    LIQUIDITY = "liquidity"
    BRIDGE_FLOW = "bridge_flow"

    # --- Module 3: social ---
    SOCIAL_MENTIONS = "social_mentions"
    SOCIAL_SENTIMENT = "social_sentiment"
    BOT_FARM = "bot_farm"

    # --- Module 4: derived ---
    MANIPULATION_SCORE = "manipulation_score"


class Severity(str, Enum):
    """Alert routing priority."""

    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}[self.value]


class MarketEvent(BaseModel):
    """The single normalised payload published on the ingestion bus.

    Attributes map 1:1 to the unified data schema:

    ``timestamp``           epoch milliseconds the observation refers to
    ``venue``               exchange id (``binance``) or chain id (``ethereum``)
    ``asset_pair``          canonical symbol, e.g. ``BTC/USDT`` or ``PEPE``
    ``metric_type``         see :class:`MetricType`
    ``raw_value``           the measurement in native units
    ``normalized_z_score``  rolling standardised deviation, ``None`` while warming up
    """

    model_config = ConfigDict(frozen=True, extra="forbid", use_enum_values=False)

    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:16])
    timestamp: int = Field(default_factory=now_ms, description="epoch ms of observation")
    source_type: SourceType
    venue: str = Field(description="exchange id or chain id")
    asset_pair: str = Field(description="canonical asset or pair symbol")
    metric_type: MetricType
    raw_value: float
    normalized_z_score: float | None = None

    # --- optional enrichment ---
    usd_value: float | None = Field(default=None, description="USD notional when known")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    meta: dict[str, Any] = Field(default_factory=dict)

    # --- pipeline instrumentation ---
    ingest_ns: int = Field(default_factory=monotonic_ns, repr=False)

    @field_validator("asset_pair")
    @classmethod
    def _normalise_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("venue")
    @classmethod
    def _normalise_venue(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("raw_value")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("raw_value must be finite")
        return float(v)

    @property
    def base_asset(self) -> str:
        """``BTC/USDT`` -> ``BTC``; bare tickers pass through unchanged."""
        return self.asset_pair.split("/")[0].split(":")[0]

    @property
    def channel(self) -> str:
        """Pub/Sub topic this event belongs on."""
        return f"cadb.{self.source_type.value}.{self.metric_type.value}"

    def age_ms(self) -> float:
        """Milliseconds elapsed since the observation timestamp."""
        return max(0.0, now_ms() - self.timestamp)

    def elapsed_ms(self) -> float:
        """Monotonic milliseconds since this event object was constructed."""
        return (monotonic_ns() - self.ingest_ns) / 1e6

    def to_wire(self) -> dict[str, Any]:
        """Serialise for the bus (enums flattened to strings)."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "source_type": self.source_type.value,
            "venue": self.venue,
            "asset_pair": self.asset_pair,
            "metric_type": self.metric_type.value,
            "raw_value": self.raw_value,
            "normalized_z_score": self.normalized_z_score,
            "usd_value": self.usd_value,
            "confidence": self.confidence,
            "meta": self.meta,
            "ingest_ns": self.ingest_ns,
        }

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> MarketEvent:
        return cls.model_validate(payload)


class AnomalySignal(BaseModel):
    """Module 4 output: a scored, explainable manipulation verdict."""

    model_config = ConfigDict(frozen=True)

    signal_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: int = Field(default_factory=now_ms)
    asset_pair: str
    venue: str = "aggregate"
    score: float = Field(ge=0.0, le=100.0, description="composite manipulation score")
    severity: Severity = Severity.INFO
    ml_score: float = Field(default=0.0, ge=0.0, le=100.0)
    rule_score: float = Field(default=0.0, ge=0.0, le=100.0)
    contributions: dict[str, float] = Field(default_factory=dict)
    features: dict[str, float] = Field(default_factory=dict)
    reasons: list[str] = Field(default_factory=list)
    latency_ms: float = 0.0

    @property
    def is_actionable(self) -> bool:
        return self.severity.rank >= Severity.HIGH.rank

    def to_event(self) -> MarketEvent:
        """Republish the verdict on the bus as a normal telemetry event."""
        return MarketEvent(
            timestamp=self.timestamp,
            source_type=SourceType.DERIVED,
            venue=self.venue,
            asset_pair=self.asset_pair,
            metric_type=MetricType.MANIPULATION_SCORE,
            raw_value=self.score,
            normalized_z_score=self.features.get("composite_z"),
            meta={
                "signal_id": self.signal_id,
                "severity": self.severity.value,
                "reasons": self.reasons,
                "contributions": self.contributions,
                "ml_score": self.ml_score,
                "rule_score": self.rule_score,
            },
        )
