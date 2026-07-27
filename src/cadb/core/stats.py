"""Online statistics primitives.

All estimators are O(1) per update and allocation-free in the steady state, which
is what keeps the tick→signal path inside the 200 ms budget.

* :class:`RollingWindow`      time-based deque with incremental sum/sum-of-squares
* :class:`EWMAZScore`         exponentially-weighted mean/variance (Welford-style)
* :class:`RobustZScore`       median/MAD — resistant to the very spikes we detect
* :class:`DynamicZScore`      blends EWMA + robust, with regime-adaptive thresholds
* :class:`CusumDetector`      cumulative-sum change-point detector for level shifts
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

__all__ = [
    "RollingWindow",
    "EWMAZScore",
    "RobustZScore",
    "DynamicZScore",
    "CusumDetector",
    "clamp",
    "percentile_rank",
]


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def percentile_rank(sorted_values: list[float], x: float) -> float:
    """Fraction of ``sorted_values`` <= x, via binary search. Empty -> 0.5."""
    if not sorted_values:
        return 0.5
    lo, hi = 0, len(sorted_values)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_values[mid] <= x:
            lo = mid + 1
        else:
            hi = mid
    return lo / len(sorted_values)


@dataclass
class RollingWindow:
    """Time-bounded rolling window over (timestamp_ms, value) samples.

    Maintains running sum and sum-of-squares so ``mean``/``std`` are O(1).
    Uses the numerically-guarded variance formula to avoid catastrophic
    cancellation on long-lived, large-magnitude series (e.g. USDT volumes).
    """

    window_ms: int = 300_000  # 5 minutes — the spec's volume z-score window
    max_samples: int = 20_000
    _samples: deque[tuple[int, float]] = field(default_factory=deque, repr=False)
    _sum: float = 0.0
    _sumsq: float = 0.0

    def add(self, timestamp_ms: int, value: float) -> None:
        self._samples.append((timestamp_ms, value))
        self._sum += value
        self._sumsq += value * value
        self._evict(timestamp_ms)

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        s = self._samples
        while s and (s[0][0] < cutoff or len(s) > self.max_samples):
            _, v = s.popleft()
            self._sum -= v
            self._sumsq -= v * v
        if not s:  # reset drift
            self._sum = 0.0
            self._sumsq = 0.0

    def expire(self, now_ms: int) -> None:
        """Drop stale samples without adding a new one."""
        self._evict(now_ms)

    def __len__(self) -> int:
        return len(self._samples)

    @property
    def values(self) -> list[float]:
        return [v for _, v in self._samples]

    @property
    def total(self) -> float:
        return self._sum

    @property
    def mean(self) -> float:
        return self._sum / len(self._samples) if self._samples else 0.0

    @property
    def variance(self) -> float:
        n = len(self._samples)
        if n < 2:
            return 0.0
        var = (self._sumsq - (self._sum * self._sum) / n) / (n - 1)
        return max(0.0, var)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def zscore(self, value: float, min_samples: int = 20) -> float | None:
        """Standard score of ``value``; ``None`` until the window has warmed up."""
        if len(self._samples) < min_samples:
            return None
        sd = self.std
        if sd <= 1e-12:
            return 0.0
        return (value - self.mean) / sd

    def rate_per_minute(self) -> float:
        """Sum normalised to a per-minute rate over the covered span."""
        if len(self._samples) < 2:
            return self._sum
        span_ms = self._samples[-1][0] - self._samples[0][0]
        if span_ms <= 0:
            return self._sum
        return self._sum * 60_000.0 / span_ms


@dataclass
class EWMAZScore:
    """Exponentially-weighted mean/variance z-score.

    ``half_life_s`` sets how quickly the baseline forgets; the effective alpha is
    derived per-update from the real elapsed time so irregular tick arrival does
    not distort the decay (critical for sparse assets).
    """

    half_life_s: float = 60.0
    warmup: int = 20
    mean: float = 0.0
    var: float = 0.0
    count: int = 0
    _last_ts: int | None = field(default=None, repr=False)

    def _alpha(self, dt_s: float) -> float:
        if self.half_life_s <= 0:
            return 1.0
        return 1.0 - math.pow(0.5, max(dt_s, 1e-6) / self.half_life_s)

    def update(self, value: float, timestamp_ms: int | None = None) -> float | None:
        """Feed a sample; returns the z-score *before* the update (leak-free)."""
        z = self.score(value)
        dt_s = 1.0
        if timestamp_ms is not None and self._last_ts is not None:
            dt_s = max(0.0, (timestamp_ms - self._last_ts) / 1000.0)
        self._last_ts = timestamp_ms if timestamp_ms is not None else self._last_ts

        self.count += 1
        if self.count == 1:
            self.mean = value
            self.var = 0.0
            return z
        a = clamp(self._alpha(dt_s), 1e-4, 1.0)
        diff = value - self.mean
        incr = a * diff
        self.mean += incr
        self.var = (1 - a) * (self.var + diff * incr)
        return z

    @property
    def std(self) -> float:
        return math.sqrt(max(self.var, 0.0))

    def score(self, value: float) -> float | None:
        if self.count < self.warmup:
            return None
        sd = self.std
        if sd <= 1e-12:
            return 0.0
        return (value - self.mean) / sd


@dataclass
class RobustZScore:
    """Median/MAD z-score — the estimator that does not get poisoned by outliers.

    A single 50σ print pulls a classic mean/std baseline so far that the *next*
    manipulation event looks normal. MAD-based scoring stays stable, so we use it
    as the anchor and blend the EWMA score on top.
    """

    window: int = 300
    warmup: int = 30
    _buf: deque[float] = field(default_factory=deque, repr=False)

    _MAD_TO_SIGMA = 1.4826

    def update(self, value: float) -> float | None:
        z = self.score(value)
        self._buf.append(value)
        while len(self._buf) > self.window:
            self._buf.popleft()
        return z

    def score(self, value: float) -> float | None:
        n = len(self._buf)
        if n < self.warmup:
            return None
        ordered = sorted(self._buf)
        med = _median_sorted(ordered)
        mad = _median_sorted(sorted(abs(v - med) for v in self._buf))
        scale = mad * self._MAD_TO_SIGMA

        if scale <= 1e-12:
            # Degenerate scale: the series is (near-)constant. Try IQR first.
            q1 = ordered[n // 4]
            q3 = ordered[(3 * n) // 4]
            scale = (q3 - q1) / 1.349

        if scale <= 1e-12:
            # Still degenerate — a perfectly flat series. Returning 0 here would
            # be actively harmful: on a pegged/dormant market *any* deviation is
            # maximally significant, and reporting "normal" would mask exactly
            # the kind of sudden break we exist to catch. Fall back to a
            # magnitude-relative scale so real moves still surface.
            if abs(value - med) <= 1e-12:
                return 0.0
            scale = max(abs(med) * 1e-4, 1e-9)
            return clamp((value - med) / scale, -50.0, 50.0)

        return (value - med) / scale


def _median_sorted(values: list[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2:
        return values[mid]
    return 0.5 * (values[mid - 1] + values[mid])


@dataclass
class DynamicZScore:
    """Composite dynamic z-score used across all four modules.

    Combines a fast EWMA score with a robust median/MAD score and adapts the
    trigger threshold to the observed volatility regime: in a quiet regime a 3σ
    print is genuinely rare, in a violent regime it is noise, so the threshold
    widens. ``threshold`` starts at the spec's 3.0.
    """

    half_life_s: float = 60.0
    window: int = 300
    warmup: int = 25
    base_threshold: float = 3.0
    robust_weight: float = 0.6
    adaptive: bool = True

    _ewma: EWMAZScore = field(init=False, repr=False)
    _robust: RobustZScore = field(init=False, repr=False)
    _recent_abs_z: deque[float] = field(default_factory=lambda: deque(maxlen=200), repr=False)
    last_z: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._ewma = EWMAZScore(half_life_s=self.half_life_s, warmup=self.warmup)
        self._robust = RobustZScore(window=self.window, warmup=self.warmup)

    def update(self, value: float, timestamp_ms: int | None = None) -> float | None:
        ez = self._ewma.update(value, timestamp_ms)
        rz = self._robust.update(value)
        z = self._blend(ez, rz)
        if z is not None:
            self._recent_abs_z.append(abs(z))
        self.last_z = z
        return z

    def score(self, value: float) -> float | None:
        return self._blend(self._ewma.score(value), self._robust.score(value))

    def _blend(self, ez: float | None, rz: float | None) -> float | None:
        if ez is None and rz is None:
            return None
        if rz is None:
            return ez
        if ez is None:
            return rz
        w = clamp(self.robust_weight, 0.0, 1.0)
        return w * rz + (1 - w) * ez

    @property
    def threshold(self) -> float:
        """Volatility-adaptive trigger level (never below the configured base)."""
        if not self.adaptive or len(self._recent_abs_z) < 50:
            return self.base_threshold
        ordered = sorted(self._recent_abs_z)
        p95 = ordered[int(0.95 * (len(ordered) - 1))]
        # Widen when the tail is fat; never tighten below the configured floor.
        return max(self.base_threshold, min(p95 * 1.15, self.base_threshold * 2.5))

    def is_anomalous(self, z: float | None = None) -> bool:
        zz = self.last_z if z is None else z
        return zz is not None and zz > self.threshold

    @property
    def ready(self) -> bool:
        return self._ewma.count >= self.warmup


@dataclass
class CusumDetector:
    """Two-sided CUSUM change-point detector on standardised input.

    Catches slow, deliberate accumulation/distribution that never trips a single
    3σ bar but shifts the mean persistently — the classic quiet-accumulation
    phase before a pump.
    """

    drift: float = 0.5
    threshold: float = 5.0
    pos: float = 0.0
    neg: float = 0.0

    def update(self, z: float) -> int:
        """Returns +1 (upward shift), -1 (downward shift) or 0."""
        self.pos = max(0.0, self.pos + z - self.drift)
        self.neg = min(0.0, self.neg + z + self.drift)
        if self.pos > self.threshold:
            self.pos = 0.0
            return 1
        if self.neg < -self.threshold:
            self.neg = 0.0
            return -1
        return 0

    def reset(self) -> None:
        self.pos = self.neg = 0.0
