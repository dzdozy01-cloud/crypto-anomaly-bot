"""Training-data generation and model bootstrapping.

Cold-start problem: an Isolation Forest needs a baseline distribution before it
can call anything anomalous, but a freshly-deployed bot has no history. We solve
it by generating a physically-plausible synthetic corpus whose *normal* regime
matches the statistics real feeds produce, then let online retraining replace it
with live data as it accumulates.

The generator is also what the test-suite uses to assert detection quality:
labelled anomalies let us measure precision/recall rather than eyeballing.

.. warning::
   **This benchmark is circular and must not be cited as accuracy.**

   ``_apply_scenario`` below and ``RuleEngine`` in :mod:`~cadb.modules.ml.classifier`
   were written by the same author against the same feature names and thresholds.
   The pump scenario emits ``volume_z ∈ [3.5, 12]`` precisely because the rule
   fires above 3.0. Scoring well here demonstrates internal consistency, nothing
   more.

   Measured: four OR'd numpy thresholds reach F1 ≈ 0.92 on this set against the
   full 20-feature pipeline's ≈ 0.95. When a trivial baseline nearly matches the
   system, the benchmark is measuring the generator.

   Legitimate uses: regression testing during refactors (it has caught several
   real defects), cold-start for the IsolationForest, and making domain
   assumptions explicit. It is **not** external validation. To validate properly,
   record live telemetry with :class:`~cadb.backtest.EventRecorder`, label known
   incidents (CFTC/SEC cases, exchange delistings, documented rug-pulls), and
   replay with ``cadb backtest``.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass

import numpy as np

from .features import FEATURE_NAMES

log = logging.getLogger(__name__)

__all__ = ["generate_training_data", "generate_labelled_set", "SyntheticSample"]


@dataclass
class SyntheticSample:
    values: list[float]
    label: int          # 0 = normal, 1 = manipulation
    scenario: str


def _normal_sample(rng: random.Random) -> list[float]:
    """A quiet-market state vector."""
    return [
        rng.gauss(0, 0.9),                        # volume_z
        max(0.05, rng.lognormvariate(0, 0.35)),   # volume_spike_ratio
        rng.gauss(0, 0.12),                       # obi
        abs(rng.gauss(0, 0.12)),                  # obi_abs
        rng.gauss(0, 0.9),                        # obi_z
        abs(rng.gauss(1.2, 0.5)),                 # spread_bps (log1p scale)
        rng.gauss(0, 0.9),                        # cvd_z
        rng.gauss(0, 0.15),                       # cvd_divergence
        abs(rng.gauss(0.08, 0.07)),               # venue_dispersion
        abs(rng.gauss(0.1, 0.15)),                # wall_activity
        rng.gauss(0, 0.7) if rng.random() < 0.25 else 0.0,   # whale_inflow_z
        rng.gauss(0, 0.12),                       # net_flow_norm
        0.0 if rng.random() < 0.97 else rng.uniform(0, 0.12),  # liquidity_drop
        abs(rng.gauss(0.05, 0.08)),               # bridge_activity
        0.0,                                      # bridge_to_cex
        rng.gauss(0, 0.9),                        # mention_z
        rng.gauss(0, 0.15),                       # mention_accel
        rng.gauss(0.05, 0.25),                    # sentiment
        abs(rng.gauss(0.05, 0.25)),               # sentiment_abs
        max(0.0, rng.gauss(0.08, 0.09)),          # bot_farm_score
    ]


def _apply_scenario(v: list[float], scenario: str, rng: random.Random) -> list[float]:
    """Overlay a manipulation archetype onto a base vector."""
    idx = {n: i for i, n in enumerate(FEATURE_NAMES)}

    if scenario == "pump_and_dump":
        v[idx["volume_z"]] = rng.uniform(3.5, 12.0)
        v[idx["volume_spike_ratio"]] = rng.uniform(4, 25)
        v[idx["obi"]] = rng.uniform(0.35, 0.85)
        v[idx["obi_abs"]] = abs(v[idx["obi"]])
        v[idx["obi_z"]] = rng.uniform(2.5, 7)
        v[idx["cvd_z"]] = rng.uniform(2.5, 8)
        v[idx["cvd_divergence"]] = rng.uniform(-0.85, -0.35)
        v[idx["mention_z"]] = rng.uniform(3.5, 11)
        v[idx["mention_accel"]] = rng.uniform(0.5, 0.98)
        v[idx["sentiment"]] = rng.uniform(0.45, 0.95)
        v[idx["sentiment_abs"]] = abs(v[idx["sentiment"]])
        v[idx["bot_farm_score"]] = rng.uniform(0.55, 0.97)
        v[idx["wall_activity"]] = rng.uniform(0.8, 2.5)

    elif scenario == "rug_pull":
        v[idx["liquidity_drop"]] = rng.uniform(0.35, 0.98)
        v[idx["net_flow_norm"]] = rng.uniform(0.35, 0.95)
        v[idx["whale_inflow_z"]] = rng.uniform(2.5, 9)
        v[idx["volume_z"]] = rng.uniform(2.5, 9)
        v[idx["sentiment"]] = rng.uniform(-0.95, -0.35)
        v[idx["sentiment_abs"]] = abs(v[idx["sentiment"]])
        v[idx["mention_z"]] = rng.uniform(2.5, 8)
        v[idx["spread_bps"]] = rng.uniform(2.5, 6)
        v[idx["obi"]] = rng.uniform(-0.9, -0.4)
        v[idx["obi_abs"]] = abs(v[idx["obi"]])

    elif scenario == "spoofing":
        v[idx["obi"]] = rng.choice([1, -1]) * rng.uniform(0.55, 0.95)
        v[idx["obi_abs"]] = abs(v[idx["obi"]])
        v[idx["obi_z"]] = rng.choice([1, -1]) * rng.uniform(3, 9)
        v[idx["wall_activity"]] = rng.uniform(1.2, 3.5)
        v[idx["cvd_divergence"]] = rng.choice([1, -1]) * rng.uniform(0.35, 0.9)
        v[idx["venue_dispersion"]] = rng.uniform(0.5, 1.4)
        v[idx["volume_z"]] = rng.uniform(1.0, 4.0)

    elif scenario == "wash_trading":
        v[idx["volume_z"]] = rng.uniform(3.0, 10.0)
        v[idx["volume_spike_ratio"]] = rng.uniform(5, 30)
        v[idx["cvd_z"]] = rng.gauss(0, 0.5)          # buys ≈ sells: the tell
        v[idx["cvd_divergence"]] = rng.gauss(0, 0.12)
        v[idx["obi_abs"]] = abs(rng.gauss(0.08, 0.06))
        v[idx["obi"]] = rng.gauss(0, 0.08)
        v[idx["spread_bps"]] = rng.uniform(0.1, 0.8)  # unusually tight
        v[idx["mention_z"]] = rng.gauss(0.5, 1.0)

    elif scenario == "whale_distribution":
        v[idx["whale_inflow_z"]] = rng.uniform(3, 10)
        v[idx["net_flow_norm"]] = rng.uniform(0.4, 0.98)
        v[idx["bridge_activity"]] = rng.uniform(0.35, 0.95)
        v[idx["bridge_to_cex"]] = rng.uniform(0.35, 1.0)
        v[idx["mention_z"]] = rng.uniform(2.5, 7)
        v[idx["sentiment"]] = rng.uniform(0.4, 0.9)
        v[idx["sentiment_abs"]] = abs(v[idx["sentiment"]])
        v[idx["volume_z"]] = rng.uniform(1.5, 5)
        v[idx["cvd_divergence"]] = rng.uniform(0.3, 0.8)

    elif scenario == "social_shill":
        v[idx["mention_z"]] = rng.uniform(4, 14)
        v[idx["mention_accel"]] = rng.uniform(0.6, 1.0)
        v[idx["bot_farm_score"]] = rng.uniform(0.6, 0.99)
        v[idx["sentiment"]] = rng.uniform(0.5, 0.95)
        v[idx["sentiment_abs"]] = abs(v[idx["sentiment"]])
        v[idx["volume_z"]] = rng.uniform(1.5, 5)

    elif scenario == "benign_news":
        # Deliberate hard negative: real volume + real interest, no manipulation.
        v[idx["volume_z"]] = rng.uniform(2.5, 6.0)
        v[idx["volume_spike_ratio"]] = rng.uniform(3, 10)
        v[idx["mention_z"]] = rng.uniform(2.5, 6.0)
        v[idx["mention_accel"]] = rng.uniform(0.3, 0.7)
        v[idx["sentiment"]] = rng.uniform(0.25, 0.6)
        v[idx["sentiment_abs"]] = abs(v[idx["sentiment"]])
        v[idx["cvd_z"]] = rng.uniform(1.5, 3.5)
        v[idx["bot_farm_score"]] = rng.uniform(0.0, 0.22)   # organic crowd
        v[idx["obi_abs"]] = abs(rng.gauss(0.15, 0.1))
        v[idx["obi"]] = rng.gauss(0.1, 0.15)

    return [float(np.clip(x, -20, 50)) for x in v]


SCENARIOS = (
    "pump_and_dump",
    "rug_pull",
    "spoofing",
    "wash_trading",
    "whale_distribution",
    "social_shill",
)


def generate_training_data(
    n_samples: int = 5000, anomaly_rate: float = 0.02, seed: int = 42
) -> np.ndarray:
    """Unlabelled corpus for unsupervised IsolationForest fitting.

    Includes a realistic minority of anomalies (Isolation Forest is trained on
    contaminated data by design) plus benign-but-unusual states so the model
    learns that "loud" does not automatically mean "manipulated".
    """
    rng = random.Random(seed)
    rows: list[list[float]] = []
    for _ in range(n_samples):
        v = _normal_sample(rng)
        roll = rng.random()
        if roll < anomaly_rate:
            v = _apply_scenario(v, rng.choice(SCENARIOS), rng)
        elif roll < anomaly_rate + 0.04:
            v = _apply_scenario(v, "benign_news", rng)
        rows.append(v)
    arr = np.asarray(rows, dtype=np.float64)
    log.info("generated %d training samples (%d features)", arr.shape[0], arr.shape[1])
    return arr


def generate_labelled_set(
    n_normal: int = 1000, n_per_scenario: int = 120, seed: int = 7
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Labelled evaluation set -> (X, y, scenario_names)."""
    rng = random.Random(seed)
    X: list[list[float]] = []
    y: list[int] = []
    names: list[str] = []

    for _ in range(n_normal):
        X.append(_normal_sample(rng))
        y.append(0)
        names.append("normal")

    for _ in range(max(1, n_normal // 12)):
        X.append(_apply_scenario(_normal_sample(rng), "benign_news", rng))
        y.append(0)
        names.append("benign_news")

    for scenario in SCENARIOS:
        for _ in range(n_per_scenario):
            X.append(_apply_scenario(_normal_sample(rng), scenario, rng))
            y.append(1)
            names.append(scenario)

    return np.asarray(X, dtype=np.float64), np.asarray(y, dtype=np.int64), names


def bootstrap_model(
    classifier: object, n_samples: int = 5000, seed: int = 42
) -> bool:  # pragma: no cover - thin wrapper
    """Fit a classifier on synthetic data (cold-start)."""
    data = generate_training_data(n_samples=n_samples, seed=seed)
    return bool(classifier.fit(data))  # type: ignore[attr-defined]
