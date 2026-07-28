"""Isolation Forest anomaly classifier + composite Manipulation Score.

Design notes
------------
*Why a hybrid?* A pure Isolation Forest tells you "this vector is unusual" but
not "this is manipulation" — unusual-but-benign states (listings, macro news)
score identically to a wash-trading ramp. A pure rule engine is explainable but
brittle. We therefore compute both and blend them, so the ML half catches novel
multi-dimensional patterns while the rule half keeps the output interpretable
and anchored to the domain thresholds in the spec.

*Score calibration.* ``IsolationForest.score_samples`` is unbounded and
distribution-dependent, so raw values are useless as a 0-100 scale. We calibrate
against the training distribution's own score quantiles, which makes the output
stable across retrains and datasets.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from ...core.schema import AnomalySignal, Severity, now_ms
from ...core.stats import clamp
from .features import FEATURE_NAMES, N_FEATURES, FeatureVector

log = logging.getLogger(__name__)

__all__ = ["ManipulationClassifier", "RuleEngine", "ScoreBreakdown"]


@dataclass
class ScoreBreakdown:
    """Explainable decomposition of a composite score."""

    composite: float
    ml_component: float
    rule_component: float
    contributions: dict[str, float] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    severity: Severity = Severity.INFO


class RuleEngine:
    """Domain rules producing an interpretable 0-100 sub-score.

    Each rule maps a feature (or feature combination) to points. Combination
    rules carry the most weight because *co-occurrence* across independent data
    sources is the strongest manipulation evidence: a volume spike alone is
    noise, a volume spike + spoofed book + bot-farm chatter is a pump.
    """

    def __init__(self, weights: dict[str, float] | None = None) -> None:
        self.weights = weights or {"exchange": 0.40, "onchain": 0.35, "social": 0.25}

    def evaluate(self, fv: FeatureVector) -> tuple[float, dict[str, float], list[str]]:
        f = fv.as_dict()
        reasons: list[str] = []

        # --- Module 1: structural order book / flow (0-100 sub-score) ---
        ex = 0.0
        vz = f["volume_z"]
        if vz > 3.0:
            ex += clamp(25 * (vz - 3.0) / 5.0 + 25, 0, 50)
            reasons.append(f"volume {vz:.1f}σ above 5m baseline")
        elif vz > 2.0:
            ex += 12
        if f["obi_abs"] > 0.35:
            ex += clamp(30 * (f["obi_abs"] - 0.35) / 0.5, 0, 30)
            side = "bid" if f["obi"] > 0 else "ask"
            reasons.append(f"order book {f['obi_abs']:.0%} {side}-heavy")
        # OBI relative to the book's *own* history is stronger evidence than raw
        # magnitude: some pairs are structurally lopsided, and what matters is
        # the deviation from that pair's normal shape.
        #
        # But statistical significance is not economic significance. A deep BTC
        # book sits at OBI +0.21 with a standard deviation of 0.03, so a routine
        # drift to +0.30 is a "3-sigma event" that means nothing at all. Gating
        # on absolute magnitude as well as z-score is what separates a genuinely
        # lopsided book from ordinary micro-variance on a very stable one.
        if abs(f["obi_z"]) > 3.0 and f["obi_abs"] >= 0.30:
            ex += clamp(22 * (abs(f["obi_z"]) - 3.0) / 4.0 + 8, 0, 30)
            reasons.append(
                f"book imbalance {f['obi_z']:.1f}σ vs its own baseline "
                f"(OBI {f['obi']:+.2f})"
            )
        # Require the volume side to be non-trivial too: a CVD z-score computed
        # over a near-empty window is noise, not aggression.
        if abs(f["cvd_z"]) > 2.5 and f["volume_z"] > -0.5:
            ex += clamp(15 * (abs(f["cvd_z"]) - 2.5) / 3.0, 0, 15)
            reasons.append(f"CVD {f['cvd_z']:.1f}σ — one-sided aggression")
        if abs(f["cvd_divergence"]) > 0.4:
            ex += clamp(20 * abs(f["cvd_divergence"]), 0, 20)
            reasons.append(
                "price/CVD divergence — "
                + ("absorption by passive wall" if f["cvd_divergence"] > 0
                   else "markup without real buying")
            )
        if f["wall_activity"] > 1.0:
            ex += clamp(10 * f["wall_activity"] / 3.0, 0, 10)
            reasons.append("large passive walls pulled/consumed")
        if f["venue_dispersion"] > 0.5:
            ex += clamp(12 * f["venue_dispersion"], 0, 12)
            reasons.append(f"cross-venue book disagreement ({f['venue_dispersion']:.2f})")

        # Wash-trading rule. The signature is *absence* where there should be
        # presence: huge volume that produces no net delta, no book pressure and
        # an abnormally tight spread — i.e. the same actor on both sides. The
        # generic rules above deliberately miss this because it avoids every
        # directional trigger, so it needs its own detector.
        wash_signature = 0.0
        # Requires *observed* book data: a zero spread or zero OBI means "no
        # order-book feed", not "suspiciously tight". Inferring wash trading
        # from absent data is how a detector invents phantom manipulation on
        # every asset whose book it cannot see.
        book_observed = f["spread_bps"] > 0.0 and (f["obi_abs"] > 0.0 or f["obi_z"] != 0.0)
        flow_observed = f["cvd_z"] != 0.0 or f["cvd_divergence"] != 0.0
        if (
            f["volume_z"] > 2.5
            and f["volume_spike_ratio"] > 3.0
            and book_observed
            and flow_observed
        ):
            neutral_cvd = abs(f["cvd_z"]) < 1.2 and abs(f["cvd_divergence"]) < 0.2
            flat_book = f["obi_abs"] < 0.2
            tight_spread = f["spread_bps"] < 1.0  # log1p scale: < ~1.7 bps
            confirms = sum((neutral_cvd, flat_book, tight_spread))
            if confirms >= 2:
                wash_signature = clamp(confirms / 3.0 * (f["volume_z"] / 5.0), 0.0, 1.0)
                ex += clamp(18 * confirms + 8 * (f["volume_z"] - 2.5), 0, 55)
                reasons.append(
                    f"wash-trading signature: {f['volume_spike_ratio']:.0f}x volume with "
                    "no net delta, flat book"
                    + (", abnormally tight spread" if tight_spread else "")
                )
        ex = clamp(ex, 0, 100)
        self._last_wash_signature = wash_signature

        # --- Module 2: on-chain ---
        oc = 0.0
        if f["whale_inflow_z"] > 2.0:
            oc += clamp(30 * (f["whale_inflow_z"] - 2.0) / 4.0 + 15, 0, 45)
            reasons.append(f"whale transfer {f['whale_inflow_z']:.1f}σ vs history")
        if f["net_flow_norm"] > 0.3:
            oc += clamp(25 * f["net_flow_norm"], 0, 25)
            reasons.append(f"net CEX deposits (flow index {f['net_flow_norm']:+.2f})")
        elif f["net_flow_norm"] < -0.3:
            oc += clamp(10 * abs(f["net_flow_norm"]), 0, 10)
            reasons.append("large CEX withdrawals — supply squeeze setup")
        if f["liquidity_drop"] > 0.30:
            oc += clamp(40 * f["liquidity_drop"], 0, 40)
            reasons.append(f"DEX liquidity -{f['liquidity_drop'] * 100:.0f}% in one block")
        if f["bridge_to_cex"] > 0.3:
            oc += clamp(25 * f["bridge_to_cex"], 0, 25)
            reasons.append("bridged stablecoins landing on CEX (pre-positioning)")
        elif f["bridge_activity"] > 0.4:
            oc += clamp(12 * f["bridge_activity"], 0, 12)
            reasons.append("elevated bridge stablecoin flow")
        oc = clamp(oc, 0, 100)

        # --- Module 3: social ---
        so = 0.0
        mz = f["mention_z"]
        if mz > 3.0:
            so += clamp(30 * (mz - 3.0) / 5.0 + 20, 0, 50)
            reasons.append(f"mention rate {mz:.1f}σ above baseline")
        elif mz > 2.0:
            so += 10
        if f["mention_accel"] > 0.4:
            so += clamp(20 * f["mention_accel"], 0, 20)
            reasons.append("mention volume accelerating")
        if f["bot_farm_score"] > 0.5:
            so += clamp(45 * f["bot_farm_score"], 0, 45)
            reasons.append(f"bot-farm pattern (confidence {f['bot_farm_score']:.0%})")
        elif f["bot_farm_score"] > 0.3:
            so += 12
        if f["sentiment_abs"] > 0.6:
            so += clamp(15 * f["sentiment_abs"], 0, 15)
            reasons.append(
                f"extreme {'bullish' if f['sentiment'] > 0 else 'bearish'} sentiment "
                f"({f['sentiment']:+.2f})"
            )
        so = clamp(so, 0, 100)

        contributions = {"exchange": ex, "onchain": oc, "social": so}

        # --- Evidence combination ---
        # A plain weighted average is the wrong operator here: it structurally
        # caps any single-source signature at its own weight (a blatant
        # exchange-only wash trade could never exceed 40/100 at w=0.4), which
        # destroys recall on venue-local manipulation. We instead combine the
        # sub-scores as independent evidence via noisy-OR, so one overwhelming
        # source can carry the verdict while multiple sources still compound.
        weighted = (
            ex * self.weights.get("exchange", 0.4)
            + oc * self.weights.get("onchain", 0.35)
            + so * self.weights.get("social", 0.25)
        )
        p_clean = 1.0
        for key, val in contributions.items():
            # Weight modulates how much a source can dominate on its own; even
            # the lightest-weighted source can still carry a verdict when its
            # own evidence is overwhelming (a pure bot-farm campaign is real
            # manipulation even with a quiet order book).
            w = clamp(0.70 + 0.75 * self.weights.get(key, 0.33), 0.0, 1.0)
            p_clean *= 1.0 - clamp(val / 100.0, 0.0, 0.985) * w
        noisy_or = (1.0 - p_clean) * 100.0

        # Keep the average's conservatism, take the max's sensitivity.
        base = max(weighted, noisy_or * 0.95)

        # --- Cross-module co-occurrence bonus ---
        # Independent confirmation is the single most informative signal we have,
        # so a pattern lighting up 2-3 modules is escalated super-additively.
        active = sum(1 for v in contributions.values() if v >= 30)
        if active >= 2:
            multiplier = 1.0 + 0.18 * (active - 1)
            base *= multiplier
            reasons.append(f"corroborated across {active} independent data sources")

        # --- Named playbooks: recognisable manipulation archetypes ---
        if f["volume_z"] > 3 and f["bot_farm_score"] > 0.5 and f["sentiment"] > 0.3:
            base += 12
            reasons.append("⚑ pump-and-dump signature (volume + shills + euphoria)")
        if f["liquidity_drop"] > 0.3 and f["net_flow_norm"] > 0.2:
            base += 15
            reasons.append("⚑ rug-pull signature (LP drain + exchange deposits)")
        if f["obi_abs"] > 0.5 and f["wall_activity"] > 1.0 and abs(f["cvd_divergence"]) > 0.3:
            base += 10
            reasons.append("⚑ spoofing/layering signature")
        if f["whale_inflow_z"] > 3 and f["mention_z"] > 3 and f["sentiment"] > 0.4:
            base += 10
            reasons.append("⚑ distribution-into-hype signature")

        # --- Organic-activity discount (false-positive control) ---
        # Genuine news moves volume, mentions and sentiment together — and looks
        # superficially like a pump. What it lacks is the *artificial* markers:
        # coordinated accounts, spoofed depth, LP drains, exchange pre-loading.
        # Without any of those, discount hard; this is what keeps precision high
        # now that the combiner is more sensitive.
        artificial = max(
            f["bot_farm_score"],
            clamp(f["obi_abs"] / 0.6, 0, 1) if f["wall_activity"] > 0.7 else 0.0,
            f["liquidity_drop"],
            clamp(f["bridge_to_cex"], 0, 1),
            clamp(abs(f["cvd_divergence"]) / 0.6, 0, 1),
            clamp(f["venue_dispersion"] / 0.8, 0, 1),
            clamp(max(0.0, f["net_flow_norm"]) / 0.5, 0, 1),
            # Wash trading is defined by the *absence* of directional markers, so
            # without this term the organic discount would suppress the very
            # pattern the wash rule just detected.
            getattr(self, "_last_wash_signature", 0.0),
        )
        # The discount exists to suppress *news-driven* volume, which looks like
        # a pump but lacks artificial markers. It must not suppress violent
        # order-flow events: a flash crash or capitulation dump has no bot farm
        # and no LP drain by nature, yet it is precisely what a surveillance
        # system exists to report. Scoring one of those 37/100 because nobody
        # was shilling it on Twitter is backwards.
        #
        # `structural_stress` measures how extreme the microstructure itself is.
        # When the book and tape are this dislocated, the event is self-evident
        # and needs no external corroboration.
        structural_stress = max(
            clamp((abs(f["cvd_z"]) - 3.0) / 4.0, 0.0, 1.0),
            clamp((abs(f["obi_z"]) - 3.0) / 4.0, 0.0, 1.0),
            clamp((f["obi_abs"] - 0.6) / 0.35, 0.0, 1.0),
            clamp((f["volume_z"] - 5.0) / 5.0, 0.0, 1.0),
        )
        loud = f["volume_z"] > 2.0 or f["mention_z"] > 2.0
        organic_cutoff = 0.45
        if loud and artificial < organic_cutoff:
            damping = 0.40 + 0.60 * (artificial / organic_cutoff)
            # Fade the discount out as structural stress rises; at full stress
            # the score passes through untouched.
            damping += (1.0 - damping) * structural_stress
            base *= damping
            if damping < 0.9:
                reasons.append("organic-activity profile — no artificial-flow markers")

        # --- Order-flow shock: a self-evident violent move ---
        # Volume spike + collapsed book depth + one-sided aggression is a
        # dump/squeeze in progress. It requires no cross-module corroboration
        # because all three legs are independent measurements of the same event.
        if f["volume_z"] > 3.0 and f["obi_abs"] > 0.5 and abs(f["cvd_z"]) > 3.0:
            direction = "SELL-OFF" if f["cvd_z"] < 0 else "BUY PANIC"
            severity = min(
                (f["volume_z"] / 3.0) * (f["obi_abs"] / 0.5) * (abs(f["cvd_z"]) / 3.0),
                8.0,
            )
            base = max(base, clamp(62.0 + severity * 5.0, 0.0, 100.0))
            reasons.insert(0, f"⚑ {direction}: volume {f['volume_z']:.1f}σ, book "
                              f"{f['obi_abs']:.0%} one-sided, CVD {f['cvd_z']:+.1f}σ")

        return clamp(base, 0, 100), contributions, reasons


class ManipulationClassifier:
    """Isolation Forest + rule blend producing the composite Manipulation Score."""

    def __init__(
        self,
        contamination: float = 0.02,
        n_estimators: int = 200,
        max_samples: str | int = "auto",
        random_state: int = 42,
        min_training_samples: int = 200,
        ml_blend: float = 0.5,
        weights: dict[str, float] | None = None,
        alert_threshold: float = 80.0,
    ) -> None:
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state
        self.min_training_samples = min_training_samples
        self.ml_blend = clamp(ml_blend, 0.0, 1.0)
        self.alert_threshold = alert_threshold
        self.rules = RuleEngine(weights)

        self.model: Any = None
        self.scaler_mean: np.ndarray | None = None
        self.scaler_scale: np.ndarray | None = None
        self._calibration: tuple[float, float] | None = None  # (p50, p01) of train scores
        self.trained_at: float = 0.0
        self.training_size: int = 0
        self.buffer: deque[list[float]] = deque(maxlen=20_000)
        self.scored = 0

    # ---- training --------------------------------------------------------
    def observe(self, fv: FeatureVector) -> None:
        """Add a vector to the online training buffer."""
        if fv.is_informative:
            self.buffer.append(fv.values)

    @property
    def can_train(self) -> bool:
        return len(self.buffer) >= self.min_training_samples

    @property
    def is_trained(self) -> bool:
        return self.model is not None

    def fit(self, data: list[list[float]] | np.ndarray | None = None) -> bool:
        """Train (or retrain) the Isolation Forest. Returns success."""
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            log.error("scikit-learn not installed; ML scoring disabled")
            return False

        X = np.asarray(data if data is not None else list(self.buffer), dtype=np.float64)
        if X.ndim != 2 or X.shape[0] < max(20, self.min_training_samples // 4):
            log.warning("insufficient training data: %s", X.shape)
            return False
        if X.shape[1] != N_FEATURES:
            log.error("feature dim mismatch: got %d, expect %d", X.shape[1], N_FEATURES)
            return False

        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        # Robust standardisation — the training set contains the very outliers
        # we hunt, so mean/std scaling would be dragged by them.
        median = np.median(X, axis=0)
        mad = np.median(np.abs(X - median), axis=0) * 1.4826
        mad[mad < 1e-8] = 1.0
        self.scaler_mean, self.scaler_scale = median, mad
        Xs = (X - median) / mad

        self.model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            max_samples=self.max_samples,
            random_state=self.random_state,
            n_jobs=-1,
            bootstrap=False,
        )
        self.model.fit(Xs)

        raw = self.model.score_samples(Xs)
        # Calibrate: p50 -> score 0, p1 -> score 100 (lower raw == more anomalous)
        self._calibration = (float(np.percentile(raw, 50)), float(np.percentile(raw, 1)))
        self.trained_at = time.time()
        self.training_size = int(X.shape[0])
        log.info(
            "IsolationForest trained on %d samples (%d features, contamination=%.3f)",
            self.training_size, X.shape[1], self.contamination,
        )
        return True

    # ---- inference -------------------------------------------------------
    def ml_scores_batch(self, rows: list[list[float]]) -> list[float]:
        """Score many vectors in one forest call.

        ``IsolationForest.score_samples`` costs ~10.4 ms regardless of whether
        it is given 1 row or 100 — essentially all of it fixed per-call
        overhead. Scoring assets one at a time therefore made cycle latency
        scale linearly with the tracked universe (15 assets ≈ 160 ms, 100 ≈ 1 s)
        and blew the 200 ms budget as soon as discovery started adding pairs.
        One batched call is O(1) in that overhead.
        """
        if not rows:
            return []
        if self.model is None or self.scaler_mean is None or self._calibration is None:
            return [0.0] * len(rows)
        x = np.nan_to_num(
            np.asarray(rows, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
        )
        xs = (x - self.scaler_mean) / self.scaler_scale
        raw = self.model.score_samples(xs)
        p50, p01 = self._calibration
        span = p50 - p01
        if span <= 1e-9:
            return [0.0] * len(rows)
        return [clamp((p50 - r) / span * 100.0, 0.0, 100.0) for r in raw]

    def _ml_score(self, values: list[float]) -> float:
        """Calibrated 0-100 anomaly score from the forest."""
        if self.model is None or self.scaler_mean is None or self._calibration is None:
            return 0.0
        x = np.nan_to_num(
            np.asarray(values, dtype=np.float64).reshape(1, -1), nan=0.0, posinf=0.0, neginf=0.0
        )
        xs = (x - self.scaler_mean) / self.scaler_scale
        raw = float(self.model.score_samples(xs)[0])
        p50, p01 = self._calibration
        span = p50 - p01
        if span <= 1e-9:
            return 0.0
        return clamp((p50 - raw) / span * 100.0, 0.0, 100.0)

    @staticmethod
    def _internally_corroborated(fv: FeatureVector) -> bool:
        """True when one module's own metrics independently agree.

        Volume, order-book imbalance and CVD are separate measurements. When all
        three are extreme and consistent, the event is self-evident and needs no
        cross-module confirmation.
        """
        f = fv.as_dict()
        return (
            f["volume_z"] > 3.0
            and f["obi_abs"] > 0.5
            and abs(f["cvd_z"]) > 3.0
        )

    @staticmethod
    def _severity(score: float) -> Severity:
        if score >= 90:
            return Severity.CRITICAL
        if score >= 80:
            return Severity.HIGH
        if score >= 60:
            return Severity.MEDIUM
        if score >= 40:
            return Severity.LOW
        return Severity.INFO

    def score(self, fv: FeatureVector, ml_score: float | None = None) -> ScoreBreakdown:
        """Compute the composite 0-100 Manipulation Score.

        ``ml_score`` may be supplied by :meth:`ml_scores_batch` to avoid a
        per-asset forest call on the hot path.
        """
        self.scored += 1
        rule_score, contributions, reasons = self.rules.evaluate(fv)
        if ml_score is None:
            ml_score = self._ml_score(fv.values) if self.is_trained else 0.0

        if not self.is_trained:
            composite = rule_score
        else:
            blend = self.ml_blend
            # Partial data coverage makes the ML vector less trustworthy (it was
            # trained on fuller vectors), so lean on rules when coverage is thin.
            blend *= clamp(fv.coverage + 0.34, 0.0, 1.0)
            blended = (1 - blend) * rule_score + blend * ml_score

            # Asymmetric fusion. The forest is *unsupervised*: it scores
            # statistical rarity, not manipulation. It may therefore legitimately
            # raise a verdict (novel pattern the rules never encoded) but must
            # not veto one — common-but-illegal behaviour like wash trading is
            # precisely what a rarity model rates as unremarkable. So the ML
            # side can only lift a confident rule score, never drag it down.
            if rule_score >= 60 and ml_score < rule_score:
                composite = max(blended, rule_score * 0.9 + ml_score * 0.1)
            else:
                composite = blended

            if ml_score >= 70 and rule_score < 40:
                reasons.append(f"ML flags unusual multi-feature state ({ml_score:.0f}/100)")

        # Confidence damping: a single-source signal should rarely alert alone,
        # because most one-module patterns are ambiguous without corroboration.
        #
        # The exception is a signal that is *internally* corroborated. A violent
        # order-flow event is measured three independent ways — traded volume,
        # book depth and aggressor delta — so it is not really "one source", and
        # penalising it for the on-chain and social modules being disabled would
        # mean a flash crash never alerts on an exchange-only deployment.
        if fv.coverage <= 0.34 and not self._internally_corroborated(fv):
            composite *= 0.82

        composite = clamp(composite, 0.0, 100.0)
        return ScoreBreakdown(
            composite=composite,
            ml_component=ml_score,
            rule_component=rule_score,
            contributions=contributions,
            reasons=reasons[:8],
            severity=self._severity(composite),
        )

    def classify(
        self, fv: FeatureVector, venue: str = "aggregate",
        ml_score: float | None = None,
    ) -> AnomalySignal:
        """Score a vector and wrap it in an :class:`AnomalySignal`."""
        breakdown = self.score(fv, ml_score=ml_score)
        top = sorted(
            (
                (name, val)
                for name, val in zip(FEATURE_NAMES, fv.values)
                if abs(val) > 0.05
            ),
            key=lambda kv: -abs(kv[1]),
        )[:10]
        return AnomalySignal(
            timestamp=fv.timestamp or now_ms(),
            asset_pair=fv.asset,
            venue=venue,
            score=breakdown.composite,
            severity=breakdown.severity,
            ml_score=breakdown.ml_component,
            rule_score=breakdown.rule_component,
            contributions={k: round(v, 2) for k, v in breakdown.contributions.items()},
            features={k: round(v, 4) for k, v in top},
            reasons=breakdown.reasons,
        )

    # ---- persistence -----------------------------------------------------
    def save(self, path: str | Path) -> bool:
        if not self.is_trained or not str(path).strip():
            return False
        try:
            import joblib
        except ImportError:
            log.warning("joblib unavailable; cannot persist model")
            return False
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "scaler_mean": self.scaler_mean,
                "scaler_scale": self.scaler_scale,
                "calibration": self._calibration,
                "feature_names": FEATURE_NAMES,
                "trained_at": self.trained_at,
                "training_size": self.training_size,
                "version": 1,
            },
            p,
        )
        log.info("model saved -> %s", p)
        return True

    def load(self, path: str | Path) -> bool:
        # An empty/blank path means "no persistence configured" — Path("")
        # resolves to "." and would otherwise raise a confusing IsADirectoryError.
        if not str(path).strip():
            return False
        p = Path(path)
        if not p.exists() or not p.is_file():
            return False
        try:
            import joblib

            blob = joblib.load(p)
        except Exception as exc:
            log.warning("failed to load model %s: %s", p, exc)
            return False
        if tuple(blob.get("feature_names", ())) != FEATURE_NAMES:
            log.warning("model %s has incompatible feature schema; ignoring", p)
            return False
        self.model = blob["model"]
        self.scaler_mean = blob["scaler_mean"]
        self.scaler_scale = blob["scaler_scale"]
        self._calibration = blob["calibration"]
        self.trained_at = blob.get("trained_at", 0.0)
        self.training_size = blob.get("training_size", 0)
        log.info("model loaded from %s (%d training samples)", p, self.training_size)
        return True

    def info(self) -> dict[str, Any]:
        return {
            "trained": self.is_trained,
            "training_size": self.training_size,
            "buffer": len(self.buffer),
            "scored": self.scored,
            "ml_blend": self.ml_blend,
            "trained_age_s": round(time.time() - self.trained_at, 1) if self.trained_at else None,
            "features": N_FEATURES,
        }
