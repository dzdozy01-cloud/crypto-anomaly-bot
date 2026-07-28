"""Module 4 tests: feature assembly, classifier behaviour, detection quality."""

from __future__ import annotations

import numpy as np
import pytest

from cadb.core.schema import MarketEvent, MetricType, Severity, SourceType, now_ms
from cadb.modules.ml.classifier import ManipulationClassifier, RuleEngine
from cadb.modules.ml.features import FEATURE_NAMES, FeatureStore, FeatureVector
from cadb.modules.ml.training import (
    generate_labelled_set,
    generate_training_data,
)


def _fv(**overrides) -> FeatureVector:
    """Build a feature vector from named overrides (rest neutral)."""
    values = [1.0 if n == "volume_spike_ratio" else 0.0 for n in FEATURE_NAMES]
    for name, val in overrides.items():
        values[FEATURE_NAMES.index(name)] = val
    return FeatureVector(
        asset="TEST", timestamp=now_ms(), values=values, coverage=1.0,
        sources_fresh={"exchange": True, "onchain": True, "social": True},
    )


class TestFeatureStore:
    def _event(self, metric, value, venue="binance", asset="BTC/USDT", z=None, usd=None, meta=None):
        source = {
            MetricType.VOLUME: SourceType.EXCHANGE,
            MetricType.ORDER_BOOK: SourceType.EXCHANGE,
            MetricType.CVD: SourceType.EXCHANGE,
            MetricType.WALLET_TRANSFER: SourceType.ONCHAIN,
            MetricType.LIQUIDITY: SourceType.ONCHAIN,
            MetricType.BRIDGE_FLOW: SourceType.ONCHAIN,
        }.get(metric, SourceType.SOCIAL)
        return MarketEvent(
            source_type=source, venue=venue, asset_pair=asset, metric_type=metric,
            raw_value=value, normalized_z_score=z, usd_value=usd, meta=meta or {},
        )

    def test_vector_has_fixed_dimension(self):
        store = FeatureStore()
        store.ingest(self._event(MetricType.VOLUME, 100.0, z=3.5))
        fv = store.build("BTC")
        assert fv is not None
        assert len(fv.values) == len(FEATURE_NAMES)

    def test_routes_by_base_asset(self):
        store = FeatureStore()
        store.ingest(self._event(MetricType.VOLUME, 1.0, asset="BTC/USDT"))
        store.ingest(self._event(MetricType.SOCIAL_MENTIONS, 5.0, asset="BTC"))
        assert "BTC" in store.assets
        assert store.assets["BTC"].updates == 2

    def test_populates_expected_features(self):
        store = FeatureStore()
        store.ingest(self._event(MetricType.VOLUME, 500.0, z=4.2,
                                 meta={"spike_ratio": 8.0}))
        store.ingest(self._event(MetricType.ORDER_BOOK, 0.6, z=3.1,
                                 meta={"spread_bps": 2.0, "spoofed_walls": 2}))
        store.ingest(self._event(MetricType.WALLET_TRANSFER, 100.0, z=3.0,
                                 usd=2_000_000, meta={"direction": "inflow"}))
        store.ingest(self._event(MetricType.BOT_FARM, 0.8, asset="BTC"))
        f = store.build("BTC").as_dict()
        assert f["volume_z"] == pytest.approx(4.2, abs=0.3)
        assert f["obi"] == pytest.approx(0.6, abs=0.05)
        assert f["obi_abs"] > 0.5
        assert f["net_flow_norm"] > 0
        assert f["bot_farm_score"] == pytest.approx(0.8, abs=0.01)

    def test_stale_data_decays_out(self):
        store = FeatureStore(ttl_s=60)
        old = now_ms() - 300_000
        store.assets.setdefault("BTC", None)
        store.assets.pop("BTC")
        e = MarketEvent(
            source_type=SourceType.EXCHANGE, venue="binance", asset_pair="BTC/USDT",
            metric_type=MetricType.VOLUME, raw_value=100.0, normalized_z_score=8.0,
            timestamp=old,
        )
        store.ingest(e)
        fv = store.build("BTC", now=now_ms())
        assert fv.as_dict()["volume_z"] == 0.0
        assert fv.coverage < 1.0

    def test_cross_venue_dispersion_ignores_stale_venues(self):
        """Dispersion must reflect real disagreement, not reporting lag."""
        store = FeatureStore()
        t = now_ms()
        store.ingest(MarketEvent(
            source_type=SourceType.EXCHANGE, venue="binance", asset_pair="BTC/USDT",
            metric_type=MetricType.ORDER_BOOK, raw_value=0.5, timestamp=t))
        store.ingest(MarketEvent(
            source_type=SourceType.EXCHANGE, venue="bybit", asset_pair="BTC/USDT",
            metric_type=MetricType.ORDER_BOOK, raw_value=0.5, timestamp=t - 120_000))
        fv = store.build("BTC", now=t)
        assert fv.as_dict()["venue_dispersion"] == 0.0

    def test_coverage_reflects_active_modules(self):
        store = FeatureStore()
        store.ingest(self._event(MetricType.VOLUME, 1.0, z=1.0))
        assert store.build("BTC").coverage == pytest.approx(1 / 3)
        store.ingest(self._event(MetricType.SOCIAL_MENTIONS, 5.0, asset="BTC", z=1.0))
        assert store.build("BTC").coverage == pytest.approx(2 / 3)

    def test_prune_removes_idle_assets(self):
        store = FeatureStore()
        store.ingest(MarketEvent(
            source_type=SourceType.EXCHANGE, venue="v", asset_pair="OLD/USDT",
            metric_type=MetricType.VOLUME, raw_value=1.0,
            timestamp=now_ms() - 7_200_000))
        assert store.prune(max_idle_s=3600) == 1
        assert "OLD" not in store.assets


class TestRuleEngine:
    def test_quiet_market_scores_near_zero(self):
        score, _, _ = RuleEngine().evaluate(_fv())
        assert score < 10

    def test_volume_spike_alone_is_not_enough(self):
        """One loud-but-clean signal must not alert; that is just activity."""
        score, _, _ = RuleEngine().evaluate(
            _fv(volume_z=4.0, volume_spike_ratio=6.0, spread_bps=1.5,
                obi=0.1, obi_abs=0.1, obi_z=0.5, cvd_z=0.8)
        )
        assert score < 80

    def test_missing_book_data_does_not_fabricate_wash_signal(self):
        """Absent order-book telemetry must not read as 'suspiciously tight'."""
        _, _, reasons = RuleEngine().evaluate(
            _fv(volume_z=5.0, volume_spike_ratio=10.0)  # no book/flow features
        )
        assert not any("wash-trading" in r for r in reasons)

    def test_multi_source_corroboration_escalates(self):
        rules = RuleEngine()
        single, _, _ = rules.evaluate(_fv(volume_z=5.0, obi=0.6, obi_abs=0.6))
        multi, _, reasons = rules.evaluate(
            _fv(volume_z=5.0, obi=0.6, obi_abs=0.6, bot_farm_score=0.8,
                mention_z=5.0, sentiment=0.7, sentiment_abs=0.7,
                whale_inflow_z=4.0, net_flow_norm=0.6)
        )
        assert multi > single
        assert any("corroborated" in r for r in reasons)

    def test_pump_and_dump_playbook(self):
        score, _, reasons = RuleEngine().evaluate(
            _fv(volume_z=6.0, volume_spike_ratio=10.0, obi=0.6, obi_abs=0.6,
                obi_z=4.0, cvd_z=4.0, cvd_divergence=-0.6, mention_z=6.0,
                mention_accel=0.8, sentiment=0.8, sentiment_abs=0.8,
                bot_farm_score=0.85, wall_activity=1.5)
        )
        assert score >= 80
        assert any("pump-and-dump" in r for r in reasons)

    def test_rug_pull_playbook(self):
        score, _, reasons = RuleEngine().evaluate(
            _fv(liquidity_drop=0.7, net_flow_norm=0.6, whale_inflow_z=5.0,
                volume_z=4.0, sentiment=-0.7, sentiment_abs=0.7)
        )
        assert score >= 80
        assert any("rug-pull" in r for r in reasons)

    def test_wash_trading_detected_despite_no_direction(self):
        """Volume without delta, flat book, tight spread == same actor both sides."""
        score, contributions, reasons = RuleEngine().evaluate(
            _fv(volume_z=5.0, volume_spike_ratio=12.0, cvd_z=0.3,
                cvd_divergence=0.05, obi=0.02, obi_abs=0.02, obi_z=0.4,
                spread_bps=0.4)
        )
        assert any("wash-trading" in r for r in reasons)
        assert contributions["exchange"] > 50

    def test_organic_news_discounted(self):
        """Real news moves volume+mentions+sentiment but has no artificial markers."""
        organic, _, reasons = RuleEngine().evaluate(
            _fv(volume_z=4.0, volume_spike_ratio=6.0, mention_z=4.0,
                mention_accel=0.5, sentiment=0.5, sentiment_abs=0.5,
                cvd_z=2.5, spread_bps=1.5, obi=0.12, obi_abs=0.12, obi_z=0.6,
                bot_farm_score=0.1)
        )
        coordinated, _, _ = RuleEngine().evaluate(
            _fv(volume_z=4.0, volume_spike_ratio=6.0, mention_z=4.0,
                mention_accel=0.5, sentiment=0.5, sentiment_abs=0.5,
                cvd_z=2.5, spread_bps=1.5, obi=0.12, obi_abs=0.12, obi_z=0.6,
                bot_farm_score=0.85)
        )
        assert organic < coordinated
        assert any("organic" in r for r in reasons)

    def test_liquidity_drop_threshold_matches_spec(self):
        below, _, _ = RuleEngine().evaluate(_fv(liquidity_drop=0.25))
        above, _, _ = RuleEngine().evaluate(_fv(liquidity_drop=0.55))
        assert above > below

    def test_score_always_bounded(self):
        score, _, _ = RuleEngine().evaluate(
            _fv(**dict.fromkeys(FEATURE_NAMES, 20.0))
        )
        assert 0.0 <= score <= 100.0


class TestClassifier:
    @pytest.fixture(scope="class")
    def trained(self):
        clf = ManipulationClassifier(n_estimators=100, min_training_samples=100)
        clf.fit(generate_training_data(3000, 0.02, seed=42))
        return clf

    def test_untrained_falls_back_to_rules(self):
        clf = ManipulationClassifier()
        assert not clf.is_trained
        b = clf.score(_fv(volume_z=6.0, obi=0.7, obi_abs=0.7, bot_farm_score=0.9,
                          mention_z=6.0, sentiment=0.8, sentiment_abs=0.8))
        assert b.composite == b.rule_component
        assert b.ml_component == 0.0

    def test_training_succeeds(self, trained):
        assert trained.is_trained
        assert trained.training_size >= 3000

    def test_rejects_wrong_feature_dimension(self):
        clf = ManipulationClassifier(min_training_samples=10)
        assert not clf.fit(np.random.rand(100, 5))

    def test_normal_state_scores_low(self, trained):
        assert trained.score(_fv()).composite < 20

    def test_severity_mapping(self, trained):
        assert trained._severity(95) is Severity.CRITICAL
        assert trained._severity(85) is Severity.HIGH
        assert trained._severity(65) is Severity.MEDIUM
        assert trained._severity(45) is Severity.LOW
        assert trained._severity(10) is Severity.INFO

    def test_ml_cannot_veto_confident_rules(self, trained):
        """An unsupervised rarity model must not suppress a strong rule verdict."""
        fv = _fv(volume_z=5.0, volume_spike_ratio=12.0, cvd_z=0.3,
                 cvd_divergence=0.05, obi=0.02, obi_abs=0.02, spread_bps=0.4)
        b = trained.score(fv)
        assert b.composite >= b.rule_component * 0.85

    def test_signal_is_explainable(self, trained):
        signal = trained.classify(
            _fv(volume_z=6.0, obi=0.7, obi_abs=0.7, obi_z=4.0, bot_farm_score=0.9,
                mention_z=6.0, sentiment=0.8, sentiment_abs=0.8, cvd_divergence=-0.6)
        )
        assert signal.reasons
        assert signal.features
        assert set(signal.contributions) == {"exchange", "onchain", "social"}
        assert 0 <= signal.score <= 100

    def test_low_coverage_is_damped(self, trained):
        full = _fv(volume_z=5.0, obi=0.6, obi_abs=0.6)
        partial = FeatureVector(
            asset="T", timestamp=now_ms(), values=full.values, coverage=0.33,
            sources_fresh={"exchange": True, "onchain": False, "social": False},
        )
        assert trained.score(partial).composite < trained.score(full).composite

    def test_persistence_roundtrip(self, trained, tmp_path):
        path = tmp_path / "m.joblib"
        assert trained.save(path)
        fresh = ManipulationClassifier()
        assert fresh.load(path)
        fv = _fv(volume_z=4.0, obi=0.5, obi_abs=0.5)
        assert fresh.score(fv).composite == pytest.approx(
            trained.score(fv).composite, abs=0.01
        )

    def test_rejects_incompatible_saved_model(self, tmp_path):
        import joblib

        path = tmp_path / "bad.joblib"
        joblib.dump({"model": None, "scaler_mean": None, "scaler_scale": None,
                     "calibration": None, "feature_names": ("a", "b")}, path)
        assert not ManipulationClassifier().load(path)

    def test_signal_to_event_roundtrip(self, trained):
        signal = trained.classify(_fv(volume_z=6.0, obi=0.7, obi_abs=0.7))
        event = signal.to_event()
        assert event.metric_type is MetricType.MANIPULATION_SCORE
        assert event.raw_value == signal.score


class TestDetectionQuality:
    """Precision/recall gates — these guard against tuning regressions."""

    @pytest.fixture(scope="class")
    def evaluated(self):
        clf = ManipulationClassifier(n_estimators=150)
        clf.fit(generate_training_data(5000, 0.02, seed=42))
        X, y, names = generate_labelled_set(800, 100, seed=7)
        scores = np.array([
            clf.score(FeatureVector(
                asset="E", timestamp=0, values=list(row), coverage=1.0,
                sources_fresh={"exchange": True, "onchain": True, "social": True},
            )).composite
            for row in X
        ])
        return scores, y, names

    def test_precision_at_alert_threshold(self, evaluated):
        scores, y, _ = evaluated
        pred = scores >= 80
        tp = int((pred & (y == 1)).sum())
        fp = int((pred & (y == 0)).sum())
        precision = tp / (tp + fp) if tp + fp else 0.0
        assert precision >= 0.90, f"precision {precision:.3f} below gate"

    def test_recall_at_alert_threshold(self, evaluated):
        scores, y, _ = evaluated
        pred = scores >= 80
        tp = int((pred & (y == 1)).sum())
        fn = int((~pred & (y == 1)).sum())
        recall = tp / (tp + fn) if tp + fn else 0.0
        assert recall >= 0.75, f"recall {recall:.3f} below gate"

    def test_normal_states_never_alert(self, evaluated):
        scores, _, names = evaluated
        normal = np.array([s for s, n in zip(scores, names) if n == "normal"])
        assert (normal >= 80).mean() < 0.01
        assert normal.mean() < 15

    def test_each_scenario_is_detectable(self, evaluated):
        """Every archetype must at least reach the medium band on average."""
        scores, _, names = evaluated
        from collections import defaultdict

        grouped = defaultdict(list)
        for s, n in zip(scores, names):
            grouped[n].append(s)
        for scenario in ("pump_and_dump", "rug_pull", "spoofing",
                         "wash_trading", "whale_distribution", "social_shill"):
            mean = float(np.mean(grouped[scenario]))
            assert mean >= 60, f"{scenario} mean score {mean:.1f} too low"

    def test_benign_news_stays_below_manipulation(self, evaluated):
        scores, _, names = evaluated
        from collections import defaultdict

        grouped = defaultdict(list)
        for s, n in zip(scores, names):
            grouped[n].append(s)
        benign = float(np.mean(grouped["benign_news"]))
        pump = float(np.mean(grouped["pump_and_dump"]))
        assert benign < pump - 25, "benign news must separate from real pumps"


class TestTrainingData:
    def test_shape_and_finiteness(self):
        data = generate_training_data(500, 0.05, seed=1)
        assert data.shape == (500, len(FEATURE_NAMES))
        assert np.isfinite(data).all()

    def test_deterministic_with_seed(self):
        a = generate_training_data(100, 0.02, seed=99)
        b = generate_training_data(100, 0.02, seed=99)
        assert np.array_equal(a, b)

    def test_labelled_set_balance(self):
        X, y, names = generate_labelled_set(200, 30, seed=3)
        assert len(X) == len(y) == len(names)
        assert 0 < y.sum() < len(y)


class TestQuietMarketDoesNotAlert:
    """Regression: ordinary market activity scored 82 and alerted continuously.

    Production symptom (2026-07): a steady stream of `MANIPULATION BTC
    score=82.0` on nothing but normal exchange flow, driven by log-normal volume
    producing z-scores of 800-1400.
    """

    @pytest.fixture(scope="class")
    def clf(self):
        c = ManipulationClassifier(n_estimators=100, min_training_samples=100)
        c.fit(generate_training_data(3000, 0.02, seed=42))
        return c

    def test_exchange_only_directional_flow_is_quiet(self, clf):
        """One-sided flow with mild divergence is ordinary, not manipulation."""
        b = clf.score(_fv(cvd_z=-2.7, cvd_divergence=0.42, volume_z=-1.46,
                          spread_bps=0.69, obi_z=0.45))
        assert b.composite < 40, f"quiet market scored {b.composite:.1f}"

    def test_moderate_volume_burst_alone_is_quiet(self, clf):
        b = clf.score(_fv(volume_z=3.2, volume_spike_ratio=4.0, spread_bps=1.2,
                          obi=0.15, obi_abs=0.15, obi_z=0.8, cvd_z=1.1))
        assert b.composite < 60, f"a lone volume burst scored {b.composite:.1f}"

    def test_lopsided_book_alone_is_quiet(self, clf):
        """Books are often lopsided; that alone must not alert."""
        b = clf.score(_fv(obi=0.72, obi_abs=0.72, obi_z=2.2, spread_bps=1.0))
        assert b.composite < 80, f"lopsided book alone scored {b.composite:.1f}"

    def test_real_multi_source_pump_still_fires(self, clf):
        """Guard against over-suppression."""
        b = clf.score(_fv(volume_z=6.0, volume_spike_ratio=9.0, obi=0.68,
                          obi_abs=0.68, obi_z=4.5, cvd_z=4.2, cvd_divergence=-0.55,
                          mention_z=5.5, mention_accel=0.8, sentiment=0.75,
                          sentiment_abs=0.75, bot_farm_score=0.82,
                          wall_activity=1.4))
        assert b.composite >= 80, f"genuine pump only scored {b.composite:.1f}"
