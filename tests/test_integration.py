"""End-to-end tests: full pipeline, latency budget, and detection under load."""

from __future__ import annotations

import asyncio
import time

from cadb.app import Application
from cadb.core.bus import InProcessBus, build_bus
from cadb.core.config import Settings
from cadb.core.schema import AnomalySignal, MarketEvent, MetricType, SourceType, now_ms
from cadb.core.telemetry import METRICS


def _demo_settings(**overrides) -> Settings:
    s = Settings()
    s.exchange.simulate = True
    s.exchange.exchanges = ["binance", "bybit"]
    s.exchange.symbols = ["BTC/USDT", "PEPE/USDT"]
    s.exchange.volume_bucket_s = 1
    s.exchange.volume_window_s = 30
    s.onchain.simulate = True
    s.social.simulate = True
    s.social.use_finbert = False
    s.social.tracked_tickers = ["BTC", "PEPE"]
    s.social.mention_window_s = 30
    s.ml.score_interval_ms = 200
    s.ml.retrain_interval_s = 100_000
    s.ml.model_path = ""
    s.alerts.dry_run = True
    s.alerts.cooldown_s = 5
    s.telemetry.log_level = "WARNING"
    s.telemetry.state_file = ""
    s.telemetry.health_interval_s = 3600
    for key, value in overrides.items():
        section, _, field = key.partition("__")
        setattr(getattr(s, section), field, value)
    return s


class TestBusFactory:
    async def test_memory_bus(self):
        bus = await build_bus("memory")
        assert isinstance(bus, InProcessBus)
        await bus.close()

    async def test_redis_falls_back_gracefully(self):
        """An unreachable Redis must degrade to in-process, not crash startup."""
        bus = await build_bus("redis", url="redis://127.0.0.1:6390/0")
        assert isinstance(bus, InProcessBus)
        await bus.close()


class TestPipeline:
    async def test_full_pipeline_produces_signals(self):
        app = Application(_demo_settings())
        await app.setup()
        captured: list[AnomalySignal] = []

        async def capture(sig: AnomalySignal) -> None:
            captured.append(sig)

        assert app.ml is not None
        app.ml.add_handler(capture)
        await app.start()

        # Warm up, then inject a coordinated episode across sources.
        await asyncio.sleep(6)
        for feed in app.exchange.feeds.values():  # type: ignore[union-attr]
            if hasattr(feed, "inject_episode"):
                feed.inject_episode("PEPE/USDT", "pump", 20)
        src = app.social.sources[0]  # type: ignore[union-attr]
        if hasattr(src, "inject_campaign"):
            src.inject_campaign("PEPE", 20, "shill")
        await asyncio.sleep(14)

        health = app.health()
        await app.stop()

        assert health["bus"]["published"] > 100
        assert app.ml.store.assets, "feature store should hold assets"
        assert app.ml.signals_emitted > 0

    async def test_latency_budget_respected(self):
        """Tick -> anomaly output must stay inside the 200ms budget at p95."""
        METRICS.reset()
        app = Application(_demo_settings())
        await app.setup()
        await app.start()
        await asyncio.sleep(12)
        await app.stop()

        snap = METRICS.snapshot()["latency"]
        cycle = snap.get("ml.cycle_ms", {})
        assert cycle.get("count", 0) > 5, "expected scoring cycles to have run"
        assert cycle["p95"] < 200.0, f"p95 {cycle['p95']}ms exceeds 200ms budget"

        for stage in ("exchange.trade_ms", "exchange.book_ms"):
            if snap.get(stage, {}).get("count", 0):
                assert snap[stage]["p95"] < 50.0, f"{stage} p95 too high"

    async def test_no_bus_drops_under_normal_load(self):
        app = Application(_demo_settings())
        await app.setup()
        await app.start()
        await asyncio.sleep(10)
        health = app.health()
        await app.stop()
        published = health["bus"]["published"]
        dropped = health["bus"]["dropped"]
        assert published > 0
        assert dropped / max(published, 1) < 0.01, f"{dropped}/{published} events dropped"

    async def test_modules_report_healthy(self):
        app = Application(_demo_settings())
        await app.setup()
        await app.start()
        await asyncio.sleep(6)
        health = app.health()
        await app.stop()
        for module in health["modules"]:
            assert module["healthy"], f"{module['module']} unhealthy: {module}"

    async def test_clean_shutdown_is_idempotent(self):
        app = Application(_demo_settings())
        await app.setup()
        await app.start()
        await asyncio.sleep(3)
        await app.stop()
        await app.stop()  # must not raise
        leaked = [
            t for t in asyncio.all_tasks()
            if not t.done() and t is not asyncio.current_task()
            and any(p in (t.get_name() or "") for p in ("exchange:", "social:", "onchain:", "ml:"))
        ]
        assert not leaked, f"leaked tasks: {[t.get_name() for t in leaked]}"

    async def test_detects_injected_manipulation(self):
        """The headline behaviour: a coordinated episode must raise the score."""
        settings = _demo_settings()
        settings.exchange.symbols = ["PEPE/USDT"]
        settings.social.tracked_tickers = ["PEPE"]
        app = Application(settings)
        await app.setup()
        await app.start()

        await asyncio.sleep(8)
        assert app.ml is not None
        baseline = app.ml.last_scores.get("PEPE", 0.0)

        for feed in app.exchange.feeds.values():  # type: ignore[union-attr]
            if hasattr(feed, "inject_episode"):
                feed.inject_episode("PEPE/USDT", "pump", 25)
        src = app.social.sources[0]  # type: ignore[union-attr]
        if hasattr(src, "inject_campaign"):
            src.inject_campaign("PEPE", 25, "shill")

        peak = baseline
        for _ in range(20):
            await asyncio.sleep(1)
            peak = max(peak, app.ml.last_scores.get("PEPE", 0.0))
        await app.stop()

        assert peak > baseline, f"score did not react (baseline {baseline}, peak {peak})"
        assert peak >= 50.0, f"peak score {peak:.1f} too low for a blatant episode"


class TestBackpressure:
    async def test_high_throughput_without_deadlock(self):
        """The bus must stay responsive when a consumer is slower than producers."""
        bus = InProcessBus(queue_size=500)
        await bus.start()
        processed = 0

        async def slow_handler(_e: MarketEvent) -> None:
            nonlocal processed
            processed += 1
            if processed % 100 == 0:
                await asyncio.sleep(0.001)

        bus.add_handler(slow_handler, "cadb.*", name="slow")
        await asyncio.sleep(0)

        start = time.monotonic()
        for i in range(5000):
            await bus.publish(MarketEvent(
                source_type=SourceType.EXCHANGE, venue="binance", asset_pair="BTC/USDT",
                metric_type=MetricType.VOLUME, raw_value=float(i),
            ))
        elapsed = time.monotonic() - start
        await asyncio.sleep(0.3)
        await bus.close()

        assert elapsed < 5.0, f"publishing 5k events took {elapsed:.2f}s"
        assert bus.stats.published == 5000
        assert processed > 0


class TestBacktest:
    async def test_replay_recorded_events(self, tmp_path):
        from cadb.backtest import EventRecorder, run_backtest

        path = tmp_path / "events.jsonl"
        recorder = EventRecorder(path)
        base = now_ms() - 120_000
        for i in range(120):
            await recorder(MarketEvent(
                timestamp=base + i * 1000,
                source_type=SourceType.EXCHANGE, venue="binance", asset_pair="BTC/USDT",
                metric_type=MetricType.VOLUME, raw_value=100.0 + i,
                normalized_z_score=0.5, meta={"spike_ratio": 1.1},
            ))
        recorder.close()
        assert await run_backtest(str(path), threshold=80.0) == 0

    async def test_missing_file_returns_error(self):
        from cadb.backtest import run_backtest

        assert await run_backtest("/nonexistent/events.jsonl") == 1


class TestCLI:
    def test_parser_accepts_all_subcommands(self):
        from cadb.cli import build_parser

        parser = build_parser()
        for argv in (
            ["run", "--simulate", "--dry-run"],
            ["demo", "--duration", "30"],
            ["train", "--samples", "500"],
            ["evaluate"],
            ["backtest", "events.jsonl"],
            ["validate", "-c", "config.yaml"],
        ):
            assert parser.parse_args(argv).command == argv[0]

    def test_validate_on_missing_config_uses_defaults(self):
        from cadb.cli import main

        assert main(["validate", "-c", "/nonexistent/config.yaml"]) == 0

    def test_train_writes_model(self, tmp_path):
        from cadb.cli import main

        out = tmp_path / "m.joblib"
        assert main(["train", "--samples", "600", "-o", str(out)]) == 0
        assert out.exists()
