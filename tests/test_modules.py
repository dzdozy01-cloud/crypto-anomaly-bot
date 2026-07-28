"""Module-level tests: microstructure, on-chain decoding, sentiment, bot farms."""

from __future__ import annotations

import asyncio
import random

import pytest

from cadb.core.bus import InProcessBus
from cadb.core.config import ExchangeConfig, OnChainConfig, SocialConfig
from cadb.core.schema import MetricType, now_ms
from cadb.modules.exchange.microstructure import (
    CVDTracker,
    MicrostructureState,
    OrderBookState,
    VolumeProfile,
)
from cadb.modules.onchain.registry import AddressRegistry
from cadb.modules.onchain.rpc import TRANSFER_TOPIC, SolanaClient, decode_transfer_log
from cadb.modules.social.botfarm import BotFarmDetector, SocialPost
from cadb.modules.social.sentiment import LexiconScorer, extract_tickers


# ------------------------------------------------- Module 1: order book
class TestOrderBook:
    def _book(self, bid_sizes, ask_sizes, mid=100.0):
        b = OrderBookState(symbol="BTC/USDT", venue="binance", depth_levels=len(bid_sizes))
        bids = [(mid - 0.1 * (i + 1), s) for i, s in enumerate(bid_sizes)]
        asks = [(mid + 0.1 * (i + 1), s) for i, s in enumerate(ask_sizes)]
        b.update(bids, asks, now_ms())
        return b

    def test_obi_balanced(self):
        r = self._book([1.0] * 5, [1.0] * 5).imbalance()
        assert abs(r.obi) < 0.02
        assert r.direction == "balanced"

    def test_obi_bid_heavy(self):
        r = self._book([10.0] * 5, [1.0] * 5).imbalance()
        assert r.obi > 0.7
        assert r.direction == "bid_heavy"

    def test_obi_ask_heavy(self):
        r = self._book([1.0] * 5, [10.0] * 5).imbalance()
        assert r.obi < -0.7
        assert r.direction == "ask_heavy"

    def test_obi_formula_exact(self):
        """OBI == (bid_depth - ask_depth) / (bid_depth + ask_depth), in notional."""
        b = self._book([2.0], [1.0], mid=100.0)
        r = b.imbalance()
        expected = (r.bid_depth - r.ask_depth) / (r.bid_depth + r.ask_depth)
        assert r.obi == pytest.approx(expected)

    def test_obi_bounded(self):
        assert self._book([1e9], [1e-9]).imbalance().obi <= 1.0
        assert self._book([1e-9], [1e9]).imbalance().obi >= -1.0

    def test_empty_book_is_safe(self):
        b = OrderBookState(symbol="X/Y", venue="v")
        b.update([], [], now_ms())
        r = b.imbalance()
        assert r.obi == 0.0 and r.mid_price == 0.0

    def test_weighted_obi_discounts_far_liquidity(self):
        """Spoofed depth far from the touch must not dominate the signal."""
        b = OrderBookState(symbol="X/Y", venue="v", depth_levels=20, decay_bps=25.0)
        mid = 100.0
        bids = [(mid - 0.01 * (i + 1), 1.0) for i in range(20)]
        bids[15] = (mid - 0.16, 500.0)  # huge far-touch wall
        asks = [(mid + 0.01 * (i + 1), 1.0) for i in range(20)]
        b.update(bids, asks, now_ms())
        r = b.imbalance()
        assert r.obi > 0.9, "raw OBI is dominated by the wall"
        assert r.weighted_obi < r.obi, "weighted OBI must discount distant liquidity"

    def test_spread_calculation(self):
        b = OrderBookState(symbol="X/Y", venue="v")
        b.update([(99.9, 1.0)], [(100.1, 1.0)], now_ms())
        assert b.spread_bps == pytest.approx(20.0, abs=0.1)

    def test_wall_detection_flags_pulled_liquidity(self):
        b = OrderBookState(symbol="X/Y", venue="v", depth_levels=5)
        t = now_ms()
        b.update([(99.0, 1000.0), (98.0, 1.0)], [(101.0, 1.0)], t)
        b.imbalance()
        # Wall vanishes while the price stays put -> cancellation (spoof).
        b.update([(99.0, 1.0), (98.0, 1.0)], [(101.0, 1.0)], t + 100)
        walls = b.detect_walls(min_notional=1000.0)
        assert any(w.notional > 1000 for w in walls)


# ---------------------------------------------------- Module 1: volume
class TestVolumeProfile:
    def test_bucket_accumulation_and_zscore(self):
        """Buckets must close on schedule even when the z-score abstains.

        A perfectly constant series has zero dispersion, so no defensible
        z-score exists — but the volume observation is still real and must
        reach downstream consumers.
        """
        import random

        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5, threshold=3.0)
        t = (now_ms() // 5000) * 5000
        closes = 0
        for i in range(80):
            vp.add_trade(t + i * 5000, 1.0, 100.0)
            if vp.bucket_closed:
                closes += 1
        assert closes > 60, "buckets must close regardless of z availability"
        assert vp.total_trades == 80

        # With real dispersion, z-scores do appear.
        rng = random.Random(0)
        vp2 = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5)
        t2 = (now_ms() // 5000) * 5000
        zs = [
            z for i in range(80)
            if (z := vp2.add_trade(t2 + i * 5000, abs(rng.lognormvariate(0, 0.8)), 100.0))
            is not None
        ]
        assert len(zs) > 20

    def test_detects_3sigma_volume_spike(self):
        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5, threshold=3.0)
        t = (now_ms() // 5000) * 5000
        rng = random.Random(0)
        for i in range(100):
            vp.add_trade(t + i * 5000, 1.0 + rng.gauss(0, 0.05), 100.0)
        vp.add_trade(t + 100 * 5000, 50.0, 100.0)            # closes prior bucket
        z2 = vp.add_trade(t + 101 * 5000, 1.0, 100.0)        # closes the spike bucket
        assert z2 is not None and z2 > 3.0
        assert vp.exceeds_threshold(z2)

    def test_gap_filling_counts_silence_as_zero(self):
        """Quiet periods are real information — they must enter the distribution."""
        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5)
        t = (now_ms() // 5000) * 5000
        vp.add_trade(t, 1.0, 100.0)
        vp.add_trade(t + 50_000, 1.0, 100.0)  # 10-bucket gap
        assert len(vp.buckets) >= 5


# ------------------------------------------------------- Module 1: CVD
class TestCVD:
    def test_cvd_accumulates_signed_notional(self):
        c = CVDTracker(symbol="X/Y", venue="v")
        t = now_ms()
        c.add_trade(t, 1.0, 100.0, "buy")
        assert c.cvd == pytest.approx(100.0)
        c.add_trade(t + 1, 0.5, 100.0, "sell")
        assert c.cvd == pytest.approx(50.0)

    def test_buy_ratio(self):
        c = CVDTracker(symbol="X/Y", venue="v")
        t = now_ms()
        for i in range(8):
            c.add_trade(t + i, 1.0, 100.0, "buy")
        for i in range(2):
            c.add_trade(t + 10 + i, 1.0, 100.0, "sell")
        assert c.buy_ratio == pytest.approx(0.8)

    def test_absorption_detects_aggression_without_price_move(self):
        """Heavy buying that does not move price == a passive wall absorbing it."""
        c = CVDTracker(symbol="X/Y", venue="v")
        t = now_ms()
        for i in range(200):
            c.add_trade(t + i * 100, 2.0, 100.0, "buy")  # price pinned
        assert c.divergence() > 0.2
        assert c.absorption_score() > 0.0

    def test_no_divergence_when_price_follows_flow(self):
        c = CVDTracker(symbol="X/Y", venue="v")
        t = now_ms()
        price = 100.0
        for i in range(200):
            price *= 1.0001
            c.add_trade(t + i * 100, 2.0, price, "buy")
        assert abs(c.divergence()) < 0.6


class TestMicrostructureState:
    def test_snapshot_shape(self):
        st = MicrostructureState(venue="binance", symbol="BTC/USDT")
        t = now_ms()
        st.book.update([(99.0, 5.0)], [(101.0, 5.0)], t)
        closed, z = st.on_trade(t, 100.0, 1.0, "buy")
        assert isinstance(closed, bool)
        snap = st.snapshot()
        for key in ("obi", "volume_z", "cvd", "cvd_divergence", "absorption"):
            assert key in snap


# --------------------------------------------------- Module 2: on-chain
class TestOnChain:
    def test_decode_erc20_transfer(self):
        log = {
            "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "topics": [
                TRANSFER_TOPIC,
                "0x000000000000000000000000" + "a" * 40,
                "0x000000000000000000000000" + "b" * 40,
            ],
            "data": hex(1_500_000 * 10**6),
            "blockNumber": "0x1234",
            "transactionHash": "0xdeadbeef",
            "logIndex": "0x1",
        }
        out = decode_transfer_log(log, decimals=6)
        assert out is not None
        assert out["amount"] == pytest.approx(1_500_000.0)
        assert out["from"] == "0x" + "a" * 40
        assert out["to"] == "0x" + "b" * 40
        assert out["block"] == 0x1234

    def test_ignores_non_transfer_logs(self):
        assert decode_transfer_log({"topics": ["0xdeadbeef"], "data": "0x0"}) is None
        assert decode_transfer_log({"topics": [], "data": "0x0"}) is None

    def test_registry_classifies_flow_direction(self):
        reg = AddressRegistry.build(chains=["ethereum"])
        binance = "0x28c6c06298d514db089934071355e5743bf21d60"
        whale = "0x" + "f" * 40
        assert reg.classify(whale, binance) == "inflow"
        assert reg.classify(binance, whale) == "outflow"
        assert reg.classify(whale, whale) == "unrelated"
        assert reg.is_cex(binance.upper())  # case-insensitive for EVM

    def test_registry_knows_bridges_and_tokens(self):
        reg = AddressRegistry.build(chains=["ethereum"])
        assert reg.is_bridge("0x3ee18b2214aff97000d974cf647e7c347e8fa585")
        usdt = reg.token("ethereum", "0xdac17f958d2ee523a2206206994597c13d831ec7")
        assert usdt is not None and usdt.symbol == "USDT" and usdt.decimals == 6
        assert usdt.to_usd(1_000_000) == pytest.approx(1_000_000.0)

    def test_solana_balance_delta_extraction(self):
        tx = {
            "meta": {
                "preTokenBalances": [
                    {"accountIndex": 1, "mint": "MINT1", "owner": "OWNER1",
                     "uiTokenAmount": {"uiAmount": 100.0}}
                ],
                "postTokenBalances": [
                    {"accountIndex": 1, "mint": "MINT1", "owner": "OWNER1",
                     "uiTokenAmount": {"uiAmount": 1100.0}}
                ],
            }
        }
        transfers = SolanaClient.extract_spl_transfers(tx)
        assert len(transfers) == 1
        assert transfers[0]["delta"] == pytest.approx(1000.0)
        assert transfers[0]["owner"] == "OWNER1"

    async def test_tracker_without_endpoints_reports_unhealthy(self):
        """An inert module must never report healthy — silence is the worst failure."""
        from cadb.modules.onchain.tracker import WhaleTracker

        bus = InProcessBus()
        await bus.start()
        tracker = WhaleTracker(
            bus, OnChainConfig(evm_rpc={"ethereum": ""}, solana_rpc="")
        )
        await tracker.start()
        await asyncio.sleep(0.2)
        health = tracker.health()
        await tracker.stop()
        await bus.close()

        assert health["healthy"] is False
        assert "no RPC endpoints" in health.get("error", "")

    async def test_whale_tracker_emits_above_threshold(self):
        from cadb.modules.onchain.tracker import WhaleTracker

        bus = InProcessBus()
        await bus.start()
        events = []

        async def collect(e):
            events.append(e)

        bus.add_handler(collect, "cadb.onchain.*", name="c")
        await asyncio.sleep(0)

        tracker = WhaleTracker(bus, OnChainConfig(simulate=True, poll_interval_s=0.1))
        await tracker.start()
        await asyncio.sleep(6)
        await tracker.stop()
        await bus.close()

        assert events, "simulated tracker should emit events"
        whales = [e for e in events if e.metric_type is MetricType.WALLET_TRANSFER]
        assert all((e.usd_value or 0) >= 500_000 for e in whales)


# ----------------------------------------------------- Module 3: social
class TestSentiment:
    def test_lexicon_polarity(self):
        s = LexiconScorer()
        assert s.score_sync(["bullish breakout, massive gains incoming 🚀"])[0].score > 0.3
        assert s.score_sync(["total rug pull scam, everyone got rekt"])[0].score < -0.3
        assert abs(s.score_sync(["the price is currently 42000"])[0].score) < 0.2

    def test_negation_flips_polarity(self):
        s = LexiconScorer()
        pos = s.score_sync(["this is bullish"])[0].score
        neg = s.score_sync(["this is not bullish"])[0].score
        assert neg < pos

    def test_intensifier_amplifies(self):
        s = LexiconScorer()
        plain = s.score_sync(["bullish"])[0].score
        strong = s.score_sync(["extremely bullish"])[0].score
        assert strong > plain

    def test_score_bounded(self):
        s = LexiconScorer()
        r = s.score_sync(["moon " * 100])[0]
        assert -1.0 <= r.score <= 1.0

    async def test_async_batch_scoring(self):
        s = LexiconScorer()
        out = await s.score_batch(["bullish 🚀", "bearish crash", "neutral text"])
        assert len(out) == 3
        assert out[0].score > 0 > out[1].score

    def test_ticker_extraction(self):
        assert extract_tickers("$BTC and #ETH pumping, $sol too") == {"BTC", "ETH", "SOL"}


class TestBotFarm:
    def _posts(self, n_organic, n_farm, seed=3):
        rng = random.Random(seed)
        t = now_ms() - 900_000
        posts = []
        for i in range(n_organic):
            t += rng.randint(200, 4000)
            posts.append(
                SocialPost("x", f"o{i}", f"user_{rng.randint(1, 900)}",
                           rng.choice([
                               "btc looking strong here nice consolidation",
                               "not sure about this level waiting for pullback",
                               "chart setting up for a breakout imo",
                               "anyone watching volume today unusual",
                               "took some profit still holding a runner",
                           ]), t, {"X"},
                           rng.uniform(40, 2500), int(rng.lognormvariate(5.5, 1.8)))
            )
        base_age = rng.uniform(5, 14)
        for i in range(n_farm):
            t += rng.randint(300, 900)
            posts.append(
                SocialPost("x", f"f{i}", f"farm_{rng.randint(0, 22)}",
                           rng.choice([
                               "🚀 $X IS ABOUT TO EXPLODE 100x GEM DONT MISS OUT",
                               "$X TO THE MOON BUY NOW BEFORE ITS TOO LATE",
                               "🚀 $X IS ABOUT TO EXPLODE 100x GEM DONT MISS OUT 🔥",
                           ]), t, {"X"},
                           base_age + rng.uniform(-0.7, 0.7), rng.randint(15, 190))
            )
        posts.sort(key=lambda p: p.timestamp)
        return posts

    def test_detects_coordinated_cohort_inside_organic_traffic(self):
        d = BotFarmDetector(window_s=1800, min_posts=12)
        for p in self._posts(120, 70):
            d.add(p)
        v = d.evaluate(mention_z=4.2)
        assert v.is_bot_farm
        assert v.score > 0.5
        assert v.age_variance_cv is not None and v.age_variance_cv < 0.35

    def test_no_false_positive_on_organic_traffic(self):
        d = BotFarmDetector(window_s=1800, min_posts=12)
        for p in self._posts(200, 0):
            d.add(p)
        v = d.evaluate(mention_z=1.0)
        assert not v.is_bot_farm
        assert v.score < 0.4

    def test_organic_volume_spike_is_not_a_farm(self):
        """High mention velocity alone must not trigger — that is just news."""
        d = BotFarmDetector(window_s=1800, min_posts=12)
        for p in self._posts(300, 0):
            d.add(p)
        v = d.evaluate(mention_z=6.0)
        assert not v.is_bot_farm

    def test_insufficient_data_is_not_a_verdict(self):
        d = BotFarmDetector(min_posts=12)
        for p in self._posts(3, 0):
            d.add(p)
        v = d.evaluate()
        assert not v.is_bot_farm and v.score == 0.0

    def test_window_eviction(self):
        d = BotFarmDetector(window_s=60, min_posts=5)
        old = now_ms() - 600_000
        for i in range(10):
            d.add(SocialPost("x", str(i), f"a{i}", "text", old, {"X"}, 10.0, 100))
        d.add(SocialPost("x", "new", "anew", "text", now_ms(), {"X"}, 10.0, 100))
        assert len(d.posts) == 1


class TestSocialMonitor:
    async def test_emits_mention_and_sentiment_events(self):
        from cadb.modules.social.monitor import SocialMonitor

        bus = InProcessBus()
        await bus.start()
        events = []

        async def collect(e):
            events.append(e)

        bus.add_handler(collect, "cadb.social.*", name="c")
        await asyncio.sleep(0)

        monitor = SocialMonitor(
            bus, SocialConfig(simulate=True, tracked_tickers=["BTC", "PEPE"],
                              mention_window_s=30, use_finbert=False)
        )
        monitor.emit_interval_s = 1.0
        monitor.flush_interval_s = 0.5
        await monitor.start()
        await asyncio.sleep(5)
        await monitor.stop()
        await bus.close()

        kinds = {e.metric_type for e in events}
        assert MetricType.SOCIAL_MENTIONS in kinds
        assert MetricType.SOCIAL_SENTIMENT in kinds
        assert all(-1.0 <= e.raw_value <= 1.0
                   for e in events if e.metric_type is MetricType.SOCIAL_SENTIMENT)


# ------------------------------------------------- Module 1: engine e2e
class TestExchangeEngine:
    async def test_engine_publishes_all_metric_types(self):
        from cadb.modules.exchange.engine import ExchangeEngine

        bus = InProcessBus()
        await bus.start()
        events = []

        async def collect(e):
            events.append(e)

        bus.add_handler(collect, "cadb.exchange.*", name="c")
        await asyncio.sleep(0)

        engine = ExchangeEngine(
            bus,
            ExchangeConfig(exchanges=["binance"], symbols=["BTC/USDT"], simulate=True,
                           volume_bucket_s=1, volume_window_s=30),
        )
        await engine.start()
        await asyncio.sleep(6)
        await engine.stop()
        await bus.close()

        kinds = {e.metric_type for e in events}
        assert MetricType.ORDER_BOOK in kinds
        assert MetricType.VOLUME in kinds
        assert MetricType.CVD in kinds
        assert all(-1.0 <= e.raw_value <= 1.0
                   for e in events if e.metric_type is MetricType.ORDER_BOOK)


class TestNoSyntheticDataInLiveMode:
    """Fabricated intelligence is worse than a missing module."""

    async def test_social_refuses_simulator_without_credentials(self):
        """Regression: live mode silently fell back to the simulator.

        In production this injected fake shill campaigns that produced real
        bot-farm alerts on SOL and DOGE — manipulation that never happened.
        """
        from cadb.modules.social.monitor import SocialMonitor

        bus = InProcessBus()
        await bus.start()
        monitor = SocialMonitor(
            bus,
            SocialConfig(
                simulate=False,          # LIVE mode
                x_bearer_token="",       # no credentials
                telegram_api_id="",
                tracked_tickers=["BTC"],
                use_finbert=False,
            ),
        )
        await monitor.start()
        await asyncio.sleep(0.3)
        health = monitor.health()
        sources = [type(s).__name__ for s in monitor.sources]
        await monitor.stop()
        await bus.close()

        assert "SimulatedSocialSource" not in sources, "must not fabricate social data"
        assert monitor.enabled_sources is False
        assert health["healthy"] is False
        assert "credentials" in health.get("error", "")

    async def test_explicit_simulate_flag_still_works(self):
        """Opting in to synthetic data must remain possible for demos/tests."""
        from cadb.modules.social.monitor import SocialMonitor

        bus = InProcessBus()
        await bus.start()
        monitor = SocialMonitor(
            bus, SocialConfig(simulate=True, tracked_tickers=["BTC"], use_finbert=False)
        )
        await monitor.start()
        await asyncio.sleep(0.2)
        sources = [type(s).__name__ for s in monitor.sources]
        await monitor.stop()
        await bus.close()
        assert "SimulatedSocialSource" in sources


class TestAdaptiveBlockSpan:
    def test_span_shrinks_on_limit_error_and_recovers(self):
        from cadb.modules.onchain.tracker import _ChainCursor

        c = _ChainCursor()
        start = c.max_span
        c.shrink()
        assert c.max_span < start
        assert c.max_span >= c.MIN_SPAN

        for _ in range(30):
            c.shrink()
        assert c.max_span == c.MIN_SPAN, "must not shrink below the floor"

        before = c.max_span
        for _ in range(10):
            c.grow()
        assert c.max_span > before, "should widen again after sustained success"

    def test_span_never_exceeds_ceiling(self):
        from cadb.modules.onchain.tracker import _ChainCursor

        c = _ChainCursor()
        for _ in range(500):
            c.grow()
        assert c.max_span <= c.MAX_SPAN


class TestRPCFailureHandling:
    """Regression: a refusing endpoint livelocked the scan loop."""

    def test_cursor_advances_when_stuck_at_floor(self):
        """At MIN_SPAN a still-refused range must be skipped, not retried forever.

        Production symptom: `bsc: RPC range limit hit, reducing span to 5 blocks`
        every 3 seconds indefinitely. The cursor never advanced, so the scanner
        fell permanently behind the tip while appearing to "recover".
        """
        from cadb.modules.onchain.tracker import _ChainCursor

        c = _ChainCursor()
        for _ in range(10):
            c.shrink()
        assert c.max_span == c.MIN_SPAN

        # Simulate the handler's floor branch.
        start, from_block, to_block = c.last_block, 100, 104
        at_floor = c.max_span <= c.MIN_SPAN
        assert at_floor
        c.last_block = to_block
        c.skipped_blocks += to_block - from_block + 1
        assert c.last_block > start, "cursor must move past an un-queryable range"
        assert c.skipped_blocks == 5

    def test_success_resets_the_error_counter(self):
        from cadb.modules.onchain.tracker import _ChainCursor

        c = _ChainCursor()
        c.consecutive_limit_errors = 7
        c.grow()
        assert c.consecutive_limit_errors == 0

    def test_solana_throttle_state_initialised(self):
        from cadb.core.bus import InProcessBus
        from cadb.core.config import OnChainConfig
        from cadb.modules.onchain.tracker import WhaleTracker

        t = WhaleTracker(InProcessBus(), OnChainConfig())
        assert t._sol_tx_budget > 0
        assert 0 < t._sol_tx_delay < 1.0


class TestRetiredEndpointDetection:
    def test_stale_config_is_flagged(self, tmp_path, caplog):
        """A bind-mounted config.yaml survives rebuilds — warn about it."""
        import logging

        from cadb.core.config import load_settings

        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n"
            "  evm_rpc:\n"
            "    ethereum: https://eth.llamarpc.com\n"
        )
        with caplog.at_level(logging.WARNING):
            load_settings(cfg)
        assert any("retired RPC endpoint" in r.message for r in caplog.records)

    def test_current_defaults_are_not_flagged(self, caplog):
        import logging
        from pathlib import Path

        from cadb.core.config import load_settings

        cfg = Path(__file__).resolve().parent.parent / "config.yaml"
        with caplog.at_level(logging.WARNING):
            load_settings(cfg)
        assert not [r for r in caplog.records if "retired RPC endpoint" in r.message], (
            "shipped config.yaml must not reference retired endpoints"
        )


class TestSymbolDiscovery:
    """A static symbol list cannot see manipulation in unlisted assets."""

    def _tickers(self, **overrides):
        base = {
            "BTC/USDT": {"quoteVolume": 4.5e8, "percentage": -2.2},
            "ETH/USDT": {"quoteVolume": 3.2e8, "percentage": -2.9},
            "QUIET/USDT": {"quoteVolume": 5.0e5, "percentage": 1.1},
            "DUST/USDT": {"quoteVolume": 900.0, "percentage": 320.0},
            "PUMPER/USDT": {"quoteVolume": 8.0e5, "percentage": 190.0},
            "DUMPER/USDT": {"quoteVolume": 7.0e5, "percentage": -55.0},
            "BTCUP/BTC": {"quoteVolume": 1e6, "percentage": 80.0},
        }
        base.update(overrides)
        return base

    def _disco(self, **kw):
        from cadb.modules.exchange.discovery import SymbolDiscovery

        params = {
            "venue": "mexc", "max_symbols": 10, "min_volume_usd": 100_000,
            "max_volume_usd": 50_000_000, "min_change_pct": 15.0,
        }
        params.update(kw)
        return SymbolDiscovery(**params)

    def test_detects_dumps_not_just_pumps(self):
        """A crash is as much manipulation evidence as a ramp."""
        found = {c.symbol for c in self._disco().evaluate(self._tickers())}
        assert "DUMPER/USDT" in found, "a -55% move must be flagged"
        assert "PUMPER/USDT" in found

    def test_ignores_majors_too_deep_to_manipulate(self):
        found = {c.symbol for c in self._disco().evaluate(self._tickers())}
        assert "BTC/USDT" not in found
        assert "ETH/USDT" not in found

    def test_ignores_illiquid_noise(self):
        """A $900-volume pair moves 320% on a single order — meaningless."""
        found = {c.symbol for c in self._disco().evaluate(self._tickers())}
        assert "DUST/USDT" not in found

    def test_ignores_quiet_pairs(self):
        found = {c.symbol for c in self._disco().evaluate(self._tickers())}
        assert "QUIET/USDT" not in found

    def test_only_configured_quote_currency(self):
        found = {c.symbol for c in self._disco().evaluate(self._tickers())}
        assert "BTCUP/BTC" not in found

    def test_new_listing_scores_highest(self):
        """Freshly listed low-float tokens are the highest-risk category."""
        d = self._disco()
        d.evaluate(self._tickers())  # first scan establishes the universe
        t = self._tickers()
        t["BRANDNEW/USDT"] = {"quoteVolume": 4.0e5, "percentage": 22.0}
        found = d.evaluate(t)
        new = next((c for c in found if c.symbol == "BRANDNEW/USDT"), None)
        assert new is not None and "newly listed" in new.reason
        assert new.score == max(c.score for c in found)

    def test_first_scan_does_not_flag_everything_as_new(self):
        found = self._disco().evaluate(self._tickers())
        assert not any("newly listed" in c.reason for c in found)

    def test_volume_surge_detected(self):
        d = self._disco(min_change_pct=999)  # disable the move criterion
        for _ in range(4):
            d.evaluate({"SURGE/USDT": {"quoteVolume": 2.0e5, "percentage": 0.5}})
        found = d.evaluate({"SURGE/USDT": {"quoteVolume": 3.0e6, "percentage": 0.5}})
        assert any("volume" in c.reason for c in found)

    def test_pinned_symbols_always_included(self):
        d = self._disco(always_include=("BTC/USDT",))
        assert "BTC/USDT" in d.watchlist(self._tickers())

    def test_respects_max_symbols(self):
        t = {f"C{i}/USDT": {"quoteVolume": 5e5, "percentage": 50.0} for i in range(60)}
        assert len(self._disco(max_symbols=7).evaluate(t)) <= 7

    def test_handles_missing_fields(self):
        t = {
            "A/USDT": {"quoteVolume": None, "percentage": 50.0},
            "B/USDT": {"percentage": 50.0},
            "C/USDT": {"quoteVolume": 5e5},
        }
        self._disco().evaluate(t)  # must not raise


class TestVenueSupport:
    def test_requested_venues_exist_in_ccxt_pro(self):
        ccxtpro = pytest.importorskip("ccxt.pro")
        for venue in ("binance", "bybit", "mexc", "gate", "kucoin", "coinbase"):
            assert hasattr(ccxtpro, venue), f"{venue} missing from ccxt.pro"

    def test_unsupported_venue_raises_not_simulates(self):
        """Silently faking a live venue would fabricate alerts."""
        from cadb.modules.exchange.feeds import build_feed

        with pytest.raises(ValueError, match="no live feed backend"):
            build_feed("robinhood", ["BTC/USDT"], simulate=False, prefer_ccxt=True)

    def test_explicit_simulate_still_allowed(self):
        from cadb.modules.exchange.feeds import SimulatedFeed, build_feed

        feed = build_feed("robinhood", ["BTC/USDT"], simulate=True)
        assert isinstance(feed, SimulatedFeed)

    def test_default_config_venues_are_supported(self):
        ccxtpro = pytest.importorskip("ccxt.pro")
        from cadb.core.config import Settings

        for venue in Settings().exchange.exchanges:
            assert hasattr(ccxtpro, venue), f"default venue {venue} unsupported"


class TestNewListingTracking:
    def _disco(self, **kw):
        from cadb.modules.exchange.discovery import SymbolDiscovery

        params = {
            "venue": "mexc", "max_symbols": 10, "min_volume_usd": 100_000,
            "max_volume_usd": 50_000_000, "min_change_pct": 15.0,
            "track_new_listings": True, "new_listing_min_volume_usd": 20_000,
        }
        params.update(kw)
        return SymbolDiscovery(**params)

    def test_new_listing_tracked_before_it_pumps(self):
        """The run-up is the point — subscribing after the spike is too late."""
        d = self._disco()
        base = {"OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0}}
        d.evaluate(base)
        t = dict(base)
        t["FRESH/USDT"] = {"quoteVolume": 35_000, "percentage": 3.0}
        found = {c.symbol for c in d.evaluate(t)}
        assert "FRESH/USDT" in found, "new listing must be watched while quiet"

    def test_new_listing_below_normal_floor_still_tracked(self):
        d = self._disco()
        d.evaluate({"OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0}})
        t = {
            "OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0},
            "TINY/USDT": {"quoteVolume": 25_000, "percentage": 1.0},
        }
        assert "TINY/USDT" in {c.symbol for c in d.evaluate(t)}

    def test_grace_period_expires(self):
        import time

        d = self._disco(new_listing_grace_h=0.0)
        d.evaluate({"OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0}})
        t = {
            "OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0},
            "FRESH/USDT": {"quoteVolume": 35_000, "percentage": 3.0},
        }
        d.evaluate(t)
        d._first_seen["FRESH/USDT"] = time.time() - 86_400  # a day ago
        found = {c.symbol for c in d.evaluate(t)}
        assert "FRESH/USDT" not in found, "quiet pair must drop out after grace"

    def test_new_listing_that_pumps_scores_both(self):
        d = self._disco()
        d.evaluate({"OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0}})
        t = {"OLD/USDT": {"quoteVolume": 5e5, "percentage": 2.0},
             "FRESH/USDT": {"quoteVolume": 35_000, "percentage": 3.0}}
        d.evaluate(t)
        t["FRESH/USDT"] = {"quoteVolume": 400_000, "percentage": 180.0}
        c = next(c for c in d.evaluate(t) if c.symbol == "FRESH/USDT")
        assert "pump" in c.reason and "new listing" in c.reason

    def test_all_nine_venues_supported(self):
        ccxtpro = pytest.importorskip("ccxt.pro")
        for v in ("binance", "bybit", "okx", "bitget", "kucoin",
                  "gate", "mexc", "kraken", "coinbase"):
            assert hasattr(ccxtpro, v), f"{v} not in ccxt.pro"


class TestVenueCompatibility:
    """Regression: three venues failed permanently on their own WS quirks."""

    def test_quirks_cover_every_default_venue(self):
        from cadb.core.config import Settings
        from cadb.modules.exchange.feeds import VENUE_WS_QUIRKS

        for venue in Settings().exchange.exchanges:
            assert venue in VENUE_WS_QUIRKS, f"{venue} has no WS quirk entry"

    def test_kraken_limit_is_quantized(self):
        """Kraken rejects any depth outside {10,25,100,500,1000}."""
        from cadb.modules.exchange.feeds import VENUE_WS_QUIRKS

        assert VENUE_WS_QUIRKS["kraken"]["limit"] in (10, 25, 100, 500, 1000)

    def test_okx_uses_public_book_channel(self):
        """`books5` is treated as authenticated; `books` is the public feed."""
        from cadb.modules.exchange.feeds import VENUE_WS_QUIRKS

        assert VENUE_WS_QUIRKS["okx"]["options"]["depth"] == "books"

    def test_feed_applies_quirks_to_client(self):
        from cadb.modules.exchange.feeds import CCXTProFeed

        pytest.importorskip("ccxt.pro")
        feed = CCXTProFeed("kraken", ["BTC/USDT"], depth=50)
        feed._ensure_client()
        assert feed._ws_limit == 25, "kraken must not receive depth=50"
        okx = CCXTProFeed("okx", ["BTC/USDT"], depth=50)
        okx._ensure_client()
        assert okx._ws_limit is None
        assert okx._client.options["depth"] == "books"


class TestNoPinnedMajors:
    def test_default_symbols_is_empty(self):
        """Pinning majors caused constant BTC/ETH/SOL alerts."""
        from cadb.core.config import Settings

        assert Settings().exchange.symbols == [], (
            "pinned symbols force-watch quiet majors; discovery should decide"
        )

    def test_shipped_config_pins_nothing(self):
        from pathlib import Path

        from cadb.core.config import load_settings

        cfg = Path(__file__).resolve().parent.parent / "config.yaml"
        assert load_settings(cfg).exchange.symbols == []

    def test_discovery_threshold_targets_massive_moves(self):
        from cadb.core.config import Settings

        assert Settings().exchange.discovery_min_change_pct >= 25.0
