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
        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5, threshold=3.0)
        t = (now_ms() // 5000) * 5000
        zs = []
        for i in range(80):
            z = vp.add_trade(t + i * 5000, 1.0, 100.0)
            if z is not None:
                zs.append(z)
        assert len(zs) > 20
        assert vp.total_trades == 80

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
        st.on_trade(t, 100.0, 1.0, "buy")
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
