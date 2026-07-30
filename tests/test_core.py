"""Core primitives: schema, bus, statistics, resilience, config."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from cadb.core.bus import InProcessBus
from cadb.core.config import Settings, load_settings
from cadb.core.resilience import BackoffPolicy, CircuitBreaker, CircuitState, RateLimiter
from cadb.core.schema import MarketEvent, MetricType, Severity, SourceType, now_ms
from cadb.core.stats import (
    CusumDetector,
    DynamicZScore,
    EWMAZScore,
    RobustZScore,
    RollingWindow,
)


# ---------------------------------------------------------------- schema
class TestSchema:
    def test_event_normalises_symbol_and_venue(self):
        e = MarketEvent(
            source_type=SourceType.EXCHANGE, venue="  BINANCE ", asset_pair=" btc/usdt ",
            metric_type=MetricType.VOLUME, raw_value=1.0,
        )
        assert e.venue == "binance"
        assert e.asset_pair == "BTC/USDT"
        assert e.base_asset == "BTC"

    def test_channel_routing(self):
        e = MarketEvent(
            source_type=SourceType.ONCHAIN, venue="ethereum", asset_pair="USDT",
            metric_type=MetricType.WALLET_TRANSFER, raw_value=1.0,
        )
        assert e.channel == "cadb.onchain.wallet_transfer"

    def test_rejects_non_finite(self):
        for bad in (float("nan"), float("inf")):
            with pytest.raises(ValueError):
                MarketEvent(
                    source_type=SourceType.EXCHANGE, venue="x", asset_pair="A/B",
                    metric_type=MetricType.VOLUME, raw_value=bad,
                )

    def test_wire_roundtrip_preserves_fields(self):
        e = MarketEvent(
            source_type=SourceType.SOCIAL, venue="aggregate", asset_pair="PEPE",
            metric_type=MetricType.SOCIAL_MENTIONS, raw_value=42.5,
            normalized_z_score=3.2, meta={"acceleration": 1.5},
        )
        back = MarketEvent.from_wire(e.to_wire())
        assert back.raw_value == e.raw_value
        assert back.normalized_z_score == e.normalized_z_score
        assert back.metric_type is e.metric_type
        assert back.meta["acceleration"] == 1.5

    def test_severity_ordering(self):
        assert Severity.CRITICAL.rank > Severity.HIGH.rank > Severity.MEDIUM.rank
        assert Severity.INFO.rank == 0

    def test_immutability(self):
        e = MarketEvent(
            source_type=SourceType.EXCHANGE, venue="v", asset_pair="A/B",
            metric_type=MetricType.VOLUME, raw_value=1.0,
        )
        with pytest.raises(ValidationError):
            e.raw_value = 2.0  # type: ignore[misc]


# ------------------------------------------------------------------- bus
class TestBus:
    def _event(self, metric=MetricType.VOLUME, value=1.0, source=SourceType.EXCHANGE):
        return MarketEvent(
            source_type=source, venue="binance", asset_pair="BTC/USDT",
            metric_type=metric, raw_value=value,
        )

    async def test_publish_and_stream(self):
        bus = InProcessBus()
        await bus.start()
        received: list[MarketEvent] = []

        async def handler(e: MarketEvent) -> None:
            received.append(e)

        bus.add_handler(handler, "cadb.exchange.*", name="t")
        await asyncio.sleep(0)
        for i in range(5):
            await bus.publish(self._event(value=float(i)))
        await asyncio.sleep(0.05)
        assert len(received) == 5
        assert bus.stats.published == 5
        await bus.close()

    async def test_pattern_filtering(self):
        bus = InProcessBus()
        await bus.start()
        ex: list[MarketEvent] = []
        oc: list[MarketEvent] = []

        bus.add_handler(lambda e: _append(ex, e), "cadb.exchange.*", name="ex")
        bus.add_handler(lambda e: _append(oc, e), "cadb.onchain.*", name="oc")
        await asyncio.sleep(0)

        await bus.publish(self._event())
        await bus.publish(
            MarketEvent(
                source_type=SourceType.ONCHAIN, venue="ethereum", asset_pair="USDT",
                metric_type=MetricType.WALLET_TRANSFER, raw_value=1e6,
            )
        )
        await asyncio.sleep(0.05)
        assert len(ex) == 1 and len(oc) == 1
        await bus.close()

    async def test_backpressure_drops_oldest_not_newest(self):
        """A lagging subscriber must lose stale ticks, never the freshest."""
        bus = InProcessBus(queue_size=5)
        await bus.start()
        sub = bus.subscribe("cadb.*", name="slow")
        for i in range(20):
            await bus.publish(self._event(value=float(i)))
        drained = []
        while not sub.queue.empty():
            drained.append(sub.queue.get_nowait().raw_value)
        assert bus.stats.dropped > 0
        assert drained[-1] == 19.0, "freshest tick must survive"
        await bus.close()

    async def test_handler_exception_does_not_kill_bus(self):
        bus = InProcessBus()
        await bus.start()
        good: list[MarketEvent] = []

        async def bad(_e):
            raise RuntimeError("boom")

        bus.add_handler(bad, "cadb.*", name="bad")
        bus.add_handler(lambda e: _append(good, e), "cadb.*", name="good")
        await asyncio.sleep(0)
        await bus.publish(self._event())
        await asyncio.sleep(0.05)
        assert len(good) == 1
        assert bus.stats.errors >= 1
        await bus.close()


async def _append(target: list, event) -> None:
    target.append(event)


# ----------------------------------------------------------------- stats
class TestStats:
    def test_rolling_window_evicts_by_time(self):
        w = RollingWindow(window_ms=1000)
        t = now_ms()
        for i in range(10):
            w.add(t + i * 100, 1.0)
        assert len(w) == 10
        w.add(t + 5000, 1.0)
        assert len(w) == 1

    def test_rolling_window_moments(self):
        w = RollingWindow(window_ms=10_000)
        t = now_ms()
        for i, v in enumerate([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]):
            w.add(t + i, v)
        assert w.mean == pytest.approx(5.0)
        assert w.std == pytest.approx(2.138, abs=0.01)  # sample std

    def test_zscore_none_until_warmup(self):
        w = RollingWindow(window_ms=60_000)
        t = now_ms()
        for i in range(5):
            w.add(t + i, 1.0)
        assert w.zscore(10.0, min_samples=20) is None

    def test_ewma_detects_spike(self):
        e = EWMAZScore(half_life_s=30, warmup=10)
        t = now_ms()
        for i in range(60):
            e.update(10.0 + (i % 3) * 0.1, t + i * 1000)
        z = e.score(25.0)
        assert z is not None and z > 3.0

    def test_robust_resists_outlier_poisoning(self):
        """The key property: one huge print must not blind the next detection."""
        robust = RobustZScore(window=200, warmup=20)
        classic = EWMAZScore(half_life_s=60, warmup=20)
        t = now_ms()
        for i in range(100):
            robust.update(10.0)
            classic.update(10.0, t + i * 1000)
        # A single 100x print.
        robust.update(1000.0)
        classic.update(1000.0, t + 100_000)
        for i in range(5):
            robust.update(10.0)
            classic.update(10.0, t + (101 + i) * 1000)
        rz = robust.score(30.0)
        cz = classic.score(30.0)
        assert rz is not None and cz is not None
        assert rz > cz, "MAD estimator should stay sensitive after an outlier"

    def test_dynamic_zscore_threshold_adapts_upward(self):
        d = DynamicZScore(base_threshold=3.0, warmup=10, adaptive=True)
        t = now_ms()
        for i in range(80):
            d.update(10.0, t + i * 1000)
        assert d.threshold >= 3.0

    def test_dynamic_zscore_flags_spike(self):
        d = DynamicZScore(half_life_s=60, warmup=20, base_threshold=3.0)
        t = now_ms()
        for i in range(120):
            d.update(100.0 + (i % 5), t + i * 1000)
        z = d.update(400.0, t + 120_000)
        assert z is not None and z > 3.0
        assert d.is_anomalous(z)

    def test_cusum_detects_level_shift(self):
        c = CusumDetector(drift=0.5, threshold=5.0)
        assert all(c.update(0.1) == 0 for _ in range(10))
        shifts = [c.update(2.0) for _ in range(10)]
        assert 1 in shifts


# ------------------------------------------------------------ resilience
class TestResilience:
    def test_backoff_grows_and_caps(self):
        p = BackoffPolicy(initial=1.0, maximum=10.0, multiplier=2.0, jitter=False)
        delays = [p.next_delay() for _ in range(8)]
        assert delays[0] == 1.0
        assert all(d <= 10.0 for d in delays)
        assert delays[-1] == 10.0

    def test_backoff_jitter_spreads_reconnects(self):
        p = BackoffPolicy(initial=1.0, maximum=30.0, jitter=True)
        samples = set()
        for _ in range(20):
            p.reset()
            samples.add(round(p.next_delay(), 4))
        assert len(samples) > 5, "jitter must decorrelate reconnect timing"

    def test_backoff_reset(self):
        p = BackoffPolicy(initial=1.0, maximum=60.0, jitter=False)
        for _ in range(5):
            p.next_delay()
        p.reset()
        assert p.next_delay() == 1.0

    async def test_circuit_breaker_opens_then_recovers(self):
        cb = CircuitBreaker(name="t", failure_threshold=3, recovery_timeout=0.1,
                            half_open_successes=1)

        async def fail():
            raise RuntimeError("nope")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await cb.call(fail)
        assert cb.state is CircuitState.OPEN

        from cadb.core.resilience import CircuitOpenError

        with pytest.raises(CircuitOpenError):
            await cb.call(fail)

        await asyncio.sleep(0.15)

        async def ok():
            return 42

        assert await cb.call(ok) == 42
        assert cb.state is CircuitState.CLOSED

    async def test_rate_limiter_enforces_rate(self):
        limiter = RateLimiter(rate_per_sec=50, burst=1)
        loop = asyncio.get_event_loop()
        start = loop.time()
        for _ in range(5):
            await limiter.acquire()
        assert loop.time() - start >= 0.05


# ---------------------------------------------------------------- config
class TestConfig:
    def test_defaults_are_valid(self):
        s = Settings()
        assert "binance" in s.exchange.exchanges
        assert s.ml.alert_threshold == 80.0
        assert s.onchain.whale_threshold_usd == 500_000.0
        assert s.onchain.liquidity_drop_pct == 30.0
        assert s.exchange.volume_z_threshold == 3.0

    def test_yaml_and_env_expansion(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_TOKEN_XYZ", "secret-value")
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "alerts:\n"
            "  telegram_bot_token: ${TEST_TOKEN_XYZ}\n"
            "  min_score: 75\n"
            "exchange:\n"
            "  symbols: [btc/usdt, eth/usdt]\n"
        )
        s = load_settings(cfg)
        assert s.alerts.telegram_bot_token == "secret-value"
        assert s.alerts.min_score == 75
        assert s.exchange.symbols == ["BTC/USDT", "ETH/USDT"]

    def test_env_override_takes_precedence(self, tmp_path, monkeypatch):
        cfg = tmp_path / "c.yaml"
        cfg.write_text("ml:\n  alert_threshold: 50\n")
        monkeypatch.setenv("CADB_ALERT_THRESHOLD", "95")
        assert load_settings(cfg).ml.alert_threshold == 95.0

    def test_env_default_syntax(self, tmp_path, monkeypatch):
        monkeypatch.delenv("UNSET_VAR_ABC", raising=False)
        cfg = tmp_path / "c.yaml"
        cfg.write_text("onchain:\n  solana_rpc: ${UNSET_VAR_ABC:-https://fallback.rpc}\n")
        assert load_settings(cfg).onchain.solana_rpc == "https://fallback.rpc"

    def test_empty_env_var_falls_back_to_default(self, tmp_path, monkeypatch):
        """A blank `.env` line must not override a working default.

        Regression: `.env.example` shipped `ETH_RPC_URL=` as a placeholder.
        Sourcing it defined the variable as an empty string, `${VAR:-default}`
        treated that as a deliberate override, and the entire on-chain module
        went inert while still reporting itself healthy.
        """
        monkeypatch.setenv("ETH_RPC_URL", "")
        monkeypatch.setenv("SOLANA_RPC_URL", "")
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n"
            "  evm_rpc:\n"
            "    ethereum: ${ETH_RPC_URL:-https://default.example/eth}\n"
            "  solana_rpc: ${SOLANA_RPC_URL:-https://default.example/sol}\n"
        )
        s = load_settings(cfg)
        assert s.onchain.evm_rpc["ethereum"] == "https://default.example/eth"
        assert s.onchain.solana_rpc == "https://default.example/sol"

    def test_set_env_var_still_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ETH_RPC_URL", "https://mine.example/eth")
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n  evm_rpc:\n    ethereum: ${ETH_RPC_URL:-https://default.example/eth}\n"
        )
        assert load_settings(cfg).onchain.evm_rpc["ethereum"] == "https://mine.example/eth"

    def test_blank_cadb_override_is_ignored(self, tmp_path, monkeypatch):
        """An empty CADB_* override must not wipe out a configured value."""
        monkeypatch.setenv("CADB_ALERT_THRESHOLD", "")
        cfg = tmp_path / "c.yaml"
        cfg.write_text("ml:\n  alert_threshold: 72\n")
        assert load_settings(cfg).ml.alert_threshold == 72.0

    def test_shipped_env_example_keeps_defaults_working(self, tmp_path, monkeypatch):
        """Source the real .env.example and assert nothing breaks."""
        import re
        from pathlib import Path

        example = Path(__file__).resolve().parent.parent / ".env.example"
        for line in example.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            if re.fullmatch(r"[A-Z0-9_]+", key.strip()):
                monkeypatch.setenv(key.strip(), val.strip())

        s = load_settings(
            Path(__file__).resolve().parent.parent / "config.yaml"
        )
        eth = [u for u in s.onchain.evm_rpc["ethereum"].split(",") if u.strip()]
        sol = [u for u in s.onchain.solana_rpc.split(",") if u.strip()]
        assert eth, "on-chain module would be inert with the shipped .env.example"
        assert sol, "solana tracking would be disabled with the shipped .env.example"

    def test_tracked_assets_union(self):
        s = Settings()
        s.exchange.symbols = ["BTC/USDT", "ETH/USDT"]
        s.social.tracked_tickers = ["SOL", "btc"]
        assets = s.tracked_assets()
        assert {"BTC", "ETH", "SOL"}.issubset(set(assets))


class TestDeployScripts:
    """update.sh must handle both supported deployment layouts."""

    def test_update_script_is_valid_bash(self):
        import subprocess
        from pathlib import Path

        script = Path(__file__).resolve().parent.parent / "deploy" / "update.sh"
        assert script.exists()
        r = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert r.returncode == 0, f"syntax error: {r.stderr}"

    def test_update_script_handles_git_clone_layout(self):
        """Regression: `cp deploy/../config.yaml config.yaml` = same-file error.

        In a git clone the repo root *is* the deployment directory, so copying
        the repo's config over itself failed and aborted the update.
        """
        from pathlib import Path

        text = (Path(__file__).resolve().parent.parent / "deploy" / "update.sh").read_text()
        assert "is-inside-work-tree" in text, "must detect a git checkout"
        assert "git checkout -- config.yaml" in text, "git layout should reset, not cp"


class TestBSCEndpointDefaults:
    """BSC endpoints were selected by measurement; guard against regression."""

    def test_no_dataseed_endpoints_in_defaults(self):
        """bsc-dataseed* cannot serve eth_getLogs — verified across 4 variants.

        They answer eth_call and eth_blockNumber normally, so a shallow health
        check passes while Module 2 is completely non-functional.
        """
        from cadb.core.config import Settings

        bsc = Settings().onchain.evm_rpc.get("bsc", "")
        assert "dataseed" not in bsc, f"dataseed endpoint reintroduced: {bsc}"

    def test_bsc_has_failover_endpoints(self):
        from cadb.core.config import Settings

        endpoints = [u for u in Settings().onchain.evm_rpc["bsc"].split(",") if u.strip()]
        assert len(endpoints) >= 2, "BSC needs failover; its public tier is unreliable"

    def test_retired_list_covers_all_tested_failures(self):
        from cadb.core.config import RETIRED_ENDPOINTS

        for host in ("bsc-dataseed", "llamarpc.com", "blockpi.network"):
            assert any(host in k for k in RETIRED_ENDPOINTS), f"{host} not flagged"

    def test_every_chain_default_is_reachable_syntax(self):
        """Defaults must be well-formed URLs with a scheme."""
        from cadb.core.config import Settings

        s = Settings()
        urls = [u for v in s.onchain.evm_rpc.values() for u in v.split(",")]
        urls += s.onchain.solana_rpc.split(",")
        for u in filter(None, (x.strip() for x in urls)):
            assert u.startswith("https://"), f"insecure or malformed endpoint: {u}"


class TestSparseSeriesZScore:
    """Regression: sparse volume buckets produced 50-sigma false positives.

    Production symptom: `volume spike bybit BTC/USDT z=990.38` and a continuous
    stream of score=82.0 "manipulation" alerts on ordinary market activity.
    Cause: for a mostly-zero series MAD collapses to 0, and the degenerate-scale
    fallback treated any non-zero value as maximally significant.
    """

    def test_sparse_counts_do_not_explode(self):
        from cadb.core.stats import RobustZScore

        r = RobustZScore(window=300, warmup=30, zero_is_normal=True)
        for _ in range(40):
            r.update(0.0)
        assert r.score(5.0) is None, "sparse count series must not fabricate sigma"

    def test_flat_price_break_still_detected(self):
        """The opposite case must keep working — a pegged price that moves."""
        from cadb.core.stats import RobustZScore

        r = RobustZScore(window=300, warmup=30, zero_is_normal=False)
        for _ in range(40):
            r.update(100.0)
        z = r.score(105.0)
        assert z is not None and abs(z) > 10

    def test_volume_profile_bounded_on_sparse_data(self):
        """Realistic bursty (log-normal) volume must not produce absurd z."""
        import random

        from cadb.core.schema import now_ms
        from cadb.modules.exchange.microstructure import VolumeProfile

        rng = random.Random(3)
        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5)
        t = (now_ms() // 5000) * 5000
        seen = []
        for i in range(300):
            v = rng.lognormvariate(0, 1.6) if rng.random() > 0.25 else 0.0001
            z = vp.add_trade(t + i * 5000, v, 60000.0)
            if z is not None:
                seen.append(z)
        assert seen, "expected some z-scores on a dispersed series"
        worst = max(abs(z) for z in seen)
        assert worst < 20, f"absurd z on ordinary bursty volume: {worst}"
        flagged = sum(1 for z in seen if vp.exceeds_threshold(z))
        assert flagged / len(seen) < 0.10, f"{flagged}/{len(seen)} false spikes"

    def test_genuine_spike_still_detected(self):
        """Guard against over-correcting into missed detections."""
        import random

        from cadb.core.schema import now_ms
        from cadb.modules.exchange.microstructure import VolumeProfile

        vp = VolumeProfile(symbol="X/Y", venue="v", window_s=300, bucket_s=5)
        t = (now_ms() // 5000) * 5000
        rng = random.Random(0)
        for i in range(100):
            vp.add_trade(t + i * 5000, 1.0 + rng.gauss(0, 0.05), 100.0)
        vp.add_trade(t + 100 * 5000, 50.0, 100.0)
        z = vp.add_trade(t + 101 * 5000, 1.0, 100.0)
        assert z is not None and z > 3.0, "real spikes must still fire"


class TestEndpointSanitisation:
    """Regression: quoted/padded env values silently broke RPC connections.

    Docker's `env_file` parser is not a shell — it keeps surrounding quotes,
    trailing whitespace and inline comments verbatim. `SOLANA_RPC_URL="https://…"`
    therefore produced a URL beginning with a literal quote, which fails as
    `%22https://…` and is indistinguishable from the provider being down. A user
    who had correctly added an Alchemy key still saw 429s from the public node.
    """

    def _resolve(self, tmp_path, monkeypatch, value):
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n  solana_rpc: ${SOLANA_RPC_URL:-https://fallback.example}\n"
        )
        monkeypatch.setenv("SOLANA_RPC_URL", value)
        return load_settings(cfg).onchain.solana_rpc

    def test_double_quotes_stripped(self, tmp_path, monkeypatch):
        got = self._resolve(tmp_path, monkeypatch, '"https://x.example/v2/KEY"')
        assert got == "https://x.example/v2/KEY"

    def test_single_quotes_stripped(self, tmp_path, monkeypatch):
        got = self._resolve(tmp_path, monkeypatch, "'https://x.example/v2/KEY'")
        assert got == "https://x.example/v2/KEY"

    def test_whitespace_stripped(self, tmp_path, monkeypatch):
        got = self._resolve(tmp_path, monkeypatch, "  https://x.example/v2/KEY  ")
        assert got == "https://x.example/v2/KEY"

    def test_inline_comment_removed(self, tmp_path, monkeypatch):
        got = self._resolve(tmp_path, monkeypatch, "https://x.example/v2/KEY  # mine")
        assert got == "https://x.example/v2/KEY"

    def test_failover_list_sanitised_per_entry(self, tmp_path, monkeypatch):
        got = self._resolve(
            tmp_path, monkeypatch, '"https://a.example/v2/K" , https://b.example '
        )
        assert got == "https://a.example/v2/K,https://b.example"

    def test_empty_entries_dropped(self, tmp_path, monkeypatch):
        got = self._resolve(tmp_path, monkeypatch, "https://a.example,,  ,https://b.example")
        assert got == "https://a.example,https://b.example"

    def test_evm_dict_sanitised_too(self, tmp_path, monkeypatch):
        cfg = tmp_path / "c.yaml"
        cfg.write_text("onchain:\n  evm_rpc:\n    ethereum: ${ETH_RPC_URL:-https://fb.example}\n")
        monkeypatch.setenv("ETH_RPC_URL", '"https://eth.example/v2/KEY"')
        assert load_settings(cfg).onchain.evm_rpc["ethereum"] == "https://eth.example/v2/KEY"

    def test_legitimate_url_untouched(self, tmp_path, monkeypatch):
        url = "https://solana-mainnet.g.alchemy.com/v2/AbC-123_xyz"
        assert self._resolve(tmp_path, monkeypatch, url) == url


class TestRetiredEndpointAttribution:
    """The warning must name the real source of a bad endpoint.

    Regression: the message always blamed config.yaml, even when the value came
    from an environment variable. That sent a user to run `update.sh --config`
    repeatedly against a file that was already correct, while the actual
    override sat in .env.
    """

    def test_env_override_is_attributed_to_env(self, tmp_path, monkeypatch, caplog):
        import logging

        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n  evm_rpc:\n    bsc: ${BSC_RPC_URL:-https://good.example}\n"
        )
        monkeypatch.setenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org")
        with caplog.at_level(logging.WARNING):
            load_settings(cfg)
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "BSC_RPC_URL" in joined, "must name the env var"
        assert ".env" in joined, "must point at .env"

    def test_yaml_source_is_attributed_to_file(self, tmp_path, monkeypatch, caplog):
        import logging

        monkeypatch.delenv("BSC_RPC_URL", raising=False)
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            "onchain:\n  evm_rpc:\n    bsc: https://bsc-dataseed.binance.org\n"
        )
        with caplog.at_level(logging.WARNING):
            load_settings(cfg)
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "update.sh --config" in joined
        assert "BSC_RPC_URL environment variable" not in joined

    def test_clean_config_produces_no_warning(self, monkeypatch, caplog):
        import logging
        from pathlib import Path

        for var in ("BSC_RPC_URL", "ETH_RPC_URL", "SOLANA_RPC_URL"):
            monkeypatch.delenv(var, raising=False)
        cfg = Path(__file__).resolve().parent.parent / "config.yaml"
        with caplog.at_level(logging.WARNING):
            load_settings(cfg)
        assert not [r for r in caplog.records if "retired RPC endpoint" in r.message]


class TestUpdateScriptForcesRecreate:
    def test_force_recreate_present(self):
        """`up -d` keeps the old container when only source changed."""
        from pathlib import Path

        text = (Path(__file__).resolve().parent.parent / "deploy" / "update.sh").read_text()
        assert "--force-recreate" in text, (
            "without it a rebuilt image silently does not take effect"
        )


class TestDeployHealthCheck:
    """Regression: the update script reported a false failure on a healthy run.

    Compose prefixes container names with the project directory
    (`crypto-anomaly-bot-cadb-1`), so `docker inspect cadb` fails. The old
    `||` fallback chain then concatenated both branches into a multi-line
    value, which never equalled "running" and aborted with
    `❌ container state=\\nrunning` on a perfectly healthy container.
    """

    def _script(self, name: str) -> str:
        from pathlib import Path

        return (Path(__file__).resolve().parent.parent / "deploy" / name).read_text()

    def test_resolves_container_via_compose(self):
        text = self._script("update.sh")
        assert "docker compose ps -q cadb" in text
        assert "container_state()" in text, "state lookup must be a single function"

    def test_no_bare_inspect_by_service_name(self):
        """`docker inspect cadb` only works if the container is literally named that."""
        text = self._script("update.sh")
        assert "docker inspect --format='{{.State.Status}}' cadb " not in text
        assert "docker inspect --format='{{.State.Status}}' cadb\n" not in text

    def test_state_output_is_single_line(self):
        text = self._script("update.sh")
        assert "head -n1" in text, "must not emit multi-line state values"

    def test_timeout_does_not_report_failure(self):
        """Missing the log line is a warning, not a failed deploy."""
        text = self._script("update.sh")
        assert "HEALTHY=0" in text and "if (( ! HEALTHY ))" in text

    def test_workflow_uses_same_resolution(self):
        from pathlib import Path

        wf = (
            Path(__file__).resolve().parent.parent
            / ".github" / "workflows" / "deploy.yml"
        ).read_text()
        assert "cid()" in wf, "CI deploy needs the same container resolution"
        assert 'docker inspect --format=\'{{.State.Status}}\' cadb ' not in wf


class TestDeployScriptsAreExecutable:
    """Shell scripts must ship with the executable bit set in git.

    Regression: `./deploy/update.sh` failed with "Permission denied" on a fresh
    clone. The scripts were chmod +x locally when first written, but later edits
    rewrote them via Python (which recreates the file at 0644), and the mode was
    never committed. Git tracks the executable bit, so the fix must live in the
    index — a local chmod does not help anyone who clones.
    """

    def test_core_filemode_is_disabled(self):
        """git must ignore the on-disk exec bit in this repo.

        This is the actual root cause of the regression. The tooling that edits
        this repo restores files without the exec bit; with core.fileMode=true
        (the default) git treats that as an intentional change, so `git add -A`
        silently rewrites 100755 -> 100644 and ships a broken script. Setting it
        false makes the index the single source of truth.
        """
        import subprocess
        from pathlib import Path

        repo = Path(__file__).resolve().parent.parent
        if not (repo / ".git").exists():
            import pytest

            pytest.skip("not a git checkout")
        value = subprocess.run(
            ["git", "config", "core.fileMode"],
            cwd=repo, capture_output=True, text=True, check=False,
        ).stdout.strip()
        assert value == "false", (
            "core.fileMode must be false or the exec bit regresses on every "
            "commit; run `git config core.fileMode false`"
        )

    def test_scripts_have_exec_bit_in_git(self):
        import subprocess
        from pathlib import Path

        repo = Path(__file__).resolve().parent.parent
        out = subprocess.run(
            ["git", "ls-files", "-s", "deploy/"],
            cwd=repo, capture_output=True, text=True, check=False,
        ).stdout
        if not out.strip():
            import pytest

            pytest.skip("not a git checkout")

        for line in out.strip().splitlines():
            mode, _, rest = line.partition(" ")
            path = rest.split("\t")[-1]
            if path.endswith(".sh"):
                assert mode == "100755", (
                    f"{path} is mode {mode}; run "
                    f"`git update-index --chmod=+x {path}`"
                )

    def test_scripts_have_shebang(self):
        from pathlib import Path

        deploy = Path(__file__).resolve().parent.parent / "deploy"
        for script in deploy.glob("*.sh"):
            first = script.read_text().splitlines()[0]
            assert first.startswith("#!"), f"{script.name} lacks a shebang"
            assert "bash" in first, f"{script.name} should specify bash"
