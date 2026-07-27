"""Alert formatting and routing: de-duplication, escalation, rate limits."""

from __future__ import annotations

import asyncio

import pytest_asyncio

from cadb.alerting.formatter import format_discord, format_plain, format_telegram
from cadb.alerting.router import AlertRouter, AlertSink
from cadb.core.config import AlertConfig
from cadb.core.schema import AnomalySignal, Severity, now_ms


def _signal(score: float = 85.0, asset: str = "PEPE", severity: Severity | None = None,
            venue: str = "aggregate") -> AnomalySignal:
    if severity is None:
        severity = (
            Severity.CRITICAL if score >= 90 else Severity.HIGH if score >= 80
            else Severity.MEDIUM if score >= 60 else Severity.LOW
        )
    return AnomalySignal(
        timestamp=now_ms(), asset_pair=asset, venue=venue, score=score, severity=severity,
        ml_score=score - 5, rule_score=score + 2,
        contributions={"exchange": 70.0, "onchain": 40.0, "social": 90.0},
        features={"volume_z": 5.2, "bot_farm_score": 0.88, "obi": 0.61},
        reasons=["volume 5.2σ above baseline", "bot-farm pattern (confidence 88%)"],
        latency_ms=42.0,
    )


class _RecordingSink(AlertSink):
    name = "recording"

    def __init__(self, should_fail: bool = False) -> None:
        super().__init__()
        self.received: list[AnomalySignal] = []
        self.should_fail = should_fail

    async def deliver(self, signal: AnomalySignal) -> bool:
        if self.should_fail:
            raise RuntimeError("sink down")
        self.received.append(signal)
        return True


class TestFormatters:
    def test_telegram_payload_structure(self):
        p = format_telegram(_signal())
        assert p["parse_mode"] == "HTML"
        assert "PEPE" in p["text"]
        assert "85.0" in p["text"]
        assert "bot-farm pattern" in p["text"]

    def test_telegram_escapes_html(self):
        sig = _signal().model_copy(update={"asset_pair": "A<B>&C"})
        assert "<b>" in format_telegram(sig)["text"]
        assert "A&lt;B&gt;&amp;C" in format_telegram(sig)["text"]

    def test_discord_embed_structure(self):
        p = format_discord(_signal(92.0))
        embed = p["embeds"][0]
        assert "PEPE" in embed["title"]
        assert embed["color"] == 0xC0392B  # critical red
        assert any("Evidence" in f["name"] for f in embed["fields"])

    def test_discord_field_length_capped(self):
        sig = _signal().model_copy(update={"reasons": ["x" * 200 for _ in range(20)]})
        for field in format_discord(sig)["embeds"][0]["fields"]:
            assert len(field["value"]) <= 1024

    def test_plain_text_contains_essentials(self):
        text = format_plain(_signal(88.0))
        assert "PEPE" in text and "88.0" in text and "HIGH" in text

    def test_severity_emoji_differs(self):
        crit = format_telegram(_signal(95.0))["text"]
        med = format_telegram(_signal(65.0))["text"]
        assert crit[:2] != med[:2]


class TestAlertRouter:
    def _router(self, **kwargs) -> tuple[AlertRouter, _RecordingSink]:
        params = {"dry_run": False, "min_score": 80.0, "cooldown_s": 300,
                  "max_alerts_per_min": 600}
        params.update(kwargs)
        cfg = AlertConfig(**params)
        router = AlertRouter(cfg)
        router.sinks.clear()
        sink = _RecordingSink()
        router.add_sink(sink)
        return router, sink

    async def test_dispatches_above_threshold(self):
        router, sink = self._router()
        assert await router.dispatch(_signal(85.0))
        assert len(sink.received) == 1

    async def test_suppresses_below_threshold(self):
        router, sink = self._router()
        assert not await router.dispatch(_signal(65.0))
        assert not sink.received
        assert router.suppressed == 1

    async def test_cooldown_deduplicates_same_asset(self):
        router, sink = self._router()
        await router.dispatch(_signal(85.0, "PEPE"))
        await router.dispatch(_signal(84.0, "PEPE"))
        await router.dispatch(_signal(86.0, "PEPE"))
        assert len(sink.received) == 1, "cooldown must collapse repeats"

    async def test_severity_escalation_breaks_cooldown(self):
        """Suppressing an escalation is worse than sending a duplicate."""
        router, sink = self._router()
        await router.dispatch(_signal(82.0, "PEPE", Severity.HIGH))
        await router.dispatch(_signal(95.0, "PEPE", Severity.CRITICAL))
        assert len(sink.received) == 2
        assert sink.received[1].severity is Severity.CRITICAL

    async def test_no_escalation_on_equal_severity(self):
        router, sink = self._router()
        await router.dispatch(_signal(85.0, "PEPE", Severity.HIGH))
        await router.dispatch(_signal(88.0, "PEPE", Severity.HIGH))
        assert len(sink.received) == 1

    async def test_different_assets_are_independent(self):
        router, sink = self._router()
        await router.dispatch(_signal(85.0, "PEPE"))
        await router.dispatch(_signal(85.0, "SHIB"))
        assert len(sink.received) == 2

    async def test_cooldown_expiry_allows_resend(self):
        router, sink = self._router(cooldown_s=0)
        await router.dispatch(_signal(85.0, "PEPE"))
        await asyncio.sleep(0.01)
        await router.dispatch(_signal(85.0, "PEPE"))
        assert len(sink.received) == 2

    async def test_one_sink_failure_does_not_block_others(self):
        router, good = self._router()
        router.add_sink(_RecordingSink(should_fail=True))
        assert await router.dispatch(_signal(85.0))
        assert len(good.received) == 1

    async def test_all_sinks_failing_reports_failure(self):
        cfg = AlertConfig(dry_run=False, min_score=80.0, max_alerts_per_min=600)
        router = AlertRouter(cfg)
        router.sinks.clear()
        router.add_sink(_RecordingSink(should_fail=True))
        assert not await router.dispatch(_signal(85.0))

    async def test_sink_circuit_breaker_opens(self):
        cfg = AlertConfig(dry_run=False, min_score=80.0, cooldown_s=0,
                          max_alerts_per_min=600)
        router = AlertRouter(cfg)
        router.sinks.clear()
        bad = _RecordingSink(should_fail=True)
        router.add_sink(bad)
        for i in range(7):
            await router.dispatch(_signal(85.0, f"A{i}"))
        assert bad.breaker.state.value in ("open", "half_open")

    async def test_dry_run_uses_console_sink(self):
        router = AlertRouter(AlertConfig(dry_run=True, min_score=80.0))
        assert [s.name for s in router.sinks] == ["console"]
        assert await router.dispatch(_signal(85.0))

    async def test_falls_back_to_console_without_credentials(self):
        router = AlertRouter(AlertConfig(dry_run=False, min_score=80.0))
        assert any(s.name == "console" for s in router.sinks)

    async def test_health_report(self):
        router, _ = self._router()
        await router.dispatch(_signal(85.0))
        await router.dispatch(_signal(50.0))
        h = router.health()
        assert h["dispatched"] == 1 and h["suppressed"] == 1
        assert h["sinks"][0]["sent"] == 1


class TestTelegramBot:
    async def test_command_registration_and_dispatch(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="", default_chat_id="123")
        calls: list[list[str]] = []

        async def handler(args: list[str], chat_id: int) -> str:
            calls.append(args)
            return "ok"

        bot.register("test", handler)
        assert "test" in bot.commands
        assert await bot.commands["test"](["arg1"], 123) == "ok"
        assert calls == [["arg1"]]

    async def test_builtin_commands_exist(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="")
        for cmd in ("help", "start", "watch", "unwatch", "mute", "metrics"):
            assert cmd in bot.commands

    async def test_watch_unwatch_subscription(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="")
        await bot.commands["watch"]([], 999)
        assert "999" in bot.subscribers
        await bot.commands["unwatch"]([], 999)
        assert "999" not in bot.subscribers

    async def test_mute_suppresses_broadcast(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="")
        sent: list[str] = []
        bot.subscribers.add("1")

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append(text)
            return True

        bot.send = fake_send  # type: ignore[assignment]
        await bot.commands["mute"](["60"], 1)
        await bot.broadcast_signal(_signal())
        assert not sent, "muted bot must not broadcast"

    async def test_broadcast_reaches_subscribers(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="")
        bot.subscribers = {"1", "2"}
        sent: list[str] = []

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append(chat_id)
            return True

        bot.send = fake_send  # type: ignore[assignment]
        await bot.broadcast_signal(_signal())
        assert set(sent) == {"1", "2"}


class TestBotCommands:
    """The command surface must never crash, even with no data yet."""

    @pytest_asyncio.fixture
    async def wired(self):
        from cadb.app import Application
        from cadb.bot.commands import register_commands
        from cadb.bot.telegram_bot import TelegramBot
        from cadb.core.config import Settings

        s = Settings()
        s.exchange.simulate = s.onchain.simulate = s.social.simulate = True
        s.exchange.exchanges = ["binance"]
        s.exchange.symbols = ["BTC/USDT"]
        s.social.tracked_tickers = ["BTC"]
        s.social.use_finbert = False
        s.ml.model_path = ""
        s.alerts.dry_run = True
        s.telemetry.log_level = "ERROR"
        s.telemetry.state_file = ""

        app = Application(s)
        await app.setup()
        bot = TelegramBot(token="")
        app.bot = bot
        register_commands(bot, app)
        yield app, bot

    async def test_every_command_registered(self, wired):
        _, bot = wired
        expected = {
            "help", "start", "status", "scores", "check", "explain", "book",
            "whales", "flows", "social", "venues", "history", "config",
            "threshold", "pause", "resume", "mute", "unmute", "watch",
            "unwatch", "metrics", "test",
        }
        assert expected.issubset(set(bot.commands))

    async def test_commands_survive_empty_state(self, wired):
        """Before any data arrives, commands must guide rather than explode."""
        _, bot = wired
        for name in ("status", "scores", "whales", "flows", "venues",
                     "history", "config", "help", "metrics"):
            out = await bot.commands[name]([], 1)
            assert isinstance(out, str) and out.strip()

    async def test_commands_requiring_args_explain_usage(self, wired):
        _, bot = wired
        for name in ("check", "explain", "book", "social"):
            out = await bot.commands[name]([], 1)
            assert "usage" in out.lower() or "tracking" in out.lower()

    async def test_unknown_asset_is_handled(self, wired):
        _, bot = wired
        out = await bot.commands["check"](["NOSUCHTOKEN"], 1)
        assert "no data" in out.lower()

    async def test_threshold_updates_all_consumers(self, wired):
        app, bot = wired
        await bot.commands["threshold"](["65"], 1)
        assert app.settings.ml.alert_threshold == 65.0
        assert app.ml.config.alert_threshold == 65.0
        assert app.router.config.min_score == 65.0

    async def test_threshold_rejects_garbage(self, wired):
        _, bot = wired
        assert "usage" in (await bot.commands["threshold"](["abc"], 1)).lower()

    async def test_threshold_clamped_to_range(self, wired):
        app, bot = wired
        await bot.commands["threshold"](["500"], 1)
        assert app.settings.ml.alert_threshold == 100.0

    async def test_pause_blocks_dispatch(self, wired):
        app, bot = wired
        await bot.commands["pause"]([], 1)
        assert app.alerts_paused
        before = app.router.dispatched
        await app._on_signal(_signal(95.0))
        assert app.router.dispatched == before, "paused router must not dispatch"
        await bot.commands["resume"]([], 1)
        assert not app.alerts_paused

    async def test_history_records_even_when_paused(self, wired):
        """Detection must keep working while delivery is muted."""
        app, bot = wired
        await bot.commands["pause"]([], 1)
        await app._on_signal(_signal(91.0, "PEPE"))
        assert any(s.asset_pair == "PEPE" for s in app.alert_history)
        out = await bot.commands["history"]([], 1)
        assert "PEPE" in out

    async def test_test_command_reaches_sinks(self, wired):
        app, bot = wired
        out = await bot.commands["test"]([], 1)
        assert "console" in out

    async def test_output_is_telegram_length_safe(self, wired):
        """A single message must stay under Telegram's 4096-char cap."""
        _, bot = wired
        for name in ("help", "config", "status"):
            assert len(await bot.commands[name]([], 1)) < 4096
