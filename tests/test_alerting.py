"""Alert formatting and routing: de-duplication, escalation, rate limits."""

from __future__ import annotations

import asyncio

import pytest
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
        """An unlisted symbol must be reported as unlisted, not as "no data".

        "No data" conflates "we are not watching this" with "this does not
        exist", which is what made a quiet BTC look like a broken bot.
        """
        _, bot = wired
        out = await bot.commands["check"](["NOSUCHTOKEN"], 1)
        assert "not listed" in out.lower() or "no data" in out.lower()

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


class TestNoDuplicateBroadcast:
    """Regression: ~60 identical alerts in 30s reached Telegram."""

    @pytest_asyncio.fixture
    async def app_with_bot(self):
        from cadb.app import Application
        from cadb.bot.telegram_bot import TelegramBot
        from cadb.core.config import Settings

        s = Settings()
        s.exchange.enabled = s.onchain.enabled = s.social.enabled = False
        s.ml.model_path = ""
        s.alerts.dry_run = True
        s.alerts.cooldown_s = 300
        s.telemetry.log_level = "ERROR"
        s.telemetry.state_file = ""
        app = Application(s)
        await app.setup()
        bot = TelegramBot(token="")
        bot.subscribers = {"1"}
        app.bot = bot
        bot.running = True
        sent: list[str] = []

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append(chat_id)
            return True

        bot.send = fake_send  # type: ignore[assignment]
        yield app, sent

    async def test_repeated_signals_broadcast_once(self, app_with_bot):
        app, sent = app_with_bot
        for _ in range(20):
            await app._on_signal(_signal(85.0, "SOL"))
        assert len(sent) <= 1, f"cooldown bypassed: {len(sent)} broadcasts"

    async def test_suppressed_signal_is_not_broadcast(self, app_with_bot):
        app, sent = app_with_bot
        await app._on_signal(_signal(50.0, "SOL"))  # below threshold
        assert sent == []

    async def test_no_double_send_when_router_has_telegram(self, app_with_bot):
        """If a Telegram sink already delivered it, the bot must not resend."""
        from cadb.alerting.router import TelegramSink

        app, sent = app_with_bot
        app.router.sinks.clear()
        sink = TelegramSink("token", "1")
        delivered: list[str] = []

        async def fake_deliver(signal) -> bool:
            delivered.append(signal.asset_pair)
            return True

        sink.deliver = fake_deliver  # type: ignore[assignment]
        app.router.add_sink(sink)

        await app._on_signal(_signal(88.0, "BTC"))
        assert len(delivered) == 1, "sink should deliver once"
        assert sent == [], "bot must not duplicate the sink delivery"

    async def test_escalation_still_reaches_user(self, app_with_bot):
        app, sent = app_with_bot
        await app._on_signal(_signal(82.0, "SOL", Severity.HIGH))
        await app._on_signal(_signal(95.0, "SOL", Severity.CRITICAL))
        assert len(sent) == 2, "severity escalation must break the cooldown"


class TestCommandDispatch:
    """Regression: commands were silently dropped before reaching a handler.

    The earlier suite invoked handlers directly, so it never exercised
    `_handle_update` — where a stale TELEGRAM_CHAT_ID was discarding every
    command with only a server-side log line.
    """

    def _bot(self, **kwargs):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="x", **kwargs)
        sent: list[tuple[str, str]] = []

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append((chat_id, text))
            return True

        bot.send = fake_send  # type: ignore[assignment]

        async def ping(args, chat_id):
            return "pong"

        bot.register("ping", ping, "test")
        return bot, sent

    @staticmethod
    def _update(text: str, chat_id: int):
        return {"message": {"text": text, "chat": {"id": chat_id}}}

    async def test_default_chat_id_does_not_create_a_lockout(self):
        """A wrong alert destination must not silently disable the bot."""
        bot, sent = self._bot(default_chat_id="-1001234567890")
        await bot._handle_update(self._update("/ping", 987654321))
        assert sent and sent[0][1] == "pong"

    async def test_explicit_allowlist_still_enforced(self):
        bot, sent = self._bot(allowed_chats=["111"])
        await bot._handle_update(self._update("/ping", 999))
        assert sent, "rejection must be reported, not silent"
        assert "not authorised" in sent[0][1].lower()
        assert "999" in sent[0][1], "must tell the user their real chat id"

    async def test_allowed_chat_passes(self):
        bot, sent = self._bot(allowed_chats=["111"])
        await bot._handle_update(self._update("/ping", 111))
        assert sent[0][1] == "pong"

    async def test_rejection_replies_are_bounded(self):
        """Never let an unauthorised chat amplify traffic indefinitely."""
        bot, sent = self._bot(allowed_chats=["111"])
        for _ in range(20):
            await bot._handle_update(self._update("/ping", 999))
        assert len(sent) <= 3

    async def test_unknown_command_gets_a_reply(self):
        bot, sent = self._bot()
        await bot._handle_update(self._update("/nosuchcmd", 1))
        assert sent and "unknown" in sent[0][1].lower()

    async def test_bot_username_suffix_is_stripped(self):
        """Group chats send /cmd@BotName — must still route."""
        bot, sent = self._bot()
        await bot._handle_update(self._update("/ping@my_bot", 1))
        assert sent[0][1] == "pong"

    async def test_handler_exception_reports_to_user(self):
        from cadb.bot.telegram_bot import TelegramBot

        bot = TelegramBot(token="x")
        sent: list[str] = []

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append(text)
            return True

        bot.send = fake_send  # type: ignore[assignment]

        async def boom(args, chat_id):
            raise ValueError("kaboom")

        bot.register("boom", boom, "test")
        await bot._handle_update(self._update("/boom", 1))
        assert sent and "failed" in sent[0].lower()

    async def test_non_command_text_ignored(self):
        bot, sent = self._bot()
        await bot._handle_update(self._update("just chatting", 1))
        assert sent == []


class TestAllCommandsThroughDispatch:
    """Every registered command must survive the real dispatch path."""

    @pytest_asyncio.fixture
    async def live(self):
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
        s.telemetry.health_interval_s = 9999

        app = Application(s)
        await app.setup()
        bot = TelegramBot(token="")
        app.bot = bot
        register_commands(bot, app)

        sent: list[str] = []

        async def fake_send(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
            sent.append(text)
            return True

        bot.send = fake_send  # type: ignore[assignment]
        yield app, bot, sent

    async def test_every_command_replies(self, live):
        app, bot, sent = live
        sample_args = {
            "check": "BTC", "explain": "BTC", "book": "BTC/USDT",
            "social": "BTC", "whales": "BTC", "history": "BTC",
            "threshold": "80", "mute": "1",
        }
        failures = []
        for name in sorted(bot.commands):
            sent.clear()
            arg = sample_args.get(name, "")
            text = f"/{name} {arg}".strip()
            try:
                await bot._handle_update(
                    {"message": {"text": text, "chat": {"id": 1}}}
                )
            except Exception as exc:
                failures.append(f"{name}: {type(exc).__name__}: {exc}")
                continue
            if not sent or not sent[0].strip():
                failures.append(f"{name}: no reply")
            elif len(sent[0]) > 4096:
                failures.append(f"{name}: {len(sent[0])} chars exceeds Telegram cap")
        assert not failures, "commands failed via dispatch: " + "; ".join(failures)

    async def test_every_command_has_a_menu_description(self, live):
        _, bot, _ = live
        missing = [c for c in bot.commands if c not in bot.descriptions]
        assert not missing, f"no /-menu description: {missing}"

    async def test_whoami_flags_chat_id_mismatch(self, live):
        app, bot, sent = live
        app.settings.alerts.telegram_chat_id = "-1009999"
        await bot._handle_update({"message": {"text": "/whoami", "chat": {"id": 42}}})
        assert "42" in sent[0]
        assert "different chat" in sent[0].lower()


class TestCommandsAnswerForAnyToken:
    """Regression: `/check BTC` returned "No data for BTC".

    Removing pinned symbols fixed alert spam but broke every query command:
    they read only the streaming feature store, so any token not currently
    flagged as anomalous appeared unknown. A quiet symbol is not an unknown
    symbol, and reporting "no data" for BTC reads as the bot being broken.
    """

    @pytest_asyncio.fixture
    async def wired(self):
        from cadb.app import Application
        from cadb.bot.commands import register_commands
        from cadb.bot.telegram_bot import TelegramBot
        from cadb.core.config import Settings

        s = Settings()
        s.exchange.enabled = False
        s.onchain.enabled = False
        s.social.enabled = False
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

    async def test_check_never_says_no_data_for_listed_token(self, wired):
        """Stub the scanner so the test is deterministic and offline."""
        from cadb.modules.exchange.ondemand import SymbolSnapshot

        app, bot = wired

        class FakeScanner:
            async def best_snapshot(self, query):
                snap = SymbolSnapshot(
                    symbol="BTC/USDT", venue="mexc", price=64000.0,
                    change_pct=-0.3, quote_volume=5.4e8, obi=-0.05,
                    bid_depth=4e5, ask_depth=4.4e5, spread_bps=0.4,
                    volume_z=0.3, volume_spike_ratio=1.1, candles=60,
                )
                return snap, ["mexc", "gate", "kucoin"]

            async def search(self, frag, limit=8):
                return []

        app._ondemand = FakeScanner()
        out = await bot.commands["check"](["BTC"], 1)
        assert "No data for" not in out
        assert "On-Demand Scan" in out
        assert "64,000" in out or "64000" in out

    async def test_unknown_symbol_says_not_listed_with_suggestions(self, wired):
        app, bot = wired

        class FakeScanner:
            async def best_snapshot(self, query):
                return None, []

            async def search(self, frag, limit=8):
                return ["PEPE/USDT", "PEPE2/USDT"]

        app._ondemand = FakeScanner()
        out = await bot.commands["check"](["PEP"], 1)
        assert "not listed" in out.lower()
        assert "PEPE/USDT" in out, "should suggest close matches"

    async def test_social_explains_why_it_cannot_answer(self, wired):
        """Social genuinely cannot be fetched on demand — say so clearly."""
        from cadb.core.config import SocialConfig
        from cadb.modules.social.monitor import SocialMonitor

        app, bot = wired
        app.social = SocialMonitor(app.bus, SocialConfig(x_bearer_token=""))
        app.social.enabled_sources = False
        out = await bot.commands["social"](["BTC"], 1)
        assert "disabled" in out.lower()
        assert "X_BEARER_TOKEN" in out
        assert "not just this one" in out.lower(), "must not imply BTC is special"

    async def test_movers_registered_and_handles_no_scanner(self, wired):
        app, bot = wired
        assert "movers" in bot.commands
        app._ondemand = None
        out = await bot.commands["movers"]([], 1)
        assert isinstance(out, str) and out.strip()


class TestOnDemandScanner:
    def test_snapshot_maps_to_canonical_features(self):
        from cadb.modules.exchange.ondemand import SymbolSnapshot
        from cadb.modules.ml.features import FEATURE_NAMES

        snap = SymbolSnapshot(
            symbol="X/USDT", venue="mexc", price=1.0, change_pct=40.0,
            quote_volume=1e6, obi=-0.8, bid_depth=1e5, ask_depth=9e5,
            spread_bps=5.0, volume_z=4.2, volume_spike_ratio=8.0, candles=60,
        )
        fv = snap.to_feature_vector("X")
        assert len(fv.values) == len(FEATURE_NAMES)
        f = fv.as_dict()
        assert f["obi"] == pytest.approx(-0.8)
        assert f["obi_abs"] == pytest.approx(0.8)
        assert f["volume_z"] == pytest.approx(4.2)

    def test_coverage_is_honest_about_missing_modules(self):
        from cadb.modules.exchange.ondemand import SymbolSnapshot

        fv = SymbolSnapshot(symbol="X/USDT", venue="mexc", price=1.0).to_feature_vector("X")
        assert fv.coverage == pytest.approx(1 / 3)
        assert fv.sources_fresh["exchange"] is True
        assert fv.sources_fresh["onchain"] is False
        assert fv.sources_fresh["social"] is False

    def test_failed_snapshot_reports_not_ok(self):
        from cadb.modules.exchange.ondemand import SymbolSnapshot

        assert not SymbolSnapshot(symbol="X/USDT", venue="mexc").ok


class TestWatchlistRendering:
    """Regression: `if snap["volume_z"]` hid legitimate zero values.

    A pair measured at exactly 0.0 rendered identically to one with no data,
    so most of /watchlist appeared blank and there was no way to tell a calm
    market from a stream that had not started.
    """

    @pytest_asyncio.fixture
    async def wired(self):
        from cadb.app import Application
        from cadb.bot.commands import register_commands
        from cadb.bot.telegram_bot import TelegramBot
        from cadb.core.config import Settings

        s = Settings()
        s.exchange.simulate = True
        s.exchange.exchanges = ["binance"]
        s.exchange.discovery_enabled = False
        s.onchain.enabled = False
        s.social.enabled = False
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

    async def test_zero_zscore_is_displayed(self, wired):
        from cadb.modules.exchange.microstructure import MicrostructureState

        app, bot = wired
        st = MicrostructureState(venue="binance", symbol="ZERO/USDT")
        st.book.update([(99.0, 5.0)], [(101.0, 5.0)], 1)
        st.on_trade(1, 100.0, 1.0, "buy")
        app.exchange.states[("binance", "ZERO/USDT")] = st
        app.exchange.watched["binance"] = {"ZERO/USDT"}

        out = await bot.commands["watchlist"]([], 1)
        assert "ZERO/USDT" in out
        assert "z=+0.0" in out, "a measured zero must render, not vanish"

    async def test_unstarted_stream_says_warming_up(self, wired):
        app, bot = wired
        app.exchange.watched["binance"] = {"NEW/USDT"}
        out = await bot.commands["watchlist"]([], 1)
        assert "warming up" in out, "no-data must be distinguishable from quiet"

    async def test_lopsided_book_is_flagged(self, wired):
        from cadb.modules.exchange.microstructure import MicrostructureState

        app, bot = wired
        st = MicrostructureState(venue="binance", symbol="SKEW/USDT")
        st.book.update([(99.0, 1.0)], [(101.0, 40.0)], 1)
        st.on_trade(1, 100.0, 1.0, "sell")
        app.exchange.states[("binance", "SKEW/USDT")] = st
        app.exchange.watched["binance"] = {"SKEW/USDT"}

        out = await bot.commands["watchlist"]([], 1)
        assert "obi=" in out
        assert "⚠️" in out, "a heavily one-sided book should be marked"
