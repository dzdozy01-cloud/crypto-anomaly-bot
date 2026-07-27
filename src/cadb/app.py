"""Application orchestrator — wires the four modules, bus, ML and alerting.

Startup order matters: the bus comes up first, then the ML scorer subscribes,
then the producers start. That way no telemetry is published before there is a
consumer to aggregate it.

Shutdown is reverse-order and idempotent, with a bounded grace period so a hung
network client cannot block process exit.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import time
from pathlib import Path
from typing import Any

from .alerting.router import AlertRouter
from .bot.telegram_bot import TelegramBot
from .core.bus import EventBus, build_bus
from .core.config import Settings
from .core.schema import AnomalySignal
from .core.telemetry import METRICS, setup_logging
from .modules.base import Module
from .modules.exchange.engine import ExchangeEngine
from .modules.ml.scorer import MLScorer
from .modules.onchain.tracker import WhaleTracker
from .modules.social.monitor import SocialMonitor

log = logging.getLogger(__name__)

__all__ = ["Application"]


class Application:
    """Top-level lifecycle manager."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bus: EventBus | None = None
        self.modules: list[Module] = []
        self.exchange: ExchangeEngine | None = None
        self.onchain: WhaleTracker | None = None
        self.social: SocialMonitor | None = None
        self.ml: MLScorer | None = None
        self.router: AlertRouter | None = None
        self.bot: TelegramBot | None = None
        self.started_at = 0.0
        self._shutdown = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    # ---- wiring ----------------------------------------------------------
    async def setup(self) -> None:
        s = self.settings
        setup_logging(s.telemetry.log_level, s.telemetry.json_logs)
        log.info("=" * 68)
        log.info("  CADB — Crypto Anomaly Detection Bot")
        log.info("=" * 68)

        self.bus = await build_bus(
            kind=s.bus.kind, url=s.bus.url, queue_size=s.bus.queue_size
        )

        # 1. ML scorer first so it never misses early telemetry.
        if s.ml.enabled:
            self.ml = MLScorer(self.bus, s.ml)
            self.modules.append(self.ml)

        # 2. Alert routing.
        self.router = AlertRouter(s.alerts)
        self.router.config.min_score = s.ml.alert_threshold

        # 3. Interactive Telegram bot.
        if s.alerts.telegram_bot_token and not s.alerts.dry_run:
            self.bot = TelegramBot(
                token=s.alerts.telegram_bot_token,
                default_chat_id=s.alerts.telegram_chat_id,
            )
            self._register_bot_commands()

        if self.ml is not None:
            self.ml.add_handler(self._on_signal)

        # 4. Producers.
        if s.exchange.enabled:
            self.exchange = ExchangeEngine(self.bus, s.exchange)
            self.modules.append(self.exchange)
        if s.onchain.enabled:
            self.onchain = WhaleTracker(self.bus, s.onchain)
            self.modules.append(self.onchain)
        if s.social.enabled:
            self.social = SocialMonitor(self.bus, s.social)
            self.modules.append(self.social)

    async def _on_signal(self, signal: AnomalySignal) -> None:
        """Route a high-scoring signal to webhooks and the interactive bot."""
        if self.router:
            await self.router.dispatch(signal)
        if self.bot and self.bot.running:
            with contextlib.suppress(Exception):
                await self.bot.broadcast_signal(signal)

    # ---- bot commands ------------------------------------------------------
    def _register_bot_commands(self) -> None:
        assert self.bot is not None

        async def status_cmd(args: list[str], chat_id: int) -> str:
            h = self.health()
            uptime = h["uptime_s"]
            lines = [
                "<b>🛡 CADB Status</b>",
                f"Uptime: <code>{uptime // 3600:.0f}h {(uptime % 3600) // 60:.0f}m</code>",
                f"Bus: <code>{h['bus']['published']:,}</code> published, "
                f"<code>{h['bus']['dropped']:,}</code> dropped",
                "",
                "<b>Modules</b>",
            ]
            for m in h["modules"]:
                icon = "🟢" if m.get("healthy") else "🔴"
                lines.append(
                    f"{icon} <code>{m['module']:<9}</code> {m.get('events', 0):,} events"
                )
            clf = h.get("ml", {}).get("classifier", {})
            if clf:
                lines += [
                    "",
                    "<b>Classifier</b>",
                    f"  trained: {clf.get('trained')} "
                    f"({clf.get('training_size', 0):,} samples)",
                    f"  scored: {clf.get('scored', 0):,}",
                    f"  alerts: {h.get('ml', {}).get('alerts', 0)}",
                ]
            lat = METRICS.snapshot()["latency"].get("ml.cycle_ms")
            if lat and lat["count"]:
                lines.append(
                    f"\n<i>Scoring p95: {lat['p95']:.1f}ms "
                    f"(budget {self.settings.telemetry.latency_budget_ms:.0f}ms)</i>"
                )
            return "\n".join(lines)

        async def scores_cmd(args: list[str], chat_id: int) -> str:
            if not self.ml:
                return "ML scorer not enabled."
            top = self.ml.top_scores(15)
            if not top:
                return "No scores yet — still warming up."
            from .alerting.formatter import SEVERITY_EMOJI

            lines = ["<b>📈 Manipulation Scores</b>", ""]
            for asset, score in top:
                sev = (
                    "critical" if score >= 90 else "high" if score >= 80
                    else "medium" if score >= 60 else "low" if score >= 40 else "info"
                )
                bar = "█" * int(score / 10) + "░" * (10 - int(score / 10))
                lines.append(
                    f"{SEVERITY_EMOJI[sev]} <code>{asset:<8} {score:5.1f}</code> {bar}"
                )
            return "\n".join(lines)

        async def check_cmd(args: list[str], chat_id: int) -> str:
            if not args:
                return "Usage: <code>/check BTC</code>"
            if not self.ml:
                return "ML scorer not enabled."
            signal = self.ml.score_asset(args[0])
            if signal is None:
                return f"No data for <code>{args[0].upper()}</code> yet."
            from .alerting.formatter import format_telegram

            return format_telegram(signal)["text"]

        async def threshold_cmd(args: list[str], chat_id: int) -> str:
            if not args:
                return f"Current threshold: <code>{self.settings.ml.alert_threshold:.0f}</code>"
            try:
                value = float(args[0])
            except ValueError:
                return "Usage: <code>/threshold 80</code>"
            value = max(0.0, min(100.0, value))
            self.settings.ml.alert_threshold = value
            if self.ml:
                self.ml.config.alert_threshold = value
            if self.router:
                self.router.config.min_score = value
            return f"✅ Alert threshold set to <code>{value:.0f}</code>"

        self.bot.register("status", status_cmd)
        self.bot.register("scores", scores_cmd)
        self.bot.register("check", check_cmd)
        self.bot.register("threshold", threshold_cmd)

    # ---- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        self.started_at = time.monotonic()
        if self.bus is None:
            await self.setup()

        # ML scorer must subscribe before producers publish.
        if self.ml:
            await self.ml.start()
        for module in self.modules:
            if module is not self.ml:
                await module.start()
        if self.bot:
            await self.bot.start()

        self._tasks.append(asyncio.create_task(self._health_loop(), name="health"))
        if self.settings.telemetry.metrics_port:
            self._tasks.append(
                asyncio.create_task(self._metrics_server(), name="metrics-http")
            )
        log.info(
            "✅ system online — %d module(s), threshold=%.0f",
            len(self.modules), self.settings.ml.alert_threshold,
        )

    async def run_forever(self) -> None:
        await self.start()
        self._install_signal_handlers()
        try:
            await self._shutdown.wait()
        finally:
            await self.stop()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._shutdown.set)

    async def stop(self, grace_s: float = 10.0) -> None:
        log.info("shutting down…")
        for task in self._tasks:
            task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True), timeout=3
            )
        # Producers first, then the consumer, so in-flight events drain.
        producers = [m for m in self.modules if m is not self.ml]
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*(m.stop() for m in producers), return_exceptions=True),
                timeout=grace_s,
            )
        if self.ml:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.ml.stop(), timeout=grace_s / 2)
        if self.bot:
            with contextlib.suppress(Exception):
                await self.bot.stop()
        if self.router:
            with contextlib.suppress(Exception):
                await self.router.close()
        if self.bus:
            with contextlib.suppress(Exception):
                await self.bus.close()
        self._persist_state()
        log.info("shutdown complete (uptime %.0fs)", time.monotonic() - self.started_at)

    # ---- observability ------------------------------------------------------
    async def _health_loop(self) -> None:
        interval = max(self.settings.telemetry.health_interval_s, 10)
        budget = self.settings.telemetry.latency_budget_ms
        while True:
            await asyncio.sleep(interval)
            h = self.health()
            unhealthy = [m["module"] for m in h["modules"] if not m.get("healthy")]
            lat = METRICS.snapshot()["latency"]
            cycle = lat.get("ml.cycle_ms", {})
            log.info(
                "health: %d modules (%s) | bus %d published / %d dropped | "
                "score p95 %.1fms | alerts %d",
                len(h["modules"]),
                "all ok" if not unhealthy else f"DEGRADED: {','.join(unhealthy)}",
                h["bus"]["published"], h["bus"]["dropped"],
                cycle.get("p95", 0.0), h.get("ml", {}).get("alerts", 0),
            )
            if cycle.get("p95", 0.0) > budget:
                log.warning(
                    "⚠️ latency budget exceeded: p95=%.1fms > %.0fms",
                    cycle["p95"], budget,
                )
            self._persist_state()

    async def _metrics_server(self) -> None:
        """Minimal Prometheus + JSON health endpoint."""
        from aiohttp import web

        async def metrics(_: web.Request) -> web.Response:
            return web.Response(text=METRICS.prometheus(), content_type="text/plain")

        async def health(_: web.Request) -> web.Response:
            h = self.health()
            status = 200 if all(m.get("healthy") for m in h["modules"]) else 503
            return web.json_response(h, status=status)

        app = web.Application()
        app.router.add_get("/metrics", metrics)
        app.router.add_get("/health", health)
        app.router.add_get("/", health)
        runner = web.AppRunner(app)
        await runner.setup()
        port = self.settings.telemetry.metrics_port
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log.info("metrics endpoint on :%d (/metrics, /health)", port)
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Event().wait()
        await runner.cleanup()

    def _persist_state(self) -> None:
        path = self.settings.telemetry.state_file
        if not path:
            return
        with contextlib.suppress(Exception):
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(self.health(), indent=2, default=str))

    def health(self) -> dict[str, Any]:
        return {
            "uptime_s": round(time.monotonic() - self.started_at, 1) if self.started_at else 0.0,
            "bus": self.bus.stats.snapshot() if self.bus else {},
            "modules": [m.health() for m in self.modules],
            "ml": self.ml.health() if self.ml else {},
            "alerts": self.router.health() if self.router else {},
            "bot": self.bot.health() if self.bot else {},
            "metrics": METRICS.snapshot(),
        }
