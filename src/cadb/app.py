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
import os
import signal
import time
from collections import deque
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
        self.alerts_paused = False
        self.alert_history: deque[AnomalySignal] = deque(maxlen=200)
        self._ondemand: Any = None  # lazily created REST scanner (bot commands)
        self._shutdown = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    # ---- wiring ----------------------------------------------------------
    async def setup(self) -> None:
        s = self.settings
        setup_logging(s.telemetry.log_level, s.telemetry.json_logs)
        from . import __version__

        # Print a build fingerprint. "Did my rebuild actually take effect?" was
        # otherwise unanswerable from the logs — `docker compose up -d` reports
        # "Running" and silently keeps the old container when only source
        # changed, so a fix can appear not to work when it simply is not loaded.
        build = os.getenv("CADB_BUILD", "")
        stamp = ""
        with contextlib.suppress(Exception):
            src = Path(__file__).resolve().parent
            newest = max(f.stat().st_mtime for f in src.rglob("*.py"))
            stamp = time.strftime("%Y-%m-%d %H:%M", time.localtime(newest))
        log.info("=" * 68)
        log.info("  CADB — Crypto Anomaly Detection Bot")
        log.info(
            "  v%s%s%s",
            __version__,
            f" · build {build}" if build else "",
            f" · code {stamp}" if stamp else "",
        )
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
        self.alert_history.append(signal)
        if self.alerts_paused:
            log.debug("alerts paused; not dispatching %s", signal.asset_pair)
            return
        # The router owns de-duplication (cooldown + severity escalation).
        # The interactive bot must broadcast ONLY when the router actually
        # dispatched, otherwise every scoring cycle re-sends the same alert:
        # in production this produced ~60 identical messages in 30 seconds
        # while the router correctly suppressed all but one.
        dispatched = False
        if self.router:
            dispatched = await self.router.dispatch(signal)

        # Skip the bot broadcast when a Telegram sink already delivered it —
        # otherwise subscribers get the same alert twice.
        router_has_telegram = bool(
            self.router and any(s.name == "telegram" for s in self.router.sinks)
        )
        if dispatched and not router_has_telegram and self.bot and self.bot.running:
            with contextlib.suppress(Exception):
                await self.bot.broadcast_signal(signal)

    def _register_bot_commands(self) -> None:
        """Attach the full command surface (see :mod:`cadb.bot.commands`)."""
        assert self.bot is not None
        from .bot.commands import register_commands

        register_commands(self.bot, self)

    # ---- lifecycle ---------------------------------------------------------
    def _install_asyncio_exception_handler(self) -> None:
        """Tame exceptions raised inside third-party asyncio callbacks.

        ccxt.pro dispatches WebSocket frames from `call_soon` callbacks. When a
        decode fails (e.g. MEXC protobuf frames without the protobuf package)
        the traceback escapes to the default handler and prints a 25-line dump
        for *every frame* — thousands of lines that bury real signal, while our
        own supervisor never sees the error because it is not in the await path.
        """
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        seen: dict[str, int] = {}

        def handler(loop_: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            exc = context.get("exception")
            name = type(exc).__name__ if exc else "unknown"
            message = str(exc) if exc else context.get("message", "")

            key = f"{name}:{message[:80]}"
            count = seen.get(key, 0) + 1
            seen[key] = count

            # Log the first occurrence in full, then exponentially less often.
            if count == 1 or (count & (count - 1)) == 0:
                hint = ""
                if "protobuf" in message.lower():
                    hint = (
                        " — install the exchange extra "
                        "(`pip install 'cadb[exchange]'`) or drop this venue"
                    )
                log.warning(
                    "suppressed callback error x%d: %s: %s%s",
                    count, name, message[:200], hint,
                )
            if previous is not None:
                return
            # Deliberately swallow: the supervised task will reconnect.

        loop.set_exception_handler(handler)

    async def start(self) -> None:
        self.started_at = time.monotonic()
        self._install_asyncio_exception_handler()
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
        # The on-demand scanner holds ccxt REST sessions created lazily by the
        # bot commands; leaking them logs "Unclosed connector" on exit.
        scanner = getattr(self, "_ondemand", None)
        if scanner is not None:
            with contextlib.suppress(Exception):
                await scanner.close()
            self._ondemand = None
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
