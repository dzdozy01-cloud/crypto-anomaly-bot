"""Interactive Telegram bot: long-polling command interface.

Implemented directly against the Bot API with aiohttp (no python-telegram-bot
dependency), so the whole surveillance stack keeps a single async HTTP client
style and no extra framework.

Commands
--------
``/start``, ``/help``      usage
``/status``                system health, uptime, module state
``/scores``                current manipulation scores, ranked
``/check <ASSET>``         on-demand scoring of one asset
``/watch``/``/unwatch``    manage the alert subscription for this chat
``/threshold <n>``         adjust the alert threshold at runtime
``/mute <minutes>``        temporarily silence alerts
``/metrics``               latency percentiles and throughput counters
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from ..alerting.formatter import format_telegram
from ..core.resilience import BackoffPolicy
from ..core.schema import AnomalySignal
from ..core.telemetry import METRICS

log = logging.getLogger(__name__)

__all__ = ["TelegramBot"]

CommandHandler = Callable[[list[str], int], Awaitable[str]]


class TelegramBot:
    """Long-polling Telegram bot with a pluggable command table."""

    API = "https://api.telegram.org/bot{token}/{method}"

    def __init__(
        self,
        token: str,
        default_chat_id: str = "",
        allowed_chats: list[str] | None = None,
        poll_timeout: int = 30,
    ) -> None:
        self.token = token
        self.default_chat_id = str(default_chat_id or "")
        self.allowed_chats = {str(c) for c in (allowed_chats or []) if c}
        if self.default_chat_id:
            self.allowed_chats.add(self.default_chat_id)
        self.poll_timeout = poll_timeout

        self._session: aiohttp.ClientSession | None = None
        self._offset = 0
        self._task: asyncio.Task[None] | None = None
        self.running = False
        self.subscribers: set[str] = {self.default_chat_id} if self.default_chat_id else set()
        self.muted_until: float = 0.0
        self.commands: dict[str, CommandHandler] = {}
        self.descriptions: dict[str, str] = {}
        self.messages_sent = 0
        self.commands_handled = 0
        self._register_builtin()

    # ---- plumbing --------------------------------------------------------
    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.poll_timeout + 15)
            )
        return self._session

    async def _api(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        session = await self.session()
        url = self.API.format(token=self.token, method=method)
        async with session.post(url, json=payload or {}) as resp:
            data = await resp.json(content_type=None)
        if not data.get("ok"):
            raise RuntimeError(f"telegram {method}: {data.get('description')}")
        return data

    async def send(self, chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
        # Telegram hard-caps messages at 4096 chars.
        chunks = [text[i: i + 4000] for i in range(0, len(text), 4000)] or [""]
        ok = True
        for chunk in chunks:
            try:
                await self._api(
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": chunk,
                        "parse_mode": parse_mode,
                        "disable_web_page_preview": True,
                    },
                )
                self.messages_sent += 1
            except Exception as exc:
                log.warning("telegram send failed: %s", exc)
                ok = False
        return ok

    async def broadcast_signal(self, signal: AnomalySignal) -> None:
        """Push an alert to every subscribed chat (respecting mute)."""
        if time.monotonic() < self.muted_until:
            log.debug("alerts muted; skipping broadcast")
            return
        payload = format_telegram(signal)
        for chat_id in list(self.subscribers):
            await self.send(chat_id, payload["text"])

    # ---- command registry -------------------------------------------------
    def command(self, name: str) -> Callable[[CommandHandler], CommandHandler]:
        def deco(fn: CommandHandler) -> CommandHandler:
            self.commands[name] = fn
            return fn
        return deco

    def register(self, name: str, handler: CommandHandler, description: str = "") -> None:
        self.commands[name] = handler
        if description:
            self.descriptions[name] = description

    def _register_builtin(self) -> None:
        async def help_cmd(args: list[str], chat_id: int) -> str:
            return (
                "<b>🛡 CADB — Crypto Anomaly Detection Bot</b>\n\n"
                "<b>Commands</b>\n"
                "/status — system health &amp; module state\n"
                "/scores — current manipulation scores\n"
                "/check &lt;ASSET&gt; — score an asset on demand\n"
                "/watch — subscribe this chat to alerts\n"
                "/unwatch — unsubscribe\n"
                "/threshold &lt;0-100&gt; — set alert threshold\n"
                "/mute &lt;minutes&gt; — silence alerts temporarily\n"
                "/metrics — latency &amp; throughput\n"
                "/help — this message\n\n"
                "<i>Monitoring: exchange microstructure · on-chain whales · "
                "social sentiment · ML classifier</i>"
            )

        async def watch_cmd(args: list[str], chat_id: int) -> str:
            self.subscribers.add(str(chat_id))
            return "✅ This chat will receive manipulation alerts."

        async def unwatch_cmd(args: list[str], chat_id: int) -> str:
            self.subscribers.discard(str(chat_id))
            return "🔕 Unsubscribed from alerts."

        async def mute_cmd(args: list[str], chat_id: int) -> str:
            minutes = 30.0
            if args:
                with contextlib.suppress(ValueError):
                    minutes = float(args[0])
            self.muted_until = time.monotonic() + minutes * 60
            return f"🔇 Alerts muted for {minutes:.0f} minutes."

        async def metrics_cmd(args: list[str], chat_id: int) -> str:
            snap = METRICS.snapshot()
            lines = [f"<b>📊 Metrics</b>  <i>uptime {snap['uptime_s']:.0f}s</i>", ""]
            if snap["latency"]:
                lines.append("<b>Latency (ms)</b>")
                for name, s in sorted(snap["latency"].items()):
                    if s["count"]:
                        lines.append(
                            f"  <code>{name:<20}</code> p50 {s['p50']:.1f} "
                            f"p95 {s['p95']:.1f} p99 {s['p99']:.1f} (n={s['count']})"
                        )
            if snap["counters"]:
                lines += ["", "<b>Counters</b>"]
                for k, v in sorted(snap["counters"].items())[:18]:
                    lines.append(f"  <code>{k:<28}</code> {v:,.0f}")
            return "\n".join(lines)

        self.register("help", help_cmd, "Show all commands")
        self.register("start", help_cmd, "Show all commands")
        self.register("watch", watch_cmd, "Subscribe this chat to alerts")
        self.register("unwatch", unwatch_cmd, "Unsubscribe from alerts")
        self.register("mute", mute_cmd, "Silence alerts for N minutes")
        self.register("metrics", metrics_cmd, "Latency and throughput")

    # ---- polling ----------------------------------------------------------
    async def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or update.get("edited_message")
        if not message:
            return
        text = (message.get("text") or "").strip()
        chat_id = message.get("chat", {}).get("id")
        if not text.startswith("/") or chat_id is None:
            return
        if self.allowed_chats and str(chat_id) not in self.allowed_chats:
            log.warning("ignoring command from unauthorised chat %s", chat_id)
            return

        parts = text.split()
        cmd = parts[0][1:].split("@")[0].lower()
        args = parts[1:]
        handler = self.commands.get(cmd)
        if handler is None:
            await self.send(str(chat_id), f"❓ Unknown command <code>/{cmd}</code>. Try /help")
            return
        self.commands_handled += 1
        try:
            reply = await handler(args, chat_id)
        except Exception as exc:
            log.exception("command /%s failed", cmd)
            reply = f"⚠️ Command failed: <code>{type(exc).__name__}: {exc}</code>"
        if reply:
            await self.send(str(chat_id), reply)

    async def _poll_loop(self) -> None:
        policy = BackoffPolicy(initial=1.0, maximum=60.0)
        log.info("telegram bot polling started")
        while self.running:
            try:
                data = await self._api(
                    "getUpdates",
                    {
                        "offset": self._offset,
                        "timeout": self.poll_timeout,
                        "allowed_updates": ["message", "edited_message"],
                    },
                )
                for update in data.get("result", []):
                    self._offset = max(self._offset, update.get("update_id", 0) + 1)
                    await self._handle_update(update)
                policy.reset()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("telegram poll error: %s", exc)
                await policy.sleep()

    async def start(self) -> None:
        if not self.token:
            log.warning("telegram bot disabled (no token)")
            return
        self.running = True
        with contextlib.suppress(Exception):
            me = await self._api("getMe")
            log.info("telegram bot connected: @%s", me["result"].get("username"))
        # Telegram shows at most 100 commands; ours is well under that.
        menu = [
            {"command": name, "description": desc[:256]}
            for name, desc in sorted(self.descriptions.items())
            if name != "start"
        ]
        if menu:
            with contextlib.suppress(Exception):
                await self._api("setMyCommands", {"commands": menu})
        self._task = asyncio.create_task(self._poll_loop(), name="telegram-bot")

    async def stop(self) -> None:
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._session and not self._session.closed:
            await self._session.close()

    def health(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "subscribers": len(self.subscribers),
            "messages_sent": self.messages_sent,
            "commands_handled": self.commands_handled,
            "muted": time.monotonic() < self.muted_until,
        }
