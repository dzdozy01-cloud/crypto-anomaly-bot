"""Alert routing: de-duplication, rate limiting, and multi-sink delivery.

Alert fatigue is a real failure mode — a single manipulation episode generates
signals every scoring cycle for minutes. The router therefore applies:

* **Per-asset cooldown** — one alert per asset per ``cooldown_s``…
* **…with severity escalation override** — a MEDIUM followed by a CRITICAL still
  gets through, because suppressing an escalation is worse than a duplicate.
* **Global token-bucket rate limit** — hard ceiling on outbound messages.
* **Independent sink failure** — Telegram being down never blocks Discord.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import aiohttp

from ..core.config import AlertConfig
from ..core.resilience import BackoffPolicy, CircuitBreaker, RateLimiter
from ..core.schema import AnomalySignal, Severity
from ..core.telemetry import METRICS
from .formatter import format_discord, format_plain, format_telegram

log = logging.getLogger(__name__)

__all__ = ["AlertRouter", "AlertSink", "TelegramSink", "DiscordSink", "WebhookSink", "ConsoleSink"]


class AlertSink(ABC):
    """A destination for alerts."""

    name: str = "sink"

    def __init__(self) -> None:
        self.sent = 0
        self.failed = 0
        self.breaker = CircuitBreaker(name=f"sink:{self.name}", failure_threshold=5,
                                      recovery_timeout=60)

    @abstractmethod
    async def deliver(self, signal: AnomalySignal) -> bool: ...

    async def send(self, signal: AnomalySignal) -> bool:
        try:
            ok = await self.breaker.call(self.deliver, signal)
        except Exception as exc:
            self.failed += 1
            log.warning("sink %s failed: %s", self.name, exc)
            return False
        if ok:
            self.sent += 1
        else:
            self.failed += 1
        return ok

    async def close(self) -> None: ...

    def health(self) -> dict[str, Any]:
        return {
            "sink": self.name,
            "sent": self.sent,
            "failed": self.failed,
            "circuit": self.breaker.state.value,
        }


class _HTTPSink(AlertSink):
    """Shared aiohttp session management with retry."""

    def __init__(self, timeout_s: float = 12.0, retries: int = 3) -> None:
        super().__init__()
        self.timeout_s = timeout_s
        self.retries = retries
        self._session: aiohttp.ClientSession | None = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_s)
            )
        return self._session

    async def _post(self, url: str, payload: dict[str, Any]) -> bool:
        session = await self.session()
        policy = BackoffPolicy(initial=0.5, maximum=8.0, jitter=True)
        for attempt in range(self.retries):
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status in (200, 201, 204):
                        return True
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 5))
                        log.warning("%s rate limited; retry in %.1fs", self.name, retry_after)
                        await asyncio.sleep(min(retry_after, 30))
                        continue
                    body = (await resp.text())[:300]
                    log.warning("%s HTTP %d: %s", self.name, resp.status, body)
                    if 400 <= resp.status < 500:
                        return False  # client error: retrying will not help
            except Exception as exc:
                log.debug("%s attempt %d failed: %s", self.name, attempt + 1, exc)
            if attempt < self.retries - 1:
                await policy.sleep()
        return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


class TelegramSink(_HTTPSink):
    """Telegram Bot API sink."""

    name = "telegram"

    def __init__(self, bot_token: str, chat_id: str) -> None:
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    async def deliver(self, signal: AnomalySignal) -> bool:
        payload = format_telegram(signal)
        payload["chat_id"] = self.chat_id
        if signal.severity is Severity.CRITICAL:
            payload["disable_notification"] = False
        return await self._post(self.url, payload)


class DiscordSink(_HTTPSink):
    """Discord webhook sink."""

    name = "discord"

    def __init__(self, webhook_url: str) -> None:
        super().__init__()
        self.webhook_url = webhook_url

    async def deliver(self, signal: AnomalySignal) -> bool:
        return await self._post(self.webhook_url, format_discord(signal))


class WebhookSink(_HTTPSink):
    """Generic JSON webhook (PagerDuty, Slack-compatible, internal services)."""

    name = "webhook"

    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url

    async def deliver(self, signal: AnomalySignal) -> bool:
        payload = signal.model_dump(mode="json")
        payload["text"] = format_plain(signal)
        return await self._post(self.url, payload)


class ConsoleSink(AlertSink):
    """Logs alerts — used in dry-run mode and as a last-resort fallback."""

    name = "console"

    async def deliver(self, signal: AnomalySignal) -> bool:
        log.warning("\n%s", format_plain(signal))
        return True


@dataclass
class _AlertRecord:
    last_sent: float
    severity_rank: int
    count: int = 0


class AlertRouter:
    """Fan-out with de-duplication, escalation override and rate limiting."""

    def __init__(self, config: AlertConfig) -> None:
        self.config = config
        self.sinks: list[AlertSink] = []
        self._history: dict[str, _AlertRecord] = {}
        self._limiter = RateLimiter(
            rate_per_sec=max(config.max_alerts_per_min / 60.0, 0.01),
            burst=max(3, config.max_alerts_per_min // 3),
        )
        self.suppressed = 0
        self.dispatched = 0
        self._build_sinks()

    def _build_sinks(self) -> None:
        cfg = self.config
        if cfg.dry_run:
            self.sinks.append(ConsoleSink())
            log.info("alert router in DRY-RUN mode (console only)")
            return
        if cfg.telegram_bot_token and cfg.telegram_chat_id:
            self.sinks.append(TelegramSink(cfg.telegram_bot_token, cfg.telegram_chat_id))
        if cfg.discord_webhook_url:
            self.sinks.append(DiscordSink(cfg.discord_webhook_url))
        for url in cfg.generic_webhooks:
            if url:
                self.sinks.append(WebhookSink(url))
        if not self.sinks:
            log.warning("no alert sinks configured; falling back to console output")
            self.sinks.append(ConsoleSink())
        log.info("alert sinks: %s", ", ".join(s.name for s in self.sinks))

    def add_sink(self, sink: AlertSink) -> None:
        self.sinks.append(sink)

    # ---- gating ----------------------------------------------------------
    def _should_send(self, signal: AnomalySignal) -> tuple[bool, str]:
        if signal.score < self.config.min_score:
            return False, "below threshold"
        key = f"{signal.asset_pair}:{signal.venue}"
        record = self._history.get(key)
        now = time.monotonic()
        if record is None:
            return True, "first alert"
        elapsed = now - record.last_sent
        if signal.severity.rank > record.severity_rank:
            return True, "severity escalation"
        if elapsed >= self.config.cooldown_s:
            return True, "cooldown elapsed"
        return False, f"cooldown ({self.config.cooldown_s - elapsed:.0f}s left)"

    async def dispatch(self, signal: AnomalySignal) -> bool:
        """Route one signal to all sinks. Returns True if anything was sent."""
        allowed, reason = self._should_send(signal)
        if not allowed:
            self.suppressed += 1
            METRICS.incr("alerts.suppressed")
            log.debug("alert suppressed for %s: %s", signal.asset_pair, reason)
            return False

        await self._limiter.acquire()
        key = f"{signal.asset_pair}:{signal.venue}"
        record = self._history.get(key)
        self._history[key] = _AlertRecord(
            last_sent=time.monotonic(),
            severity_rank=signal.severity.rank,
            count=(record.count + 1) if record else 1,
        )

        results = await asyncio.gather(
            *(sink.send(signal) for sink in self.sinks), return_exceptions=True
        )
        ok = sum(1 for r in results if r is True)
        self.dispatched += 1
        METRICS.incr("alerts.dispatched")
        if ok == 0:
            METRICS.incr("alerts.delivery_failed")
            log.error("alert delivery failed on all %d sink(s)", len(self.sinks))
        log.info(
            "alert dispatched %s score=%.1f -> %d/%d sinks (%s)",
            signal.asset_pair, signal.score, ok, len(self.sinks), reason,
        )
        return ok > 0

    async def close(self) -> None:
        for sink in self.sinks:
            with contextlib.suppress(Exception):
                await sink.close()

    def health(self) -> dict[str, Any]:
        return {
            "dispatched": self.dispatched,
            "suppressed": self.suppressed,
            "sinks": [s.health() for s in self.sinks],
            "tracked_keys": len(self._history),
        }
