"""Asynchronous ingestion bus.

Two interchangeable implementations behind one interface:

* :class:`InProcessBus`  — zero-dependency asyncio fan-out. Default; lowest latency
  (~20µs per delivery) and what the test-suite uses.
* :class:`RedisBus`      — Redis Pub/Sub, for multi-process / multi-host deployments
  where collectors and the scorer run as separate services.

Both guarantee:
  * back-pressure isolation — one slow subscriber never blocks a publisher;
  * pattern subscriptions (``cadb.exchange.*``);
  * bounded per-subscriber queues with drop-oldest semantics and drop counters.
"""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .schema import MarketEvent

log = logging.getLogger(__name__)

Handler = Callable[[MarketEvent], Awaitable[None]]

try:  # pragma: no cover - optional accel
    import orjson

    def _dumps(obj: dict[str, Any]) -> bytes:
        return orjson.dumps(obj)

    def _loads(raw: bytes | str) -> dict[str, Any]:
        return orjson.loads(raw)

except ImportError:  # pragma: no cover

    def _dumps(obj: dict[str, Any]) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

    def _loads(raw: bytes | str) -> dict[str, Any]:
        return json.loads(raw)


@dataclass
class BusStats:
    published: int = 0
    delivered: int = 0
    dropped: int = 0
    errors: int = 0
    subscribers: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "published": self.published,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "errors": self.errors,
            "subscribers": self.subscribers,
        }


@dataclass
class _Subscription:
    patterns: tuple[str, ...]
    queue: asyncio.Queue[MarketEvent]
    name: str
    dropped: int = 0
    task: asyncio.Task[None] | None = field(default=None, repr=False)

    def matches(self, channel: str) -> bool:
        return any(fnmatch.fnmatchcase(channel, p) for p in self.patterns)


class EventBus(ABC):
    """Common interface for every bus implementation."""

    def __init__(self, queue_size: int = 10_000) -> None:
        self.queue_size = queue_size
        self.stats = BusStats()
        self._subs: list[_Subscription] = []
        self._closed = False

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def publish(self, event: MarketEvent) -> None: ...

    async def publish_many(self, events: list[MarketEvent]) -> None:
        for e in events:
            await self.publish(event=e)

    def subscribe(self, *patterns: str, name: str = "anon") -> _Subscription:
        """Register a subscription and return its handle (an async queue wrapper)."""
        sub = _Subscription(
            patterns=patterns or ("cadb.*",),
            queue=asyncio.Queue(maxsize=self.queue_size),
            name=name,
        )
        self._subs.append(sub)
        self.stats.subscribers = len(self._subs)
        return sub

    def unsubscribe(self, sub: _Subscription) -> None:
        with contextlib.suppress(ValueError):
            self._subs.remove(sub)
        if sub.task:
            sub.task.cancel()
        self.stats.subscribers = len(self._subs)

    async def stream(self, *patterns: str, name: str = "stream") -> AsyncIterator[MarketEvent]:
        """Async-iterate matching events. Cancels cleanly on task cancellation."""
        sub = self.subscribe(*patterns, name=name)
        try:
            while True:
                yield await sub.queue.get()
        finally:
            self.unsubscribe(sub)

    def on(self, *patterns: str, name: str | None = None) -> Callable[[Handler], Handler]:
        """Decorator registering a coroutine as a push-based consumer."""

        def deco(fn: Handler) -> Handler:
            self.add_handler(fn, *patterns, name=name or fn.__name__)
            return fn

        return deco

    def add_handler(self, handler: Handler, *patterns: str, name: str = "handler") -> _Subscription:
        sub = self.subscribe(*patterns, name=name)

        async def _pump() -> None:
            while True:
                event = await sub.queue.get()
                try:
                    await handler(event)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.stats.errors += 1
                    log.exception("bus handler %s failed", name)

        sub.task = asyncio.create_task(_pump(), name=f"bus-handler:{name}")
        return sub

    def _fanout(self, event: MarketEvent) -> None:
        """Non-blocking local delivery with drop-oldest back-pressure."""
        channel = event.channel
        for sub in self._subs:
            if not sub.matches(channel):
                continue
            try:
                sub.queue.put_nowait(event)
                self.stats.delivered += 1
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    sub.queue.get_nowait()  # evict oldest, keep the freshest tick
                with contextlib.suppress(asyncio.QueueFull):
                    sub.queue.put_nowait(event)
                sub.dropped += 1
                self.stats.dropped += 1
                if sub.dropped % 100 == 1:
                    log.warning("subscriber %s lagging: %d dropped", sub.name, sub.dropped)

    async def close(self) -> None:
        self._closed = True
        for sub in list(self._subs):
            self.unsubscribe(sub)


class InProcessBus(EventBus):
    """Single-process asyncio fan-out bus."""

    async def start(self) -> None:
        log.info("in-process event bus started")

    async def publish(self, event: MarketEvent) -> None:
        if self._closed:
            return
        self.stats.published += 1
        self._fanout(event)


class RedisBus(EventBus):
    """Redis Pub/Sub bus for distributed deployments.

    Local subscribers are still served synchronously in-process (so a single-node
    deployment keeps sub-millisecond latency) while remote consumers receive the
    JSON-encoded payload over Redis.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        queue_size: int = 10_000,
        namespace: str = "cadb",
    ) -> None:
        super().__init__(queue_size=queue_size)
        self.url = url
        self.namespace = namespace
        self._redis: Any = None
        self._pubsub: Any = None
        self._reader: asyncio.Task[None] | None = None

    async def start(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("RedisBus requires `pip install redis`") from exc

        self._redis = aioredis.from_url(self.url, decode_responses=False)
        await self._redis.ping()
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.psubscribe(f"{self.namespace}.*")
        self._reader = asyncio.create_task(self._read_loop(), name="redis-bus-reader")
        log.info("redis event bus connected: %s", self.url)

    async def _read_loop(self) -> None:
        assert self._pubsub is not None
        while not self._closed:
            try:
                msg = await self._pubsub.get_message(timeout=1.0)
                if not msg or msg.get("type") not in ("pmessage", "message"):
                    continue
                payload = _loads(msg["data"])
                if payload.pop("_origin", None) == id(self):
                    continue  # our own publish, already delivered locally
                self._fanout(MarketEvent.from_wire(payload))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats.errors += 1
                log.exception("redis bus read failure")
                await asyncio.sleep(0.5)

    async def publish(self, event: MarketEvent) -> None:
        if self._closed:
            return
        self.stats.published += 1
        self._fanout(event)  # local first — no network hop for co-located consumers
        if self._redis is not None:
            wire = event.to_wire()
            wire["_origin"] = id(self)
            try:
                await self._redis.publish(event.channel, _dumps(wire))
            except Exception:
                self.stats.errors += 1
                log.exception("redis publish failed for %s", event.channel)

    async def close(self) -> None:
        await super().close()
        if self._reader:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()
        if self._redis is not None:
            with contextlib.suppress(Exception):
                await self._redis.aclose()


async def build_bus(kind: str = "memory", url: str = "", queue_size: int = 10_000) -> EventBus:
    """Factory with graceful degradation: redis -> memory when unavailable."""
    if kind == "redis":
        bus: EventBus = RedisBus(url=url or "redis://localhost:6379/0", queue_size=queue_size)
        try:
            await bus.start()
            return bus
        except Exception as exc:
            log.warning("redis bus unavailable (%s); falling back to in-process bus", exc)
    bus = InProcessBus(queue_size=queue_size)
    await bus.start()
    return bus
