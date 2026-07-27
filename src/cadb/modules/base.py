"""Base class every ingestion module inherits from.

Guarantees a uniform lifecycle (`start`/`stop`), supervised sub-tasks with
exponential backoff, health reporting, and a single `emit()` path so all
telemetry lands on the bus in the unified schema.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

from ..core.bus import EventBus
from ..core.resilience import BackoffPolicy, CircuitBreaker, ResilientTask
from ..core.schema import MarketEvent
from ..core.telemetry import METRICS

log = logging.getLogger(__name__)


class Module(ABC):
    """Lifecycle-managed telemetry producer."""

    name: str = "module"

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.log = logging.getLogger(f"cadb.{self.name}")
        self._tasks: list[ResilientTask] = []
        self._plain_tasks: list[asyncio.Task[Any]] = []
        self.running = False
        self.events_emitted = 0

    # ---- lifecycle ----------------------------------------------------
    @abstractmethod
    async def run(self) -> None:
        """Spawn the module's collectors. Must return once tasks are started."""

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.log.info("starting module %s", self.name)
        await self.run()

    async def stop(self) -> None:
        self.running = False
        for task in self._tasks:
            await task.stop()
        for t in self._plain_tasks:
            if not t.done():
                t.cancel()
        if self._plain_tasks:
            await asyncio.gather(*self._plain_tasks, return_exceptions=True)
        self._tasks.clear()
        self._plain_tasks.clear()
        await self.cleanup()
        self.log.info("module %s stopped", self.name)

    async def cleanup(self) -> None:
        """Optional: release clients/sockets."""

    # ---- helpers ------------------------------------------------------
    def supervise(
        self,
        name: str,
        factory: Callable[[], Awaitable[Any]],
        policy: BackoffPolicy | None = None,
    ) -> ResilientTask:
        """Run a long-lived coroutine under auto-reconnect supervision."""
        task = ResilientTask(
            name=f"{self.name}:{name}",
            coro_factory=factory,
            policy=policy or BackoffPolicy(),
            breaker=CircuitBreaker(name=f"{self.name}:{name}"),
        )
        task.start()
        self._tasks.append(task)
        return task

    def spawn(self, name: str, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        """Fire-and-forget internal task (not auto-restarted)."""
        task = asyncio.create_task(coro, name=f"{self.name}:{name}")
        self._plain_tasks.append(task)
        return task

    async def emit(self, event: MarketEvent) -> None:
        """Publish one normalised event to the bus."""
        self.events_emitted += 1
        METRICS.incr(f"{self.name}.events")
        await self.bus.publish(event)

    def health(self) -> dict[str, Any]:
        return {
            "module": self.name,
            "running": self.running,
            "events": self.events_emitted,
            "tasks": [t.health() for t in self._tasks],
            "healthy": self.running and all(t.healthy for t in self._tasks),
        }
