"""Reliability primitives: exponential backoff, circuit breaking, rate limiting.

Every network-facing component (WebSocket stream, RPC endpoint, HTTP API,
webhook) runs through :func:`retry_forever` or :class:`ResilientTask` so a
transient outage never kills the process and a persistent one never turns into a
reconnect storm against the venue.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = [
    "BackoffPolicy",
    "CircuitBreaker",
    "CircuitState",
    "CircuitOpenError",
    "RateLimiter",
    "ResilientTask",
    "retry_forever",
    "with_timeout",
]


@dataclass
class BackoffPolicy:
    """Exponential backoff with decorrelated jitter.

    Decorrelated jitter (AWS architecture blog) spreads reconnects far better
    than plain exponential backoff when many streams fail simultaneously — e.g.
    a venue-wide disconnect where all 30 symbol streams would otherwise retry in
    lockstep and get rate-limited on reconnect.
    """

    initial: float = 1.0
    maximum: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    reset_after: float = 120.0  # a connection alive this long resets the ladder

    _attempt: int = field(default=0, init=False)
    _sleep: float = field(default=0.0, init=False)

    def reset(self) -> None:
        self._attempt = 0
        self._sleep = 0.0

    @property
    def attempt(self) -> int:
        return self._attempt

    def next_delay(self) -> float:
        self._attempt += 1
        if self._sleep <= 0:
            base = self.initial
        else:
            base = min(self.maximum, self._sleep * self.multiplier)
        if self.jitter:
            # decorrelated jitter: U(initial, base * 3) capped at maximum
            delay = min(self.maximum, random.uniform(self.initial, max(base * 3, self.initial)))
        else:
            delay = base
        self._sleep = delay
        return delay

    async def sleep(self) -> float:
        delay = self.next_delay()
        await asyncio.sleep(delay)
        return delay


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a call is attempted while the breaker is open."""


@dataclass
class CircuitBreaker:
    """Stops hammering an endpoint that is consistently failing."""

    name: str = "circuit"
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_successes: int = 2

    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    successes: int = 0
    opened_at: float = 0.0

    def _can_attempt(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if time.monotonic() - self.opened_at >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.successes = 0
                log.info("circuit %s -> half_open", self.name)
                return True
            return False
        return True  # half-open: allow probes

    def record_success(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self.successes += 1
            if self.successes >= self.half_open_successes:
                self.state = CircuitState.CLOSED
                self.failures = 0
                log.info("circuit %s -> closed", self.name)
        else:
            self.failures = 0

    def record_failure(self) -> None:
        self.failures += 1
        if self.state is CircuitState.HALF_OPEN or self.failures >= self.failure_threshold:
            if self.state is not CircuitState.OPEN:
                log.warning("circuit %s -> OPEN after %d failures", self.name, self.failures)
            self.state = CircuitState.OPEN
            self.opened_at = time.monotonic()

    async def call(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        if not self._can_attempt():
            raise CircuitOpenError(f"circuit {self.name} is open")
        try:
            result = await fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result


class RateLimiter:
    """Token-bucket limiter, safe for concurrent coroutines."""

    def __init__(self, rate_per_sec: float, burst: int | None = None) -> None:
        self.rate = max(rate_per_sec, 1e-6)
        self.capacity = float(burst if burst is not None else max(1, int(rate_per_sec)))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                await asyncio.sleep((tokens - self._tokens) / self.rate)


async def with_timeout(coro: Awaitable[T], seconds: float, default: T | None = None) -> T | None:
    """Await with a timeout, returning ``default`` instead of raising."""
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except TimeoutError:
        return default


async def retry_forever(
    fn: Callable[[], Awaitable[Any]],
    *,
    name: str = "task",
    policy: BackoffPolicy | None = None,
    breaker: CircuitBreaker | None = None,
    on_error: Callable[[Exception, int], None] | None = None,
    max_attempts: int | None = None,
) -> None:
    """Run ``fn`` forever, reconnecting with exponential backoff on failure.

    ``fn`` is expected to be long-running (a stream read loop). Returning
    normally is treated as a disconnect and triggers a reconnect.
    """
    policy = policy or BackoffPolicy()
    attempts = 0
    while True:
        started = time.monotonic()
        try:
            if breaker is not None:
                await breaker.call(fn)
            else:
                await fn()
            log.info("%s stream ended cleanly; reconnecting", name)
        except asyncio.CancelledError:
            log.debug("%s cancelled", name)
            raise
        except CircuitOpenError:
            await asyncio.sleep(policy.maximum / 2)
            continue
        except Exception as exc:
            attempts += 1
            if on_error:
                with contextlib.suppress(Exception):
                    on_error(exc, attempts)
            log.warning("%s failed (attempt %d): %s: %s", name, attempts, type(exc).__name__, exc)
            if max_attempts is not None and attempts >= max_attempts:
                log.error("%s exceeded max attempts (%d); giving up", name, max_attempts)
                return
        # A long-lived connection means the endpoint is healthy again.
        if time.monotonic() - started >= policy.reset_after:
            policy.reset()
        delay = await policy.sleep()
        log.debug("%s reconnecting in %.2fs (attempt %d)", name, delay, policy.attempt)


class ResilientTask:
    """Supervised long-running task with backoff, breaker and health reporting."""

    def __init__(
        self,
        name: str,
        coro_factory: Callable[[], Awaitable[Any]],
        policy: BackoffPolicy | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self.name = name
        self.coro_factory = coro_factory
        self.policy = policy or BackoffPolicy()
        self.breaker = breaker or CircuitBreaker(name=name)
        self.task: asyncio.Task[None] | None = None
        self.started_at: float = 0.0
        self.restarts: int = 0
        self.last_error: str | None = None

    def _note_error(self, exc: Exception, attempts: int) -> None:
        self.restarts = attempts
        self.last_error = f"{type(exc).__name__}: {exc}"

    def start(self) -> asyncio.Task[None]:
        self.started_at = time.monotonic()
        self.task = asyncio.create_task(
            retry_forever(
                self.coro_factory,
                name=self.name,
                policy=self.policy,
                breaker=self.breaker,
                on_error=self._note_error,
            ),
            name=self.name,
        )
        return self.task

    async def stop(self) -> None:
        if self.task and not self.task.done():
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task

    @property
    def healthy(self) -> bool:
        return (
            self.task is not None
            and not self.task.done()
            and self.breaker.state is not CircuitState.OPEN
        )

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "uptime_s": round(time.monotonic() - self.started_at, 1) if self.started_at else 0.0,
            "restarts": self.restarts,
            "circuit": self.breaker.state.value,
            "last_error": self.last_error,
        }
