"""Social data sources: X (Twitter) API v2, Telegram, and a simulator.

Each source yields :class:`SocialPost` objects. Missing credentials degrade
gracefully — the source logs once and idles rather than crashing the module.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import aiohttp

from ...core.resilience import RateLimiter
from ...core.schema import now_ms
from .botfarm import SocialPost
from .sentiment import extract_tickers

log = logging.getLogger(__name__)

__all__ = ["SocialSource", "XSource", "TelegramSource", "SimulatedSocialSource"]


class SocialSource(ABC):
    """Abstract stream of social posts."""

    platform: str = "unknown"

    def __init__(self, tickers: list[str]) -> None:
        self.tickers = [t.upper() for t in tickers]
        self.posts_seen = 0
        self.enabled = True

    @abstractmethod
    def stream(self) -> AsyncIterator[SocialPost]: ...

    async def close(self) -> None: ...


class XSource(SocialSource):
    """X API v2 filtered stream, with recent-search polling fallback.

    The filtered stream endpoint is preferred (true push, no polling delay); if
    the bearer token lacks stream access we fall back to `recent search`
    polling, which most access tiers allow.
    """

    platform = "x"
    STREAM_URL = "https://api.twitter.com/2/tweets/search/stream"
    RULES_URL = "https://api.twitter.com/2/tweets/search/stream/rules"
    SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

    TWEET_FIELDS = "created_at,author_id,public_metrics,lang,referenced_tweets"
    USER_FIELDS = "created_at,public_metrics,verified"

    def __init__(self, tickers: list[str], bearer_token: str, poll_interval_s: float = 15.0) -> None:
        super().__init__(tickers)
        self.token = bearer_token
        self.poll_interval_s = poll_interval_s
        self._session: aiohttp.ClientSession | None = None
        self._limiter = RateLimiter(rate_per_sec=0.2, burst=3)  # 180 req / 15 min
        self._user_cache: dict[str, dict[str, Any]] = {}
        self._seen: set[str] = set()
        self.enabled = bool(bearer_token)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=aiohttp.ClientTimeout(total=None, sock_read=90),
            )
        return self._session

    def _query(self) -> str:
        cashtags = " OR ".join(f"${t}" for t in self.tickers[:20])
        return f"({cashtags}) -is:retweet lang:en"

    async def _sync_rules(self) -> None:
        session = await self._get_session()
        async with session.get(self.RULES_URL) as resp:
            existing = (await resp.json()).get("data") or []
        if existing:
            async with session.post(
                self.RULES_URL, json={"delete": {"ids": [r["id"] for r in existing]}}
            ) as resp:
                await resp.read()
        # X caps rule length; chunk the cashtag list.
        rules = []
        chunk: list[str] = []
        for t in self.tickers:
            chunk.append(f"${t}")
            if len(" OR ".join(chunk)) > 380:
                rules.append({"value": f"({' OR '.join(chunk[:-1])}) -is:retweet lang:en"})
                chunk = [f"${t}"]
        if chunk:
            rules.append({"value": f"({' OR '.join(chunk)}) -is:retweet lang:en"})
        async with session.post(self.RULES_URL, json={"add": rules}) as resp:
            body = await resp.json()
            if resp.status >= 400:
                raise RuntimeError(f"rule sync failed: {body}")
        log.info("X stream rules synced (%d rules)", len(rules))

    def _to_post(self, tweet: dict[str, Any], users: dict[str, dict[str, Any]]) -> SocialPost:
        author_id = str(tweet.get("author_id", ""))
        user = users.get(author_id, {})
        age_days = None
        if created := user.get("created_at"):
            with contextlib.suppress(ValueError, TypeError):
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                age_days = (datetime.now(UTC) - dt).total_seconds() / 86400.0
        metrics = user.get("public_metrics", {})
        text = tweet.get("text", "")
        ts = now_ms()
        if created_at := tweet.get("created_at"):
            with contextlib.suppress(ValueError, TypeError):
                ts = int(
                    datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp() * 1000
                )
        found = extract_tickers(text) & set(self.tickers)
        return SocialPost(
            platform=self.platform,
            post_id=str(tweet.get("id", "")),
            author_id=author_id,
            text=text,
            timestamp=ts,
            tickers=found or {t for t in self.tickers if t.lower() in text.lower()},
            author_age_days=age_days,
            author_followers=metrics.get("followers_count"),
            author_post_count=metrics.get("tweet_count"),
            is_reply=any(
                r.get("type") == "replied_to" for r in tweet.get("referenced_tweets", []) or []
            ),
        )

    async def stream(self) -> AsyncIterator[SocialPost]:
        if not self.enabled:
            log.warning("X source disabled (no bearer token)")
            return
        try:
            await self._sync_rules()
            async for post in self._filtered_stream():
                yield post
        except Exception as exc:
            log.warning("X filtered stream unavailable (%s); switching to search polling", exc)
            async for post in self._search_poll():
                yield post

    async def _filtered_stream(self) -> AsyncIterator[SocialPost]:
        import json

        session = await self._get_session()
        params = {
            "tweet.fields": self.TWEET_FIELDS,
            "expansions": "author_id",
            "user.fields": self.USER_FIELDS,
        }
        async with session.get(self.STREAM_URL, params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(f"stream HTTP {resp.status}: {await resp.text()}")
            log.info("X filtered stream connected")
            async for raw in resp.content:
                line = raw.strip()
                if not line:
                    continue
                with contextlib.suppress(json.JSONDecodeError):
                    payload = json.loads(line)
                    tweet = payload.get("data")
                    if not tweet:
                        continue
                    users = {
                        u["id"]: u for u in (payload.get("includes", {}).get("users") or [])
                    }
                    self.posts_seen += 1
                    yield self._to_post(tweet, users)

    async def _search_poll(self) -> AsyncIterator[SocialPost]:
        session = await self._get_session()
        while True:
            await self._limiter.acquire()
            params = {
                "query": self._query(),
                "max_results": "100",
                "tweet.fields": self.TWEET_FIELDS,
                "expansions": "author_id",
                "user.fields": self.USER_FIELDS,
            }
            try:
                async with session.get(self.SEARCH_URL, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(60)
                        continue
                    resp.raise_for_status()
                    payload = await resp.json()
            except Exception as exc:
                log.warning("X search poll failed: %s", exc)
                await asyncio.sleep(self.poll_interval_s * 2)
                continue

            users = {u["id"]: u for u in (payload.get("includes", {}).get("users") or [])}
            for tweet in payload.get("data") or []:
                tid = str(tweet.get("id"))
                if tid in self._seen:
                    continue
                self._seen.add(tid)
                if len(self._seen) > 50_000:
                    self._seen = set(list(self._seen)[-20_000:])
                self.posts_seen += 1
                yield self._to_post(tweet, users)
            await asyncio.sleep(self.poll_interval_s)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


class TelegramSource(SocialSource):
    """Telegram channel monitor.

    Uses Telethon (MTProto) when installed and credentialed — that is the only
    way to read public channels in real time. Without it the source idles.
    """

    platform = "telegram"

    def __init__(
        self,
        tickers: list[str],
        api_id: str = "",
        api_hash: str = "",
        channels: list[str] | None = None,
        session_name: str = "cadb_social",
    ) -> None:
        super().__init__(tickers)
        self.api_id = api_id
        self.api_hash = api_hash
        self.channels = channels or []
        self.session_name = session_name
        self._client: Any = None
        self.enabled = bool(api_id and api_hash and self.channels)

    async def stream(self) -> AsyncIterator[SocialPost]:
        if not self.enabled:
            log.info("Telegram source disabled (missing api_id/api_hash/channels)")
            return
        try:
            from telethon import TelegramClient, events
        except ImportError:
            log.warning("telethon not installed; Telegram source inactive")
            return

        queue: asyncio.Queue[SocialPost] = asyncio.Queue(maxsize=2000)
        client = TelegramClient(self.session_name, int(self.api_id), self.api_hash)
        self._client = client
        await client.start()
        log.info("Telegram client connected; watching %d channel(s)", len(self.channels))

        @client.on(events.NewMessage(chats=self.channels))
        async def _handler(event: Any) -> None:  # pragma: no cover - network callback
            text = event.message.message or ""
            if not text:
                return
            found = extract_tickers(text) & set(self.tickers)
            if not found:
                found = {t for t in self.tickers if t.lower() in text.lower()}
            if not found:
                return
            sender = await event.get_sender()
            post = SocialPost(
                platform=self.platform,
                post_id=str(event.message.id),
                author_id=str(getattr(sender, "id", "channel")),
                text=text,
                timestamp=int(event.message.date.timestamp() * 1000),
                tickers=found,
                author_followers=getattr(sender, "participants_count", None),
            )
            self.posts_seen += 1
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(post)

        while True:
            yield await queue.get()

    async def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.disconnect()


class SimulatedSocialSource(SocialSource):
    """Synthetic social feed with organic chatter and injectable shill campaigns."""

    platform = "sim"

    ORGANIC = [
        "{t} looking strong here, nice consolidation above support",
        "not sure about {t} at these levels, waiting for a pullback",
        "{t} chart is setting up for a breakout imo",
        "anyone else watching {t} volume today? unusual",
        "took some profit on {t}, still holding a runner",
        "{t} funding rates flipping negative, could squeeze",
        "bearish divergence on {t} 4h, careful up here",
        "just dca'd more {t}, long term thesis intact",
        "{t} liquidity looks thin this morning",
        "macro is rough, {t} holding up better than most",
    ]
    SHILL = [
        "🚀🚀 ${t} IS ABOUT TO EXPLODE! 100x GEM! DON'T MISS OUT! 🚀🚀",
        "${t} TO THE MOON! 🌙 BUY NOW BEFORE IT'S TOO LATE! 💎",
        "BREAKING: ${t} MASSIVE PARTNERSHIP INCOMING! LOAD UP NOW! 🔥",
        "${t} 100x GEM! WHALES ARE ACCUMULATING! 🚀 GET IN EARLY!",
        "🔥 ${t} NEXT 1000x! MASSIVE PUMP LOADING! BUY BUY BUY 🚀",
    ]
    FUD = [
        "${t} is a scam, team is dumping on retail, get out now",
        "WARNING: ${t} liquidity being pulled, this is a rugpull 🚨",
        "${t} team wallet just moved funds to exchange, RUN",
    ]

    def __init__(
        self,
        tickers: list[str],
        base_rate_hz: float = 2.5,
        seed: int = 11,
        campaign_probability: float = 0.02,
    ) -> None:
        super().__init__(tickers)
        self.base_rate_hz = base_rate_hz
        self.rng = random.Random(seed)
        self.campaign_probability = campaign_probability
        self._campaign: dict[str, float] = {}
        self._campaign_kind: dict[str, str] = {}
        self._farm_authors: dict[str, list[tuple[str, float, int]]] = {}

    def inject_campaign(self, ticker: str, duration_s: float = 45.0, kind: str = "shill") -> None:
        """Start a coordinated campaign on ``ticker`` (used by the demo/tests)."""
        loop_t = asyncio.get_event_loop().time()
        self._campaign[ticker] = loop_t + duration_s
        self._campaign_kind[ticker] = kind
        # A cohort of accounts created within days of each other => tiny age CV.
        base_age = self.rng.uniform(4, 16)
        self._farm_authors[ticker] = [
            (
                f"farm_{ticker}_{i}",
                base_age + self.rng.uniform(-0.8, 0.8),   # deliberately low variance
                self.rng.randint(12, 180),                # uniformly low follower counts
            )
            for i in range(self.rng.randint(18, 40))
        ]
        log.info("[sim] injected %s campaign on %s for %.0fs", kind, ticker, duration_s)

    def _active(self, ticker: str) -> bool:
        return self._campaign.get(ticker, 0.0) > asyncio.get_event_loop().time()

    async def stream(self) -> AsyncIterator[SocialPost]:
        counter = 0
        while True:
            active_tickers = [t for t in self.tickers if self._active(t)]
            rate = self.base_rate_hz * (5.0 if active_tickers else 1.0)
            await asyncio.sleep(self.rng.expovariate(max(rate, 0.1)))

            if not active_tickers and self.rng.random() < self.campaign_probability:
                self.inject_campaign(
                    self.rng.choice(self.tickers),
                    self.rng.uniform(30, 70),
                    self.rng.choice(["shill", "shill", "fud"]),
                )

            counter += 1
            ticker = (
                self.rng.choice(active_tickers)
                if active_tickers and self.rng.random() < 0.8
                else self.rng.choice(self.tickers)
            )
            campaigning = self._active(ticker)
            kind = self._campaign_kind.get(ticker, "shill")

            if campaigning and self.rng.random() < 0.75:
                pool = self.SHILL if kind == "shill" else self.FUD
                author_id, age, followers = self.rng.choice(self._farm_authors[ticker])
                text = self.rng.choice(pool).format(t=ticker)
                if self.rng.random() < 0.35:  # slight mutation to dodge exact-match filters
                    text += " " + self.rng.choice(["", "#crypto", "🚀", "!!", "#" + ticker])
            else:
                author_id = f"user_{self.rng.randint(1, 4000)}"
                age = self.rng.uniform(30, 2600)
                followers = int(self.rng.lognormvariate(5.5, 1.8))
                text = self.rng.choice(self.ORGANIC).format(t=ticker)

            self.posts_seen += 1
            yield SocialPost(
                platform=self.platform,
                post_id=f"sim_{counter}",
                author_id=author_id,
                text=text,
                timestamp=now_ms(),
                tickers={ticker},
                author_age_days=age,
                author_followers=followers,
                author_post_count=self.rng.randint(1, 9000),
            )
