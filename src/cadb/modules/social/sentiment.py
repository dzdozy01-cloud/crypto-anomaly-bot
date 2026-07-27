"""Sentiment scoring backends.

:class:`FinBERTScorer` runs the real transformer when ``transformers`` + ``torch``
are installed; otherwise :class:`LexiconScorer` — a crypto-tuned lexicon with
negation, intensifier and emoji handling — takes over automatically. Both expose
the same ``score_batch`` API returning values in [-1, +1], so the pipeline never
branches on which backend is live.

Inference runs in a thread executor: a FinBERT forward pass is 20-80 ms, which
would otherwise block the event loop and blow the 200 ms budget.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache

log = logging.getLogger(__name__)

__all__ = ["SentimentScorer", "FinBERTScorer", "LexiconScorer", "build_scorer", "SentimentResult"]


@dataclass
class SentimentResult:
    score: float            # -1.0 (bearish) .. +1.0 (bullish)
    label: str              # positive | negative | neutral
    confidence: float       # 0..1
    backend: str = "lexicon"


# --- Crypto/finance lexicon -------------------------------------------------
_POSITIVE = {
    "moon": 1.0, "mooning": 1.0, "bullish": 0.9, "pump": 0.7, "pumping": 0.8, "rally": 0.8,
    "surge": 0.8, "soar": 0.9, "breakout": 0.8, "gains": 0.7, "profit": 0.7, "buy": 0.5,
    "long": 0.4, "accumulate": 0.6, "hodl": 0.5, "ath": 0.8, "green": 0.5, "up": 0.3,
    "rocket": 0.9, "gem": 0.8, "undervalued": 0.7, "strong": 0.6, "support": 0.4,
    "adoption": 0.7, "partnership": 0.7, "listing": 0.8, "burn": 0.6, "staking": 0.4,
    "bounce": 0.6, "recovery": 0.6, "outperform": 0.8, "upgrade": 0.6, "beat": 0.6,
    "100x": 1.0, "10x": 0.9, "parabolic": 0.9, "squeeze": 0.6, "golden": 0.6, "win": 0.6,
    "🚀": 1.0, "🌙": 0.8, "📈": 0.8, "💎": 0.7, "🔥": 0.6, "🐂": 0.8, "✅": 0.4, "💰": 0.6,
}
_NEGATIVE = {
    "dump": -0.8, "dumping": -0.9, "bearish": -0.9, "crash": -1.0, "collapse": -1.0,
    "rug": -1.0, "rugpull": -1.0, "scam": -1.0, "sell": -0.5, "short": -0.4, "fud": -0.5,
    "fear": -0.6, "panic": -0.9, "liquidated": -0.9, "liquidation": -0.8, "loss": -0.7,
    "down": -0.3, "red": -0.5, "weak": -0.6, "resistance": -0.3, "overvalued": -0.7,
    "bubble": -0.7, "exploit": -1.0, "hacked": -1.0, "hack": -0.9, "exit": -0.7,
    "delisting": -0.9, "delisted": -0.9, "halt": -0.7, "investigation": -0.7, "lawsuit": -0.7,
    "ponzi": -1.0, "honeypot": -1.0, "capitulation": -0.9, "bleeding": -0.8, "rekt": -0.9,
    "📉": -0.8, "🐻": -0.8, "💀": -0.8, "🩸": -0.8, "⚠️": -0.5, "🚨": -0.5,
}
_INTENSIFIERS = {"very": 1.5, "extremely": 1.8, "super": 1.5, "massive": 1.7, "huge": 1.6,
                 "insane": 1.7, "absolutely": 1.6, "totally": 1.4, "slightly": 0.5, "kinda": 0.6}
_NEGATIONS = {"not", "no", "never", "isn't", "aren't", "wasn't", "won't", "don't", "doesn't",
              "didn't", "can't", "cannot", "nothing", "nobody", "without"}

_TOKEN_RE = re.compile(r"[\w'#$]+|[\U0001F000-\U0001FAFF\u2600-\u27BF]")
_URL_RE = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE = re.compile(r"@\w+")
_CASHTAG_RE = re.compile(r"[$#]([A-Za-z]{2,10})\b")


def normalize_text(text: str) -> str:
    text = _URL_RE.sub(" ", text)
    text = _MENTION_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_tickers(text: str) -> set[str]:
    """Pull ``$BTC`` / ``#ETH`` style cashtags out of a post."""
    return {m.upper() for m in _CASHTAG_RE.findall(text)}


class SentimentScorer(ABC):
    """Common interface for all sentiment backends."""

    backend: str = "base"

    @abstractmethod
    def score_sync(self, texts: list[str]) -> list[SentimentResult]: ...

    async def score_batch(self, texts: list[str]) -> list[SentimentResult]:
        """Off-loop scoring so heavy models never stall the ingestion path."""
        if not texts:
            return []
        return await asyncio.get_running_loop().run_in_executor(None, self.score_sync, texts)

    async def score(self, text: str) -> SentimentResult:
        return (await self.score_batch([text]))[0]


class LexiconScorer(SentimentScorer):
    """Fast rule-based scorer with negation and intensifier handling.

    Roughly 0.02 ms per post — used as the default so the system has zero heavy
    dependencies, and as the fallback whenever FinBERT fails to load.
    """

    backend = "lexicon"

    def __init__(self) -> None:
        self._cache: dict[str, SentimentResult] = {}

    @staticmethod
    @lru_cache(maxsize=4096)
    def _tokens(text: str) -> tuple[str, ...]:
        return tuple(_TOKEN_RE.findall(text.lower()))

    def score_sync(self, texts: list[str]) -> list[SentimentResult]:
        return [self._score_one(t) for t in texts]

    def _score_one(self, text: str) -> SentimentResult:
        cached = self._cache.get(text)
        if cached is not None:
            return cached
        tokens = self._tokens(normalize_text(text))
        total = 0.0
        hits = 0
        for i, tok in enumerate(tokens):
            weight = _POSITIVE.get(tok, 0.0) or _NEGATIVE.get(tok, 0.0)
            if weight == 0.0:
                continue
            hits += 1
            mult = 1.0
            for prev in tokens[max(0, i - 2): i]:
                if prev in _INTENSIFIERS:
                    mult *= _INTENSIFIERS[prev]
                if prev in _NEGATIONS:
                    mult *= -0.85
            total += weight * mult

        if hits == 0:
            result = SentimentResult(0.0, "neutral", 0.25, self.backend)
        else:
            # tanh keeps long posts from saturating purely by token count
            score = math.tanh(total / math.sqrt(hits) / 1.6)
            label = "positive" if score > 0.15 else "negative" if score < -0.15 else "neutral"
            confidence = min(1.0, 0.35 + 0.12 * hits)
            result = SentimentResult(round(score, 4), label, round(confidence, 3), self.backend)

        if len(self._cache) < 20_000:
            self._cache[text] = result
        return result


class FinBERTScorer(SentimentScorer):
    """ProsusAI/finbert (or any compatible checkpoint) sentiment scorer."""

    backend = "finbert"

    def __init__(self, model_name: str = "ProsusAI/finbert", batch_size: int = 16,
                 max_length: int = 160, device: str | None = None) -> None:
        self.model_name = model_name
        self.batch_size = batch_size
        self.max_length = max_length
        self.device = device
        self._pipeline = None
        self._fallback = LexiconScorer()
        self._loaded = False
        self._load_failed = False

    def load(self) -> bool:
        """Load the model. Returns False (and enables fallback) on any failure."""
        if self._loaded:
            return True
        if self._load_failed:
            return False
        try:
            import torch
            from transformers import (
                AutoModelForSequenceClassification,
                AutoTokenizer,
                TextClassificationPipeline,
            )

            device = self.device or ("cuda" if torch.cuda.is_available() else "cpu")
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            model.eval()
            if device == "cpu":
                torch.set_num_threads(max(1, (torch.get_num_threads() or 2) // 2))
            self._pipeline = TextClassificationPipeline(
                model=model,
                tokenizer=tokenizer,
                device=0 if device == "cuda" else -1,
                top_k=None,
                truncation=True,
                max_length=self.max_length,
            )
            self._loaded = True
            log.info("FinBERT loaded (%s) on %s", self.model_name, device)
            return True
        except Exception as exc:
            self._load_failed = True
            log.warning("FinBERT unavailable (%s); using lexicon sentiment backend", exc)
            return False

    def score_sync(self, texts: list[str]) -> list[SentimentResult]:
        if not self.load() or self._pipeline is None:
            return self._fallback.score_sync(texts)
        cleaned = [normalize_text(t)[: self.max_length * 6] or "neutral" for t in texts]
        try:
            raw = self._pipeline(cleaned, batch_size=self.batch_size)
        except Exception as exc:
            log.warning("FinBERT inference failed (%s); falling back", exc)
            return self._fallback.score_sync(texts)

        results: list[SentimentResult] = []
        for entry in raw:
            scores = {d["label"].lower(): float(d["score"]) for d in entry}
            pos = scores.get("positive", 0.0)
            neg = scores.get("negative", 0.0)
            neu = scores.get("neutral", 0.0)
            # Signed expectation over the three classes.
            score = pos - neg
            label = max(scores, key=lambda k: scores[k])
            results.append(
                SentimentResult(round(score, 4), label, round(1.0 - neu, 3), self.backend)
            )
        return results


def build_scorer(use_finbert: bool = True, model_name: str = "ProsusAI/finbert",
                 batch_size: int = 16) -> SentimentScorer:
    """Return FinBERT when available and requested, else the lexicon scorer."""
    if use_finbert:
        scorer = FinBERTScorer(model_name=model_name, batch_size=batch_size)
        if scorer.load():
            return scorer
    return LexiconScorer()
