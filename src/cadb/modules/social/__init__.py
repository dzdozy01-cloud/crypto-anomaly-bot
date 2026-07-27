"""Module 3 — NLP & Social Sentiment Monitor."""

from .botfarm import BotFarmDetector, BotFarmVerdict, SocialPost
from .monitor import SocialMonitor, TickerState
from .sentiment import FinBERTScorer, LexiconScorer, SentimentScorer, build_scorer
from .sources import SimulatedSocialSource, SocialSource, TelegramSource, XSource

__all__ = [
    "BotFarmDetector",
    "BotFarmVerdict",
    "FinBERTScorer",
    "LexiconScorer",
    "SentimentScorer",
    "SimulatedSocialSource",
    "SocialMonitor",
    "SocialPost",
    "SocialSource",
    "TelegramSource",
    "TickerState",
    "XSource",
    "build_scorer",
]
