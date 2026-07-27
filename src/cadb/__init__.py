"""CADB — Crypto Anomaly Detection Bot.

A modular, async market-manipulation surveillance system unifying four
intelligence sources behind a shared ingestion bus:

1. Exchange Anomaly Engine   — L2 order books, volume z-scores, OBI, CVD
2. On-Chain Whale Tracker    — EVM + Solana CEX flows, LP drains, bridges
3. Social Sentiment Monitor  — X/Telegram, FinBERT, bot-farm detection
4. ML Manipulation Classifier — Isolation Forest + dynamic z-score rules

Quick start::

    from cadb import Application, load_settings
    app = Application(load_settings("config.yaml"))
    await app.run_forever()
"""

from .app import Application
from .core.config import Settings, load_settings
from .core.schema import AnomalySignal, MarketEvent, MetricType, Severity, SourceType

__version__ = "1.0.0"

__all__ = [
    "Application",
    "AnomalySignal",
    "MarketEvent",
    "MetricType",
    "Settings",
    "Severity",
    "SourceType",
    "__version__",
    "load_settings",
]
