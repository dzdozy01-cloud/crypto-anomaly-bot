"""Alerting: formatting and multi-sink routing."""

from .formatter import format_discord, format_plain, format_telegram
from .router import AlertRouter, AlertSink, ConsoleSink, DiscordSink, TelegramSink, WebhookSink

__all__ = [
    "AlertRouter",
    "AlertSink",
    "ConsoleSink",
    "DiscordSink",
    "TelegramSink",
    "WebhookSink",
    "format_discord",
    "format_plain",
    "format_telegram",
]
