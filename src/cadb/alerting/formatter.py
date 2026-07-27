"""Alert rendering for Telegram (HTML), Discord (embeds) and plain text."""

from __future__ import annotations

import time
from typing import Any

from ..core.schema import AnomalySignal

__all__ = ["format_telegram", "format_discord", "format_plain", "SEVERITY_EMOJI"]

SEVERITY_EMOJI: dict[str, str] = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "low": "🔵",
    "info": "⚪",
}

SEVERITY_COLOR: dict[str, int] = {
    "critical": 0xC0392B,
    "high": 0xE67E22,
    "medium": 0xF1C40F,
    "low": 0x3498DB,
    "info": 0x95A5A6,
}

_MODULE_LABEL = {
    "exchange": "Order Flow",
    "onchain": "On-Chain",
    "social": "Social",
}


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _bar(value: float, width: int = 10) -> str:
    """Unicode progress bar for a 0-100 value."""
    filled = int(round(max(0.0, min(100.0, value)) / 100 * width))
    return "█" * filled + "░" * (width - filled)


def _timestamp(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ms / 1000))


def format_telegram(signal: AnomalySignal) -> dict[str, Any]:
    """Telegram sendMessage payload (HTML parse mode)."""
    emoji = SEVERITY_EMOJI.get(signal.severity.value, "⚪")
    lines = [
        f"{emoji} <b>MANIPULATION ALERT — {_escape_html(signal.asset_pair)}</b>",
        "",
        f"<b>Score:</b> <code>{signal.score:.1f}/100</code>  {_bar(signal.score)}",
        f"<b>Severity:</b> {signal.severity.value.upper()}",
        f"<b>Venue:</b> {_escape_html(signal.venue)}",
        "",
        "<b>Signal breakdown</b>",
    ]
    for module, value in sorted(signal.contributions.items(), key=lambda kv: -kv[1]):
        label = _MODULE_LABEL.get(module, module.title())
        lines.append(f"  <code>{label:<11}</code> {_bar(value, 8)} {value:5.1f}")

    if signal.reasons:
        lines += ["", "<b>Evidence</b>"]
        lines += [f"  • {_escape_html(r)}" for r in signal.reasons[:6]]

    if signal.features:
        top = sorted(signal.features.items(), key=lambda kv: -abs(kv[1]))[:5]
        lines += ["", "<b>Key features</b>"]
        lines.append(
            "  <code>"
            + " | ".join(f"{k}={v:+.2f}" for k, v in top)
            + "</code>"
        )

    lines += [
        "",
        f"<i>ML {signal.ml_score:.0f} · Rules {signal.rule_score:.0f} · "
        f"{signal.latency_ms:.0f}ms</i>",
        f"<i>{_timestamp(signal.timestamp)}</i>",
    ]
    return {
        "text": "\n".join(lines),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }


def format_discord(signal: AnomalySignal) -> dict[str, Any]:
    """Discord webhook payload with a rich embed."""
    emoji = SEVERITY_EMOJI.get(signal.severity.value, "⚪")
    fields = [
        {
            "name": "Composite Score",
            "value": f"**{signal.score:.1f}** / 100\n`{_bar(signal.score)}`",
            "inline": True,
        },
        {"name": "Severity", "value": signal.severity.value.upper(), "inline": True},
        {"name": "Venue", "value": signal.venue, "inline": True},
    ]
    if signal.contributions:
        fields.append(
            {
                "name": "Module Contributions",
                "value": "\n".join(
                    f"`{_MODULE_LABEL.get(k, k):<11}` {_bar(v, 8)} {v:5.1f}"
                    for k, v in sorted(signal.contributions.items(), key=lambda kv: -kv[1])
                ),
                "inline": False,
            }
        )
    if signal.reasons:
        fields.append(
            {
                "name": "Evidence",
                "value": "\n".join(f"• {r}" for r in signal.reasons[:6])[:1024],
                "inline": False,
            }
        )
    if signal.features:
        top = sorted(signal.features.items(), key=lambda kv: -abs(kv[1]))[:6]
        fields.append(
            {
                "name": "Key Features",
                "value": "```" + "\n".join(f"{k:<20} {v:+.3f}" for k, v in top) + "```",
                "inline": False,
            }
        )
    return {
        "username": "CADB Surveillance",
        "embeds": [
            {
                "title": f"{emoji} Manipulation Alert — {signal.asset_pair}",
                "color": SEVERITY_COLOR.get(signal.severity.value, 0x95A5A6),
                "fields": fields,
                "footer": {
                    "text": f"ML {signal.ml_score:.0f} · Rules {signal.rule_score:.0f} · "
                            f"{signal.latency_ms:.0f}ms · id {signal.signal_id}"
                },
                "timestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(signal.timestamp / 1000)
                ),
            }
        ],
    }


def format_plain(signal: AnomalySignal) -> str:
    """Console / log / generic-webhook representation."""
    emoji = SEVERITY_EMOJI.get(signal.severity.value, "⚪")
    parts = [
        f"{emoji} MANIPULATION ALERT — {signal.asset_pair} @ {signal.venue}",
        f"   Score {signal.score:.1f}/100 [{_bar(signal.score)}] {signal.severity.value.upper()}",
        f"   ML {signal.ml_score:.0f} | Rules {signal.rule_score:.0f} | "
        f"{signal.latency_ms:.1f}ms | {_timestamp(signal.timestamp)}",
    ]
    if signal.contributions:
        parts.append(
            "   Modules: "
            + ", ".join(f"{k}={v:.0f}" for k, v in sorted(signal.contributions.items(), key=lambda kv: -kv[1]))
        )
    for reason in signal.reasons[:6]:
        parts.append(f"     • {reason}")
    return "\n".join(parts)
