"""Telegram command handlers.

Kept separate from :mod:`cadb.bot.telegram_bot` (which owns the transport) and
from :mod:`cadb.app` (which owns lifecycle), so the command surface can grow
without touching either. Every handler is a coroutine
``(args, chat_id) -> str`` returning Telegram-flavoured HTML.

Handlers read live state directly off the running modules — there is no cache to
go stale, and a command always reflects what the classifier is seeing right now.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..alerting.formatter import SEVERITY_EMOJI, format_telegram
from ..core.schema import now_ms
from ..core.telemetry import METRICS

if TYPE_CHECKING:
    from ..app import Application
    from .telegram_bot import TelegramBot

__all__ = ["register_commands"]


# --------------------------------------------------------------- formatting
def _esc(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _usd(value: float) -> str:
    """Compact USD rendering that never collapses a real amount to ``$0``.

    Micro-cap depth and CVD figures are legitimately fractional, so rounding to
    whole dollars below $1k would hide live data behind a misleading zero.
    """
    a = abs(value)
    sign = "-" if value < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.2f}M"
    if a >= 1e3:
        return f"{sign}${a / 1e3:.1f}K"
    if a >= 1:
        return f"{sign}${a:.0f}"
    if a >= 0.01:
        return f"{sign}${a:.2f}"
    if a > 0:
        return f"{sign}${a:.2g}"
    return "$0"


def _ago(timestamp_ms: int) -> str:
    delta = max(0.0, (now_ms() - timestamp_ms) / 1000.0)
    if delta < 60:
        return f"{delta:.0f}s ago"
    if delta < 3600:
        return f"{delta / 60:.0f}m ago"
    if delta < 86_400:
        return f"{delta / 3600:.1f}h ago"
    return f"{delta / 86_400:.1f}d ago"


def _bar(value: float, width: int = 10, scale: float = 100.0) -> str:
    filled = int(round(max(0.0, min(scale, value)) / scale * width))
    return "█" * filled + "░" * (width - filled)


def _severity_of(score: float) -> str:
    if score >= 90:
        return "critical"
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium"
    if score >= 40:
        return "low"
    return "info"


def _signed(value: float, digits: int = 2) -> str:
    return f"{value:+.{digits}f}"


def _uptime(seconds: float) -> str:
    d, rem = divmod(int(seconds), 86_400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


# ------------------------------------------------------------------ registry
def register_commands(bot: TelegramBot, app: Application) -> None:
    """Attach every command handler to ``bot``, bound to ``app`` state."""

    # ---------------------------------------------------------- /help
    async def help_cmd(args: list[str], chat_id: int) -> str:
        return (
            "<b>🛡 CADB — Crypto Anomaly Detection Bot</b>\n"
            "<i>Market-manipulation surveillance across exchange, chain and social.</i>\n\n"
            "<b>📊 Monitoring</b>\n"
            "/scores — risk board, all assets ranked\n"
            "/check &lt;ASSET&gt; — full manipulation report\n"
            "/explain &lt;ASSET&gt; — feature-by-feature breakdown\n"
            "/history [ASSET] — recent alerts fired\n\n"
            "<b>🔍 Per-module detail</b>\n"
            "/book &lt;PAIR&gt; — order book, OBI, CVD by venue\n"
            "/watchlist — pairs currently streamed\n"
            "/whales [ASSET] — recent large on-chain transfers\n"
            "/flows — net exchange inflow/outflow\n"
            "/social &lt;TICKER&gt; — sentiment, mentions, bot farms\n\n"
            "<b>⚙️ Control</b>\n"
            "/watch · /unwatch — subscribe this chat\n"
            "/threshold &lt;0-100&gt; — alert sensitivity\n"
            "/mute &lt;min&gt; · /unmute — silence alerts\n"
            "/pause · /resume — halt all alert delivery\n"
            "/test — send a sample alert (verify wiring)\n"
            "/whoami — chat id &amp; alert-routing diagnostics\n\n"
            "<b>🩺 Diagnostics</b>\n"
            "/status — module health\n"
            "/venues — feed connections\n"
            "/config — active thresholds\n"
            "/metrics — latency &amp; throughput\n\n"
            "<i>Score &gt; 80 fires an alert. Higher = more likely manipulated.</i>"
        )

    # -------------------------------------------------------- /status
    async def status_cmd(args: list[str], chat_id: int) -> str:
        h = app.health()
        lines = [
            "<b>🛡 System Status</b>",
            f"Uptime: <code>{_uptime(h['uptime_s'])}</code>",
            "",
            "<b>Modules</b>",
        ]
        for m in h["modules"]:
            icon = "🟢" if m.get("healthy") else "🔴"
            lines.append(
                f"{icon} <code>{m['module']:<9}</code> {m.get('events', 0):>7,} events"
            )

        bus = h["bus"]
        drop_pct = bus.get("dropped", 0) / max(bus.get("published", 1), 1) * 100
        lines += [
            "",
            "<b>Bus</b>",
            f"  {bus.get('published', 0):,} published · {bus.get('dropped', 0):,} dropped "
            f"({drop_pct:.2f}%)",
        ]

        clf = h.get("ml", {}).get("classifier", {})
        if clf:
            lines += [
                "",
                "<b>Classifier</b>",
                f"  {'trained' if clf.get('trained') else 'rules-only'} on "
                f"{clf.get('training_size', 0):,} samples",
                f"  {clf.get('scored', 0):,} vectors scored · "
                f"{h.get('ml', {}).get('alerts', 0)} threshold breaches",
            ]

        alerts = h.get("alerts", {})
        if alerts:
            lines += [
                "",
                "<b>Alerts</b>",
                f"  {alerts.get('dispatched', 0)} delivered · "
                f"{alerts.get('suppressed', 0)} deduped",
                f"  sinks: {', '.join(s['sink'] for s in alerts.get('sinks', [])) or 'none'}",
            ]
            if app.alerts_paused:
                lines.append("  ⏸ <b>delivery paused</b>")

        cycle = METRICS.snapshot()["latency"].get("ml.cycle_ms", {})
        if cycle.get("count"):
            budget = app.settings.telemetry.latency_budget_ms
            flag = " ⚠️" if cycle["p95"] > budget else " ✅"
            lines += [
                "",
                f"<i>Scoring p95 {cycle['p95']:.1f}ms / {budget:.0f}ms budget{flag}</i>",
            ]
        return "\n".join(lines)

    # -------------------------------------------------------- /scores
    async def scores_cmd(args: list[str], chat_id: int) -> str:
        if not app.ml:
            return "⚠️ ML scorer not enabled."
        top = app.ml.top_scores(20)
        if not top:
            return "⏳ No scores yet — the statistical baselines are still warming up."

        threshold = app.settings.ml.alert_threshold
        lines = ["<b>📈 Manipulation Risk Board</b>", ""]
        for asset, score in top:
            emoji = SEVERITY_EMOJI[_severity_of(score)]
            flag = " 🚨" if score >= threshold else ""
            lines.append(f"{emoji} <code>{asset:<7}{score:>5.1f}</code> {_bar(score)}{flag}")

        elevated = sum(1 for _, s in top if s >= threshold)
        lines += [
            "",
            f"<i>{elevated} above threshold ({threshold:.0f}) · "
            f"{len(top)} assets tracked</i>",
            "<i>Use /check &lt;ASSET&gt; for the full report.</i>",
        ]
        return "\n".join(lines)

    # --------------------------------------------------------- /check
    async def check_cmd(args: list[str], chat_id: int) -> str:
        if not args:
            return "Usage: <code>/check BTC</code>"
        if not app.ml:
            return "⚠️ ML scorer not enabled."
        asset = args[0].upper().split("/")[0]
        signal = app.ml.score_asset(asset)
        if signal is None:
            known = ", ".join(sorted(app.ml.store.assets)[:12]) or "none yet"
            return f"No data for <code>{_esc(asset)}</code>.\n\n<i>Tracking: {known}</i>"
        return format_telegram(signal)["text"]

    # ------------------------------------------------------- /explain
    async def explain_cmd(args: list[str], chat_id: int) -> str:
        """Feature-level transparency — why the score is what it is."""
        if not args:
            return "Usage: <code>/explain PEPE</code>"
        if not app.ml:
            return "⚠️ ML scorer not enabled."
        asset = args[0].upper().split("/")[0]
        fv = app.ml.store.build(asset)
        if fv is None:
            return f"No data for <code>{_esc(asset)}</code>."

        breakdown = app.ml.classifier.score(fv)
        emoji = SEVERITY_EMOJI[_severity_of(breakdown.composite)]
        lines = [
            f"{emoji} <b>{_esc(asset)} — Score Breakdown</b>",
            "",
            f"<b>Composite:</b> <code>{breakdown.composite:.1f}/100</code>",
            f"  Rules: <code>{breakdown.rule_component:5.1f}</code>  "
            f"ML: <code>{breakdown.ml_component:5.1f}</code>",
            "",
            "<b>Module contributions</b>",
        ]
        for module, value in sorted(breakdown.contributions.items(), key=lambda kv: -kv[1]):
            lines.append(f"  <code>{module:<9}{value:5.1f}</code> {_bar(value, 8)}")

        fresh = fv.sources_fresh
        lines += [
            "",
            "<b>Data freshness</b>",
            "  " + " ".join(
                f"{'🟢' if fresh.get(k) else '⚪'}{k}"
                for k in ("exchange", "onchain", "social")
            ),
            f"  coverage {fv.coverage:.0%}"
            + ("  <i>(score damped — thin data)</i>" if fv.coverage <= 0.34 else ""),
            "",
            "<b>Active features</b>",
        ]
        active = sorted(
            ((k, v) for k, v in fv.as_dict().items() if abs(v) > 0.05),
            key=lambda kv: -abs(kv[1]),
        )[:12]
        if active:
            lines.append("<code>" + "\n".join(
                f"{k:<20}{v:>+8.3f}" for k, v in active
            ) + "</code>")
        else:
            lines.append("  <i>all features neutral</i>")

        if breakdown.reasons:
            lines += ["", "<b>Evidence</b>"]
            lines += [f"  • {_esc(r)}" for r in breakdown.reasons[:7]]
        return "\n".join(lines)

    # ---------------------------------------------------------- /book
    async def book_cmd(args: list[str], chat_id: int) -> str:
        """Live microstructure per venue — the Module 1 view."""
        if not app.exchange:
            return "⚠️ Exchange engine not enabled."
        if not args:
            pairs = sorted({sym for _, sym in app.exchange.states})
            return (
                "Usage: <code>/book BTC/USDT</code>\n\n"
                f"<i>Tracked: {', '.join(pairs[:10]) or 'none yet'}</i>"
            )

        query = args[0].upper()
        if "/" not in query:
            query = f"{query}/USDT"
        matches = {v: st for (v, sym), st in app.exchange.states.items() if sym == query}
        if not matches:
            return f"No order-book data for <code>{_esc(query)}</code>."

        lines = [f"<b>📖 {_esc(query)} — Order Book</b>", ""]
        for venue, state in sorted(matches.items()):
            snap = state.snapshot()
            obi = snap["obi"]
            direction = "bid-heavy" if obi > 0.05 else "ask-heavy" if obi < -0.05 else "balanced"
            warn = " ⚠️" if abs(obi) >= app.settings.exchange.obi_threshold else ""
            lines += [
                f"<b>{venue}</b>{warn}",
                f"  price <code>{snap['price']:,.6g}</code>  "
                f"spread <code>{snap['spread_bps']:.2f}bps</code>",
                f"  OBI <code>{_signed(obi)}</code> ({direction})  "
                f"z=<code>{_signed(snap['obi_z'], 1)}</code>",
                f"  depth  bid {_usd(snap['bid_depth'])} / ask {_usd(snap['ask_depth'])}",
                f"  vol-z <code>{_signed(snap['volume_z'], 1)}</code>  "
                f"spike <code>{snap['volume_spike_ratio']:.1f}x</code>",
                f"  CVD <code>{_usd(snap['cvd'])}</code>  "
                f"buy-ratio <code>{snap['buy_ratio']:.0%}</code>",
            ]
            if abs(snap["cvd_divergence"]) > 0.25:
                kind = (
                    "absorption by passive wall" if snap["cvd_divergence"] > 0
                    else "markup without real buying"
                )
                lines.append(f"  ⚑ divergence {_signed(snap['cvd_divergence'])} — {kind}")
            lines.append("")

        lines.append("<i>OBI = (bid−ask)/(bid+ask) depth in quote notional.</i>")
        return "\n".join(lines)

    # -------------------------------------------------------- /whales
    async def whales_cmd(args: list[str], chat_id: int) -> str:
        """Recent large CEX transfers — the Module 2 view."""
        if not app.onchain:
            return "⚠️ On-chain tracker not enabled."
        whales = list(app.onchain.recent_whales)
        if args:
            wanted = args[0].upper()
            whales = [w for w in whales if w.symbol == wanted]
        if not whales:
            scope = f" for <code>{_esc(args[0].upper())}</code>" if args else ""
            return (
                f"No whale transfers recorded{scope} yet.\n\n"
                f"<i>Threshold: {_usd(app.settings.onchain.whale_threshold_usd)}</i>"
            )

        recent = whales[-12:][::-1]
        lines = [
            f"<b>🐋 Whale Transfers</b>  <i>(&gt;{_usd(app.settings.onchain.whale_threshold_usd)})</i>",
            "",
        ]
        for w in recent:
            arrow = "📥" if w.direction == "inflow" else "📤"
            label = "→ CEX" if w.direction == "inflow" else "← CEX"
            lines.append(
                f"{arrow} <b>{_usd(w.usd_value)}</b> {_esc(w.symbol)} {label} "
                f"<i>{_esc(w.counterparty or '?')}</i>"
            )
            lines.append(f"    <code>{w.chain}</code> · {_ago(w.timestamp)}")

        inflow = sum(w.usd_value for w in whales if w.direction == "inflow")
        outflow = sum(w.usd_value for w in whales if w.direction == "outflow")
        net = inflow - outflow
        pressure = (
            "sell pressure building" if net > 0 else "accumulation / supply squeeze"
        )
        lines += [
            "",
            f"<b>Session net:</b> {_usd(net)} — <i>{pressure}</i>",
            f"<i>in {_usd(inflow)} · out {_usd(outflow)} · {len(whales)} transfers</i>",
        ]
        return "\n".join(lines)

    # --------------------------------------------------------- /flows
    async def flows_cmd(args: list[str], chat_id: int) -> str:
        if not app.onchain:
            return "⚠️ On-chain tracker not enabled."
        flows = app.onchain.net_flow_usd
        if not flows:
            return "No exchange flows recorded yet."

        ranked = sorted(flows.items(), key=lambda kv: -abs(kv[1]))[:15]
        lines = ["<b>💱 Net Exchange Flows</b>", ""]
        for key, value in ranked:
            chain, _, symbol = key.partition(":")
            icon = "📥" if value > 0 else "📤"
            lines.append(
                f"{icon} <code>{symbol:<6}</code> {_usd(value):>10}  "
                f"<i>{chain}</i>"
            )
        lines += [
            "",
            "<i>📥 positive = net deposits to exchanges (sell pressure)</i>",
            "<i>📤 negative = net withdrawals (accumulation)</i>",
        ]

        bridges = list(app.onchain.recent_bridge)[-4:][::-1]
        if bridges:
            lines += ["", "<b>Recent bridge flows</b>"]
            for b in bridges:
                lines.append(
                    f"  🌉 {_usd(b.usd_value)} {_esc(b.token)} "
                    f"<i>{_esc(b.bridge)}</i> · {_ago(b.timestamp)}"
                )
        return "\n".join(lines)

    # -------------------------------------------------------- /social
    async def social_cmd(args: list[str], chat_id: int) -> str:
        """Sentiment, mention velocity and bot-farm verdict — the Module 3 view."""
        if not app.social:
            return "⚠️ Social monitor not enabled."
        if not args:
            tracked = ", ".join(sorted(app.social.states)) or "none yet"
            return f"Usage: <code>/social PEPE</code>\n\n<i>Tracking: {tracked}</i>"

        ticker = args[0].upper()
        state = app.social.states.get(ticker)
        if state is None:
            return f"No social data for <code>{_esc(ticker)}</code>."

        snap = state.snapshot()
        sentiment = snap["sentiment"]
        mood = (
            "🟢 bullish" if sentiment > 0.15
            else "🔴 bearish" if sentiment < -0.15
            else "⚪ neutral"
        )
        lines = [
            f"<b>💬 {_esc(ticker)} — Social Signal</b>",
            "",
            f"<b>Sentiment:</b> <code>{_signed(sentiment)}</code> {mood}",
            f"<b>Mentions:</b> <code>{snap['mention_rate']:.1f}/min</code>  "
            f"z=<code>{_signed(snap['mention_z'], 1)}</code>",
            f"<b>Acceleration:</b> <code>{_signed(snap['acceleration'], 1)}</code>/min²",
            f"<b>Posts seen:</b> <code>{int(snap['posts']):,}</code>",
        ]
        if snap["mention_z"] > app.settings.social.mention_z_threshold:
            lines.append("  ⚠️ <b>mention spike above threshold</b>")

        verdict = state.botfarm.evaluate(mention_z=state.mention_z.last_z)
        lines += ["", "<b>🤖 Bot-farm analysis</b>"]
        if verdict.posts_considered < app.settings.social.bot_farm_min_posts:
            lines.append(
                f"  <i>insufficient sample ({verdict.posts_considered} posts, "
                f"need {app.settings.social.bot_farm_min_posts})</i>"
            )
        else:
            icon = "🚨" if verdict.is_bot_farm else "✅"
            lines += [
                f"  {icon} confidence <code>{verdict.score:.0%}</code> {_bar(verdict.score * 100, 8)}",
                f"  <code>{verdict.unique_authors}</code> authors / "
                f"<code>{verdict.posts_considered}</code> posts",
            ]
            if verdict.age_variance_cv is not None:
                lines.append(f"  age CV <code>{verdict.age_variance_cv:.2f}</code> "
                             f"· duplication <code>{verdict.duplicate_ratio:.0%}</code>")
            for reason in verdict.reasons[:4]:
                lines.append(f"    • {_esc(reason)}")

        backend = app.social.scorer.backend if app.social.scorer else "?"
        lines += ["", f"<i>Sentiment backend: {backend}</i>"]
        return "\n".join(lines)

    # -------------------------------------------------------- /venues
    async def venues_cmd(args: list[str], chat_id: int) -> str:
        lines = ["<b>🔌 Feed Connections</b>", ""]
        if app.exchange:
            lines.append("<b>Exchanges</b>")
            for venue, feed in sorted(app.exchange.feeds.items()):
                icon = "🟢" if feed.connected else "🔴"
                lines.append(
                    f"{icon} <code>{venue:<9}</code> {feed.messages:,} msgs "
                    f"<i>({type(feed).__name__})</i>"
                )
            unhealthy = [t.health() for t in app.exchange._tasks if not t.healthy]
            if unhealthy:
                lines.append("")
                lines.append("<b>Degraded streams</b>")
                for t in unhealthy[:5]:
                    lines.append(
                        f"  🔴 <code>{_esc(t['name'])}</code> "
                        f"restarts={t['restarts']} circuit={t['circuit']}"
                    )

        if app.onchain:
            health = app.onchain.health().get("chains", {})
            if health:
                lines += ["", "<b>Chains</b>"]
                for chain, info in health.items():
                    for ep in info.get("endpoints", []):
                        icon = "🟢" if ep["circuit"] == "closed" else "🔴"
                        lines.append(
                            f"{icon} <code>{chain:<9}</code> {_esc(ep['url'])} "
                            f"<i>{ep['latency_ms']:.0f}ms</i>"
                        )

        if app.social:
            lines += ["", "<b>Social</b>"]
            for src in app.social.sources:
                icon = "🟢" if src.enabled else "⚪"
                lines.append(
                    f"{icon} <code>{src.platform:<9}</code> {src.posts_seen:,} posts"
                )
        return "\n".join(lines)

    # ------------------------------------------------------- /history
    async def history_cmd(args: list[str], chat_id: int) -> str:
        history = list(app.alert_history)
        if args:
            wanted = args[0].upper()
            history = [s for s in history if s.asset_pair == wanted]
        if not history:
            return "No alerts fired yet. 🟢"

        lines = ["<b>🗂 Recent Alerts</b>", ""]
        for sig in history[-12:][::-1]:
            emoji = SEVERITY_EMOJI[sig.severity.value]
            lines.append(
                f"{emoji} <code>{sig.asset_pair:<7}{sig.score:5.1f}</code> "
                f"<i>{_ago(sig.timestamp)}</i>"
            )
            if sig.reasons:
                lines.append(f"    {_esc(sig.reasons[0][:70])}")
        lines += ["", f"<i>{len(app.alert_history)} alerts in buffer</i>"]
        return "\n".join(lines)

    # -------------------------------------------------------- /config
    async def config_cmd(args: list[str], chat_id: int) -> str:
        s = app.settings
        return "\n".join([
            "<b>⚙️ Active Configuration</b>",
            "",
            "<b>Detection thresholds</b>",
            f"  alert score      <code>{s.ml.alert_threshold:.0f}</code>",
            f"  volume z-score   <code>{s.exchange.volume_z_threshold:.1f}σ</code>",
            f"  order book OBI   <code>{s.exchange.obi_threshold:.2f}</code>",
            f"  whale transfer   <code>{_usd(s.onchain.whale_threshold_usd)}</code>",
            f"  LP drop          <code>{s.onchain.liquidity_drop_pct:.0f}%</code>",
            f"  bridge transfer  <code>{_usd(s.onchain.bridge_threshold_usd)}</code>",
            f"  mention z-score  <code>{s.social.mention_z_threshold:.1f}σ</code>",
            f"  bot age CV       <code>{s.social.bot_age_variance_threshold:.2f}</code>",
            "",
            "<b>Scoring</b>",
            f"  ML blend         <code>{s.ml.ml_blend:.2f}</code> "
            f"<i>(0=rules, 1=forest)</i>",
            "  weights          " + " ".join(
                f"<code>{k}={v:.2f}</code>" for k, v in s.ml.weights.items()
            ),
            f"  cadence          <code>{s.ml.score_interval_ms}ms</code>",
            "",
            "<b>Alerting</b>",
            f"  cooldown         <code>{s.alerts.cooldown_s}s</code>",
            f"  rate cap         <code>{s.alerts.max_alerts_per_min}/min</code>",
            f"  dry run          <code>{s.alerts.dry_run}</code>",
            "",
            "<b>Coverage</b>",
            f"  venues    {', '.join(s.exchange.exchanges)}",
            f"  symbols   {len(s.exchange.symbols)} pairs",
            f"  chains    {', '.join(s.onchain.evm_rpc)} + solana",
            f"  tickers   {', '.join(s.social.tracked_tickers[:8])}",
            "",
            "<i>Change sensitivity live with /threshold &lt;0-100&gt;</i>",
        ])

    # ----------------------------------------------------- /threshold
    async def threshold_cmd(args: list[str], chat_id: int) -> str:
        if not args:
            return (
                f"Alert threshold: <code>{app.settings.ml.alert_threshold:.0f}</code>\n\n"
                "<i>Lower = more sensitive. Usage:</i> <code>/threshold 75</code>"
            )
        try:
            value = float(args[0])
        except ValueError:
            return "Usage: <code>/threshold 80</code>"
        value = max(0.0, min(100.0, value))
        previous = app.settings.ml.alert_threshold
        app.settings.ml.alert_threshold = value
        if app.ml:
            app.ml.config.alert_threshold = value
        if app.router:
            app.router.config.min_score = value
        note = (
            "\n<i>⚠️ Below 60 expect false positives from ordinary news.</i>"
            if value < 60 else ""
        )
        return (
            f"✅ Threshold <code>{previous:.0f}</code> → <code>{value:.0f}</code>{note}"
        )

    # ------------------------------------------------- pause / resume
    async def pause_cmd(args: list[str], chat_id: int) -> str:
        app.alerts_paused = True
        return "⏸ Alert delivery paused. Detection continues — /resume to re-enable."

    async def resume_cmd(args: list[str], chat_id: int) -> str:
        app.alerts_paused = False
        return "▶️ Alert delivery resumed."

    async def unmute_cmd(args: list[str], chat_id: int) -> str:
        bot.muted_until = 0.0
        return "🔔 Unmuted."

    # ----------------------------------------------------- /watchlist
    async def watchlist_cmd(args: list[str], chat_id: int) -> str:
        """Show which pairs are streamed, and which were auto-discovered."""
        if not app.exchange:
            return "⚠️ Exchange engine not enabled."
        pinned = set(app.settings.exchange.symbols)
        lines = ["<b>📡 Watchlist</b>", ""]
        total = 0
        for venue in sorted(app.exchange.watched):
            syms = sorted(app.exchange.watched[venue])
            total += len(syms)
            lines.append(f"<b>{venue}</b> — {len(syms)} pair(s)")
            for sym in syms[:14]:
                tag = "📌" if sym in pinned else "🔎"
                st = app.exchange.states.get((venue, sym))
                extra = ""
                if st:
                    snap = st.snapshot()
                    if snap["volume_z"]:
                        extra = f"  z={snap['volume_z']:+.1f}"
                lines.append(f"  {tag} <code>{sym}</code>{extra}")
            if len(syms) > 14:
                lines.append(f"  <i>… and {len(syms) - 14} more</i>")
            lines.append("")

        if app.exchange.discovery:
            lines.append("<b>Discovery</b>")
            for venue, d in app.exchange.discovery.items():
                st = d.stats()
                ago = st["last_scan_s_ago"]
                lines.append(
                    f"  {venue}: {st['universe']:,} pairs scanned"
                    + (f", last {ago:.0f}s ago" if ago else ", pending")
                )
        else:
            lines.append(
                "<i>Discovery disabled — only pinned symbols are watched. "
                "Set exchange.discovery_enabled: true to auto-track movers.</i>"
            )
        lines += ["", f"<i>📌 pinned · 🔎 auto-discovered · {total} streams</i>"]
        return "\n".join(lines)

    # -------------------------------------------------------- /whoami
    async def whoami_cmd(args: list[str], chat_id: int) -> str:
        """Report this chat's id and whether alerts will actually reach it.

        Exists because the single most common misconfiguration is a wrong
        TELEGRAM_CHAT_ID: alerts fail with "chat not found" and the cause is
        invisible from inside Telegram.
        """
        configured = str(app.settings.alerts.telegram_chat_id or "")
        here = str(chat_id)
        subscribed = here in bot.subscribers
        lines = [
            "<b>🪪 Chat Diagnostics</b>",
            "",
            f"This chat id:   <code>{here}</code>",
            f"Configured id:  <code>{configured or '(unset)'}</code>",
            f"Subscribed:     {'✅ yes' if subscribed else '❌ no — send /watch'}",
        ]
        if bot.allowed_chats:
            ok = here in bot.allowed_chats
            lines.append(f"Authorised:     {'✅ yes' if ok else '❌ no'}")
        else:
            lines.append("Authorised:     ✅ open (no allow-list configured)")

        if configured and configured != here:
            lines += [
                "",
                "⚠️ <b>Alerts are being sent to a different chat.</b>",
                f"To receive them here, set <code>TELEGRAM_CHAT_ID={here}</code> "
                "in <code>.env</code> and restart.",
            ]
        elif not configured:
            lines += [
                "",
                f"⚠️ No chat configured. Set <code>TELEGRAM_CHAT_ID={here}</code> "
                "in <code>.env</code> and restart, or just send /watch to "
                "subscribe this chat for the current session.",
            ]
        else:
            lines += ["", "✅ Alert delivery is correctly targeted at this chat."]
        return "\n".join(lines)

    # ---------------------------------------------------------- /test
    async def test_cmd(args: list[str], chat_id: int) -> str:
        """Fire a synthetic alert so users can verify delivery end to end."""
        from ..core.schema import AnomalySignal, Severity

        sample = AnomalySignal(
            asset_pair="TEST", venue="diagnostic", score=87.5, severity=Severity.HIGH,
            ml_score=84.0, rule_score=90.0,
            contributions={"exchange": 78.0, "onchain": 55.0, "social": 88.0},
            features={"volume_z": 4.8, "bot_farm_score": 0.81, "obi": 0.58},
            reasons=[
                "this is a test alert — no real manipulation detected",
                "volume 4.8σ above baseline",
                "bot-farm pattern (confidence 81%)",
            ],
            latency_ms=12.0,
        )
        delivered = []
        if app.router:
            for sink in app.router.sinks:
                ok = await sink.send(sample)
                delivered.append(f"{'✅' if ok else '❌'} {sink.name}")
        return (
            "<b>🧪 Test alert dispatched</b>\n\n"
            + ("\n".join(delivered) if delivered else "no sinks configured")
            + "\n\n<i>Bypasses cooldown and threshold checks.</i>"
        )

    # ------------------------------------------------------------ wire
    bot.register("help", help_cmd, "Show all commands")
    bot.register("start", help_cmd, "Show all commands")
    bot.register("status", status_cmd, "System health and module state")
    bot.register("scores", scores_cmd, "Manipulation risk board")
    bot.register("check", check_cmd, "Full report for an asset")
    bot.register("explain", explain_cmd, "Feature-by-feature score breakdown")
    bot.register("book", book_cmd, "Order book, OBI and CVD by venue")
    bot.register("whales", whales_cmd, "Recent large on-chain transfers")
    bot.register("flows", flows_cmd, "Net exchange inflow/outflow")
    bot.register("social", social_cmd, "Sentiment, mentions and bot farms")
    bot.register("venues", venues_cmd, "Feed connection health")
    bot.register("history", history_cmd, "Recent alerts fired")
    bot.register("config", config_cmd, "Active thresholds and coverage")
    bot.register("threshold", threshold_cmd, "Set alert sensitivity")
    bot.register("pause", pause_cmd, "Pause alert delivery")
    bot.register("resume", resume_cmd, "Resume alert delivery")
    bot.register("unmute", unmute_cmd, "Cancel an active mute")
    bot.register("watchlist", watchlist_cmd, "Pairs currently streamed")
    bot.register("whoami", whoami_cmd, "Show this chat id and alert routing")
    bot.register("test", test_cmd, "Send a test alert")
