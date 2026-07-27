"""Self-contained demonstration run.

Boots the full pipeline against simulated feeds, injects a coordinated
manipulation episode across all three data sources simultaneously, and prints
the resulting alerts plus a latency/accuracy report. This is the fastest way to
verify an install end to end without any credentials.
"""

from __future__ import annotations

import asyncio
import contextlib

from .alerting.formatter import format_plain
from .app import Application
from .core.config import Settings
from .core.schema import AnomalySignal
from .core.telemetry import setup_logging


async def run_demo(duration_s: int = 90, seed: int = 42) -> int:
    """Run a scripted simulation. Returns a process exit code."""
    setup_logging("INFO")

    settings = Settings()
    settings.exchange.simulate = True
    settings.exchange.exchanges = ["binance", "bybit", "mexc"]
    settings.exchange.symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "PEPE/USDT"]
    settings.exchange.volume_bucket_s = 2
    settings.exchange.volume_window_s = 90
    settings.onchain.simulate = True
    settings.social.simulate = True
    settings.social.tracked_tickers = ["BTC", "ETH", "SOL", "PEPE"]
    settings.social.mention_window_s = 90
    settings.ml.score_interval_ms = 250
    settings.ml.retrain_interval_s = 10_000
    settings.alerts.dry_run = True
    settings.alerts.cooldown_s = 20
    settings.telemetry.health_interval_s = 30
    settings.telemetry.state_file = ""

    app = Application(settings)
    await app.setup()

    captured: list[AnomalySignal] = []

    original = app._on_signal

    async def capture(signal: AnomalySignal) -> None:
        captured.append(signal)
        await original(signal)

    app._on_signal = capture  # type: ignore[method-assign]
    if app.ml:
        app.ml.handlers.clear()
        app.ml.add_handler(capture)

    await app.start()

    print("\n" + "=" * 72)
    print("  CADB DEMO — simulated multi-source manipulation detection")
    print(f"  duration: {duration_s}s | alert threshold: {settings.ml.alert_threshold:.0f}/100")
    print("=" * 72 + "\n")

    target = "PEPE/USDT"
    ticker = "PEPE"

    async def script() -> None:
        # Phase 1: quiet baseline so the z-score windows warm up.
        await asyncio.sleep(min(20, duration_s * 0.25))
        print(f"\n>>> PHASE 1 complete: baseline established for {target}\n")

        # Phase 2: coordinated pump across all three data sources at once.
        print(f">>> PHASE 2: injecting coordinated manipulation on {ticker}")
        print("    · exchange: volume burst + spoofed bid wall + one-sided aggression")
        print("    · social:   bot-farm shill campaign")
        print("    · on-chain: whale deposits (simulated stream)\n")
        if app.exchange:
            for feed in app.exchange.feeds.values():
                if hasattr(feed, "inject_episode"):
                    feed.inject_episode(target, "pump", duration_s * 0.45)
        if app.social and app.social.sources:
            src = app.social.sources[0]
            if hasattr(src, "inject_campaign"):
                src.inject_campaign(ticker, duration_s * 0.45, "shill")

        await asyncio.sleep(duration_s * 0.5)
        print("\n>>> PHASE 3: episode ending, observing decay\n")

    task = asyncio.create_task(script())
    with contextlib.suppress(asyncio.CancelledError, TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.sleep(duration_s), timeout=duration_s + 5)
    task.cancel()

    health = app.health()
    await app.stop()

    # ---- report ----------------------------------------------------------
    print("\n" + "=" * 72)
    print("  DEMO REPORT")
    print("=" * 72)

    bus = health["bus"]
    print(f"\nBus: {bus.get('published', 0):,} events published, "
          f"{bus.get('dropped', 0):,} dropped")
    for m in health["modules"]:
        icon = "✅" if m.get("healthy") else "⚠️ "
        print(f"  {icon} {m['module']:<10} {m.get('events', 0):>7,} events")

    lat = health["metrics"]["latency"]
    print("\nLatency (ms):")
    for name in sorted(lat):
        s = lat[name]
        if s["count"]:
            flag = " ⚠️ OVER BUDGET" if s["p95"] > 200 else ""
            print(f"  {name:<22} p50={s['p50']:>6.2f} p95={s['p95']:>6.2f} "
                  f"p99={s['p99']:>6.2f} n={s['count']:,}{flag}")

    ml = health.get("ml", {})
    print(f"\nClassifier: {ml.get('classifier', {}).get('training_size', 0):,} training samples, "
          f"{ml.get('classifier', {}).get('scored', 0):,} vectors scored")
    print(f"Signals emitted: {ml.get('signals', 0)}  |  "
          f"Threshold breaches: {ml.get('alerts', 0)}")
    alerts_health = health.get("alerts", {})
    if alerts_health:
        dispatched = alerts_health.get("dispatched", 0)
        suppressed = alerts_health.get("suppressed", 0)
        print(f"Router: {dispatched} alert(s) delivered, {suppressed} suppressed "
              f"by cooldown/dedup")

    if ml.get("top_scores"):
        print("\nFinal scores:")
        for asset, score in ml["top_scores"]:
            bar = "█" * int(score / 5) + "░" * (20 - int(score / 5))
            print(f"  {asset:<8} {score:>5.1f} {bar}")

    if captured:
        print(f"\n{'=' * 72}\n  ALERTS FIRED ({len(captured)})\n{'=' * 72}")
        seen: set[str] = set()
        shown = 0
        for sig in captured:
            key = f"{sig.asset_pair}:{sig.severity.value}"
            if key in seen or shown >= 4:
                continue
            seen.add(key)
            shown += 1
            print("\n" + format_plain(sig))
        detected = {s.asset_pair for s in captured}
        print(f"\n{'=' * 72}")
        if ticker in detected:
            print(f"  ✅ SUCCESS — injected manipulation on {ticker} was detected")
        else:
            print(f"  ⚠️  {ticker} not flagged; detected: {', '.join(sorted(detected)) or 'none'}")
        print("=" * 72 + "\n")
    else:
        print("\n⚠️  No alerts fired — try a longer duration (--duration 120)\n")

    return 0
