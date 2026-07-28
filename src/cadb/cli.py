"""Command-line entry point.

    cadb run       --config config.yaml     start the full system
    cadb demo      --duration 90            self-contained simulated run
    cadb train     --samples 20000          (re)train the Isolation Forest
    cadb evaluate                           precision/recall on a labelled set
    cadb backtest  --events events.jsonl    replay recorded telemetry
    cadb validate  --config config.yaml     check configuration
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
from pathlib import Path

from .core.config import Settings, load_settings
from .core.telemetry import setup_logging


def _cmd_run(args: argparse.Namespace) -> int:
    from .app import Application

    settings = load_settings(args.config)
    if args.simulate:
        settings.exchange.simulate = True
        settings.onchain.simulate = True
        settings.social.simulate = True
    if args.dry_run:
        settings.alerts.dry_run = True
    if args.threshold is not None:
        settings.ml.alert_threshold = args.threshold
        settings.alerts.min_score = args.threshold
    if args.log_level:
        settings.telemetry.log_level = args.log_level

    app = Application(settings)
    try:
        asyncio.run(app.run_forever())
    except KeyboardInterrupt:
        print("\ninterrupted")
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    return asyncio.run(run_demo(duration_s=args.duration, seed=args.seed))


def _cmd_train(args: argparse.Namespace) -> int:
    from .modules.ml.classifier import ManipulationClassifier
    from .modules.ml.training import generate_training_data

    setup_logging("INFO")
    settings = load_settings(args.config) if args.config else Settings()
    clf = ManipulationClassifier(
        contamination=settings.ml.contamination,
        n_estimators=settings.ml.n_estimators,
        random_state=settings.ml.random_state,
    )

    if args.data:
        path = Path(args.data)
        if not path.exists():
            print(f"error: {path} not found", file=sys.stderr)
            return 1
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        data = [r["values"] if isinstance(r, dict) else r for r in rows]
        print(f"loaded {len(data)} samples from {path}")
    else:
        data = generate_training_data(
            n_samples=args.samples, anomaly_rate=args.anomaly_rate, seed=args.seed
        ).tolist()
        print(f"generated {len(data)} synthetic samples")

    if not clf.fit(data):
        print("training failed", file=sys.stderr)
        return 1
    out = args.output or settings.ml.model_path
    clf.save(out)
    print(f"✅ model trained on {clf.training_size} samples -> {out}")
    return 0


def _cmd_evaluate(args: argparse.Namespace) -> int:
    import numpy as np

    from .modules.ml.classifier import ManipulationClassifier
    from .modules.ml.features import FeatureVector
    from .modules.ml.training import generate_labelled_set, generate_training_data

    setup_logging("WARNING")
    clf = ManipulationClassifier()
    if args.model and Path(args.model).exists():
        clf.load(args.model)
        print(f"loaded model: {args.model}")
    else:
        clf.fit(generate_training_data(args.train_samples, 0.02, 42))
        print(f"trained fresh model on {args.train_samples} synthetic samples")

    X, y, names = generate_labelled_set(args.normal, args.per_scenario, seed=args.seed)
    scores = np.array(
        [
            clf.score(
                FeatureVector(
                    asset="EVAL", timestamp=0, values=list(row), coverage=1.0,
                    sources_fresh={"exchange": True, "onchain": True, "social": True},
                )
            ).composite
            for row in X
        ]
    )

    print(f"\nEvaluated {len(X)} samples ({int(y.sum())} anomalies)\n")
    print(f"{'threshold':>10} {'precision':>10} {'recall':>8} {'f1':>7} {'TP':>5} {'FP':>4} {'FN':>5}")
    print("-" * 56)
    for thr in (40, 50, 60, 70, 80, 90):
        pred = scores >= thr
        tp = int((pred & (y == 1)).sum())
        fp = int((pred & (y == 0)).sum())
        fn = int((~pred & (y == 1)).sum())
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        mark = "  <- alert threshold" if thr == 80 else ""
        print(f"{thr:>10} {prec:>10.3f} {rec:>8.3f} {f1:>7.3f} {tp:>5} {fp:>4} {fn:>5}{mark}")

    print(f"\n{'scenario':<22} {'mean':>7} {'p10':>7} {'p90':>7} {'>=80':>6}")
    print("-" * 52)
    from collections import defaultdict

    grouped: dict[str, list[float]] = defaultdict(list)
    for score, name in zip(scores, names):
        grouped[name].append(score)
    for name in sorted(grouped):
        arr = np.array(grouped[name])
        print(
            f"{name:<22} {arr.mean():>7.1f} {np.percentile(arr, 10):>7.1f} "
            f"{np.percentile(arr, 90):>7.1f} {(arr >= 80).mean():>5.0%}"
        )
    return 0


def _cmd_backtest(args: argparse.Namespace) -> int:
    from .backtest import run_backtest

    return asyncio.run(run_backtest(args.events, threshold=args.threshold, speed=args.speed))


def _cmd_validate(args: argparse.Namespace) -> int:
    try:
        settings = load_settings(args.config)
    except Exception as exc:
        print(f"❌ invalid configuration: {exc}", file=sys.stderr)
        return 1
    print("✅ configuration valid\n")
    print(f"  bus:        {settings.bus.kind}")
    print(f"  exchanges:  {', '.join(settings.exchange.exchanges)} "
          f"({len(settings.exchange.symbols)} symbols)")
    print(f"  tickers:    {', '.join(settings.social.tracked_tickers[:8])}")
    print(f"  threshold:  {settings.ml.alert_threshold:.0f}")

    # Resolve RPC endpoints explicitly. An empty endpoint silently disables a
    # chain, so surface exactly what each one resolved to rather than just
    # listing the chain names.
    print("\n  on-chain endpoints:")
    inert = True
    for chain, url in settings.onchain.evm_rpc.items():
        endpoints = [u.strip() for u in (url or "").split(",") if u.strip()]
        if endpoints:
            inert = False
            host = endpoints[0].split("/")[2] if "//" in endpoints[0] else endpoints[0]
            keyed = len([p for p in endpoints[0].split("/")[3:] if p]) > 0
            tag = "keyed" if keyed else "public — rate limited"
            extra = f" (+{len(endpoints) - 1} failover)" if len(endpoints) > 1 else ""
            print(f"    ✅ {chain:<9} {host} [{tag}]{extra}")
        else:
            print(f"    ❌ {chain:<9} NOT CONFIGURED — this chain will not be monitored")
    sol = [u.strip() for u in (settings.onchain.solana_rpc or "").split(",") if u.strip()]
    if sol:
        inert = False
        host = sol[0].split("/")[2] if "//" in sol[0] else sol[0]
        keyed = len([p for p in sol[0].split("/")[3:] if p]) > 0
        tag = "keyed" if keyed else "public — rate limited"
        extra = f" (+{len(sol) - 1} failover)" if len(sol) > 1 else ""
        print(f"    ✅ {'solana':<9} {host} [{tag}]{extra}")
    else:
        print(f"    ❌ {'solana':<9} NOT CONFIGURED — SPL tracking disabled")
    if inert and settings.onchain.enabled and not settings.onchain.simulate:
        print("\n  ⚠️  on-chain module has NO endpoints and will do nothing.")
        print("     Comment out the *_RPC_URL lines in .env to use the defaults.")
    sinks = []
    if settings.alerts.telegram_bot_token and settings.alerts.telegram_chat_id:
        sinks.append("telegram")
    if settings.alerts.discord_webhook_url:
        sinks.append("discord")
    sinks += [f"webhook({i})" for i, _ in enumerate(settings.alerts.generic_webhooks)]
    print(f"  sinks:      {', '.join(sinks) or 'none (console fallback)'}")
    missing = []
    if not settings.alerts.telegram_bot_token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not settings.social.x_bearer_token:
        missing.append("X_BEARER_TOKEN")
    if missing:
        print(f"\n  ⚠️  unset credentials: {', '.join(missing)}")
        print("     (affected sources degrade to simulation / are skipped)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cadb",
        description="Crypto Anomaly Detection Bot — unified market manipulation surveillance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="start the full system")
    run.add_argument("-c", "--config", default="config.yaml")
    run.add_argument("--simulate", action="store_true", help="use synthetic feeds")
    run.add_argument("--dry-run", action="store_true", help="log alerts instead of sending")
    run.add_argument("--threshold", type=float, help="override alert threshold")
    run.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    run.set_defaults(func=_cmd_run)

    demo = sub.add_parser("demo", help="self-contained simulated run")
    demo.add_argument("-d", "--duration", type=int, default=90)
    demo.add_argument("--seed", type=int, default=42)
    demo.set_defaults(func=_cmd_demo)

    train = sub.add_parser("train", help="train the Isolation Forest")
    train.add_argument("-c", "--config")
    train.add_argument("-o", "--output")
    train.add_argument("--data", help="JSONL of recorded feature vectors")
    train.add_argument("--samples", type=int, default=10_000)
    train.add_argument("--anomaly-rate", type=float, default=0.02)
    train.add_argument("--seed", type=int, default=42)
    train.set_defaults(func=_cmd_train)

    ev = sub.add_parser("evaluate", help="precision/recall on labelled synthetic data")
    ev.add_argument("-m", "--model")
    ev.add_argument("--train-samples", type=int, default=6000)
    ev.add_argument("--normal", type=int, default=1200)
    ev.add_argument("--per-scenario", type=int, default=150)
    ev.add_argument("--seed", type=int, default=7)
    ev.set_defaults(func=_cmd_evaluate)

    bt = sub.add_parser("backtest", help="replay recorded telemetry")
    bt.add_argument("events", help="JSONL file of MarketEvent payloads")
    bt.add_argument("--threshold", type=float, default=80.0)
    bt.add_argument("--speed", type=float, default=0.0, help="0 = as fast as possible")
    bt.set_defaults(func=_cmd_backtest)

    val = sub.add_parser("validate", help="validate configuration")
    val.add_argument("-c", "--config", default="config.yaml")
    val.set_defaults(func=_cmd_validate)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    with contextlib.suppress(KeyboardInterrupt):
        return int(args.func(args) or 0)
    return 130


if __name__ == "__main__":
    sys.exit(main())
