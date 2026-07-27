# CADB — Crypto Anomaly Detection Bot

Real-time market-manipulation surveillance that fuses **exchange microstructure**,
**on-chain whale flow**, **social sentiment** and an **ML classifier** into a single
0-100 Manipulation Score, delivered to Telegram and Discord.

The premise: no single data source proves manipulation. A volume spike is just
activity, a whale deposit is just a transfer, a mention burst is just news. What
distinguishes a *pump* is those signals firing **together, in a specific order**.
CADB's job is to correlate them.

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ 1. Exchange  │  │ 2. On-Chain  │  │ 3. Social    │
│ CCXT Pro WS  │  │ EVM + Solana │  │ X + Telegram │
│ OBI·CVD·Vol-Z│  │ Whales·LP·   │  │ FinBERT·Bot- │
│              │  │ Bridges      │  │ farm cohorts │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       └─────────────────┼─────────────────┘
                ┌────────▼─────────┐
                │  Ingestion Bus   │  asyncio fan-out / Redis Pub-Sub
                │  MarketEvent{}   │  one normalised schema
                └────────┬─────────┘
                ┌────────▼─────────┐
                │ 4. ML Classifier │  20-feature vector
                │ IsolationForest  │  + dynamic z-score rules
                │ + Rule Engine    │  → Manipulation Score 0-100
                └────────┬─────────┘
                ┌────────▼─────────┐
                │  Alert Router    │  dedup · escalation · rate limit
                │  Telegram/Discord│
                └──────────────────┘
```

**Measured on the labelled evaluation set** (`cadb evaluate`) at the score > 80
alert threshold: **precision 0.99, recall 0.93, F1 0.955**. Scoring latency
**p95 ≈ 65 ms**, well inside the 200 ms budget. 154 tests passing.

---

## Quick start

```bash
git clone <your-repo-url> && cd crypto-anomaly-bot
pip install -e ".[dev]"

cadb demo --duration 90        # full pipeline, synthetic feeds, no credentials
```

The demo boots all four modules, injects a coordinated pump across exchange +
social + on-chain simultaneously, and prints the alerts it fires with a latency
report. Nothing leaves your machine.

### Running against live data

```bash
cp .env.example .env            # add your tokens
set -a && source .env && set +a

cadb validate -c config.yaml    # check config + credentials
cadb run -c config.yaml         # go live
cadb run -c config.yaml --dry-run   # live data, alerts only logged
```

### Docker

```bash
docker compose up -d            # includes Redis
docker compose logs -f cadb
```

### Deploy to a server with auto-update

Push to `main` → CI tests it, builds an ARM64 image, your server pulls and
restarts, with a health gate and automatic rollback. One command to provision:

```bash
curl -fsSL https://raw.githubusercontent.com/dzdozy01-cloud/crypto-anomaly-bot/main/deploy/bootstrap.sh \
  | bash -s -- dzdozy01-cloud/crypto-anomaly-bot
```

Full walkthrough incl. Oracle Cloud free-tier specifics and exchange
geo-blocking: **[docs/DEPLOY.md](docs/DEPLOY.md)**.

---

## What each module detects

### Module 1 — Exchange Anomaly Engine
L2 order books and raw trades over WebSocket (Binance, Bybit, MEXC) via CCXT Pro,
with hand-rolled native WS clients as a fallback.

| Metric | Definition | Catches |
|---|---|---|
| **Volume Z-Score** | 5-min rolling, 5s buckets, fires at `V > μ + 3σ` | Sudden inorganic volume |
| **Order Book Imbalance** | `(bid_depth − ask_depth) / (bid_depth + ask_depth)` in quote notional | Spoofed walls, layering |
| **Weighted OBI** | Same, exponentially discounted by distance from mid | Far-touch fake liquidity |
| **CVD** | Σ signed aggressor notional, plus price divergence | Absorption, markup without buying |

Two details that matter: depth is measured in **quote notional** (a 1 BTC wall and
a 1,000,000 PEPE wall aren't otherwise comparable), and **empty volume buckets are
recorded as zeros** — silence is real information that a naive implementation drops,
which otherwise inflates the baseline and hides spikes.

### Module 2 — On-Chain Whale Tracker
Raw async JSON-RPC (no sync web3 provider blocking the loop), with endpoint
failover and circuit breaking.

- **CEX flows** — ERC-20 `Transfer` logs + Solana pre/post token balance deltas
  touching ~30 known hot wallets, filtered at **> $500k**. Inflow = sell pressure,
  outflow = accumulation.
- **LP drains** — UniswapV2-style `getReserves()` polled per block; **> 30% single-block**
  TVL drops flagged as rug candidates.
- **Bridge correlation** — large stablecoin bridge transfers are matched forward
  in time against subsequent CEX deposits of similar size. That
  bridge → exchange → dump sequence is the highest-signal on-chain pattern here.

Solana transfers are read from **balance diffs, not instruction parsing**, so
transfers routed through aggregators and arbitrary programs are still caught.

### Module 3 — Social Sentiment Monitor
X (filtered stream, with search-polling fallback) and Telegram (MTProto).

- **FinBERT** sentiment when `transformers`+`torch` are installed; otherwise a
  crypto-tuned lexicon backend with negation/intensifier/emoji handling.
  Inference runs in a thread executor so a 20-80 ms forward pass never stalls the loop.
- **Mention acceleration** — d(rate)/dt. A pump's tell isn't a high mention rate,
  it's a *rapidly increasing* one; organic interest ramps smoothly.
- **Bot-farm detection** — five orthogonal signals: account-age variance, shingled
  text near-duplication, posting-cadence regularity, follower uniformity, mention velocity.

The important part is **cohort isolation**. A farm never operates in a vacuum — its
posts are diluted in organic chatter, so global account-age variance stays high and
naive detection misses it entirely. CADB finds the densest cluster of similarly-aged
young accounts and evaluates *that subgroup*, which is what a human investigator does.
Measured separation: farm **0.75**, organic **0.10**, organic-with-volume-spike **0.17**.

### Module 4 — ML Manipulation Classifier
A 20-feature vector per asset → Isolation Forest **+** an explainable rule engine.

Three design decisions worth calling out, each of which measurably changed results:

1. **Noisy-OR evidence combination, not a weighted average.** A plain average
   structurally caps any single-source signature at its own weight — a blatant
   exchange-only wash trade could never exceed 40/100 at `w=0.4`. Combining
   sub-scores as independent evidence let one overwhelming source carry a verdict
   while multiple sources still compound. *Recall 0.45 → 0.75.*
2. **The ML half can raise a verdict but never veto one.** An unsupervised forest
   scores *rarity*, not *manipulation* — and wash trading is common in crypto, so
   the forest rates it unremarkable and was dragging confident rule detections
   below threshold. *Recall 0.75 → 0.93.*
3. **An organic-activity discount.** Real news moves volume, mentions and sentiment
   together and looks superficially like a pump. What it lacks is artificial markers
   (coordinated accounts, spoofed depth, LP drains, exchange pre-loading). Without
   any of those, the score is discounted hard — this is what keeps precision at 0.99
   despite the more sensitive combiner.

Recognised archetypes: pump-and-dump, rug-pull, spoofing/layering, wash trading,
whale distribution, social shill.

---

## Detection quality

```
$ cadb evaluate

 threshold  precision   recall      f1    TP   FP    FN
--------------------------------------------------------
        60      0.968    1.000   0.984   900   30     0
        70      0.983    0.990   0.987   891   15     9
        80      0.987    0.926   0.955   833   11    67   <- alert threshold
        90      0.989    0.716   0.830   644    7   256

scenario                  mean     p10     p90   >=80
----------------------------------------------------
benign_news               53.3    34.8    67.5     1%
normal                     2.2     0.0     6.9     0%
pump_and_dump            100.0   100.0   100.0   100%
rug_pull                  99.9   100.0   100.0   100%
social_shill              90.4    79.5    98.3    89%
spoofing                  93.0    82.2   100.0    95%
wash_trading              84.3    75.3    89.7    74%
whale_distribution        98.0    92.5   100.0    97%
```

`benign_news` is a deliberate hard negative — genuine high-volume news events that
*should not* alert. Its separation from `pump_and_dump` is the metric that matters
most for real-world usability.

> These figures are measured against the synthetic labelled corpus in
> `cadb.modules.ml.training`, which encodes the manipulation archetypes described
> above. It is a rigorous regression harness and a cold-start baseline — **not**
> a substitute for validation on labelled real-market incidents. Retrain on your
> own captured data (`cadb train --data events.jsonl`) before trusting the
> absolute numbers in production.

---

## Telegram bot

22 commands across four groups. The bot is not just an alert pipe — you can
interrogate every module's live state and control sensitivity without a restart.

**📊 Monitoring**

| Command | Action |
|---|---|
| `/scores` | Risk board — every tracked asset ranked by manipulation score |
| `/check <ASSET>` | Full report: score, module breakdown, evidence, key features |
| `/explain <ASSET>` | Feature-by-feature breakdown with data-freshness per module |
| `/history [ASSET]` | Recent alerts fired, with timestamps |

**🔍 Per-module detail**

| Command | Action |
|---|---|
| `/book <PAIR>` | Order book by venue — OBI, depth, spread, CVD, divergence |
| `/whales [ASSET]` | Recent >$500k CEX transfers with direction and net pressure |
| `/flows` | Net exchange inflow/outflow per asset, plus bridge activity |
| `/social <TICKER>` | Sentiment, mention rate/acceleration, bot-farm verdict |

**⚙️ Control**

| Command | Action |
|---|---|
| `/watch`, `/unwatch` | Subscribe/unsubscribe this chat |
| `/threshold <0-100>` | Adjust alert sensitivity live |
| `/mute <min>`, `/unmute` | Silence alerts temporarily |
| `/pause`, `/resume` | Halt delivery — **detection keeps running** |
| `/test` | Fire a sample alert to verify sink wiring |

**🩺 Diagnostics**

| Command | Action |
|---|---|
| `/status` | Module health, bus throughput, classifier state, latency |
| `/venues` | Feed connections, message counts, degraded streams |
| `/config` | Every active threshold and coverage setting |
| `/metrics` | Latency percentiles and counters |

Example — `/check PEPE` during a live episode:

```
🟠 MANIPULATION ALERT — PEPE
Score: 83.2/100  ████████░░   Severity: HIGH

Signal breakdown
  Social      ████░░░░  53.1
  Order Flow  ██░░░░░░  30.0
  On-Chain    ██░░░░░░  19.4

Evidence
  • order book 71% bid-heavy
  • price/CVD divergence — markup without real buying
  • DEX liquidity -49% in one block
  • bot-farm pattern (confidence 53%)
  • extreme bullish sentiment (+0.61)
```

Alerts are de-duplicated per asset with a cooldown, **but severity escalation always
breaks through** — suppressing a MEDIUM→CRITICAL upgrade is worse than a duplicate.
In one 80-second demo run this collapsed 339 raw threshold breaches into **9 delivered
alerts**.

## Architecture notes

**Unified schema.** Every module publishes the same `MarketEvent`: `timestamp`,
`venue`, `asset_pair`, `metric_type`, `raw_value`, `normalized_z_score`, plus optional
`usd_value`/`confidence`/`meta`. That contract is what lets the ML layer treat an
order-book imbalance, a whale transfer and a mention burst as columns of one vector.

**Bus.** In-process asyncio fan-out by default (~20 µs/delivery); Redis Pub/Sub for
distributed deployments. Bounded per-subscriber queues with **drop-oldest** semantics —
under back-pressure you want the *freshest* tick, not the stalest.

**Statistics.** All estimators are O(1) per update. `RobustZScore` (median/MAD) anchors
the scoring because a single 50σ print poisons a classic mean/std baseline so badly
that the *next* manipulation looks normal. Thresholds adapt to the volatility regime.

**Resilience.** Every network component runs under supervised auto-reconnect with
**decorrelated-jitter** exponential backoff (plain backoff makes all 30 symbol streams
retry in lockstep after a venue-wide disconnect and get rate-limited), plus circuit
breakers and token-bucket rate limiting.

**Graceful degradation.** No Redis → in-process bus. No `ccxt` → native WS clients.
No `transformers` → lexicon sentiment. No credentials → that source is skipped, the
rest keep running. The system always starts.

---

## Performance

Measured on 2 vCPU during the simulated demo:

| Stage | p50 | p95 | p99 |
|---|---|---|---|
| Trade → metrics | 0.07 ms | 0.14 ms | 0.28 ms |
| Order book → OBI | 0.29 ms | 0.41 ms | 0.54 ms |
| Full scoring cycle | 53 ms | 65 ms | 74 ms |

Throughput: 5,000 events published in < 5 s with zero drops under a deliberately
slow consumer. The 200 ms budget is asserted as a **test**, not just documented
(`tests/test_integration.py::test_latency_budget_respected`).

---

## Project layout

```
src/cadb/
├── core/              schema · bus · stats · resilience · config · telemetry
├── modules/
│   ├── exchange/      microstructure (pure) · feeds (I/O) · engine
│   ├── onchain/       rpc · registry · tracker
│   ├── social/        sentiment · botfarm · sources · monitor
│   └── ml/            features · classifier · training · scorer
├── alerting/          formatter · router (dedup, escalation, rate limit)
├── bot/               telegram_bot (transport) · commands (22 handlers)
├── app.py             orchestrator
└── cli.py             run · demo · train · evaluate · backtest · validate
tests/                 154 tests
```

Analytics are deliberately separated from I/O: `microstructure.py` has no network
code, so the maths is unit-testable and per-tick cost is predictable.

---

## CLI

```bash
cadb run -c config.yaml [--simulate] [--dry-run] [--threshold 85]
cadb demo --duration 90              # scripted end-to-end demonstration
cadb train --samples 20000           # (re)train the Isolation Forest
cadb train --data events.jsonl       # train on your own recorded telemetry
cadb evaluate                        # precision/recall report
cadb backtest events.jsonl           # replay recorded telemetry
cadb validate -c config.yaml         # check configuration
```

Capture live telemetry for later training/backtesting with `backtest.EventRecorder`.

---

## Testing

```bash
pytest tests/ -v                                  # 154 tests, ~2 min
pytest tests/test_ml.py::TestDetectionQuality -v  # precision/recall gates
```

The suite includes precision/recall gates that fail CI on tuning regressions, a
latency-budget assertion, back-pressure tests, and false-positive tests (organic
news, organic volume spikes) alongside the true-positive ones.

Several of these tests caught real bugs during development — a case-sensitivity bug
that made checksummed EVM addresses silently fail to match known hot wallets, a
degenerate-scale path that reported "normal" for any deviation on a flat series, and
a config bug where unexpanded `${VAR}` placeholders registered a webhook sink pointing
at a literal placeholder string.

---

## Configuration

Layered: **defaults → `config.yaml` → environment**. Secrets never live in the YAML;
use `${VAR}` or `${VAR:-default}` references.

**No exchange API keys are needed** — CADB reads public market data only and never
places orders. **Etherscan is not used** either; the on-chain tracker speaks raw
JSON-RPC. **Only two variables are required** — `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
Everything else has a working default or degrades gracefully; the system starts
with a completely empty environment. Full reference: **[docs/SETUP.md](docs/SETUP.md)**.

Key knobs: `ml.alert_threshold` (default 80), `ml.ml_blend` (0 = pure rules,
1 = pure forest), `ml.weights`, `exchange.volume_z_threshold` (3.0),
`onchain.whale_threshold_usd` (500k), `onchain.liquidity_drop_pct` (30),
`social.bot_age_variance_threshold` (0.35).

---

## Limitations

- **The synthetic corpus is a harness, not ground truth.** Real manipulation is
  messier. Retrain on captured data before trusting absolute scores.
- **Isolation Forest is unsupervised** — it flags rarity. The rule engine supplies
  the domain semantics, which is why the two are fused asymmetrically.
- **Social coverage depends on API tier.** X's filtered stream needs elevated access;
  the search-polling fallback is rate-limited and slower.
- **Public RPCs are slow.** For real on-chain latency use a dedicated provider.
- **A high score is an investigation trigger, not proof.** Legitimate events
  (listings, macro shocks, genuine news) can score high.

## Legal

For research and defensive market-surveillance purposes. Not financial advice.
Verify your use complies with the terms of service of every exchange and API you
connect to, and with local regulations. MIT licensed.
