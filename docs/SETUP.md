# Environment Setup

**Short answer: only two variables are truly required** — `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID`. Everything else has a working default or degrades gracefully.

Every variable below is optional in the strict sense: the system starts with an
empty environment. Unset credentials disable *that source only*; the rest keeps
running.

---

## Tier 1 — Required to receive alerts

Without these the bot still detects manipulation, but only logs to the console.

| Variable | What it is | How to get it |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Bot API token | Message [@BotFather](https://t.me/BotFather) → `/newbot` → copy the token |
| `TELEGRAM_CHAT_ID` | Where alerts go | See "Finding your chat ID" below |

```bash
TELEGRAM_BOT_TOKEN=8123456789:AAHx1_exampleTokenReplaceMe
TELEGRAM_CHAT_ID=-1001234567890
```

### Finding your chat ID

Send any message to your bot, then:

```bash
curl -s "https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates" \
  | python3 -c "import json,sys; [print(u['message']['chat']['id'], '-', u['message']['chat'].get('title') or u['message']['chat'].get('username')) for u in json.load(sys.stdin)['result'] if 'message' in u]"
```

Personal chats give a positive number (`123456789`); groups and channels give a
negative one (`-1001234567890`). For a channel, add the bot as an **admin** first.

---

## Tier 2 — Strongly recommended

The public RPC defaults work, but they are rate-limited and slow enough to make
Module 2 miss blocks under load. A free Alchemy/Ankr/QuickNode key fixes that.

| Variable | Default if unset | Why override |
|---|---|---|
| `ETH_RPC_URL` | `https://eth.llamarpc.com` | Public node — rate-limited, misses blocks |
| `BSC_RPC_URL` | `https://binance.llamarpc.com` | Same |
| `SOLANA_RPC_URL` | `https://api.mainnet-beta.solana.com` | Heavily throttled; SPL scanning suffers most |

```bash
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY
```

**Failover:** comma-separate multiple endpoints. The client rotates on failure and
opens a circuit breaker per endpoint.

```bash
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/KEY,https://rpc.ankr.com/eth
```

---

## Tier 3 — Optional data sources

Unset means that source is skipped. Detection continues on the remaining modules
with a reduced feature vector (the classifier damps scores when coverage is thin).

| Variable | Enables | Notes |
|---|---|---|
| `X_BEARER_TOKEN` | X/Twitter ingestion | [developer.x.com](https://developer.x.com) → Project → Bearer Token |
| `TELEGRAM_API_ID` | Telegram channel monitoring | [my.telegram.org/apps](https://my.telegram.org/apps) — **not** the bot token |
| `TELEGRAM_API_HASH` | Same | Also needs `telegram_channels` in `config.yaml` and `pip install telethon` |
| `DISCORD_WEBHOOK_URL` | Discord alerts | Server Settings → Integrations → Webhooks |

> `TELEGRAM_API_ID`/`API_HASH` are **user-account** credentials (MTProto), entirely
> separate from `TELEGRAM_BOT_TOKEN`. Bots cannot read arbitrary public channels;
> only a user session can. Skip these unless you specifically want channel scraping.

**X API tiers:** the filtered stream needs elevated access. On the free/basic tier
the bot automatically falls back to recent-search polling — slower, rate-limited,
but functional.

---

## Tier 4 — Infrastructure & tuning

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Only used when `bus.kind: redis`. Unreachable → falls back to in-process bus |
| `CADB_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### `CADB_*` runtime overrides

These take precedence over both `config.yaml` **and** the plain variables above —
useful for per-deployment overrides without editing the file:

```
CADB_TELEGRAM_BOT_TOKEN   CADB_TELEGRAM_CHAT_ID   CADB_DISCORD_WEBHOOK
CADB_X_BEARER_TOKEN       CADB_ETH_RPC            CADB_SOLANA_RPC
CADB_REDIS_URL            CADB_LOG_LEVEL          CADB_ALERT_THRESHOLD
```

Example — same image, stricter alerting in production:

```bash
CADB_ALERT_THRESHOLD=90 CADB_LOG_LEVEL=WARNING cadb run -c config.yaml
```

---

## Minimum viable setups

**Zero config** — works right now, no variables at all:

```bash
cadb demo --duration 90        # synthetic feeds, console output
```

**Just alerts** — real market data, Telegram delivery, public RPCs:

```bash
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

**Recommended production**:

```bash
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/KEY
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/KEY
X_BEARER_TOKEN=...
```

---

## Loading and verifying

```bash
cp .env.example .env          # edit it
set -a && source .env && set +a
cadb validate -c config.yaml  # reports which credentials are missing
```

`validate` prints active thresholds, coverage, configured sinks, and warns about
unset credentials — it does **not** make network calls, so it is safe to run anywhere.

Then confirm delivery actually works end to end:

```bash
cadb run -c config.yaml
```

…and send `/test` to your bot. It fires a sample alert through every configured
sink and reports ✅/❌ per sink. If that works, real alerts will too.

---

## Docker

`docker-compose.yml` reads `.env` automatically:

```bash
cp .env.example .env && $EDITOR .env
docker compose up -d
docker compose logs -f cadb
```

Compose sets `CADB_REDIS_URL=redis://redis:6379/0` internally, so leave `REDIS_URL`
alone there.

---

## What breaks if you set nothing

Verified with a completely empty environment:

| Component | Behaviour |
|---|---|
| Exchange engine | ✅ Fully working — public WebSockets need no auth |
| On-chain tracker | ⚠️ Works on rate-limited public RPCs |
| Social monitor | ⚠️ No credentials → source skipped; lexicon sentiment backend |
| ML classifier | ✅ Fully working — bootstraps on the synthetic corpus |
| Alerts | ⚠️ Console only |
| Bus | ✅ In-process |

Nothing crashes. That is deliberate: a missing API key should never take down
market surveillance that is otherwise healthy.

---

## Security

- **Never commit `.env`** — it is gitignored; only `.env.example` is tracked.
- Prefer a **dedicated bot** per deployment so you can revoke one without affecting others.
- RPC keys with **read-only** scope are sufficient; this system never signs transactions.
- The bot holds **no wallet keys and executes no trades** — worst case on compromise
  is leaked API keys, not lost funds.
- Restrict who can command the bot by passing `allowed_chats` to `TelegramBot`;
  by default only the configured `TELEGRAM_CHAT_ID` is authorised.
