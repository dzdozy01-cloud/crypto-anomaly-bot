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

> **Need keys?** [docs/FREE_KEYS.md](FREE_KEYS.md) walks through obtaining every
> credential on a free tier, in priority order, with no credit card.

## Tier 2 — Strongly recommended

### Exchange API keys: not needed, ever

CADB reads **public market data only** — L2 order books and the trade tape. Those
streams are unauthenticated on every supported venue. There is no `apiKey` field
anywhere in the exchange layer, because the bot never places an order, never reads
a balance, and never touches a private endpoint.

Verified with no credentials at all:

```
200  https://api.mexc.com/api/v3/ping        # public data, no auth
```

**Do not create exchange API keys for this.** A surveillance tool that only reads
public tape has no reason to hold a key that could trade or withdraw. If you ever
extend CADB to place orders, that is the moment to introduce keys — with
withdrawal permission disabled and IP allow-listing on.

> **Geo-blocking is a separate issue.** Binance returns HTTP `451` and Bybit `403`
> from many datacenter IPs (AWS/GCP US and EU regions especially). That is a legal
> region block, *not* an auth failure — a key would not fix it. If you hit this,
> host the bot somewhere unblocked, or drop the affected venue from
> `exchange.exchanges` in `config.yaml`. MEXC is generally the most permissive.

### Etherscan: not used

CADB talks **raw JSON-RPC** to nodes, not block-explorer APIs. No `ETHERSCAN_API_KEY`
exists in the config. The tracker uses `eth_getLogs`, `eth_call`,
`eth_getBlockByNumber` and `eth_blockNumber` on EVM chains, plus
`getSignaturesForAddress` / `getTransaction` on Solana.

This is deliberate. Explorer APIs cap you at ~5 req/s, add indexing lag, and would
put a third party between you and consensus. Direct RPC gives lower latency and no
dependency on someone else's index.

### RPC endpoints: the one thing genuinely worth paying for

The defaults work but are the weakest link. Measured 2026-07 (`eth_getLogs` over
3 blocks of USDT transfers — the exact call Module 2 makes every poll):

| Endpoint | Result |
|---|---|
| `ethereum-rpc.publicnode.com` | ✅ 364 logs in **539 ms** — current default |
| `1rpc.io/eth` | ✅ 364 logs in **1192 ms** — fallback default |
| `eth.llamarpc.com` | ❌ HTTP 521 — dead |
| `rpc.ankr.com/eth` | ❌ `Unauthorized` — now needs a key |
| `cloudflare-eth.com` | ❌ `-32046 Cannot fulfill request` |
| `eth.drpc.org` | ❌ HTTP 408 timeout on `getLogs` |
| `eth.meowrpc.com` | ❌ `eth_getLogs is not supported` |

**What rate limiting actually costs you.** Module 2 polls every 3 s and requests
logs for a block range. On a throttled public node you get HTTP 429s, the circuit
breaker opens, and the scanner falls behind the chain tip. It catches up by widening
the range — but a whale deposit detected 90 seconds late is worth far less than one
detected in the same block. Modules 1, 3 and 4 are unaffected; you lose on-chain
timeliness, not the whole system.

A free tier from Alchemy, Infura or QuickNode (~100M compute units/month) is far
more than this workload needs and removes the problem entirely:

```bash
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY
```

**Failover is built in** — comma-separate endpoints and the client rotates on
failure, with an independent circuit breaker per endpoint:

```bash
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/KEY,https://ethereum-rpc.publicnode.com
```

Verified: with a dead endpoint first in the list, the request still succeeds on the
next one and the dead host is marked failed.

| Variable | Default (comma = failover order) |
|---|---|
| `ETH_RPC_URL` | `ethereum-rpc.publicnode.com,1rpc.io/eth` |
| `BSC_RPC_URL` | `bsc-rpc.publicnode.com,bsc-dataseed.binance.org` |
| `SOLANA_RPC_URL` | `api.mainnet-beta.solana.com,solana-rpc.publicnode.com` |

Both Solana defaults responded in **75-85 ms**, so Solana is in better shape than
Ethereum on free infrastructure.

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

## Blank vs. commented: both safe

`${VAR:-default}` uses POSIX **colon-dash** semantics — the default applies when a
variable is unset **or set-but-empty**. So both of these use the built-in default:

```bash
#ETH_RPC_URL=              # commented out  → default
ETH_RPC_URL=               # blank          → default (also fine)
ETH_RPC_URL=https://...    # set            → your value wins
```

Commenting out is clearer, which is how `.env.example` now ships.

> This was a genuine bug until recently: a blank `ETH_RPC_URL=` was treated as a
> deliberate empty override, which left the on-chain tracker with **zero
> endpoints** — and it still reported itself healthy. Both halves are fixed:
> empty now means "not configured", and a tracker with no endpoints reports
> `healthy: false` with an explicit error.

Confirm what your config actually resolved to:

```bash
cadb validate -c config.yaml
```

```
  on-chain endpoints:
    ✅ ethereum  ethereum-rpc.publicnode.com (+1 failover)
    ✅ bsc       bsc-rpc.publicnode.com (+1 failover)
    ✅ solana    api.mainnet-beta.solana.com (+1 failover)
```

A `❌ NOT CONFIGURED` line means that chain is not being monitored at all.

---

## Your key is set but still rate limited?

Docker's `env_file` parser is **not** a shell. It keeps quotes, trailing spaces
and inline comments literally:

```bash
SOLANA_RPC_URL="https://..."      # ❌ quotes became part of the URL
SOLANA_RPC_URL=https://...  # key # ❌ comment became part of the URL
SOLANA_RPC_URL=https://...        # ✅ correct
```

CADB now sanitises these automatically, but older builds failed with a generic
connection error that looked exactly like the provider being down.

Confirm which endpoint is actually in use — this is the fastest way to tell a
config problem from a provider problem:

```bash
docker compose exec cadb cadb validate -c config.yaml
```

```
  on-chain endpoints:
    ✅ ethereum  eth-mainnet.g.alchemy.com [keyed]
    ✅ solana    solana-mainnet.g.alchemy.com [keyed]
```

`[public — rate limited]` means your key is **not** being picked up. The same
line is logged at startup, so `docker compose logs cadb | head -30` shows it too.

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
| Exchange engine | ✅ Fully working — public WebSockets need no auth, no API keys ever |
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
