# Getting free API keys

Everything CADB needs has a free tier that comfortably covers this workload.
Nothing below requires a credit card.

**Priority order** — do these in sequence and stop when you have what you need:

| # | Key | Fixes | Time |
|---|---|---|---|
| 1 | **Alchemy** (ETH + Solana) | RPC rate limits, missed whale moves | 5 min |
| 2 | **X / Twitter** | Re-enables the whole social module | 15 min |
| 3 | **Alchemy BSC** or **Chainstack** | BSC `eth_getLogs` limits | 2 min |

---

## 1. Alchemy — Ethereum + Solana (highest value)

**Free tier: 30M compute units/month.** CADB's on-chain scanning uses roughly
2-4M CU/month at a 3-second poll, so you will not come close to the cap.

1. Sign up at **[alchemy.com](https://www.alchemy.com/)** — email or Google, no card.
2. **Create App** → name it `cadb` → Chain: **Ethereum**, Network: **Mainnet**.
3. Click **API Key** and copy the HTTPS URL:
   `https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY`
4. Repeat with Chain: **Solana**, Network: **Mainnet** — same dashboard, second app.

Add to `~/crypto-anomaly-bot/.env`:

```bash
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
SOLANA_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY
```

> Keep the public node as a fallback — the client rotates automatically:
> ```bash
> ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY,https://ethereum-rpc.publicnode.com
> ```

**Why this matters most:** on public RPCs Module 2 falls behind the chain tip and
detects whale deposits late — sometimes after the dump has already happened. That
is the difference between a warning and a post-mortem.

### Alternatives

| Provider | Free tier | Notes |
|---|---|---|
| [Alchemy](https://www.alchemy.com/) | 30M CU/mo | Best all-rounder, ETH + SOL + BSC in one account |
| [Infura](https://www.infura.io/) | 3M credits/day | Solid for ETH; no Solana on free tier |
| [Chainstack](https://chainstack.com/) | 3M requests/mo | Simple 1-call-=-1-request billing; good BSC support |
| [Helius](https://www.helius.dev/) | 1M credits, 10 RPS | Solana specialist, excellent if you only need SOL |
| [QuickNode](https://www.quicknode.com/) | 10M credits | Fastest latency, but the free tier is trial-shaped |

---

## 2. X (Twitter) — re-enables the social module

Without this, CADB deliberately disables Module 3 rather than fabricating data,
so you lose bot-farm detection entirely.

1. Go to **[developer.x.com](https://developer.x.com/en/portal/dashboard)** and
   sign in with the X account you want to use.
2. Apply for **Free** access. Describe the use case honestly — something like
   *"Read-only research bot monitoring public cashtag mentions for market
   anomaly detection. No posting, no automation, no DMs."* Approval is usually
   instant to a few hours.
3. In the portal: **Projects & Apps → your app → Keys and tokens**.
4. Under **Authentication Tokens**, generate the **Bearer Token** and copy it —
   it is shown only once.

```bash
X_BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAA...
```

**Free-tier reality:** 100 posts/month read cap and no filtered-stream access.
CADB detects this and automatically falls back to recent-search polling. That is
enough to prove the pipeline works, but the volume is too low for reliable
bot-farm detection — that needs the **Basic** tier ($200/mo, 10k posts) to be
genuinely useful.

**If that is not worth it to you**, leave `X_BEARER_TOKEN` unset. Exchange
microstructure and on-chain detection work fully without it, and CADB will say
so plainly rather than pretending.

**Telegram channels are a free alternative** — most pump-and-dump coordination
happens there anyway:

1. Get `api_id` and `api_hash` from **[my.telegram.org/apps](https://my.telegram.org/apps)**
   (a *user* credential, not the bot token).
2. `pip install telethon`, then list channels in `config.yaml`:
   ```yaml
   social:
     telegram_channels: ["@somepumpchannel", "@anothersignals"]
   ```

---

## 3. BSC — only if you want Binance Smart Chain

The public BSC nodes that survive are already configured, but if you hit limits:

- **Alchemy** supports BSC — create a third app, same account, same 30M CU pool.
- **Chainstack** free tier has good BSC coverage.

```bash
BSC_RPC_URL=https://bnb-mainnet.g.alchemy.com/v2/YOUR_KEY
```

Or simply drop BSC — most manipulation worth catching happens on Ethereum and
Solana:

```yaml
onchain:
  evm_rpc:
    ethereum: ${ETH_RPC_URL:-https://ethereum-rpc.publicnode.com}
    # bsc: removed
```

---

## Applying the keys

```bash
cd ~/crypto-anomaly-bot
nano .env                      # paste the keys
docker compose up -d           # restart to pick them up
docker compose exec cadb cadb validate -c config.yaml
```

`validate` prints the resolved endpoint for each chain, so you can confirm your
key is actually in use:

```
  on-chain endpoints:
    ✅ ethereum  eth-mainnet.g.alchemy.com (+1 failover)
    ✅ solana    solana-mainnet.g.alchemy.com (+1 failover)
```

Then watch the logs — the `rate limited (429)` and `limit exceeded` warnings
should disappear entirely.

---

## Cost summary

| Component | Free tier enough? |
|---|---|
| Exchange feeds (Binance/Bybit/MEXC) | ✅ No key needed at all — public WebSockets |
| Ethereum + Solana RPC | ✅ Yes, comfortably |
| BSC RPC | ✅ Yes |
| Telegram bot + alerts | ✅ Yes, unlimited |
| X / Twitter | ⚠️ Works, but too rate-limited for strong bot-farm signal |

**Running cost: $0** for everything except high-volume social monitoring.
