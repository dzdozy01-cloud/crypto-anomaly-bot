# Deploying to Oracle Cloud with auto-update

Push to `main` → GitHub tests it, builds an ARM image, and your server pulls and
restarts. Roughly 3-4 minutes end to end, with an automatic rollback if the new
container fails to come up.

```
git push  →  test gate  →  build multi-arch  →  push to GHCR
                                                     │
                                    SSH ─────────────┘
                                     ↓
                        Oracle ARM: pull + restart + health-gate
                                     ↓
                          ✅ healthy   or   ↩️ auto-rollback
```

**Why build on GitHub and not on the server?** The Always Free shape is 2 OCPU /
12 GB. Compiling an image there takes minutes and starves the bot of the CPU it
needs for sub-200 ms scoring. GitHub's runners build it in ~90 s; your server just
pulls a finished layer.

---

## Before you start: Oracle free-tier reality check

**Oracle halved the Always Free ARM tier in June 2026** — it is now **2 OCPU /
12 GB** total (was 4/24), across *all* your A1 instances. Existing oversized
instances get shut down until resized. Plan for one instance at 2 OCPU / 12 GB.

That is still comfortably more than CADB needs (it peaks around 400 MB with
FinBERT disabled).

| Shape | Verdict |
|---|---|
| **VM.Standard.A1.Flex** — 2 OCPU / 12 GB, ARM | ✅ Recommended |
| **VM.Standard.E2.1.Micro** — 1/8 OCPU / 1 GB, x86 | ⚠️ Testing only; too small for FinBERT |

**Pick a non-US region.** This matters more than the shape — see below.

---

## The geo-blocking problem (read this first)

Binance returns HTTP **451** and Bybit **403** from many datacenter IPs. That is a
regulatory region block, not an authentication failure — no API key fixes it.

Measured from a Google Cloud US host (`34.168.157.77`, Oregon):

```
451  api.binance.com     ❌ blocked
403  api.bybit.com       ❌ blocked
200  api.mexc.com        ✅ works
```

**Choose your Oracle home region accordingly.** Region is fixed at signup and
cannot be changed later, so decide now:

| Region | Binance / Bybit |
|---|---|
| Frankfurt, Amsterdam, Zurich | ✅ Generally fine |
| Singapore, Tokyo, Seoul, Mumbai | ✅ Generally fine |
| São Paulo, Johannesburg | ✅ Generally fine |
| **Any US region (Ashburn, Phoenix, San Jose)** | ❌ Expect 451/403 |
| London | ⚠️ Mixed — UK rules vary by venue |

Verify from the server before committing to a setup:

```bash
for u in api.binance.com api.bybit.com api.mexc.com; do
  echo "$(curl -sS -o /dev/null -w '%{http_code}' https://$u/api/v3/ping 2>/dev/null || echo ---)  $u"
done
```

If a venue is blocked, drop it from `exchange.exchanges` in `config.yaml`. The
other three modules are unaffected — you lose one venue's microstructure, not the
system. MEXC is the most permissive of the three.

---

## Setup

### 1. Create the instance

Oracle Cloud console → **Compute → Instances → Create**:

- **Image**: Ubuntu 22.04 or 24.04 (ARM build)
- **Shape**: `VM.Standard.A1.Flex`, 2 OCPU, 12 GB
- **SSH keys**: upload your public key
- Note the **public IPv4** once it boots

> "Out of host capacity" is common for A1. Retry in a different availability
> domain, or at a quieter hour — it does free up.

### 2. Provision it

```bash
ssh ubuntu@<YOUR_SERVER_IP>

curl -fsSL https://raw.githubusercontent.com/dzdozy01-cloud/crypto-anomaly-bot/main/deploy/bootstrap.sh \
  | bash -s -- dzdozy01-cloud/crypto-anomaly-bot
```

This installs Docker, adds swap on small hosts, creates `~/cadb` with
`docker-compose.yml` + `config.yaml` + `.env`, generates a deploy keypair, and
prints the secrets you need. It is idempotent — safe to re-run.

### 3. Add your credentials

```bash
nano ~/cadb/.env       # TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are the only required ones
```

### 4. Add the GitHub secrets

**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Value |
|---|---|
| `ORACLE_HOST` | Your server's public IP |
| `ORACLE_USER` | `ubuntu` (or `opc` on Oracle Linux) |
| `ORACLE_SSH_KEY` | The **private** key the bootstrap printed — all of it, including `BEGIN`/`END` |
| `ORACLE_PORT` | Only if SSH is not on 22 |
| `TELEGRAM_BOT_TOKEN` | Optional — deploy success/failure pings |
| `TELEGRAM_CHAT_ID` | Optional |

### 5. First deploy

```bash
cd ~/cadb
docker compose pull      # needs the image to exist — push to main once first
docker compose up -d
docker compose logs -f cadb
```

Look for `✅ system online — 4 module(s)`. Then send `/status` and `/test` to your
bot.

From here, **every push to `main` deploys automatically.**

---

## What the pipeline does

1. **Test gate** — ruff, the full 149-test suite, and the detection-quality gate
   (precision ≥ 0.90, recall ≥ 0.75). A regression here blocks the deploy entirely.
2. **Build** — multi-arch `linux/amd64,linux/arm64` via QEMU, pushed to GHCR with
   layer caching, tagged `latest` and the short SHA.
3. **Deploy** — SSH in, `docker compose pull`, restart, then **wait for health**:
   polls up to 150 s for the `system online` log line. If the container dies, it
   dumps logs and **retags the previous image and restarts it**.

Docs-only pushes are skipped via `paths-ignore`.

### Rolling back manually

```bash
cd ~/cadb
docker compose down
docker run -d --name cadb ghcr.io/dzdozy01-cloud/crypto-anomaly-bot:abc1234   # a known-good SHA
```

Or revert the commit and push — the pipeline redeploys the previous state.

---

## Alternative: Watchtower (no SSH secrets)

If you would rather not put an SSH key in GitHub, Watchtower polls GHCR every
5 minutes and restarts when the tag moves:

```bash
cd ~/cadb && docker compose --profile watchtower up -d
```

Then delete the `deploy` job from `.github/workflows/deploy.yml` — the build job
still publishes to GHCR, which is all Watchtower needs.

| | GitHub Actions SSH | Watchtower |
|---|---|---|
| Deploy latency | ~30 s after build | up to 5 min |
| Secrets on GitHub | SSH private key | none |
| Health gate + rollback | ✅ | ❌ |
| Logs in Actions tab | ✅ | server only |

The Actions path is the better default; Watchtower is the pragmatic choice if the
SSH key bothers you.

---

## Operations

```bash
cd ~/cadb

docker compose logs -f cadb          # live logs
docker compose logs --tail=200 cadb  # recent
docker stats --no-stream             # CPU / memory
docker compose restart cadb          # restart
docker compose down && docker compose up -d   # full recycle

curl -s localhost:9090/health | python3 -m json.tool   # if metrics_port: 9090
```

The trained model lives in the `model-data` volume and **survives redeploys**, so
online retraining is not lost on every push.

### Log growth

Both services cap JSON logs (20 MB × 5 for cadb, 10 MB × 3 for redis), so a busy
bot cannot fill a 200 GB boot volume.

### Keeping the free tier

Oracle reclaims **idle** Always Free compute (under ~10% CPU, ~10% network over a
7-day window). A running CADB with live feeds sits comfortably above that
threshold, so it will not be flagged — but do not leave it stopped for days.

---

## Troubleshooting

**`docker compose pull` says manifest unknown** — the image does not exist yet.
Push to `main` once and let the build job publish it. For a private repo, log in
first: `echo <PAT> | docker login ghcr.io -u <user> --password-stdin`.

**Deploy job fails on SSH** — confirm `ORACLE_SSH_KEY` contains the *private* key
including both header lines, that `ORACLE_USER` matches the image
(`ubuntu` vs `opc`), and that Oracle's **security list** allows inbound TCP 22.

**Container restarts in a loop** — `docker compose logs --tail=100 cadb`. Usually a
malformed `.env` or a `config.yaml` typo; `docker compose run --rm cadb validate -c config.yaml`
pinpoints it.

**Exchange 451/403** — geo-block, covered above. Drop the venue or change region.

**Out of memory on the micro shape** — set `social.use_finbert: false` in
`config.yaml`. The lexicon backend is ~0.02 ms/post and needs no torch.

**A1 "out of capacity"** — retry in another availability domain or later; it is a
transient Oracle constraint, not an account problem.
