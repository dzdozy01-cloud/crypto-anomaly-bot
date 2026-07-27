#!/usr/bin/env bash
# One-shot provisioning for an Oracle Cloud (or any Ubuntu/Oracle Linux) host.
#
#   curl -fsSL https://raw.githubusercontent.com/OWNER/REPO/main/deploy/bootstrap.sh | bash -s -- OWNER/REPO
#
# Installs Docker, opens the firewall as needed, lays out ~/cadb, and prints
# the exact GitHub secrets to add. Safe to re-run: every step is idempotent.

set -euo pipefail

REPO="${1:-}"
CADB_DIR="${HOME}/cadb"

c_ok()   { printf '\033[32m✅ %s\033[0m\n' "$*"; }
c_info() { printf '\033[36m→  %s\033[0m\n' "$*"; }
c_warn() { printf '\033[33m⚠️  %s\033[0m\n' "$*"; }
c_err()  { printf '\033[31m❌ %s\033[0m\n' "$*" >&2; }

if [[ -z "$REPO" || "$REPO" != */* ]]; then
  c_err "usage: bootstrap.sh <owner/repo>   e.g. bootstrap.sh dzdozy01-cloud/crypto-anomaly-bot"
  exit 1
fi
OWNER="${REPO%%/*}"
NAME="${REPO##*/}"

echo
echo "════════════════════════════════════════════════════════════"
echo "  CADB — server provisioning"
echo "  repo: $REPO"
echo "  arch: $(uname -m)   host: $(hostname)"
echo "════════════════════════════════════════════════════════════"
echo

# ---------------------------------------------------------------- sanity
if [[ $EUID -eq 0 ]]; then
  c_warn "running as root — the bot will run as root too."
  c_warn "Prefer a normal user with sudo (Oracle images give you 'ubuntu' or 'opc')."
fi

ARCH="$(uname -m)"
[[ "$ARCH" == "aarch64" ]] && c_info "ARM64 detected (Ampere A1) — image is multi-arch, fine."

TOTAL_MB=$(free -m | awk '/^Mem:/{print $2}')
c_info "memory: ${TOTAL_MB}MB"
if (( TOTAL_MB < 1800 )); then
  c_warn "under ~2GB. Use the AMD micro shape only for testing; prefer Ampere A1."
fi

# ------------------------------------------------------------ swap (small hosts)
if (( TOTAL_MB < 4096 )) && ! swapon --show | grep -q .; then
  c_info "adding 2GB swap (guards against OOM during image pulls)"
  sudo fallocate -l 2G /swapfile 2>/dev/null || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
  c_ok "swap enabled"
fi

# ---------------------------------------------------------------- docker
if ! command -v docker >/dev/null 2>&1; then
  c_info "installing Docker…"
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  NEEDS_RELOGIN=1
  c_ok "Docker installed"
else
  c_ok "Docker present: $(docker --version)"
fi

sudo systemctl enable --now docker >/dev/null 2>&1 || true

if ! docker compose version >/dev/null 2>&1; then
  c_info "installing compose plugin…"
  sudo apt-get update -qq && sudo apt-get install -y docker-compose-plugin \
    || sudo dnf install -y docker-compose-plugin \
    || c_warn "install docker-compose-plugin manually"
fi

# ------------------------------------------------------------- firewall
# Oracle images ship a REJECT-all iptables policy that silently breaks
# published ports. Nothing here needs inbound access except SSH, so we only
# make sure we are not locking ourselves out.
if command -v firewall-cmd >/dev/null 2>&1; then
  sudo firewall-cmd --permanent --add-service=ssh >/dev/null 2>&1 || true
  sudo firewall-cmd --reload >/dev/null 2>&1 || true
  c_ok "firewalld: ssh allowed"
elif command -v ufw >/dev/null 2>&1 && sudo ufw status | grep -q inactive; then
  c_info "ufw inactive — leaving as is (metrics bind to 127.0.0.1 only)"
fi

# ------------------------------------------------------------ layout
c_info "preparing ${CADB_DIR}"
mkdir -p "$CADB_DIR"
cd "$CADB_DIR"

RAW="https://raw.githubusercontent.com/${REPO}/main"

if [[ ! -f docker-compose.yml ]]; then
  curl -fsSL "${RAW}/deploy/docker-compose.prod.yml" -o docker-compose.yml
  # Point the image at this repo (lowercased — GHCR rejects uppercase paths).
  IMAGE_PATH="$(echo "$REPO" | tr '[:upper:]' '[:lower:]')"
  sed -i "s|ghcr.io/OWNER/REPO|ghcr.io/${IMAGE_PATH}|g" docker-compose.yml
  c_ok "docker-compose.yml written (ghcr.io/${IMAGE_PATH}:latest)"
else
  c_ok "docker-compose.yml already present — left untouched"
fi

if [[ ! -f config.yaml ]]; then
  curl -fsSL "${RAW}/config.yaml" -o config.yaml
  c_ok "config.yaml fetched"
else
  c_ok "config.yaml already present — left untouched"
fi

if [[ ! -f .env ]]; then
  curl -fsSL "${RAW}/.env.example" -o .env
  chmod 600 .env
  c_warn ".env created from the example — YOU MUST EDIT IT"
else
  chmod 600 .env
  c_ok ".env already present"
fi

# --------------------------------------------------- deploy key for Actions
KEY="${HOME}/.ssh/cadb_deploy"
if [[ ! -f "$KEY" ]]; then
  c_info "generating a deploy keypair for GitHub Actions"
  mkdir -p "${HOME}/.ssh" && chmod 700 "${HOME}/.ssh"
  ssh-keygen -t ed25519 -N "" -C "cadb-deploy@$(hostname)" -f "$KEY" >/dev/null
  cat "${KEY}.pub" >> "${HOME}/.ssh/authorized_keys"
  chmod 600 "${HOME}/.ssh/authorized_keys"
  c_ok "deploy key created and authorised"
fi

PUBLIC_IP="$(curl -fsSL --max-time 8 https://api.ipify.org 2>/dev/null || echo '<your-server-ip>')"

echo
echo "════════════════════════════════════════════════════════════"
echo "  NEXT STEPS"
echo "════════════════════════════════════════════════════════════"
echo
echo "1️⃣  Edit your credentials:"
echo "      nano ${CADB_DIR}/.env"
echo "    (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are the only required ones)"
echo
echo "2️⃣  Add these repository secrets at:"
echo "      https://github.com/${REPO}/settings/secrets/actions"
echo
echo "    ORACLE_HOST     ${PUBLIC_IP}"
echo "    ORACLE_USER     ${USER}"
echo "    ORACLE_SSH_KEY  (the private key printed below — include both BEGIN/END lines)"
echo
echo "    Optional, for deploy notifications:"
echo "    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID"
echo
echo "──────────────── private key (copy all of it) ────────────────"
cat "$KEY"
echo "──────────────────────────────────────────────────────────────"
echo
echo "3️⃣  First run (pulls the image built by CI):"
echo "      cd ${CADB_DIR} && docker compose pull && docker compose up -d"
echo "      docker compose logs -f cadb"
echo
echo "    From then on, every push to main redeploys automatically."
echo
[[ -n "${NEEDS_RELOGIN:-}" ]] && c_warn "log out and back in first, so your user picks up the docker group"
c_ok "provisioning complete"
