#!/usr/bin/env bash
# Update a running CADB deployment.
#
#   ./deploy/update.sh              # pull image + restart
#   ./deploy/update.sh --config     # ALSO refresh config.yaml from the repo
#
# Why --config exists: docker-compose bind-mounts ~/cadb/config.yaml over the
# copy inside the image. A `docker compose build` therefore does NOT update it,
# so fixes to defaults (e.g. replacing a dead RPC endpoint) silently do not
# apply. This flag backs up your file and installs the current one.

set -euo pipefail
cd "$(dirname "$0")/.." 2>/dev/null || cd "${HOME}/cadb"

REFRESH_CONFIG=0
[[ "${1:-}" == "--config" ]] && REFRESH_CONFIG=1

c_ok()   { printf '\033[32m✅ %s\033[0m\n' "$*"; }
c_info() { printf '\033[36m→  %s\033[0m\n' "$*"; }
c_warn() { printf '\033[33m⚠️  %s\033[0m\n' "$*"; }

if [[ -d .git ]]; then
  c_info "pulling latest source"
  git pull --ff-only
fi

if (( REFRESH_CONFIG )) && [[ -f config.yaml ]]; then
  STAMP="config.yaml.bak.$(date +%Y%m%d-%H%M%S)"
  cp config.yaml "$STAMP"
  c_ok "backed up existing config -> ${STAMP}"
  if [[ -f "$(dirname "$0")/../config.yaml" ]] && [[ "$(dirname "$0")/../config.yaml" != "./config.yaml" ]]; then
    cp "$(dirname "$0")/../config.yaml" config.yaml
  else
    curl -fsSL "https://raw.githubusercontent.com/dzdozy01-cloud/crypto-anomaly-bot/main/config.yaml" -o config.yaml
  fi
  c_ok "config.yaml refreshed — re-apply any custom edits from ${STAMP}"
fi

# Warn about a stale config even when not refreshing.
if grep -q "llamarpc\|bsc-dataseed" config.yaml 2>/dev/null; then
  c_warn "config.yaml references retired RPC endpoints (llamarpc / bsc-dataseed)."
  c_warn "These do not support eth_getLogs. Re-run with --config to update."
fi

if [[ -f docker-compose.yml ]] && grep -q "build:" docker-compose.yml; then
  c_info "building image locally"
  docker compose build
else
  c_info "pulling image"
  docker compose pull
fi

docker compose up -d --remove-orphans
c_ok "restarted"

echo
c_info "waiting for health…"
for i in $(seq 1 24); do
  sleep 5
  if docker compose logs --tail=200 cadb 2>&1 | grep -q "system online"; then
    c_ok "healthy after $((i*5))s"
    break
  fi
  STATE=$(docker inspect --format='{{.State.Status}}' cadb 2>/dev/null \
        || docker compose ps -q cadb | xargs -r docker inspect --format='{{.State.Status}}' 2>/dev/null \
        || echo missing)
  if [[ "$STATE" != "running" ]]; then
    printf '\033[31m❌ container state=%s\033[0m\n' "$STATE"
    docker compose logs --tail=50 cadb
    exit 1
  fi
done

echo
docker compose exec -T cadb cadb validate -c config.yaml 2>/dev/null || true
echo
c_ok "update complete — send /whoami to your bot to verify chat routing"
