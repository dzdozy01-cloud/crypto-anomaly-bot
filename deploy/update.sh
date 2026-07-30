#!/usr/bin/env bash
# Update a running CADB deployment.
#
#   bash deploy/update.sh           # always works, regardless of file mode
#   ./deploy/update.sh              # rebuild/pull + force-recreate + verify
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

  # Self-heal the executable bit. The upstream repo is edited by tooling that
  # restores files without it, and git honours the on-disk mode by default, so
  # a routine `git add -A` can silently commit 100755 -> 100644 and every clone
  # then fails with "Permission denied". Repair it locally so the next
  # ./deploy/update.sh works even if upstream regresses again.
  git config core.fileMode false 2>/dev/null || true
  for f in deploy/*.sh; do
    [[ -f "$f" && ! -x "$f" ]] && chmod +x "$f" && c_warn "restored exec bit on $f"
  done
fi

if (( REFRESH_CONFIG )) && [[ -f config.yaml ]]; then
  # Two supported layouts, and they need opposite handling:
  #   * git clone   — config.yaml is tracked, so `git pull` already updated it.
  #                   Copying the repo file over itself is a no-op at best and
  #                   `cp: same file` at worst.
  #   * ~/cadb      — laid out by bootstrap.sh via curl; nothing updates the
  #                   file automatically, so fetch it from GitHub.
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1 \
     && git ls-files --error-unmatch config.yaml >/dev/null 2>&1; then
    if ! git diff --quiet -- config.yaml; then
      STAMP="config.yaml.bak.$(date +%Y%m%d-%H%M%S)"
      cp config.yaml "$STAMP"
      c_warn "local edits to config.yaml backed up -> ${STAMP}"
      git checkout -- config.yaml
      c_ok "config.yaml reset to the repo version (re-apply edits from ${STAMP})"
    else
      c_ok "config.yaml is tracked by git and already current (updated by git pull)"
    fi
  else
    STAMP="config.yaml.bak.$(date +%Y%m%d-%H%M%S)"
    cp config.yaml "$STAMP"
    c_ok "backed up existing config -> ${STAMP}"
    curl -fsSL "https://raw.githubusercontent.com/dzdozy01-cloud/crypto-anomaly-bot/main/config.yaml" \
      -o config.yaml
    c_ok "config.yaml refreshed from GitHub (re-apply edits from ${STAMP})"
  fi
fi

# Warn about a stale config even when not refreshing.
if grep -q "llamarpc\|bsc-dataseed" config.yaml 2>/dev/null; then
  c_warn "config.yaml references retired RPC endpoints (llamarpc / bsc-dataseed)."
  c_warn "These do not support eth_getLogs. Re-run with --config to update."
fi

if [[ -f docker-compose.yml ]] && grep -q "build:" docker-compose.yml; then
  c_info "building image locally (source changed)"
  docker compose build
else
  c_info "pulling image"
  docker compose pull
fi

# --force-recreate is required. `docker compose up -d` compares the *compose
# file*, not the image contents, so after a rebuild that only changed Python
# source it reports "Running" and leaves the old container in place — the new
# code never runs, which is indistinguishable from the fix not working.
docker compose up -d --remove-orphans --force-recreate
c_ok "restarted"

echo
# Resolve the real container id. Compose prefixes names with the project
# directory (crypto-anomaly-bot-cadb-1), so `docker inspect cadb` fails and the
# old `||` fallback chain concatenated both branches into a multi-line value —
# which then never equalled "running" and reported a false failure on a
# perfectly healthy container.
container_state() {
  local cid
  cid=$(docker compose ps -q cadb 2>/dev/null | head -n1)
  if [[ -z "$cid" ]]; then
    echo "missing"
    return
  fi
  docker inspect --format='{{.State.Status}}' "$cid" 2>/dev/null | head -n1 || echo unknown
}

c_info "waiting for health…"
HEALTHY=0
for i in $(seq 1 24); do
  sleep 5
  if docker compose logs --tail=200 cadb 2>&1 | grep -q "system online"; then
    c_ok "healthy after $((i*5))s"
    HEALTHY=1
    break
  fi
  STATE="$(container_state)"
  if [[ "$STATE" != "running" ]]; then
    printf '\033[31m❌ container state=%s\033[0m\n' "$STATE"
    docker compose logs --tail=50 cadb
    exit 1
  fi
done

if (( ! HEALTHY )); then
  c_warn "no 'system online' within 120s — container is running; check logs"
  docker compose logs --tail=30 cadb
fi

echo
docker compose exec -T cadb cadb validate -c config.yaml 2>/dev/null || true
echo
c_ok "update complete — send /whoami to your bot to verify chat routing"
