"""Layered configuration: defaults -> YAML file -> environment variables.

Secrets never live in the YAML file. Any string of the form ``${ENV_VAR}`` is
expanded from the environment at load time, and the well-known credential fields
are also readable directly from ``CADB_*`` variables.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = ["Settings", "load_settings", "RETIRED_ENDPOINTS"]

# Endpoints removed from the defaults because they are dead or cannot serve
# eth_getLogs. A bind-mounted config.yaml survives `docker compose build`, so a
# stale file keeps pointing at these long after the image is updated — we detect
# that explicitly rather than letting the user chase phantom RPC errors.
RETIRED_ENDPOINTS: dict[str, str] = {
    "llamarpc.com": "returns HTTP 521 (dead)",
    "bsc-dataseed": "does not support eth_getLogs (-32005 on any range)",
    "rpc.ankr.com": "now requires an API key",
    "cloudflare-eth.com": "returns -32046 for eth_getLogs",
    "meowrpc.com": "eth_getLogs not supported",
    "bsc-dataseed1.ninicoin.io": "does not support eth_getLogs",
    "bsc-dataseed.bnbchain.org": "does not support eth_getLogs",
    "endpoints.omniatech.io": "returns HTTP 521",
    "blockpi.network": "returns HTTP 521",
}

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _clean_endpoint(url: str) -> str:
    """Normalise a URL that came from an env var or YAML.

    Docker's ``env_file`` parser is not a shell: it does **not** strip surrounding
    quotes, trailing whitespace or inline ``#`` comments. A line like
    ``SOLANA_RPC_URL="https://..."`` therefore yields a literal leading quote,
    and the request fails with a URL-encoded ``%22`` host — which surfaces as a
    generic connection error and looks identical to the endpoint being down.
    That cost real debugging time, so we sanitise instead of trusting the input.
    """
    if not isinstance(url, str):
        return url
    cleaned = url.strip()
    # Strip an inline comment, but only when it is clearly separated, so we
    # never truncate a legitimate URL fragment.
    for sep in ("  #", "\t#"):
        if sep in cleaned:
            cleaned = cleaned.split(sep, 1)[0].rstrip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in "\"'":
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _clean_endpoint_list(value: str) -> str:
    """Sanitise a comma-separated endpoint list, dropping empty entries."""
    if not isinstance(value, str):
        return value
    parts = [_clean_endpoint(p) for p in value.split(",")]
    return ",".join(p for p in parts if p)


def _expand(value: Any) -> Any:
    """Recursively expand ``${VAR}`` / ``${VAR:-default}`` references.

    ``${VAR:-default}`` follows POSIX *colon-dash* semantics: the default is used
    when the variable is unset **or set-but-empty**. This matters enormously in
    practice — a ``.env`` file containing a blank placeholder line like
    ``ETH_RPC_URL=`` defines the variable as an empty string, and treating that
    as a deliberate override silently disabled the entire on-chain module while
    it still reported itself healthy. An empty value means "not configured".
    """
    if isinstance(value, str):
        def sub(m: re.Match[str]) -> str:
            var, default = m.group(1), m.group(2)
            env_value = os.getenv(var)
            if env_value:
                return env_value
            # unset, or set-but-empty -> fall back to the default (if any)
            return default or ""

        return _ENV_PATTERN.sub(sub, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


class BusConfig(BaseModel):
    kind: str = Field(default="memory", description="memory | redis")
    url: str = "redis://localhost:6379/0"
    queue_size: int = 10_000


class ExchangeConfig(BaseModel):
    enabled: bool = True
    exchanges: list[str] = Field(
        default_factory=lambda: ["binance", "bybit", "mexc", "gate", "kucoin", "coinbase"]
    )
    symbols: list[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    orderbook_depth: int = 50
    volume_window_s: int = 300          # 5-minute rolling volume window
    volume_bucket_s: int = 5            # bucket size for volume aggregation
    volume_z_threshold: float = 3.0     # V > mu + 3*sigma
    obi_threshold: float = 0.35         # |OBI| beyond this is a lopsided book
    obi_depth_levels: int = 20          # levels aggregated for depth
    cvd_window_s: int = 300
    cvd_z_threshold: float = 2.5
    use_ccxt_pro: bool = True
    simulate: bool = False              # deterministic synthetic feed (demo/tests)

    # --- dynamic symbol discovery ---
    # A static symbol list cannot see manipulation in assets you did not think
    # to list — which is where it overwhelmingly happens. When enabled, each
    # venue is periodically ranked and the most suspicious pairs are subscribed
    # automatically alongside `symbols`.
    discovery_enabled: bool = True
    discovery_interval_s: int = 300     # rescan cadence
    discovery_max_symbols: int = 20     # per venue, on top of `symbols`
    discovery_min_volume_usd: float = 100_000.0   # below this is noise
    discovery_max_volume_usd: float = 50_000_000.0  # above this is not cheaply moved
    discovery_min_change_pct: float = 15.0        # |24h move| to qualify
    discovery_volume_surge: float = 3.0           # x median volume to qualify


class OnChainConfig(BaseModel):
    # validate_default so ${ENV} placeholders in defaults are expanded too.
    model_config = ConfigDict(validate_default=True)

    enabled: bool = True
    evm_rpc: dict[str, str] = Field(
        default_factory=lambda: {
            # Verified-working public defaults with failover, benchmarked
            # against the exact eth_getLogs call Module 2 makes.
            # Dropped: llamarpc (HTTP 521), Ankr (auth), Cloudflare (-32046),
            # drpc (429), meowrpc + bsc-dataseed* (no eth_getLogs support at
            # all — they reject even a 5-block range with -32005).
            #
            # BSC order is by *sustained* throughput, not one-off latency:
            # under 12 rounds of continuous 20-block polling, publicnode failed
            # 8/12 despite passing single probes, while nodereal (116ms median,
            # 12/12) and blockrazor (217ms, 12/12) held up. Endpoints that look
            # healthy on a single request but collapse under a real poll loop
            # are the worst kind of default — they fail only in production.
            "ethereum": "${ETH_RPC_URL:-https://ethereum-rpc.publicnode.com,https://1rpc.io/eth,https://eth.drpc.org}",
            "bsc": "${BSC_RPC_URL:-https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3,https://bsc.blockrazor.xyz,https://bsc-rpc.publicnode.com}",
        }
    )
    solana_rpc: str = "${SOLANA_RPC_URL:-https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com}"
    poll_interval_s: float = 3.0
    whale_threshold_usd: float = 500_000.0
    liquidity_drop_pct: float = 30.0
    bridge_threshold_usd: float = 1_000_000.0
    tracked_tokens: list[dict[str, Any]] = Field(default_factory=list)
    cex_wallets: dict[str, str] = Field(default_factory=dict)
    bridge_contracts: dict[str, str] = Field(default_factory=dict)
    dex_pools: list[dict[str, Any]] = Field(default_factory=list)
    simulate: bool = False

    @field_validator("evm_rpc", "solana_rpc", mode="after")
    @classmethod
    def _resolve_rpc(cls, v):
        """Expand ${ENV} placeholders and sanitise the resulting URLs."""
        if isinstance(v, str):
            return _clean_endpoint_list(_expand(v) if "${" in v else v)
        if isinstance(v, dict):
            return {
                k: _clean_endpoint_list(_expand(u) if isinstance(u, str) and "${" in u else u)
                for k, u in v.items()
            }
        return v


class SocialConfig(BaseModel):
    model_config = ConfigDict(validate_default=True)

    enabled: bool = True
    x_bearer_token: str = "${X_BEARER_TOKEN:-}"
    telegram_api_id: str = "${TELEGRAM_API_ID:-}"
    telegram_api_hash: str = "${TELEGRAM_API_HASH:-}"
    telegram_channels: list[str] = Field(default_factory=list)
    tracked_tickers: list[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    finbert_model: str = "ProsusAI/finbert"
    use_finbert: bool = True
    sentiment_batch_size: int = 16
    mention_window_s: int = 300
    mention_z_threshold: float = 3.0
    bot_farm_min_posts: int = 12
    bot_account_age_days: float = 30.0
    bot_age_variance_threshold: float = 0.35  # low CV of account age => farm
    poll_interval_s: float = 15.0
    simulate: bool = False

    @field_validator("x_bearer_token", "telegram_api_id", "telegram_api_hash", mode="after")
    @classmethod
    def _resolve_creds(cls, v: str) -> str:
        """Same placeholder-expansion guard as AlertConfig (see note there)."""
        return _expand(v) if isinstance(v, str) and "${" in v else v


class MLConfig(BaseModel):
    enabled: bool = True
    model_path: str = "models/isolation_forest.joblib"
    contamination: float = 0.02
    n_estimators: int = 200
    max_samples: str | int = "auto"
    random_state: int = 42
    min_training_samples: int = 200
    retrain_interval_s: int = 3600
    online_training: bool = True
    feature_ttl_s: int = 600            # how long a module's feature stays fresh
    score_interval_ms: int = 250        # scoring cadence per asset
    alert_threshold: float = 80.0       # composite score > 80 fires an alert
    weights: dict[str, float] = Field(
        default_factory=lambda: {
            "exchange": 0.40,
            "onchain": 0.35,
            "social": 0.25,
        }
    )
    ml_blend: float = 0.5               # 0 = pure rules, 1 = pure IsolationForest


class AlertConfig(BaseModel):
    model_config = ConfigDict(validate_default=True)

    telegram_bot_token: str = "${TELEGRAM_BOT_TOKEN:-}"
    telegram_chat_id: str = "${TELEGRAM_CHAT_ID:-}"
    discord_webhook_url: str = "${DISCORD_WEBHOOK_URL:-}"
    generic_webhooks: list[str] = Field(default_factory=list)
    min_score: float = 80.0
    cooldown_s: int = 300               # per asset+severity de-duplication
    max_alerts_per_min: int = 20
    dry_run: bool = False               # log instead of sending
    include_chart: bool = False

    @field_validator(
        "telegram_bot_token", "telegram_chat_id", "discord_webhook_url", mode="after"
    )
    @classmethod
    def _resolve_placeholder(cls, v: str) -> str:
        """Expand ``${VAR}`` refs even when the model is built directly.

        ``load_settings`` expands these during load, but a caller constructing
        ``AlertConfig()`` in code would otherwise keep the literal placeholder —
        and a string like ``"${DISCORD_WEBHOOK_URL:-}"`` is truthy, so the router
        would register a sink pointing at a nonsense URL and every alert would
        fail delivery. Resolving here makes the default safe in both paths.
        """
        return _expand(v) if isinstance(v, str) and "${" in v else v


class TelemetryConfig(BaseModel):
    log_level: str = "INFO"
    json_logs: bool = False
    metrics_port: int = 0               # 0 disables the HTTP metrics endpoint
    latency_budget_ms: float = 200.0
    health_interval_s: int = 60
    state_file: str = "state/runtime.json"


class Settings(BaseModel):
    """Root configuration object."""

    bus: BusConfig = Field(default_factory=BusConfig)
    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    onchain: OnChainConfig = Field(default_factory=OnChainConfig)
    social: SocialConfig = Field(default_factory=SocialConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)

    @field_validator("exchange")
    @classmethod
    def _upper_symbols(cls, v: ExchangeConfig) -> ExchangeConfig:
        v.symbols = [s.strip().upper() for s in v.symbols]
        v.exchanges = [e.strip().lower() for e in v.exchanges]
        return v

    # ---- convenience -------------------------------------------------
    @property
    def all_symbols(self) -> list[str]:
        return self.exchange.symbols

    def tracked_assets(self) -> list[str]:
        assets = {s.split("/")[0] for s in self.exchange.symbols}
        assets.update(t.upper() for t in self.social.tracked_tickers)
        assets.update(str(t.get("symbol", "")).upper() for t in self.onchain.tracked_tokens)
        return sorted(a for a in assets if a)


_ENV_OVERRIDES: dict[str, tuple[str, ...]] = {
    "CADB_TELEGRAM_BOT_TOKEN": ("alerts", "telegram_bot_token"),
    "CADB_TELEGRAM_CHAT_ID": ("alerts", "telegram_chat_id"),
    "CADB_DISCORD_WEBHOOK": ("alerts", "discord_webhook_url"),
    "CADB_X_BEARER_TOKEN": ("social", "x_bearer_token"),
    "CADB_ETH_RPC": ("onchain", "evm_rpc", "ethereum"),
    "CADB_SOLANA_RPC": ("onchain", "solana_rpc"),
    "CADB_REDIS_URL": ("bus", "url"),
    "CADB_LOG_LEVEL": ("telemetry", "log_level"),
    "CADB_ALERT_THRESHOLD": ("ml", "alert_threshold"),
}


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    for env_key, path in _ENV_OVERRIDES.items():
        raw = os.getenv(env_key)
        # Blank overrides are placeholders, not intent — ignore them so an
        # empty line in .env cannot wipe out a working default.
        if not raw:
            continue
        node: Any = data
        for part in path[:-1]:
            node = node.setdefault(part, {})
        leaf = path[-1]
        existing = node.get(leaf)
        if isinstance(existing, (int, float)) and not isinstance(existing, bool):
            try:
                node[leaf] = type(existing)(raw)
                continue
            except ValueError:
                pass
        node[leaf] = raw
    return data


def load_settings(path: str | Path | None = None) -> Settings:
    """Load configuration from YAML (optional) with env expansion + overrides."""
    data: dict[str, Any] = {}
    if path:
        p = Path(path)
        if p.exists():
            loaded = yaml.safe_load(p.read_text()) or {}
            if not isinstance(loaded, dict):
                raise ValueError(f"config root must be a mapping: {p}")
            data = loaded
    data = _expand(data)
    data = _apply_env_overrides(data)
    settings = Settings.model_validate(data)
    # Expand env refs that came from *defaults* (not present in the YAML).
    settings = Settings.model_validate(_expand(settings.model_dump()))
    _warn_retired_endpoints(settings, path)
    return settings


def _warn_retired_endpoints(settings: Settings, path: str | Path | None) -> None:
    """Log a clear warning when config still points at a retired endpoint."""
    import logging

    log = logging.getLogger(__name__)
    # Map each chain to the env var that can override it, so the warning can
    # name the *actual* source. Blaming config.yaml when the value came from an
    # environment variable sends people to edit a file that is already correct.
    env_for_chain = {
        "ethereum": "ETH_RPC_URL",
        "bsc": "BSC_RPC_URL",
        "polygon": "POLYGON_RPC_URL",
    }
    targets: list[tuple[str, str, str | None]] = [
        (chain, url, env_for_chain.get(chain))
        for chain, url in settings.onchain.evm_rpc.items()
    ]
    targets.append(("solana", settings.onchain.solana_rpc, "SOLANA_RPC_URL"))

    for chain, url, env_var in targets:
        for bad, reason in RETIRED_ENDPOINTS.items():
            if bad not in (url or ""):
                continue
            from_env = bool(env_var and os.getenv(env_var))
            if from_env:
                log.warning(
                    "%s is using retired RPC endpoint '%s' (%s). "
                    "This came from the %s environment variable, not %s — "
                    "edit or remove that line in your .env and restart.",
                    chain, bad, reason, env_var, path or "config.yaml",
                )
            else:
                log.warning(
                    "%s is using retired RPC endpoint '%s' (%s). "
                    "Your %s is out of date — if it is bind-mounted, rebuilding "
                    "the image does NOT update it. Run: ./deploy/update.sh --config",
                    chain, bad, reason, path or "config.yaml",
                )
