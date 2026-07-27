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

__all__ = ["Settings", "load_settings"]

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _expand(value: Any) -> Any:
    """Recursively expand ``${VAR}`` / ``${VAR:-default}`` references."""
    if isinstance(value, str):
        def sub(m: re.Match[str]) -> str:
            return os.getenv(m.group(1), m.group(2) or "")

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
    exchanges: list[str] = Field(default_factory=lambda: ["binance", "bybit", "mexc"])
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


class OnChainConfig(BaseModel):
    # validate_default so ${ENV} placeholders in defaults are expanded too.
    model_config = ConfigDict(validate_default=True)

    enabled: bool = True
    evm_rpc: dict[str, str] = Field(
        default_factory=lambda: {
            "ethereum": "${ETH_RPC_URL:-https://eth.llamarpc.com}",
            "bsc": "${BSC_RPC_URL:-https://binance.llamarpc.com}",
        }
    )
    solana_rpc: str = "${SOLANA_RPC_URL:-https://api.mainnet-beta.solana.com}"
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
        """Same placeholder-expansion guard as AlertConfig (see note there)."""
        if isinstance(v, str):
            return _expand(v) if "${" in v else v
        if isinstance(v, dict):
            return {k: (_expand(u) if isinstance(u, str) and "${" in u else u)
                    for k, u in v.items()}
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
        if raw is None:
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
    return Settings.model_validate(_expand(settings.model_dump()))
