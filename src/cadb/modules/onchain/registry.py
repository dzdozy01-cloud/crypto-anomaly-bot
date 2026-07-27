"""Known-address registry: CEX hot wallets, bridges, DEX pools, token metadata.

Ships with a curated default set of mainnet addresses so the tracker is useful
out of the box; everything is overridable from config. Addresses are stored
lower-cased and looked up case-insensitively.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = ["AddressRegistry", "TokenMeta", "PoolMeta", "DEFAULT_CEX_WALLETS", "DEFAULT_TOKENS"]

Direction = Literal["inflow", "outflow", "internal", "unrelated"]


# --- Curated mainnet CEX hot wallets (label -> address) --------------------
DEFAULT_CEX_WALLETS: dict[str, dict[str, str]] = {
    "ethereum": {
        "0x28c6c06298d514db089934071355e5743bf21d60": "binance_14",
        "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "binance_15",
        "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance_16",
        "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "binance_17",
        "0x9696f59e4d72e237be84ffd425dcad154bf96976": "binance_18",
        "0x4976a4a02f38326660d17bf34b431dc6e2eb2327": "binance_19",
        "0xf977814e90da44bfa03b6295a0616a897441acec": "binance_cold_8",
        "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": "coinbase_10",
        "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase_1",
        "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase_2",
        "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase_3",
        "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "bybit_hot",
        "0xa7efae728d2936e78bda97dc267687568dd593f3": "okx_hot",
        "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "gate_hot",
        "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c": "gate_2",
        "0x0211f3cedbef3143223d3acf0e589747933e8527": "mexc_hot",
        "0x3cc936b795a188f0e246cbb2d74c5bd190aecf18": "mexc_2",
        "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "mexc_3",
        "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin_hot",
        "0x46340b20830761efd32832a74d7169b29feb9758": "crypto_com",
    },
    "bsc": {
        "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "binance_bsc_hot",
        "0xe2fc31f816a9b94326492132018c3aecc4a93ae1": "binance_bsc_2",
        "0x4982085c9e2f89f2ecb8131eca71afad896e89cb": "mexc_bsc",
    },
    "solana": {
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "binance_sol_hot",
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "binance_sol_2",
        "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "coinbase_sol",
        "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "bybit_sol",
        "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ": "mexc_sol",
    },
}

# --- Bridge contracts worth watching ---------------------------------------
DEFAULT_BRIDGES: dict[str, dict[str, str]] = {
    "ethereum": {
        "0x8484ef722627bf18ca5ae6bcf031c23e6e922b30": "polygon_pos_erc20",
        "0xa0c68c638235ee32657e8f720a23cec1bfc77c77": "polygon_pos_plasma",
        "0x3ee18b2214aff97000d974cf647e7c347e8fa585": "wormhole_portal",
        "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1": "optimism_gateway",
        "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a": "arbitrum_bridge",
        "0x3154cf16ccdb4c6d922629664174b904d80f2c35": "base_bridge",
        "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": "polygon_zkevm",
        "0xd19d4b5d358258f05d7b411e21a1460d11b0876f": "linea_bridge",
    },
    "bsc": {
        "0xb6f6d86a8f9879a9c87f643768d9efc38c1da6e7": "stargate_bsc",
    },
}

# --- Token metadata --------------------------------------------------------
DEFAULT_TOKENS: list[dict[str, Any]] = [
    {"chain": "ethereum", "symbol": "USDT", "address": "0xdac17f958d2ee523a2206206994597c13d831ec7", "decimals": 6, "price_usd": 1.0, "stable": True},
    {"chain": "ethereum", "symbol": "USDC", "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "decimals": 6, "price_usd": 1.0, "stable": True},
    {"chain": "ethereum", "symbol": "WETH", "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "decimals": 18},
    {"chain": "ethereum", "symbol": "WBTC", "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "decimals": 8},
    {"chain": "ethereum", "symbol": "PEPE", "address": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "decimals": 18},
    {"chain": "ethereum", "symbol": "SHIB", "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", "decimals": 18},
    {"chain": "ethereum", "symbol": "LINK", "address": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18},
    {"chain": "ethereum", "symbol": "ARB", "address": "0xb50721bcf8d664c30412cfbc6cf7a15145234ad1", "decimals": 18},
    {"chain": "bsc", "symbol": "USDT", "address": "0x55d398326f99059ff775485246999027b3197955", "decimals": 18, "price_usd": 1.0, "stable": True},
    {"chain": "solana", "symbol": "USDC", "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "decimals": 6, "price_usd": 1.0, "stable": True},
    {"chain": "solana", "symbol": "USDT", "address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "decimals": 6, "price_usd": 1.0, "stable": True},
    {"chain": "solana", "symbol": "BONK", "address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "decimals": 5},
    {"chain": "solana", "symbol": "WIF", "address": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "decimals": 6},
]

# --- Major DEX pools (UniswapV2-compatible reserve reads) ------------------
DEFAULT_POOLS: list[dict[str, Any]] = [
    {"chain": "ethereum", "name": "PEPE/WETH", "address": "0xa43fe16908251ee70ef74718545e4fe6c5ccec9f", "token0": "PEPE", "token1": "WETH", "decimals0": 18, "decimals1": 18},
    {"chain": "ethereum", "name": "SHIB/WETH", "address": "0x811beed0119b4afce20d2583eb608c6f7af1954f", "token0": "SHIB", "token1": "WETH", "decimals0": 18, "decimals1": 18},
    {"chain": "ethereum", "name": "USDC/WETH", "address": "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "token0": "USDC", "token1": "WETH", "decimals0": 6, "decimals1": 18},
]


@dataclass
class TokenMeta:
    chain: str
    symbol: str
    address: str
    decimals: int = 18
    price_usd: float = 0.0
    stable: bool = False

    def to_usd(self, amount: float) -> float:
        return amount * self.price_usd

    def from_raw(self, raw: int) -> float:
        return raw / (10**self.decimals)


@dataclass
class PoolMeta:
    chain: str
    name: str
    address: str
    token0: str = ""
    token1: str = ""
    decimals0: int = 18
    decimals1: int = 18
    last_reserve0: float = 0.0
    last_reserve1: float = 0.0
    last_block: int = 0
    last_tvl_usd: float = 0.0


@dataclass
class AddressRegistry:
    """Case-insensitive lookup of labelled on-chain addresses."""

    cex_wallets: dict[str, str] = field(default_factory=dict)   # address -> label
    bridges: dict[str, str] = field(default_factory=dict)
    tokens: dict[str, TokenMeta] = field(default_factory=dict)  # "chain:address" -> meta
    tokens_by_symbol: dict[str, TokenMeta] = field(default_factory=dict)
    pools: dict[str, PoolMeta] = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        chains: list[str] | None = None,
        extra_wallets: dict[str, str] | None = None,
        extra_tokens: list[dict[str, Any]] | None = None,
        extra_pools: list[dict[str, Any]] | None = None,
        extra_bridges: dict[str, str] | None = None,
    ) -> AddressRegistry:
        reg = cls()
        chains = chains or ["ethereum", "bsc", "solana"]

        for chain in chains:
            for addr, label in DEFAULT_CEX_WALLETS.get(chain, {}).items():
                reg.cex_wallets[reg._key(addr)] = label
            for addr, label in DEFAULT_BRIDGES.get(chain, {}).items():
                reg.bridges[reg._key(addr)] = label

        for tok in DEFAULT_TOKENS + list(extra_tokens or []):
            if tok.get("chain") not in chains and extra_tokens is None:
                continue
            reg.add_token(tok)

        for pool in DEFAULT_POOLS + list(extra_pools or []):
            meta = PoolMeta(
                chain=pool.get("chain", "ethereum"),
                name=pool.get("name", pool.get("address", "")[:10]),
                address=pool["address"].lower(),
                token0=pool.get("token0", ""),
                token1=pool.get("token1", ""),
                decimals0=int(pool.get("decimals0", 18)),
                decimals1=int(pool.get("decimals1", 18)),
            )
            reg.pools[meta.address] = meta

        for addr, label in (extra_wallets or {}).items():
            reg.cex_wallets[reg._key(addr)] = label
        for addr, label in (extra_bridges or {}).items():
            reg.bridges[reg._key(addr)] = label
        return reg

    @staticmethod
    def _key(address: str) -> str:
        """Normalise an address for lookup.

        EVM addresses are case-insensitive (EIP-55 checksums are presentational
        only), so they are lower-cased — note the prefix test must itself be
        case-insensitive, otherwise a checksummed ``0XAB…`` address would be
        treated as base58 and silently fail to match a known hot wallet.
        Solana addresses are case-sensitive base58 and pass through unchanged.
        """
        a = address.strip()
        return a.lower() if a[:2].lower() == "0x" else a

    def add_token(self, spec: dict[str, Any]) -> TokenMeta:
        meta = TokenMeta(
            chain=spec.get("chain", "ethereum"),
            symbol=str(spec.get("symbol", "?")).upper(),
            address=self._key(str(spec.get("address", ""))),
            decimals=int(spec.get("decimals", 18)),
            price_usd=float(spec.get("price_usd", 0.0)),
            stable=bool(spec.get("stable", False)),
        )
        self.tokens[f"{meta.chain}:{meta.address}"] = meta
        self.tokens_by_symbol.setdefault(meta.symbol, meta)
        return meta

    # ---- queries -------------------------------------------------------
    def token(self, chain: str, address: str) -> TokenMeta | None:
        return self.tokens.get(f"{chain}:{self._key(address)}")

    def is_cex(self, address: str) -> bool:
        return self._key(address) in self.cex_wallets

    def cex_label(self, address: str) -> str | None:
        return self.cex_wallets.get(self._key(address))

    def is_bridge(self, address: str) -> bool:
        return self._key(address) in self.bridges

    def bridge_label(self, address: str) -> str | None:
        return self.bridges.get(self._key(address))

    def classify(self, from_addr: str, to_addr: str) -> Direction:
        """Direction of a transfer relative to CEX hot wallets."""
        f, t = self.is_cex(from_addr), self.is_cex(to_addr)
        if f and t:
            return "internal"
        if t:
            return "inflow"   # deposit to exchange -> potential sell pressure
        if f:
            return "outflow"  # withdrawal -> accumulation / self-custody
        return "unrelated"

    def watched_addresses(self, chain: str | None = None) -> list[str]:
        return list(self.cex_wallets) + list(self.bridges)

    def token_addresses(self, chain: str) -> list[str]:
        return [m.address for k, m in self.tokens.items() if k.startswith(f"{chain}:")]

    def update_price(self, symbol: str, price_usd: float) -> None:
        sym = symbol.upper()
        for meta in self.tokens.values():
            if meta.symbol == sym and not meta.stable:
                meta.price_usd = price_usd
