"""Module 2 — On-Chain Whale Tracker.

Three independent surveillance loops:

1. **CEX flow monitor** — ERC-20 ``Transfer`` logs and Solana SPL balance deltas
   touching known exchange hot wallets, filtered at > $500k notional. Inflows are
   pre-sell pressure; outflows are accumulation.
2. **Liquidity monitor** — DEX pool reserves polled per block; a single-block
   drop > 30% is flagged as a rug/pull candidate.
3. **Bridge monitor** — high-volume stablecoin transfers through bridge
   contracts, correlated forward in time with CEX deposits (the classic
   bridge -> exchange -> dump sequence).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.bus import EventBus
from ...core.config import OnChainConfig
from ...core.resilience import BackoffPolicy
from ...core.schema import MarketEvent, MetricType, SourceType, now_ms
from ...core.stats import DynamicZScore
from ...core.telemetry import METRICS
from ..base import Module
from .registry import AddressRegistry, PoolMeta
from .rpc import TRANSFER_TOPIC, EVMClient, SolanaClient, decode_transfer_log

__all__ = ["WhaleTracker", "WhaleTransfer", "LiquidityEvent", "BridgeFlow"]


@dataclass
class WhaleTransfer:
    chain: str
    token: str
    symbol: str
    from_addr: str
    to_addr: str
    amount: float
    usd_value: float
    direction: str            # inflow | outflow | internal | unrelated
    counterparty: str | None  # exchange label when known
    tx_hash: str
    block: int
    timestamp: int


@dataclass
class LiquidityEvent:
    chain: str
    pool: str
    pool_address: str
    prev_tvl_usd: float
    new_tvl_usd: float
    drop_pct: float
    block: int
    timestamp: int


@dataclass
class BridgeFlow:
    chain: str
    bridge: str
    token: str
    amount: float
    usd_value: float
    direction: str
    tx_hash: str
    timestamp: int


@dataclass
class _ChainCursor:
    """Per-chain scan position with catch-up guards."""

    last_block: int = 0
    lag_blocks: int = 2           # stay behind the tip to avoid reorgs
    max_span: int = 200           # never request more than this many blocks at once
    errors: int = 0


class WhaleTracker(Module):
    """Cross-chain (EVM + Solana) whale, liquidity and bridge surveillance."""

    name = "onchain"

    def __init__(self, bus: EventBus, config: OnChainConfig) -> None:
        super().__init__(bus)
        self.config = config
        self.registry = AddressRegistry.build(
            chains=list(config.evm_rpc.keys()) + ["solana"],
            extra_wallets=config.cex_wallets,
            extra_tokens=config.tracked_tokens or None,
            extra_pools=config.dex_pools or None,
            extra_bridges=config.bridge_contracts,
        )
        self.evm: dict[str, EVMClient] = {}
        self.solana: SolanaClient | None = None
        self.cursors: dict[str, _ChainCursor] = {}
        self.flow_z: dict[str, DynamicZScore] = {}
        self.recent_bridge: deque[BridgeFlow] = deque(maxlen=500)
        self.recent_whales: deque[WhaleTransfer] = deque(maxlen=1000)
        self._sol_seen: deque[str] = deque(maxlen=4000)
        self._sol_seen_set: set[str] = set()
        self.net_flow_usd: dict[str, float] = {}

    # ---- lifecycle -----------------------------------------------------
    async def run(self) -> None:
        if self.config.simulate:
            self.spawn("simulator", self._simulate_loop())
            self.log.info("onchain tracker running in simulation mode")
            return

        for chain, url in self.config.evm_rpc.items():
            if not url:
                continue
            urls = [u.strip() for u in url.split(",") if u.strip()]
            self.evm[chain] = EVMClient(urls, chain=chain, rate_per_sec=12.0)
            self.cursors[chain] = _ChainCursor()
            self.supervise(
                f"evm-flows:{chain}",
                self._make_evm_loop(chain),
                BackoffPolicy(initial=2.0, maximum=120.0),
            )

        if self.config.solana_rpc:
            self.solana = SolanaClient(
                [u.strip() for u in self.config.solana_rpc.split(",") if u.strip()],
                chain="solana",
                rate_per_sec=8.0,
            )
            self.supervise(
                "solana-flows", self._solana_loop, BackoffPolicy(initial=3.0, maximum=120.0)
            )

        if self.registry.pools and self.evm:
            self.supervise(
                "liquidity", self._liquidity_loop, BackoffPolicy(initial=3.0, maximum=120.0)
            )

        self.log.info(
            "onchain tracker: %d EVM chain(s), solana=%s, %d pools, %d watched wallets",
            len(self.evm), bool(self.solana), len(self.registry.pools),
            len(self.registry.cex_wallets),
        )

    def _make_evm_loop(self, chain: str) -> Any:
        async def loop() -> None:
            await self._evm_flow_loop(chain)
        return loop

    async def cleanup(self) -> None:
        for client in self.evm.values():
            await client.close()
        if self.solana:
            await self.solana.close()

    # ---- EVM: CEX flows + bridges ---------------------------------------
    async def _evm_flow_loop(self, chain: str) -> None:
        client = self.evm[chain]
        cursor = self.cursors[chain]
        tokens = self.registry.token_addresses(chain)
        if not tokens:
            self.log.warning("no tracked tokens for chain %s; flow loop idle", chain)
            await asyncio.sleep(60)
            return

        while True:
            tip = await client.block_number()
            target = max(0, tip - cursor.lag_blocks)
            if cursor.last_block == 0:
                cursor.last_block = max(0, target - 5)
            if target <= cursor.last_block:
                await asyncio.sleep(self.config.poll_interval_s)
                continue

            from_block = cursor.last_block + 1
            to_block = min(target, from_block + cursor.max_span - 1)

            logs = await client.get_logs(
                from_block=from_block,
                to_block=to_block,
                address=tokens,
                topics=[TRANSFER_TOPIC],
            )
            block_ts = await self._block_timestamp(client, to_block)
            await self._process_evm_logs(chain, logs, block_ts)
            cursor.last_block = to_block
            METRICS.gauge(f"onchain.{chain}.block", to_block)

            if to_block >= target:
                await asyncio.sleep(self.config.poll_interval_s)

    async def _block_timestamp(self, client: EVMClient, block: int) -> int:
        with contextlib.suppress(Exception):
            blk = await client.get_block(block)
            ts = blk.get("timestamp")
            if ts:
                return int(str(ts), 16) * 1000 if str(ts).startswith("0x") else int(ts) * 1000
        return now_ms()

    async def _process_evm_logs(
        self, chain: str, logs: list[dict[str, Any]], block_ts: int
    ) -> None:
        for entry in logs:
            token_addr = (entry.get("address") or "").lower()
            meta = self.registry.token(chain, token_addr)
            if meta is None:
                continue
            decoded = decode_transfer_log(entry, decimals=meta.decimals)
            if not decoded:
                continue

            usd = meta.to_usd(decoded["amount"])
            direction = self.registry.classify(decoded["from"], decoded["to"])
            is_bridge = self.registry.is_bridge(decoded["from"]) or self.registry.is_bridge(
                decoded["to"]
            )

            if is_bridge and usd >= self.config.bridge_threshold_usd and meta.stable:
                await self._emit_bridge(
                    BridgeFlow(
                        chain=chain,
                        bridge=(
                            self.registry.bridge_label(decoded["to"])
                            or self.registry.bridge_label(decoded["from"])
                            or "unknown"
                        ),
                        token=meta.symbol,
                        amount=decoded["amount"],
                        usd_value=usd,
                        direction="into_bridge"
                        if self.registry.is_bridge(decoded["to"])
                        else "out_of_bridge",
                        tx_hash=str(decoded["tx_hash"]),
                        timestamp=block_ts,
                    )
                )

            if direction in ("inflow", "outflow") and usd >= self.config.whale_threshold_usd:
                await self._emit_whale(
                    WhaleTransfer(
                        chain=chain,
                        token=token_addr,
                        symbol=meta.symbol,
                        from_addr=decoded["from"],
                        to_addr=decoded["to"],
                        amount=decoded["amount"],
                        usd_value=usd,
                        direction=direction,
                        counterparty=(
                            self.registry.cex_label(decoded["to"])
                            if direction == "inflow"
                            else self.registry.cex_label(decoded["from"])
                        ),
                        tx_hash=str(decoded["tx_hash"]),
                        block=int(decoded["block"]),
                        timestamp=block_ts,
                    )
                )

    # ---- Solana: SPL flows ----------------------------------------------
    async def _solana_loop(self) -> None:
        assert self.solana is not None
        watched = [a for a in self.registry.cex_wallets if not a.startswith("0x")]
        if not watched:
            await asyncio.sleep(60)
            return
        while True:
            for wallet in watched:
                sigs = await self.solana.get_signatures(wallet, limit=20)
                for sig_info in sigs:
                    sig = sig_info.get("signature", "")
                    if not sig or sig in self._sol_seen_set or sig_info.get("err"):
                        continue
                    self._remember_sig(sig)
                    tx = await self.solana.get_transaction(sig)
                    if not tx:
                        continue
                    await self._process_solana_tx(tx, wallet, sig, sig_info)
                await asyncio.sleep(0.35)  # spread RPC load across wallets
            await asyncio.sleep(self.config.poll_interval_s)

    def _remember_sig(self, sig: str) -> None:
        if len(self._sol_seen) == self._sol_seen.maxlen:
            self._sol_seen_set.discard(self._sol_seen[0])
        self._sol_seen.append(sig)
        self._sol_seen_set.add(sig)

    async def _process_solana_tx(
        self, tx: dict[str, Any], wallet: str, sig: str, sig_info: dict[str, Any]
    ) -> None:
        assert self.solana is not None
        ts = int(sig_info.get("blockTime") or time.time()) * 1000
        for transfer in self.solana.extract_spl_transfers(tx):
            meta = self.registry.token("solana", transfer["mint"] or "")
            if meta is None:
                continue
            usd = meta.to_usd(abs(transfer["delta"]))
            if usd < self.config.whale_threshold_usd:
                continue
            owner = transfer.get("owner") or ""
            touches_cex = self.registry.is_cex(owner) or owner == wallet
            if not touches_cex:
                continue
            direction = "inflow" if transfer["delta"] > 0 else "outflow"
            await self._emit_whale(
                WhaleTransfer(
                    chain="solana",
                    token=transfer["mint"],
                    symbol=meta.symbol,
                    from_addr="" if direction == "inflow" else owner,
                    to_addr=owner if direction == "inflow" else "",
                    amount=abs(transfer["delta"]),
                    usd_value=usd,
                    direction=direction,
                    counterparty=self.registry.cex_label(owner) or "unknown_sol_cex",
                    tx_hash=sig,
                    block=int(sig_info.get("slot") or 0),
                    timestamp=ts,
                )
            )

    # ---- DEX liquidity ----------------------------------------------------
    async def _liquidity_loop(self) -> None:
        while True:
            for pool in list(self.registry.pools.values()):
                client = self.evm.get(pool.chain)
                if client is None:
                    continue
                with contextlib.suppress(Exception):
                    await self._check_pool(client, pool)
            await asyncio.sleep(max(self.config.poll_interval_s, 2.0))

    async def _check_pool(self, client: EVMClient, pool: PoolMeta) -> None:
        reserves = await client.get_reserves(pool.address)
        if not reserves:
            return
        r0, r1, _ = reserves
        a0 = r0 / (10**pool.decimals0)
        a1 = r1 / (10**pool.decimals1)

        t0 = self.registry.tokens_by_symbol.get(pool.token0.upper())
        t1 = self.registry.tokens_by_symbol.get(pool.token1.upper())
        # Value the pool from whichever side we can price, doubled (constant product).
        tvl = 0.0
        if t1 and t1.price_usd > 0:
            tvl = a1 * t1.price_usd * 2
        elif t0 and t0.price_usd > 0:
            tvl = a0 * t0.price_usd * 2
        else:
            tvl = a0 + a1  # unpriced: track relative change only

        block = await client.block_number()
        prev = pool.last_tvl_usd
        if prev > 0:
            drop_pct = (prev - tvl) / prev * 100.0
            blocks_elapsed = max(1, block - pool.last_block)
            # "single-block" drop: only alert if the change happened within ~1 block
            if drop_pct >= self.config.liquidity_drop_pct and blocks_elapsed <= 3:
                await self._emit_liquidity(
                    LiquidityEvent(
                        chain=pool.chain,
                        pool=pool.name,
                        pool_address=pool.address,
                        prev_tvl_usd=prev,
                        new_tvl_usd=tvl,
                        drop_pct=drop_pct,
                        block=block,
                        timestamp=now_ms(),
                    )
                )
        pool.last_reserve0, pool.last_reserve1 = a0, a1
        pool.last_tvl_usd, pool.last_block = tvl, block

    # ---- emission --------------------------------------------------------
    def _flow_key(self, chain: str, symbol: str) -> str:
        return f"{chain}:{symbol}"

    async def _emit_whale(self, w: WhaleTransfer) -> None:
        self.recent_whales.append(w)
        key = self._flow_key(w.chain, w.symbol)
        signed = w.usd_value if w.direction == "inflow" else -w.usd_value
        self.net_flow_usd[key] = self.net_flow_usd.get(key, 0.0) + signed

        z_tracker = self.flow_z.setdefault(
            key, DynamicZScore(half_life_s=1800, window=200, warmup=8, base_threshold=2.5)
        )
        z = z_tracker.update(abs(w.usd_value), w.timestamp)

        # A deposit that follows a recent bridge inflow is the highest-signal pattern.
        bridge_precursor = self._match_bridge_precursor(w)
        METRICS.incr(f"onchain.whale.{w.direction}")

        await self.emit(
            MarketEvent(
                timestamp=w.timestamp,
                source_type=SourceType.ONCHAIN,
                venue=w.chain,
                asset_pair=w.symbol,
                metric_type=MetricType.WALLET_TRANSFER,
                raw_value=w.amount,
                normalized_z_score=z,
                usd_value=w.usd_value,
                meta={
                    "direction": w.direction,
                    "counterparty": w.counterparty,
                    "from": w.from_addr,
                    "to": w.to_addr,
                    "tx_hash": w.tx_hash,
                    "block": w.block,
                    "net_flow_usd": round(self.net_flow_usd[key], 2),
                    "bridge_precursor": bridge_precursor,
                    "threshold_usd": self.config.whale_threshold_usd,
                },
            )
        )
        self.log.info(
            "whale %s %s %.0f USD %s (%s)",
            w.direction, w.symbol, w.usd_value, w.chain, w.counterparty or "?",
        )

    def _match_bridge_precursor(self, w: WhaleTransfer, window_s: int = 1800) -> dict[str, Any] | None:
        """Link a CEX deposit back to a recent bridge transfer of similar size."""
        if w.direction != "inflow":
            return None
        cutoff = w.timestamp - window_s * 1000
        for flow in reversed(self.recent_bridge):
            if flow.timestamp < cutoff:
                break
            if flow.usd_value <= 0:
                continue
            ratio = min(flow.usd_value, w.usd_value) / max(flow.usd_value, w.usd_value)
            if ratio >= 0.75:
                return {
                    "bridge": flow.bridge,
                    "usd": flow.usd_value,
                    "lag_s": round((w.timestamp - flow.timestamp) / 1000, 1),
                    "size_ratio": round(ratio, 3),
                }
        return None

    async def _emit_bridge(self, b: BridgeFlow) -> None:
        self.recent_bridge.append(b)
        METRICS.incr("onchain.bridge_flows")
        await self.emit(
            MarketEvent(
                timestamp=b.timestamp,
                source_type=SourceType.ONCHAIN,
                venue=b.chain,
                asset_pair=b.token,
                metric_type=MetricType.BRIDGE_FLOW,
                raw_value=b.amount,
                usd_value=b.usd_value,
                meta={
                    "bridge": b.bridge,
                    "direction": b.direction,
                    "tx_hash": b.tx_hash,
                    "threshold_usd": self.config.bridge_threshold_usd,
                },
            )
        )

    async def _emit_liquidity(self, ev: LiquidityEvent) -> None:
        METRICS.incr("onchain.liquidity_drops")
        # Map drop% onto a pseudo-z so the ML layer sees a comparable scale.
        pseudo_z = min(10.0, ev.drop_pct / max(self.config.liquidity_drop_pct, 1e-9) * 3.0)
        await self.emit(
            MarketEvent(
                timestamp=ev.timestamp,
                source_type=SourceType.ONCHAIN,
                venue=ev.chain,
                asset_pair=ev.pool.split("/")[0].upper(),
                metric_type=MetricType.LIQUIDITY,
                raw_value=-ev.drop_pct,
                normalized_z_score=pseudo_z,
                usd_value=ev.new_tvl_usd,
                meta={
                    "pool": ev.pool,
                    "pool_address": ev.pool_address,
                    "prev_tvl_usd": round(ev.prev_tvl_usd, 2),
                    "new_tvl_usd": round(ev.new_tvl_usd, 2),
                    "drop_pct": round(ev.drop_pct, 2),
                    "block": ev.block,
                    "threshold_pct": self.config.liquidity_drop_pct,
                },
            )
        )
        self.log.warning(
            "liquidity drop %s %s -%.1f%% (TVL $%.0f -> $%.0f)",
            ev.chain, ev.pool, ev.drop_pct, ev.prev_tvl_usd, ev.new_tvl_usd,
        )

    # ---- simulation ------------------------------------------------------
    async def _simulate_loop(self) -> None:
        """Synthetic on-chain activity for demos and integration tests."""
        import random

        rng = random.Random(7)
        symbols = ["BTC", "ETH", "SOL", "PEPE", "USDT"]
        chains = ["ethereum", "solana", "bsc"]
        exchanges = ["binance_14", "coinbase_1", "bybit_hot", "mexc_hot"]
        while True:
            await asyncio.sleep(rng.uniform(1.5, 5.0))
            roll = rng.random()
            ts = now_ms()
            if roll < 0.62:
                usd = rng.choice([6e5, 9e5, 1.4e6, 3.2e6, 8.5e6])
                sym = rng.choice(symbols)
                await self._emit_whale(
                    WhaleTransfer(
                        chain=rng.choice(chains), token="0xsim", symbol=sym,
                        from_addr="0xwhale", to_addr="0xcex",
                        amount=usd / rng.uniform(0.5, 4000),
                        usd_value=usd,
                        direction=rng.choice(["inflow", "inflow", "outflow"]),
                        counterparty=rng.choice(exchanges),
                        tx_hash=f"0x{rng.getrandbits(64):016x}",
                        block=rng.randint(19_000_000, 19_999_999), timestamp=ts,
                    )
                )
            elif roll < 0.85:
                await self._emit_bridge(
                    BridgeFlow(
                        chain="ethereum", bridge=rng.choice(["wormhole_portal", "arbitrum_bridge"]),
                        token="USDT", amount=rng.uniform(1e6, 2e7),
                        usd_value=rng.uniform(1e6, 2e7),
                        direction=rng.choice(["into_bridge", "out_of_bridge"]),
                        tx_hash=f"0x{rng.getrandbits(64):016x}", timestamp=ts,
                    )
                )
            else:
                prev = rng.uniform(2e6, 4e7)
                drop = rng.uniform(31, 92)
                await self._emit_liquidity(
                    LiquidityEvent(
                        chain="ethereum", pool=f"{rng.choice(['PEPE','SHIB','ARB'])}/WETH",
                        pool_address="0xsimpool", prev_tvl_usd=prev,
                        new_tvl_usd=prev * (1 - drop / 100), drop_pct=drop,
                        block=rng.randint(19_000_000, 19_999_999), timestamp=ts,
                    )
                )

    # ---- introspection ----------------------------------------------------
    def health(self) -> dict[str, Any]:
        base = super().health()
        base["chains"] = {c: cl.health() for c, cl in self.evm.items()}
        if self.solana:
            base["chains"]["solana"] = self.solana.health()
        base["whales_seen"] = len(self.recent_whales)
        base["net_flow_usd"] = {k: round(v, 2) for k, v in self.net_flow_usd.items()}
        return base
