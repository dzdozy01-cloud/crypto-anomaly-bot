"""Async JSON-RPC clients for EVM chains and Solana.

Implemented against raw JSON-RPC rather than web3.py's sync provider so the
whole tracker stays inside one event loop. web3 is used opportunistically for
address checksumming when installed, but is never required.

Features: connection pooling, batch requests, per-endpoint rate limiting,
circuit breaking, and automatic failover across a list of endpoints.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from ...core.resilience import CircuitBreaker, CircuitOpenError, RateLimiter

log = logging.getLogger(__name__)

__all__ = ["EVMClient", "SolanaClient", "RPCError", "erc20_transfer_topic", "decode_transfer_log"]

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# keccak256("Sync(uint112,uint112)") — UniswapV2-style reserve updates
SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
# keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


class RPCError(RuntimeError):
    """JSON-RPC level failure."""


def erc20_transfer_topic() -> str:
    return TRANSFER_TOPIC


def _hex_to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value), 16) if str(value).startswith("0x") else int(value)
    except (ValueError, TypeError):
        return default


def _topic_to_address(topic: str) -> str:
    """Left-padded 32-byte topic -> 20-byte hex address."""
    return "0x" + topic[-40:].lower()


def decode_transfer_log(log_entry: dict[str, Any], decimals: int = 18) -> dict[str, Any] | None:
    """Decode an ERC-20 Transfer log into a plain dict."""
    topics = log_entry.get("topics") or []
    if len(topics) < 3 or topics[0].lower() != TRANSFER_TOPIC:
        return None
    raw = _hex_to_int(log_entry.get("data", "0x0"))
    return {
        "token": (log_entry.get("address") or "").lower(),
        "from": _topic_to_address(topics[1]),
        "to": _topic_to_address(topics[2]),
        "raw_amount": raw,
        "amount": raw / (10**decimals),
        "block": _hex_to_int(log_entry.get("blockNumber")),
        "tx_hash": log_entry.get("transactionHash", ""),
        "log_index": _hex_to_int(log_entry.get("logIndex")),
    }


@dataclass
class _Endpoint:
    url: str
    breaker: CircuitBreaker
    limiter: RateLimiter
    failures: int = 0
    latency_ms: float = 0.0


class _BaseRPC:
    """Shared HTTP/JSON-RPC plumbing with endpoint failover."""

    def __init__(
        self,
        urls: list[str] | str,
        chain: str,
        rate_per_sec: float = 20.0,
        timeout_s: float = 15.0,
    ) -> None:
        url_list = [urls] if isinstance(urls, str) else list(urls)
        self.chain = chain
        self.timeout_s = timeout_s
        self.endpoints = [
            _Endpoint(
                url=u,
                breaker=CircuitBreaker(name=f"{chain}:{i}", failure_threshold=4, recovery_timeout=20),
                limiter=RateLimiter(rate_per_sec, burst=int(rate_per_sec)),
            )
            for i, u in enumerate(url_list)
            if u
        ]
        self._session: aiohttp.ClientSession | None = None
        self._rid = 0
        self._idx = 0

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                connector=aiohttp.TCPConnector(limit=32, ttl_dns_cache=300),
                headers={"Content-Type": "application/json"},
            )
        return self._session

    def _next_id(self) -> int:
        self._rid += 1
        return self._rid

    async def _post(self, endpoint: _Endpoint, payload: Any) -> Any:
        await endpoint.limiter.acquire()
        session = await self.session()

        async def _do() -> Any:
            loop = asyncio.get_event_loop()
            t0 = loop.time()
            async with session.post(endpoint.url, json=payload) as resp:
                if resp.status == 429:
                    raise RPCError("rate limited (429)")
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            endpoint.latency_ms = (loop.time() - t0) * 1000
            return data

        return await endpoint.breaker.call(_do)

    async def call(self, method: str, params: list[Any] | None = None) -> Any:
        """Single JSON-RPC call with automatic endpoint failover."""
        payload = {"jsonrpc": "2.0", "id": self._next_id(), "method": method, "params": params or []}
        last_exc: Exception | None = None
        for attempt in range(len(self.endpoints) or 1):
            if not self.endpoints:
                raise RPCError(f"no RPC endpoints configured for {self.chain}")
            ep = self.endpoints[(self._idx + attempt) % len(self.endpoints)]
            try:
                data = await self._post(ep, payload)
            except CircuitOpenError as exc:
                last_exc = exc
                continue
            except Exception as exc:
                ep.failures += 1
                last_exc = exc
                log.debug("%s rpc %s failed on %s: %s", self.chain, method, ep.url, exc)
                continue
            self._idx = (self._idx + attempt) % len(self.endpoints)
            if isinstance(data, dict) and "error" in data:
                raise RPCError(f"{method}: {data['error']}")
            return data.get("result") if isinstance(data, dict) else data
        raise RPCError(f"all endpoints failed for {method}: {last_exc}")

    async def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Batch JSON-RPC. Falls back to sequential calls if the node rejects batching."""
        if not calls:
            return []
        payload = [
            {"jsonrpc": "2.0", "id": self._next_id(), "method": m, "params": p} for m, p in calls
        ]
        try:
            ep = self.endpoints[self._idx % len(self.endpoints)]
            data = await self._post(ep, payload)
            if not isinstance(data, list):
                raise RPCError("batch response not a list")
            by_id = {item.get("id"): item for item in data}
            return [by_id.get(req["id"], {}).get("result") for req in payload]
        except Exception as exc:
            log.debug("%s batch failed (%s); falling back to sequential", self.chain, exc)
            out = []
            for m, p in calls:
                try:
                    out.append(await self.call(m, p))
                except Exception:
                    out.append(None)
            return out

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def health(self) -> dict[str, Any]:
        return {
            "chain": self.chain,
            "endpoints": [
                {
                    "url": ep.url.split("/")[2] if "//" in ep.url else ep.url,
                    "circuit": ep.breaker.state.value,
                    "failures": ep.failures,
                    "latency_ms": round(ep.latency_ms, 1),
                }
                for ep in self.endpoints
            ],
        }


class EVMClient(_BaseRPC):
    """Ethereum-compatible JSON-RPC client (Ethereum, BSC, Arbitrum, Base, ...)."""

    async def block_number(self) -> int:
        return _hex_to_int(await self.call("eth_blockNumber"))

    async def get_block(self, block: int | str, full_tx: bool = False) -> dict[str, Any]:
        tag = block if isinstance(block, str) else hex(block)
        return await self.call("eth_getBlockByNumber", [tag, full_tx]) or {}

    async def get_logs(
        self,
        from_block: int,
        to_block: int,
        address: str | list[str] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address:
            params["address"] = address
        if topics:
            params["topics"] = topics
        result = await self.call("eth_getLogs", [params])
        return result or []

    async def eth_call(self, to: str, data: str, block: str = "latest") -> str:
        return await self.call("eth_call", [{"to": to, "data": data}, block]) or "0x"

    async def get_reserves(self, pair_address: str) -> tuple[float, float, int] | None:
        """UniswapV2 ``getReserves()`` -> (reserve0, reserve1, blockTimestampLast)."""
        raw = await self.eth_call(pair_address, "0x0902f1ac")
        if not raw or len(raw) < 194:
            return None
        body = raw[2:]
        r0 = int(body[0:64], 16)
        r1 = int(body[64:128], 16)
        ts = int(body[128:192], 16)
        return float(r0), float(r1), ts

    async def erc20_balance(self, token: str, holder: str) -> int:
        """``balanceOf(address)`` = 0x70a08231."""
        data = "0x70a08231" + holder.lower().replace("0x", "").rjust(64, "0")
        return _hex_to_int(await self.eth_call(token, data))

    async def erc20_decimals(self, token: str) -> int:
        try:
            return _hex_to_int(await self.eth_call(token, "0x313ce567"), 18) or 18
        except Exception:
            return 18


class SolanaClient(_BaseRPC):
    """Solana JSON-RPC client for SPL token flow monitoring."""

    async def get_slot(self) -> int:
        return int(await self.call("getSlot", [{"commitment": "confirmed"}]) or 0)

    async def get_signatures(self, address: str, limit: int = 50, before: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "commitment": "confirmed"}
        if before:
            params["before"] = before
        return await self.call("getSignaturesForAddress", [address, params]) or []

    async def get_transaction(self, signature: str) -> dict[str, Any] | None:
        return await self.call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                         "commitment": "confirmed"}],
        )

    async def get_token_account_balance(self, account: str) -> float:
        res = await self.call("getTokenAccountBalance", [account, {"commitment": "confirmed"}])
        if not res:
            return 0.0
        return float((res.get("value") or {}).get("uiAmount") or 0.0)

    async def get_token_supply(self, mint: str) -> float:
        res = await self.call("getTokenSupply", [mint, {"commitment": "confirmed"}])
        return float(((res or {}).get("value") or {}).get("uiAmount") or 0.0)

    @staticmethod
    def extract_spl_transfers(tx: dict[str, Any]) -> list[dict[str, Any]]:
        """Pull SPL token deltas out of a parsed transaction's balance diff.

        Using pre/post token balances rather than instruction parsing catches
        transfers made through arbitrary programs (aggregators, routers), which
        is exactly where whales hide.
        """
        meta = (tx or {}).get("meta") or {}
        pre = {(b.get("accountIndex"), b.get("mint")): b for b in meta.get("preTokenBalances", [])}
        post = {(b.get("accountIndex"), b.get("mint")): b for b in meta.get("postTokenBalances", [])}
        out: list[dict[str, Any]] = []
        for key in set(pre) | set(post):
            idx, mint = key
            p = pre.get(key, {}).get("uiTokenAmount", {})
            q = post.get(key, {}).get("uiTokenAmount", {})
            before = float(p.get("uiAmount") or 0.0)
            after = float(q.get("uiAmount") or 0.0)
            delta = after - before
            if abs(delta) < 1e-12:
                continue
            owner = post.get(key, {}).get("owner") or pre.get(key, {}).get("owner") or ""
            out.append(
                {
                    "mint": mint,
                    "owner": owner,
                    "account_index": idx,
                    "delta": delta,
                    "post_balance": after,
                }
            )
        return out
