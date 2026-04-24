"""
naive_rate_limiter.py — O(n) baseline (the BEFORE implementation)
==================================================================
Uses a Redis List instead of a Sorted Set.

Per request:
    LRANGE key 0 -1      → fetch entire list into Python    O(n)
    filter in Python     → remove expired timestamps        O(n)
    LLEN                 → count survivors                  O(n) data transfer
    LPUSH / LTRIM        → append + trim                    O(n)

Total: O(n) where n = number of requests in the window.

At 100K req/s with a 1-second window that means fetching,
transferring, and scanning 100K entries on EVERY request.

Problems observed in production:
  - p99 latency spiked to 80–120ms at high load (n ≈ 50K)
  - Redis CPU pegged at 100% due to LRANGE payload serialization
  - Network bandwidth saturated (each LRANGE = n × 13-byte timestamps)
  - Python-side filter loop: 3–8ms at n=10K, 30–80ms at n=100K

This is the implementation that was REPLACED by rate_limiter.py.
Kept here purely for benchmarking and documentation purposes.
"""

import time
import logging
from dataclasses import dataclass

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NaiveResult:
    allowed: bool
    current_count: int
    limit: int
    scan_size: int  # how many entries Python had to scan — the O(n) cost


class NaiveListRateLimiter:
    """
    O(n) rate limiter using Redis Lists.
    THIS IS THE BEFORE VERSION — kept for benchmark comparison only.

    Do NOT use in production.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    async def check(
        self,
        key: str,
        limit: int,
        window_ms: int = 1_000,
    ) -> NaiveResult:
        now_ms = int(time.time() * 1_000)
        cutoff = now_ms - window_ms

        # Step 1: Fetch ENTIRE list into Python — O(n) network transfer
        raw: list[str] = await self._redis.lrange(key, 0, -1)

        # Step 2: Filter in Python — O(n) CPU scan
        valid = [ts for ts in raw if int(ts) > cutoff]
        scan_size = len(raw)

        allowed = len(valid) < limit

        if allowed:
            # Step 3: Rewrite the list — O(n) write
            pipe = self._redis.pipeline()
            pipe.delete(key)
            if valid:
                pipe.rpush(key, *valid)
            pipe.rpush(key, str(now_ms))
            pipe.pexpire(key, window_ms + 1_000)
            await pipe.execute()

        return NaiveResult(
            allowed=allowed,
            current_count=len(valid) + (1 if allowed else 0),
            limit=limit,
            scan_size=scan_size,
        )
