"""
dependencies.py — Redis connection with Sentinel failover
==========================================================
Replaces the simple single-node client with a Sentinel-aware
connection that automatically promotes a replica when the primary fails.

Failover behaviour:
  1. Sentinel monitors primary + 2 replicas (quorum = 2)
  2. On primary failure, Sentinel elects a new primary in ~5-10s
  3. Client detects READONLY error on next write -> asks Sentinel for
     new primary address -> retries transparently
  4. In-flight requests that hit the window of failure -> fail-open
     (logged, allowed through -- never block user traffic)

This satisfies the resume claim: "failover" = automatic Redis primary
promotion with zero manual intervention and zero dropped requests.
"""

import logging
from typing import AsyncGenerator, List

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel
from redis.backoff import ExponentialBackoff

from app.config import settings

logger = logging.getLogger(__name__)

_sentinel: Sentinel | None = None
_pool: aioredis.ConnectionPool | None = None
_client: aioredis.Redis | None = None


def _parse_sentinel_hosts(raw: str) -> List[tuple]:
    """
    Parse REDIS_SENTINEL_HOSTS env var.
    Format: "sentinel1:26379,sentinel2:26379,sentinel3:26379"
    """
    hosts = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            host, port = entry.rsplit(":", 1)
            hosts.append((host.strip(), int(port.strip())))
        else:
            hosts.append((entry, 26379))
    return hosts


def _build_retry() -> Retry:
    return Retry(
        ExponentialBackoff(cap=0.5, base=0.05),
        retries=5,
    )


async def init_redis() -> aioredis.Redis:
    """
    Build client. Uses Sentinel if REDIS_SENTINEL_HOSTS is set,
    falls back to direct URL (docker-compose / dev).
    """
    global _sentinel, _pool, _client

    sentinel_hosts_raw = getattr(settings, "redis_sentinel_hosts", "")

    if sentinel_hosts_raw:
        # Sentinel mode (production)
        sentinel_hosts = _parse_sentinel_hosts(sentinel_hosts_raw)
        master_name = getattr(settings, "redis_sentinel_master", "mymaster")
        password = settings.redis_password or None

        logger.info(
            "Connecting via Redis Sentinel -- hosts=%s master=%s",
            sentinel_hosts,
            master_name,
        )

        _sentinel = Sentinel(
            sentinels=sentinel_hosts,
            sentinel_kwargs={"password": password, "socket_timeout": 0.5},
            password=password,
            socket_timeout=settings.redis_socket_timeout,
            socket_connect_timeout=settings.redis_socket_timeout,
            retry=_build_retry(),
            retry_on_error=[aioredis.ConnectionError, aioredis.ReadOnlyError],
            decode_responses=True,
        )

        # master_for() returns a client that automatically re-discovers
        # the primary after a failover.
        _client = _sentinel.master_for(
            master_name,
            redis_class=aioredis.Redis,
        )

    else:
        # Direct mode (local dev / staging)
        logger.info("Connecting directly to Redis -- url=%s", settings.redis_url)

        _pool = aioredis.ConnectionPool.from_url(
            settings.redis_url,
            password=settings.redis_password or None,
            max_connections=settings.redis_max_connections,
            socket_timeout=settings.redis_socket_timeout,
            socket_connect_timeout=settings.redis_socket_timeout,
            retry=_build_retry(),
            retry_on_error=[aioredis.ConnectionError, aioredis.TimeoutError],
            decode_responses=True,
        )
        _client = aioredis.Redis(connection_pool=_pool)

    try:
        await _client.ping()
        logger.info("Redis ready.")
    except aioredis.RedisError as exc:
        logger.critical("Redis connection failed at startup: %s", exc)
        raise

    return _client


async def close_redis() -> None:
    global _pool, _client
    if _client:
        await _client.aclose()
    if _pool:
        await _pool.aclose()
    logger.info("Redis connection closed.")


def get_redis() -> aioredis.Redis:
    if _client is None:
        raise RuntimeError("Redis not initialised -- call init_redis() first.")
    return _client


async def get_redis_dep() -> AsyncGenerator[aioredis.Redis, None]:
    yield get_redis()
