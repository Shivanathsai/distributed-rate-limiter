"""
conftest.py — shared pytest fixtures
=====================================
Uses fakeredis so tests run with no real Redis instance.
Provides both a plain limiter and a PipelinedRateLimiter (with its
background flush task managed by the fixture).
"""

import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis

from app.rate_limiter import SlidingWindowRateLimiter, TieredRateLimiter
from app.pipeline_limiter import PipelinedRateLimiter


@pytest_asyncio.fixture
async def fake_redis() -> AsyncGenerator[fakeredis.FakeRedis, None]:
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def limiter(fake_redis) -> SlidingWindowRateLimiter:
    return SlidingWindowRateLimiter(fake_redis)


@pytest_asyncio.fixture
async def tiered_limiter(fake_redis) -> TieredRateLimiter:
    return TieredRateLimiter(SlidingWindowRateLimiter(fake_redis))


@pytest_asyncio.fixture
async def pipeline_limiter(fake_redis):
    """
    PipelinedRateLimiter with its background flush task running.
    Task is cancelled cleanly after the test.
    """
    pl   = PipelinedRateLimiter(fake_redis, batch_size=50)
    task = asyncio.create_task(pl.run(), name="test-flusher")
    yield pl
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
