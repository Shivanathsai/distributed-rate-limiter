import asyncio
from typing import AsyncGenerator

import fakeredis
import fakeredis.aioredis as aioredis_fake
import pytest_asyncio

from app.rate_limiter import SlidingWindowRateLimiter, TieredRateLimiter
from app.pipeline_limiter import PipelinedRateLimiter


@pytest_asyncio.fixture
async def fake_redis() -> AsyncGenerator[aioredis_fake.FakeRedis, None]:
    server = fakeredis.FakeServer()
    client = aioredis_fake.FakeRedis(server=server, decode_responses=True)
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
    pl = PipelinedRateLimiter(fake_redis, batch_size=50)
    task = asyncio.create_task(pl.run(), name="test-flusher")
    yield pl
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
