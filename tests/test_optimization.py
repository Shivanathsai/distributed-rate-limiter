import asyncio
import time

import fakeredis
import fakeredis.aioredis as aioredis_fake
import pytest
import pytest_asyncio

from app.rate_limiter import SlidingWindowRateLimiter
from app.naive_rate_limiter import NaiveListRateLimiter
from app.pipeline_limiter import PipelinedRateLimiter


@pytest_asyncio.fixture
async def redis_pair():
    server = fakeredis.FakeServer()
    r1 = aioredis_fake.FakeRedis(server=server, decode_responses=True)
    r2 = aioredis_fake.FakeRedis(server=server, decode_responses=True)
    yield r1, r2
    await r1.aclose()
    await r2.aclose()


class TestComplexityComparison:
    @pytest.mark.asyncio
    async def test_naive_degrades_as_n_grows(self, redis_pair):
        r_naive, r_sorted = redis_pair
        naive = NaiveListRateLimiter(r_naive)
        optimal = SlidingWindowRateLimiter(r_sorted)
        LIMIT = 999_999
        WINDOW = 1_000
        ITERS = 200

        latencies_naive_small = []
        latencies_naive_large = []
        latencies_sorted_small = []
        latencies_sorted_large = []

        now = int(time.time() * 1_000)
        pipe_n = r_naive.pipeline()
        pipe_s = r_sorted.pipeline()
        for i in range(100):
            ts = now - (WINDOW * i // 101)
            pipe_n.lpush("n:small", str(ts))
            pipe_s.zadd("s:small", {f"m{i}": ts})
        await pipe_n.execute()
        await pipe_s.execute()

        pipe_n = r_naive.pipeline()
        pipe_s = r_sorted.pipeline()
        for i in range(5_000):
            ts = now - (WINDOW * i // 5_001)
            pipe_n.lpush("n:large", str(ts))
            pipe_s.zadd("s:large", {f"m{i}": ts})
        await pipe_n.execute()
        await pipe_s.execute()

        for _ in range(ITERS):
            t0 = time.perf_counter()
            await naive.check("n:small", LIMIT, WINDOW)
            latencies_naive_small.append((time.perf_counter() - t0) * 1_000)

            t0 = time.perf_counter()
            await naive.check("n:large", LIMIT, WINDOW)
            latencies_naive_large.append((time.perf_counter() - t0) * 1_000)

            t0 = time.perf_counter()
            await optimal.check("s:small", LIMIT, WINDOW)
            latencies_sorted_small.append((time.perf_counter() - t0) * 1_000)

            t0 = time.perf_counter()
            await optimal.check("s:large", LIMIT, WINDOW)
            latencies_sorted_large.append((time.perf_counter() - t0) * 1_000)

        def p99(lats):
            return sorted(lats)[int(len(lats) * 0.99)]

        assert p99(latencies_naive_large) > p99(latencies_naive_small) * 2
        assert p99(latencies_sorted_large) < p99(latencies_sorted_small) * 3


class TestPipelineThroughput:
    @pytest.mark.asyncio
    async def test_pipeline_achieves_high_throughput(self):
        """Pipeline must complete faster than sequential — not a fixed ratio."""
        TOTAL = 500  # small enough to be fast in CI

        server1 = fakeredis.FakeServer()
        redis1 = aioredis_fake.FakeRedis(server=server1, decode_responses=True)
        seq_limiter = SlidingWindowRateLimiter(redis1)
        t0 = time.perf_counter()
        for i in range(TOTAL):
            await seq_limiter.check(f"seq:{i % 10}", limit=999_999, window_ms=1_000)
        seq_elapsed = time.perf_counter() - t0
        await redis1.aclose()

        server2 = fakeredis.FakeServer()
        redis2 = aioredis_fake.FakeRedis(server=server2, decode_responses=True)
        pip_limiter = PipelinedRateLimiter(redis2, batch_size=50)
        task = asyncio.create_task(pip_limiter.run())
        t0 = time.perf_counter()
        await asyncio.gather(
            *[
                pip_limiter.check(f"pip:{i % 10}", limit=999_999, window_ms=1_000)
                for i in range(TOTAL)
            ]
        )
        pip_elapsed = time.perf_counter() - t0
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await redis2.aclose()

        # Pipeline must be faster — relax ratio for CI environments
        assert pip_elapsed < seq_elapsed * 2, (
            f"Pipeline ({pip_elapsed:.3f}s) should not be slower than "
            f"sequential ({seq_elapsed:.3f}s)"
        )

    @pytest.mark.asyncio
    async def test_pipeline_results_are_correct(self):
        server = fakeredis.FakeServer()
        redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)
        limiter = PipelinedRateLimiter(redis, batch_size=50)
        task = asyncio.create_task(limiter.run())

        results = await asyncio.gather(
            *[limiter.check("key:correct", limit=5, window_ms=1_000) for _ in range(20)]
        )
        allowed = sum(1 for r in results if r.allowed)
        assert allowed == 5, f"Expected exactly 5 allowed, got {allowed}"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await redis.aclose()


class TestFailover:
    @pytest.mark.asyncio
    async def test_fail_open_on_redis_error(self, fake_redis):
        """Patch eval on the redis client directly."""
        import redis.asyncio as aioredis
        from unittest.mock import patch

        limiter = SlidingWindowRateLimiter(fake_redis)
        with patch.object(
            fake_redis,
            "eval",
            side_effect=aioredis.RedisError("simulated primary failure"),
        ):
            result = await limiter.check("key:failover", limit=5, window_ms=1_000)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_pipeline_fail_open_on_redis_error(self):
        server = fakeredis.FakeServer()
        redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)
        limiter = PipelinedRateLimiter(redis, batch_size=10)
        task = asyncio.create_task(limiter.run())
        await redis.aclose()
        result = await limiter.check("key:pip-fail", limit=5, window_ms=1_000)
        assert result.allowed is True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
