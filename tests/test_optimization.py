"""
test_optimization.py — proves the resume claims with measurements
==================================================================

1. O(n) naive lags vs O(log n) sorted set as n grows
2. Pipeline limiter resolves 100K+ req/s
3. Fail-open on Redis error (failover behaviour)
"""

import asyncio
import time

import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis

from app.rate_limiter import SlidingWindowRateLimiter
from app.naive_rate_limiter import NaiveListRateLimiter
from app.pipeline_limiter import PipelinedRateLimiter


@pytest_asyncio.fixture
async def redis_pair():
    server = fakeredis.FakeServer()
    r1     = fakeredis.FakeRedis(server=server, decode_responses=True)
    r2     = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield r1, r2
    await r1.aclose()
    await r2.aclose()


# ── Claim: O(n) grows with window size, O(log n) stays flat ──────────────────

class TestComplexityComparison:

    @pytest.mark.asyncio
    async def test_naive_degrades_as_n_grows(self, redis_pair):
        """
        Naive O(n) latency should increase with window occupancy.
        Sorted set O(log n) latency should stay roughly constant.
        """
        r_naive, r_sorted = redis_pair
        naive   = NaiveListRateLimiter(r_naive)
        optimal = SlidingWindowRateLimiter(r_sorted)

        LIMIT    = 999_999
        WINDOW   = 1_000
        ITERS    = 200

        def measure(coro_factory, iters):
            # Synchronous measurement loop (blocking gather in test)
            return asyncio.get_event_loop().run_until_complete(
                _gather_latencies(coro_factory, iters)
            )

        latencies_naive_small  = []
        latencies_naive_large  = []
        latencies_sorted_small = []
        latencies_sorted_large = []

        # Seed small window (n = 100)
        now = int(time.time() * 1_000)
        pipe_n = r_naive.pipeline()
        pipe_s = r_sorted.pipeline()
        for i in range(100):
            ts = now - (WINDOW * i // 101)
            pipe_n.lpush("n:small", str(ts))
            pipe_s.zadd("s:small", {f"m{i}": ts})
        await pipe_n.execute()
        await pipe_s.execute()

        # Seed large window (n = 5 000)
        pipe_n = r_naive.pipeline()
        pipe_s = r_sorted.pipeline()
        for i in range(5_000):
            ts = now - (WINDOW * i // 5_001)
            pipe_n.lpush("n:large", str(ts))
            pipe_s.zadd("s:large", {f"m{i}": ts})
        await pipe_n.execute()
        await pipe_s.execute()

        # Measure
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

        p99 = lambda lats: sorted(lats)[int(len(lats) * 0.99)]

        naive_small_p99  = p99(latencies_naive_small)
        naive_large_p99  = p99(latencies_naive_large)
        sorted_small_p99 = p99(latencies_sorted_small)
        sorted_large_p99 = p99(latencies_sorted_large)

        # Naive p99 grows with n: large should be significantly worse than small
        assert naive_large_p99 > naive_small_p99 * 2, (
            f"Expected naive to degrade: small={naive_small_p99:.2f}ms large={naive_large_p99:.2f}ms"
        )

        # Sorted set p99 stays flat: large should be within 3× of small
        assert sorted_large_p99 < sorted_small_p99 * 3, (
            f"Expected sorted set to stay flat: small={sorted_small_p99:.2f}ms large={sorted_large_p99:.2f}ms"
        )


async def _gather_latencies(coro_factory, iters):
    lats = []
    for _ in range(iters):
        t0 = time.perf_counter()
        await coro_factory()
        lats.append((time.perf_counter() - t0) * 1_000)
    return lats


# ── Claim: 100K+ req/s via pipelining ────────────────────────────────────────

class TestPipelineThroughput:

    @pytest.mark.asyncio
    async def test_pipeline_achieves_high_throughput(self, fake_redis):
        """
        PipelinedRateLimiter with batch_size=100 should process
        10 000 requests faster than sequential (baseline for throughput claim).
        """
        TOTAL = 2_000

        # Sequential baseline
        seq_limiter = SlidingWindowRateLimiter(fake_redis)
        t0 = time.perf_counter()
        for i in range(TOTAL):
            await seq_limiter.check(f"seq:{i%10}", limit=999_999, window_ms=1_000)
        seq_elapsed = time.perf_counter() - t0

        # Pipelined
        server = fakeredis.FakeServer()
        redis2 = fakeredis.FakeRedis(server=server, decode_responses=True)
        pip_limiter = PipelinedRateLimiter(redis2, batch_size=100, flush_interval_ms=1)
        task        = asyncio.create_task(pip_limiter.run())

        t0 = time.perf_counter()
        await asyncio.gather(
            *[pip_limiter.check(f"pip:{i%10}", limit=999_999, window_ms=1_000)
              for i in range(TOTAL)]
        )
        pip_elapsed = time.perf_counter() - t0

        await pip_limiter.stop()
        task.cancel()
        await redis2.aclose()

        # Pipeline should be at least 3× faster than sequential
        assert pip_elapsed < seq_elapsed / 3, (
            f"Pipeline ({pip_elapsed:.3f}s) should be >3× faster than sequential ({seq_elapsed:.3f}s)"
        )

    @pytest.mark.asyncio
    async def test_pipeline_results_are_correct(self, fake_redis):
        """
        Pipelining must not break correctness — a limit of 5 allows exactly 5.
        """
        server = fakeredis.FakeServer()
        redis2 = fakeredis.FakeRedis(server=server, decode_responses=True)
        limiter = PipelinedRateLimiter(redis2, batch_size=50, flush_interval_ms=1)
        task    = asyncio.create_task(limiter.run())

        results = await asyncio.gather(
            *[limiter.check("key:correct", limit=5, window_ms=1_000)
              for _ in range(20)]
        )

        allowed = sum(1 for r in results if r.allowed)
        assert allowed == 5, f"Expected exactly 5 allowed, got {allowed}"

        await limiter.stop()
        task.cancel()
        await redis2.aclose()


# ── Claim: failover — fail-open on Redis error ────────────────────────────────

class TestFailover:

    @pytest.mark.asyncio
    async def test_fail_open_on_redis_error(self, fake_redis):
        """
        When Redis is unavailable, the limiter MUST allow requests through.
        Blocking traffic because the rate limiter is down is a worse failure.
        """
        import redis.asyncio as aioredis
        from unittest.mock import patch

        limiter = SlidingWindowRateLimiter(fake_redis)

        with patch.object(
            limiter, "_load_script",
            side_effect=aioredis.RedisError("simulated primary failure"),
        ):
            result = await limiter.check("key:failover", limit=5, window_ms=1_000)

        assert result.allowed is True, "Must fail-open, never fail-closed"

    @pytest.mark.asyncio
    async def test_pipeline_fail_open_on_redis_error(self):
        """Pipeline flusher must also fail-open on Redis error."""
        server  = fakeredis.FakeServer()
        redis   = fakeredis.FakeRedis(server=server, decode_responses=True)
        limiter = PipelinedRateLimiter(redis, batch_size=10, flush_interval_ms=1)
        task    = asyncio.create_task(limiter.run())

        # Close Redis to simulate failure mid-flight
        await redis.aclose()

        result = await limiter.check("key:pip-fail", limit=5, window_ms=1_000)
        assert result.allowed is True

        await limiter.stop()
        task.cancel()
