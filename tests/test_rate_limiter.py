import asyncio
from unittest.mock import patch

import fakeredis
import fakeredis.aioredis as aioredis_fake
import pytest

from app.rate_limiter import RateLimitResult, SlidingWindowRateLimiter


async def _allow_n(limiter, key, n, limit=10, window=1_000):
    return [await limiter.check(key, limit, window) for _ in range(n)]


class TestBasicAllowDeny:
    @pytest.mark.asyncio
    async def test_first_request_always_allowed(self, limiter):
        result = await limiter.check("key:test", limit=5, window_ms=1_000)
        assert result.allowed is True
        assert result.current_count == 1
        assert result.remaining == 4

    @pytest.mark.asyncio
    async def test_requests_up_to_limit_are_allowed(self, limiter):
        results = await _allow_n(limiter, "key:basic", 10, limit=10)
        assert all(r.allowed for r in results)
        assert results[-1].current_count == 10
        assert results[-1].remaining == 0

    @pytest.mark.asyncio
    async def test_request_exceeding_limit_is_denied(self, limiter):
        await _allow_n(limiter, "key:deny", 10, limit=10)
        result = await limiter.check("key:deny", limit=10, window_ms=1_000)
        assert result.allowed is False
        assert result.current_count == 10

    @pytest.mark.asyncio
    async def test_exact_boundary(self, limiter):
        key = "key:boundary"
        for i in range(1, 11):
            r = await limiter.check(key, limit=10, window_ms=1_000)
            assert r.allowed, f"Request {i} should be allowed"
        denied = await limiter.check(key, limit=10, window_ms=1_000)
        assert not denied.allowed

    @pytest.mark.asyncio
    async def test_different_keys_are_independent(self, limiter):
        await _allow_n(limiter, "key:a", 10, limit=10)
        result_a = await limiter.check("key:a", limit=10, window_ms=1_000)
        result_b = await limiter.check("key:b", limit=10, window_ms=1_000)
        assert not result_a.allowed
        assert result_b.allowed

    @pytest.mark.asyncio
    async def test_limit_of_one(self, limiter):
        r1 = await limiter.check("key:one", limit=1, window_ms=1_000)
        r2 = await limiter.check("key:one", limit=1, window_ms=1_000)
        assert r1.allowed
        assert not r2.allowed

    @pytest.mark.asyncio
    async def test_very_large_limit(self, limiter):
        results = await _allow_n(limiter, "key:large", 1_000, limit=10_000)
        assert all(r.allowed for r in results)


class TestWindowSemantics:
    @pytest.mark.asyncio
    async def test_result_carries_window_ms(self, limiter):
        result = await limiter.check("key:win", limit=5, window_ms=5_000)
        assert result.window_ms == 5_000

    @pytest.mark.asyncio
    async def test_retry_after_is_zero_when_allowed(self, limiter):
        result = await limiter.check("key:ra", limit=5, window_ms=1_000)
        assert result.retry_after_ms == 0

    @pytest.mark.asyncio
    async def test_retry_after_is_positive_when_denied(self, limiter):
        await _allow_n(limiter, "key:retry", 5, limit=5)
        result = await limiter.check("key:retry", limit=5, window_ms=1_000)
        assert not result.allowed
        assert result.retry_after_ms > 0
        assert result.retry_after_ms <= 1_000


class TestPeekAndReset:
    @pytest.mark.asyncio
    async def test_peek_returns_zero_for_empty_key(self, limiter):
        count = await limiter.peek("key:empty", window_ms=1_000)
        assert count == 0

    @pytest.mark.asyncio
    async def test_peek_does_not_increment(self, limiter):
        await _allow_n(limiter, "key:peek", 3, limit=10)
        before = await limiter.peek("key:peek", window_ms=1_000)
        await limiter.peek("key:peek", window_ms=1_000)
        after = await limiter.peek("key:peek", window_ms=1_000)
        assert before == after == 3

    @pytest.mark.asyncio
    async def test_reset_clears_key(self, limiter):
        await _allow_n(limiter, "key:reset", 10, limit=10)
        assert (await limiter.peek("key:reset")) == 10
        deleted = await limiter.reset("key:reset")
        assert deleted is True
        assert (await limiter.peek("key:reset")) == 0

    @pytest.mark.asyncio
    async def test_reset_nonexistent_key_returns_false(self, limiter):
        result = await limiter.reset("key:ghost")
        assert result is False

    @pytest.mark.asyncio
    async def test_after_reset_requests_are_allowed_again(self, limiter):
        key = "key:refill"
        await _allow_n(limiter, key, 5, limit=5)
        assert not (await limiter.check(key, 5, 1_000)).allowed
        await limiter.reset(key)
        assert (await limiter.check(key, 5, 1_000)).allowed


class TestRateLimitResult:
    def _result(self, allowed, count, limit, retry=0):
        return RateLimitResult(
            allowed=allowed,
            current_count=count,
            limit=limit,
            window_ms=1_000,
            retry_after_ms=retry,
        )

    def test_remaining_allowed(self):
        assert self._result(True, 3, 10).remaining == 7

    def test_remaining_at_limit(self):
        assert self._result(True, 10, 10).remaining == 0

    def test_remaining_never_negative(self):
        assert self._result(False, 10, 10).remaining == 0

    def test_retry_after_seconds_conversion(self):
        r = self._result(False, 10, 10, retry=750)
        assert r.retry_after_seconds == pytest.approx(0.75)

    def test_headers_allowed(self):
        h = self._result(True, 3, 10).as_headers()
        assert h["X-RateLimit-Limit"] == "10"
        assert h["X-RateLimit-Remaining"] == "7"
        assert "Retry-After" not in h

    def test_headers_denied(self):
        h = self._result(False, 10, 10, retry=500).as_headers()
        assert "Retry-After" in h
        assert int(h["Retry-After"]) >= 1


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_requests_respect_limit(self):
        server = fakeredis.FakeServer()
        redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)
        limiter = SlidingWindowRateLimiter(redis)
        results = await asyncio.gather(
            *[limiter.check("key:concurrent", 20, 1_000) for _ in range(50)]
        )
        assert sum(1 for r in results if r.allowed) == 20
        assert sum(1 for r in results if not r.allowed) == 30

    @pytest.mark.asyncio
    async def test_no_count_exceeds_limit_under_concurrent_load(self):
        server = fakeredis.FakeServer()
        redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)
        limiter = SlidingWindowRateLimiter(redis)
        results = await asyncio.gather(
            *[limiter.check("key:safe", 10, 1_000) for _ in range(100)]
        )
        assert max(r.current_count for r in results if r.allowed) <= 10


class TestFailOpen:
    @pytest.mark.asyncio
    async def test_redis_error_allows_request(self, fake_redis):
        """Patch eval directly — fail-open on Redis error."""
        import redis.asyncio as aioredis

        limiter = SlidingWindowRateLimiter(fake_redis)
        # Patch the underlying redis client's eval to raise RedisError
        with patch.object(
            fake_redis,
            "eval",
            side_effect=aioredis.RedisError("simulated failure"),
        ):
            result = await limiter.check("key:fail", limit=5, window_ms=1_000)
        assert result.allowed is True


class TestHealth:
    @pytest.mark.asyncio
    async def test_health_returns_true_when_redis_up(self, limiter):
        assert await limiter.health() is True
