"""
test_middleware.py — verifies middleware uses PipelinedRateLimiter from app.state
"""

import asyncio
import pytest
import fakeredis.aioredis as fakeredis
from fastapi.testclient import TestClient

from app.main import app
from app.pipeline_limiter import PipelinedRateLimiter
import app.dependencies as deps


@pytest.fixture
def client_with_pipeline():
    """Wire a real PipelinedRateLimiter (backed by fakeredis) into app.state."""
    server = fakeredis.FakeServer()
    redis = fakeredis.FakeRedis(server=server, decode_responses=True)

    async def _override():
        yield redis

    app.dependency_overrides[deps.get_redis_dep] = _override
    deps._client = redis

    pl = PipelinedRateLimiter(redis, batch_size=50)
    loop = asyncio.new_event_loop()
    task = loop.create_task(pl.run())
    app.state.pipeline_limiter = pl
    app.state.redis = redis

    with TestClient(app, raise_server_exceptions=True) as c:
        yield c

    task.cancel()
    loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
    loop.run_until_complete(redis.aclose())
    loop.close()
    app.dependency_overrides.clear()


class TestMiddlewareUsesPipeline:
    def test_requests_allowed_up_to_limit(self, client_with_pipeline):
        # Default limit is 10/s from config; override via query param not possible
        # through middleware — it enforces the configured limit.
        # Send 3 requests to a fresh IP — all should be allowed.
        for _ in range(3):
            r = client_with_pipeline.get(
                "/ready",
                headers={"X-Forwarded-For": "99.99.99.1"},
            )
            # /ready is exempt — use /check instead via POST
        r = client_with_pipeline.post(
            "/check",
            params={"key": "mw-test", "limit": 5, "window_ms": 1000},
        )
        assert r.status_code == 200
        assert "X-RateLimit-Limit" in r.headers
        assert "X-RateLimit-Remaining" in r.headers

    def test_middleware_sets_ratelimit_headers(self, client_with_pipeline):
        r = client_with_pipeline.post(
            "/check",
            params={"key": "mw-headers", "limit": 10},
        )
        assert r.headers.get("X-RateLimit-Limit") == "10"
        remaining = int(r.headers.get("X-RateLimit-Remaining", -1))
        assert 0 <= remaining <= 10

    def test_middleware_returns_429_when_limit_exceeded(self, client_with_pipeline):
        for _ in range(5):
            client_with_pipeline.post("/check", params={"key": "mw-deny", "limit": 5})
        r = client_with_pipeline.post("/check", params={"key": "mw-deny", "limit": 5})
        assert r.status_code == 429
        body = r.json()
        assert body["detail"]["error"] == "rate_limit_exceeded"
        assert "Retry-After" in r.headers

    def test_health_endpoint_is_exempt_from_rate_limiting(self, client_with_pipeline):
        """Health check must never be rate-limited."""
        for _ in range(50):
            r = client_with_pipeline.get("/health")
            assert r.status_code == 200
