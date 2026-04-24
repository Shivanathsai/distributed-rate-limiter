"""
test_middleware.py — verifies middleware uses PipelinedRateLimiter from app.state.
"""

from unittest.mock import AsyncMock, patch

import fakeredis
import fakeredis.aioredis as aioredis_fake
import pytest
from fastapi.testclient import TestClient

import app.dependencies as deps
from app.main import app
from app.pipeline_limiter import PipelinedRateLimiter


@pytest.fixture
def client_with_pipeline():
    server = fakeredis.FakeServer()
    redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)

    async def _dep_override():
        yield redis

    app.dependency_overrides[deps.get_redis_dep] = _dep_override
    deps._client = redis

    pl = PipelinedRateLimiter(redis, batch_size=50)

    with (
        patch("app.main.init_redis", new_callable=AsyncMock, return_value=redis),
        patch("app.main.close_redis", new_callable=AsyncMock),
    ):
        with TestClient(app, raise_server_exceptions=True) as c:
            if (
                not hasattr(app.state, "pipeline_limiter")
                or app.state.pipeline_limiter is None
            ):
                app.state.pipeline_limiter = pl
                app.state.redis = redis
            yield c

    app.dependency_overrides.clear()


class TestMiddlewareUsesPipeline:
    def test_requests_allowed_up_to_limit(self, client_with_pipeline):
        r = client_with_pipeline.post(
            "/check", params={"key": "mw-test", "limit": 5, "window_ms": 1000}
        )
        assert r.status_code == 200

    def test_middleware_sets_ratelimit_headers(self, client_with_pipeline):
        r = client_with_pipeline.post(
            "/check", params={"key": "mw-headers", "limit": 10}
        )
        assert r.headers.get("X-RateLimit-Limit") == "10"
        assert 0 <= int(r.headers.get("X-RateLimit-Remaining", -1)) <= 10

    def test_middleware_returns_429_when_limit_exceeded(self, client_with_pipeline):
        for _ in range(5):
            client_with_pipeline.post("/check", params={"key": "mw-deny", "limit": 5})
        r = client_with_pipeline.post("/check", params={"key": "mw-deny", "limit": 5})
        assert r.status_code == 429
        assert r.json()["detail"]["error"] == "rate_limit_exceeded"
        assert "Retry-After" in r.headers

    def test_health_endpoint_is_exempt_from_rate_limiting(self, client_with_pipeline):
        for _ in range(50):
            assert client_with_pipeline.get("/health").status_code == 200
