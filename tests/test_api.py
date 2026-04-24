"""
test_api.py — full HTTP stack tests.
Mocks init_redis/close_redis so the lifespan never touches real Redis.
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
def client():
    server = fakeredis.FakeServer()
    redis = aioredis_fake.FakeRedis(server=server, decode_responses=True)

    async def _dep_override():
        yield redis

    app.dependency_overrides[deps.get_redis_dep] = _dep_override
    deps._client = redis

    pl = PipelinedRateLimiter(redis, batch_size=50)

    # Patch lifespan's init_redis and close_redis so no real connection
    with (
        patch("app.main.init_redis", new_callable=AsyncMock, return_value=redis),
        patch("app.main.close_redis", new_callable=AsyncMock),
    ):
        with TestClient(app, raise_server_exceptions=True) as c:
            # Ensure pipeline_limiter is on app.state (lifespan sets it,
            # but our mock returns fakeredis so it works)
            if (
                not hasattr(app.state, "pipeline_limiter")
                or app.state.pipeline_limiter is None
            ):
                app.state.pipeline_limiter = pl
                app.state.redis = redis
            yield c

    app.dependency_overrides.clear()


class TestCheckEndpoint:
    def test_check_allowed(self, client):
        r = client.post("/check", params={"key": "ip:1.2.3.4", "limit": 10})
        assert r.status_code == 200
        assert r.json()["status"] == "allowed"

    def test_check_sets_ratelimit_headers(self, client):
        r = client.post("/check", params={"key": "ip:hdr", "limit": 10})
        assert "X-RateLimit-Limit" in r.headers
        assert "X-RateLimit-Remaining" in r.headers

    def test_check_denied_returns_429(self, client):
        for _ in range(5):
            client.post("/check", params={"key": "ip:burst", "limit": 5})
        r = client.post("/check", params={"key": "ip:burst", "limit": 5})
        assert r.status_code == 429
        assert r.json()["detail"]["error"] == "rate_limit_exceeded"

    def test_check_429_includes_retry_after(self, client):
        for _ in range(5):
            client.post("/check", params={"key": "ip:retry", "limit": 5})
        r = client.post("/check", params={"key": "ip:retry", "limit": 5})
        assert "Retry-After" in r.headers

    def test_check_remaining_decrements(self, client):
        r1 = client.post("/check", params={"key": "ip:dec", "limit": 10})
        r2 = client.post("/check", params={"key": "ip:dec", "limit": 10})
        assert r1.json()["rate_limit"]["remaining"] == 9
        assert r2.json()["rate_limit"]["remaining"] == 8


class TestBulkEndpoint:
    def test_bulk_check(self, client):
        r = client.post(
            "/check/bulk",
            json={
                "keys": ["user:a", "user:b", "user:c"],
                "limit": 10,
                "window_ms": 1000,
            },
        )
        assert r.status_code == 200
        assert len(r.json()["results"]) == 3

    def test_bulk_some_denied(self, client):
        for _ in range(5):
            client.post("/check", params={"key": "user:x", "limit": 5})
        r = client.post(
            "/check/bulk",
            json={
                "keys": ["user:x", "user:fresh"],
                "limit": 5,
                "window_ms": 1000,
            },
        )
        results = r.json()["results"]
        x = next(res for res in results if res["key"] == "user:x")
        fresh = next(res for res in results if res["key"] == "user:fresh")
        assert not x["allowed"]
        assert fresh["allowed"]


class TestPeekEndpoint:
    def test_peek_empty(self, client):
        r = client.get("/peek/ip:unseen")
        assert r.status_code == 200
        assert r.json()["current_count"] == 0

    def test_peek_reflects_count(self, client):
        for _ in range(4):
            client.post("/check", params={"key": "ip:peek4", "limit": 10})
        assert client.get("/peek/ip:peek4").json()["current_count"] == 4


class TestResetEndpoint:
    def test_reset_clears_key(self, client):
        for _ in range(5):
            client.post("/check", params={"key": "ip:rst", "limit": 5})
        r = client.delete("/reset/ip:rst")
        assert r.json()["reset"] is True
        assert client.get("/peek/ip:rst").json()["current_count"] == 0

    def test_reset_nonexistent(self, client):
        assert client.delete("/reset/ip:ghost").json()["reset"] is False


class TestOpsEndpoints:
    def test_health(self, client):
        assert client.get("/health").json()["status"] == "ok"

    def test_ready(self, client):
        r = client.get("/ready")
        assert r.status_code == 200
        assert r.json()["redis"] is True

    def test_metrics(self, client):
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "rate_limiter" in r.text
