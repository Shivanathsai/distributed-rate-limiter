"""
API integration tests — uses FastAPI TestClient + fakeredis.
Tests the full HTTP stack: routing, headers, status codes, body.

Run: pytest tests/test_api.py -v
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

import fakeredis.aioredis as fakeredis

from app.main import app
from app.dependencies import _client as _global_client
import app.dependencies as deps


@pytest.fixture
def client(fake_redis):
    """TestClient with fakeredis injected."""
    # Override the get_redis dependency
    async def _override():
        yield fake_redis

    app.dependency_overrides[deps.get_redis_dep] = _override
    deps._client = fake_redis   # for middleware

    with TestClient(app, raise_server_exceptions=True) as c:
        yield c

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /check
# ---------------------------------------------------------------------------

class TestCheckEndpoint:

    def test_check_allowed(self, client):
        r = client.post("/check", params={"key": "ip:1.2.3.4", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "allowed"
        assert body["rate_limit"]["allowed"] is True

    def test_check_sets_ratelimit_headers(self, client):
        r = client.post("/check", params={"key": "ip:hdr", "limit": 10})
        assert "X-RateLimit-Limit"     in r.headers
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
        rem1 = r1.json()["rate_limit"]["remaining"]
        rem2 = r2.json()["rate_limit"]["remaining"]
        assert rem1 == 9
        assert rem2 == 8


# ---------------------------------------------------------------------------
# /check/bulk
# ---------------------------------------------------------------------------

class TestBulkEndpoint:

    def test_bulk_check(self, client):
        r = client.post("/check/bulk", json={
            "keys": ["user:a", "user:b", "user:c"],
            "limit": 10,
            "window_ms": 1000,
        })
        assert r.status_code == 200
        results = r.json()["results"]
        assert len(results) == 3
        assert all(res["allowed"] for res in results)

    def test_bulk_some_denied(self, client):
        # Fill user:x
        for _ in range(5):
            client.post("/check", params={"key": "user:x", "limit": 5})
        r = client.post("/check/bulk", json={
            "keys": ["user:x", "user:fresh"],
            "limit": 5,
            "window_ms": 1000,
        })
        results = r.json()["results"]
        x     = next(res for res in results if res["key"] == "user:x")
        fresh = next(res for res in results if res["key"] == "user:fresh")
        assert not x["allowed"]
        assert fresh["allowed"]


# ---------------------------------------------------------------------------
# /peek
# ---------------------------------------------------------------------------

class TestPeekEndpoint:

    def test_peek_empty(self, client):
        r = client.get("/peek/ip:unseen")
        assert r.status_code == 200
        assert r.json()["current_count"] == 0

    def test_peek_reflects_count(self, client):
        for _ in range(4):
            client.post("/check", params={"key": "ip:peek4", "limit": 10})
        r = client.get("/peek/ip:peek4")
        assert r.json()["current_count"] == 4


# ---------------------------------------------------------------------------
# /reset
# ---------------------------------------------------------------------------

class TestResetEndpoint:

    def test_reset_clears_key(self, client):
        for _ in range(5):
            client.post("/check", params={"key": "ip:rst", "limit": 5})
        assert client.get("/peek/ip:rst").json()["current_count"] == 5

        r = client.delete("/reset/ip:rst")
        assert r.status_code == 200
        assert r.json()["reset"] is True
        assert client.get("/peek/ip:rst").json()["current_count"] == 0

    def test_reset_nonexistent(self, client):
        r = client.delete("/reset/ip:ghost")
        assert r.json()["reset"] is False


# ---------------------------------------------------------------------------
# /health and /ready
# ---------------------------------------------------------------------------

class TestOpsEndpoints:

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_ready(self, client):
        r = client.get("/ready")
        assert r.status_code == 200
        assert r.json()["redis"] is True

    def test_metrics(self, client):
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "rate_limiter" in r.text
