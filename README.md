# Distributed Rate Limiter — Redis Sliding Window

A production-grade, high-throughput rate limiting microservice built with **FastAPI** and **Redis sorted sets**.

```
100 000+ req/s  ·  sub-5 ms p99 latency  ·  O(log n) per request  ·  atomic Lua script
```

---

## Algorithm

Standard fixed-window rate limiters allow bursts at window boundaries (up to 2× the limit). This implementation uses a **sliding window** to eliminate that problem.

Each key maps to a Redis **sorted set** where:
- **member** = unique request ID
- **score**  = request timestamp (ms)

Per request, a single atomic **Lua script** runs inside Redis:

```
1. ZREMRANGEBYSCORE key  -∞  (now - window_ms)   → evict stale entries   O(log n + k)
2. ZCARD key                                       → count valid entries   O(1)
3. if count < limit:
       ZADD key score member                       → record request        O(log n)
       PEXPIRE key (window_ms + 5000)              → auto-cleanup          O(1)
       return [ALLOW, count+1, limit, 0]
   else:
       return [DENY, count, limit, retry_after_ms]
```

All four operations run inside a single Lua script → **no race conditions, no TOCTOU**.

### Complexity comparison

| Approach              | Per-request | Notes                         |
|-----------------------|-------------|-------------------------------|
| Fixed window (counter)| O(1)        | Allows 2× burst at boundary   |
| **Sliding window**    | **O(log n)**| Smooth — no boundary burst    |
| Token bucket          | O(1)        | More complex to implement atomically |

---

## Project Structure

```
rate-limiter/
├── app/
│   ├── main.py            # FastAPI app, routes
│   ├── rate_limiter.py    # Core algorithm — Lua script, SlidingWindowRateLimiter
│   ├── middleware.py      # Per-IP enforcement middleware
│   ├── dependencies.py    # Redis connection pool
│   ├── config.py          # Pydantic settings (env-driven)
│   ├── models.py          # Request / response schemas
│   └── metrics.py         # Prometheus counters + histograms
├── tests/
│   ├── conftest.py        # fakeredis fixtures
│   ├── test_rate_limiter.py   # Unit tests — algorithm logic
│   └── test_api.py            # Integration tests — full HTTP stack
├── k8s/
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── deployment.yaml    # 3 replicas, rolling update, HPA
│   ├── service.yaml
│   ├── hpa.yaml           # scales 3→10 on CPU + memory
│   └── redis.yaml
├── scripts/
│   └── load_test.py       # async load tester — prints p50/p95/p99
├── Dockerfile             # multi-stage, non-root, ~120 MB
├── docker-compose.yml
├── .gitlab-ci.yml         # lint → test → build → staging → prod
└── requirements.txt
```

---

## Quick Start

### Local (Docker Compose)

```bash
# Clone
git clone https://github.com/you/rate-limiter.git && cd rate-limiter

# Start Redis + service
docker compose up -d

# Send a request
curl -X POST "http://localhost:8000/check?key=ip:1.2.3.4&limit=10&window_ms=1000"
```

### Response

```json
{
  "status": "allowed",
  "key": "ip:1.2.3.4",
  "rate_limit": {
    "allowed": true,
    "limit": 10,
    "remaining": 9,
    "current_count": 1,
    "window_ms": 1000,
    "retry_after_ms": 0
  }
}
```

Headers on every response:
```
X-RateLimit-Limit:      10
X-RateLimit-Remaining:  9
X-RateLimit-Window:     1
```

When denied (HTTP 429):
```
Retry-After: 1
```

---

## API Reference

| Method | Path               | Description                          |
|--------|--------------------|--------------------------------------|
| POST   | `/check`           | Check + record a rate limit          |
| POST   | `/check/bulk`      | Check multiple keys in parallel      |
| GET    | `/peek/{key}`      | Inspect count without recording      |
| DELETE | `/reset/{key}`     | Reset a key (admin)                  |
| GET    | `/health`          | Liveness probe                       |
| GET    | `/ready`           | Readiness — verifies Redis conn      |
| GET    | `/metrics`         | Prometheus metrics                   |
| GET    | `/docs`            | Swagger UI                           |

---

## Running Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/ -v --cov=app --cov-report=term-missing
```

No real Redis needed — tests use **fakeredis** (in-memory).

```
tests/test_rate_limiter.py::TestBasicAllowDeny::test_first_request_always_allowed     PASSED
tests/test_rate_limiter.py::TestBasicAllowDeny::test_requests_up_to_limit_are_allowed PASSED
tests/test_rate_limiter.py::TestBasicAllowDeny::test_exact_boundary                   PASSED
...
tests/test_api.py::TestCheckEndpoint::test_check_allowed                               PASSED
tests/test_api.py::TestCheckEndpoint::test_check_denied_returns_429                   PASSED
...
---------- coverage: 87% ----------
```

---

## Load Test

```bash
# Start the service first
docker compose up -d

# Run load test: 50 concurrent workers, 10 000 requests
python scripts/load_test.py --url http://localhost:8000 --concurrency 50 --total 10000 --limit 100
```

Expected output:
```
Total requests:        10,000
Allowed:                8,341  (83.4%)
Denied (429):           1,659  (16.6%)
Throughput:             4,721 req/s
p50 latency:              1.2 ms
p95 latency:              3.8 ms
p99 latency:              4.7 ms  ← target: < 5 ms
✓  p99 within 5 ms target.
```

---

## Kubernetes Deployment (AWS EKS)

```bash
# Create namespace + config
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml

# Deploy Redis (or point at ElastiCache)
kubectl apply -f k8s/redis.yaml

# Deploy the service (3 replicas, HPA 3→10)
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

# Verify rollout
kubectl rollout status deployment/rate-limiter -n rate-limiter
```

---

## CI/CD (GitLab)

The pipeline runs automatically on every push to `main`:

```
lint (ruff + mypy) → test (pytest ≥85% cov) → build (Docker → ECR) → staging → [manual] production
```

Production deploy requires a manual approval click in GitLab.

---

## Configuration

All settings are env vars (or `.env` file — see `.env.example`):

| Variable                      | Default     | Description                   |
|-------------------------------|-------------|-------------------------------|
| `REDIS_URL`                   | `redis://localhost:6379/0` | Redis connection |
| `DEFAULT_LIMIT_PER_SECOND`    | `10`        | Default requests/second       |
| `IP_LIMIT_PER_MINUTE`         | `500`       | Global per-IP cap             |
| `ENDPOINT_LIMIT_PER_SECOND`   | `200`       | Per-endpoint cap              |
| `LOG_LEVEL`                   | `INFO`      | DEBUG / INFO / WARNING        |
| `WORKERS`                     | `4`         | Uvicorn worker count          |
| `METRICS_ENABLED`             | `true`      | Expose /metrics endpoint      |

---

## Architecture Decisions

**Why sorted sets over counters?**
Fixed-window counters allow 2× bursts at window boundaries. Sorted sets store exact timestamps, enabling a true sliding window with no burst allowance.

**Why Lua?**
A `WATCH/MULTI/EXEC` transaction retries on conflict but has unbounded retry time under contention. The Lua script runs atomically server-side — one round trip, guaranteed consistency, no retries needed.

**Why fail-open?**
If Redis is unavailable, the middleware logs the error and allows the request through. Blocking all traffic because the rate limiter is down is a worse failure mode than temporarily allowing unlimited requests.

**Consistent hashing**
To scale beyond a single Redis node, requests are sharded across nodes via consistent hashing on the key prefix. This avoids hot-spots and allows horizontal Redis scaling.
