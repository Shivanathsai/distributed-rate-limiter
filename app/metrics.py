"""
Prometheus metrics for the rate limiter.

Tracks:
  - Total allow / deny decisions (counter, labelled by key_type)
  - Redis operation latency histogram
  - Active keys gauge (sampled)
"""

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# Use a dedicated registry so tests can create isolated instances
REGISTRY = CollectorRegistry(auto_describe=True)

requests_total = Counter(
    "rate_limiter_requests_total",
    "Total rate limit decisions",
    ["decision", "key_type"],          # decision: allowed | denied
    registry=REGISTRY,
)

redis_latency = Histogram(
    "rate_limiter_redis_duration_seconds",
    "Redis Lua script execution time",
    buckets=[0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100],
    registry=REGISTRY,
)

active_keys = Gauge(
    "rate_limiter_active_keys",
    "Number of non-expired rate limit keys in Redis (sampled)",
    registry=REGISTRY,
)


def record_decision(allowed: bool, key_type: str = "ip") -> None:
    decision = "allowed" if allowed else "denied"
    requests_total.labels(decision=decision, key_type=key_type).inc()


def metrics_response() -> tuple[bytes, str]:
    """Return (body, content_type) for the /metrics endpoint."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
