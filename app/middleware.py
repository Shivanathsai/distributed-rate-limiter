"""
middleware.py — RateLimitMiddleware
=====================================
Reads the shared PipelinedRateLimiter from app.state (set at startup
in main.py lifespan).  Every non-exempt request goes through one
pipeline.check() call — batched by the background flusher, never
one-at-a-time.
"""

import ipaddress
import logging
from typing import Callable, List

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from app.config import settings

logger = logging.getLogger(__name__)

_EXEMPT = frozenset({"/health", "/ready", "/metrics", "/docs", "/openapi.json", "/redoc"})

_TRUSTED_RANGES: List[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
for _cidr in settings.trusted_proxies:
    try:
        _TRUSTED_RANGES.append(ipaddress.ip_network(_cidr, strict=False))
    except ValueError:
        logger.warning("Invalid trusted proxy CIDR: %s", _cidr)


def _real_ip(request: Request) -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        ips = [ip.strip() for ip in xff.split(",")]
        for ip in reversed(ips):
            try:
                addr = ipaddress.ip_address(ip)
                if not any(addr in net for net in _TRUSTED_RANGES):
                    return ip
            except ValueError:
                continue
        return ips[0]
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-IP sliding-window rate limiter middleware.

    Uses the singleton PipelinedRateLimiter on app.state — every request
    is batched into a Redis pipeline, never a standalone round-trip.
    Fails open if the limiter is not yet initialised (startup race) or
    if Redis is unavailable.
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in _EXEMPT:
            return await call_next(request)

        # Read the pipelined limiter wired in at startup
        limiter = getattr(request.app.state, "pipeline_limiter", None)
        if limiter is None:
            # Startup not complete yet — fail open
            logger.warning("pipeline_limiter not ready — failing open for %s", request.url.path)
            return await call_next(request)

        client_ip = _real_ip(request)
        key       = f"rl:ip:{client_ip}"
        limit     = settings.default_limit_per_second
        window    = 1_000   # ms

        result = await limiter.check(key, limit, window)
        headers = result.as_headers()

        if result.allowed:
            response = await call_next(request)
            for k, v in headers.items():
                response.headers[k] = v
            return response

        logger.warning(
            "Rate limit exceeded ip=%s count=%d/%d",
            client_ip, result.current_count, result.limit,
        )
        return JSONResponse(
            status_code=429,
            content={
                "error":          "rate_limit_exceeded",
                "message":        "Too many requests.",
                "retry_after_ms": result.retry_after_ms,
                "limit":          result.limit,
                "window_ms":      result.window_ms,
            },
            headers={**headers, "Content-Type": "application/json"},
        )
