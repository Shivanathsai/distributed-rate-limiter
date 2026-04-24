"""
main.py — FastAPI application
==============================
PipelinedRateLimiter is created once at startup, stored on app.state,
and shared by every request via the middleware.  The background flush
task is managed inside the lifespan context manager.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Path, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

import redis.asyncio as aioredis

from app.config import settings
from app.dependencies import get_redis_dep, init_redis, close_redis
from app.metrics import metrics_response, record_decision, redis_latency
from app.middleware import RateLimitMiddleware
from app.models import (
    BulkCheckRequest,
    CheckResponse,
    ErrorResponse,
    HealthResponse,
    PeekResponse,
    RateLimitInfo,
    ResetResponse,
)
from app.pipeline_limiter import PipelinedRateLimiter
from app.rate_limiter import SlidingWindowRateLimiter, RateLimitResult

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────
    logger.info("Starting %s v%s", settings.app_name, settings.app_version)

    redis_client = await init_redis()

    # Wire PipelinedRateLimiter into app.state — middleware reads from here.
    pipeline_limiter = PipelinedRateLimiter(
        redis_client,
        batch_size=settings.pipeline_batch_size,
    )
    app.state.pipeline_limiter = pipeline_limiter
    app.state.redis = redis_client

    # Start the background flush task (lives for the whole server lifetime).
    flush_task = asyncio.create_task(
        pipeline_limiter.run(),
        name="pipeline-flusher",
    )
    logger.info(
        "PipelinedRateLimiter started — batch_size=%d",
        settings.pipeline_batch_size,
    )

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    logger.info("Shutting down — draining pipeline...")
    flush_task.cancel()
    try:
        await flush_task
    except asyncio.CancelledError:
        pass

    await close_redis()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Distributed Rate Limiter",
        description=(
            "Sliding-window rate limiter using Redis sorted sets.\n\n"
            "**Algorithm**: Lua script — O(log n), atomic, race-condition-free.\n"
            "**Transport**: Redis pipeline — 100K+ req/s, sub-5ms p99."
        ),
        version=settings.app_version,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RateLimitMiddleware)
    return app


app = create_app()


def _to_info(result: RateLimitResult) -> RateLimitInfo:
    return RateLimitInfo(
        allowed=result.allowed,
        limit=result.limit,
        remaining=result.remaining,
        current_count=result.current_count,
        window_ms=result.window_ms,
        retry_after_ms=result.retry_after_ms,
    )


# ── Routes ───────────────────────────────────────────────────────────────────


@app.post(
    "/check",
    response_model=CheckResponse,
    responses={429: {"model": ErrorResponse}},
    tags=["Rate Limiting"],
)
async def check_rate_limit(
    key: str = Query(..., description="Rate-limit key, e.g. `ip:1.2.3.4`"),
    limit: int = Query(settings.default_limit_per_second, gt=0, le=100_000),
    window_ms: int = Query(1_000, gt=0, le=3_600_000),
) -> CheckResponse:
    limiter = app.state.pipeline_limiter

    t0 = time.perf_counter()
    result = await limiter.check(key, limit, window_ms)
    redis_latency.observe(time.perf_counter() - t0)

    record_decision(
        result.allowed, key_type=key.split(":")[0] if ":" in key else "custom"
    )

    if not result.allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "retry_after_ms": result.retry_after_ms,
                "rate_limit": _to_info(result).model_dump(),
            },
            headers=result.as_headers(),
        )

    return CheckResponse(status="allowed", key=key, rate_limit=_to_info(result))


@app.post("/check/bulk", tags=["Rate Limiting"])
async def check_bulk(body: BulkCheckRequest) -> dict:
    limiter = app.state.pipeline_limiter
    results = await asyncio.gather(
        *[limiter.check(k, body.limit, body.window_ms) for k in body.keys]
    )
    return {
        "results": [
            {
                "key": k,
                "allowed": r.allowed,
                "remaining": r.remaining,
                "retry_after_ms": r.retry_after_ms,
            }
            for k, r in zip(body.keys, results)
        ]
    }


@app.get("/peek/{key:path}", response_model=PeekResponse, tags=["Rate Limiting"])
async def peek(
    key: str = Path(...),
    window_ms: int = Query(1_000, gt=0),
    redis: aioredis.Redis = Depends(get_redis_dep),
) -> PeekResponse:
    limiter = SlidingWindowRateLimiter(redis)
    count = await limiter.peek(key, window_ms)
    return PeekResponse(key=key, current_count=count, window_ms=window_ms)


@app.delete("/reset/{key:path}", response_model=ResetResponse, tags=["Admin"])
async def reset(
    key: str = Path(...),
    redis: aioredis.Redis = Depends(get_redis_dep),
) -> ResetResponse:
    limiter = SlidingWindowRateLimiter(redis)
    deleted = await limiter.reset(key)
    return ResetResponse(key=key, reset=deleted)


@app.get("/health", response_model=HealthResponse, tags=["Ops"])
async def health() -> HealthResponse:
    return HealthResponse(status="ok", redis=True, version=settings.app_version)


@app.get("/ready", response_model=HealthResponse, tags=["Ops"])
async def ready(redis: aioredis.Redis = Depends(get_redis_dep)) -> HealthResponse:
    limiter = SlidingWindowRateLimiter(redis)
    redis_alive = await limiter.health()
    if not redis_alive:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return HealthResponse(
        status="ready", redis=redis_alive, version=settings.app_version
    )


@app.get("/metrics", include_in_schema=False, tags=["Ops"])
async def metrics() -> PlainTextResponse:
    body, content_type = metrics_response()
    return PlainTextResponse(content=body, media_type=content_type)
