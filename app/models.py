"""Response models used by the API."""

from pydantic import BaseModel, Field


class RateLimitInfo(BaseModel):
    allowed: bool
    limit: int
    remaining: int
    current_count: int
    window_ms: int
    retry_after_ms: int = 0

    model_config = {
        "json_schema_extra": {
            "example": {
                "allowed": True,
                "limit": 10,
                "remaining": 7,
                "current_count": 3,
                "window_ms": 1000,
                "retry_after_ms": 0,
            }
        }
    }


class CheckResponse(BaseModel):
    status: str = Field(..., examples=["allowed", "denied"])
    rate_limit: RateLimitInfo
    key: str


class HealthResponse(BaseModel):
    status: str
    redis: bool
    version: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    status_code: int


class ResetResponse(BaseModel):
    key: str
    reset: bool


class PeekResponse(BaseModel):
    key: str
    current_count: int
    window_ms: int


class BulkCheckRequest(BaseModel):
    keys: list[str]
    limit: int = Field(10, gt=0, le=100_000)
    window_ms: int = Field(1_000, gt=0, le=3_600_000)

    model_config = {
        "json_schema_extra": {
            "example": {
                "keys": ["user:alice", "user:bob"],
                "limit": 10,
                "window_ms": 1000,
            }
        }
    }
