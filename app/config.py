from functools import lru_cache
from typing import Dict, List
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # Application
    app_name: str = "rate-limiter-service"
    app_version: str = "1.0.0"
    debug: bool = False
    log_level: str = "INFO"
    workers: int = 4

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_password: str = ""
    redis_max_connections: int = 100
    redis_socket_timeout: float = 0.5
    redis_sentinel_hosts: str = ""
    redis_sentinel_master: str = "mymaster"

    # Pipeline
    pipeline_batch_size: int = 100

    # Rate limiting
    default_limit_per_second: int = 10
    ip_limit_per_minute: int = 500
    endpoint_limit_per_second: int = 200

    tier_limits: Dict[str, Dict[str, int]] = {
        "free": {"limit": 100, "window_ms": 60_000},
        "pro": {"limit": 1_000, "window_ms": 60_000},
        "enterprise": {"limit": 10_000, "window_ms": 60_000},
    }

    # Networking
    trusted_proxies: List[str] = ["10.0.0.0/8", "172.16.0.0/12"]
    allowed_origins: List[str] = ["*"]

    # Metrics
    metrics_enabled: bool = True

    @field_validator("log_level")
    @classmethod
    def normalise(cls, v: str) -> str:
        return v.upper()


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
