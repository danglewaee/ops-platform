from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
import time
from typing import Any


@dataclass(slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: int | None = None


class InMemoryRateLimiter:
    def __init__(self, *, max_requests: int, window_seconds: int) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive.")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive.")
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def allow(self, key: str, *, now: float | None = None) -> RateLimitDecision:
        current_time = time.monotonic() if now is None else now
        with self._lock:
            events = self._events[key]
            cutoff = current_time - self.window_seconds
            while events and events[0] <= cutoff:
                events.popleft()

            if len(events) >= self.max_requests:
                retry_after_seconds = max(1, int(events[0] + self.window_seconds - current_time))
                return RateLimitDecision(
                    allowed=False,
                    remaining=0,
                    retry_after_seconds=retry_after_seconds,
                )

            events.append(current_time)
            remaining = max(self.max_requests - len(events), 0)
            return RateLimitDecision(allowed=True, remaining=remaining)


class RedisRateLimiter:
    def __init__(
        self,
        *,
        max_requests: int,
        window_seconds: int,
        redis_url: str | None = None,
        key_prefix: str = "ops-platform:rate-limit",
        redis_client: Any | None = None,
    ) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive.")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive.")
        if redis_client is None and not redis_url:
            raise ValueError("redis_url is required when redis_client is not provided.")

        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.key_prefix = key_prefix
        self.client = redis_client or _load_redis_client(redis_url)
        self.client.ping()

    def allow(self, key: str, *, now: float | None = None) -> RateLimitDecision:
        current_time = time.time() if now is None else now
        bucket = int(current_time // self.window_seconds)
        redis_key = f"{self.key_prefix}:{bucket}:{key}"
        count = self._increment(redis_key)
        remaining = max(self.max_requests - count, 0)
        if count > self.max_requests:
            retry_after_seconds = max(1, int(self.window_seconds - (current_time % self.window_seconds)))
            return RateLimitDecision(
                allowed=False,
                remaining=0,
                retry_after_seconds=retry_after_seconds,
            )
        return RateLimitDecision(allowed=True, remaining=remaining)

    def _increment(self, redis_key: str) -> int:
        if hasattr(self.client, "pipeline"):
            pipeline = self.client.pipeline()
            pipeline.incr(redis_key, 1)
            pipeline.expire(redis_key, self.window_seconds)
            count, _ = pipeline.execute()
            return int(count)

        count = self.client.incr(redis_key, 1)
        self.client.expire(redis_key, self.window_seconds)
        return int(count)


def build_rate_limiter(
    *,
    backend: str,
    max_requests: int,
    window_seconds: int,
    redis_url: str | None = None,
    redis_key_prefix: str = "ops-platform:rate-limit",
    redis_client: Any | None = None,
):
    normalized_backend = backend.strip().lower()
    if normalized_backend == "memory":
        return InMemoryRateLimiter(max_requests=max_requests, window_seconds=window_seconds)
    if normalized_backend == "redis":
        return RedisRateLimiter(
            max_requests=max_requests,
            window_seconds=window_seconds,
            redis_url=redis_url,
            key_prefix=redis_key_prefix,
            redis_client=redis_client,
        )
    raise ValueError(f"Unsupported rate limit backend '{backend}'.")


def _load_redis_client(redis_url: str):
    try:  # pragma: no cover - optional dependency import
        import redis
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency import
        raise RuntimeError(
            "Redis-backed rate limiting requires the redis package. Install it with `pip install -e .[security]` first."
        ) from exc
    return redis.from_url(redis_url, decode_responses=False)
