from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class RetryPolicy:
    attempts: int = 3
    backoff_seconds: float = 0.5
    max_backoff_seconds: float = 5.0


def retry_call(
    func: Callable[[], T],
    *,
    policy: RetryPolicy,
    retry_exceptions: tuple[type[BaseException], ...],
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    if policy.attempts <= 0:
        raise ValueError("RetryPolicy.attempts must be positive.")

    for attempt in range(1, policy.attempts + 1):
        try:
            return func()
        except retry_exceptions:
            if attempt >= policy.attempts:
                raise
            delay = min(policy.backoff_seconds * (2 ** (attempt - 1)), policy.max_backoff_seconds)
            if delay > 0:
                sleep_fn(delay)

    raise RuntimeError("retry_call exhausted without returning or raising.")
