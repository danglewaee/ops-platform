from __future__ import annotations

import unittest

from ops_platform.security import InMemoryRateLimiter, RedisRateLimiter, build_rate_limiter


class _FakeRedisPipeline:
    def __init__(self, client) -> None:
        self.client = client
        self._ops: list[tuple[str, tuple[object, ...]]] = []

    def incr(self, key: str, amount: int = 1):
        self._ops.append(("incr", (key, amount)))
        return self

    def expire(self, key: str, ttl: int):
        self._ops.append(("expire", (key, ttl)))
        return self

    def execute(self):
        results: list[int | bool] = []
        for op, args in self._ops:
            if op == "incr":
                results.append(self.client.incr(*args))
            elif op == "expire":
                results.append(self.client.expire(*args))
        self._ops.clear()
        return results


class _FakeRedisClient:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def ping(self) -> bool:
        return True

    def pipeline(self):
        return _FakeRedisPipeline(self)

    def incr(self, key: str, amount: int = 1) -> int:
        self.counts[key] = self.counts.get(key, 0) + amount
        return self.counts[key]

    def expire(self, key: str, ttl: int) -> bool:
        return True


class SecurityTests(unittest.TestCase):
    def test_in_memory_rate_limiter_blocks_after_budget(self) -> None:
        limiter = InMemoryRateLimiter(max_requests=2, window_seconds=60)
        self.assertTrue(limiter.allow("client-a", now=10).allowed)
        self.assertTrue(limiter.allow("client-a", now=11).allowed)
        decision = limiter.allow("client-a", now=12)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.remaining, 0)

    def test_redis_rate_limiter_shares_counter_state(self) -> None:
        fake_client = _FakeRedisClient()
        limiter_a = RedisRateLimiter(
            max_requests=2,
            window_seconds=60,
            redis_client=fake_client,
            key_prefix="ops",
        )
        limiter_b = RedisRateLimiter(
            max_requests=2,
            window_seconds=60,
            redis_client=fake_client,
            key_prefix="ops",
        )

        self.assertTrue(limiter_a.allow("client-a", now=120).allowed)
        self.assertTrue(limiter_b.allow("client-a", now=121).allowed)
        decision = limiter_a.allow("client-a", now=122)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.remaining, 0)

    def test_build_rate_limiter_supports_redis_backend(self) -> None:
        limiter = build_rate_limiter(
            backend="redis",
            max_requests=10,
            window_seconds=60,
            redis_client=_FakeRedisClient(),
            redis_key_prefix="ops",
        )
        self.assertIsInstance(limiter, RedisRateLimiter)


if __name__ == "__main__":
    unittest.main()
