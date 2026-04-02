from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TestbedProfile:
    name: str
    description: str
    services: tuple[str, ...]
    dependencies: dict[str, list[str]]
    baselines: dict[str, dict[str, float]]


DEFAULT_TESTBED_PROFILE = "core"

CORE_PROFILE = TestbedProfile(
    name="core",
    description="Compact distributed-systems profile used for the flagship deterministic benchmark.",
    services=("gateway", "auth", "payments", "worker", "router", "db"),
    dependencies={
        "gateway": ["auth", "payments", "router"],
        "auth": ["db"],
        "payments": ["db", "worker"],
        "worker": ["db"],
        "router": ["worker"],
        "db": [],
    },
    baselines={
        "gateway": {"request_rate": 1800, "p95_latency_ms": 95, "error_rate_pct": 0.4, "queue_depth": 4, "cpu_pct": 48},
        "auth": {"request_rate": 1200, "p95_latency_ms": 88, "error_rate_pct": 0.3, "queue_depth": 2, "cpu_pct": 42},
        "payments": {"request_rate": 900, "p95_latency_ms": 92, "error_rate_pct": 0.5, "queue_depth": 3, "cpu_pct": 44},
        "worker": {"request_rate": 700, "p95_latency_ms": 110, "error_rate_pct": 0.2, "queue_depth": 6, "cpu_pct": 54},
        "router": {"request_rate": 950, "p95_latency_ms": 84, "error_rate_pct": 0.2, "queue_depth": 1, "cpu_pct": 38},
        "db": {"request_rate": 2500, "p95_latency_ms": 55, "error_rate_pct": 0.1, "queue_depth": 1, "cpu_pct": 58},
    },
)

BOUTIQUE_LIKE_PROFILE = TestbedProfile(
    name="boutique_like",
    description="Microservice retail topology inspired by a production storefront workload with checkout, payment, cart, and recommendation paths.",
    services=(
        "frontend",
        "checkout",
        "cart",
        "productcatalog",
        "payment",
        "shipping",
        "recommendation",
        "email",
        "currency",
        "redis",
    ),
    dependencies={
        "frontend": ["cart", "productcatalog", "recommendation", "checkout", "currency"],
        "checkout": ["payment", "shipping", "email", "cart"],
        "cart": ["redis"],
        "productcatalog": [],
        "payment": [],
        "shipping": [],
        "recommendation": ["productcatalog"],
        "email": [],
        "currency": [],
        "redis": [],
    },
    baselines={
        "frontend": {"request_rate": 2200, "p95_latency_ms": 82, "error_rate_pct": 0.25, "queue_depth": 2, "cpu_pct": 44},
        "checkout": {"request_rate": 680, "p95_latency_ms": 108, "error_rate_pct": 0.35, "queue_depth": 4, "cpu_pct": 47},
        "cart": {"request_rate": 950, "p95_latency_ms": 76, "error_rate_pct": 0.18, "queue_depth": 2, "cpu_pct": 36},
        "productcatalog": {"request_rate": 1040, "p95_latency_ms": 70, "error_rate_pct": 0.14, "queue_depth": 1, "cpu_pct": 34},
        "payment": {"request_rate": 620, "p95_latency_ms": 96, "error_rate_pct": 0.22, "queue_depth": 2, "cpu_pct": 38},
        "shipping": {"request_rate": 480, "p95_latency_ms": 91, "error_rate_pct": 0.15, "queue_depth": 3, "cpu_pct": 41},
        "recommendation": {"request_rate": 760, "p95_latency_ms": 86, "error_rate_pct": 0.24, "queue_depth": 3, "cpu_pct": 39},
        "email": {"request_rate": 420, "p95_latency_ms": 81, "error_rate_pct": 0.1, "queue_depth": 6, "cpu_pct": 45},
        "currency": {"request_rate": 1400, "p95_latency_ms": 61, "error_rate_pct": 0.05, "queue_depth": 1, "cpu_pct": 30},
        "redis": {"request_rate": 2600, "p95_latency_ms": 18, "error_rate_pct": 0.05, "queue_depth": 1, "cpu_pct": 43},
    },
)

TESTBED_PROFILES: dict[str, TestbedProfile] = {
    DEFAULT_TESTBED_PROFILE: CORE_PROFILE,
    BOUTIQUE_LIKE_PROFILE.name: BOUTIQUE_LIKE_PROFILE,
}

ALL_SERVICE_DEPENDENCIES: dict[str, list[str]] = {}
ALL_BASELINES: dict[str, dict[str, float]] = {}
for profile in TESTBED_PROFILES.values():
    ALL_SERVICE_DEPENDENCIES.update(profile.dependencies)
    ALL_BASELINES.update(profile.baselines)


def resolve_testbed_profile(name: str | None = None) -> TestbedProfile:
    profile_name = name or DEFAULT_TESTBED_PROFILE
    if profile_name not in TESTBED_PROFILES:
        available = ", ".join(sorted(TESTBED_PROFILES))
        raise ValueError(f"Unknown testbed profile '{profile_name}'. Available: {available}")
    return TESTBED_PROFILES[profile_name]


def list_testbed_profiles() -> list[str]:
    return sorted(TESTBED_PROFILES)
