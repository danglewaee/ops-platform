from __future__ import annotations

from .schemas import ScenarioMetadata
from .testbed import ALL_SERVICE_DEPENDENCIES, BOUTIQUE_LIKE_PROFILE, CORE_PROFILE, list_testbed_profiles

SCENARIOS: dict[str, ScenarioMetadata] = {
    "traffic_spike": ScenarioMetadata(
        name="traffic_spike",
        description="Demand surges at the edge, queues build up, and latency starts rising downstream.",
        root_cause="gateway",
        expected_action="scale_out",
        impacted_services=["gateway", "worker", "payments"],
        category="capacity",
        testbed_profile=CORE_PROFILE.name,
    ),
    "bad_deploy": ScenarioMetadata(
        name="bad_deploy",
        description="A deploy introduces elevated errors in one service and contaminates downstream signals.",
        root_cause="payments",
        expected_action="rollback_candidate",
        impacted_services=["payments", "gateway", "worker"],
        category="change",
        testbed_profile=CORE_PROFILE.name,
    ),
    "queue_backlog": ScenarioMetadata(
        name="queue_backlog",
        description="Consumers cannot keep up, queues swell, and p95 latency drifts upward.",
        root_cause="worker",
        expected_action="increase_consumers",
        impacted_services=["worker", "payments"],
        category="throughput",
        testbed_profile=CORE_PROFILE.name,
    ),
    "memory_leak": ScenarioMetadata(
        name="memory_leak",
        description="One service degrades gradually as memory pressure increases and restarts follow.",
        root_cause="auth",
        expected_action="reroute_traffic",
        impacted_services=["auth", "gateway"],
        category="degradation",
        testbed_profile=CORE_PROFILE.name,
    ),
    "transient_noise": ScenarioMetadata(
        name="transient_noise",
        description="A short-lived latency wobble creates local noise, but the system should avoid overreacting.",
        root_cause="gateway",
        expected_action="hold_steady",
        impacted_services=["gateway"],
        category="transient",
        testbed_profile=CORE_PROFILE.name,
    ),
}

BOUTIQUE_SCENARIOS: dict[str, ScenarioMetadata] = {
    "boutique_frontend_spike": ScenarioMetadata(
        name="boutique_frontend_spike",
        description="Storefront demand surges at the frontend, pushing checkout and recommendation paths toward higher latency.",
        root_cause="frontend",
        expected_action="scale_out",
        impacted_services=["frontend", "checkout", "recommendation"],
        category="capacity",
        testbed_profile=BOUTIQUE_LIKE_PROFILE.name,
    ),
    "boutique_bad_canary": ScenarioMetadata(
        name="boutique_bad_canary",
        description="A checkout canary release introduces user-visible errors that spread into the order path.",
        root_cause="checkout",
        expected_action="rollback_candidate",
        impacted_services=["checkout", "frontend", "payment"],
        category="change",
        testbed_profile=BOUTIQUE_LIKE_PROFILE.name,
    ),
    "boutique_payment_timeout": ScenarioMetadata(
        name="boutique_payment_timeout",
        description="The payment dependency starts timing out, contaminating checkout latency while the safer move is to reroute away from the failing path.",
        root_cause="payment",
        expected_action="reroute_traffic",
        impacted_services=["payment", "checkout", "frontend"],
        category="dependency",
        testbed_profile=BOUTIQUE_LIKE_PROFILE.name,
    ),
    "boutique_email_backlog": ScenarioMetadata(
        name="boutique_email_backlog",
        description="Order confirmation workers fall behind, queue depth spikes, and the order path begins to back up.",
        root_cause="email",
        expected_action="increase_consumers",
        impacted_services=["email", "checkout"],
        category="throughput",
        testbed_profile=BOUTIQUE_LIKE_PROFILE.name,
    ),
    "boutique_cache_jitter": ScenarioMetadata(
        name="boutique_cache_jitter",
        description="A brief cart-path jitter creates local latency noise, but the safer decision is to avoid unnecessary action churn.",
        root_cause="cart",
        expected_action="hold_steady",
        impacted_services=["cart", "frontend"],
        category="transient",
        testbed_profile=BOUTIQUE_LIKE_PROFILE.name,
    ),
}

TESTBED_SCENARIOS: dict[str, dict[str, ScenarioMetadata]] = {
    CORE_PROFILE.name: SCENARIOS,
    BOUTIQUE_LIKE_PROFILE.name: BOUTIQUE_SCENARIOS,
}

ALL_SCENARIOS: dict[str, ScenarioMetadata] = {
    **SCENARIOS,
    **BOUTIQUE_SCENARIOS,
}

SERVICE_DEPENDENCIES: dict[str, list[str]] = ALL_SERVICE_DEPENDENCIES


def list_scenarios(*, profile: str | None = None) -> list[str]:
    if profile is None:
        return sorted(SCENARIOS)
    if profile not in TESTBED_SCENARIOS:
        available = ", ".join(list_testbed_profiles())
        raise ValueError(f"Unknown testbed profile '{profile}'. Available: {available}")
    return sorted(TESTBED_SCENARIOS[profile])


def get_scenario_metadata(name: str) -> ScenarioMetadata:
    if name not in ALL_SCENARIOS:
        available = ", ".join(sorted(ALL_SCENARIOS))
        raise ValueError(f"Unknown scenario '{name}'. Available: {available}")
    return ALL_SCENARIOS[name]


def resolve_scenario_profile(name: str) -> str:
    return get_scenario_metadata(name).testbed_profile
