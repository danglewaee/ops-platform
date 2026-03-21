from __future__ import annotations

from .schemas import ScenarioMetadata


SERVICE_DEPENDENCIES: dict[str, list[str]] = {
    "gateway": ["auth", "payments", "router"],
    "auth": ["db"],
    "payments": ["db", "worker"],
    "worker": ["db"],
    "router": ["worker"],
    "db": [],
}


SCENARIOS: dict[str, ScenarioMetadata] = {
    "traffic_spike": ScenarioMetadata(
        name="traffic_spike",
        description="Demand surges at the edge, queues build up, and latency starts rising downstream.",
        root_cause="gateway",
        expected_action="scale_out",
        impacted_services=["gateway", "worker", "payments"],
    ),
    "bad_deploy": ScenarioMetadata(
        name="bad_deploy",
        description="A deploy introduces elevated errors in one service and contaminates downstream signals.",
        root_cause="payments",
        expected_action="rollback_candidate",
        impacted_services=["payments", "gateway", "worker"],
    ),
    "queue_backlog": ScenarioMetadata(
        name="queue_backlog",
        description="Consumers cannot keep up, queues swell, and p95 latency drifts upward.",
        root_cause="worker",
        expected_action="increase_consumers",
        impacted_services=["worker", "payments"],
    ),
    "memory_leak": ScenarioMetadata(
        name="memory_leak",
        description="One service degrades gradually as memory pressure increases and restarts follow.",
        root_cause="auth",
        expected_action="reroute_traffic",
        impacted_services=["auth", "gateway"],
    ),
}


def list_scenarios() -> list[str]:
    return sorted(SCENARIOS)

