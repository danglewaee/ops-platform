from __future__ import annotations

from collections import defaultdict

from .scenarios import SERVICE_DEPENDENCIES
from .schemas import Anomaly, ChangeEvent, Incident

SEVERITY_WEIGHT = {
    "low": 1.0,
    "medium": 2.0,
    "high": 3.5,
    "critical": 5.0,
}


def correlate_incidents(anomalies: list[Anomaly], change_events: list[ChangeEvent]) -> list[Incident]:
    if not anomalies:
        return []

    clusters: list[list[Anomaly]] = []
    for anomaly in anomalies:
        if not clusters:
            clusters.append([anomaly])
            continue

        current = clusters[-1]
        if _belongs_to_cluster(anomaly, current):
            current.append(anomaly)
        else:
            clusters.append([anomaly])

    change_lookup = {(event.service, event.step): event for event in change_events}
    incidents: list[Incident] = []

    for index, cluster in enumerate(clusters, start=1):
        service_scores: dict[str, float] = defaultdict(float)
        trigger = _find_trigger_event(cluster, change_events)
        for anomaly in cluster:
            score = SEVERITY_WEIGHT[anomaly.severity]
            if anomaly.metric in {"p95_latency_ms", "error_rate_pct", "queue_depth"}:
                score += 0.8
            if trigger and anomaly.service == trigger.service:
                score += 2.5
                if anomaly.metric in {"error_rate_pct", "p95_latency_ms"}:
                    score += 1.5
            service_scores[anomaly.service] += score

            if (anomaly.service, anomaly.step) in change_lookup:
                service_scores[anomaly.service] += 5.0

        ranked_services = [
            service for service, _ in sorted(service_scores.items(), key=lambda item: item[1], reverse=True)
        ]
        severity = max(cluster, key=lambda anomaly: SEVERITY_WEIGHT[anomaly.severity]).severity
        impacted = sorted({anomaly.service for anomaly in cluster})

        if trigger and trigger.service in ranked_services:
            ranked_services.remove(trigger.service)
            ranked_services.insert(0, trigger.service)

        incidents.append(
            Incident(
                incident_id=f"incident-{index}",
                opened_at=cluster[0].timestamp,
                services=impacted,
                root_cause_candidates=ranked_services[:3],
                severity=severity,
                trigger_event=trigger.description if trigger else None,
                anomaly_count=len(cluster),
                summary=_summarize_cluster(cluster, ranked_services[:3]),
            )
        )

    return incidents


def _belongs_to_cluster(candidate: Anomaly, cluster: list[Anomaly]) -> bool:
    latest_step = max(anomaly.step for anomaly in cluster)
    if candidate.step - latest_step > 2:
        return False

    cluster_services = {anomaly.service for anomaly in cluster}
    if candidate.service in cluster_services:
        return True

    return any(_services_related(candidate.service, service) for service in cluster_services)


def _services_related(left: str, right: str) -> bool:
    if right in SERVICE_DEPENDENCIES.get(left, []):
        return True
    if left in SERVICE_DEPENDENCIES.get(right, []):
        return True
    return False


def _find_trigger_event(cluster: list[Anomaly], events: list[ChangeEvent]) -> ChangeEvent | None:
    first_step = min(anomaly.step for anomaly in cluster)
    last_step = max(anomaly.step for anomaly in cluster)
    cluster_services = {anomaly.service for anomaly in cluster}

    for event in events:
        if event.service in cluster_services and first_step - 1 <= event.step <= last_step:
            return event
    return None


def _summarize_cluster(cluster: list[Anomaly], root_candidates: list[str]) -> str:
    services = sorted({anomaly.service for anomaly in cluster})
    service_list = ", ".join(services[:3])
    root_hint = root_candidates[0] if root_candidates else services[0]
    return (
        f"Correlated {len(cluster)} anomaly signals across {service_list}; "
        f"likely root cause starts with {root_hint}."
    )
