from __future__ import annotations

from collections import defaultdict

from .scenarios import SERVICE_DEPENDENCIES
from .schemas import Anomaly, ChangeEvent, Incident, IncidentEvidence, IncidentGraphEdge

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
        service_metric_scores: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        metric_scores: dict[str, float] = defaultdict(float)
        anomalies_by_service: dict[str, list[Anomaly]] = defaultdict(list)
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
            service_metric_scores[anomaly.service][anomaly.metric] += score
            metric_scores[anomaly.metric] += score
            anomalies_by_service[anomaly.service].append(anomaly)

            if (anomaly.service, anomaly.step) in change_lookup:
                service_scores[anomaly.service] += 5.0
                service_metric_scores[anomaly.service][anomaly.metric] += 5.0
                metric_scores[anomaly.metric] += 5.0

        ranked_services = [
            service for service, _ in sorted(service_scores.items(), key=lambda item: item[1], reverse=True)
        ]
        severity = max(cluster, key=lambda anomaly: SEVERITY_WEIGHT[anomaly.severity]).severity
        impacted = sorted({anomaly.service for anomaly in cluster})
        top_signals = _top_signals(metric_scores)

        if trigger and trigger.service in ranked_services:
            ranked_services.remove(trigger.service)
            ranked_services.insert(0, trigger.service)

        blast_radius_services = _build_blast_radius(impacted)
        graph_edges = _build_graph_edges(ranked_services[:3], blast_radius_services, impacted)
        evidence = _build_incident_evidence(
            ranked_services=ranked_services[:3],
            trigger=trigger,
            anomalies_by_service=anomalies_by_service,
            service_metric_scores=service_metric_scores,
            impacted_services=impacted,
            graph_edges=graph_edges,
        )

        incidents.append(
            Incident(
                incident_id=f"incident-{index}",
                opened_at=cluster[0].timestamp,
                services=impacted,
                root_cause_candidates=ranked_services[:3],
                severity=severity,
                trigger_event=trigger.description if trigger else None,
                anomaly_count=len(cluster),
                summary=_summarize_cluster(cluster, ranked_services[:3], top_signals, blast_radius_services),
                blast_radius_services=blast_radius_services,
                top_signals=top_signals,
                evidence=evidence,
                graph_edges=graph_edges,
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


def _top_signals(metric_scores: dict[str, float]) -> list[str]:
    ranked_metrics = sorted(metric_scores.items(), key=lambda item: item[1], reverse=True)
    return [metric for metric, _ in ranked_metrics[:3]]


def _build_blast_radius(impacted_services: list[str]) -> list[str]:
    radius = set(impacted_services)
    for service in impacted_services:
        radius.update(SERVICE_DEPENDENCIES.get(service, []))
        radius.update(REVERSE_DEPENDENCIES.get(service, []))
    return sorted(radius)


def _build_graph_edges(
    ranked_services: list[str],
    blast_radius_services: list[str],
    impacted_services: list[str],
) -> list[IncidentGraphEdge]:
    graph_services = set(blast_radius_services)
    impacted = set(impacted_services)
    edges: dict[tuple[str, str, str], IncidentGraphEdge] = {}

    for source_service, dependencies in SERVICE_DEPENDENCIES.items():
        if source_service not in graph_services:
            continue
        for target_service in dependencies:
            if target_service not in graph_services:
                continue
            weight = 2.4 if source_service in impacted and target_service in impacted else 1.6
            edges[(source_service, target_service, "depends_on")] = IncidentGraphEdge(
                source_service=source_service,
                target_service=target_service,
                relation="depends_on",
                weight=weight,
            )

    if ranked_services:
        root_service = ranked_services[0]
        for downstream_service in _reachable_downstream(root_service):
            if downstream_service == root_service or downstream_service not in graph_services:
                continue
            weight = 2.8 if downstream_service in impacted else 1.8
            edges[(root_service, downstream_service, "impacts")] = IncidentGraphEdge(
                source_service=root_service,
                target_service=downstream_service,
                relation="impacts",
                weight=weight,
            )

    return sorted(edges.values(), key=lambda edge: (-edge.weight, edge.source_service, edge.target_service, edge.relation))


def _reachable_downstream(service: str) -> list[str]:
    visited: set[str] = set()
    frontier = list(REVERSE_DEPENDENCIES.get(service, []))

    while frontier:
        current = frontier.pop(0)
        if current in visited:
            continue
        visited.add(current)
        frontier.extend(REVERSE_DEPENDENCIES.get(current, []))

    return sorted(visited)


def _build_incident_evidence(
    *,
    ranked_services: list[str],
    trigger: ChangeEvent | None,
    anomalies_by_service: dict[str, list[Anomaly]],
    service_metric_scores: dict[str, dict[str, float]],
    impacted_services: list[str],
    graph_edges: list[IncidentGraphEdge],
) -> list[IncidentEvidence]:
    evidence: list[IncidentEvidence] = []

    if trigger is not None:
        evidence.append(
            IncidentEvidence(
                evidence_type="change_event",
                service=trigger.service,
                signal=trigger.event_type,
                summary=trigger.description,
                weight=9.0,
            )
        )

    for service in ranked_services:
        anomalies = anomalies_by_service.get(service, [])
        if not anomalies:
            continue
        ranked_metrics = sorted(
            service_metric_scores[service].items(),
            key=lambda item: item[1],
            reverse=True,
        )
        dominant_metrics = [metric for metric, _ in ranked_metrics[:2]]
        strongest_metric = dominant_metrics[0] if dominant_metrics else None
        latest_step = max(anomaly.step for anomaly in anomalies)
        severity = max(anomalies, key=lambda anomaly: SEVERITY_WEIGHT[anomaly.severity]).severity
        metric_phrase = ", ".join(_metric_label(metric) for metric in dominant_metrics) or "telemetry drift"
        evidence.append(
            IncidentEvidence(
                evidence_type="anomaly_cluster",
                service=service,
                signal=strongest_metric,
                summary=(
                    f"{service} shows {severity} anomaly pressure in {metric_phrase} "
                    f"around step {latest_step}."
                ),
                weight=round(sum(score for _, score in ranked_metrics), 3),
            )
        )

    for edge in graph_edges:
        if edge.relation != "depends_on":
            continue
        if edge.source_service not in impacted_services and edge.target_service not in impacted_services:
            continue
        evidence.append(
            IncidentEvidence(
                evidence_type="dependency_path",
                service=edge.source_service,
                signal=None,
                summary=(
                    f"{edge.source_service} depends on {edge.target_service}, keeping that path "
                    f"inside the incident blast radius."
                ),
                weight=edge.weight,
            )
        )

    evidence.sort(key=lambda item: item.weight, reverse=True)
    return evidence[:6]


def _metric_label(metric: str) -> str:
    return metric.replace("_pct", "").replace("_ms", "").replace("_", " ")


def _summarize_cluster(
    cluster: list[Anomaly],
    root_candidates: list[str],
    top_signals: list[str],
    blast_radius_services: list[str],
) -> str:
    services = sorted({anomaly.service for anomaly in cluster})
    service_list = ", ".join(services[:3])
    root_hint = root_candidates[0] if root_candidates else services[0]
    signal_hint = _metric_label(top_signals[0]) if top_signals else "telemetry drift"
    blast_hint = ", ".join(blast_radius_services[:4])
    return (
        f"Correlated {len(cluster)} anomaly signals across {service_list}; "
        f"likely root cause starts with {root_hint} via {signal_hint}. "
        f"Blast radius includes {blast_hint}."
    )


def _build_reverse_dependencies() -> dict[str, list[str]]:
    reverse: dict[str, list[str]] = {service: [] for service in SERVICE_DEPENDENCIES}
    for service, dependencies in SERVICE_DEPENDENCIES.items():
        for dependency in dependencies:
            reverse.setdefault(dependency, []).append(service)
    return {service: sorted(consumers) for service, consumers in reverse.items()}


REVERSE_DEPENDENCIES: dict[str, list[str]] = _build_reverse_dependencies()
