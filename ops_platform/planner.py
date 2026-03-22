from __future__ import annotations

from dataclasses import dataclass

from .schemas import DecisionConstraints, Recommendation


@dataclass(slots=True)
class ActionCandidate:
    recommendation: Recommendation
    score: float


def select_recommendations(
    candidates_by_incident: list[list[ActionCandidate]],
    *,
    planner_mode: str,
    constraints: DecisionConstraints | None,
) -> tuple[list[Recommendation], str]:
    filtered_candidates = [_filter_candidates(candidates, constraints) for candidates in candidates_by_incident]
    if any(not candidates for candidates in filtered_candidates):
        return [], "heuristic"

    if planner_mode == "cp_sat":
        planned = _select_with_cp_sat(filtered_candidates, constraints)
        if planned is not None:
            return planned, "cp_sat"

    return _heuristic_plan(filtered_candidates, constraints), "heuristic"


def _filter_candidates(
    candidates: list[ActionCandidate],
    constraints: DecisionConstraints | None,
) -> list[ActionCandidate]:
    allowed = [candidate for candidate in candidates if _recommendation_allowed(candidate.recommendation, constraints)]
    if allowed:
        return allowed

    hold_steady = [candidate for candidate in candidates if candidate.recommendation.action == "hold_steady"]
    return hold_steady


def _heuristic_choice(candidates: list[ActionCandidate]) -> ActionCandidate:
    return max(candidates, key=lambda candidate: candidate.score)


def _heuristic_plan(
    candidates_by_incident: list[list[ActionCandidate]],
    constraints: DecisionConstraints | None,
) -> list[Recommendation]:
    if constraints is None or constraints.max_total_cost_delta_pct is None:
        return [_heuristic_choice(candidates).recommendation for candidates in candidates_by_incident]

    remaining_budget = constraints.max_total_cost_delta_pct
    recommendations: list[Recommendation] = []

    for candidates in candidates_by_incident:
        budget_safe = [
            candidate
            for candidate in candidates
            if max(candidate.recommendation.projected_cost_delta_pct, 0.0) <= remaining_budget
        ]
        selected = _heuristic_choice(budget_safe or candidates)
        recommendations.append(selected.recommendation)
        remaining_budget -= max(selected.recommendation.projected_cost_delta_pct, 0.0)

    return recommendations


def _recommendation_allowed(
    recommendation: Recommendation,
    constraints: DecisionConstraints | None,
) -> bool:
    if constraints is None:
        return True

    action = recommendation.action
    if action == "hold_steady" and not constraints.allow_hold_steady:
        return False
    if action == "reroute_traffic" and not constraints.allow_reroute_traffic:
        return False
    if action == "scale_out" and not constraints.allow_scale_out:
        return False
    if action == "increase_consumers" and not constraints.allow_increase_consumers:
        return False
    if action == "rollback_candidate" and not constraints.allow_rollback_candidate:
        return False

    if (
        constraints.max_cost_delta_pct_per_action is not None
        and recommendation.projected_cost_delta_pct > constraints.max_cost_delta_pct_per_action
    ):
        return False

    if (
        constraints.max_allowed_p95_delta_ms is not None
        and recommendation.projected_p95_delta_ms > constraints.max_allowed_p95_delta_ms
    ):
        return False

    return True


def _select_with_cp_sat(
    candidates_by_incident: list[list[ActionCandidate]],
    constraints: DecisionConstraints | None,
) -> list[Recommendation] | None:
    try:  # pragma: no cover - optional dependency import
        from ortools.sat.python import cp_model
    except ModuleNotFoundError:
        return None

    model = cp_model.CpModel()
    choice_vars: dict[tuple[int, int], object] = {}

    for incident_index, candidates in enumerate(candidates_by_incident):
        incident_vars = []
        for candidate_index, candidate in enumerate(candidates):
            variable = model.NewBoolVar(f"incident_{incident_index}_candidate_{candidate_index}")
            choice_vars[(incident_index, candidate_index)] = variable
            incident_vars.append(variable)
        model.Add(sum(incident_vars) == 1)

    if constraints and constraints.max_total_cost_delta_pct is not None:
        model.Add(
            sum(
                int(max(candidate.recommendation.projected_cost_delta_pct, 0.0) * 100) * choice_vars[(incident_index, candidate_index)]
                for incident_index, candidates in enumerate(candidates_by_incident)
                for candidate_index, candidate in enumerate(candidates)
            )
            <= int(constraints.max_total_cost_delta_pct * 100)
        )

    objective_terms = [
        int(candidate.score * 1000) * choice_vars[(incident_index, candidate_index)]
        for incident_index, candidates in enumerate(candidates_by_incident)
        for candidate_index, candidate in enumerate(candidates)
    ]
    model.Maximize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 0.5
    status = solver.Solve(model)
    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return None

    recommendations: list[Recommendation] = []
    for incident_index, candidates in enumerate(candidates_by_incident):
        for candidate_index, candidate in enumerate(candidates):
            if solver.Value(choice_vars[(incident_index, candidate_index)]) == 1:
                recommendations.append(candidate.recommendation)
                break
    return recommendations
