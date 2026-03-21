from __future__ import annotations

import json
from html import escape
from pathlib import Path

from .pipeline import run_pipeline
from .schemas import MetricSample
from .scenarios import list_scenarios
from .simulator import generate_scenario

SCENARIO_VISUALS = {
    "traffic_spike": {
        "metric": "request_rate",
        "label": "edge load",
        "color": "#5ea8ff",
        "accent": "#9fd1ff",
    },
    "bad_deploy": {
        "metric": "error_rate_pct",
        "label": "error rate",
        "color": "#ff728a",
        "accent": "#ffc2cf",
    },
    "queue_backlog": {
        "metric": "queue_depth",
        "label": "queue depth",
        "color": "#8b7cff",
        "accent": "#d0c8ff",
    },
    "memory_leak": {
        "metric": "cpu_pct",
        "label": "cpu pressure",
        "color": "#4fd4b6",
        "accent": "#b8f5e7",
    },
    "transient_noise": {
        "metric": "p95_latency_ms",
        "label": "latency wobble",
        "color": "#f4c35d",
        "accent": "#ffe4a1",
    },
}


def build_bundles() -> list[dict[str, object]]:
    return [build_bundle(name) for name in list_scenarios()]


def build_bundle(scenario_name: str) -> dict[str, object]:
    telemetry, _, _ = generate_scenario(scenario_name)
    report = run_pipeline(scenario_name)
    root_service = report.metadata.root_cause
    visual = SCENARIO_VISUALS[scenario_name]
    series = metric_series(telemetry, root_service, visual["metric"])

    primary_recommendation = report.recommendations[0] if report.recommendations else None
    summary = {
        "scenario": report.metadata.name,
        "description": report.metadata.description,
        "root_cause": report.metadata.root_cause,
        "expected_action": report.metadata.expected_action,
        "incident_count": report.evaluation.incident_count,
        "anomaly_count": report.evaluation.anomaly_count,
        "alert_reduction_pct": report.evaluation.alert_reduction_pct,
        "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
        "recommended_action_match": report.evaluation.recommended_action_match,
        "decision_latency_ms": report.evaluation.decision_latency_ms,
        "root_cause_candidates": report.incidents[0].root_cause_candidates if report.incidents else [],
        "recommendation": {
            "action": primary_recommendation.action if primary_recommendation else "none",
            "target_service": primary_recommendation.target_service if primary_recommendation else "",
            "confidence": primary_recommendation.confidence if primary_recommendation else 0,
            "projected_cost_delta_pct": (
                primary_recommendation.projected_cost_delta_pct if primary_recommendation else 0
            ),
            "projected_p95_delta_ms": (
                primary_recommendation.projected_p95_delta_ms if primary_recommendation else 0
            ),
        },
        "baseline_comparisons": report.evaluation.baseline_comparisons,
        "chart_metric": visual["metric"],
        "chart_series": series,
    }

    return {
        "report": report,
        "summary": summary,
        "visual": visual,
    }


def write_artifacts(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(exist_ok=True)
    bundles = build_bundles()

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps([bundle["summary"] for bundle in bundles], indent=2), encoding="utf-8")

    dashboard_path = output_dir / "dashboard.html"
    dashboard_path.write_text(render_dashboard(bundles), encoding="utf-8")
    return summary_path, dashboard_path


def metric_series(samples: list[MetricSample], service: str, metric: str) -> list[float]:
    series = [sample.value for sample in samples if sample.service == service and sample.metric == metric]
    return [round(value, 2) for value in series]


def render_dashboard(bundles: list[dict[str, object]]) -> str:
    cards = "\n".join(render_card(bundle) for bundle in bundles)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Ops Decision Platform - Shadow Mode</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #050607;
      --panel-border: rgba(255, 255, 255, 0.08);
      --text: #f5efe6;
      --muted: #a49a8f;
      --warm: rgba(240, 176, 91, 0.16);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top left, rgba(57, 87, 153, 0.18), transparent 28rem),
        radial-gradient(circle at bottom right, rgba(76, 138, 118, 0.14), transparent 30rem),
        var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Inter, system-ui, sans-serif;
    }}

    .shell {{
      width: min(1280px, calc(100vw - 64px));
      margin: 0 auto;
      padding: 40px 0 56px;
    }}

    .intro {{
      display: grid;
      gap: 18px;
      margin-bottom: 28px;
    }}

    .eyebrow {{
      margin: 0;
      color: #e1b44d;
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }}

    h1 {{
      margin: 0;
      font-size: clamp(3.2rem, 6vw, 5.4rem);
      line-height: 0.92;
      letter-spacing: -0.05em;
    }}

    .lede {{
      margin: 0;
      max-width: 34rem;
      color: var(--muted);
      font-size: 1.08rem;
      line-height: 1.65;
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 18px;
      margin-bottom: 22px;
    }}

    .summary-card, .scenario-card {{
      border: 1px solid var(--panel-border);
      border-radius: 28px;
      background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
      backdrop-filter: blur(14px);
    }}

    .summary-card {{
      padding: 20px 22px;
      display: grid;
      gap: 10px;
    }}

    .summary-label {{
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}

    .summary-value {{
      font-size: 2.2rem;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.04em;
    }}

    .summary-note {{
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .scenario-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 22px;
    }}

    .scenario-card {{
      padding: 24px;
      display: grid;
      gap: 18px;
      min-height: 360px;
    }}

    .scenario-top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }}

    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border: 1px solid var(--panel-border);
      border-radius: 999px;
      color: var(--muted);
      font-size: 0.8rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      white-space: nowrap;
    }}

    .scenario-title {{
      margin: 0;
      font-size: 2.05rem;
      line-height: 0.98;
      letter-spacing: -0.05em;
    }}

    .scenario-subtitle {{
      margin: 0;
      color: var(--muted);
      font-size: 0.96rem;
      line-height: 1.5;
      max-width: 25rem;
    }}

    .metric-strip {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }}

    .metric {{
      padding: 14px 14px 16px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid rgba(255, 255, 255, 0.05);
      display: grid;
      gap: 8px;
    }}

    .metric-label {{
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}

    .metric-value {{
      font-size: 1.55rem;
      font-weight: 800;
      line-height: 1;
      letter-spacing: -0.04em;
    }}

    .policy-grid {{
      display: grid;
      gap: 10px;
    }}

    .policy-row {{
      display: grid;
      grid-template-columns: 110px 1fr auto;
      gap: 14px;
      align-items: center;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.025);
      border: 1px solid rgba(255, 255, 255, 0.05);
    }}

    .policy-row--ours {{
      background: rgba(240, 176, 91, 0.08);
      border-color: rgba(240, 176, 91, 0.18);
    }}

    .policy-name {{
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}

    .policy-action {{
      font-size: 0.96rem;
      font-weight: 700;
      text-transform: capitalize;
    }}

    .policy-delta {{
      color: var(--muted);
      font-size: 0.88rem;
      white-space: nowrap;
    }}

    .chart-shell {{
      border-radius: 22px;
      border: 1px solid rgba(255, 255, 255, 0.06);
      background:
        linear-gradient(180deg, rgba(255,255,255,0.02), transparent 35%),
        rgba(5, 8, 12, 0.78);
      padding: 16px 16px 14px;
      display: grid;
      gap: 12px;
    }}

    .chart-top {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 14px;
    }}

    .chart-label {{
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}

    .chart-service {{
      font-size: 1.1rem;
      font-weight: 700;
    }}

    .svg-frame {{
      width: 100%;
      height: 146px;
      display: block;
    }}

    .footer-row {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      flex-wrap: wrap;
    }}

    .caption {{
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.5;
    }}

    .action {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 12px 14px;
      border-radius: 999px;
      background: var(--warm);
      color: var(--text);
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      font-size: 0.82rem;
    }}

    @media (max-width: 980px) {{
      .summary-grid,
      .scenario-grid,
      .metric-strip {{
        grid-template-columns: 1fr;
      }}

      .policy-row {{
        grid-template-columns: 1fr;
      }}

      .shell {{
        width: min(100vw - 32px, 1280px);
        padding-top: 28px;
      }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="intro">
      <p class="eyebrow">Shadow Mode Evaluation</p>
      <h1>From noisy signals to safer actions.</h1>
      <p class="lede">A minimal decision layer for distributed systems: detect, correlate, forecast, recommend.</p>
    </section>
    <section class="summary-grid">
      {render_summary_cards(bundles)}
    </section>
    <section class="scenario-grid">
      {cards}
    </section>
  </main>
</body>
</html>"""


def render_summary_cards(bundles: list[dict[str, object]]) -> str:
    summaries = [bundle["summary"] for bundle in bundles]
    total_alert_reduction = sum(summary["alert_reduction_pct"] for summary in summaries) / len(summaries)
    action_match_count = sum(1 for summary in summaries if summary["recommended_action_match"])
    threshold_match_count = sum(
        1
        for summary in summaries
        for baseline in summary["baseline_comparisons"]
        if baseline["policy"] == "threshold_autoscaling" and baseline["recommended_action_match"]
    )
    avg_latency = sum(summary["decision_latency_ms"] for summary in summaries) / len(summaries)

    cards = [
        ("scenario set", str(len(summaries)), "controlled failure cases"),
        ("ours", f"{action_match_count}/{len(summaries)}", "expected shadow-mode actions"),
        ("threshold", f"{threshold_match_count}/{len(summaries)}", "reactive threshold policy"),
        ("alert reduction", f"{total_alert_reduction:.1f}%", f"{avg_latency:.3f} ms decision latency"),
    ]
    return "\n".join(
        f"""
        <article class="summary-card">
          <div class="summary-label">{label}</div>
          <div class="summary-value">{value}</div>
          <div class="summary-note">{note}</div>
        </article>
        """
        for label, value, note in cards
    )


def render_card(bundle: dict[str, object]) -> str:
    summary = bundle["summary"]
    visual = bundle["visual"]
    recommendation = summary["recommendation"]
    chart = sparkline_svg(summary["chart_series"], visual["color"], visual["accent"])
    policy_rows = [render_policy_row("ours", recommendation, True)]
    policy_rows.extend(render_policy_row(baseline["policy"], baseline) for baseline in summary["baseline_comparisons"])

    metrics = [
        ("anomalies", str(summary["anomaly_count"])),
        ("incidents", str(summary["incident_count"])),
        ("root cause", summary["root_cause"]),
        ("action", recommendation["action"].replace("_", " ")),
    ]
    metric_markup = "\n".join(
        f"""
        <div class="metric">
          <div class="metric-label">{escape(label)}</div>
          <div class="metric-value">{escape(value)}</div>
        </div>
        """
        for label, value in metrics
    )

    candidates = ", ".join(summary["root_cause_candidates"][:3]) or "n/a"
    return f"""
      <article class="scenario-card">
        <div class="scenario-top">
          <span class="chip">{escape(summary["scenario"].replace("_", " "))}</span>
          <span class="chip">{escape(summary["expected_action"].replace("_", " "))}</span>
        </div>
        <div>
          <h2 class="scenario-title">{title_case(summary["scenario"])}</h2>
          <p class="scenario-subtitle">{escape(summary["description"])}</p>
        </div>
        <div class="metric-strip">
          {metric_markup}
        </div>
        <div class="policy-grid">
          {"".join(policy_rows)}
        </div>
        <section class="chart-shell">
          <div class="chart-top">
            <div>
              <div class="chart-label">{escape(visual["label"])}</div>
              <div class="chart-service">{escape(summary["root_cause"])}</div>
            </div>
            <div class="chart-label">{escape(candidates)}</div>
          </div>
          {chart}
        </section>
        <div class="footer-row">
          <div class="caption">
            {summary["alert_reduction_pct"]:.1f}% alert compression · top-2 root cause {status_word(summary["top2_root_cause_hit"])} · our action {status_word(summary["recommended_action_match"])}
          </div>
          <div class="action">{escape(recommendation["action"].replace("_", " "))}</div>
        </div>
      </article>
    """


def sparkline_svg(series: list[float], color: str, accent: str) -> str:
    width = 560
    height = 146
    padding_x = 12
    padding_y = 14
    inner_width = width - padding_x * 2
    inner_height = height - padding_y * 2
    minimum = min(series)
    maximum = max(series)
    span = maximum - minimum or 1

    points = []
    for index, value in enumerate(series):
        x = padding_x + (inner_width * index / max(len(series) - 1, 1))
        normalized = (value - minimum) / span
        y = height - padding_y - normalized * inner_height
        points.append((x, y))

    line_points = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    area_points = (
        f"{padding_x},{height - padding_y} "
        + line_points
        + f" {width - padding_x},{height - padding_y}"
    )
    latest_value = series[-1]

    grid = "".join(
        f'<line x1="{padding_x}" y1="{padding_y + i * (inner_height / 3):.1f}" x2="{width - padding_x}" y2="{padding_y + i * (inner_height / 3):.1f}" stroke="rgba(255,255,255,0.07)" stroke-width="1" />'
        for i in range(4)
    )

    return f"""
      <svg class="svg-frame" viewBox="0 0 {width} {height}" fill="none" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="fill-{color[1:]}" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="{accent}" stop-opacity="0.34"/>
            <stop offset="100%" stop-color="{accent}" stop-opacity="0.04"/>
          </linearGradient>
        </defs>
        {grid}
        <polygon points="{area_points}" fill="url(#fill-{color[1:]})"/>
        <polyline points="{line_points}" fill="none" stroke="{color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="{points[-1][0]:.1f}" cy="{points[-1][1]:.1f}" r="5" fill="{color}" />
        <text x="{width - padding_x}" y="{padding_y + 2}" fill="{accent}" font-size="12" font-family="Segoe UI, sans-serif" text-anchor="end">{format_metric_value(latest_value)}</text>
      </svg>
    """


def format_metric_value(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k"
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def title_case(name: str) -> str:
    return " ".join(part.capitalize() for part in name.split("_"))


def status_word(value: bool) -> str:
    return "hit" if value else "miss"


def render_policy_row(policy: str, recommendation: dict[str, object], ours: bool = False) -> str:
    label = {
        "ours": "ours",
        "threshold_autoscaling": "threshold",
        "no_action": "no action",
    }.get(policy, policy.replace("_", " "))
    action = str(recommendation["action"]).replace("_", " ")
    latency = float(recommendation.get("projected_p95_delta_ms", recommendation.get("average_p95_delta_ms", 0.0)))
    cost = float(recommendation.get("projected_cost_delta_pct", recommendation.get("average_cost_delta_pct", 0.0)))
    sign_latency = "+" if latency > 0 else ""
    sign_cost = "+" if cost > 0 else ""
    class_name = "policy-row policy-row--ours" if ours else "policy-row"
    return f"""
      <div class="{class_name}">
        <div class="policy-name">{escape(label)}</div>
        <div class="policy-action">{escape(action)}</div>
        <div class="policy-delta">Δp95 {sign_latency}{latency:.0f} ms · cost {sign_cost}{cost:.0f}%</div>
      </div>
    """
