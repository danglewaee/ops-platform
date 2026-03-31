from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

from .pipeline import run_pipeline
from .schemas import MetricSample
from .scenarios import list_scenarios
from .simulator import generate_scenario
from .storage import get_storage_stats, list_ingested_streams, load_ingested_stream

SCENARIO_VISUALS = {
    "traffic_spike": {"metric": "request_rate", "label": "edge load", "color": "#5ea8ff", "accent": "#9fd1ff"},
    "bad_deploy": {"metric": "error_rate_pct", "label": "error rate", "color": "#ff728a", "accent": "#ffc2cf"},
    "queue_backlog": {"metric": "queue_depth", "label": "queue depth", "color": "#8b7cff", "accent": "#d0c8ff"},
    "memory_leak": {"metric": "cpu_pct", "label": "cpu pressure", "color": "#4fd4b6", "accent": "#b8f5e7"},
    "transient_noise": {"metric": "p95_latency_ms", "label": "latency wobble", "color": "#f4c35d", "accent": "#ffe4a1"},
}

LIVE_VISUALS = {
    "scale_out": {"metric": "request_rate", "label": "demand pressure", "color": "#5ea8ff", "accent": "#9fd1ff"},
    "scale_in": {"metric": "request_rate", "label": "demand pressure", "color": "#5ea8ff", "accent": "#9fd1ff"},
    "increase_consumers": {"metric": "queue_depth", "label": "queue pressure", "color": "#8b7cff", "accent": "#d0c8ff"},
    "reroute_traffic": {"metric": "p95_latency_ms", "label": "latency risk", "color": "#4fd4b6", "accent": "#b8f5e7"},
    "rollback_candidate": {"metric": "error_rate_pct", "label": "error drift", "color": "#ff728a", "accent": "#ffc2cf"},
    "hold_steady": {"metric": "p95_latency_ms", "label": "latency watch", "color": "#f4c35d", "accent": "#ffe4a1"},
}

FALLBACK_LIVE_VISUALS = [
    {"metric": "request_rate", "label": "demand pressure", "color": "#5ea8ff", "accent": "#9fd1ff"},
    {"metric": "queue_depth", "label": "queue pressure", "color": "#8b7cff", "accent": "#d0c8ff"},
    {"metric": "p95_latency_ms", "label": "latency risk", "color": "#4fd4b6", "accent": "#b8f5e7"},
    {"metric": "error_rate_pct", "label": "error drift", "color": "#ff728a", "accent": "#ffc2cf"},
]


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
        "top_signals": report.incidents[0].top_signals if report.incidents else [],
        "blast_radius_services": report.incidents[0].blast_radius_services if report.incidents else [],
        "incident_evidence": [item.summary for item in report.incidents[0].evidence] if report.incidents else [],
        "recommendation": {
            "action": primary_recommendation.action if primary_recommendation else "none",
            "target_service": primary_recommendation.target_service if primary_recommendation else "",
            "confidence": primary_recommendation.confidence if primary_recommendation else 0,
            "projected_cost_delta_pct": primary_recommendation.projected_cost_delta_pct if primary_recommendation else 0,
            "projected_p95_delta_ms": primary_recommendation.projected_p95_delta_ms if primary_recommendation else 0,
        },
        "baseline_comparisons": report.evaluation.baseline_comparisons,
        "chart_metric": visual["metric"],
        "chart_series": series,
    }
    return {"report": report, "summary": summary, "visual": visual}


def build_live_stream_bundles(
    *,
    limit: int = 4,
    environment: str | None = None,
    source: str | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, object]]:
    streams = list_ingested_streams(environment=environment, source=source, limit=limit, db_path=db_path)
    return [_build_live_stream_bundle(stream, db_path=db_path) for stream in streams]


def build_live_summary_payload(
    *,
    limit: int = 4,
    environment: str | None = None,
    source: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, object]:
    bundles = build_live_stream_bundles(limit=limit, environment=environment, source=source, db_path=db_path)
    stats = get_storage_stats(environment=environment, source=source, db_path=db_path)
    return {"stats": stats, "streams": [bundle["summary"] for bundle in bundles]}


def write_artifacts(output_dir: Path, *, db_path: str | Path | None = None) -> tuple[Path, Path]:
    output_dir.mkdir(exist_ok=True)
    bundles = build_bundles()
    live_payload = build_live_summary_payload(db_path=db_path)

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps([bundle["summary"] for bundle in bundles], indent=2), encoding="utf-8")

    live_summary_path = output_dir / "live_summary.json"
    live_summary_path.write_text(json.dumps(live_payload, indent=2), encoding="utf-8")

    dashboard_path = output_dir / "dashboard.html"
    dashboard_path.write_text(
        render_dashboard(
            bundles,
            live_bundles=[{"summary": summary, "visual": _live_visual(summary)} for summary in live_payload["streams"]],
            live_stats=live_payload["stats"],
        ),
        encoding="utf-8",
    )
    return summary_path, dashboard_path


def metric_series(samples: list[MetricSample], service: str, metric: str) -> list[float]:
    series = [sample.value for sample in samples if sample.service == service and sample.metric == metric]
    return [round(value, 2) for value in series]


def render_dashboard(
    bundles: list[dict[str, object]],
    *,
    live_bundles: list[dict[str, object]] | None = None,
    live_stats: dict[str, object] | None = None,
) -> str:
    cards = "\n".join(render_card(bundle) for bundle in bundles)
    live_bundles = live_bundles or []
    live_stats = live_stats or {}
    live_section = render_live_section(live_bundles, live_stats)
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
      --cool: rgba(94, 168, 255, 0.12);
    }}
    * {{ box-sizing: border-box; }}
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
    .intro, .section-heading {{ display: grid; gap: 18px; }}
    .intro {{ margin-bottom: 28px; }}
    .section-block {{ display: grid; gap: 20px; margin-bottom: 28px; }}
    .eyebrow {{
      margin: 0;
      color: #e1b44d;
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }}
    h1, h2 {{ margin: 0; letter-spacing: -0.05em; }}
    h1 {{ font-size: clamp(3.2rem, 6vw, 5.4rem); line-height: 0.92; }}
    h2 {{ font-size: clamp(2rem, 4vw, 2.8rem); line-height: 0.96; }}
    .lede, .section-copy {{
      margin: 0;
      max-width: 40rem;
      color: var(--muted);
      font-size: 1.02rem;
      line-height: 1.65;
    }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 18px; }}
    .summary-card, .scenario-card, .live-card, .empty-card {{
      border: 1px solid var(--panel-border);
      border-radius: 28px;
      background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
      backdrop-filter: blur(14px);
    }}
    .summary-card {{ padding: 20px 22px; display: grid; gap: 10px; }}
    .summary-label {{
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .summary-value {{ font-size: 2.2rem; line-height: 1; font-weight: 800; letter-spacing: -0.04em; }}
    .summary-note {{ color: var(--muted); font-size: 0.95rem; }}
    .scenario-grid, .live-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 22px; }}
    .scenario-card, .live-card {{ padding: 24px; display: grid; gap: 18px; min-height: 360px; }}
    .live-card {{ background: linear-gradient(180deg, rgba(94,168,255,0.06), rgba(255,255,255,0.015)); }}
    .scenario-top, .live-top, .chart-top, .footer-row, .meta-row {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      justify-content: space-between;
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
    .chip--live {{ background: var(--cool); color: #cfe2ff; }}
    .scenario-title, .live-title {{
      margin: 0;
      font-size: 2.05rem;
      line-height: 0.98;
      letter-spacing: -0.05em;
    }}
    .scenario-subtitle, .live-subtitle, .meta-note {{
      margin: 0;
      color: var(--muted);
      font-size: 0.96rem;
      line-height: 1.5;
    }}
    .metric-strip {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }}
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
    .metric-value {{ font-size: 1.55rem; font-weight: 800; line-height: 1; letter-spacing: -0.04em; }}
    .policy-grid {{ display: grid; gap: 10px; }}
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
    .policy-row--ours {{ background: rgba(240, 176, 91, 0.08); border-color: rgba(240, 176, 91, 0.18); }}
    .policy-name {{
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}
    .policy-action {{ font-size: 0.96rem; font-weight: 700; text-transform: capitalize; }}
    .policy-delta {{ color: var(--muted); font-size: 0.88rem; white-space: nowrap; }}
    .chart-shell {{
      border-radius: 22px;
      border: 1px solid rgba(255, 255, 255, 0.06);
      background: linear-gradient(180deg, rgba(255,255,255,0.02), transparent 35%), rgba(5, 8, 12, 0.78);
      padding: 16px 16px 14px;
      display: grid;
      gap: 12px;
    }}
    .chart-label {{
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}
    .chart-service {{ font-size: 1.1rem; font-weight: 700; }}
    .svg-frame {{ width: 100%; height: 146px; display: block; }}
    .caption {{ color: var(--muted); font-size: 0.95rem; line-height: 1.5; }}
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
    .empty-card {{ padding: 28px; display: grid; gap: 10px; }}
    @media (max-width: 980px) {{
      .summary-grid, .scenario-grid, .live-grid, .metric-strip {{ grid-template-columns: 1fr; }}
      .policy-row {{ grid-template-columns: 1fr; }}
      .shell {{ width: min(100vw - 32px, 1280px); padding-top: 28px; }}
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
    {live_section}
    <section class="section-block">
      <div class="section-heading">
        <p class="eyebrow">Scenario Matrix</p>
        <h2>Controlled baselines and failure drills.</h2>
        <p class="section-copy">Synthetic scenarios still matter because they keep the pipeline explainable and benchmarkable as the live stream layer grows.</p>
      </div>
      <section class="summary-grid">
        {render_summary_cards(bundles)}
      </section>
      <section class="scenario-grid">
        {cards}
      </section>
    </section>
  </main>
</body>
</html>"""


def render_summary_cards(bundles: list[dict[str, object]]) -> str:
    if not bundles:
        return render_empty_card("No Scenarios", "No static scenario bundles were available.")
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
    return "\n".join(render_summary_card(label, value, note) for label, value, note in cards)


def render_live_section(live_bundles: list[dict[str, object]], live_stats: dict[str, object]) -> str:
    heading = """
    <div class="section-heading">
      <p class="eyebrow">Live Streams</p>
      <h2>Recurring pulls and the latest shadow evaluations.</h2>
      <p class="section-copy">These cards are backed by persisted SQLite stream snapshots. They show what the system has ingested recently, what it recommended, and how much signal the evaluation path compressed.</p>
    </div>
    """
    if not live_bundles:
        return f"""
        <section class="section-block">
          {heading}
          <section class="summary-grid">
            {render_live_summary_cards([], live_stats)}
          </section>
          {render_empty_card("No Live Streams Yet", "Run the recurring pull workflow or ingest a stream into SQLite to light up the live dashboard.")}
        </section>
        """
    live_cards = "\n".join(render_live_card(bundle) for bundle in live_bundles)
    return f"""
    <section class="section-block">
      {heading}
      <section class="summary-grid">
        {render_live_summary_cards(live_bundles, live_stats)}
      </section>
      <section class="live-grid">
        {live_cards}
      </section>
    </section>
    """


def render_live_summary_cards(live_bundles: list[dict[str, object]], live_stats: dict[str, object]) -> str:
    latest = live_bundles[0]["summary"] if live_bundles else None
    cards = [
        ("persisted streams", str(live_stats.get("stream_count", 0)), "SQLite-backed recurring windows"),
        ("shadow evals", str(live_stats.get("report_count", 0)), "latest stored shadow-mode reports"),
        (
            "latest action",
            latest["recommendation"]["action"].replace("_", " ") if latest else "none",
            latest["recommendation"]["target_service"] if latest and latest["recommendation"]["target_service"] else "no evaluated streams",
        ),
        ("db size", format_bytes(int(live_stats.get("db_file_size_bytes", 0))), _live_summary_note(latest)),
    ]
    return "\n".join(render_summary_card(label, value, note) for label, value, note in cards)


def render_summary_card(label: str, value: str, note: str) -> str:
    return f"""
    <article class="summary-card">
      <div class="summary-label">{escape(label)}</div>
      <div class="summary-value">{escape(value)}</div>
      <div class="summary-note">{escape(note)}</div>
    </article>
    """


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
    metric_markup = "\n".join(render_metric(label, value) for label, value in metrics)
    candidates = ", ".join(summary["root_cause_candidates"][:3]) or "n/a"
    top_signals = ", ".join(summary["top_signals"][:2]) or "n/a"
    blast_radius = ", ".join(summary["blast_radius_services"][:4]) or "n/a"
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
            {summary["alert_reduction_pct"]:.1f}% alert compression | top signals {escape(top_signals)} | blast radius {escape(blast_radius)} | our action {status_word(summary["recommended_action_match"])}
          </div>
          <div class="action">{escape(recommendation["action"].replace("_", " "))}</div>
        </div>
      </article>
    """


def render_live_card(bundle: dict[str, object]) -> str:
    summary = bundle["summary"]
    visual = bundle["visual"]
    recommendation = summary["recommendation"]
    chart = sparkline_svg(summary["chart_series"], visual["color"], visual["accent"])
    policy_rows = [render_policy_row("ours", recommendation, True)]
    policy_rows.extend(render_policy_row(baseline["policy"], baseline) for baseline in summary["baseline_comparisons"])
    metrics = [
        ("services", str(summary["service_count"])),
        ("anomalies", str(summary["anomaly_count"])),
        ("incidents", str(summary["incident_count"])),
        ("action", recommendation["action"].replace("_", " ")),
    ]
    metric_markup = "\n".join(render_metric(label, value) for label, value in metrics)
    candidates = ", ".join(summary["root_cause_candidates"][:3]) or "n/a"
    top_signals = ", ".join(summary["top_signals"][:2]) or "n/a"
    blast_radius = ", ".join(summary["blast_radius_services"][:4]) or "n/a"
    return f"""
      <article class="live-card">
        <div class="live-top">
          <span class="chip chip--live">{escape(summary["environment"])}</span>
          <span class="chip">{escape(summary["evaluation_mode"].replace("_", " "))}</span>
        </div>
        <div>
          <h2 class="live-title">{escape(summary["name"])}</h2>
          <p class="live-subtitle">{escape(summary["description"])}</p>
        </div>
        <div class="meta-row">
          <p class="meta-note">{escape(summary["stream_id"])}</p>
          <p class="meta-note">{escape(summary["created_at"])}</p>
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
              <div class="chart-service">{escape(summary["chart_service"])}</div>
            </div>
            <div class="chart-label">{escape(candidates)}</div>
          </div>
          {chart}
        </section>
        <div class="footer-row">
          <div class="caption">
            {summary["source"]} | top signals {escape(top_signals)} | blast radius {escape(blast_radius)} | baseline win {summary["baseline_win_rate_pct"]:.1f}% | latency protection {summary["latency_protection_pct"]:.1f}%
          </div>
          <div class="action">{escape(recommendation["action"].replace("_", " "))}</div>
        </div>
      </article>
    """


def render_metric(label: str, value: str) -> str:
    return f"""
    <div class="metric">
      <div class="metric-label">{escape(label)}</div>
      <div class="metric-value">{escape(value)}</div>
    </div>
    """


def render_empty_card(title: str, copy: str) -> str:
    return f"""
    <article class="empty-card">
      <div class="summary-label">{escape(title)}</div>
      <div class="summary-note">{escape(copy)}</div>
    </article>
    """


def sparkline_svg(series: list[float], color: str, accent: str) -> str:
    if not series:
        series = [0.0]
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
    area_points = f"{padding_x},{height - padding_y} " + line_points + f" {width - padding_x},{height - padding_y}"
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


def status_word(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "hit" if value else "miss"


def render_policy_row(policy: str, recommendation: dict[str, object], ours: bool = False) -> str:
    label = {
        "ours": "ours",
        "threshold_autoscaling": "threshold",
        "no_action": "no action",
        "naive_reroute": "naive reroute",
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
        <div class="policy-delta">dP95 {sign_latency}{latency:.0f} ms | cost {sign_cost}{cost:.0f}%</div>
      </div>
    """


def format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def _build_live_stream_bundle(stream: dict[str, Any], *, db_path: str | Path | None = None) -> dict[str, object]:
    loaded = load_ingested_stream(stream["stream_id"], db_path=db_path)
    latest_report_payload = loaded["latest_report"]
    report = latest_report_payload["report"] if latest_report_payload else None
    recommendation = report.recommendations[0] if report and report.recommendations else None
    incident = report.incidents[0] if report and report.incidents else None
    unique_services = sorted({sample.service for sample in loaded["telemetry"]})
    target_service = (
        recommendation.target_service
        if recommendation
        else incident.root_cause_candidates[0]
        if incident and incident.root_cause_candidates
        else unique_services[0]
        if unique_services
        else "n/a"
    )
    visual = _live_visual({"recommendation": {"action": recommendation.action if recommendation else "hold_steady"}})
    visual, chart_series = _select_live_series(loaded["telemetry"], target_service, visual)

    summary = {
        "stream_id": loaded["stream_id"],
        "name": str(loaded["metadata"].get("name") or loaded["stream_id"]),
        "description": str(loaded["metadata"].get("description") or "Persisted telemetry stream evaluated in shadow mode."),
        "created_at": loaded["created_at"],
        "source": loaded["source"],
        "environment": loaded["environment"],
        "service_count": len(unique_services),
        "metric_count": len(loaded["telemetry"]),
        "event_count": len(loaded["events"]),
        "incident_count": report.evaluation.incident_count if report else 0,
        "anomaly_count": report.evaluation.anomaly_count if report else 0,
        "evaluation_mode": report.evaluation.evaluation_mode if report else "not_evaluated",
        "action_stability_pct": report.evaluation.action_stability_pct if report else 0.0,
        "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct if report else 0.0,
        "latency_protection_pct": report.evaluation.latency_protection_pct if report else 0.0,
        "root_cause_candidates": incident.root_cause_candidates if incident else [],
        "top_signals": incident.top_signals if incident else [],
        "blast_radius_services": incident.blast_radius_services if incident else [],
        "incident_evidence": [item.summary for item in incident.evidence] if incident else [],
        "recommendation": {
            "action": recommendation.action if recommendation else "none",
            "target_service": recommendation.target_service if recommendation else "",
            "confidence": recommendation.confidence if recommendation else 0.0,
            "projected_cost_delta_pct": recommendation.projected_cost_delta_pct if recommendation else 0.0,
            "projected_p95_delta_ms": recommendation.projected_p95_delta_ms if recommendation else 0.0,
        },
        "baseline_comparisons": report.evaluation.baseline_comparisons if report else [],
        "chart_metric": visual["metric"],
        "chart_service": target_service,
        "chart_series": chart_series,
    }
    return {"summary": summary, "visual": visual}


def _select_live_series(
    samples: list[MetricSample],
    service: str,
    preferred_visual: dict[str, str],
) -> tuple[dict[str, str], list[float]]:
    preferred_series = metric_series(samples, service, preferred_visual["metric"])
    if preferred_series:
        return preferred_visual, preferred_series[-18:]
    for visual in FALLBACK_LIVE_VISUALS:
        fallback_series = metric_series(samples, service, visual["metric"])
        if fallback_series:
            return visual, fallback_series[-18:]
    return preferred_visual, []


def _live_visual(summary: dict[str, Any]) -> dict[str, str]:
    action = str(summary.get("recommendation", {}).get("action", "hold_steady"))
    return LIVE_VISUALS.get(action, LIVE_VISUALS["hold_steady"])


def _live_summary_note(latest: dict[str, Any] | None) -> str:
    if latest is None:
        return "no persisted live stream data"
    return f'{latest["environment"]} / {latest["source"]}'
