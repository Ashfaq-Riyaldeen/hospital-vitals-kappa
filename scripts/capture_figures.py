"""Capture the report's screenshots from the RUNNING stack.

    .venv/bin/python scripts/capture_figures.py            # every shot
    .venv/bin/python scripts/capture_figures.py grafana    # shots whose name matches

Every image is a real browser capture of a real page, or (for Cassandra) the real
output of cqlsh run inside the container. The first version of this script drew HTML
mock-ups styled to look like Grafana, filled with invented numbers, and saved those
as the report's "evidence". That mode is gone; there is no way to produce a figure
here without the system running.
"""

from __future__ import annotations

import html
import os
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

OUT = Path(__file__).resolve().parents[1] / "docs/report/figures"
VIEWPORT = {"width": 1600, "height": 900}


@dataclass
class Shot:
    name: str
    url: str
    wait_for: str | None = None  # text or selector that proves the page has data
    settle: float = 2.0
    before: Callable[[Page], None] | None = None
    full_page: bool = False


def airflow_login(page: Page) -> None:
    page.goto("http://localhost:8182/login/")
    if page.locator("input#username").count():
        page.fill("input#username", "admin")
        page.fill("input#password", "admin")
        page.click("input[type=submit], button[type=submit]")
        page.wait_for_load_state("networkidle")


def spark_query_stats(port: int) -> Callable[[Page], None]:
    def go(page: Page) -> None:
        page.goto(f"http://localhost:{port}/StreamingQuery/")
        link = page.locator("a[href*='StreamingQuery/statistics']").first
        link.click()
        page.wait_for_load_state("networkidle")

    return go


GRAFANA = "http://localhost:3100/d/{uid}?orgId=1&kiosk&theme=light&from=now-{span}&to=now"

SHOTS: list[Shot] = [
    # Kafka UI
    Shot("kafka-topics", "http://localhost:8180/ui/clusters/ward/all-topics", "vitals.readings.v1"),
    Shot(
        "kafka-labs-settings",
        "http://localhost:8180/ui/clusters/ward/all-topics/labs.results.v1/settings",
        "cleanup.policy",
    ),
    Shot(
        "kafka-vitals-messages",
        "http://localhost:8180/ui/clusters/ward/all-topics/vitals.readings.v1/messages",
        "heart_rate",
        settle=4,
    ),
    Shot(
        "kafka-dlq-messages",
        "http://localhost:8180/ui/clusters/ward/all-topics/vitals.readings.dlq/messages",
        "rejection_reason",
        settle=4,
    ),
    Shot("kafka-schemas", "http://localhost:8180/ui/clusters/ward/schemas", "vitals.readings.v1"),
    Shot("kafka-consumers", "http://localhost:8180/ui/clusters/ward/consumer-groups"),
    # Spark
    Shot("spark-jobs", "http://localhost:4140/jobs/", "Completed Jobs"),
    Shot(
        "spark-streaming-v1",
        "",
        "Input Rate",
        before=spark_query_stats(4140),
        settle=3,
        full_page=True,
    ),
    Shot(
        "spark-streaming-v2",
        "",
        "Input Rate",
        before=spark_query_stats(4141),
        settle=3,
        full_page=True,
    ),
    # API
    Shot("api-docs", "http://localhost:8100/docs", "Ward Monitor", settle=3),
    # Airflow
    Shot("airflow-dags", "http://localhost:8182/home", "ward_lab_ingest", before=airflow_login),
    Shot(
        "airflow-lab-ingest-grid",
        "http://localhost:8182/dags/ward_lab_ingest/grid",
        "publish_to_kafka",
        before=airflow_login,
        settle=4,
    ),
    Shot(
        "airflow-replay-graph",
        "http://localhost:8182/dags/ward_replay/grid?tab=graph",
        "await_human_approval",
        before=airflow_login,
        settle=5,
    ),
    Shot(
        "airflow-daily-report-grid",
        "http://localhost:8182/dags/ward_daily_report/grid",
        "render_clinical_report",
        before=airflow_login,
        settle=4,
    ),
    # Prometheus and Alertmanager
    Shot(
        "prometheus-targets", "http://localhost:9190/targets", "ward-stream:8104/metrics", settle=3
    ),
    Shot("prometheus-alerts", "http://localhost:9190/alerts", "WardMonitoringSilent", settle=3),
    Shot("alertmanager", "http://localhost:9193/#/alerts", "alertname", settle=4),
    # Grafana
    Shot(
        "grafana-ward-monitor",
        GRAFANA.format(uid="ward-clinical-monitor", span="30m"),
        "Ward list",
        settle=8,
    ),
    Shot(
        "grafana-pipeline-health",
        GRAFANA.format(uid="ward-pipeline-health", span="30m"),
        "End-to-end",
        settle=8,
    ),
    Shot(
        "grafana-pipeline-outage",
        GRAFANA.format(uid="ward-pipeline-health", span="30m"),
        "End-to-end",
        settle=8,
    ),
    Shot(
        "grafana-replay",
        GRAFANA.format(uid="ward-replay-comparison", span="1h"),
        "Replay progress",
        settle=8,
    ),
]


def take(page: Page, shot: Shot) -> Path:
    if shot.before:
        shot.before(page)
    if shot.url:
        page.goto(shot.url, wait_until="networkidle", timeout=60_000)
    if shot.wait_for:
        page.get_by_text(shot.wait_for).first.wait_for(timeout=30_000)
    time.sleep(shot.settle)
    path = OUT / f"{shot.name}.png"
    page.screenshot(path=str(path), full_page=shot.full_page)
    return path


# --- Cassandra: real cqlsh output, rendered as a terminal image --------------

CQL_SHOTS = {
    "cassandra-ward-snapshot": (
        "SELECT patient_id, risk_score, news2_total, lab_contribution, risk_tier, scored_at "
        "FROM ward.ward_risk_snapshot WHERE ward_id='WARD-A' AND scorer_version='v1' LIMIT 12;"
    ),
    "cassandra-p014-alerts": None,  # filled at run time with today's date
    "cassandra-p031-versions": (
        "SELECT scorer_version, scored_at, news2_total, clinical_risk "
        "FROM ward.risk_scores_by_patient WHERE patient_id='P031' AND scorer_version='v1' "
        "LIMIT 5; "
        "SELECT scorer_version, scored_at, news2_total, clinical_risk "
        "FROM ward.risk_scores_by_patient WHERE patient_id='P031' AND scorer_version='v2' "
        "LIMIT 5;"
    ),
}


def cqlsh(statement: str) -> str:
    result = subprocess.run(
        ["docker", "exec", "ward-cassandra", "cqlsh", "-e", statement],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout.strip("\n")


def terminal_page(
    command: str, output: str, title: str = "docker exec ward-cassandra cqlsh"
) -> str:
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
      body {{ margin:0; background:#1e1e1e; }}
      .t {{ font: 13px/1.45 'DejaVu Sans Mono', monospace; color:#e6e6e6; padding:18px 22px;
            white-space:pre; }}
      .p {{ color:#7ec699; }} .bar {{ background:#333; color:#bbb; font:12px sans-serif;
            padding:6px 12px; }}
    </style></head><body><div class="bar">{html.escape(title)}</div>
    <div class="t"><span class="p">{"cqlsh&gt;" if "cqlsh" in title else "$"}</span> {html.escape(command)}

{html.escape(output)}</div></body></html>"""


def api_shot(page: Page, only: str) -> list[Path]:
    """The ward list from the real API, pretty-printed, first five patients."""
    if only not in "api-ward-monitor":
        return []
    import json
    import urllib.request

    url = "http://localhost:8100/api/v1/ward/WARD-A/monitor"
    body = json.load(urllib.request.urlopen(url, timeout=10))
    body["patients"] = body["patients"][:5]
    keep = (
        "patient_id",
        "bed_id",
        "risk_score",
        "risk_tier",
        "news2_total",
        "lab_contribution",
        "labs_stale",
        "scored_at",
        "hr_latest",
        "spo2_latest",
    )
    body["patients"] = [{k: p[k] for k in keep} for p in body["patients"]]
    text = json.dumps(body, indent=2) + "\n  ... (first 5 of the ward's patients shown)"
    page.set_content(terminal_page(f"curl -s {url}", text, title="terminal"))
    path = OUT / "api-ward-monitor.png"
    page.locator(".t").screenshot(path=str(path))
    return [path]


def cassandra_shots(page: Page, only: str) -> list[Path]:
    shots = dict(CQL_SHOTS)
    # P014's sepsis episode: simulated day 2, the hours after the 08:00 onset.
    # A clustering-key range, so still a single-partition query.
    shots["cassandra-p014-alerts"] = (
        "SELECT alert_time, patient_id, alert_type, severity, news2_total, composite_risk "
        "FROM ward.alerts_by_ward WHERE ward_id='WARD-A' AND sim_date='2026-04-02' "
        "AND scorer_version='v1' AND alert_time >= '2026-04-02 09:30:00+0000' "
        "AND alert_time <= '2026-04-02 11:30:00+0000';"
    )
    paths = []
    for name, statement in shots.items():
        if only not in name:
            continue
        assert statement is not None
        page.set_content(terminal_page(statement, cqlsh(statement)))
        path = OUT / f"{name}.png"
        page.locator(".t").screenshot(path=str(path))
        paths.append(path)
    return paths


def daily_report_page(only: str) -> list[Path]:
    if only not in "daily-report":
        return []
    # REPORT_DATE picks the simulated day to show; files older than the current
    # simulation (state/sim_epoch.json) are ignored so a previous run is never shown.
    started = Path("state/sim_epoch.json").stat().st_mtime
    wanted = os.environ.get("REPORT_DATE", "")
    reports = sorted(
        p
        for p in Path("reports").glob(f"ward_daily_report_{wanted}*.pdf")
        if p.stat().st_mtime > started
    )
    if not reports:
        print("no daily report PDF from this run in reports/", file=sys.stderr)
        return []
    target = OUT / "daily-report"
    subprocess.run(
        [
            "pdftoppm",
            "-png",
            "-r",
            "110",
            "-f",
            "1",
            "-l",
            "1",
            "-singlefile",
            str(reports[-1]),
            str(target),
        ],
        check=True,
    )
    return [target.with_suffix(".png")]


def main() -> int:
    only = sys.argv[1] if len(sys.argv) > 1 else ""
    OUT.mkdir(parents=True, exist_ok=True)
    failures = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1.25)
        for shot in SHOTS:
            if only not in shot.name:
                continue
            try:
                print("captured", take(page, shot).name)
            except Exception as exc:
                failures.append(shot.name)
                print(f"FAILED {shot.name}: {exc}", file=sys.stderr)
        for p in api_shot(page, only):
            print("captured", p.name)
        if "cassandra" in only or not only:
            for p in cassandra_shots(page, only):
                print("captured", p.name)
        browser.close()
    for p in daily_report_page(only):
        print("captured", p.name)
    if failures:
        print(f"{len(failures)} failed: {failures}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
