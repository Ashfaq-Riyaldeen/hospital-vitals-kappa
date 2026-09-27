"""Daily Consolidated Patient Risk Report Renderer.

Renders HTML and PDF clinical reports from pre-computed Cassandra daily summaries.
In strict accordance with the Kappa architecture, this module performs zero clinical
scoring; it reads pre-calculated values directly from Cassandra query Q6.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from ward import settings
from ward.obs.log import configure, get_logger
from ward.store.dao import DailyPatientSummaryRow, WardStoreDAO
from ward.store.session import create_cluster, get_session

log = get_logger()

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
URGENT_NEWS2 = 5
EMERGENCY_NEWS2 = 7


@dataclass(frozen=True, slots=True)
class NarrativeHighlight:
    patient_id: str
    bed_id: str
    admitting_condition: str
    max_composite: int
    is_critical: bool
    deterioration_detected: bool
    narrative: str


def build_clinical_narratives(rows: Sequence[DailyPatientSummaryRow]) -> list[NarrativeHighlight]:
    """One plain sentence or three for each patient who needed attention that day.

    Built only from the numbers in the summary row. The first version wrote fixed
    text for P014 ("hyperlactatemia ... ICU review required") and P031 ("Scale 2
    accommodated ...") whatever the data said - including under v1, which does not
    use Scale 2 at all.
    """
    highlights: list[NarrativeHighlight] = []
    for r in rows:
        if not (r.deterioration_detected or r.max_news2 >= URGENT_NEWS2):
            continue
        parts = [
            f"Bed {r.bed_id} ({r.admitting_condition}) reached NEWS2 {r.max_news2} and "
            f"composite risk {r.max_composite_risk}; the day ended at {r.final_risk_tier}."
        ]
        if r.lab_contribution:
            parts.append(f"Lab results added {r.lab_contribution} to the latest score.")
        elif r.labs_stale:
            parts.append("No fresh lab results were available, so vitals alone were scored.")
        if r.alert_count:
            parts.append(
                f"{r.alert_count} alert episode(s), highest severity {r.highest_severity}."
            )
        if r.max_news2 >= EMERGENCY_NEWS2:
            parts.append("NEWS2 of 7 or more calls for an emergency clinical response.")
        elif r.max_news2 >= URGENT_NEWS2:
            parts.append("NEWS2 of 5 or more calls for an urgent clinical review.")
        highlights.append(
            NarrativeHighlight(
                patient_id=r.patient_id,
                bed_id=r.bed_id,
                admitting_condition=r.admitting_condition,
                max_composite=r.max_composite_risk,
                is_critical=r.max_news2 >= EMERGENCY_NEWS2 or r.max_composite_risk >= 10,
                deterioration_detected=r.deterioration_detected,
                narrative=" ".join(parts),
            )
        )
    return highlights


def render_from_rows(
    ward_id: str,
    sim_date: date | str,
    rows: Sequence[DailyPatientSummaryRow],
    output_dir: Path | str = "./reports",
    scorer_version: str = "v1",
) -> tuple[Path, Path]:
    """Render daily HTML and PDF reports from a sequence of DailyPatientSummaryRow objects."""
    date_str = sim_date.isoformat() if isinstance(sim_date, date) else str(sim_date)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Sort patients worst-first
    sorted_patients = sorted(
        rows,
        key=lambda r: (
            -r.max_composite_risk,
            -int(r.deterioration_detected),
            -r.max_news2,
            r.patient_id,
        ),
    )

    total_patients = len(sorted_patients)
    critical_count = sum(
        1 for r in sorted_patients if r.final_risk_tier == "CRITICAL" or r.max_composite_risk >= 10
    )
    high_count = sum(1 for r in sorted_patients if r.final_risk_tier == "HIGH")
    medium_count = sum(1 for r in sorted_patients if r.final_risk_tier == "MEDIUM")
    low_count = sum(1 for r in sorted_patients if r.final_risk_tier == "LOW")
    deteriorating_count = sum(1 for r in sorted_patients if r.deterioration_detected)
    total_alerts = sum(r.alert_count for r in sorted_patients)
    stale_labs_count = sum(1 for r in sorted_patients if r.labs_stale)
    total_readings = sum(r.readings_count for r in sorted_patients)
    total_rejected = sum(r.readings_rejected for r in sorted_patients)

    highlights = build_clinical_narratives(sorted_patients)

    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    template = env.get_template("daily_report.html")

    rendered_html = template.render(
        ward_id=ward_id,
        sim_date=date_str,
        generation_time=datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        scorer_version=scorer_version,
        total_patients=total_patients,
        critical_count=critical_count,
        high_count=high_count,
        medium_count=medium_count,
        low_count=low_count,
        deteriorating_count=deteriorating_count,
        total_alerts=total_alerts,
        stale_labs_count=stale_labs_count,
        total_readings=total_readings,
        total_rejected=total_rejected,
        critical_highlights=highlights,
        patients=sorted_patients,
    )

    html_path = out_dir / f"ward_daily_report_{date_str}.html"
    pdf_path = out_dir / f"ward_daily_report_{date_str}.pdf"

    html_path.write_text(rendered_html, encoding="utf-8")
    log.info("daily_report_html_written", path=str(html_path))

    # Compile to PDF using WeasyPrint
    try:
        from weasyprint import HTML

        HTML(string=rendered_html).write_pdf(str(pdf_path))
        log.info("daily_report_pdf_written", path=str(pdf_path))
    except Exception as exc:
        log.warning("pdf_compilation_failed", error=str(exc), html_fallback=str(html_path))

    return html_path, pdf_path


def render_daily_report(
    ward_id: str,
    sim_date: date | str,
    dao: WardStoreDAO | None = None,
    output_dir: Path | str = "./reports",
    scorer_version: str = "v1",
) -> tuple[Path, Path]:
    """Fetch Q6 rows from Cassandra and render consolidated daily risk report."""
    target_date = date.fromisoformat(sim_date) if isinstance(sim_date, str) else sim_date

    if dao is None:
        store_cfg = settings.storage()
        cluster = create_cluster()
        session = get_session(keyspace=store_cfg.keyspace, cluster=cluster)
        dao = WardStoreDAO(session)

    rows = dao.get_daily_patient_summary(
        ward_id=ward_id,
        report_date=target_date,
        scorer_version=scorer_version,
    )
    return render_from_rows(
        ward_id=ward_id,
        sim_date=target_date,
        rows=rows,
        output_dir=output_dir,
        scorer_version=scorer_version,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Render Ward Consolidated Daily Risk Report")
    parser.add_argument("--sim-date", required=True, help="Simulated report date (YYYY-MM-DD)")
    parser.add_argument("--ward-id", default="WARD-A", help="Ward ID (default: WARD-A)")
    parser.add_argument("--scorer-version", default="v1", help="Scorer version (v1 or v2)")
    parser.add_argument("--output-dir", default="./reports", help="Output directory for reports")
    args = parser.parse_args()

    obs = settings.observability()
    configure(
        service="report-renderer",
        stage="serve",
        level=obs.log_level,
        json_output=obs.log_json,
    )

    log.info("rendering_daily_report", ward_id=args.ward_id, sim_date=args.sim_date)
    html_p, pdf_p = render_daily_report(
        ward_id=args.ward_id,
        sim_date=args.sim_date,
        output_dir=args.output_dir,
        scorer_version=args.scorer_version,
    )
    print(f"Report generated:\n  HTML: {html_p}\n  PDF:  {pdf_p}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
