"""Unit tests for the daily clinical risk report renderer."""

from __future__ import annotations

import tempfile
from datetime import date
from decimal import Decimal

import pytest
from ward.reporting.renderer import build_clinical_narratives, render_from_rows
from ward.store.dao import DailyPatientSummaryRow


@pytest.fixture
def mock_summary_rows() -> list[DailyPatientSummaryRow]:
    report_d = date(2026, 4, 1)
    return [
        DailyPatientSummaryRow(
            ward_id="WARD-A",
            sim_date=report_d,
            patient_id="P001",
            scorer_version="v1",
            bed_id="BED-01",
            admitting_condition="Elective knee arthroplasty",
            max_news2=1,
            mean_news2=Decimal("0.8"),
            max_composite_risk=1,
            final_risk_tier="LOW",
            lab_contribution=0,
            labs_stale=False,
            alert_count=0,
            highest_severity="NONE",
            readings_count=96,
            readings_rejected=0,
            deterioration_detected=False,
        ),
        DailyPatientSummaryRow(
            ward_id="WARD-A",
            sim_date=report_d,
            patient_id="P014",
            scorer_version="v1",
            bed_id="BED-14",
            admitting_condition="Suspected bacterial pneumonia",
            max_news2=7,
            mean_news2=Decimal("4.5"),
            max_composite_risk=10,
            final_risk_tier="CRITICAL",
            lab_contribution=3,
            labs_stale=False,
            alert_count=5,
            highest_severity="CRITICAL",
            readings_count=96,
            readings_rejected=1,
            deterioration_detected=True,
        ),
        DailyPatientSummaryRow(
            ward_id="WARD-A",
            sim_date=report_d,
            patient_id="P031",
            scorer_version="v1",
            bed_id="BED-31",
            admitting_condition="COPD exacerbation",
            max_news2=4,
            mean_news2=Decimal("3.1"),
            max_composite_risk=4,
            final_risk_tier="LOW",
            lab_contribution=0,
            labs_stale=False,
            alert_count=1,
            highest_severity="MEDIUM",
            readings_count=96,
            readings_rejected=0,
            deterioration_detected=False,
        ),
        DailyPatientSummaryRow(
            ward_id="WARD-A",
            sim_date=report_d,
            patient_id="P008",
            scorer_version="v1",
            bed_id="BED-08",
            admitting_condition="Acute pancreatitis",
            max_news2=5,
            mean_news2=Decimal("3.8"),
            max_composite_risk=6,
            final_risk_tier="MEDIUM",
            lab_contribution=1,
            labs_stale=True,
            alert_count=2,
            highest_severity="HIGH",
            readings_count=94,
            readings_rejected=2,
            deterioration_detected=False,
        ),
    ]


def test_narratives_come_from_the_numbers(
    mock_summary_rows: list[DailyPatientSummaryRow],
) -> None:
    highlights = {h.patient_id: h for h in build_clinical_narratives(mock_summary_rows)}
    # P014 deteriorated; P008 reached NEWS2 5. P031 (peak 4, no deterioration) and the
    # calm patient need no call-out.
    assert set(highlights) == {"P014", "P008"}

    p014 = highlights["P014"]
    assert p014.is_critical is True
    assert "NEWS2 7" in p014.narrative
    assert "Lab results added 3" in p014.narrative
    assert "emergency" in p014.narrative

    p008 = highlights["P008"]
    assert p008.is_critical is False
    assert "urgent" in p008.narrative


def test_no_scripted_text_whatever_the_data() -> None:
    """The old renderer claimed hyperlactatemia for P014 even on a calm day."""
    calm = DailyPatientSummaryRow(
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        patient_id="P014",
        scorer_version="v1",
        bed_id="BED-14",
        admitting_condition="sepsis_risk",
        max_news2=2,
        mean_news2=Decimal("1.0"),
        max_composite_risk=2,
        final_risk_tier="LOW",
        lab_contribution=0,
        labs_stale=False,
        alert_count=0,
        highest_severity="NONE",
        readings_count=96,
        readings_rejected=0,
        deterioration_detected=False,
    )
    assert build_clinical_narratives([calm]) == []


def test_render_from_rows_generates_html_and_pdf(
    mock_summary_rows: list[DailyPatientSummaryRow],
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        report_d = date(2026, 4, 1)
        html_p, pdf_p = render_from_rows(
            ward_id="WARD-A",
            sim_date=report_d,
            rows=mock_summary_rows,
            output_dir=tmpdir,
            scorer_version="v1",
        )

        assert html_p.exists()
        assert pdf_p.exists()
        assert html_p.stat().st_size > 500
        assert pdf_p.stat().st_size > 1000

        content = html_p.read_text(encoding="utf-8")
        assert "St Jude Hospital Clinical Surveillance" in content
        assert "WARD-A" in content
        assert "P014" in content
        assert "BED-14" in content
        assert "CRITICAL" in content
        assert "Kappa Architecture" in content


def test_render_empty_rows_does_not_crash() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        report_d = date(2026, 4, 1)
        html_p, pdf_p = render_from_rows(
            ward_id="WARD-A",
            sim_date=report_d,
            rows=[],
            output_dir=tmpdir,
            scorer_version="v1",
        )
        assert html_p.exists()
        assert pdf_p.exists()
