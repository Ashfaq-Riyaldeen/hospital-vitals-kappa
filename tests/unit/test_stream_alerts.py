"""Unit tests for clinical alerts evaluation and deduplication.

Tests all 7 clinical alert conditions, severity ratings,
and deterministic alert hash generation.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from ward.clinical.composite_risk import CompositeRiskResult
from ward.clinical.news2 import News2Result
from ward.stream.alerts import (
    ALERT_LAB_CORROBORATED_RISK,
    ALERT_NEWS2_HIGH,
    ALERT_NEWS2_MEDIUM,
    ALERT_RAPID_DETERIORATION,
    ALERT_SINGLE_PARAM_RED,
    ALERT_SPO2_CRITICAL,
    ALERT_TREND_CONCERNING,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    evaluate_clinical_alerts,
    make_alert_id,
)
from ward.stream.windows import WindowedVitalsTrend


@pytest.fixture
def base_window_trend() -> WindowedVitalsTrend:
    return WindowedVitalsTrend(
        patient_id="P001",
        window_start=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        readings_in_window=16,
        confidence="high",
        hr_latest=72,
        spo2_latest=98,
        sbp_latest=120,
        dbp_latest=80,
        temp_latest=Decimal("37.0"),
        rr_latest=16,
        consciousness_latest="A",
        on_o2_latest=False,
        hr_slope=Decimal("0.0"),
        spo2_slope=Decimal("0.0"),
        sbp_slope=Decimal("0.0"),
        temp_slope=Decimal("0.0"),
    )


@pytest.fixture
def base_news2_low() -> News2Result:
    return News2Result(
        total=1,
        subscores={
            "heart_rate": 0,
            "spo2": 0,
            "systolic_bp": 0,
            "respiratory_rate": 1,
            "temperature": 0,
            "consciousness": 0,
            "oxygen": 0,
        },
        any_parameter_is_3=False,
        clinical_risk="LOW",
        parameters_missing=0,
        scorer_version="v2",
    )


@pytest.fixture
def base_composite_low() -> CompositeRiskResult:
    return CompositeRiskResult(
        patient_id="P001",
        news2_total=1,
        lab_contribution=0,
        composite_risk=1,
        risk_tier="LOW",
        labs_stale=False,
        explanation="Routine monitoring.",
    )


def test_make_alert_id_is_deterministic() -> None:
    t0 = datetime(2026, 4, 1, 10, 0, tzinfo=UTC)
    id1 = make_alert_id("P001", ALERT_NEWS2_HIGH, t0)
    id2 = make_alert_id("P001", ALERT_NEWS2_HIGH, t0)
    assert id1 == id2
    assert len(id1) == 16

    # Different patient -> different id
    id_other_patient = make_alert_id("P002", ALERT_NEWS2_HIGH, t0)
    assert id1 != id_other_patient

    # Different window start -> different id
    t1 = datetime(2026, 4, 1, 11, 0, tzinfo=UTC)
    id_other_time = make_alert_id("P001", ALERT_NEWS2_HIGH, t1)
    assert id1 != id_other_time


def test_news2_high_triggers_critical_alert(
    base_window_trend: WindowedVitalsTrend,
    base_composite_low: CompositeRiskResult,
) -> None:
    high_news2 = News2Result(
        total=8,
        subscores={
            "heart_rate": 2,
            "spo2": 3,
            "systolic_bp": 1,
            "respiratory_rate": 2,
            "temperature": 0,
            "consciousness": 0,
            "oxygen": 0,
        },
        any_parameter_is_3=True,
        clinical_risk="HIGH",
        parameters_missing=0,
        scorer_version="v2",
    )
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=base_window_trend,
        news2=high_news2,
        composite=base_composite_low,
    )
    types = [a.alert_type for a in alerts]
    assert ALERT_NEWS2_HIGH in types
    news2_alert = next(a for a in alerts if a.alert_type == ALERT_NEWS2_HIGH)
    assert news2_alert.severity == SEVERITY_CRITICAL


def test_news2_medium_triggers_high_alert(
    base_window_trend: WindowedVitalsTrend,
    base_composite_low: CompositeRiskResult,
) -> None:
    medium_news2 = News2Result(
        total=5,
        subscores={
            "heart_rate": 1,
            "spo2": 1,
            "systolic_bp": 1,
            "respiratory_rate": 2,
            "temperature": 0,
            "consciousness": 0,
            "oxygen": 0,
        },
        any_parameter_is_3=False,
        clinical_risk="MEDIUM",
        parameters_missing=0,
        scorer_version="v2",
    )
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=base_window_trend,
        news2=medium_news2,
        composite=base_composite_low,
    )
    types = [a.alert_type for a in alerts]
    assert ALERT_NEWS2_MEDIUM in types
    alert = next(a for a in alerts if a.alert_type == ALERT_NEWS2_MEDIUM)
    assert alert.severity == SEVERITY_HIGH


def test_single_parameter_red_escalation(
    base_window_trend: WindowedVitalsTrend,
    base_composite_low: CompositeRiskResult,
) -> None:
    # Total is low (3) but a single parameter is 3 (e.g. severe hypothermia or bradypnea)
    red_param_news2 = News2Result(
        total=3,
        subscores={
            "heart_rate": 0,
            "spo2": 0,
            "systolic_bp": 0,
            "respiratory_rate": 3,
            "temperature": 0,
            "consciousness": 0,
            "oxygen": 0,
        },
        any_parameter_is_3=True,
        clinical_risk="LOW_MEDIUM",
        parameters_missing=0,
        scorer_version="v2",
    )
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=base_window_trend,
        news2=red_param_news2,
        composite=base_composite_low,
    )
    types = [a.alert_type for a in alerts]
    assert ALERT_SINGLE_PARAM_RED in types
    alert = next(a for a in alerts if a.alert_type == ALERT_SINGLE_PARAM_RED)
    assert alert.severity == SEVERITY_HIGH


def test_critical_spo2_alert_scale1_and_scale2(
    base_news2_low: News2Result,
    base_composite_low: CompositeRiskResult,
) -> None:
    # Scale 1: SpO2 < 88 triggers critical hypoxia
    trend_86 = WindowedVitalsTrend(
        patient_id="P001",
        window_start=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        readings_in_window=16,
        confidence="high",
        hr_latest=72,
        spo2_latest=86,
        sbp_latest=120,
        dbp_latest=80,
        temp_latest=Decimal("37.0"),
        rr_latest=16,
        consciousness_latest="A",
        on_o2_latest=False,
        hr_slope=Decimal("0.0"),
        spo2_slope=Decimal("0.0"),
        sbp_slope=Decimal("0.0"),
        temp_slope=Decimal("0.0"),
    )
    alerts_scale1 = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=trend_86,
        news2=base_news2_low,
        composite=base_composite_low,
        copd_scale2=False,
    )
    assert ALERT_SPO2_CRITICAL in [a.alert_type for a in alerts_scale1]

    # For COPD patient on Scale 2: 86% is permissible (threshold is 84%)
    alerts_scale2 = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=trend_86,
        news2=base_news2_low,
        composite=base_composite_low,
        copd_scale2=True,
    )
    assert ALERT_SPO2_CRITICAL not in [a.alert_type for a in alerts_scale2]


def test_rapid_deterioration_alert(
    base_window_trend: WindowedVitalsTrend,
    base_composite_low: CompositeRiskResult,
) -> None:
    current_news2 = News2Result(
        total=6,
        subscores={
            "heart_rate": 2,
            "spo2": 2,
            "systolic_bp": 1,
            "respiratory_rate": 1,
            "temperature": 0,
            "consciousness": 0,
            "oxygen": 0,
        },
        any_parameter_is_3=False,
        clinical_risk="MEDIUM",
        parameters_missing=0,
        scorer_version="v2",
    )
    # Prior score was 2 (jump of 4 points within the window)
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=base_window_trend,
        news2=current_news2,
        composite=base_composite_low,
        prior_score_in_window=2,
    )
    types = [a.alert_type for a in alerts]
    assert ALERT_RAPID_DETERIORATION in types
    alert = next(a for a in alerts if a.alert_type == ALERT_RAPID_DETERIORATION)
    assert alert.severity == SEVERITY_CRITICAL


def test_lab_corroborated_risk_alert(
    base_window_trend: WindowedVitalsTrend,
    base_news2_low: News2Result,
) -> None:
    # Composite risk >= 7 and lab contribution >= 2
    corroborated_composite = CompositeRiskResult(
        patient_id="P001",
        news2_total=5,
        lab_contribution=3,
        composite_risk=8,
        risk_tier="HIGH",
        labs_stale=False,
        explanation="Lactate 3.8 mmol/L (+2), WBC 16.5 (+1)",
    )
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=base_window_trend,
        news2=base_news2_low,
        composite=corroborated_composite,
    )
    types = [a.alert_type for a in alerts]
    assert ALERT_LAB_CORROBORATED_RISK in types
    alert = next(a for a in alerts if a.alert_type == ALERT_LAB_CORROBORATED_RISK)
    assert alert.severity == SEVERITY_HIGH


def test_trend_concerning_requires_high_confidence(
    base_news2_low: News2Result,
    base_composite_low: CompositeRiskResult,
) -> None:
    # Adverse slopes: HR rising (+), SpO2 dropping (-), SBP dropping (-)
    trend_high_conf = WindowedVitalsTrend(
        patient_id="P001",
        window_start=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        readings_in_window=10,
        confidence="high",
        hr_latest=88,
        spo2_latest=94,
        sbp_latest=105,
        dbp_latest=70,
        temp_latest=Decimal("37.8"),
        rr_latest=18,
        consciousness_latest="A",
        on_o2_latest=False,
        hr_slope=Decimal("1.25"),  # adverse: rising
        spo2_slope=Decimal("-0.80"),  # adverse: falling
        sbp_slope=Decimal("-1.10"),  # adverse: falling
        temp_slope=Decimal("0.10"),  # adverse: rising
    )
    scoring_news2 = News2Result(
        total=3,
        subscores={**base_news2_low.subscores, "heart_rate": 1, "systolic_bp": 1},
        any_parameter_is_3=False,
        clinical_risk="LOW",
        parameters_missing=0,
        scorer_version="v2",
    )
    alerts = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=trend_high_conf,
        news2=scoring_news2,
        composite=base_composite_low,
    )
    assert ALERT_TREND_CONCERNING in [a.alert_type for a in alerts]

    # The same slopes in a patient scoring 0-2 are ordinary variation, not a concern.
    calm = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=trend_high_conf,
        news2=base_news2_low,
        composite=base_composite_low,
    )
    assert ALERT_TREND_CONCERNING not in [a.alert_type for a in calm]

    # If confidence is low (< 4 readings), trend alert is suppressed
    trend_low_conf = WindowedVitalsTrend(
        patient_id="P001",
        window_start=datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        readings_in_window=2,
        confidence="low",
        hr_latest=88,
        spo2_latest=94,
        sbp_latest=105,
        dbp_latest=70,
        temp_latest=Decimal("37.8"),
        rr_latest=18,
        consciousness_latest="A",
        on_o2_latest=False,
        hr_slope=Decimal("1.25"),
        spo2_slope=Decimal("-0.80"),
        sbp_slope=Decimal("-1.10"),
        temp_slope=Decimal("0.10"),
    )
    alerts_low_conf = evaluate_clinical_alerts(
        patient_id="P001",
        bed_id="BED-01",
        ward_id="WARD-A",
        sim_date=date(2026, 4, 1),
        alert_time=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        window_trend=trend_low_conf,
        news2=scoring_news2,
        composite=base_composite_low,
    )
    assert ALERT_TREND_CONCERNING not in [a.alert_type for a in alerts_low_conf]


def test_single_red_needs_two_readings_in_a_row(
    base_window_trend: WindowedVitalsTrend,
    base_composite_low: CompositeRiskResult,
) -> None:
    """P007's lone HR 145 scores 3. One such reading must not alert; two must."""
    spike = News2Result(
        total=4,
        subscores={"heart_rate": 3, "respiratory_rate": 1},
        any_parameter_is_3=True,
        clinical_risk="LOW_MEDIUM",
        parameters_missing=0,
        scorer_version="v1",
    )
    kwargs: dict[str, object] = {
        "patient_id": "P007",
        "bed_id": "BED-07",
        "ward_id": "WARD-A",
        "sim_date": date(2026, 4, 1),
        "alert_time": datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
        "window_trend": base_window_trend,
        "news2": spike,
        "composite": base_composite_low,
        "prior_score_in_window": 1,
        "previous_score": 1,
    }

    first = evaluate_clinical_alerts(**kwargs, previous_any_parameter_is_3=False)  # type: ignore[arg-type]
    assert [a.alert_type for a in first] == []
    second = evaluate_clinical_alerts(**kwargs, previous_any_parameter_is_3=True)  # type: ignore[arg-type]
    assert ALERT_SINGLE_PARAM_RED in [a.alert_type for a in second]
