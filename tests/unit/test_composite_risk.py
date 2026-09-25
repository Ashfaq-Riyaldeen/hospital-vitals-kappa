"""Unit tests for composite clinical risk evaluation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ward.clinical.composite_risk import (
    TIER_CRITICAL,
    TIER_LOW,
    TIER_MEDIUM,
    evaluate_composite_risk,
)
from ward.contracts.models import LabResult


def _make_lab(
    test_type: str,
    value: float,
    unit: str,
    ref_low: float | None,
    ref_high: float | None,
    reported_at: datetime | None = None,
) -> LabResult:
    now = reported_at or datetime(2026, 4, 2, 8, 0, tzinfo=UTC)
    return LabResult(
        patient_id="P014",
        test_type=test_type,
        result_value=value,
        unit=unit,
        reference_low=ref_low,
        reference_high=ref_high,
        collected_at=now - timedelta(hours=1),
        reported_at=now,
    )


def test_normal_labs_preserve_news2_score_and_tier() -> None:
    labs = [
        _make_lab("lactate", 1.2, "mmol/L", 0.5, 2.2),
        _make_lab("wbc", 6.5, "10^9/L", 4.0, 11.0),
        _make_lab("crp", 2.0, "mg/L", None, 5.0),
        _make_lab("creatinine", 80.0, "umol/L", 60.0, 110.0),
    ]
    res = evaluate_composite_risk("P001", news2_total=3, labs=labs)
    assert res.news2_total == 3
    assert res.lab_contribution == 0
    assert res.composite_risk == 3
    assert res.risk_tier == TIER_LOW
    assert not res.labs_stale
    assert "confirmed with normal laboratory results" in res.explanation


def test_sepsis_panel_raises_high_to_critical() -> None:
    """Patient P014's scripted day-2 sepsis labs escalate risk to CRITICAL."""
    sepsis_labs = [
        _make_lab("lactate", 3.8, "mmol/L", 0.5, 2.2),  # +1 (>2.0)
        _make_lab("wbc", 18.4, "10^9/L", 4.0, 11.0),  # +1 (abnormal)
        _make_lab("crp", 142.0, "mg/L", None, 5.0),  # +1 (>100)
        _make_lab("creatinine", 148.0, "umol/L", 60.0, 110.0),  # +1 (>120/abnormal)
    ]
    # NEWS2 = 7 (HIGH)
    res = evaluate_composite_risk("P014", news2_total=7, labs=sepsis_labs)
    assert res.news2_total == 7
    assert res.lab_contribution == 4
    assert res.composite_risk == 11
    assert res.risk_tier == TIER_CRITICAL
    assert not res.labs_stale
    assert "lactate 3.8" in res.explanation
    assert "WBC 18.4" in res.explanation


def test_severe_hyperlactatemia_adds_two_points() -> None:
    labs = [_make_lab("lactate", 5.2, "mmol/L", 0.5, 2.2)]
    res = evaluate_composite_risk("P002", news2_total=4, labs=labs)
    assert res.lab_contribution == 2
    assert res.composite_risk == 6
    assert res.risk_tier == TIER_MEDIUM


def test_stale_labs_contribute_zero_and_flag_stale() -> None:
    """Stale labs rule: absent or old data must never reassure or modify risk."""
    now = datetime(2026, 4, 5, 12, 0, tzinfo=UTC)
    old_time = now - timedelta(days=3)  # 3 days old > 2 days limit
    labs = [_make_lab("lactate", 4.5, "mmol/L", 0.5, 2.2, reported_at=old_time)]

    res = evaluate_composite_risk("P003", news2_total=5, labs=labs, as_of=now, max_lab_age_days=2)
    assert res.lab_contribution == 0
    assert res.composite_risk == 5
    assert res.risk_tier == TIER_MEDIUM
    assert res.labs_stale
    assert "lab results are stale" in res.explanation


def test_decomposition_invariant_always_holds() -> None:
    """Invariant: composite_risk - news2_total == lab_contribution across all tests."""
    labs = [
        _make_lab("lactate", 3.0, "mmol/L", 0.5, 2.2),
        _make_lab("potassium", 5.8, "mmol/L", 3.5, 5.3),
        _make_lab("haemoglobin", 72.0, "g/L", 115.0, 165.0),
    ]
    for news2 in range(0, 15):
        res = evaluate_composite_risk("P004", news2_total=news2, labs=labs)
        assert res.composite_risk - res.news2_total == res.lab_contribution
