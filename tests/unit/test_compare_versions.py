"""Unit tests for the Clinical Audit & Version Comparison Engine."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from ward.replay.compare_versions import (
    evaluate_version_differences,
)
from ward.store.dao import RiskScoreRow


def _make_row(
    patient_id: str,
    version: str,
    scored_at: datetime,
    composite: int,
    tier: str,
) -> RiskScoreRow:
    return RiskScoreRow(
        patient_id=patient_id,
        scorer_version=version,
        scored_at=scored_at,
        window_start=scored_at,
        window_end=scored_at,
        news2_total=composite,
        news2_subscores={"spo2": composite},
        any_parameter_is_3=(composite >= 3),
        clinical_risk=tier,
        lab_contribution=0,
        composite_risk=composite,
        risk_tier=tier,
        labs_stale=False,
        confidence=1.0,
        readings_in_window=10,
        hr_slope=Decimal("0.0"),
        spo2_slope=Decimal("0.0"),
        sbp_slope=Decimal("0.0"),
        temp_slope=Decimal("0.0"),
    )


def test_evaluate_version_differences_synthetic() -> None:
    now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)

    # Patient P031 is COPD: v1 scores 6 (MEDIUM), v2 scores 3 (LOW)
    v1_p31 = _make_row("P031", "v1", now, composite=6, tier="MEDIUM")
    v2_p31 = _make_row("P031", "v2", now, composite=3, tier="LOW")

    # Patient P001 is Non-COPD: v1 scores 2 (LOW), v2 scores 2 (LOW)
    v1_p1 = _make_row("P001", "v1", now, composite=2, tier="LOW")
    v2_p1 = _make_row("P001", "v2", now, composite=2, tier="LOW")

    copd_set = {"P031"}

    report = evaluate_version_differences(
        trajectories_v1=[v1_p31, v1_p1],
        trajectories_v2=[v2_p31, v2_p1],
        copd_patient_ids=copd_set,
    )

    assert report.total_evaluations == 2
    assert report.total_changed == 1
    assert report.total_identical == 1
    assert report.percent_changed == 50.0
    assert report.copd_evaluations_changed == 1
    assert report.non_copd_evaluations_changed == 0
    assert report.control_group_bit_identical is True

    # Tier transition check
    assert report.tier_transition_matrix.get("MEDIUM -> LOW") == 1
    assert report.tier_transition_matrix.get("LOW -> LOW") == 1

    # False alarms suppressed
    assert report.estimated_false_alarms_suppressed == 1
    assert report.new_alarms_raised == 0

    # Markdown rendering
    md = report.to_markdown()
    assert "Clinical Scorer Audit Diff: v1 vs v2" in md
    assert "Bit-Identical Control Proof" in md
    assert "PASSED (Zero difference)" in md


def test_a_non_copd_change_or_an_upward_change_is_reported() -> None:
    """The comparison must be able to FAIL: a changed control patient, or a COPD
    score going up, are exactly what a clinical reviewer needs to see."""
    now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    report = evaluate_version_differences(
        trajectories_v1=[
            _make_row("P031", "v1", now, composite=3, tier="LOW_MEDIUM"),
            _make_row("P014", "v1", now, composite=7, tier="HIGH"),
        ],
        trajectories_v2=[
            _make_row("P031", "v2", now, composite=4, tier="MEDIUM"),
            _make_row("P014", "v2", now, composite=6, tier="MEDIUM"),
        ],
        copd_patient_ids={"P031"},
    )
    assert report.control_group_bit_identical is False
    assert report.non_copd_evaluations_changed == 1
    assert report.upward_changes == 1
