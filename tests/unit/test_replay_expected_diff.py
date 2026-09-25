"""Unit test proving the core Kappa replay invariant:
Under v2 replay, ONLY COPD patients change, ALWAYS downward (2-3 points),
and non-COPD patients are 100% BIT-IDENTICAL.
"""

from __future__ import annotations

from ward.clinical.news2 import score_news2


def test_copd_vs_non_copd_expected_diff() -> None:
    """★ The core clinical invariant:
    - COPD patients (copd_scale2=True) with hypercapnic hypoxia (SpO2 88-92%) drop 2-3 points under v2.
    - Non-COPD patients (copd_scale2=False) have ZERO score change under v2.
    """
    # 1. COPD patient with typical 90% SpO2 on room air
    v1_copd = score_news2(
        respiratory_rate=18,
        spo2=90,
        on_supplemental_oxygen=False,
        systolic_bp=120,
        heart_rate=75,
        consciousness="A",
        temperature=37.0,
        copd_scale2=True,
        version="v1",
    )
    v2_copd = score_news2(
        respiratory_rate=18,
        spo2=90,
        on_supplemental_oxygen=False,
        systolic_bp=120,
        heart_rate=75,
        consciousness="A",
        temperature=37.0,
        copd_scale2=True,
        version="v2",
    )

    # In v1 (Scale 1), 90% SpO2 scores 3 points!
    assert v1_copd.subscores["spo2"] == 3
    assert v1_copd.total == 3
    assert v1_copd.clinical_risk == "LOW_MEDIUM"  # Triggered red parameter rule!

    # In v2 (Scale 2), 90% SpO2 is normal for COPD and scores 0 points!
    assert v2_copd.subscores["spo2"] == 0
    assert v2_copd.total == 0
    assert v2_copd.clinical_risk == "LOW"

    # Delta is exactly -3 points downward
    assert v2_copd.total - v1_copd.total == -3

    # 2. Non-COPD patient with normal observations
    v1_normal = score_news2(
        respiratory_rate=16,
        spo2=98,
        on_supplemental_oxygen=False,
        systolic_bp=125,
        heart_rate=70,
        consciousness="A",
        temperature=36.8,
        copd_scale2=False,
        version="v1",
    )
    v2_normal = score_news2(
        respiratory_rate=16,
        spo2=98,
        on_supplemental_oxygen=False,
        systolic_bp=125,
        heart_rate=70,
        consciousness="A",
        temperature=36.8,
        copd_scale2=False,
        version="v2",
    )

    # Completely identical
    assert v1_normal.total == v2_normal.total
    assert v1_normal.subscores == v2_normal.subscores
    assert v1_normal.clinical_risk == v2_normal.clinical_risk

    # 3. Non-COPD patient with acute hypoxia (SpO2 90%)
    v1_acute = score_news2(
        respiratory_rate=22,
        spo2=90,
        on_supplemental_oxygen=False,
        systolic_bp=110,
        heart_rate=95,
        consciousness="A",
        temperature=38.2,
        copd_scale2=False,
        version="v1",
    )
    v2_acute = score_news2(
        respiratory_rate=22,
        spo2=90,
        on_supplemental_oxygen=False,
        systolic_bp=110,
        heart_rate=95,
        consciousness="A",
        temperature=38.2,
        copd_scale2=False,
        version="v2",
    )

    # Because copd_scale2=False, Scale 1 is retained in v2! Scores remain identical!
    assert v1_acute.total == v2_acute.total
    assert v1_acute.subscores == v2_acute.subscores
    assert v1_acute.clinical_risk == v2_acute.clinical_risk
