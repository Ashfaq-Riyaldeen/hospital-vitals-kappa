"""Unit test for replay determinism.

Asserts that evaluating the same sequence of observations twice produces byte-identical results.
Reprocessing must be deterministic or the Kappa architectural argument collapses.
"""

from __future__ import annotations

from ward.clinical.news2 import score_news2


def test_replay_scoring_is_deterministic() -> None:
    observations = [
        (18, 92, False, 120, 80, "A", 37.0, True, "v2"),
        (24, 88, True, 90, 110, "V", 38.5, True, "v2"),
        (14, 99, False, 130, 65, "A", 36.5, False, "v2"),
        (30, 85, True, 85, 130, "P", 39.2, False, "v2"),
    ]

    pass_1 = [
        score_news2(
            respiratory_rate=rr,
            spo2=sp,
            on_supplemental_oxygen=ox,
            systolic_bp=sbp,
            heart_rate=hr,
            consciousness=c,
            temperature=t,
            copd_scale2=copd,
            version=v,
        )
        for rr, sp, ox, sbp, hr, c, t, copd, v in observations
    ]

    pass_2 = [
        score_news2(
            respiratory_rate=rr,
            spo2=sp,
            on_supplemental_oxygen=ox,
            systolic_bp=sbp,
            heart_rate=hr,
            consciousness=c,
            temperature=t,
            copd_scale2=copd,
            version=v,
        )
        for rr, sp, ox, sbp, hr, c, t, copd, v in observations
    ]

    for s1, s2 in zip(pass_1, pass_2, strict=True):
        assert s1.total == s2.total
        assert s1.subscores == s2.subscores
        assert s1.clinical_risk == s2.clinical_risk
        assert s1.any_parameter_is_3 == s2.any_parameter_is_3
        assert s1.scorer_version == s2.scorer_version
        assert s1.explanation == s2.explanation
