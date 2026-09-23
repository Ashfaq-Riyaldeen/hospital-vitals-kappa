"""The NEWS2 reference table, asserted against the published specification.

★ The single most important test file in the repository. ★

Its value comes from where the expected values originate: they are not derived from
our implementation, they are read off the Royal College of Physicians table. A test
that computes its expectation the same way the code does proves only that the code is
self-consistent. These fixtures were known before the implementation existed, which is
what makes them evidence rather than a restatement.

Every case below runs in milliseconds with no Kafka, no Spark and no Cassandra --
which is exactly why the scorer was written first.

Source: RCP, *National Early Warning Score (NEWS) 2* (2017).
"""

from __future__ import annotations

import ast
import re
from itertools import pairwise
from pathlib import Path

import pytest
from ward.clinical.news2 import (
    HEART_RATE,
    RESPIRATORY_RATE,
    SPO2_SCALE_1,
    SPO2_SCALE_2_ON_AIR,
    SPO2_SCALE_2_ON_OXYGEN,
    SYSTOLIC_BP,
    TEMPERATURE,
    News2Result,
    score_news2,
)

WARD = Path(__file__).resolve().parents[2] / "ward"

# A patient scoring zero on everything. Each test perturbs ONE parameter so that the
# resulting total is attributable to that parameter alone.
NORMAL: dict[str, object] = {
    "respiratory_rate": 16,
    "spo2": 98,
    "on_supplemental_oxygen": False,
    "systolic_bp": 120,
    "heart_rate": 70,
    "consciousness": "A",
    "temperature": 37.0,
}


def scored(**overrides: object) -> News2Result:
    return score_news2(**{**NORMAL, **overrides})  # type: ignore[arg-type]


# --------------------------------------------------------------------------- table


@pytest.mark.parametrize(
    ("rate", "expected"),
    [(4, 3), (8, 3), (9, 1), (11, 1), (12, 0), (20, 0), (21, 2), (24, 2), (25, 3), (40, 3)],
)
def test_respiratory_rate_bands(rate: int, expected: int) -> None:
    assert scored(respiratory_rate=rate).subscores["respiratory_rate"] == expected


@pytest.mark.parametrize(
    ("spo2", "expected"),
    [(85, 3), (91, 3), (92, 2), (93, 2), (94, 1), (95, 1), (96, 0), (100, 0)],
)
def test_spo2_scale_1_bands(spo2: int, expected: int) -> None:
    assert scored(spo2=spo2).subscores["spo2"] == expected


@pytest.mark.parametrize(
    ("sbp", "expected"),
    [(80, 3), (90, 3), (91, 2), (100, 2), (101, 1), (110, 1), (111, 0), (219, 0), (220, 3)],
)
def test_systolic_bp_bands(sbp: int, expected: int) -> None:
    assert scored(systolic_bp=sbp).subscores["systolic_bp"] == expected


@pytest.mark.parametrize(
    ("hr", "expected"),
    [
        (35, 3),
        (40, 3),
        (41, 1),
        (50, 1),
        (51, 0),
        (90, 0),
        (91, 1),
        (110, 1),
        (111, 2),
        (130, 2),
        (131, 3),
        (180, 3),
    ],
)
def test_heart_rate_bands(hr: int, expected: int) -> None:
    assert scored(heart_rate=hr).subscores["heart_rate"] == expected


@pytest.mark.parametrize(
    ("temp", "expected"),
    [
        (34.0, 3),
        (35.0, 3),
        (35.1, 1),
        (36.0, 1),
        (36.1, 0),
        (38.0, 0),
        (38.1, 1),
        (39.0, 1),
        (39.1, 2),
        (41.0, 2),
    ],
)
def test_temperature_bands(temp: float, expected: int) -> None:
    assert scored(temperature=temp).subscores["temperature"] == expected


@pytest.mark.parametrize(("acvpu", "expected"), [("A", 0), ("C", 3), ("V", 3), ("P", 3), ("U", 3)])
def test_consciousness_scores(acvpu: str, expected: int) -> None:
    """Anything other than Alert scores 3 -- there is no intermediate value, which is
    why one consciousness finding can trigger the red-parameter rule alone."""
    assert scored(consciousness=acvpu).subscores["consciousness"] == expected


def test_supplemental_oxygen_scores_two() -> None:
    """Needing oxygen is itself a severity signal, so it scores independently of SpO2."""
    assert scored(on_supplemental_oxygen=False).subscores["supplemental_oxygen"] == 0
    assert scored(on_supplemental_oxygen=True).subscores["supplemental_oxygen"] == 2


# ------------------------------------------------------- the rule most people miss


def test_single_red_parameter_escalates_despite_a_low_total() -> None:
    """★ SpO2 85% with everything else normal totals 3 -- inside the 'low' band --
    but a single parameter scoring 3 requires urgent review.

    This is the case that separates implementing THIS score from implementing A
    score, and getting the check order wrong silently downgrades exactly the patients
    the rule exists to catch.
    """
    result = scored(spo2=85)
    assert result.total == 3
    assert result.any_parameter_is_3 is True
    assert result.clinical_risk == "LOW_MEDIUM"
    assert any("urgent review" in line for line in result.explanation)


def test_a_total_of_three_without_a_red_parameter_stays_low() -> None:
    """The control for the test above: same total, no single parameter at 3."""
    result = scored(
        respiratory_rate=9, heart_rate=95, on_supplemental_oxygen=False, temperature=38.5
    )
    assert result.total == 3
    assert result.any_parameter_is_3 is False
    assert result.clinical_risk == "LOW"


@pytest.mark.parametrize(
    ("total_driver", "expected_risk"),
    [({}, "LOW"), ({"heart_rate": 95}, "LOW")],
)
def test_low_band(total_driver: dict[str, object], expected_risk: str) -> None:
    assert scored(**total_driver).clinical_risk == expected_risk


def test_risk_tiers_at_their_boundaries() -> None:
    """5 is the first MEDIUM, 7 the first HIGH. Off-by-one here is a clinical error."""
    assert scored(respiratory_rate=25, heart_rate=95, temperature=38.5).total == 5
    assert scored(respiratory_rate=25, heart_rate=95, temperature=38.5).clinical_risk == "MEDIUM"
    assert scored(respiratory_rate=25, heart_rate=115, temperature=38.5).total == 6
    assert scored(respiratory_rate=25, heart_rate=115, temperature=38.5).clinical_risk == "MEDIUM"
    assert scored(respiratory_rate=25, heart_rate=115, temperature=38.5, spo2=94).total == 7
    assert (
        scored(respiratory_rate=25, heart_rate=115, temperature=38.5, spo2=94).clinical_risk
        == "HIGH"
    )


# ----------------------------------------------------------- v1 vs v2, the replay


def test_copd_patient_is_over_scored_under_v1_and_correct_under_v2() -> None:
    """★ The reason the replay exists.

    A COPD patient breathing air at 90% is AT their target saturation of 88-92%.
    Under v1's single scale that is 3 points -- a red parameter, urgent review, every
    single reading, forever. Under v2's Scale 2 it is 0. That is not a tuning
    difference; it is the score being wrong for a whole class of patient in a way
    that trains staff to ignore it.
    """
    v1 = score_news2(**{**NORMAL, "spo2": 90}, copd_scale2=True, version="v1")  # type: ignore[arg-type]
    v2 = score_news2(**{**NORMAL, "spo2": 90}, copd_scale2=True, version="v2")  # type: ignore[arg-type]

    assert v1.subscores["spo2"] == 3
    assert v1.clinical_risk == "LOW_MEDIUM"
    assert v2.subscores["spo2"] == 0
    assert v2.clinical_risk == "LOW"
    assert v2.total < v1.total, "the replay must only ever move COPD scores downward"


def test_non_copd_patients_are_identical_under_v1_and_v2() -> None:
    """The other half of the replay's expected diff: everyone else must not move.

    `test_replay_expected_diff` later asserts this over real data. If a
    non-COPD patient's score changed between versions, the replay would be
    re-deriving history under a rule that differs in ways nobody intended.
    """
    for spo2 in (85, 91, 92, 94, 96, 100):
        v1 = score_news2(**{**NORMAL, "spo2": spo2}, copd_scale2=False, version="v1")  # type: ignore[arg-type]
        v2 = score_news2(**{**NORMAL, "spo2": spo2}, copd_scale2=False, version="v2")  # type: ignore[arg-type]
        assert v1.total == v2.total, f"spo2={spo2} changed between versions"


def test_scale_2_splits_on_oxygen() -> None:
    """90% on air is the target; 90% on oxygen means the oxygen is doing the work."""
    on_air = score_news2(
        **{**NORMAL, "spo2": 90, "on_supplemental_oxygen": False},  # type: ignore[arg-type]
        copd_scale2=True,
    )
    on_o2 = score_news2(
        **{**NORMAL, "spo2": 96, "on_supplemental_oxygen": True},  # type: ignore[arg-type]
        copd_scale2=True,
    )
    assert on_air.subscores["spo2"] == 0
    assert on_o2.subscores["spo2"] == 2


# ------------------------------------------------------------- partial readings


def test_a_failed_probe_is_scored_on_what_remains() -> None:
    """A missing SpO2 is not a normal SpO2.

    Discarding the whole reading because one probe failed loses real information
    about a patient who may be deteriorating -- the failure direction a monitoring
    system must never take.
    """
    result = scored(spo2=None)
    assert "spo2" not in result.subscores
    assert result.parameters_missing == 1
    assert result.total == 0


def test_missing_parameters_do_not_score_as_normal() -> None:
    """The specific hazard: None must not collapse to 0 and hide a sick patient."""
    healthy = scored(spo2=98, heart_rate=70)
    partial = scored(spo2=None, heart_rate=None)
    assert partial.parameters_missing == 2
    assert healthy.parameters_missing == 0


def test_every_parameter_missing_still_returns_a_result() -> None:
    result = score_news2(None, None, False, None, None, None, None)
    assert result.parameters_missing == 6
    assert result.total == 0
    assert result.clinical_risk == "LOW"


# ------------------------------------------------------------------ table shape


ALL_TABLES = [
    ("respiratory_rate", RESPIRATORY_RATE, 1.0),
    ("spo2_scale_1", SPO2_SCALE_1, 1.0),
    ("spo2_scale_2_air", SPO2_SCALE_2_ON_AIR, 1.0),
    ("spo2_scale_2_oxygen", SPO2_SCALE_2_ON_OXYGEN, 1.0),
    ("systolic_bp", SYSTOLIC_BP, 1.0),
    ("heart_rate", HEART_RATE, 1.0),
    # Temperature is continuous, so it is swept far more finely than the others.
    ("temperature", TEMPERATURE, 0.01),
]


@pytest.mark.parametrize(
    ("name", "bands", "_step"), ALL_TABLES, ids=[t[0] for t in ALL_TABLES]
)
def test_bands_are_ordered_and_unbounded_at_both_ends(
    name: str, bands: tuple, _step: float
) -> None:
    assert bands[0][0] is None, f"{name}: first band must be unbounded below"
    assert bands[-1][1] is None, f"{name}: last band must be unbounded above"
    for (_, prev_high, _), (next_low, _, _) in pairwise(bands):
        assert prev_high is not None and next_low is not None
        assert next_low >= prev_high, f"{name}: bands run backwards at {prev_high}/{next_low}"


@pytest.mark.parametrize(
    ("name", "bands", "step"), ALL_TABLES, ids=[t[0] for t in ALL_TABLES]
)
def test_no_value_in_the_plausible_range_falls_through_a_gap(
    name: str, bands: tuple, step: float
) -> None:
    """★ A SWEEP, not an inspection.

    The previous version of this test checked only that bands did not OVERLAP, and
    passed happily on a temperature table with holes in it: the published RCP bands
    read 35.1-36.0 and 36.1-38.0, which leaves 36.05 matching nothing. `_band_score`
    raises on an unmatched value, so the sepsis narrative crashed the first time it
    generated a continuous temperature.

    Checking a property by walking the actual domain would have caught it; checking a
    weaker property that happened to hold did not. Sweeping is the honest version.
    """
    from ward.clinical.news2 import _band_score

    low = (bands[0][1] or 0) - 50
    high = (bands[-1][0] or 0) + 50
    value = low
    while value <= high:
        assert _band_score(value, bands) is not None, f"{name}: {value} fell through a gap"
        value = round(value + step, 4)


# --------------------------------------------------------- the architecture test


@pytest.mark.arch
def test_only_one_scorer_exists() -> None:
    """★ The Kappa argument, expressed as a test. Run this on screen in the viva.

    Nothing outside `ward/clinical/` may compute a risk score. The moment a second
    implementation appears -- a "quick" score inside the streaming job, a duplicate in
    the API for a dashboard -- the claim that the ward screen and the daily report
    cannot disagree about a patient becomes false, and the architecture argument
    collapses with it.

    Matched on the AST rather than by grepping text, so a comment mentioning NEWS2
    does not trip it and a cleverly formatted definition cannot evade it.
    """
    offenders: list[str] = []
    for path in sorted(WARD.rglob("*.py")):
        if "clinical" in path.parts:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and re.search(
                r"score|news2", node.name, re.IGNORECASE
            ):
                offenders.append(f"{path.relative_to(WARD.parent)}:{node.lineno} def {node.name}")
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and re.fullmatch(
                        r"(NEWS2_\w+|\w*RISK_TABLE|\w*SCORING_TABLE)", target.id
                    ):
                        offenders.append(
                            f"{path.relative_to(WARD.parent)}:{node.lineno} {target.id}"
                        )

    assert not offenders, (
        "a second scoring implementation exists outside ward/clinical/:\n  "
        + "\n  ".join(offenders)
    )
