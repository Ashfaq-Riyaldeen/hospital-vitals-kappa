"""NEWS2 — the National Early Warning Score 2.

★ THE ONE CLINICAL RULE. ★

This module is what the entire Kappa argument protects. Every output the system
produces -- the ward screen, the per-patient alert, the daily risk report -- is a view
of one quantity: a patient's current risk. Computing that quantity in two places, as a
Lambda speed layer and batch layer would require, is not a maintenance inconvenience in
this domain. It is a patient-safety hazard: the two implementations drift, and the
screen and the report disagree about whether somebody is deteriorating.

So there is exactly one implementation, and `tests/unit/test_news2.py::
test_only_one_scorer_exists` fails the build if a second one ever appears.

DESIGN CONSTRAINTS, all deliberate:

* **Pure Python.** This file imports nothing from Spark, Kafka or Cassandra. It takes
  primitives and returns a frozen dataclass. That is what lets the whole clinical rule
  be tested exhaustively in milliseconds with no infrastructure, and what lets the
  replay run the same function over historical data without a pipeline.

* **The table is data, not `if`-chains.** Each parameter is a tuple of bands. A
  threshold is a row you can read against the published specification, not a branch
  buried in control flow. Reviewing `(21, 24, 2)` against the RCP table is a
  five-second check; reviewing `elif 21 <= rr <= 24:` is not.

* **No magic numbers anywhere else.** Every clinical threshold lives here with its
  source. If a number that scores a patient appears in another file, that is a bug.

SOURCE: Royal College of Physicians, *National Early Warning Score (NEWS) 2:
Standardising the assessment of acute-illness severity in the NHS* (2017).

    THIS IS NOT A MEDICAL DEVICE. It is a teaching implementation over entirely
    simulated data and has not been clinically validated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Literal

# A band is (low, high, score), both bounds INCLUSIVE. `None` means unbounded on that
# side. Bands are checked in order and the first match wins, so they must not overlap.
Band = tuple[float | None, float | None, int]

# --------------------------------------------------------------------------- bands
#
# Read each of these against the RCP table. That is the point of the shape.

RESPIRATORY_RATE: Final[tuple[Band, ...]] = (
    (None, 8, 3),
    (9, 11, 1),
    (12, 20, 0),
    (21, 24, 2),
    (25, None, 3),
)

# Scale 1 — the default, for patients whose target oxygen saturation is 94-98%.
SPO2_SCALE_1: Final[tuple[Band, ...]] = (
    (None, 91, 3),
    (92, 93, 2),
    (94, 95, 1),
    (96, None, 0),
)

# Scale 2 — for patients with hypercapnic respiratory failure (typically COPD) whose
# target saturation is 88-92%. This is the clinically important difference between
# NEWS and NEWS2, and the reason the replay in plan/06 is a real scenario rather than
# a contrived one: under Scale 1 these patients are PERSISTENTLY OVER-SCORED for a
# saturation that is normal for them, generating alert noise that trains staff to
# ignore the score.
#
# Scale 2 is split because the scoring depends on whether the patient is on oxygen:
# 88-92% breathing air is the target range and scores 0, while the same saturation on
# supplemental oxygen means the oxygen is doing the work and scores upward.
SPO2_SCALE_2_ON_AIR: Final[tuple[Band, ...]] = (
    (None, 83, 3),
    (84, 85, 2),
    (86, 87, 1),
    (88, None, 0),  # 88-92 is target; >=93 on air is also 0
)

SPO2_SCALE_2_ON_OXYGEN: Final[tuple[Band, ...]] = (
    (None, 83, 3),
    (84, 85, 2),
    (86, 87, 1),
    (88, 92, 0),
    (93, 94, 1),
    (95, 96, 2),
    (97, None, 3),
)

SYSTOLIC_BP: Final[tuple[Band, ...]] = (
    (None, 90, 3),
    (91, 100, 2),
    (101, 110, 1),
    (111, 219, 0),
    (220, None, 3),
)

HEART_RATE: Final[tuple[Band, ...]] = (
    (None, 40, 3),
    (41, 50, 1),
    (51, 90, 0),
    (91, 110, 1),
    (111, 130, 2),
    (131, None, 3),
)

# Temperature is the only CONTINUOUS parameter here, and it is the one place the
# published table cannot be transcribed literally.
#
# The RCP table reads 35.1-36.0, 36.1-38.0, 38.1-39.0 because a temperature is charted
# to one decimal place. Encoded literally, those bands leave GAPS -- 36.05 degrees
# matches nothing -- and a simulator producing continuous values walks straight into
# them. Found exactly that way: the sepsis narrative raised on its first run.
#
# Each band's lower bound is therefore the previous band's upper bound. Because bands
# are checked in order and the FIRST match wins, every published boundary value still
# scores as the table says (36.0 -> 1, 38.0 -> 0, 39.0 -> 1) while the line is now
# fully covered. Verified by a sweep test, not by inspection.
TEMPERATURE: Final[tuple[Band, ...]] = (
    (None, 35.0, 3),
    (35.0, 36.0, 1),
    (36.0, 38.0, 0),
    (38.0, 39.0, 1),
    (39.0, None, 2),
)

# Air scores nothing; supplemental oxygen scores 2. Needing oxygen is itself a sign of
# illness severity, which is why it is a scored parameter rather than only a modifier
# of the SpO2 scale.
SUPPLEMENTAL_OXYGEN_SCORE: Final[int] = 2

# ACVPU. Anything other than Alert scores 3 -- there are no intermediate values, which
# is why a single consciousness finding can trigger the red-parameter rule on its own.
ALERT: Final[str] = "A"
ALTERED_CONSCIOUSNESS_SCORE: Final[int] = 3

# --------------------------------------------------------------------------- risk

LOW: Final[str] = "LOW"
LOW_MEDIUM: Final[str] = "LOW_MEDIUM"
MEDIUM: Final[str] = "MEDIUM"
HIGH: Final[str] = "HIGH"

MEDIUM_THRESHOLD: Final[int] = 5
HIGH_THRESHOLD: Final[int] = 7
RED_PARAMETER_SCORE: Final[int] = 3

ScorerVersion = Literal["v1", "v2"]

PARAMETERS: Final[tuple[str, ...]] = (
    "respiratory_rate",
    "spo2",
    "supplemental_oxygen",
    "systolic_bp",
    "heart_rate",
    "consciousness",
    "temperature",
)


@dataclass(frozen=True, slots=True)
class News2Result:
    """One patient, one moment, one score.

    Frozen because a score is a fact about an instant. Anything that wants a different
    score computes a new one; nothing mutates a score after the fact, which is what
    makes a stored score safe to compare against a replayed one.
    """

    total: int
    subscores: dict[str, int]
    any_parameter_is_3: bool
    clinical_risk: str
    parameters_missing: int
    scorer_version: str
    # Carried so the API can show WHY a patient scored what they did. A number a
    # clinician cannot decompose is a number they will not act on.
    explanation: tuple[str, ...] = field(default_factory=tuple)


def _band_score(value: float | None, bands: tuple[Band, ...]) -> int | None:
    """Score one parameter, or None when the reading is absent.

    None is returned rather than 0 on purpose. A missing SpO2 is not a normal SpO2,
    and collapsing the two would let a failed probe look like a healthy patient --
    which is precisely the direction a monitoring system must not fail in.
    """
    if value is None:
        return None
    for low, high, score in bands:
        if (low is None or value >= low) and (high is None or value <= high):
            return score
    # Unreachable while the bands cover the real line, which the band-coverage test
    # asserts. Raising rather than returning 0 keeps a table edit from silently
    # scoring an out-of-range value as normal.
    raise ValueError(f"no band matched value {value!r}; the scoring table has a gap")


def _spo2_bands(
    *, on_supplemental_oxygen: bool, copd_scale2: bool, version: ScorerVersion
) -> tuple[Band, ...]:
    """Pick the SpO2 scale. This one choice is the whole v1/v2 difference.

    v1 (NEWS, 2012) had a single scale for every patient. v2 (NEWS2, 2017) added
    Scale 2 for hypercapnic respiratory failure. Keeping both behind a version flag in
    ONE module -- rather than forking the scorer -- is what makes the replay a version
    change instead of two codebases, and is the same argument as the single-rule
    constraint one level down.
    """
    if version == "v1" or not copd_scale2:
        return SPO2_SCALE_1
    return SPO2_SCALE_2_ON_OXYGEN if on_supplemental_oxygen else SPO2_SCALE_2_ON_AIR


def _clinical_risk(total: int, any_parameter_is_3: bool) -> str:
    """Map an aggregate to a response tier.

    THE RED-PARAMETER RULE is the part naive implementations miss, and it is checked
    before the aggregate bands: a patient with SpO2 85% and everything else normal
    totals 3 -- comfortably inside the "low" band -- but a single parameter scoring 3
    requires urgent review regardless of the total. Ordering the checks the other way
    round would silently downgrade exactly the patients the rule exists to catch.
    """
    if total >= HIGH_THRESHOLD:
        return HIGH
    if total >= MEDIUM_THRESHOLD:
        return MEDIUM
    if any_parameter_is_3:
        return LOW_MEDIUM
    return LOW


def score_news2(
    respiratory_rate: int | None,
    spo2: int | None,
    on_supplemental_oxygen: bool,
    systolic_bp: int | None,
    heart_rate: int | None,
    consciousness: str | None,
    temperature: float | None,
    *,
    copd_scale2: bool = False,
    version: ScorerVersion = "v2",
) -> News2Result:
    """Compute NEWS2 for one set of observations.

    Units, because this project has two genuine units hazards and this is one of them:
    `respiratory_rate` breaths/min, `spo2` percent, `systolic_bp` mmHg, `heart_rate`
    beats/min, `temperature` degrees CELSIUS, `consciousness` one of ACVPU.

    PARTIAL READINGS ARE SCORED, NOT DISCARDED. A reading with a valid heart rate and a
    failed SpO2 probe is scored on the parameters present and reports
    `parameters_missing`. Throwing the whole reading away because one probe failed
    would lose real information about a patient who may be deteriorating -- and in a
    monitoring system, discarding data is the failure mode that kills people. The
    caller decides what to do with a partial score; this function does not decide for
    them by returning nothing.
    """
    subscores: dict[str, int] = {}
    missing = 0
    explanation: list[str] = []

    numeric: tuple[tuple[str, float | None, tuple[Band, ...]], ...] = (
        ("respiratory_rate", respiratory_rate, RESPIRATORY_RATE),
        (
            "spo2",
            spo2,
            _spo2_bands(
                on_supplemental_oxygen=on_supplemental_oxygen,
                copd_scale2=copd_scale2,
                version=version,
            ),
        ),
        ("systolic_bp", systolic_bp, SYSTOLIC_BP),
        ("heart_rate", heart_rate, HEART_RATE),
        ("temperature", temperature, TEMPERATURE),
    )

    for name, value, bands in numeric:
        score = _band_score(value, bands)
        if score is None:
            missing += 1
            continue
        subscores[name] = score
        if score > 0:
            explanation.append(f"{name}={value} scores {score}")

    # Supplemental oxygen is a boolean observation, so it is never "missing" in the
    # way a failed probe is -- either the patient is on oxygen or they are not.
    oxygen_score = SUPPLEMENTAL_OXYGEN_SCORE if on_supplemental_oxygen else 0
    subscores["supplemental_oxygen"] = oxygen_score
    if oxygen_score:
        explanation.append(f"on supplemental oxygen scores {oxygen_score}")

    if consciousness is None:
        missing += 1
    else:
        conscious_score = (
            0 if consciousness.strip().upper() == ALERT else ALTERED_CONSCIOUSNESS_SCORE
        )
        subscores["consciousness"] = conscious_score
        if conscious_score:
            explanation.append(f"consciousness={consciousness} scores {conscious_score}")

    total = sum(subscores.values())
    any_red = any(score >= RED_PARAMETER_SCORE for score in subscores.values())
    risk = _clinical_risk(total, any_red)

    if any_red and total < MEDIUM_THRESHOLD:
        explanation.append("single parameter scoring 3 requires urgent review")

    return News2Result(
        total=total,
        subscores=subscores,
        any_parameter_is_3=any_red,
        clinical_risk=risk,
        parameters_missing=missing,
        scorer_version=version,
        explanation=tuple(explanation),
    )
